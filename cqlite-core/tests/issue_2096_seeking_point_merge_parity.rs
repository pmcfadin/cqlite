//! Issue #2096: the multi-candidate `WHERE pk = ?` point read must SEEK each
//! candidate directly to the target partition (BTI/Index.db) and reconcile
//! through the partition-seeking merger, decoding ONLY the target partition per
//! generation — NOT the former full-scan `KWayMerger::new` that sequentially
//! decodes every partition with token <= the target before reaching it.
//!
//! This file pins two things that must never regress:
//!
//! A. **Parity oracle (correctness, non-negotiable)** — the seeking point read
//!    (`SSTableManager::scan_partition`, the public surface) returns rows
//!    BYTE-IDENTICAL to the full-scan reconciliation oracle (`scan(..)` retained
//!    to the target partition) across cross-generation last-write-wins,
//!    cell/row-tombstone shadowing, AND a partition-tombstone resurrection.
//!
//! B. **Work-counter red→green (the headline AC)** — the new
//!    `work_counters::merge_run_partitions_decoded` counter proves the multi-gen
//!    point read decodes only the target partition per generation: the OLD
//!    full-scan merge (exercised here through `scan(..)`, which routes through the
//!    same `merge_generations_for_read` full-scan `KWayMerger::new`) decodes every
//!    partition and reads a LARGE count; the NEW seek path reads a SMALL count
//!    that does not scale with the number of below-target filler partitions.
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features write-support \
//!     --test issue_2096_seeking_point_merge_parity

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::{work_counters, SSTableManager};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, PartitionTombstone, TableId, WriteEngine,
    WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use cqlite_core::{RowKey, ScanRow};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

const KS: &str = "seek_ks";
const TBL: &str = "items";

/// The overwrite + cell-tombstone + row-tombstone target partition.
const TARGET: i32 = 500;
/// The partition-tombstone (resurrection) target partition.
const PTOMB: i32 = 600;
/// Filler partitions written into gen1 only, so the full-scan merge has many
/// partitions to decode while the seek touches only the target.
const FILLER_LO: i32 = 1000;
const FILLER_HI: i32 = 1025; // exclusive → 25 filler partitions

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "score".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn clustering(ck: i32) -> Option<ClusteringKey> {
    Some(ClusteringKey::single("ck", Value::Integer(ck)))
}

fn write_row(id: i32, ck: i32, name: &str, score: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
        CellOperation::Write {
            column: "score".to_string(),
            value: Value::Integer(score),
        },
    ];
    Mutation::new(TableId::new(KS, TBL), pk, clustering(ck), ops, ts, None)
}

/// Write only the `name` column at `(id, ck)` — a disjoint overwrite that must
/// keep the older generation's `score`.
fn write_name_only(id: i32, ck: i32, name: &str, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: Value::Text(name.to_string()),
    }];
    Mutation::new(TableId::new(KS, TBL), pk, clustering(ck), ops, ts, None)
}

fn delete_row(id: i32, ck: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    Mutation::new(
        TableId::new(KS, TBL),
        pk,
        clustering(ck),
        vec![CellOperation::DeleteRow],
        ts,
        None,
    )
}

fn delete_score(id: i32, ck: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Delete {
        column: "score".to_string(),
        local_deletion_time: None,
    }];
    Mutation::new(TableId::new(KS, TBL), pk, clustering(ck), ops, ts, None)
}

/// A mutation whose only effect is a partition-level tombstone on `id`.
fn partition_delete(id: i32, deletion_micros: i64, local_secs: i32) -> Mutation {
    let mut m = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![],
        deletion_micros,
        None,
    );
    m.partition_tombstone = Some(PartitionTombstone {
        deletion_time: deletion_micros,
        local_deletion_time: local_secs,
    });
    m
}

fn count_data_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
        .count()
}

/// Build the 3-generation, clustering-keyed multi-candidate fixture and return
/// its data dir. No compaction runs, so all three generations stay on disk.
fn build_fixture(data_dir: &std::path::Path, wal_dir: &std::path::Path, schema: &TableSchema) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let config = WriteEngineConfig::new(data_dir.to_path_buf(), wal_dir.to_path_buf(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // ── Gen 1 (ts=100): base state ────────────────────────────────────────────
    // TARGET: three clustering rows.
    engine.write(write_row(TARGET, 1, "a1", 10, 100)).unwrap();
    engine.write(write_row(TARGET, 2, "b1", 20, 100)).unwrap();
    engine.write(write_row(TARGET, 3, "c1", 30, 100)).unwrap();
    // PTOMB: one clustering row that a later partition tombstone shadows.
    engine.write(write_row(PTOMB, 1, "p_old", 1, 100)).unwrap();
    // Many filler partitions — the below/above-target partitions the OLD
    // sequential merge walks past.
    for id in FILLER_LO..FILLER_HI {
        engine.write(write_row(id, 1, "filler", id, 100)).unwrap();
    }
    rt.block_on(engine.flush()).expect("flush 1").expect("gen1");

    // ── Gen 2 (ts=200): overwrite + partition tombstone ───────────────────────
    // TARGET ck=1: disjoint name-only overwrite (score=10 from gen1 must survive).
    engine.write(write_name_only(TARGET, 1, "a2", 200)).unwrap();
    // PTOMB: partition tombstone @200µs shadows gen1's ck=1 (writetime 100).
    engine.write(partition_delete(PTOMB, 200, 200)).unwrap();
    rt.block_on(engine.flush()).expect("flush 2").expect("gen2");

    // ── Gen 3 (ts=300): tombstones + resurrection ─────────────────────────────
    // TARGET ck=2: row tombstone shadowing gen1's whole row.
    engine.write(delete_row(TARGET, 2, 300)).unwrap();
    // TARGET ck=3: cell tombstone on `score` (name survives).
    engine.write(delete_score(TARGET, 3, 300)).unwrap();
    // PTOMB ck=1: resurrecting write (writetime 300 > partition tombstone 200).
    engine.write(write_row(PTOMB, 1, "p_new", 9, 300)).unwrap();
    rt.block_on(engine.flush()).expect("flush 3").expect("gen3");

    rt.block_on(engine.close()).expect("close engine");
}

fn open_manager(data_dir: &std::path::Path) -> (SSTableManager, tokio::runtime::Runtime) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let cqlite_config = Config::default();
    let manager = rt.block_on(async {
        let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
        SSTableManager::new(
            data_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager open")
    });
    (manager, rt)
}

fn pk(id: i32) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn col<'a>(row: &'a ScanRow, name: &str) -> Option<&'a Value> {
    match row {
        ScanRow::Row(cells) => cells
            .iter()
            .find_map(|(k, v)| if k.as_ref() == name { Some(v) } else { None }),
        _ => None,
    }
}

/// A. Parity + C. Wiring: the public seeking point read is byte-identical to the
/// full-scan reconciliation oracle for BOTH the overwrite/tombstone target and
/// the partition-tombstone (resurrection) partition.
// `#[serial(work_counters)]`: this test drives scans that bump the process-global
// `merge_run_partitions_decoded`, so it must never run concurrently (in this test
// binary) with the counter-asserting test below, whose `reset()`→scan→read delta
// would otherwise be contaminated (issue #2428 contamination shape).
#[test]
#[serial_test::serial(work_counters)]
fn seeking_point_read_is_byte_identical_to_full_scan_oracle() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();
    build_fixture(&data_dir, &wal_dir, &schema);

    let sstable_dir = data_dir.join(KS).join(TBL);
    assert_eq!(
        count_data_files(&sstable_dir),
        3,
        "fixture must exercise a 3-generation directory (multi-candidate merge)"
    );

    let (manager, rt) = open_manager(&data_dir);
    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());

    // Full-scan reconciliation oracle: the authoritative merged table state.
    let full = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(&schema)))
        .expect("full scan must not error");

    for target in [TARGET, PTOMB] {
        let oracle: Vec<(RowKey, ScanRow)> = full
            .iter()
            .filter(|(k, _)| k.as_bytes() == pk(target))
            .cloned()
            .collect();

        // The NEW seeking public surface (issue #2096) — wiring evidence (C).
        let (seek, engaged) = rt
            .block_on(manager.scan_partition(&table_id, &pk(target), Some(&schema)))
            .expect("scan_partition must not error");
        assert!(engaged, "scan_partition always reports the partition-targeted path");

        assert_eq!(
            seek, oracle,
            "seeking point read for id={target} must be BYTE-IDENTICAL to the full-scan \
             reconciliation oracle (cross-generation LWW / tombstone / partition-tombstone \
             semantics must not diverge)"
        );
        assert!(
            !oracle.is_empty(),
            "id={target} must have surviving reconciled rows (a 0-row parity is a false pass)"
        );
    }

    // Spot-check the reconciled semantics themselves so the parity oracle above is
    // not vacuously comparing two identically-wrong results.
    let target_rows: Vec<&(RowKey, ScanRow)> =
        full.iter().filter(|(k, _)| k.as_bytes() == pk(TARGET)).collect();
    assert_eq!(
        target_rows.len(),
        2,
        "TARGET keeps ck=1 (overwrite) + ck=3 (cell-tombstoned score); ck=2 row-deleted"
    );
    // ck=1 overwrite: gen2 name wins, gen1 score survives (disjoint merge).
    let r1 = &target_rows[0].1;
    assert_eq!(col(r1, "name"), Some(&Value::Text("a2".to_string())));
    assert_eq!(col(r1, "score"), Some(&Value::Integer(10)));
    // ck=3 cell tombstone: name survives, score gone.
    let r3 = &target_rows[1].1;
    assert_eq!(col(r3, "name"), Some(&Value::Text("c1".to_string())));
    assert!(col(r3, "score").is_none(), "score cell-deleted in gen3");

    // PTOMB: only the resurrecting gen3 row survives the gen2 partition tombstone.
    let ptomb_rows: Vec<&(RowKey, ScanRow)> =
        full.iter().filter(|(k, _)| k.as_bytes() == pk(PTOMB)).collect();
    assert_eq!(ptomb_rows.len(), 1, "PTOMB keeps only the resurrecting gen3 row");
    let rp = &ptomb_rows[0].1;
    assert_eq!(
        col(rp, "name"),
        Some(&Value::Text("p_new".to_string())),
        "gen1 row must stay shadowed by the partition tombstone; gen3 resurrects"
    );
    assert_eq!(col(rp, "score"), Some(&Value::Integer(9)));

    drop(temp_dir);
}

/// B. Work-counter red→green: the multi-candidate point read decodes ONLY the
/// target partition per generation. The OLD full-scan merge (exercised via the
/// full `scan(..)`, which routes through the same `KWayMerger::new`) decodes
/// every partition and reads a LARGE `merge_run_partitions_decoded`; the NEW
/// seek path reads a SMALL count that does not scale with the filler count.
///
/// Process-global counters: `#[serial]` + a `reset()` before each measured scan
/// keeps the deltas uncontaminated by any concurrent scan-driving test.
#[test]
#[serial_test::serial(work_counters)]
fn seeking_point_read_decodes_only_the_target_partition() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();
    build_fixture(&data_dir, &wal_dir, &schema);

    let (manager, rt) = open_manager(&data_dir);
    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());

    // TARGET / PTOMB each live in all three generations → 3 candidates hold them.
    const CANDIDATES_HOLDING_TARGET: u64 = 3;
    // Total partitions on disk (25 fillers + TARGET + PTOMB). The OLD full-scan
    // merge decodes at least this many entries.
    let n_partitions = (FILLER_HI - FILLER_LO) as u64 + 2;

    // ── RED proof: the OLD full-scan merge decodes every partition ─────────────
    work_counters::reset();
    let _ = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(&schema)))
        .expect("full scan must not error");
    let counter_full = work_counters::merge_run_partitions_decoded();
    assert!(
        counter_full >= n_partitions,
        "full-scan merge must decode at least one entry per partition (got {counter_full}, \
         expected >= {n_partitions}) — the O(all partitions) path #2096 replaces"
    );

    // ── GREEN: the NEW seek path decodes only the target partition ─────────────
    work_counters::reset();
    let (seek_target, _) = rt
        .block_on(manager.scan_partition(&table_id, &pk(TARGET), Some(&schema)))
        .expect("scan_partition must not error");
    let counter_seek_target = work_counters::merge_run_partitions_decoded();
    assert!(
        !seek_target.is_empty(),
        "TARGET seek must return rows (guards against a 0-row false pass)"
    );

    work_counters::reset();
    let (seek_ptomb, _) = rt
        .block_on(manager.scan_partition(&table_id, &pk(PTOMB), Some(&schema)))
        .expect("scan_partition must not error");
    let counter_seek_ptomb = work_counters::merge_run_partitions_decoded();
    assert!(!seek_ptomb.is_empty(), "PTOMB seek must return rows");

    // The seek decodes at least one entry per candidate holding the key ...
    assert!(
        counter_seek_target >= CANDIDATES_HOLDING_TARGET,
        "the seek decodes >= 1 entry per candidate holding TARGET (got {counter_seek_target}, \
         expected >= {CANDIDATES_HOLDING_TARGET})"
    );
    // ... and stays O(target rows): a small constant independent of the filler
    // count, decisively below the full-scan count. The TARGET partition holds at
    // most ~6 merge entries across the three generations (3 gen1 rows + 1 gen2
    // overwrite + 2 gen3 tombstones); PTOMB fewer. `2 * CANDIDATES...` headroom
    // keeps the bound robust without weakening the O(target) guarantee.
    const SEEK_UPPER_BOUND: u64 = 4 * CANDIDATES_HOLDING_TARGET; // 12
    assert!(
        counter_seek_target <= SEEK_UPPER_BOUND,
        "seeking TARGET read must stay O(target rows) (got {counter_seek_target}, bound \
         {SEEK_UPPER_BOUND}); a regression to the full-scan merge would balloon this toward \
         {counter_full}"
    );
    assert!(
        counter_seek_ptomb <= SEEK_UPPER_BOUND,
        "seeking PTOMB read must stay O(target rows) (got {counter_seek_ptomb}, bound \
         {SEEK_UPPER_BOUND})"
    );
    // The essential red→green relationship: the seek decodes strictly, decisively
    // fewer entries than the OLD full-scan merge on the SAME fixture.
    assert!(
        counter_seek_target < counter_full,
        "seek ({counter_seek_target}) must decode strictly fewer entries than the full-scan \
         merge ({counter_full})"
    );

    drop(temp_dir);
}
