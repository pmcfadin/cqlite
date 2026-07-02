//! Issue #1394: Flush/truncate crash window — replaying already-flushed WAL
//! mutations must be idempotent (double-replay test). Mirrors Cassandra
//! `RecoveryManagerFlushedTest`.
//!
//! ## The gap
//!
//! `WriteEngine::flush` does two durable steps in sequence:
//!   1. `writer.finish()` — the SSTable (gen-N) becomes durable on disk.
//!   2. `wal.truncate()`  — the WAL entries for that data are discarded.
//!
//! A crash BETWEEN these two steps leaves the SSTable durable AND the WAL
//! un-truncated. On restart the engine replays those already-flushed mutations
//! back into the memtable and flushes them again as gen-(N+1). This SHOULD be
//! last-write-wins idempotent — the duplicate generation carries identical
//! timestamps/values, so the reconciled table state is unchanged — but nothing
//! tested it, and silent duplication doubles rows through compaction.
//!
//! ## How the crash window is simulated (no production seam required)
//!
//! Every `write()` fsyncs the WAL, so the on-disk `commitlog.wal` immediately
//! before `flush()` is EXACTLY the byte state that would survive a crash in the
//! window (SSTable finish() done, truncate() not yet run). The tests snapshot
//! that file before the flush and restore it after — reproducing the crash
//! window with pure filesystem operations against the public API.
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features write-support \
//!     --test issue_1394_wal_double_replay_idempotence

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::merge::compact_sstables;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteAheadLog, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

const KS: &str = "recovery_ks";
const TBL: &str = "flushed";

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
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

fn write_row(id: i32, name: &str, score: i32, ts: i64) -> Mutation {
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
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

fn delete_row(id: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    Mutation::new(
        TableId::new(KS, TBL),
        pk,
        None,
        vec![CellOperation::DeleteRow],
        ts,
        None,
    )
}

fn sstable_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(KS).join(TBL)
}

/// Sorted list of `*-big-Data.db` files under a table directory.
fn data_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().ends_with("-big-Data.db"))
                .unwrap_or(false)
        })
        .collect();
    v.sort();
    v
}

/// Extract a column value from a scan row (`Value::Map` of `(Text(col), value)`).
fn col<'a>(row: &'a Value, name: &str) -> Option<&'a Value> {
    match row {
        Value::Map(pairs) => pairs.iter().find_map(|(k, v)| match k {
            Value::Text(c) if c == name => Some(v),
            _ => None,
        }),
        _ => None,
    }
}

/// Write `mutations` through a fresh engine, snapshot the (already fsynced) WAL
/// into `wal_snapshot` just before the flush — the crash-window byte state
/// (SSTable durable, WAL not yet truncated) — then flush and drop the engine.
fn write_and_flush_snapshotting_wal(
    rt: &tokio::runtime::Runtime,
    data_dir: &Path,
    wal_dir: &Path,
    schema: &TableSchema,
    mutations: Vec<Mutation>,
    wal_snapshot: &Path,
) {
    let config = WriteEngineConfig::new(
        data_dir.to_path_buf(),
        wal_dir.to_path_buf(),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("engine open");
    for m in mutations {
        engine.write(m).expect("write");
    }
    // The WAL is fsynced on every write, so this file is exactly what a crash
    // between writer.finish() and wal.truncate() would leave on disk.
    let wal_file = wal_dir.join(WriteAheadLog::WAL_FILENAME);
    std::fs::copy(&wal_file, wal_snapshot).expect("snapshot WAL before flush");
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("non-empty sstable");
    // Drop releases the dir lock and closes the WAL handle so we can restore the
    // snapshot underneath a fresh engine (the restart).
    drop(engine);
}

/// Simulate the crash: pretend `wal.truncate()` never ran. Restore the snapshot,
/// reopen the engine (replay repopulates the memtable), assert the replayed row
/// count, then flush the replayed data to the NEXT generation.
fn restore_wal_and_replay_flush(
    rt: &tokio::runtime::Runtime,
    data_dir: &Path,
    wal_dir: &Path,
    schema: &TableSchema,
    wal_snapshot: &Path,
    expect_replayed_rows: usize,
) {
    let wal_file = wal_dir.join(WriteAheadLog::WAL_FILENAME);
    std::fs::copy(wal_snapshot, &wal_file).expect("restore crash-window WAL");

    let config = WriteEngineConfig::new(
        data_dir.to_path_buf(),
        wal_dir.to_path_buf(),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("engine reopen (restart)");
    assert_eq!(
        engine.memtable_row_count(),
        expect_replayed_rows,
        "WAL replay must repopulate the memtable with the un-truncated (already-flushed) mutations"
    );
    rt.block_on(engine.flush())
        .expect("replay flush")
        .expect("non-empty replayed sstable");
    drop(engine);
}

/// Reopen `data_dir` via `SSTableManager` and return the reconciled live rows
/// keyed by partition key bytes.
fn scan_reconciled(
    rt: &tokio::runtime::Runtime,
    data_dir: &Path,
    schema: &TableSchema,
) -> (usize, HashMap<Vec<u8>, Value>) {
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

    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());
    let results = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(schema)))
        .expect("scan must not error");

    let by_pk: HashMap<Vec<u8>, Value> = results
        .iter()
        .map(|(k, v)| (k.0.clone(), v.clone()))
        .collect();
    (results.len(), by_pk)
}

fn pk_bytes(id: i32) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

// ════════════════════════════════════════════════════════════════════════════
// AC1: full read across gen-1 + gen-2 returns each row EXACTLY ONCE.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn ac1_double_replay_reads_each_row_exactly_once() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let wal_snapshot = temp.path().join("crash-window.wal");
    let schema = make_schema();

    // Five distinct live rows with explicit timestamps (no tombstones/TTLs).
    let mutations = vec![
        write_row(1, "alice", 10, 100),
        write_row(2, "bob", 20, 100),
        write_row(3, "carol", 30, 100),
        write_row(4, "dave", 40, 100),
        write_row(5, "erin", 50, 100),
    ];

    // gen-1 durable; WAL snapshotted in the crash window (pre-truncate).
    write_and_flush_snapshotting_wal(
        &rt,
        &data_dir,
        &wal_dir,
        &schema,
        mutations.clone(),
        &wal_snapshot,
    );

    // Crash → restart → replay → gen-2 (duplicate of gen-1).
    restore_wal_and_replay_flush(
        &rt,
        &data_dir,
        &wal_dir,
        &schema,
        &wal_snapshot,
        mutations.len(),
    );

    // Precondition: two DISTINCT generations of the same data are on disk.
    let tbl_dir = sstable_dir(&data_dir);
    let gens = data_files(&tbl_dir);
    assert_eq!(
        gens.len(),
        2,
        "test must exercise a duplicated (gen-1 + replayed gen-2) directory"
    );

    // Idempotence signal: the replayed generation reproduces gen-1's Data.db
    // byte-for-byte (identical mutations → identical flush output).
    let g1 = std::fs::read(&gens[0]).expect("read gen1 Data.db");
    let g2 = std::fs::read(&gens[1]).expect("read gen2 Data.db");
    assert_eq!(
        g1, g2,
        "the replayed generation must be a byte-identical duplicate of the flushed generation"
    );

    // AC1: reconciled scan returns each row exactly once with correct values.
    let (row_count, by_pk) = scan_reconciled(&rt, &data_dir, &schema);
    assert_eq!(
        row_count,
        by_pk.len(),
        "no duplicate partition keys across the duplicated generations"
    );
    assert_eq!(
        row_count, 5,
        "expected exactly 5 live rows after double-replay merge, got {row_count}"
    );

    for (id, name, score) in [
        (1, "alice", 10),
        (2, "bob", 20),
        (3, "carol", 30),
        (4, "dave", 40),
        (5, "erin", 50),
    ] {
        let row = by_pk.get(&pk_bytes(id)).expect("row present exactly once");
        assert_eq!(
            col(row, "name"),
            Some(&Value::Text(name.to_string())),
            "PK{id} name value/timestamp must be intact after double-replay"
        );
        assert_eq!(
            col(row, "score"),
            Some(&Value::Integer(score)),
            "PK{id} score value must be intact after double-replay"
        );
    }

    drop(temp);
}

// ════════════════════════════════════════════════════════════════════════════
// AC2: compacting the duplicated gen-1 + gen-2 equals the single-copy
// expectation byte-wise (no duplicate cells, no timestamp drift).
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn ac2_compaction_of_double_replay_matches_single_copy_bytewise() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let schema = make_schema();
    // A fixed output generation for BOTH compactions so nothing generation-derived
    // can diverge between the reference and the double-replay output.
    const OUT_GEN: u64 = 500;

    let mutations = vec![
        write_row(10, "one", 111, 700),
        write_row(20, "two", 222, 700),
        write_row(30, "three", 333, 700),
    ];

    // ── Double-replay side: gen-1 + replayed gen-2 (identical duplicates). ──
    let dup_temp = TempDir::new().unwrap();
    let dup_data = dup_temp.path().join("data");
    let dup_wal = dup_temp.path().join("wal");
    let dup_snapshot = dup_temp.path().join("crash-window.wal");
    write_and_flush_snapshotting_wal(
        &rt,
        &dup_data,
        &dup_wal,
        &schema,
        mutations.clone(),
        &dup_snapshot,
    );
    restore_wal_and_replay_flush(
        &rt,
        &dup_data,
        &dup_wal,
        &schema,
        &dup_snapshot,
        mutations.len(),
    );
    let dup_inputs = data_files(&sstable_dir(&dup_data));
    assert_eq!(
        dup_inputs.len(),
        2,
        "double-replay must produce two generations"
    );

    // ── Reference side: the same mutations flushed ONCE (single copy). ──
    let ref_temp = TempDir::new().unwrap();
    let ref_data = ref_temp.path().join("data");
    let ref_wal = ref_temp.path().join("wal");
    {
        let config = WriteEngineConfig::new(ref_data.clone(), ref_wal.clone(), schema.clone());
        let mut engine = WriteEngine::new(config).expect("ref engine");
        for m in mutations.clone() {
            engine.write(m).expect("ref write");
        }
        rt.block_on(engine.flush())
            .expect("ref flush")
            .expect("ref sstable");
        drop(engine);
    }
    let ref_inputs = data_files(&sstable_dir(&ref_data));
    assert_eq!(ref_inputs.len(), 1, "reference must be a single generation");

    // Compact BOTH through the identical one-shot path (full compaction,
    // purge_safe=true, same output generation, gc/now = None → no wall-clock).
    let dup_out = TempDir::new().unwrap();
    let dup_report = rt
        .block_on(compact_sstables(
            dup_inputs,
            dup_out.path(),
            &schema,
            OUT_GEN,
            None,
            None,
            true,
        ))
        .expect("compact double-replay inputs");

    let ref_out = TempDir::new().unwrap();
    let ref_report = rt
        .block_on(compact_sstables(
            ref_inputs,
            ref_out.path(),
            &schema,
            OUT_GEN,
            None,
            None,
            true,
        ))
        .expect("compact single-copy input");

    let dup_bytes = std::fs::read(&dup_report.output.data_path).expect("read dup Data.db");
    let ref_bytes = std::fs::read(&ref_report.output.data_path).expect("read ref Data.db");

    assert_eq!(
        dup_bytes,
        ref_bytes,
        "compacting the duplicated (double-replay) generations must yield a Data.db \
         byte-identical to the single-copy expectation — proving no duplicate cells \
         and no timestamp drift ({} vs {} bytes)",
        dup_bytes.len(),
        ref_bytes.len()
    );

    // Sanity: the reconciled compaction output has exactly the three rows.
    let (row_count, by_pk) = scan_reconciled(&rt, dup_out.path(), &schema);
    assert_eq!(
        row_count, 3,
        "compacted double-replay output must have 3 rows"
    );
    assert_eq!(
        row_count,
        by_pk.len(),
        "no duplicate PKs in compacted output"
    );

    drop(dup_temp);
    drop(ref_temp);
}

// ════════════════════════════════════════════════════════════════════════════
// AC3: a replayed deletion still shadows EXACTLY what it shadowed — no
// resurrection, no double-delete of unrelated rows.
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn ac3_replayed_tombstone_shadows_exactly_what_it_shadowed() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let schema = make_schema();

    // ── gen-1 (base): PK2 live and PK3 live at ts=100. ──
    {
        let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
        let mut engine = WriteEngine::new(config).expect("base engine");
        engine.write(write_row(2, "victim", 22, 100)).unwrap();
        engine.write(write_row(3, "bystander", 33, 100)).unwrap();
        rt.block_on(engine.flush())
            .expect("base flush")
            .expect("gen1");
        // WAL truncated by this flush; drop cleanly.
        drop(engine);
    }

    // ── Crash window: delete PK2 at ts=200, snapshot WAL pre-truncate, flush gen-2. ──
    let wal_snapshot = temp.path().join("crash-window.wal");
    write_and_flush_snapshotting_wal(
        &rt,
        &data_dir,
        &wal_dir,
        &schema,
        vec![delete_row(2, 200)],
        &wal_snapshot,
    );

    // ── Crash → restart → replay the delete → flush gen-3 (duplicate tombstone). ──
    restore_wal_and_replay_flush(&rt, &data_dir, &wal_dir, &schema, &wal_snapshot, 1);

    // Three generations: live base, tombstone, duplicated tombstone.
    let tbl_dir = sstable_dir(&data_dir);
    assert_eq!(
        data_files(&tbl_dir).len(),
        3,
        "expected gen1(live) + gen2(delete) + gen3(replayed delete)"
    );

    // Reconciled scan: PK2 stays deleted (tombstone shadows the gen-1 live row and
    // the duplicate does not resurrect it); PK3 is untouched by the tombstone.
    let (row_count, by_pk) = scan_reconciled(&rt, &data_dir, &schema);
    assert!(
        !by_pk.contains_key(&pk_bytes(2)),
        "PK2 was deleted at ts=200; the replayed duplicate tombstone must not resurrect it"
    );
    let row3 = by_pk
        .get(&pk_bytes(3))
        .expect("PK3 must remain live — the replayed tombstone must not over-shadow it");
    assert_eq!(
        col(row3, "name"),
        Some(&Value::Text("bystander".to_string()))
    );
    assert_eq!(col(row3, "score"), Some(&Value::Integer(33)));
    assert_eq!(
        row_count, 1,
        "exactly one live row (PK3) survives; got {row_count}"
    );

    // Compact all three generations; the tombstone must still shadow exactly PK2.
    let inputs = data_files(&tbl_dir);
    let out = TempDir::new().unwrap();
    let report = rt
        .block_on(compact_sstables(
            inputs,
            out.path(),
            &schema,
            600,
            None,
            None,
            true,
        ))
        .expect("compact tombstone generations");
    assert!(
        report.output.data_path.exists(),
        "compaction must produce an output SSTable"
    );

    let (c_row_count, c_by_pk) = scan_reconciled(&rt, out.path(), &schema);
    assert!(
        !c_by_pk.contains_key(&pk_bytes(2)),
        "after compaction PK2 must remain shadowed (no resurrection through compaction)"
    );
    assert!(
        c_by_pk.contains_key(&pk_bytes(3)),
        "after compaction PK3 must remain live (tombstone must not double-delete unrelated rows)"
    );
    assert_eq!(
        c_row_count, 1,
        "compacted output must have exactly PK3 live"
    );

    drop(temp);
}
