//! Issue #947 — cross-path reconcile parity (PINNING TEST).
//!
//! The Cassandra `Cells#reconcile` tie-break (timestamp wins; at EQUAL timestamp
//! a cell DELETION beats a LIVE/EXPIRING cell, decided BEFORE any
//! `localDeletionTime` compare — parity `a62c749`, issues #848/#498) is applied on
//! TWO independent write paths:
//!
//! * the COMPACTION/merge path — `KWayMerger` reconcile (`reconcile.rs` /
//!   `merge/mod.rs::cell_reconcile_replace`), reconciling `CellData`;
//! * the FLUSH/write path — `DataWriter::merge_row_group`, reconciling `MergedOp`.
//!
//! Issue #947 unifies the two into one shared rule layer. This test PINS the
//! pre-refactor behavior end-to-end: it feeds the SAME logical boundary scenarios
//! through BOTH paths and asserts the read-back rows are byte-for-byte identical,
//! proving the two implementations already agree on the boundary cases (so the
//! extraction is a behavior-frozen refactor, not a semantic change).
//!
//! Boundary cases covered (the cell tie-break axis):
//! * strict-greater timestamp wins (live over live);
//! * a live cell with a strictly-greater timestamp beats an older tombstone;
//! * EQUAL-ts cell tombstone beats a LIVE cell (#848/#498);
//! * EQUAL-ts cell tombstone beats an EXPIRING (TTL) cell.
//!
//! The complex-deletion strict-supersede + shadow-before-purge boundaries
//! (`mfda` strict-greater vs equal; element `ts == mfda` shadowed vs `ts > mfda`
//! survives) are pinned as unit tests on the shared rule layer
//! (`write_engine::reconcile_rules`) and end-to-end by the #819 differential
//! compaction harness.

#![cfg(feature = "write-support")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::merge::compact_sstables;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use std::collections::HashMap;
use tempfile::TempDir;

const TS_LO: i64 = 1_700_000_000_000_000;
const TS_HI: i64 = TS_LO + 1_000_000;

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "recon_ks".to_string(),
        table: "items".to_string(),
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
                name: "a".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "b".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Which input SSTable an edit lands in for the COMPACTION path. The FLUSH path
/// puts every edit in one memtable regardless. Reconcile is driven by each
/// cell's own write TIMESTAMP, not the generation, so a high-ts write may live
/// in the OLD generation file.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Gen {
    Old,
    New,
}

#[derive(Clone)]
struct Edit {
    gen: Gen,
    ops: Vec<CellOperation>,
    ts: i64,
}

fn write(col: &str, v: &str) -> CellOperation {
    CellOperation::Write {
        column: col.to_string(),
        value: Value::Text(v.to_string()),
    }
}

fn write_ttl(col: &str, v: &str, ttl: u32) -> CellOperation {
    CellOperation::WriteWithTtl {
        column: col.to_string(),
        value: Value::Text(v.to_string()),
        ttl_seconds: ttl,
    }
}

fn delete(col: &str) -> CellOperation {
    CellOperation::Delete {
        column: col.to_string(),
        local_deletion_time: None,
    }
}

/// The boundary scenarios, keyed by partition id. Each value is the ordered list
/// of edits applied to that partition.
fn scenarios() -> BTreeMap<i32, Vec<Edit>> {
    let mut m: BTreeMap<i32, Vec<Edit>> = BTreeMap::new();

    // id=1: strict-greater timestamp wins (live over live) -> a="new".
    m.insert(
        1,
        vec![
            Edit {
                gen: Gen::Old,
                ops: vec![write("a", "old")],
                ts: TS_LO,
            },
            Edit {
                gen: Gen::New,
                ops: vec![write("a", "new")],
                ts: TS_HI,
            },
        ],
    );

    // id=2: a live cell at a strictly-greater ts beats an older tombstone.
    m.insert(
        2,
        vec![
            Edit {
                gen: Gen::Old,
                ops: vec![delete("a"), write("b", "w2")],
                ts: TS_LO,
            },
            Edit {
                gen: Gen::New,
                ops: vec![write("a", "live")],
                ts: TS_HI,
            },
        ],
    );

    // id=3: strict-greater live-over-live with a witness column present.
    m.insert(
        3,
        vec![
            Edit {
                gen: Gen::Old,
                ops: vec![write("a", "A"), write("b", "w3")],
                ts: TS_LO,
            },
            Edit {
                gen: Gen::New,
                ops: vec![write("a", "B")],
                ts: TS_HI,
            },
        ],
    );

    // id=4: a live write at a strictly-greater ts beats a NEWER-generation
    // tombstone written at a lower ts (reconcile is by cell ts, not gen).
    m.insert(
        4,
        vec![
            Edit {
                gen: Gen::Old,
                ops: vec![write("a", "keep")],
                ts: TS_HI,
            },
            Edit {
                gen: Gen::New,
                ops: vec![delete("a")],
                ts: TS_LO,
            },
        ],
    );

    // id=5: EQUAL-ts cell tombstone beats a LIVE cell (#848/#498). a deleted,
    // witness b survives.
    m.insert(
        5,
        vec![
            Edit {
                gen: Gen::Old,
                ops: vec![write("a", "x"), write("b", "w5")],
                ts: TS_LO,
            },
            Edit {
                gen: Gen::New,
                ops: vec![delete("a")],
                ts: TS_LO,
            },
        ],
    );

    // id=6: EQUAL-ts cell tombstone beats an EXPIRING (TTL) cell — the deletion
    // wins BEFORE any localDeletionTime/expiry compare. a deleted, witness b
    // survives.
    m.insert(
        6,
        vec![
            Edit {
                gen: Gen::Old,
                ops: vec![write_ttl("a", "e", 3600), write("b", "w6")],
                ts: TS_LO,
            },
            Edit {
                gen: Gen::New,
                ops: vec![delete("a")],
                ts: TS_LO,
            },
        ],
    );

    m
}

fn mutation(id: i32, ops: Vec<CellOperation>, ts: i64) -> Mutation {
    let table_id = TableId::new("recon_ks", "items");
    let pk = PartitionKey::single("id", Value::Integer(id));
    Mutation::new(table_id, pk, None, ops, ts, None)
}

/// Discover published `nb-*-big-Data.db` input files, newest-generation first.
fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    collect(dir, &mut found, 8);
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

fn collect(dir: &Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
        if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
            let base = name.trim_end_matches("-Data.db");
            if !path.with_file_name(format!("{base}-TOC.txt")).exists() {
                continue;
            }
            let generation = name
                .strip_prefix("nb-")
                .and_then(|s| s.split("-big-").next())
                .and_then(|g| g.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((generation, path));
        } else if depth > 0 && path.is_dir() {
            collect(&path, out, depth - 1);
        }
    }
}

/// Read every partition of an output dir into a stable `pk_bytes -> rendered`
/// map. The rendered string is the debug form of the row value; if two write
/// paths reconcile identically, these maps are equal.
fn read_back(rt: &tokio::runtime::Runtime, dir: &Path, schema: &TableSchema) -> BTreeMap<Vec<u8>, String> {
    let cqlite_config = Config::default();
    let manager = rt.block_on(async {
        let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
        SSTableManager::new(
            dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager opens output")
    });
    let table_id = CqlTableId::from("recon_ks.items");
    let results = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(schema)))
        .expect("scan");
    results
        .into_iter()
        .map(|(k, v)| (k.0, format!("{v:?}")))
        .collect()
}

/// FLUSH path: write every edit into one memtable, flush to a single SSTable,
/// read it back.
fn flush_path(rt: &tokio::runtime::Runtime, schema: &TableSchema) -> BTreeMap<Vec<u8>, String> {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");

    for (id, edits) in scenarios() {
        // Old edits first, then New — mirrors the compaction generation order;
        // for these boundary cases the winner is order-independent.
        for edit in edits.iter().filter(|e| e.gen == Gen::Old) {
            engine.write(mutation(id, edit.ops.clone(), edit.ts)).expect("write");
        }
        for edit in edits.iter().filter(|e| e.gen == Gen::New) {
            engine.write(mutation(id, edit.ops.clone(), edit.ts)).expect("write");
        }
    }
    rt.block_on(engine.flush()).expect("flush").expect("info");
    drop(engine);
    read_back(rt, &data_dir, schema)
}

/// COMPACTION path: flush the Old edits into one SSTable and the New edits into
/// another, compact the two, read the merged output back.
fn compaction_path(rt: &tokio::runtime::Runtime, schema: &TableSchema) -> BTreeMap<Vec<u8>, String> {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let output_dir = temp.path().join("out");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");

    // Generation 1 (older): all Old edits.
    for (id, edits) in scenarios() {
        for edit in edits.iter().filter(|e| e.gen == Gen::Old) {
            engine.write(mutation(id, edit.ops.clone(), edit.ts)).expect("write old");
        }
    }
    rt.block_on(engine.flush()).expect("flush old").expect("info old");

    // Generation 2 (newer): all New edits.
    for (id, edits) in scenarios() {
        for edit in edits.iter().filter(|e| e.gen == Gen::New) {
            engine.write(mutation(id, edit.ops.clone(), edit.ts)).expect("write new");
        }
    }
    rt.block_on(engine.flush()).expect("flush new").expect("info new");
    drop(engine);

    let inputs = discover_inputs(&data_dir);
    assert_eq!(inputs.len(), 2, "expected 2 input SSTables, got {inputs:?}");

    rt.block_on(compact_sstables(
        inputs,
        &output_dir,
        schema,
        9,    // output generation
        None, // gc_before: disable gc purge so tombstones are retained (parity with flush)
        None, // now_sec
        true, // purge_safe (full compaction)
    ))
    .expect("compaction");

    read_back(rt, &output_dir, schema)
}

#[test]
fn flush_and_compaction_reconcile_boundaries_identically() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let schema = make_schema();

    let flushed = flush_path(&rt, &schema);
    let compacted = compaction_path(&rt, &schema);

    // The core pinning assertion: the two reconcile implementations produce the
    // SAME read-back rows for every boundary partition.
    assert_eq!(
        flushed, compacted,
        "flush-path and compaction-path reconcile must agree on every boundary case"
    );

    // Targeted semantic anchors so a both-paths-wrong regression is also caught.
    let by_id = |m: &BTreeMap<Vec<u8>, String>, id: i32| -> String {
        let key: Vec<u8> = id.to_be_bytes().into();
        m.get(&key).cloned().unwrap_or_default()
    };

    // id=1 strict-greater live wins.
    let r1 = by_id(&flushed, 1);
    assert!(r1.contains("new") && !r1.contains("old"), "id=1: strict-greater live wins, got {r1}");
    // id=2 live > older tombstone.
    assert!(by_id(&flushed, 2).contains("live"), "id=2: live beats older tombstone");
    // id=4 live@HI beats newer-gen tombstone@LO.
    assert!(by_id(&flushed, 4).contains("keep"), "id=4: live ts wins over lower-ts tombstone");
    // id=5 EQUAL-ts tombstone beats live: column a gone, witness b survives.
    let r5 = by_id(&flushed, 5);
    assert!(r5.contains("w5"), "id=5: witness b must survive, got {r5}");
    assert!(!r5.contains("\"x\""), "id=5: equal-ts tombstone must delete a, got {r5}");
    // id=6 EQUAL-ts tombstone beats expiring: a gone, witness b survives.
    let r6 = by_id(&flushed, 6);
    assert!(r6.contains("w6"), "id=6: witness b must survive, got {r6}");
    assert!(!r6.contains("\"e\""), "id=6: equal-ts tombstone must delete expiring a, got {r6}");
}
