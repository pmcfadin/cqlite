//! Issue #922 — exact per-cell dropped-column purging (per-cell timestamps).
//!
//! Follow-up to #847. #847 filtered dropped-column cells at **row-timestamp**
//! granularity: every live cell inherited the single row timestamp, so a row
//! mixing a **pre-drop** cell of the dropped column with a **post-drop** cell of
//! another column carried one (newer) row timestamp and the dropped cell could
//! wrongly survive compaction.
//!
//! The reader→merge enrichment from #886/#899 now surfaces each simple cell's
//! OWN writetime (`SimpleCell.timestamp` = the cell's `write_timestamp_micros`,
//! not the row timestamp). The existing `drop_time` predicate in
//! `reconcile_cluster` therefore becomes exact: a dropped-column cell written
//! before the drop is purged even when a sibling column in the same row was
//! written after the drop.
//!
//! This test builds exactly that mixed-per-cell-timestamp row end-to-end: two
//! mutations to the SAME (partition, clustering) row at DISTINCT timestamps —
//! `score` (later dropped) written at ts=100, `name` written at ts=300 — flushed
//! into a single SSTable so the row carries two cells with two writetimes. After
//! compacting with `score` dropped at T=150, the `score` cell (ts=100 <= 150)
//! must be purged while `name` (ts=300 > 150) survives.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{compact_sstables, MergeStep, RowData};
use cqlite_core::storage::write_engine::{
    CellOperation, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

/// Schema: keyspace=drop_ks, table=items, PK=id(int), CK=ck(int),
/// columns name(text), score(int). `dropped` lets a test inject drop times.
fn schema_with_drops(dropped: HashMap<String, i64>) -> TableSchema {
    TableSchema {
        keyspace: "drop_ks".to_string(),
        table: "items".to_string(),
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
            col("id", "int"),
            col("ck", "int"),
            col("name", "text"),
            col("score", "int"),
        ],
        comments: HashMap::new(),
        dropped_columns: dropped,
    }
}

fn col(name: &str, ty: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// A mutation writing a SINGLE column to row (id=1, ck=0) at `ts`. Issuing two of
/// these at distinct timestamps assembles one row with two per-cell writetimes.
fn write_one_cell(column: &str, value: Value, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("drop_ks", "items"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(cqlite_core::storage::write_engine::ClusteringKey::single(
            "ck",
            Value::Integer(0),
        )),
        vec![CellOperation::Write {
            column: column.to_string(),
            value,
        }],
        ts,
        None,
    )
}

/// Write one SSTable containing a single row (id=1, ck=0) whose `score` cell was
/// written at ts=100 and whose `name` cell was written at ts=300 — two distinct
/// per-cell writetimes in one row. Returns the discovered input Data.db paths.
fn build_mixed_timestamp_input(temp: &TempDir) -> Vec<PathBuf> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let data_dir = temp.path().join("inputs");
    let wal_dir = temp.path().join("wal");
    let schema = schema_with_drops(HashMap::new());

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    // Pre-drop cell of the column that will be dropped.
    engine
        .write(write_one_cell("score", Value::Integer(42), 100))
        .expect("write score @100");
    // Post-drop cell of a different column in the SAME row.
    engine
        .write(write_one_cell(
            "name",
            Value::Text("alice".to_string()),
            300,
        ))
        .expect("write name @300");
    rt.block_on(engine.flush()).expect("flush").expect("info");
    rt.block_on(engine.close()).expect("close");

    discover_inputs(&data_dir)
}

fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p
                    .file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db") || n.ends_with("Data.db"))
                {
                    out.push(p);
                }
            }
        }
    }
    walk(dir, &mut out);
    out
}

fn compact(inputs: Vec<PathBuf>, out_dir: &Path, schema: &TableSchema) -> PathBuf {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let report = rt
        .block_on(compact_sstables(
            inputs, out_dir, schema, 922, None, None, true,
        ))
        .expect("compaction must succeed");
    report.output.data_path
}

/// Read every surviving (column, value) cell out of `data_path`, using a schema
/// with NO drop map so the reader does not re-apply any filter — this reflects
/// what was physically written by compaction.
fn surviving_columns(data_path: PathBuf) -> Vec<(String, Value)> {
    let read_schema = schema_with_drops(HashMap::new());
    let mut merger = KWayMerger::new(vec![data_path], &read_schema).expect("merger over output");
    let mut cells = Vec::new();
    loop {
        match merger.step().expect("step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for row in rows {
                    if let RowData::Live { cells: row_cells } = row.row_data {
                        for c in row_cells {
                            cells.push((c.column, c.value));
                        }
                    }
                }
            }
        }
    }
    cells
}

/// AC (#922): in a row mixing a pre-drop cell of the dropped column with a
/// post-drop cell of another column, the dropped cell is purged by its OWN
/// writetime while the other column survives by ITS writetime.
#[test]
fn pre_drop_cell_purged_while_post_drop_sibling_survives() {
    let temp = TempDir::new().expect("tempdir");
    let inputs = build_mixed_timestamp_input(&temp);
    assert!(!inputs.is_empty(), "expected at least one input SSTable");

    // Drop `score` at T=150: BEFORE score's cell ts=100 would be filtered, but
    // AFTER name's cell ts=300. The row's newest cell (name@300) means the OLD
    // row-timestamp behavior would assign ts=300 to `score` too and wrongly keep
    // it. Per-cell writetimes purge score (100 <= 150) and keep name (300 > 150).
    let mut dropped = HashMap::new();
    dropped.insert("score".to_string(), 150_i64);
    let drop_schema = schema_with_drops(dropped);

    let out_dir = temp.path().join("out");
    let data_path = compact(inputs, &out_dir, &drop_schema);

    let cols = surviving_columns(data_path);

    assert!(
        cols.iter().all(|(c, _)| c != "score"),
        "the dropped `score` cell (own ts=100 <= drop_time=150) must be purged \
         even though a sibling cell in the same row was written at ts=300; \
         got surviving columns: {:?}",
        cols.iter().map(|(c, _)| c).collect::<Vec<_>>()
    );
    assert!(
        cols.iter()
            .any(|(c, v)| c == "name" && matches!(v, Value::Text(t) if t == "alice")),
        "the post-drop `name` cell (own ts=300 > drop_time=150) must survive; \
         got: {:?}",
        cols
    );
}

/// Mirror case: drop `score` at T=50, BEFORE score's own ts=100. Score is
/// re-added (post-drop write) so it survives by its own writetime, and the
/// per-cell granularity must not purge it just because name was written later.
#[test]
fn pre_drop_then_readded_cell_survives_by_own_writetime() {
    let temp = TempDir::new().expect("tempdir");
    let inputs = build_mixed_timestamp_input(&temp);

    let mut dropped = HashMap::new();
    dropped.insert("score".to_string(), 50_i64); // score ts=100 > 50 → survives
    let drop_schema = schema_with_drops(dropped);

    let out_dir = temp.path().join("out");
    let data_path = compact(inputs, &out_dir, &drop_schema);

    let cols = surviving_columns(data_path);
    assert!(
        cols.iter()
            .any(|(c, v)| c == "score" && matches!(v, Value::Integer(42))),
        "score (own ts=100 > drop_time=50) was re-added and must survive: {:?}",
        cols
    );
    assert!(
        cols.iter().any(|(c, _)| c == "name"),
        "name must survive: {:?}",
        cols.iter().map(|(c, _)| c).collect::<Vec<_>>()
    );
}
