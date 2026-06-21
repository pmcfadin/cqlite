//! Issue #847 — filter dropped-column cells during compaction (end-to-end).
//!
//! Cassandra `cb34ad47` (`compaction.purge`): a column dropped at `drop_time`
//! discards every cell whose `timestamp <= drop_time`. The drop time is supplied
//! through `TableSchema::dropped_columns` (plumbed in #904) and applied in the
//! merge reconcile loop (`KWayMerger::reconcile_cluster`, #847).
//!
//! This test exercises the FULL wiring: write an SSTable holding `score` cells,
//! compact it with a schema that drops `score` at a time at/after those cells'
//! timestamp, then **read the compacted output back with a schema that has NO
//! drop map** — proving the cells were physically removed by the writer, not just
//! hidden by a second read-time filter.

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

fn write_row(id: i32, ck: i32, name: &str, score: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("drop_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(cqlite_core::storage::write_engine::ClusteringKey::single(
            "ck",
            Value::Integer(ck),
        )),
        vec![
            CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text(name.to_string()),
            },
            CellOperation::Write {
                column: "score".to_string(),
                value: Value::Integer(score),
            },
        ],
        ts,
        None,
    )
}

/// Write one SSTable at `ts` with rows (id=1 ck=0..=2). Returns its input paths.
fn build_input(temp: &TempDir, ts: i64) -> (Vec<PathBuf>, TableSchema) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let data_dir = temp.path().join("inputs");
    let wal_dir = temp.path().join("wal");
    let schema = schema_with_drops(HashMap::new());

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");
    for ck in 0_i32..=2 {
        engine
            .write(write_row(1, ck, &format!("name-{ck}"), ck * 10, ts))
            .expect("write row");
    }
    rt.block_on(engine.flush()).expect("flush").expect("info");
    rt.block_on(engine.close()).expect("close");

    (discover_inputs(&data_dir), schema)
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
        .block_on(compact_sstables(inputs, out_dir, schema, 901, None, None))
        .expect("compaction must succeed");
    report.output.data_path
}

/// Read every surviving (column, value) cell out of `data_path`, using a schema
/// with NO drop map so the reader does not re-apply the filter. This reflects what
/// was physically written by compaction.
fn surviving_columns(data_path: PathBuf) -> Vec<(String, Value)> {
    surviving_columns_with(data_path, &schema_with_drops(HashMap::new()))
}

/// Schema describing the table AFTER `name` was dropped: it omits `name`
/// entirely (the natural post-drop schema a reader would use). `name` sorts
/// before `score` in the alphabetical serialization-header order, so reading the
/// compacted output with this schema proves the dropped column is absent from
/// the output header/bitmap and `score` is not misparsed.
fn post_drop_schema_without_name() -> TableSchema {
    let mut s = schema_with_drops(HashMap::new());
    s.columns.retain(|c| c.name != "name");
    s
}

/// Read surviving (column, value) cells out of `data_path` using `read_schema`.
fn surviving_columns_with(data_path: PathBuf, read_schema: &TableSchema) -> Vec<(String, Value)> {
    let mut merger = KWayMerger::new(vec![data_path], read_schema).expect("merger over output");
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

/// AC: a column dropped at T discards cells with ts <= T from the compacted output.
#[test]
fn dropped_column_cells_discarded_from_compaction_output() {
    let temp = TempDir::new().expect("tempdir");
    let (inputs, _) = build_input(&temp, 100); // score cells written at ts=100
    assert!(!inputs.is_empty(), "expected at least one input SSTable");

    // Drop `score` at T=150 (>= the cells' ts=100 → must be filtered).
    let mut dropped = HashMap::new();
    dropped.insert("score".to_string(), 150_i64);
    let drop_schema = schema_with_drops(dropped);

    let out_dir = temp.path().join("out");
    let data_path = compact(inputs, &out_dir, &drop_schema);

    let cols = surviving_columns(data_path);
    assert!(
        !cols.is_empty(),
        "name cells must still be present after the drop"
    );
    assert!(
        cols.iter().all(|(c, _)| c != "score"),
        "every `score` cell (ts=100 <= drop_time=150) must be physically removed, \
         got surviving columns: {:?}",
        cols.iter().map(|(c, _)| c).collect::<Vec<_>>()
    );
    assert!(
        cols.iter().any(|(c, _)| c == "name"),
        "the non-dropped `name` column must survive"
    );
}

/// Output-header cleanliness (roborev #847 review): when a dropped column sorts
/// BEFORE a surviving column in the serialization-header order, the compacted
/// output must not encode the dropped column in its header/bitmap — otherwise a
/// post-drop reader schema (omitting the dropped column) misaligns and misparses
/// the surviving column. Here `name` (dropped) sorts before `score` (surviving).
#[test]
fn dropped_column_before_surviving_is_absent_from_output_header() {
    let temp = TempDir::new().expect("tempdir");
    let (inputs, _) = build_input(&temp, 100); // name + score cells at ts=100

    // Drop `name` (sorts before `score`) at T=150 (>= the cells' ts=100).
    let mut dropped = HashMap::new();
    dropped.insert("name".to_string(), 150_i64);
    let drop_schema = schema_with_drops(dropped);

    let out_dir = temp.path().join("out");
    let data_path = compact(inputs, &out_dir, &drop_schema);

    // Read the output with a POST-DROP schema that omits `name` entirely.
    let cols = surviving_columns_with(data_path, &post_drop_schema_without_name());

    assert!(
        cols.iter().all(|(c, _)| c != "name"),
        "the dropped column must not appear in the output, got: {:?}",
        cols.iter().map(|(c, _)| c).collect::<Vec<_>>()
    );
    // `score` must still decode correctly (as an integer) — proving the header
    // did not carry the dropped `name` column and misalign `score`.
    assert!(
        cols.iter()
            .any(|(c, v)| c == "score" && matches!(v, Value::Integer(_))),
        "surviving `score` must parse correctly under a post-drop reader schema, got: {:?}",
        cols
    );
}

/// Decode-contract guard (roborev High, #904/#847): a dropped column that is
/// ABSENT from `columns` cannot be decoded by the schema-driven reader, so its
/// cells would never reach the filter and surrounding columns could misalign.
/// Compaction must reject this configuration with a clear error rather than
/// silently mis-decoding.
#[test]
fn dropped_column_absent_from_columns_is_rejected() {
    let temp = TempDir::new().expect("tempdir");
    let (inputs, _) = build_input(&temp, 100);

    // Build a schema that drops `score` but OMITS it from `columns` entirely.
    let mut schema = schema_with_drops({
        let mut m = HashMap::new();
        m.insert("score".to_string(), 150_i64);
        m
    });
    schema.columns.retain(|c| c.name != "score");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let out_dir = temp.path().join("out");
    let result = rt.block_on(compact_sstables(inputs, &out_dir, &schema, 902, None, None));

    let err = result.expect_err("compaction must reject a dropped column absent from `columns`");
    let msg = err.to_string();
    assert!(
        msg.contains("score") && msg.contains("columns"),
        "error must name the offending column and the contract, got: {msg}"
    );
}

/// A cell written AFTER `drop_time` (ts > drop_time) SURVIVES compaction — the
/// column was re-added — and the output retains that column so the surviving
/// cell has a matching serialization-header entry (roborev #847 review). Only
/// fully-purged dropped columns are stripped from the output header.
#[test]
fn dropped_column_cell_after_drop_time_survives_in_output() {
    let temp = TempDir::new().expect("tempdir");
    let (inputs, _) = build_input(&temp, 300); // score cells written at ts=300

    // Drop `score` at T=150, BEFORE the cells' ts=300 (re-added after the drop).
    let mut dropped = HashMap::new();
    dropped.insert("score".to_string(), 150_i64);
    let drop_schema = schema_with_drops(dropped);

    let out_dir = temp.path().join("out");
    let data_path = compact(inputs, &out_dir, &drop_schema);

    let cols = surviving_columns(data_path);
    assert!(
        cols.iter()
            .any(|(c, v)| c == "score" && matches!(v, Value::Integer(_))),
        "a re-added cell (ts > drop_time) must survive in the output, got: {:?}",
        cols
    );
    assert!(
        cols.iter().any(|(c, _)| c == "name"),
        "non-dropped `name` must survive"
    );
}

/// Control: with no drop configured, `score` cells survive compaction unchanged.
#[test]
fn no_drop_keeps_all_columns() {
    let temp = TempDir::new().expect("tempdir");
    let (inputs, schema) = build_input(&temp, 100);

    let out_dir = temp.path().join("out");
    let data_path = compact(inputs, &out_dir, &schema); // schema has empty drop map

    let cols = surviving_columns(data_path);
    assert!(
        cols.iter().any(|(c, _)| c == "score"),
        "without a drop entry, score must survive: {:?}",
        cols.iter().map(|(c, _)| c).collect::<Vec<_>>()
    );
}
