//! Issue #921 finding 2 — background compaction must strip a fully-purged
//! dropped column from the output SerializationHeader.
//!
//! `WriteEngine::compact_sstables` already runs a dropped-column survivor
//! pre-pass (`compute_surviving_dropped_columns`) and writes with
//! `effective_schema.for_compaction_output(&retained_dropped)`, so a dropped
//! column whose last cell is purged is removed from the output header. The
//! BACKGROUND compaction path (`maintenance_step` → `start_merge`) previously
//! initialized the `SSTableWriter` with `effective_schema` DIRECTLY, so it could
//! purge the last cell of a dropped column while still emitting that column in
//! the output header — misaligning a post-drop reader.
//!
//! This test drives a FULL background compaction (the merge policy selects every
//! candidate SSTable, so `purge_safe == true`) over an input whose only cells for
//! the dropped column `name` are all older than the drop time. After compaction,
//! reading the output with a POST-DROP schema (which omits `name`) must NOT see
//! `name`, and the surviving `score` column — which sorts AFTER `name` in the
//! serialization-header order — must still decode as an integer (proving the
//! header did not carry the dropped column and misalign `score`).

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cqlite_core::error::Result;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{MergeStep, RowData};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, KWayMerger, MergePolicy, Mutation, PartitionKey, TableId,
    WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

/// Merge policy that selects EVERY candidate (a full/major compaction). With the
/// selected set equal to the candidate set, `maintenance_step` marks the merge
/// `purge_safe == true` — the precondition for gc/drop purging.
#[derive(Debug)]
struct SelectAllPolicy;

impl MergePolicy for SelectAllPolicy {
    fn select_merge(&self, candidates: &[PathBuf]) -> Result<Vec<PathBuf>> {
        Ok(candidates.to_vec())
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

/// Schema: keyspace=drop_ks, table=items, PK=id(int), CK=ck(int),
/// columns name(text), score(int). `dropped` injects per-column drop times.
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

/// Schema describing the table AFTER `name` was dropped: it omits `name`
/// entirely (the natural post-drop schema a reader would use). `name` sorts
/// before `score` in the alphabetical serialization-header order, so reading the
/// compacted output with this schema proves the dropped column is absent from
/// the output header and `score` is not misparsed.
fn post_drop_schema_without_name() -> TableSchema {
    let mut s = schema_with_drops(HashMap::new());
    s.columns.retain(|c| c.name != "name");
    s
}

fn write_row(id: i32, ck: i32, name: &str, score: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("drop_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
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
                    .is_some_and(|n| n.ends_with("Data.db"))
                {
                    out.push(p);
                }
            }
        }
    }
    walk(dir, &mut out);
    out
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

/// A FULL background compaction (`maintenance_step` → `start_merge`, purge_safe)
/// that purges the last cell of a dropped column must NOT emit that column in the
/// output header.
#[test]
fn background_compaction_strips_fully_purged_dropped_column_from_header() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");

    // Drop `name` at T=150; all cells are written at ts=100 (<= drop time) so
    // every `name` cell is purged by the drop-time filter during compaction.
    let mut dropped = HashMap::new();
    dropped.insert("name".to_string(), 150_i64);
    let drop_schema = schema_with_drops(dropped);

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, drop_schema);
    let mut engine = WriteEngine::new(config).expect("engine");

    // Write two flushed SSTables so the compaction has multiple inputs; the
    // SelectAllPolicy then makes the merge a full (purge_safe) compaction.
    for ck in 0_i32..=1 {
        engine
            .write(write_row(1, ck, &format!("name-{ck}"), ck * 10, 100))
            .expect("write row");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(engine.flush()).expect("flush").expect("info");

    for ck in 2_i32..=3 {
        engine
            .write(write_row(1, ck, &format!("name-{ck}"), ck * 10, 100))
            .expect("write row");
    }
    rt.block_on(engine.flush()).expect("flush").expect("info");

    // Sanity: more than one input SSTable so the merge is a real compaction.
    let inputs_before = discover_inputs(&data_dir);
    assert!(
        inputs_before.len() >= 2,
        "expected >= 2 input SSTables, got {}",
        inputs_before.len()
    );

    // Drive a FULL background compaction: SelectAllPolicy selects every candidate
    // so maintenance_step computes purge_safe == true.
    engine
        .set_merge_policy(Box::new(SelectAllPolicy))
        .expect("set policy");

    // Run maintenance to completion (the merge may span several budgeted steps).
    let mut guard = 0;
    loop {
        let report = engine
            .maintenance_step(Duration::from_millis(500))
            .expect("maintenance step");
        if !report.pending_compaction {
            break;
        }
        guard += 1;
        assert!(guard < 1000, "maintenance did not converge");
    }
    rt.block_on(engine.close()).expect("close");

    // Find the compacted output (the newest generation Data.db). After a full
    // compaction the inputs are deleted, so any remaining Data.db is the output.
    let outputs = discover_inputs(&data_dir);
    assert_eq!(
        outputs.len(),
        1,
        "full compaction should leave exactly one output SSTable, got {:?}",
        outputs
    );
    let data_path = outputs.into_iter().next().expect("one output");

    // Read the output with a POST-DROP schema that omits `name` entirely.
    let cols = surviving_columns_with(data_path, &post_drop_schema_without_name());

    assert!(
        cols.iter().all(|(c, _)| c != "name"),
        "the dropped column must not appear in the background-compaction output, got: {:?}",
        cols.iter().map(|(c, _)| c).collect::<Vec<_>>()
    );
    // `score` must still decode correctly (as an integer) — proving the output
    // header did not carry the purged `name` column and misalign `score`.
    assert!(
        cols.iter()
            .any(|(c, v)| c == "score" && matches!(v, Value::Integer(_))),
        "surviving `score` must parse correctly under a post-drop reader schema, got: {:?}",
        cols
    );
}
