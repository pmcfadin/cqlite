//! Issue #844 (Epic #842): per-cell-path merge of multi-cell collections / UDTs.
//!
//! END-TO-END evidence at the public-API level. Two SSTables are written through
//! the [`WriteEngine`], each updating DISJOINT elements of the SAME non-frozen
//! collection column on the SAME `(pk, ck)`. They are then compacted via the
//! public [`compact_sstables`] and the compacted output is read back through the
//! [`KWayMerger`] read model (the same path `canonical_tuples_from_sstables` uses
//! in the differential harness).
//!
//! Before #844 the whole-collection cell with the higher timestamp won and the
//! other SSTable's element was silently dropped (data loss). After #844 the
//! disjoint elements must BOTH survive; a shared map key takes the higher-ts
//! cell's value.
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features write-support \
//!     --test issue_844_multicell_collection_merge

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{compact_sstables, MergeStep, RowData};
use cqlite_core::storage::write_engine::{
    CellOperation, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const KS: &str = "mc844_ks";
const TBL: &str = "mc844";

/// Single-partition (no clustering) table with a non-frozen set and map column.
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
                name: "tags".to_string(),
                // Non-frozen set → multi-cell.
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "attrs".to_string(),
                // Non-frozen map → multi-cell.
                data_type: "map<text, text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

fn write_tags_and_attrs(set_elems: &[&str], map_pairs: &[(&str, &str)], ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            CellOperation::Write {
                column: "tags".to_string(),
                value: Value::Set(
                    set_elems
                        .iter()
                        .map(|e| Value::Text(e.to_string()))
                        .collect(),
                ),
            },
            CellOperation::Write {
                column: "attrs".to_string(),
                value: Value::Map(
                    map_pairs
                        .iter()
                        .map(|(k, v)| (Value::Text(k.to_string()), Value::Text(v.to_string())))
                        .collect(),
                ),
            },
        ],
        ts,
        None,
    )
}

fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    collect(dir, &mut found, 8);
    // Newest generation first (run index 0 = newest).
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

fn collect(dir: &Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
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

/// Read back the merged row for partition id=1 through the merge read model and
/// return its surviving cells.
fn read_back_cells(data_path: &Path, schema: &TableSchema) -> Vec<(String, Value)> {
    let mut merger =
        KWayMerger::new(vec![data_path.to_path_buf()], schema).expect("KWayMerger::new");
    let mut out = Vec::new();
    loop {
        match merger.step().expect("merger step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for row in rows {
                    if let RowData::Live { cells } = row.row_data {
                        for c in cells {
                            out.push((c.column, c.value));
                        }
                    }
                }
            }
        }
    }
    out
}

/// Build two SSTables updating disjoint elements of the same collection columns
/// on the same partition, in the given timestamp order. Returns inputs
/// newest-first plus the temp dir kept alive by the caller.
fn build_two_inputs(
    first_set: &[&str],
    first_map: &[(&str, &str)],
    first_ts: i64,
    second_set: &[&str],
    second_map: &[(&str, &str)],
    second_ts: i64,
) -> (TempDir, Vec<PathBuf>, TableSchema) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("inputs");
    let wal_dir = temp.path().join("wal");
    let schema = make_schema();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");

    engine
        .write(write_tags_and_attrs(first_set, first_map, first_ts))
        .expect("write A");
    rt.block_on(engine.flush())
        .expect("flush A")
        .expect("info A");

    engine
        .write(write_tags_and_attrs(second_set, second_map, second_ts))
        .expect("write B");
    rt.block_on(engine.flush())
        .expect("flush B")
        .expect("info B");

    rt.block_on(engine.close()).expect("close engine");

    let inputs = discover_inputs(&data_dir);
    assert!(
        inputs.len() >= 2,
        "expected >=2 input SSTables, got {}",
        inputs.len()
    );
    (temp, inputs, schema)
}

/// Extract the unioned set element strings (sorted) from read-back cells.
fn set_elems(cells: &[(String, Value)]) -> Vec<String> {
    let mut v: Vec<String> = cells
        .iter()
        .find(|(c, _)| c == "tags")
        .and_then(|(_, val)| match val {
            Value::Set(items) | Value::List(items) => Some(
                items
                    .iter()
                    .filter_map(|e| match e {
                        Value::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// Extract map (key, value) pairs (sorted by key) from read-back cells.
fn map_pairs(cells: &[(String, Value)]) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = cells
        .iter()
        .find(|(c, _)| c == "attrs")
        .and_then(|(_, val)| match val {
            Value::Map(pairs) => Some(
                pairs
                    .iter()
                    .filter_map(|(k, val)| match (k, val) {
                        (Value::Text(k), Value::Text(v)) => Some((k.clone(), v.clone())),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// AC: disjoint set elements and map keys from two SSTables both survive
/// compaction; a shared map key takes the higher-timestamp cell's value.
/// Asserted with the NEWER SSTable written second (typical order).
#[test]
fn disjoint_elements_survive_newer_second() {
    let (temp, inputs, schema) = build_two_inputs(
        &["a"],
        &[("k1", "v1"), ("shared", "old")],
        100,
        &["b"],
        &[("k2", "v2"), ("shared", "new")],
        200,
    );
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&out_dir).expect("mkdir out");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let report = rt
        .block_on(compact_sstables(inputs, &out_dir, &schema, 100, None, None))
        .expect("compaction");

    let cells = read_back_cells(&report.output.data_path, &schema);
    assert_eq!(
        set_elems(&cells),
        vec!["a".to_string(), "b".to_string()],
        "disjoint set elements both survive compaction (#844)"
    );
    assert_eq!(
        map_pairs(&cells),
        vec![
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string()),
            ("shared".to_string(), "new".to_string()),
        ],
        "disjoint map keys survive; shared key takes higher-ts value (#844)"
    );
}

/// Same assertion with the NEWER SSTable written FIRST (reverse source order),
/// proving the merge is order-insensitive.
#[test]
fn disjoint_elements_survive_newer_first() {
    let (temp, inputs, schema) = build_two_inputs(
        &["b"],
        &[("k2", "v2"), ("shared", "new")],
        200,
        &["a"],
        &[("k1", "v1"), ("shared", "old")],
        100,
    );
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&out_dir).expect("mkdir out");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let report = rt
        .block_on(compact_sstables(inputs, &out_dir, &schema, 100, None, None))
        .expect("compaction");

    let cells = read_back_cells(&report.output.data_path, &schema);
    assert_eq!(
        set_elems(&cells),
        vec!["a".to_string(), "b".to_string()],
        "disjoint set elements both survive regardless of source order (#844)"
    );
    assert_eq!(
        map_pairs(&cells),
        vec![
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string()),
            ("shared".to_string(), "new".to_string()),
        ],
        "shared key still takes higher-ts value regardless of source order (#844)"
    );
}
