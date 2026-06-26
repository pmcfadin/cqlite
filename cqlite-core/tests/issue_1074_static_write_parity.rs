//! Issue #1074 — a STATIC column written alongside a regular column at a
//! clustering key must land in the PARTITION static row, not inside the
//! clustering row.
//!
//! Discovered during epic #972 by the #1015 parity suite. The compaction read
//! path historically FOLDED the partition static cells into every clustering
//! row (`ck=1, cells=[ck, s, v]`) instead of surfacing the static row as its own
//! partition-level row (Cassandra's `Row.staticRow`). Two real consequences:
//!
//!  1. **Data loss on row delete** — a row tombstone covering the clustering row
//!     also shadowed the folded static cell, dropping a live static value.
//!  2. **Static-only partition dropped** — a partition with only a static row
//!     (no clustering rows) emitted nothing on the compaction path.
//!
//! These tests are fully SYNTHETIC (no datasets required) — they write through
//! the real `SSTableWriter` and read back through the compaction merge
//! (`KWayMerger` / `compact_sstables`), exactly the path the #1015 suite had to
//! route around the real fixture to avoid.

#![cfg(feature = "write-support")]

use std::collections::HashMap;

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{compact_sstables, KWayMerger, MergeStep, RowData};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;

/// `pk int, ck int, stat_col text static, row_col text` — a clustering table
/// with one static and one regular column.
fn schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "static_write".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
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
                name: "stat_col".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "row_col".to_string(),
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

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Text(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

/// Write `mutations` into a fresh single-generation SSTable and return its
/// `Data.db` path (the temp dir is returned too so it outlives the read).
async fn write_sstable(
    schema: &TableSchema,
    generation: u64,
    mutations: Vec<Mutation>,
) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let key = mutations
        .first()
        .expect("at least one mutation")
        .decorated_key(schema)
        .expect("decorated key");
    let mut writer =
        cqlite_core::storage::sstable::writer::SSTableWriter::with_expected_partitions(
            dir.path().to_path_buf(),
            generation,
            schema,
            1,
        )
        .expect("writer");
    writer
        .write_partition(key, mutations)
        .expect("write_partition");
    let info = writer.finish().await.expect("finish");
    (dir, info.data_path)
}

/// One decoded row from the compaction merge.
struct DecodedRow {
    ck: Option<String>,
    /// Non-tombstone live cells as (column, value-string).
    cells: Vec<(String, String)>,
    is_tombstone: bool,
}

/// Drive `KWayMerger` over `data_paths` and collect every emitted row.
fn collect_merge_rows(
    data_paths: Vec<std::path::PathBuf>,
    schema: &TableSchema,
) -> Vec<DecodedRow> {
    let mut merger = KWayMerger::new(data_paths, schema).expect("merger");
    let mut out = Vec::new();
    loop {
        match merger.step().expect("merge step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for row in rows {
                    let ck = row
                        .clustering_key
                        .as_ref()
                        .and_then(|c| c.columns.first().map(|(_, v)| value_to_string(v)));
                    match &row.row_data {
                        RowData::Live { cells } => {
                            let cells = cells
                                .iter()
                                .filter(|c| !matches!(c.value, Value::Tombstone(_)))
                                .map(|c| (c.column.clone(), value_to_string(&c.value)))
                                .collect();
                            out.push(DecodedRow {
                                ck,
                                cells,
                                is_tombstone: false,
                            });
                        }
                        RowData::Tombstone { .. } => out.push(DecodedRow {
                            ck,
                            cells: Vec::new(),
                            is_tombstone: true,
                        }),
                    }
                }
            }
        }
    }
    out
}

fn cell<'a>(row: &'a DecodedRow, name: &str) -> Option<&'a str> {
    row.cells
        .iter()
        .find(|(c, _)| c == name)
        .map(|(_, v)| v.as_str())
}

/// The exact bug from the issue: a single mutation writing a static column plus
/// a regular column at `ck=1` must surface the static cell in the PARTITION
/// static row (clustering = None), NOT folded into the `ck=1` clustering row.
#[tokio::test]
async fn static_cell_lands_in_partition_static_row_not_clustering_row() {
    let schema = schema();
    let mutation = Mutation::new(
        TableId::new("test_ks", "static_write"),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![
            CellOperation::Write {
                column: "stat_col".to_string(),
                value: Value::Text("S".to_string()),
            },
            CellOperation::Write {
                column: "row_col".to_string(),
                value: Value::Text("V".to_string()),
            },
        ],
        1_000_000,
        None,
    );

    let (_dir, data_path) = write_sstable(&schema, 1, vec![mutation]).await;
    let rows = collect_merge_rows(vec![data_path], &schema);

    // The partition static row: clustering = None, carries stat_col, NOT row_col.
    let static_rows: Vec<&DecodedRow> = rows
        .iter()
        .filter(|r| r.ck.is_none() && !r.is_tombstone && cell(r, "stat_col").is_some())
        .collect();
    assert_eq!(
        static_rows.len(),
        1,
        "#1074: expected exactly ONE partition static row (clustering=None) carrying stat_col, \
         got rows: {:?}",
        rows.iter()
            .map(|r| (r.ck.clone(), r.cells.clone(), r.is_tombstone))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        cell(static_rows[0], "stat_col"),
        Some("S"),
        "#1074: the partition static row must carry stat_col=S"
    );
    assert!(
        cell(static_rows[0], "row_col").is_none(),
        "#1074: the partition static row must NOT carry the regular row_col cell"
    );

    // The ck=1 clustering row carries row_col and must NOT carry the static cell.
    let ck1 = rows
        .iter()
        .find(|r| r.ck.as_deref() == Some("1") && !r.is_tombstone)
        .expect("#1074: a live ck=1 clustering row must be present");
    assert_eq!(
        cell(ck1, "row_col"),
        Some("V"),
        "#1074: the ck=1 clustering row must carry row_col=V"
    );
    assert!(
        cell(ck1, "stat_col").is_none(),
        "#1074: the static cell must NOT be folded into the ck=1 clustering row \
         (the merger previously emitted ck=1, cells=[ck, s, v])"
    );
}

/// The load-bearing consequence (#1074 summary): a row tombstone covering the
/// clustering row must NOT shadow the partition static cell. Compact a base
/// SSTable (static + regular at ck=1) against a newer one that row-deletes ck=1;
/// the static cell must survive.
#[tokio::test]
async fn row_delete_does_not_shadow_partition_static_cell() {
    let schema = schema();

    // gen-1: static stat_col=S + regular row_col=V at ck=1 (older).
    let base = Mutation::new(
        TableId::new("test_ks", "static_write"),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![
            CellOperation::Write {
                column: "stat_col".to_string(),
                value: Value::Text("S".to_string()),
            },
            CellOperation::Write {
                column: "row_col".to_string(),
                value: Value::Text("V".to_string()),
            },
        ],
        1_000_000,
        None,
    );
    let (_dir_a, path_a) = write_sstable(&schema, 1, vec![base]).await;

    // gen-2: row-delete ck=1 at a STRICTLY NEWER timestamp.
    let row_delete = Mutation::new(
        TableId::new("test_ks", "static_write"),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![CellOperation::DeleteRow],
        2_000_000,
        None,
    );
    let (_dir_b, path_b) = write_sstable(&schema, 2, vec![row_delete]).await;

    // Full (overlap-safe) compaction of both generations.
    let out_dir = tempfile::TempDir::new().expect("out dir");
    let report = compact_sstables(
        vec![path_a, path_b],
        out_dir.path(),
        &schema,
        3,
        None,
        None,
        true,
    )
    .await
    .expect("compaction must succeed");

    // Read the compacted output: the static cell must survive the row delete.
    let rows = collect_merge_rows(vec![report.output.data_path], &schema);

    let static_survives = rows
        .iter()
        .any(|r| !r.is_tombstone && cell(r, "stat_col") == Some("S"));
    assert!(
        static_survives,
        "#1074: the partition static cell (stat_col=S) MUST survive a row delete of its \
         clustering row — it was wrongly shadowed when folded into the clustering row. \
         Got rows: {:?}",
        rows.iter()
            .map(|r| (r.ck.clone(), r.cells.clone(), r.is_tombstone))
            .collect::<Vec<_>>()
    );

    // The deleted ck=1 row must NOT resurrect a live row_col.
    let ck1_live_rowcol = rows
        .iter()
        .any(|r| r.ck.as_deref() == Some("1") && cell(r, "row_col").is_some());
    assert!(
        !ck1_live_rowcol,
        "#1074: row-deleted ck=1 must not resurrect a live row_col after compaction"
    );
}

/// A partition with ONLY a static row (no clustering rows) must not be dropped
/// by compaction — the folding path emitted nothing for it.
#[tokio::test]
async fn static_only_partition_survives_compaction() {
    let schema = schema();

    // A pure static write (no clustering key): only the partition static row.
    let static_only = Mutation::new(
        TableId::new("test_ks", "static_write"),
        PartitionKey::single("pk", Value::Integer(7)),
        None,
        vec![CellOperation::Write {
            column: "stat_col".to_string(),
            value: Value::Text("ONLY".to_string()),
        }],
        1_000_000,
        None,
    );
    let (_dir, path) = write_sstable(&schema, 1, vec![static_only]).await;

    // Compact the single input and read it back.
    let out_dir = tempfile::TempDir::new().expect("out dir");
    let report = compact_sstables(vec![path], out_dir.path(), &schema, 2, None, None, true)
        .await
        .expect("compaction must succeed");

    let rows = collect_merge_rows(vec![report.output.data_path], &schema);
    let found = rows
        .iter()
        .any(|r| r.ck.is_none() && cell(r, "stat_col") == Some("ONLY"));
    assert!(
        found,
        "#1074: a static-only partition must survive compaction (its static row was dropped \
         when the reader folded statics into clustering rows). Got rows: {:?}",
        rows.iter()
            .map(|r| (r.ck.clone(), r.cells.clone(), r.is_tombstone))
            .collect::<Vec<_>>()
    );
}
