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

/// `pk int, ck int, stat_a text static, stat_b text static, row_col text` — a
/// clustering table with TWO static columns and one regular column. Mirrors
/// [`schema`] so tests that need a second static column don't have to mutate the
/// shared single-static schema other tests depend on.
fn schema_two_statics() -> TableSchema {
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
                name: "stat_a".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "stat_b".to_string(),
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

/// HIGH — a static cell is decided by its OWN write timestamp under LWW, NOT by
/// any clustering row's timestamp. The fix routes static reconciliation through
/// the writer's schema-driven `collect_static_operations`, which keys LWW on the
/// static cell's own mutation timestamp. Here the two generations carry
/// conflicting `stat_col` writes at DISTINCT static timestamps, and each
/// generation's clustering-row timestamp is deliberately interleaved/inverted
/// relative to the static timestamps — so a clustering-row-driven tiebreak would
/// pick the WRONG static value.
#[tokio::test]
async fn static_cell_keeps_own_write_timestamp_under_lww() {
    let schema = schema();

    // gen-1: static stat_col=OLD at a LOW static ts, but its clustering row is
    // the NEWEST clustering row in the test. If statics were (wrongly) decided
    // by the clustering row's timestamp, this OLD static would win.
    let gen1 = vec![
        Mutation::new(
            TableId::new("test_ks", "static_write"),
            PartitionKey::single("pk", Value::Integer(1)),
            None,
            vec![CellOperation::Write {
                column: "stat_col".to_string(),
                value: Value::Text("OLD".to_string()),
            }],
            1_000_000, // static ts: LOW
            None,
        ),
        Mutation::new(
            TableId::new("test_ks", "static_write"),
            PartitionKey::single("pk", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(1))),
            vec![CellOperation::Write {
                column: "row_col".to_string(),
                value: Value::Text("V1".to_string()),
            }],
            9_000_000, // clustering ts: HIGHEST in the test
            None,
        ),
    ];
    let (_dir_a, path_a) = write_sstable(&schema, 1, gen1).await;

    // gen-2: static stat_col=NEW at a HIGH static ts (must win), but its
    // clustering row is the OLDEST clustering row. A clustering-row tiebreak
    // would pick gen-1's OLD static — the bug this test guards against.
    let gen2 = vec![
        Mutation::new(
            TableId::new("test_ks", "static_write"),
            PartitionKey::single("pk", Value::Integer(1)),
            None,
            vec![CellOperation::Write {
                column: "stat_col".to_string(),
                value: Value::Text("NEW".to_string()),
            }],
            5_000_000, // static ts: HIGHER than gen-1's static (1_000_000)
            None,
        ),
        Mutation::new(
            TableId::new("test_ks", "static_write"),
            PartitionKey::single("pk", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(2))),
            vec![CellOperation::Write {
                column: "row_col".to_string(),
                value: Value::Text("V2".to_string()),
            }],
            2_000_000, // clustering ts: LOWER than gen-1's clustering row
            None,
        ),
    ];
    let (_dir_b, path_b) = write_sstable(&schema, 2, gen2).await;

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

    let rows = collect_merge_rows(vec![report.output.data_path], &schema);

    let static_rows: Vec<&DecodedRow> = rows
        .iter()
        .filter(|r| r.ck.is_none() && !r.is_tombstone && cell(r, "stat_col").is_some())
        .collect();
    assert_eq!(
        static_rows.len(),
        1,
        "#1074: expected exactly ONE partition static row after compaction, got rows: {:?}",
        rows.iter()
            .map(|r| (r.ck.clone(), r.cells.clone(), r.is_tombstone))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        cell(static_rows[0], "stat_col"),
        Some("NEW"),
        "#1074: the surviving static value must be decided by the static cell's OWN timestamp \
         (gen-2 static ts=5_000_000 > gen-1 static ts=1_000_000), NOT by any clustering row's \
         timestamp (gen-1's clustering row is the newest at 9_000_000). Got rows: {:?}",
        rows.iter()
            .map(|r| (r.ck.clone(), r.cells.clone(), r.is_tombstone))
            .collect::<Vec<_>>()
    );

    // Both clustering rows survive and neither carries the static cell.
    for ck in ["1", "2"] {
        let row = rows
            .iter()
            .find(|r| r.ck.as_deref() == Some(ck) && !r.is_tombstone)
            .unwrap_or_else(|| panic!("#1074: live ck={ck} clustering row must be present"));
        assert!(
            cell(row, "stat_col").is_none(),
            "#1074: the static cell must NOT be folded into the ck={ck} clustering row"
        );
    }
}

/// HIGH — a static cell tombstone (`Delete` on a static column) is honored under
/// LWW. First: a newer `Delete{stat_col}` removes an older `Write stat_col`.
/// Then the reverse: a `Write stat_col` newer than the `Delete` resurrects it.
#[tokio::test]
async fn static_cell_tombstone_deletes_and_resurrects_under_lww() {
    let schema = schema();

    // --- Direction 1: newer Delete removes an older static Write. ---
    let write_static = Mutation::new(
        TableId::new("test_ks", "static_write"),
        PartitionKey::single("pk", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "stat_col".to_string(),
            value: Value::Text("S".to_string()),
        }],
        1_000_000,
        None,
    );
    let (_dir_a, path_a) = write_sstable(&schema, 1, vec![write_static]).await;

    let delete_static = Mutation::new(
        TableId::new("test_ks", "static_write"),
        PartitionKey::single("pk", Value::Integer(1)),
        None,
        vec![CellOperation::Delete {
            column: "stat_col".to_string(),
            local_deletion_time: None,
        }],
        2_000_000, // newer than the static Write
        None,
    );
    let (_dir_b, path_b) = write_sstable(&schema, 2, vec![delete_static]).await;

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
    let compacted_path = report.output.data_path.clone();
    let rows = collect_merge_rows(vec![report.output.data_path], &schema);

    // `collect_merge_rows` filters Value::Tombstone, so a deleted static cell
    // simply has no LIVE stat_col on any clustering=None static row. Assert the
    // absence directly rather than trusting only the live-cell filter.
    let live_static = rows
        .iter()
        .any(|r| !r.is_tombstone && cell(r, "stat_col") == Some("S"));
    assert!(
        !live_static,
        "#1074: a newer Delete on stat_col MUST remove the static cell — no live stat_col=S \
         should survive compaction. Got rows: {:?}",
        rows.iter()
            .map(|r| (r.ck.clone(), r.cells.clone(), r.is_tombstone))
            .collect::<Vec<_>>()
    );

    // --- Direction 2: a Write newer than the Delete resurrects the static. ---
    let resurrect = Mutation::new(
        TableId::new("test_ks", "static_write"),
        PartitionKey::single("pk", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "stat_col".to_string(),
            value: Value::Text("R".to_string()),
        }],
        3_000_000, // newer than the Delete (2_000_000)
        None,
    );
    let (_dir_c, path_c) = write_sstable(&schema, 4, vec![resurrect]).await;

    let out_dir2 = tempfile::TempDir::new().expect("out dir 2");
    let report2 = compact_sstables(
        vec![compacted_path, path_c],
        out_dir2.path(),
        &schema,
        5,
        None,
        None,
        true,
    )
    .await
    .expect("compaction must succeed");
    let rows2 = collect_merge_rows(vec![report2.output.data_path], &schema);

    let static_rows: Vec<&DecodedRow> = rows2
        .iter()
        .filter(|r| r.ck.is_none() && !r.is_tombstone && cell(r, "stat_col").is_some())
        .collect();
    assert_eq!(
        static_rows.len(),
        1,
        "#1074: the resurrected static row must reappear exactly once, got rows: {:?}",
        rows2
            .iter()
            .map(|r| (r.ck.clone(), r.cells.clone(), r.is_tombstone))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        cell(static_rows[0], "stat_col"),
        Some("R"),
        "#1074: a static Write (ts=3_000_000) newer than the Delete (ts=2_000_000) must \
         resurrect the static cell with the new value R. Got rows: {:?}",
        rows2
            .iter()
            .map(|r| (r.ck.clone(), r.cells.clone(), r.is_tombstone))
            .collect::<Vec<_>>()
    );
}

/// MEDIUM — multiple static columns reconcile independently. Two static columns
/// are written in one mutation; a later generation updates only one of them.
/// Both must land in the clustering=None static row with their correct winning
/// values, and neither must appear in any clustering row.
#[tokio::test]
async fn multiple_static_columns_reconcile_independently() {
    let schema = schema_two_statics();

    // gen-1: both statics written together, plus a clustering row.
    let gen1 = vec![
        Mutation::new(
            TableId::new("test_ks", "static_write"),
            PartitionKey::single("pk", Value::Integer(1)),
            None,
            vec![
                CellOperation::Write {
                    column: "stat_a".to_string(),
                    value: Value::Text("A1".to_string()),
                },
                CellOperation::Write {
                    column: "stat_b".to_string(),
                    value: Value::Text("B1".to_string()),
                },
            ],
            1_000_000,
            None,
        ),
        Mutation::new(
            TableId::new("test_ks", "static_write"),
            PartitionKey::single("pk", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(1))),
            vec![CellOperation::Write {
                column: "row_col".to_string(),
                value: Value::Text("V".to_string()),
            }],
            1_000_000,
            None,
        ),
    ];
    let (_dir_a, path_a) = write_sstable(&schema, 1, gen1).await;

    // gen-2: update only stat_a at a newer timestamp; stat_b unchanged.
    let gen2 = vec![Mutation::new(
        TableId::new("test_ks", "static_write"),
        PartitionKey::single("pk", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "stat_a".to_string(),
            value: Value::Text("A2".to_string()),
        }],
        2_000_000,
        None,
    )];
    let (_dir_b, path_b) = write_sstable(&schema, 2, gen2).await;

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
    let rows = collect_merge_rows(vec![report.output.data_path], &schema);

    // Exactly one static row carrying BOTH static columns with the winning values.
    let static_rows: Vec<&DecodedRow> = rows
        .iter()
        .filter(|r| r.ck.is_none() && !r.is_tombstone)
        .filter(|r| cell(r, "stat_a").is_some() || cell(r, "stat_b").is_some())
        .collect();
    assert_eq!(
        static_rows.len(),
        1,
        "#1074: both static columns must land in ONE clustering=None static row, got rows: {:?}",
        rows.iter()
            .map(|r| (r.ck.clone(), r.cells.clone(), r.is_tombstone))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        cell(static_rows[0], "stat_a"),
        Some("A2"),
        "#1074: stat_a must take the newer value A2 (ts=2_000_000 > 1_000_000)"
    );
    assert_eq!(
        cell(static_rows[0], "stat_b"),
        Some("B1"),
        "#1074: stat_b must keep B1 (only stat_a was updated in gen-2)"
    );

    // No clustering row may carry either static column.
    for row in rows.iter().filter(|r| r.ck.is_some() && !r.is_tombstone) {
        assert!(
            cell(row, "stat_a").is_none() && cell(row, "stat_b").is_none(),
            "#1074: static columns must NOT be folded into clustering row ck={:?}",
            row.ck
        );
    }
}

/// MEDIUM — multiple clustering rows alongside a static. The partition has ck=1
/// and ck=2 regular rows plus a static cell. NEITHER clustering row may carry
/// the static cell, and there must be exactly ONE clustering=None static row
/// carrying it. Guards against the old "fold the static into EVERY clustering
/// row" regression that a single-clustering-row test cannot fully catch.
#[tokio::test]
async fn static_not_folded_into_any_of_multiple_clustering_rows() {
    let schema = schema();

    let mutations = vec![
        Mutation::new(
            TableId::new("test_ks", "static_write"),
            PartitionKey::single("pk", Value::Integer(1)),
            None,
            vec![CellOperation::Write {
                column: "stat_col".to_string(),
                value: Value::Text("S".to_string()),
            }],
            1_000_000,
            None,
        ),
        Mutation::new(
            TableId::new("test_ks", "static_write"),
            PartitionKey::single("pk", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(1))),
            vec![CellOperation::Write {
                column: "row_col".to_string(),
                value: Value::Text("V1".to_string()),
            }],
            1_000_000,
            None,
        ),
        Mutation::new(
            TableId::new("test_ks", "static_write"),
            PartitionKey::single("pk", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(2))),
            vec![CellOperation::Write {
                column: "row_col".to_string(),
                value: Value::Text("V2".to_string()),
            }],
            1_000_000,
            None,
        ),
    ];
    let (_dir, data_path) = write_sstable(&schema, 1, mutations).await;
    let rows = collect_merge_rows(vec![data_path], &schema);

    // Exactly one static row (clustering=None) carrying stat_col.
    let static_rows: Vec<&DecodedRow> = rows
        .iter()
        .filter(|r| r.ck.is_none() && !r.is_tombstone && cell(r, "stat_col").is_some())
        .collect();
    assert_eq!(
        static_rows.len(),
        1,
        "#1074: expected exactly ONE clustering=None static row carrying stat_col, got rows: {:?}",
        rows.iter()
            .map(|r| (r.ck.clone(), r.cells.clone(), r.is_tombstone))
            .collect::<Vec<_>>()
    );

    // Both clustering rows present, each with its own row_col, NEITHER with stat_col.
    for (ck, expected) in [("1", "V1"), ("2", "V2")] {
        let row = rows
            .iter()
            .find(|r| r.ck.as_deref() == Some(ck) && !r.is_tombstone)
            .unwrap_or_else(|| panic!("#1074: live ck={ck} clustering row must be present"));
        assert_eq!(
            cell(row, "row_col"),
            Some(expected),
            "#1074: ck={ck} clustering row must carry row_col={expected}"
        );
        assert!(
            cell(row, "stat_col").is_none(),
            "#1074: the static cell must NOT be folded into clustering row ck={ck} \
             (the old bug folded it into EVERY clustering row)"
        );
    }
}
