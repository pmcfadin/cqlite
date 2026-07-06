//! Issue #1723 (P0): a `set<int>` column in the schema must NOT perturb regular
//! (scalar) column decode for rows that carry NO collection cells.
//!
//! Surfaced out-of-scope during #1384: a clustered table with scalar columns
//! plus a `set<int>` column, where a row writes ONLY the scalars (the collection
//! is absent), read back a scalar incorrectly.
//!
//! ## Root cause
//!
//! When a row omits the complex (non-frozen collection) column, `HAS_ALL_COLUMNS`
//! is NOT set, so the row header carries a **missing-columns bitmap** and the row
//! body carries no complex framing for the absent column. The reader resolves
//! each on-disk cell to a column POSITIONALLY, by the on-disk SerializationHeader
//! column order, and it interprets the missing-columns bitmap against that same
//! order (`row_data.rs`, `is_present(idx)`). The `Statistics.db` header writer,
//! however, used to sort regular/static columns purely ALPHABETICALLY while the
//! `Data.db` writer emitted cells (and computed the missing-columns bitmap) in
//! Cassandra's `ColumnMetadata.comparisonOrder` (SIMPLE columns before COMPLEX,
//! then by name — `column_order_key`). Whenever a scalar's name sorted AFTER the
//! `set<int>` column's, the header order and the bitmap order DISAGREED, so the
//! reader mapped the "collection missing" bit onto a SCALAR column instead — it
//! then tried to parse the complex column from the scalar's bytes and desynced
//! the row cursor, dropping/mis-decoding the scalar.
//!
//! The header-order/data-order alignment landed as the writer fix in **#2035**
//! (`serialization_header.rs`: sort by `column_order_key`, not by name). That fix
//! was validated only for a row with the collection PRESENT; #1723 is the DISTINCT
//! collection-ABSENT scenario (the missing-columns bitmap path), which this suite
//! pins. Reverting #2035's header sort turns every assertion below RED with the
//! wrong-value evidence (a scalar reads back absent/None), so these are genuine
//! regression guards, not vacuous round-trips.
//!
//! Why a cqlite-write → cqlite-read self-consistency check is sufficient here:
//! the defect is an INTERNAL invariant between CQLite's own SerializationHeader
//! column order / missing-columns bitmap and its Data.db cell order — both
//! produced by this same writer, and both now matching Cassandra's
//! `comparisonOrder`. Reading back the exact written scalar values directly
//! exercises the broken invariant on BOTH read decoders: the `CompactionRow`
//! path (KWayMerger) and the `ScanRow` path (`SSTableReader::scan`, the query
//! engine's decoder). Cassandra-golden byte parity of the SerializationHeader is
//! covered separately by the compaction-parity gate.
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features write-support \
//!     --test issue_1723_set_int_scalar_decode

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::write_engine::merge::{KWayMerger, MergeStep, RowData};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::Config;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

const KS: &str = "s1723_ks";
const TBL: &str = "s1723_tbl";

/// Schema: `pk int` partition key, `ck int` clustering key, a `set<int>`
/// collection `tags`, and two scalars `v int` / `w int`. Column names are chosen
/// so a NAME-ONLY (pre-#2035) header sort would place the complex `tags` FIRST
/// (`tags` < `v` < `w`), disagreeing with the data/bitmap `column_order_key`
/// order (`v`, `w`, `tags`) — the exact ordering that reproduced the #1723
/// missing-columns-bitmap desync.
fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
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
            col("pk", "int", false),
            col("ck", "int", false),
            col("tags", "set<int>", true),
            col("v", "int", true),
            col("w", "int", true),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn col(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable,
        default: None,
        is_static: false,
    }
}

fn data_files(dir: &std::path::Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with("-Data.db"))
                .unwrap_or(false)
        })
        .collect()
}

#[derive(Debug, Clone)]
struct CellSnapshot {
    /// Clustering-key value of the owning row (single `int` ck here).
    ck: Option<Value>,
    column: String,
    value: Value,
    cell_path: Option<Vec<u8>>,
    is_complex_element: bool,
}

/// A flushed single-generation fixture whose `TempDir` outlives both read paths.
struct Fixture {
    _temp: TempDir,
    data_dir: PathBuf,
    schema: TableSchema,
}

impl Fixture {
    fn data_path(&self) -> PathBuf {
        let sstable_dir = self.data_dir.join(KS).join(TBL);
        let paths = data_files(&sstable_dir);
        assert_eq!(paths.len(), 1, "expected exactly one flushed generation");
        paths.into_iter().next().unwrap()
    }
}

/// Read back every live cell of the single flushed generation through
/// `KWayMerger`, tagging each cell with its row's clustering key.
fn read_back_cells(data_dir: &std::path::Path, schema: &TableSchema) -> Vec<CellSnapshot> {
    let sstable_dir = data_dir.join(KS).join(TBL);
    let paths = data_files(&sstable_dir);
    assert_eq!(paths.len(), 1, "expected exactly one flushed generation");

    let mut merger = KWayMerger::new(paths, schema).expect("KWayMerger open");
    let mut all_cells = Vec::new();
    while let MergeStep::Partition { rows, .. } = merger.step().expect("merge step") {
        for entry in rows {
            let ck = entry
                .clustering_key
                .as_ref()
                .and_then(|k| k.columns.first())
                .map(|(_, v)| v.clone());
            if let RowData::Live { cells } = &entry.row_data {
                for c in cells {
                    all_cells.push(CellSnapshot {
                        ck: ck.clone(),
                        column: c.column.clone(),
                        value: c.value.clone(),
                        cell_path: c.cell_path.clone(),
                        is_complex_element: c.is_complex_element,
                    });
                }
            }
        }
    }
    all_cells
}

/// Fetch the scalar (non-complex-element) value of `column` for the row whose
/// clustering key is `ck_int`.
fn scalar_in(cells: &[CellSnapshot], ck_int: i32, column: &str) -> Option<Value> {
    cells
        .iter()
        .find(|c| {
            c.ck == Some(Value::Integer(ck_int)) && c.column == column && !c.is_complex_element
        })
        .map(|c| c.value.clone())
}

/// Write the given rows (one partition `pk=1`, varying clustering keys) through
/// a fresh WriteEngine and flush once, returning the retained fixture.
fn write_flush(rows: Vec<(i32, Vec<CellOperation>)>) -> Fixture {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // Pinned timestamp — no wall-clock races.
    let ts = 1_700_000_000_000_000i64;
    for (ck_int, ops) in rows {
        let pk = PartitionKey::single("pk", Value::Integer(1));
        let ck = Some(ClusteringKey::single("ck", Value::Integer(ck_int)));
        engine
            .write(Mutation::new(TableId::new(KS, TBL), pk, ck, ops, ts, None))
            .expect("write");
    }
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("generation produced");
    rt.block_on(engine.close()).expect("close engine");

    Fixture {
        _temp: temp_dir,
        data_dir,
        schema,
    }
}

/// KWayMerger (compaction_row) read path.
fn write_flush_read(rows: Vec<(i32, Vec<CellOperation>)>) -> Vec<CellSnapshot> {
    let fx = write_flush(rows);
    read_back_cells(&fx.data_dir, &fx.schema)
}

/// One scan row projected for assertions: its clustering-key int and the
/// remaining named cell values (`ck` stripped out).
type ScanRowProjection = (Option<i32>, Vec<(String, Value)>);

/// SCAN read path (`SSTableReader::scan` → `ScanRow`), the distinct decoder the
/// query engine uses. Each `ScanRow::Row` for our single partition carries a
/// `ck` cell (the clustering-key column surfaces as a cell) plus the data
/// columns; returns each row's `(ck, [(column, value)])`.
fn scan_rows(fx: &Fixture) -> Vec<ScanRowProjection> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let data_path = fx.data_path();
    rt.block_on(async {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let reader = SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("open reader");
        let table_id = cqlite_core::types::TableId::from(format!("{KS}.{TBL}").as_str());
        let rows = reader
            .scan(&table_id, None, None, None, Some(&fx.schema))
            .await
            .expect("scan");
        rows.into_iter()
            .filter_map(|(_k, sr)| sr.into_cells())
            .map(|cells| {
                let mut ck = None;
                let mut out = Vec::new();
                for (name, value) in cells {
                    if name.as_ref() == "ck" {
                        if let Value::Integer(i) = value {
                            ck = Some(i);
                        }
                    } else {
                        out.push((name.to_string(), value));
                    }
                }
                (ck, out)
            })
            .collect()
    })
}

/// Fetch a scalar column's value for the scan row whose clustering key is
/// `ck_int` (excludes multi-cell/collection representations).
fn scan_scalar(rows: &[ScanRowProjection], ck_int: i32, column: &str) -> Option<Value> {
    rows.iter()
        .find(|(ck, _)| *ck == Some(ck_int))
        .and_then(|(_, cells)| {
            cells
                .iter()
                .find(|(n, _)| n == column)
                .map(|(_, v)| v.clone())
        })
}

fn write_scalar(column: &str, v: i32) -> CellOperation {
    CellOperation::Write {
        column: column.to_string(),
        value: Value::Integer(v),
    }
}

fn write_tags(members: &[i32]) -> CellOperation {
    CellOperation::Write {
        column: "tags".to_string(),
        value: Value::Set(members.iter().map(|m| Value::Integer(*m)).collect()),
    }
}

/// PRIMARY REPRO: a clustered row with ONLY the scalar columns set (the
/// `set<int>` `tags` collection is ABSENT). Both scalars must decode to their
/// exact written values. Before the fix, the missing complex column desynced the
/// row cursor and the scalar read back wrong.
#[test]
fn scalar_only_row_with_absent_set_int_decodes_correctly() {
    let cells = write_flush_read(vec![(0, vec![write_scalar("v", 42), write_scalar("w", 7)])]);

    assert_eq!(
        scalar_in(&cells, 0, "v"),
        Some(Value::Integer(42)),
        "v desynced by absent set<int> column; cells={cells:#?}"
    );
    assert_eq!(
        scalar_in(&cells, 0, "w"),
        Some(Value::Integer(7)),
        "w desynced by absent set<int> column; cells={cells:#?}"
    );
    // The absent collection must NOT surface any element cells.
    assert!(
        !cells.iter().any(|c| c.column == "tags"),
        "absent set<int> must not read back any cells; cells={cells:#?}"
    );
}

/// BROADENING: multiple rows in one partition — scalar-only, collection+scalars,
/// collection+one-scalar, and collection-only — must ALL decode correctly.
#[test]
fn mixed_rows_with_and_without_set_int_decode_correctly() {
    let cells = write_flush_read(vec![
        // ck=0: scalar-only (collection absent)
        (0, vec![write_scalar("v", 42), write_scalar("w", 7)]),
        // ck=1: collection present + both scalars
        (
            1,
            vec![
                write_tags(&[100, 200]),
                write_scalar("v", 11),
                write_scalar("w", 22),
            ],
        ),
        // ck=2: collection present + only ONE scalar (v absent)
        (2, vec![write_tags(&[300]), write_scalar("w", 33)]),
        // ck=3: collection-only (both scalars absent)
        (3, vec![write_tags(&[400, 500])]),
    ]);

    // ck=0: scalar-only
    assert_eq!(scalar_in(&cells, 0, "v"), Some(Value::Integer(42)));
    assert_eq!(scalar_in(&cells, 0, "w"), Some(Value::Integer(7)));

    // ck=1: collection present + scalars
    assert_eq!(scalar_in(&cells, 1, "v"), Some(Value::Integer(11)));
    assert_eq!(scalar_in(&cells, 1, "w"), Some(Value::Integer(22)));
    let c1_tags: Vec<&CellSnapshot> = cells
        .iter()
        .filter(|c| c.ck == Some(Value::Integer(1)) && c.column == "tags")
        .collect();
    assert_eq!(c1_tags.len(), 2, "ck=1 set<int> must have 2 members");
    assert!(c1_tags
        .iter()
        .any(|c| c.cell_path.as_deref() == Some(&100i32.to_be_bytes())));
    assert!(c1_tags
        .iter()
        .any(|c| c.cell_path.as_deref() == Some(&200i32.to_be_bytes())));

    // ck=2: collection + only w (v absent)
    assert_eq!(scalar_in(&cells, 2, "v"), None, "v was not written");
    assert_eq!(scalar_in(&cells, 2, "w"), Some(Value::Integer(33)));
    assert!(cells.iter().any(|c| c.ck == Some(Value::Integer(2))
        && c.column == "tags"
        && c.cell_path.as_deref() == Some(&300i32.to_be_bytes())));

    // ck=3: collection-only
    assert_eq!(scalar_in(&cells, 3, "v"), None);
    assert_eq!(scalar_in(&cells, 3, "w"), None);
    let c3_tags: Vec<&CellSnapshot> = cells
        .iter()
        .filter(|c| c.ck == Some(Value::Integer(3)) && c.column == "tags")
        .collect();
    assert_eq!(c3_tags.len(), 2, "ck=3 set<int> must have 2 members");
}

/// PRIMARY REPRO on the SCAN read path (`SSTableReader::scan` → `ScanRow`), the
/// decoder the query engine uses (distinct from KWayMerger's `CompactionRow`).
/// A clustered row with ONLY scalars set (the `set<int>` `tags` ABSENT) must
/// decode both scalars to their exact written values.
#[test]
fn scalar_only_row_with_absent_set_int_decodes_correctly_on_scan() {
    let fx = write_flush(vec![(0, vec![write_scalar("v", 42), write_scalar("w", 7)])]);
    let rows = scan_rows(&fx);

    assert_eq!(
        scan_scalar(&rows, 0, "v"),
        Some(Value::Integer(42)),
        "v desynced by absent set<int> column on scan; rows={rows:#?}"
    );
    assert_eq!(
        scan_scalar(&rows, 0, "w"),
        Some(Value::Integer(7)),
        "w desynced by absent set<int> column on scan; rows={rows:#?}"
    );
}

/// BROADENING on the SCAN path: scalar-only, collection+scalars,
/// collection+one-scalar rows must all decode their scalars correctly.
#[test]
fn mixed_rows_with_and_without_set_int_decode_correctly_on_scan() {
    let fx = write_flush(vec![
        (0, vec![write_scalar("v", 42), write_scalar("w", 7)]),
        (
            1,
            vec![
                write_tags(&[100, 200]),
                write_scalar("v", 11),
                write_scalar("w", 22),
            ],
        ),
        (2, vec![write_tags(&[300]), write_scalar("w", 33)]),
    ]);
    let rows = scan_rows(&fx);

    assert_eq!(scan_scalar(&rows, 0, "v"), Some(Value::Integer(42)));
    assert_eq!(scan_scalar(&rows, 0, "w"), Some(Value::Integer(7)));
    assert_eq!(scan_scalar(&rows, 1, "v"), Some(Value::Integer(11)));
    assert_eq!(scan_scalar(&rows, 1, "w"), Some(Value::Integer(22)));
    assert_eq!(scan_scalar(&rows, 2, "w"), Some(Value::Integer(33)));
}
