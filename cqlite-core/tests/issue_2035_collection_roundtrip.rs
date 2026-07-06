//! Issue #2035: a freshly-written NON-FROZEN collection must round-trip through
//! the WriteEngine → SSTable → KWayMerger merge reader.
//!
//! Before the fix, flushing a whole-column non-frozen `map`/`set` (or a lone
//! per-element complex write) plus a trailing simple column silently DROPPED the
//! collection column on read-back and MIS-DECODED the immediately-following
//! simple cell (a complex-column framing miscount desynced the row byte cursor:
//! `score=7` read back as `-8388609`, a 3-byte misread).
//!
//! This test writes the collection + trailing `score int` via the public
//! WriteEngine, flushes to a single generation, then reads it back through
//! `KWayMerger` and asserts (a) every collection element is present at the
//! correct `cell_path` with the correct value and (b) the trailing simple column
//! decodes to its exact written value (catching the byte desync).
//!
//! Why a cqlite-write → cqlite-read self-consistency check is the sufficient
//! property here (no Cassandra golden fixture needed): the defect was an INTERNAL
//! desync between CQLite's own SerializationHeader column order and its Data.db
//! cell order — both produced by this same writer. A round-trip that recovers the
//! collection `cell_path`s and the trailing simple `score == 7` canary directly
//! exercises the exact header-order-vs-cell-order invariant that was broken.
//! Cassandra-golden BYTE-parity of the SerializationHeader is covered separately
//! by the compaction-byte-parity gate component.
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features write-support \
//!     --test issue_2035_collection_roundtrip

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{KWayMerger, MergeStep, RowData};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;

const KS: &str = "coll_ks";
const TBL: &str = "coll_tbl";

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
                name: "attrs".to_string(),
                data_type: "map<text, int>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "labels".to_string(),
                data_type: "set<int>".to_string(),
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

/// Collect the flushed `-big-Data.db` (or `-da-Data.db`) generation paths.
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

/// Read back the single flushed generation through `KWayMerger` and return the
/// live cells of the first (only) row.
fn read_back_cells(data_dir: &std::path::Path, schema: &TableSchema) -> Vec<CellSnapshot> {
    let sstable_dir = data_dir.join(KS).join(TBL);
    let paths = data_files(&sstable_dir);
    assert_eq!(paths.len(), 1, "expected exactly one flushed generation");

    let mut merger = KWayMerger::new(paths, schema).expect("KWayMerger open");
    let mut all_cells = Vec::new();
    while let MergeStep::Partition { rows, .. } = merger.step().expect("merge step") {
        for entry in rows {
            if let RowData::Live { cells } = &entry.row_data {
                for c in cells {
                    all_cells.push(CellSnapshot {
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

#[derive(Debug, Clone)]
struct CellSnapshot {
    column: String,
    value: Value,
    cell_path: Option<Vec<u8>>,
    is_complex_element: bool,
}

#[test]
fn nonfrozen_collection_plus_trailing_simple_column_roundtrips() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    // ── Write one row: whole-column map + whole-column set + trailing int ──────
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    let pk = PartitionKey::single("id", Value::Integer(1));
    // Pinned timestamp — no wall-clock.
    let ts = 1_700_000_000_000_000i64;
    let ops = vec![
        CellOperation::Write {
            column: "attrs".to_string(),
            value: Value::Map(vec![
                (Value::Text("a".to_string()), Value::Integer(10)),
                (Value::Text("b".to_string()), Value::Integer(20)),
            ]),
        },
        CellOperation::Write {
            column: "labels".to_string(),
            value: Value::Set(vec![Value::Integer(100), Value::Integer(200)]),
        },
        CellOperation::Write {
            column: "score".to_string(),
            value: Value::Integer(7),
        },
    ];
    engine
        .write(Mutation::new(
            TableId::new(KS, TBL),
            pk,
            None,
            ops,
            ts,
            None,
        ))
        .expect("write");
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("generation produced");
    rt.block_on(engine.close()).expect("close engine");

    // ── Read back through the merge reader ────────────────────────────────────
    let cells = read_back_cells(&data_dir, &schema);

    // (b) Trailing simple column MUST decode to its exact written value. This is
    // the byte-desync canary: before the fix `score` read back as -8388609.
    let score = cells
        .iter()
        .find(|c| c.column == "score" && !c.is_complex_element)
        .unwrap_or_else(|| panic!("score column missing on read-back; cells={cells:#?}"));
    assert_eq!(
        score.value,
        Value::Integer(7),
        "trailing simple column desynced (byte miscount in complex framing); cells={cells:#?}"
    );

    // (a) Every map element present at the correct cell_path with correct value.
    // cell_path for map<text,int> = serialized key ("a"/"b" as UTF-8 bytes).
    let attr_elems: Vec<&CellSnapshot> = cells
        .iter()
        .filter(|c| c.column == "attrs" && c.is_complex_element)
        .collect();
    assert_eq!(
        attr_elems.len(),
        2,
        "map must read back as 2 per-element complex cells; cells={cells:#?}"
    );
    let attr_a = attr_elems
        .iter()
        .find(|c| c.cell_path.as_deref() == Some(b"a"))
        .expect("map element a missing");
    assert_eq!(attr_a.value, Value::Integer(10));
    let attr_b = attr_elems
        .iter()
        .find(|c| c.cell_path.as_deref() == Some(b"b"))
        .expect("map element b missing");
    assert_eq!(attr_b.value, Value::Integer(20));

    // set<int> elements: cell_path = serialized member (4-byte BE int), value empty.
    let label_elems: Vec<&CellSnapshot> = cells
        .iter()
        .filter(|c| c.column == "labels" && c.is_complex_element)
        .collect();
    assert_eq!(
        label_elems.len(),
        2,
        "set must read back as 2 per-element complex cells; cells={cells:#?}"
    );
    let has_path = |bytes: &[u8]| {
        label_elems
            .iter()
            .any(|c| c.cell_path.as_deref() == Some(bytes))
    };
    assert!(
        has_path(&100i32.to_be_bytes()),
        "set member 100 missing; cells={cells:#?}"
    );
    assert!(
        has_path(&200i32.to_be_bytes()),
        "set member 200 missing; cells={cells:#?}"
    );

    drop(temp_dir);
}
