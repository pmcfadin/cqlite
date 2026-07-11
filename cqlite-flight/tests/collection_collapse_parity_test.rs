//! Issue #2324: the Flight producer's `entry_to_row` must reassemble a non-frozen
//! collection's per-element cells (list/set/map) into the WHOLE collection value,
//! not collapse it to the last cell.
//!
//! The k-way merger emits every element of a non-frozen collection as its own
//! cell, all sharing the column name. `build_row_from_scan` keys the row's cells
//! by name into a `HashMap`, so before the fix every collection column returned
//! only its LAST element over `do_get` — silent partial data. This is identical
//! on the scan and point paths (both drive `drive_merge` → `entry_to_row`), which
//! is exactly why the dual-path parity oracle stayed green while the OUTPUT was
//! wrong on both.
//!
//! Oracle: the sstabledump JSONL golden for `test_collections.collection_table`.
//! Partition `8d08e3a4-…` carries a `SET<INT>` (9 members), a `SET<TEXT>` (4), a
//! `LIST<INT>` (3, order-significant), a `MAP<TEXT,TEXT>` (8 entries) and a
//! `MAP<TEXT,BIGINT>` (5 entries) — every one multi-element, so a last-cell-wins
//! collapse is unmissable. Both the full scan and a PK-targeted point read are
//! asserted to return the FULL collection contents.
//!
//! Skips (never fails) when `CQLITE_DATASETS_ROOT` is unset or the `Data.db`
//! binary is absent (a worktree without `fetch-datasets.sh`), but asserts the
//! target row IS found whenever it runs — never a silent 0-row false pass.

use std::collections::HashMap;
use std::path::PathBuf;

use arrow::array::{
    Array, FixedSizeBinaryArray, Int32Array, Int64Array, ListArray, MapArray, StringArray,
};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;

use cqlite_core::query::{SSTableFilterOp, SSTablePredicate};
use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::types::Value;
use cqlite_flight::cancel::CancelFlag;
use cqlite_flight::filter::{FilterExpr, ScanSpec};
use cqlite_flight::producer::{DirSource, MergeProducer, SstableSource};

/// Raw 16 bytes of a hyphenated UUID string.
fn uuid_bytes(s: &str) -> [u8; 16] {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    assert_eq!(hex.len(), 32, "not a 16-byte UUID: {s:?}");
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).expect("hex");
    }
    out
}

fn col(name: &str, ty: &str) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

fn collection_table_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_collections".into(),
        table: "collection_table".into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "uuid".into(),
            position: 0,
        }],
        clustering_keys: Vec::<ClusteringColumn>::new(),
        columns: vec![
            col("id", "uuid"),
            col("tags", "set<text>"),
            col("scores", "list<int>"),
            col("properties", "map<text, text>"),
            col("numbers_set", "set<int>"),
            col("ordered_values", "list<timestamp>"),
            col("metadata_map", "map<text, bigint>"),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Index of the row whose `id` FixedSizeBinary(16) equals `target`.
fn find_row(batch: &RecordBatch, target: &[u8; 16]) -> usize {
    let idx = batch.schema().index_of("id").expect("id column present");
    let ids = batch
        .column(idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("id is FixedSizeBinary(16)");
    (0..batch.num_rows())
        .find(|&r| ids.value(r) == target.as_slice())
        .unwrap_or_else(|| {
            panic!(
                "target partition row not found in {} rows",
                batch.num_rows()
            )
        })
}

/// The `List<Int32>` value at `row` of column `name`, in on-disk order.
fn list_i32(batch: &RecordBatch, name: &str, row: usize) -> Vec<i32> {
    let idx = batch.schema().index_of(name).expect("column present");
    let list = batch
        .column(idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap_or_else(|| panic!("{name} is not a List array"));
    let vals = list.value(row);
    let ints = vals
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("list items are Int32");
    (0..ints.len()).map(|i| ints.value(i)).collect()
}

/// The `List<Utf8>` value at `row` of column `name`.
fn list_text(batch: &RecordBatch, name: &str, row: usize) -> Vec<String> {
    let idx = batch.schema().index_of(name).expect("column present");
    let list = batch
        .column(idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap_or_else(|| panic!("{name} is not a List array"));
    let vals = list.value(row);
    let strs = vals
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("list items are Utf8");
    (0..strs.len()).map(|i| strs.value(i).to_string()).collect()
}

/// The `Map<Utf8,Utf8>` entries at `row` of column `name`.
fn map_text(batch: &RecordBatch, name: &str, row: usize) -> Vec<(String, String)> {
    let idx = batch.schema().index_of(name).expect("column present");
    let map = batch
        .column(idx)
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap_or_else(|| panic!("{name} is not a Map array"));
    let entries = map.value(row);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("map keys Utf8");
    let vals = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("map values Utf8");
    (0..keys.len())
        .map(|i| (keys.value(i).to_string(), vals.value(i).to_string()))
        .collect()
}

/// The `Map<Utf8,Int64>` entries at `row` of column `name`.
fn map_bigint(batch: &RecordBatch, name: &str, row: usize) -> Vec<(String, i64)> {
    let idx = batch.schema().index_of(name).expect("column present");
    let map = batch
        .column(idx)
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap_or_else(|| panic!("{name} is not a Map array"));
    let entries = map.value(row);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("map keys Utf8");
    let vals = entries
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("map values Int64");
    (0..keys.len())
        .map(|i| (keys.value(i).to_string(), vals.value(i)))
        .collect()
}

/// Assert the full collection contents of the target partition in `combined`.
fn assert_full_collections(combined: &RecordBatch, target: &[u8; 16], path: &str) {
    let row = find_row(combined, target);

    // SET<INT> — 9 members (order not significant for a set).
    let mut numbers = list_i32(combined, "numbers_set", row);
    numbers.sort_unstable();
    assert_eq!(
        numbers,
        vec![148, 461, 473, 794, 831, 857, 881, 976, 998],
        "{path}: numbers_set SET<INT> must reassemble ALL 9 members, not last-cell-wins"
    );

    // LIST<INT> — 3 members, on-disk ORDER is significant.
    assert_eq!(
        list_i32(combined, "scores", row),
        vec![53, 45, 17],
        "{path}: scores LIST<INT> must preserve all 3 members in order"
    );

    // SET<TEXT> — 4 members.
    let mut tags = list_text(combined, "tags", row);
    tags.sort();
    assert_eq!(
        tags,
        vec![
            "out".to_string(),
            "reflect".to_string(),
            "street".to_string(),
            "win".to_string()
        ],
        "{path}: tags SET<TEXT> must reassemble all 4 members"
    );

    // MAP<TEXT,TEXT> — 8 entries (map key decoded from the element cell_path).
    let mut props = map_text(combined, "properties", row);
    props.sort();
    assert_eq!(
        props,
        vec![
            ("clearly".into(), "population".into()),
            ("close".into(), "time".into()),
            ("condition".into(), "plan".into()),
            ("former".into(), "stand".into()),
            ("leg".into(), "meeting".into()),
            ("probably".into(), "must".into()),
            ("suggest".into(), "nation".into()),
            ("word".into(), "help".into()),
        ],
        "{path}: properties MAP<TEXT,TEXT> must reassemble all 8 entries"
    );

    // MAP<TEXT,BIGINT> — 5 entries.
    let mut md = map_bigint(combined, "metadata_map", row);
    md.sort();
    assert_eq!(
        md,
        vec![
            ("along".into(), 264406),
            ("court".into(), 646569),
            ("his".into(), 43965),
            ("professor".into(), 992825),
            ("stop".into(), 818561),
        ],
        "{path}: metadata_map MAP<TEXT,BIGINT> must reassemble all 5 entries"
    );
}

fn table_dir() -> Option<PathBuf> {
    let root = std::env::var_os("CQLITE_DATASETS_ROOT")?;
    let dir = PathBuf::from(&root)
        .join("sstables")
        .join("test_collections")
        .join("collection_table-6b8c8fb0a25111f0a3fef1a551383fb9");
    if dir.join("nb-1-big-Data.db").is_file() {
        Some(dir)
    } else {
        None
    }
}

// The multi-element target partition (from the JSONL golden).
const TARGET_UUID: &str = "8d08e3a4-a8b1-4697-8957-82bbded1e343";

#[test]
fn scan_path_reassembles_full_collections() {
    let Some(dir) = table_dir() else {
        eprintln!("collection_table Data.db absent — skipping (run fetch-datasets.sh)");
        return;
    };
    let schema = collection_table_schema();
    let spec = ScanSpec {
        token: None,
        filter: None,
        projection: None,
        limit: None,
    };
    let producer = MergeProducer::with_spec(schema, 64, spec).unwrap();
    let batches = producer
        .produce_from_paths(DirSource::new(&dir).data_paths().unwrap())
        .unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(rows > 0, "scan must return rows (never a 0-row false pass)");

    let arrow_schema = producer.arrow_schema().unwrap();
    let combined = concat_batches(&arrow_schema.into(), &batches).unwrap();
    assert_full_collections(&combined, &uuid_bytes(TARGET_UUID), "scan");
}

#[test]
fn point_path_reassembles_full_collections() {
    let Some(dir) = table_dir() else {
        eprintln!("collection_table Data.db absent — skipping (run fetch-datasets.sh)");
        return;
    };
    let target = uuid_bytes(TARGET_UUID);
    let schema = collection_table_schema();
    // PK-targeted point read (issue #2207 path): id = <target>.
    let filter = FilterExpr::Leaf(SSTablePredicate {
        column: "id".into(),
        operation: SSTableFilterOp::Equal,
        values: vec![Value::Uuid(target)],
        token_columns: None,
    });
    let spec = ScanSpec {
        token: None,
        filter: Some(filter),
        projection: None,
        limit: None,
    };
    let producer = MergeProducer::with_spec(schema, 64, spec).unwrap();
    let paths = producer.resolve_paths(&DirSource::new(&dir)).unwrap();
    let batches = producer
        .produce_streaming_to_vec(paths, &CancelFlag::new())
        .unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        rows > 0,
        "point read must return the target row (never a 0-row false pass)"
    );

    let arrow_schema = producer.arrow_schema().unwrap();
    let combined = concat_batches(&arrow_schema.into(), &batches).unwrap();
    assert_full_collections(&combined, &target, "point");
}
