//! Issue #717: byte-level regression tests for row-tombstone and
//! range-tombstone serialization in the Data.db writer.
//!
//! Cassandra 5.0.2 rejected CQLite-written SSTables containing row deletes
//! or range tombstones with
//! `IOException: Invalid Columns subset bytes; too many bits set:1001`:
//!
//! 1. Row tombstones omitted the columns-subset field. Cassandra's
//!    `UnfilteredSerializer.deserializeRowBody` reads the subset right after
//!    the deletion times whenever HAS_ALL_COLUMNS is unset — for EVERY row,
//!    deleted or not. With the subset missing, the next row's flags byte
//!    (0x24) was consumed as the subset VInt; after consuming 2 bits for the
//!    2-column superset, the leftover bits `1001` triggered the exception.
//!
//! 2. Range tombstone bound markers used a private format: home-grown kind
//!    ordinals (0..5), no u16 cluster count, and no marker_body_size /
//!    prev_size VInts — and markers were emitted before all rows instead of
//!    interleaved in clustering order.
//!
//! These tests assert the WRITTEN BYTES, not a CQLite round-trip: a
//! round-trip passes even when reader and writer are wrong the same way.

#![cfg(feature = "write-support")]

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, RangeTombstone, TableId,
    WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

const T0: i64 = 1_704_067_200_000_000; // 2024-01-01T00:00:00Z in µs
const T1: i64 = T0 + 1;

// Row flag bits (Cassandra UnfilteredSerializer)
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_DELETION: u8 = 0x10;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;

const IS_MARKER: u8 = 0x02;
const END_OF_PARTITION: u8 = 0x01;

// Cassandra ClusteringPrefix.Kind ordinals
const INCL_START_BOUND: u8 = 1;
const INCL_END_BOUND: u8 = 6;

/// pk int PK, ck int clustering, two regular columns a text / b int.
fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue717".to_string(),
        table: "t".to_string(),
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
                name: "a".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "b".to_string(),
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

async fn flush_mutations(schema: &TableSchema, mutations: Vec<Mutation>) -> Vec<u8> {
    let temp = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp.path().join("data"),
        temp.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in mutations {
        engine.write_async(m).await.expect("write");
    }
    let info = engine
        .flush()
        .await
        .expect("flush")
        .expect("flush produced an sstable");
    std::fs::read(&info.data_path).expect("read Data.db")
}

/// Read a Cassandra unsigned VInt; returns (value, new_pos).
fn read_vuint(data: &[u8], pos: usize) -> (u64, usize) {
    let first = data[pos];
    let extra = first.leading_ones() as usize;
    assert!(
        extra < 8,
        "vint with 8 extension bytes not expected in tests"
    );
    let mut value = (first as u64) & (0xFFu64 >> (extra + 1).min(8));
    for i in 0..extra {
        value = (value << 8) | data[pos + 1 + i] as u64;
    }
    (value, pos + 1 + extra)
}

fn write_row(table_id: &TableId, pk: &PartitionKey, ck: i32, ts: i64) -> Mutation {
    Mutation::new(
        table_id.clone(),
        pk.clone(),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![
            CellOperation::Write {
                column: "a".to_string(),
                value: Value::Text("v".to_string()),
            },
            CellOperation::Write {
                column: "b".to_string(),
                value: Value::Integer(ck),
            },
        ],
        ts,
        None,
    )
}

/// Issue #717 core defect: a pure row tombstone must serialize as
/// flags 0x10 (HAS_DELETION, no liveness, no HAS_ALL_COLUMNS) with body
/// [prev_size][mfda delta][ldt delta][columns subset = all-missing bitmask].
#[tokio::test]
async fn row_tombstone_emits_columns_subset() {
    let schema = test_schema();
    let table_id = TableId::new("issue717", "t");
    let pk = PartitionKey::single("pk", Value::Integer(1));

    let delete_row = Mutation::new(
        table_id.clone(),
        pk.clone(),
        Some(ClusteringKey::single("ck", Value::Integer(5))),
        vec![CellOperation::DeleteRow],
        T1,
        None,
    );

    let data = flush_mutations(&schema, vec![write_row(&table_id, &pk, 5, T0), delete_row]).await;

    // Partition header: keylen(2) + int key(4) + LIVE deletion(12)
    let mut pos = 2 + 4 + 12;

    // The T0 write and the T1 DeleteRow share clustering ck=5, so they MUST
    // merge into a single pure tombstone row (two equal-clustering rows in
    // one partition are invalid; Cassandra does not reconcile rows against
    // deletions within the same sstable).
    let flags = data[pos];
    pos += 1;
    assert_eq!(
        flags & ROW_HAS_DELETION,
        ROW_HAS_DELETION,
        "row tombstone must set HAS_DELETION"
    );
    assert_eq!(
        flags & ROW_HAS_TIMESTAMP,
        0,
        "pure row tombstone carries no primary-key liveness"
    );
    assert_eq!(flags & ROW_HAS_ALL_COLUMNS, 0);

    // Clustering prefix: header VInt (all-present = 0) + 4-byte int value
    let (ck_header, p) = read_vuint(&data, pos);
    pos = p;
    assert_eq!(ck_header, 0);
    assert_eq!(
        i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()),
        5
    );
    pos += 4;

    let (row_size, p) = read_vuint(&data, pos);
    pos = p;
    let body_start = pos;
    let (_prev, p) = read_vuint(&data, pos);
    pos = p;
    let (mfda_delta, p) = read_vuint(&data, pos);
    pos = p;
    assert_eq!(
        mfda_delta, 1,
        "deletion timestamp T1 = min_timestamp(T0) + 1"
    );
    let (_ldt_delta, p) = read_vuint(&data, pos);
    pos = p;

    // Issue #717: the columns subset MUST follow the deletion times. For a
    // pure tombstone over a 2-regular-column table it is the all-missing
    // bitmask 0b11.
    let (subset, p) = read_vuint(&data, pos);
    pos = p;
    assert_eq!(
        subset, 0b11,
        "row tombstone must write the all-missing columns subset; omitting it \
         makes Cassandra read the next row's flags as the subset (Issue #717)"
    );

    assert_eq!(
        pos - body_start,
        row_size as usize,
        "row body ends exactly after the subset (no cells)"
    );

    // Nothing else in the partition: the shadowed T0 row was merged away.
    assert_eq!(data[pos], END_OF_PARTITION);
    assert_eq!(pos + 1, data.len());
}

/// Issue #717: range tombstone bound markers must use the Cassandra wire
/// format (ClusteringPrefix.Kind ordinals, u16 cluster count,
/// marker_body_size + prev_size VInts) and be interleaved with rows in
/// clustering order.
#[tokio::test]
async fn range_tombstone_markers_cassandra_format_and_ordering() {
    let schema = test_schema();
    let table_id = TableId::new("issue717", "t");
    let pk = PartitionKey::single("pk", Value::Integer(2));

    let mut carrier = Mutation::new(table_id.clone(), pk.clone(), None, vec![], T1, None);
    carrier.range_tombstones.push(RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(2))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(2))),
        deletion_time: T1,
        local_deletion_time: (T1 / 1_000_000) as i32,
    });

    let data = flush_mutations(
        &schema,
        vec![
            write_row(&table_id, &pk, 1, T0),
            write_row(&table_id, &pk, 2, T0), // shadowed by the range tombstone
            write_row(&table_id, &pk, 3, T0),
            carrier,
        ],
    )
    .await;

    let mut pos = 2 + 4 + 12; // partition header (LIVE)

    // --- Row ck=1 (full row: HAS_TIMESTAMP | HAS_ALL_COLUMNS) ---
    assert_eq!(data[pos], ROW_HAS_TIMESTAMP | ROW_HAS_ALL_COLUMNS);
    pos += 1;
    let (_, p) = read_vuint(&data, pos); // clustering header
    pos = p;
    assert_eq!(
        i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()),
        1
    );
    pos += 4;
    let (row_size, p) = read_vuint(&data, pos);
    pos = p + row_size as usize;

    // --- Open marker: INCL_START_BOUND at ck=2, before the (dropped) row ---
    assert_eq!(
        data[pos], IS_MARKER,
        "open marker must precede ck=2 content"
    );
    pos += 1;
    assert_eq!(
        data[pos], INCL_START_BOUND,
        "open bound kind must be ClusteringPrefix.Kind ordinal 1 (INCL_START_BOUND)"
    );
    pos += 1;
    assert_eq!(
        u16::from_be_bytes([data[pos], data[pos + 1]]),
        1,
        "u16 cluster count follows the kind byte"
    );
    pos += 2;
    let (_, p) = read_vuint(&data, pos); // clustering header
    pos = p;
    assert_eq!(
        i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()),
        2
    );
    pos += 4;
    let (marker_body, p) = read_vuint(&data, pos);
    pos = p;
    let body_start = pos;
    let (_prev, p) = read_vuint(&data, pos);
    pos = p;
    let (mfda_delta, p) = read_vuint(&data, pos);
    pos = p;
    assert_eq!(mfda_delta, 1, "deletion T1 = min_timestamp(T0) + 1");
    let (_ldt_delta, p) = read_vuint(&data, pos);
    pos = p;
    assert_eq!(
        pos - body_start,
        marker_body as usize,
        "marker_body_size covers prev_size + deletion times"
    );

    // --- Close marker: INCL_END_BOUND at ck=2 ---
    // The ck=2 row (T0 <= T1) is shadowed by the range tombstone and must
    // NOT appear between the bounds.
    assert_eq!(
        data[pos], IS_MARKER,
        "close marker follows immediately: the shadowed ck=2 row is dropped"
    );
    pos += 1;
    assert_eq!(
        data[pos], INCL_END_BOUND,
        "close bound kind must be ClusteringPrefix.Kind ordinal 6 (INCL_END_BOUND)"
    );
    pos += 1;
    assert_eq!(u16::from_be_bytes([data[pos], data[pos + 1]]), 1);
    pos += 2;
    let (_, p) = read_vuint(&data, pos);
    pos = p;
    assert_eq!(
        i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()),
        2
    );
    pos += 4;
    let (marker_body, p) = read_vuint(&data, pos);
    pos = p + marker_body as usize;

    // --- Row ck=3 survives after the close bound ---
    assert_eq!(data[pos], ROW_HAS_TIMESTAMP | ROW_HAS_ALL_COLUMNS);
    pos += 1;
    let (_, p) = read_vuint(&data, pos);
    pos = p;
    assert_eq!(
        i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()),
        3
    );
    pos += 4;
    let (row_size, p) = read_vuint(&data, pos);
    pos = p + row_size as usize;

    assert_eq!(data[pos], END_OF_PARTITION);
    assert_eq!(pos + 1, data.len());
}
