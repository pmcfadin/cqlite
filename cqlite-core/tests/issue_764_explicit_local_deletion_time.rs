//! Issue #764 (writer D2): an explicit `local_deletion_time` on a `Mutation`
//! must flow end-to-end to the row tombstone written in Data.db and to the
//! Statistics.db min/max localDeletionTime aggregates.
//!
//! Default behavior (`local_deletion_time: None`) MUST be identical to the
//! historical timestamp-derived value (`timestamp_micros / 1_000_000`).
//!
//! These tests assert the WRITTEN BYTES and the parsed Statistics.db, not a
//! pure CQLite round-trip, so a reader/writer that are wrong in the same way
//! cannot mask a regression.

#![cfg(feature = "write-support")]

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

// 2024-01-01T00:00:00Z in microseconds. Derived LDT = 1_704_067_200 s.
const T0: i64 = 1_704_067_200_000_000;
const DERIVED_LDT: i32 = (T0 / 1_000_000) as i32;
// An explicit local deletion time deliberately DIFFERENT from the derived one.
const EXPLICIT_LDT: i32 = 1_650_000_000; // 2022-04-15-ish, well below DERIVED_LDT

const ROW_HAS_DELETION: u8 = 0x10;

/// Single int PK, single regular text column.
fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue764".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "name".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
    }
}

/// Flush the mutations to a one-SSTable directory and return (Data.db, Statistics.db) bytes.
async fn flush_mutations(schema: &TableSchema, mutations: Vec<Mutation>) -> (Vec<u8>, Vec<u8>) {
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
    let data = std::fs::read(&info.data_path).expect("read Data.db");
    let stats = std::fs::read(&info.stats_path).expect("read Statistics.db");
    (data, stats)
}

/// Read a Cassandra unsigned VInt; returns (value, new_pos).
fn read_vuint(data: &[u8], pos: usize) -> (u64, usize) {
    let first = data[pos];
    let extra = first.leading_ones() as usize;
    assert!(extra < 8, "vint with 8 extension bytes not expected");
    let mut value = (first as u64) & (0xFFu64 >> (extra + 1).min(8));
    for i in 0..extra {
        value = (value << 8) | data[pos + 1 + i] as u64;
    }
    (value, pos + 1 + extra)
}

/// Decode the row tombstone localDeletionTime from a single-partition,
/// single-row-tombstone Data.db file, returning (min_ldt_from_stats_baseline,
/// reconstructed_row_ldt).
///
/// Layout for an int PK with no partition tombstone:
///   keylen(2) + key(4) + partition deletion LDT(4) + MFDA(8) + row...
/// Row (pure DeleteRow): flags(=ROW_HAS_DELETION) + row_size(vint) +
///   prev_size(vint) + ts_delta(vint) + ldt_delta(vint) + columns subset(vint)
fn decode_row_tombstone_ldt(data: &[u8], stats_min_ldt: i32) -> i32 {
    assert_eq!(
        u16::from_be_bytes([data[0], data[1]]),
        4,
        "int partition key length"
    );
    let mut pos = 2 + 4;
    // Partition deletion must be LIVE (the deletion is at the ROW level here).
    assert_eq!(
        i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]),
        i32::MAX,
        "partition deletion must be LIVE for a row tombstone"
    );
    pos += 4 + 8; // skip partition LDT + markedForDeleteAt

    assert_eq!(
        data[pos] & ROW_HAS_DELETION,
        ROW_HAS_DELETION,
        "row flags must include HAS_DELETION (0x10)"
    );
    pos += 1;
    let (_row_size, p) = read_vuint(data, pos);
    pos = p;
    let (_prev, p) = read_vuint(data, pos); // prev_unfiltered_size
    pos = p;
    // HAS_DELETION (no HAS_TIMESTAMP for a pure row tombstone): markedForDeleteAt
    // delta then localDeletionTime delta, both unsigned VInts.
    let (_ts_delta, p) = read_vuint(data, pos);
    pos = p;
    let (ldt_delta, _p) = read_vuint(data, pos);
    // localDeletionTime = min_local_deletion_time + ldt_delta (wrapping add).
    (stats_min_ldt as i64 + ldt_delta as i64) as i32
}

/// Helper: parse Statistics.db and return min localDeletionTime
/// (exposed by the parser as `min_deletion_time`).
fn stats_min_ldt(stats_bytes: &[u8]) -> i32 {
    let (_rem, stats) =
        parse_statistics_with_fallback(stats_bytes, None).expect("parse Statistics.db");
    stats.timestamp_stats.min_deletion_time as i32
}

#[tokio::test]
async fn explicit_local_deletion_time_flows_to_data_and_statistics() {
    let schema = test_schema();
    let table_id = TableId::new("issue764", "t");
    let pk = PartitionKey::single("id", Value::Integer(7));

    let delete_row = Mutation::new(table_id, pk, None, vec![CellOperation::DeleteRow], T0, None)
        .with_local_deletion_time(EXPLICIT_LDT);

    let (data, stats) = flush_mutations(&schema, vec![delete_row]).await;

    // Statistics.db min localDeletionTime must reflect the EXPLICIT value,
    // not the timestamp-derived one.
    let min_ldt = stats_min_ldt(&stats);
    assert_eq!(
        min_ldt, EXPLICIT_LDT,
        "Statistics.db min localDeletionTime must equal the explicit value"
    );
    assert_ne!(
        min_ldt, DERIVED_LDT,
        "the explicit value must differ from the timestamp-derived value (test sanity)"
    );

    // Data.db row tombstone localDeletionTime must reconstruct to EXPLICIT_LDT.
    let row_ldt = decode_row_tombstone_ldt(&data, min_ldt);
    assert_eq!(
        row_ldt, EXPLICIT_LDT,
        "Data.db row tombstone localDeletionTime must equal the explicit value"
    );
}

#[tokio::test]
async fn default_none_preserves_timestamp_derived_local_deletion_time() {
    let schema = test_schema();
    let table_id = TableId::new("issue764", "t");
    let pk = PartitionKey::single("id", Value::Integer(7));

    // No explicit local_deletion_time: behavior must match the historical
    // timestamp-derived value.
    let delete_row = Mutation::new(table_id, pk, None, vec![CellOperation::DeleteRow], T0, None);
    assert_eq!(delete_row.local_deletion_time, None);

    let (data, stats) = flush_mutations(&schema, vec![delete_row]).await;

    let min_ldt = stats_min_ldt(&stats);
    assert_eq!(
        min_ldt, DERIVED_LDT,
        "with None, Statistics.db min localDeletionTime must be timestamp-derived"
    );

    let row_ldt = decode_row_tombstone_ldt(&data, min_ldt);
    assert_eq!(
        row_ldt, DERIVED_LDT,
        "with None, Data.db row tombstone localDeletionTime must be timestamp-derived"
    );
}

#[test]
fn effective_local_deletion_time_prefers_explicit() {
    let m = Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::DeleteRow],
        T0,
        None,
    );
    assert_eq!(m.effective_local_deletion_time(), DERIVED_LDT);

    let m2 = m.with_local_deletion_time(EXPLICIT_LDT);
    assert_eq!(m2.effective_local_deletion_time(), EXPLICIT_LDT);
}
