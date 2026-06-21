//! Issue #716: byte-level regression tests for tombstone cell serialization
//! and partition-tombstone handling in the Data.db writer.
//!
//! Cassandra 5.0.2 rejected CQLite-written SSTables containing a cell delete
//! or a partition tombstone:
//!
//! 1. Tombstone cells were written with flags `0x01` (IS_DELETED) only.
//!    Cassandra's `Cell.Serializer` derives `hasValue = (flags & 0x04) == 0`,
//!    so it read a value that was never written and desynced the row stream
//!    (`CorruptSSTableException` / `EOFException: EOF after 0 bytes out of 4`).
//!    A deleted cell MUST carry `HAS_EMPTY_VALUE` (0x04): flags `0x05`.
//!
//! 2. `SSTableWriter::write_partition` extracted the partition tombstone from
//!    the FIRST mutation only, dropping a DELETE that followed earlier
//!    INSERTs (header stayed LIVE), while the tombstone-carrier mutation
//!    leaked a phantom row.
//!
//! 3. Two mutations for the same (partition, clustering) produced two rows
//!    with equal clustering in one partition — invalid in the OA format.
//!    Cassandra only reconciles rows against deletions from OTHER sources
//!    (memtable/sstable merge); an sstable must be internally reconciled,
//!    which is why shadowed rows must be merged/dropped at write time.
//!
//! These tests assert the WRITTEN BYTES, not a CQLite round-trip: a
//! round-trip passes even when reader and writer are wrong the same way.

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, PartitionTombstone, TableId, WriteEngine,
    WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

const T0: i64 = 1_704_067_200_000_000; // 2024-01-01T00:00:00Z in µs
const T1: i64 = T0 + 1;

// Cell flag bits (Cassandra Cell.Serializer)
const CELL_IS_DELETED: u8 = 0x01;
const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;

const END_OF_PARTITION: u8 = 0x01;

/// Two-regular-column table: id int PK, age int, name text.
/// Regular column serialization order: simple columns by name → [age, name].
fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue716".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "age".to_string(),
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

/// Issue #716 core defect: a deleted cell must be flags 0x05
/// (IS_DELETED | HAS_EMPTY_VALUE) followed by timestamp + local deletion
/// time VInts and NO value bytes.
#[tokio::test]
async fn tombstone_cell_sets_has_empty_value_flag() {
    let schema = test_schema();
    let table_id = TableId::new("issue716", "t");
    let pk = PartitionKey::single("id", Value::Integer(7));

    let write = Mutation::new(
        table_id.clone(),
        pk.clone(),
        None,
        vec![
            CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("x".to_string()),
            },
            CellOperation::Write {
                column: "age".to_string(),
                value: Value::Integer(30),
            },
        ],
        T0,
        None,
    );
    let delete_age = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Delete {
            column: "age".to_string(),
        }],
        T1,
        None,
    );

    let data = flush_mutations(&schema, vec![write, delete_age]).await;

    // Partition header: keylen(2) + int key(4) + deletion LDT(4) + MFDA(8)
    assert_eq!(
        u16::from_be_bytes([data[0], data[1]]),
        4,
        "int partition key length"
    );
    let mut pos = 2 + 4;
    assert_eq!(
        i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]),
        i32::MAX,
        "partition deletion must be LIVE"
    );
    pos += 4 + 8; // skip LDT + markedForDeleteAt

    // Exactly ONE merged row: flags HAS_TIMESTAMP (0x04); the cell delete
    // prevents HAS_ALL_COLUMNS.
    assert_eq!(data[pos], 0x04, "row flags: HAS_TIMESTAMP only");
    pos += 1;
    let (row_size, p) = read_vuint(&data, pos);
    pos = p;
    let row_body_start = pos;
    let (_prev, p) = read_vuint(&data, pos); // prev_unfiltered_size
    pos = p;
    let (ts_delta, p) = read_vuint(&data, pos); // liveness ts delta vs min_timestamp
    pos = p;
    assert_eq!(ts_delta, 0, "liveness comes from the T0 write");
    let (subset, p) = read_vuint(&data, pos); // columns subset (missing bitmask)
    pos = p;
    assert_eq!(
        subset, 0,
        "both regular columns are present (tombstone counts as present)"
    );

    // Cells in serialization order: age (tombstone), then name (live).
    // Tombstone cell: flags 0x05, ts delta, ldt delta — and NOTHING else.
    assert_eq!(
        data[pos],
        CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE,
        "deleted cell flags must be IS_DELETED | HAS_EMPTY_VALUE (0x05); \
         without HAS_EMPTY_VALUE Cassandra reads a phantom value (Issue #716)"
    );
    pos += 1;
    let (cell_ts_delta, p) = read_vuint(&data, pos);
    pos = p;
    assert_eq!(cell_ts_delta, 1, "tombstone keeps its own timestamp (T1)");
    let (_ldt_delta, p) = read_vuint(&data, pos);
    pos = p;

    // Immediately after the tombstone cell, the NAME cell must start — no
    // 4-byte int value in between.
    assert_eq!(
        data[pos], CELL_USE_ROW_TIMESTAMP,
        "live name cell follows the tombstone cell immediately (no value bytes)"
    );
    pos += 1;
    let (name_len, p) = read_vuint(&data, pos);
    pos = p;
    assert_eq!(name_len, 1, "name value 'x'");
    assert_eq!(data[pos], b'x');
    pos += 1;

    // Row body size accounting must close exactly at the end of the cells.
    assert_eq!(
        pos - row_body_start,
        row_size as usize,
        "row_size must cover prev_size + body exactly"
    );

    // End of partition, end of file.
    assert_eq!(data[pos], END_OF_PARTITION);
    assert_eq!(pos + 1, data.len(), "no trailing bytes after the partition");
}

/// Issue #716: a partition tombstone arriving on a later mutation must land
/// in the partition header, the carrier mutation must not leak a phantom
/// row, and the shadowed older row must be dropped (sstables are required
/// to be internally reconciled — Cassandra serves same-sstable shadowed
/// rows as live).
#[tokio::test]
async fn partition_tombstone_in_header_and_shadowed_row_dropped() {
    let schema = test_schema();
    let table_id = TableId::new("issue716", "t");
    let pk = PartitionKey::single("id", Value::Integer(9));

    let write = Mutation::new(
        table_id.clone(),
        pk.clone(),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("doomed".to_string()),
        }],
        T0,
        None,
    );
    let mut carrier = Mutation::new(table_id, pk, None, vec![], T1, None);
    carrier.partition_tombstone = Some(PartitionTombstone {
        deletion_time: T1,
        local_deletion_time: (T1 / 1_000_000) as i32,
    });

    let data = flush_mutations(&schema, vec![write, carrier]).await;

    // Header carries the tombstone (raw values, not deltas).
    let mut pos = 2 + 4;
    let ldt = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
    pos += 4;
    let mfda = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
    pos += 8;
    assert_eq!(ldt, (T1 / 1_000_000) as i32, "header local deletion time");
    assert_eq!(mfda, T1, "header markedForDeleteAt");

    // The shadowed row (T0 <= T1) and the carrier's phantom row must both be
    // gone: the partition is header + END marker, nothing else.
    assert_eq!(
        data[pos], END_OF_PARTITION,
        "no rows may follow: the T0 row is shadowed and the carrier is not a row"
    );
    assert_eq!(pos + 1, data.len());
}

/// A row written AFTER the partition tombstone survives it.
#[tokio::test]
async fn newer_row_survives_partition_tombstone() {
    let schema = test_schema();
    let table_id = TableId::new("issue716", "t");
    let pk = PartitionKey::single("id", Value::Integer(11));

    let mut carrier = Mutation::new(table_id.clone(), pk.clone(), None, vec![], T0, None);
    carrier.partition_tombstone = Some(PartitionTombstone {
        deletion_time: T0,
        local_deletion_time: (T0 / 1_000_000) as i32,
    });
    let newer_write = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("alive".to_string()),
        }],
        T1,
        None,
    );

    let data = flush_mutations(&schema, vec![carrier, newer_write]).await;

    // Header tombstone present...
    let mut pos = 2 + 4;
    let mfda = i64::from_be_bytes(data[pos + 4..pos + 12].try_into().unwrap());
    assert_eq!(mfda, T0);
    pos += 12;

    // ...and a row follows (flags byte is a row, not END_OF_PARTITION).
    assert_ne!(
        data[pos], END_OF_PARTITION,
        "the newer (T1 > T0) row must survive the partition tombstone"
    );
    assert_eq!(data[pos] & 0x04, 0x04, "row has a liveness timestamp");
}
