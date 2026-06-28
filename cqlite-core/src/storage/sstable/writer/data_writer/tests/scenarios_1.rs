//! data_writer tests, group 1/6 (issue #1118 split).
//! Relocated verbatim from the original inline `mod tests`.

#![allow(unused_imports)]

use super::super::*;
use super::support::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, CqlType, KeyColumn, TableSchema};
use crate::storage::serialization::types::TypeSerializer;
use crate::storage::write_engine::mutation::{CellOperation, ClusteringKey, PartitionKey, TableId};
use crate::types::UdtValue;
use std::collections::HashMap;

/// Regression for #857. In the compaction path, `merge_entry_to_mutation`
/// turns the retained clustering cell into a `Write` op, so the merged mutation
/// carries the clustering column in BOTH `clustering_key` AND `operations`.
/// `merge_row_group` must drop primary-key (partition + clustering) columns from
/// `RowWrite.ops`; otherwise the writer emits the clustering value a second time
/// as a phantom regular cell, which:
///   - corrupts the row body for Cassandra's reader (CorruptSSTableException at
///     Columns$Serializer.deserializeSubset), and
///   - desyncs HAS_ALL_COLUMNS (ops.len() != regular_column_count).
#[test]
fn merge_row_group_excludes_primary_key_columns_from_ops() {
    let schema = clustering_test_schema();

    // Exactly the shape merge_entry_to_mutation produces for a compacted row.
    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(7))),
        vec![
            CellOperation::Write {
                column: "ck".to_string(),
                value: Value::Integer(7),
            },
            CellOperation::Write {
                column: "v".to_string(),
                value: Value::Text("hello".to_string()),
            },
        ],
        2000,
        None,
    );

    let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
        .expect("row group must produce a row");

    let cols = op_columns(&row);
    assert!(
        !cols.iter().any(|c| c == "ck"),
        "clustering column 'ck' must not appear as a cell op (#857); got {cols:?}"
    );
    assert!(
        cols.iter().any(|c| c == "v"),
        "regular column 'v' must be present; got {cols:?}"
    );
    assert_eq!(
        cols.len(),
        1,
        "only the single regular column should remain as a cell op; got {cols:?}"
    );
}

/// A partition-key column accidentally present in `operations` must also be
/// dropped from the row ops (defends the same invariant for the pk).
#[test]
fn merge_row_group_excludes_partition_key_column_from_ops() {
    let schema = clustering_test_schema();
    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(7))),
        vec![
            CellOperation::Write {
                column: "id".to_string(),
                value: Value::Integer(1),
            },
            CellOperation::Write {
                column: "v".to_string(),
                value: Value::Text("hello".to_string()),
            },
        ],
        2000,
        None,
    );

    let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
        .expect("row group must produce a row");
    let cols = op_columns(&row);
    assert_eq!(
        cols,
        vec!["v".to_string()],
        "partition-key column 'id' must not appear as a cell op; got {cols:?}"
    );
}

/// Direct (non-compaction) mutations never put key columns in `operations`, so
/// the filter must be a no-op for them — guards against over-filtering.
#[test]
fn merge_row_group_keeps_all_regular_ops_for_direct_mutation() {
    let schema = clustering_test_schema();
    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(7))),
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::Text("hello".to_string()),
        }],
        2000,
        None,
    );

    let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
        .expect("row group must produce a row");
    assert_eq!(op_columns(&row), vec!["v".to_string()]);
}

/// A row whose only cells are primary-key columns (a pure primary-key row,
/// e.g. `INSERT INTO t (id, ck) VALUES (...)`) must SURVIVE compaction with its
/// liveness intact even though the key columns are dropped from the cells. The
/// key-column write still signals liveness, so the row is emitted (no cells).
/// Without that, filtering would silently drop such rows.
#[test]
fn merge_row_group_keeps_pure_primary_key_row_alive() {
    let schema = clustering_test_schema();
    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(7))),
        // Only the clustering column is present as an op (as the compaction path
        // produces for a row that has no regular columns set).
        vec![CellOperation::Write {
            column: "ck".to_string(),
            value: Value::Integer(7),
        }],
        2000,
        None,
    );

    let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
        .expect("a pure primary-key row must not be dropped");
    assert!(
        op_columns(&row).is_empty(),
        "no regular cells for a pure primary-key row; got {:?}",
        op_columns(&row)
    );
    assert_eq!(
        row.liveness_ts,
        Some(2000),
        "pure primary-key row must keep its liveness timestamp"
    );
}

#[test]
fn test_data_writer_new() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);
    assert_eq!(writer.position(), 0);
}

#[test]
fn test_write_partition_header() {
    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]); // int = 42
    writer.write_partition_header(&key, None).unwrap();

    let bytes = writer.finish().unwrap();

    // Verify structure (Cassandra BigFormat):
    // [0x00, 0x04] key length (u16 BE = 4 bytes)
    // [0x00, 0x00, 0x00, 0x2A] key bytes
    // [0x7F, 0xFF, 0xFF, 0xFF] DeletionTime.LIVE local_deletion_time (i32::MAX)
    // [0x80, 0x00...] DeletionTime.LIVE deletion_timestamp (i64::MIN)
    assert_eq!(&bytes[0..2], &[0x00, 0x04]); // key length (u16 BE)
    assert_eq!(&bytes[2..6], &[0x00, 0x00, 0x00, 0x2A]); // key bytes
    assert_eq!(&bytes[6..10], &i32::MAX.to_be_bytes()); // DeletionTime.LIVE ldt
    assert_eq!(&bytes[10..18], &i64::MIN.to_be_bytes()); // DeletionTime.LIVE ts
}

#[test]
fn test_write_simple_row() {
    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);
    let schema = create_test_schema();

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![
            CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("Alice".to_string()),
            },
            CellOperation::Write {
                column: "age".to_string(),
                value: Value::Integer(30),
            },
        ],
        1001000, // timestamp (delta = 1000)
        None,
    );

    writer.write_row(&mutation, &schema).unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Verify row flags
    let flags = bytes[0];
    assert_eq!(
        flags & ROW_HAS_TIMESTAMP,
        ROW_HAS_TIMESTAMP,
        "Should have timestamp"
    );
    assert_eq!(
        flags & ROW_HAS_ALL_COLUMNS,
        ROW_HAS_ALL_COLUMNS,
        "Should have all columns"
    );
}

#[test]
fn test_write_row_with_clustering() {
    let mut schema = create_test_schema();
    schema.clustering_keys = vec![ClusteringColumn {
        name: "ts".to_string(),
        data_type: "timestamp".to_string(),
        position: 0,
        order: ClusteringOrder::Asc,
    }];

    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let ck = ClusteringKey::single("ts", Value::Timestamp(1234567890));
    let mutation = Mutation::new(
        table_id,
        pk,
        Some(ck),
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("Bob".to_string()),
        }],
        1001000,
        None,
    );

    writer.write_row(&mutation, &schema).unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Verify row has flags and clustering prefix
    let flags = bytes[0];
    assert_eq!(flags & ROW_HAS_TIMESTAMP, ROW_HAS_TIMESTAMP);
}

#[test]
fn test_write_partition_complete() {
    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);
    let schema = create_test_schema();

    let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x01]);
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    let mutations = vec![
        Mutation::new(
            table_id.clone(),
            pk.clone(),
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("Alice".to_string()),
            }],
            1001000,
            None,
        ),
        Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("Bob".to_string()),
            }],
            1002000,
            None,
        ),
    ];

    let offset = writer
        .write_partition(&key, &mutations, &schema, None, &[])
        .unwrap();
    assert_eq!(offset, 0); // First partition starts at offset 0

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Verify end-of-partition marker is present
    assert_eq!(bytes[bytes.len() - 1], END_OF_PARTITION);
}

/// Regression test for bug #644 (S6): temporal deltas MUST use unsigned VInt.
///
/// The writer previously used ZigZag-encoded signed VInt (`encode_signed`) for
/// all row-header temporal deltas (timestamp, TTL, LDT).  ZigZag maps positive
/// integer n → 2n, so a delta of 5000 would be encoded as 10000, which the
/// reader (fixed in S1, using `parse_vuint` = unsigned VInt) would decode as
/// 10000 — doubling every timestamp on readback.
///
/// Per Cassandra `SerializationHeader.java:167`:
///   `out.writeUnsignedVInt(timestamp - stats.minTimestamp)`
///   `out.writeUnsignedVInt(ttl - stats.minTTL)`
///   `out.writeUnsignedVInt(localDeletionTime - stats.minLocalDeletionTime)`
///
/// Expected encodings (2-byte unsigned VInt, Cassandra format: leading 1-bits + data):
///   unsigned VInt(5000 = 0x1388):
///     extra_bytes=1, first=(0x80 | (0x1388>>8)&0x3F)=0x93, second=0x88  → [0x93, 0x88]
///     ZigZag(5000)=10000 would give [0xA7, 0x10]  ← WRONG (pre-fix value)
///
///   unsigned VInt(3600 = 0x0E10):
///     extra_bytes=1, first=(0x80 | (0x0E10>>8)&0x3F)=0x8E, second=0x10  → [0x8E, 0x10]
///     ZigZag(3600)=7200 would give [0x9C, 0x20]  ← WRONG (pre-fix value)
#[test]
fn test_delta_encoding_unsigned_vint_fix_644() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1_000_000;
    stats.min_ttl = 3_600;
    stats.min_local_deletion_time = 0;

    let writer = DataWriter::new(stats.clone());
    let schema = create_test_schema();

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("Test".to_string()),
        }],
        1_005_000,  // timestamp_micros; delta from min_timestamp(1_000_000) = 5_000
        Some(7200), // ttl; delta from min_ttl(3_600) = 3_600
    );

    let row_body = writer
        .build_row_body(&mutation, &schema, ROW_HAS_TIMESTAMP | ROW_HAS_TTL)
        .unwrap();
    assert!(!row_body.is_empty(), "row body must be non-empty");

    // The row body for HAS_TIMESTAMP | HAS_TTL starts with:
    //   [0..2] timestamp delta as unsigned VInt
    //   [2..4] ttl delta as unsigned VInt
    //   [4..]  ldt delta as unsigned VInt (time-dependent, not asserted)
    //   ...    column bitmap, cells
    //
    // timestamp_delta = 5000 → unsigned VInt = [0x93, 0x88]
    // ZigZag(5000) = 10000 → would give [0xA7, 0x10]  ← OLD/WRONG pre-fix encoding
    assert_eq!(
        &row_body[0..2],
        &[0x93u8, 0x88u8],
        "Fix #644: timestamp delta=5000 must encode as unsigned VInt [0x93, 0x88], \
             not ZigZag [0xA7, 0x10]. Reader uses parse_vuint (unsigned), so ZigZag would \
             double the delta on readback (5000 → decoded as 10000)."
    );

    // ttl_delta = 7200 - 3600 = 3600 → unsigned VInt = [0x8E, 0x10]
    // ZigZag(3600) = 7200 → would give [0x9C, 0x20]  ← OLD/WRONG pre-fix encoding
    assert_eq!(
        &row_body[2..4],
        &[0x8Eu8, 0x10u8],
        "Fix #644: TTL delta=3600 must encode as unsigned VInt [0x8E, 0x10], \
             not ZigZag [0x9C, 0x20]. This is the first of two HAS_TTL fields."
    );
}

#[test]
fn test_delta_encoding() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1000000;
    stats.min_ttl = 3600;

    let writer = DataWriter::new(stats.clone());
    let schema = create_test_schema();

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("Test".to_string()),
        }],
        1005000,    // timestamp (delta = 5000)
        Some(7200), // TTL (delta = 3600)
    );

    let row_body = writer
        .build_row_body(&mutation, &schema, ROW_HAS_TIMESTAMP | ROW_HAS_TTL)
        .unwrap();
    assert!(!row_body.is_empty());
}

#[test]
fn test_serialize_value_types() {
    // Boolean
    let bytes = serialize_value(&Value::Boolean(true)).unwrap();
    assert_eq!(bytes, vec![1]);

    // Integer
    let bytes = serialize_value(&Value::Integer(42)).unwrap();
    assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x2A]);

    // Text
    let bytes = serialize_value(&Value::Text("hello".to_string())).unwrap();
    assert_eq!(bytes, b"hello");

    // BigInt
    let bytes = serialize_value(&Value::BigInt(9223372036854775807)).unwrap();
    assert_eq!(bytes, vec![0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);

    // Null
    let bytes = serialize_value(&Value::Null).unwrap();
    assert_eq!(bytes, Vec::<u8>::new());
}

#[test]
fn test_column_bitmap() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);
    let schema = create_test_schema();

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // Only write "name" column (not "age")
    // Schema has 2 regular columns sorted alphabetically: [age(0), name(1)]
    // "age" is MISSING → bitmap bit 0 set → bitmap = 0b01 = 1
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("Alice".to_string()),
        }],
        1001000,
        None,
    );

    let mut buf = Vec::new();
    writer
        .write_column_bitmap(&mut buf, &mutation, &schema)
        .unwrap();

    // Cassandra format: single VUInt of missing columns bitmask
    // "age" (index 0) is missing → bitmap = 0x01
    assert_eq!(buf, vec![0x01]);
}

#[test]
fn test_partition_key_size_limit() {
    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    // 256 bytes should succeed (u16 allows up to 65535)
    let key_256 = vec![0xFF; 256];
    let key = DecoratedKey::new(12345, key_256);
    let result = writer.write_partition_header(&key, None);
    assert!(result.is_ok());

    // Create a partition key larger than 65535 bytes
    let mut writer2 = DataWriter::new(create_test_stats());
    let large_key = vec![0xFF; 65536];
    let key = DecoratedKey::new(12345, large_key);

    let result = writer2.write_partition_header(&key, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("too large"));
}

#[test]
fn test_write_tombstone_cell() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1000000;
    stats.min_local_deletion_time = 1700000000; // Jan 2023
    let writer = DataWriter::new(stats);

    let mut buf = Vec::new();
    let timestamp = 1001000; // delta = 1000
    let local_deletion_time = 1700000010; // delta = 10
    writer
        .write_tombstone_cell(&mut buf, "deleted_col", timestamp, local_deletion_time)
        .unwrap();

    assert!(!buf.is_empty());
    // First byte should be tombstone flags (only IS_DELETED, no USE_ROW_TIMESTAMP)
    let flags = buf[0];
    assert_eq!(
        flags & CELL_IS_DELETED,
        CELL_IS_DELETED,
        "Should have IS_DELETED flag"
    );
    assert_eq!(
        flags & CELL_USE_ROW_TIMESTAMP,
        0,
        "Should NOT have USE_ROW_TIMESTAMP flag"
    );

    // Should have timestamp delta and local_deletion_time delta encoded as VInts
    assert!(
        buf.len() > 1,
        "Should have timestamp and deletion_time deltas"
    );
}

#[test]
fn test_serialize_clustering_value_fixed_width() {
    // Integer (fixed-width, no length prefix)
    let bytes = serialize_value_for_clustering(&Value::Integer(42), &ComparatorType::Int).unwrap();
    assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x2A]);

    // BigInt (fixed-width)
    let bytes =
        serialize_value_for_clustering(&Value::BigInt(1000), &ComparatorType::BigInt).unwrap();
    assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8]);
}

#[test]
fn test_serialize_clustering_value_variable_width() {
    // Text (variable-width, VInt length prefix)
    let bytes =
        serialize_value_for_clustering(&Value::Text("test".to_string()), &ComparatorType::Text)
            .unwrap();
    assert!(!bytes.is_empty());
    // First byte(s) should be VInt length (4), followed by "test"
    // VInt(4) = 0x04, then "test"
    assert_eq!(bytes[0], 0x04); // VInt length = 4
    assert_eq!(&bytes[1..], b"test");
}

#[test]
fn test_serialize_clustering_date_includes_length_prefix() {
    let bytes = serialize_value_for_clustering(&Value::Date(0), &ComparatorType::Date).unwrap();
    assert_eq!(
        bytes[0], 0x04,
        "date clustering values should be length-prefixed"
    );
    assert_eq!(
        bytes.len(),
        5,
        "date clustering value should be 1-byte length + 4-byte payload"
    );
}

#[test]
fn test_serialize_clustering_frozen_list_text() {
    let value = Value::Frozen(Box::new(Value::List(vec![Value::Text("solo".to_string())])));
    let comparator = ComparatorType::Frozen(Box::new(ComparatorType::List(Box::new(
        ComparatorType::Text,
    ))));

    let bytes = serialize_value_for_clustering(&value, &comparator).unwrap();
    let expected_inner =
        serialize_value(&Value::List(vec![Value::Text("solo".to_string())])).unwrap();

    let mut expected = vec![expected_inner.len() as u8];
    expected.extend_from_slice(&expected_inner);

    assert_eq!(bytes, expected);
}

#[test]
fn test_null_vs_empty_string() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    // Test NULL - should not be written as a cell
    let result = writer.write_cell(&mut Vec::new(), "test_col", &Value::Null, 1001000);
    assert!(result.is_err(), "NULL values should return error");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("NULL values should not be written"));

    // Test empty string - should have HAS_EMPTY_VALUE flag
    let mut buf = Vec::new();
    writer
        .write_cell(&mut buf, "test_col", &Value::Text(String::new()), 1001000)
        .unwrap();

    assert!(!buf.is_empty());
    let flags = buf[0];
    assert_eq!(
        flags & CELL_HAS_EMPTY_VALUE,
        CELL_HAS_EMPTY_VALUE,
        "Empty string should have HAS_EMPTY_VALUE flag"
    );

    // Test non-empty string - should NOT have HAS_EMPTY_VALUE flag
    let mut buf2 = Vec::new();
    writer
        .write_cell(
            &mut buf2,
            "test_col",
            &Value::Text("test".to_string()),
            1001000,
        )
        .unwrap();

    let flags2 = buf2[0];
    assert_eq!(
        flags2 & CELL_HAS_EMPTY_VALUE,
        0,
        "Non-empty string should NOT have HAS_EMPTY_VALUE flag"
    );

    assert_eq!(buf, vec![CELL_USE_ROW_TIMESTAMP | CELL_HAS_EMPTY_VALUE]);
}

#[test]
fn test_fixed_width_cell_omits_length_prefix() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);
    let mut buf = Vec::new();

    writer
        .write_cell(&mut buf, "value", &Value::Integer(42), 1001000)
        .unwrap();

    assert_eq!(buf, vec![CELL_USE_ROW_TIMESTAMP, 0x00, 0x00, 0x00, 0x2A]);
}

#[test]
fn test_variable_width_cell_keeps_length_prefix() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);
    let mut buf = Vec::new();

    writer
        .write_cell(&mut buf, "value", &Value::Text("abc".to_string()), 1001000)
        .unwrap();

    assert_eq!(buf, vec![CELL_USE_ROW_TIMESTAMP, 0x03, b'a', b'b', b'c']);
}
