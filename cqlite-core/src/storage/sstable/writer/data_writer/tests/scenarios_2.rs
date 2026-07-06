//! data_writer tests, group 2/6 (issue #1118 split).
//! Relocated verbatim from the original inline `mod tests`.

#![allow(unused_imports)]

use super::super::*;
use super::support::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, CqlType, KeyColumn, TableSchema};
use crate::storage::serialization::types::TypeSerializer;
use crate::storage::write_engine::mutation::{CellOperation, ClusteringKey, PartitionKey, TableId};
use crate::types::UdtValue;
use std::collections::HashMap;

#[test]
fn test_value_length_bounds_check() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    // Create a value that exceeds i64::MAX (simulated via the check)
    // Since we can't actually allocate > i64::MAX bytes, we test the logic path
    // by checking that reasonable values pass
    let mut buf = Vec::new();
    let large_text = "x".repeat(1000);
    let result = writer.write_cell(&mut buf, "test_col", &Value::Text(large_text), 1001000);
    assert!(result.is_ok(), "Reasonable-sized values should succeed");
}

#[test]
fn test_tombstone_requires_deletion_time() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1000000;
    stats.min_local_deletion_time = 1700000000;
    let writer = DataWriter::new(stats);

    let mut buf = Vec::new();

    // Test with valid deletion_time > min_local_deletion_time
    let result = writer.write_tombstone_cell(
        &mut buf,
        "deleted_col",
        1001000,
        1700000010, // Greater than min
    );
    assert!(result.is_ok(), "Valid deletion_time should succeed");

    // Test with deletion_time < min_local_deletion_time (should error)
    let mut buf2 = Vec::new();
    let result2 = writer.write_tombstone_cell(
        &mut buf2,
        "deleted_col",
        1001000,
        1600000000, // Less than min
    );
    assert!(result2.is_err(), "deletion_time < min should fail");
    assert!(result2
        .unwrap_err()
        .to_string()
        .contains("less than min_local_deletion_time"));
}

#[test]
fn test_column_bitmap_skips_nulls() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);
    let schema = create_test_schema();

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // Write "name" with value, "age" with NULL
    // Schema has 2 regular columns sorted alphabetically: [age(0), name(1)]
    // "age" is NULL (missing) → bit 0 = 1
    // "name" is present → bit 1 = 0
    // bitmap = 0b01 = 0x01
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
                value: Value::Null,
            },
        ],
        1001000,
        None,
    );

    let mut buf = Vec::new();
    writer
        .write_column_bitmap(&mut buf, &mutation, &schema)
        .unwrap();

    // Cassandra format: single VUInt bitmask where bit=1 means MISSING
    // Only "age" (index 0) is missing → bitmap = 0x01
    assert_eq!(
        buf,
        vec![0x01],
        "Bitmap should encode age as missing (bit 0)"
    );
}

#[test]
fn test_row_with_null_values() {
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
                value: Value::Null, // NULL value
            },
        ],
        1001000,
        None,
    );

    writer.write_row(&mutation, &schema).unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Verify row flags do NOT have HAS_ALL_COLUMNS (because of NULL)
    let flags = bytes[0];
    assert_eq!(
        flags & ROW_HAS_ALL_COLUMNS,
        0,
        "Row with NULL should NOT have HAS_ALL_COLUMNS flag"
    );
}

#[test]
fn test_multiple_partitions() {
    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);
    let schema = create_test_schema();

    // Write first partition
    let key1 = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x01]);
    let table_id = TableId::new("test_ks", "test_table");
    let pk1 = PartitionKey::single("id", Value::Integer(1));
    let mutations1 = vec![Mutation::new(
        table_id.clone(),
        pk1,
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("Alice".to_string()),
        }],
        1001000,
        None,
    )];

    let offset1 = writer
        .write_partition(&key1, &mutations1, &schema, None, &[])
        .unwrap();
    assert_eq!(offset1, 0);

    // Write second partition
    let key2 = DecoratedKey::new(200, vec![0x00, 0x00, 0x00, 0x02]);
    let pk2 = PartitionKey::single("id", Value::Integer(2));
    let mutations2 = vec![Mutation::new(
        table_id,
        pk2,
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("Bob".to_string()),
        }],
        1002000,
        None,
    )];

    let offset2 = writer
        .write_partition(&key2, &mutations2, &schema, None, &[])
        .unwrap();
    assert!(offset2 > offset1); // Second partition starts after first

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Both partitions should have end-of-partition markers
    // Note: END_OF_PARTITION (0x01) may appear elsewhere (e.g., in cell flags)
    // For this test, we verify the file structure is valid and both partitions were written
    assert!(
        offset2 > offset1,
        "Second partition should start after first"
    );

    // The last byte should be an END_OF_PARTITION marker
    assert_eq!(
        bytes[bytes.len() - 1],
        END_OF_PARTITION,
        "File should end with END_OF_PARTITION"
    );
}

#[test]
fn test_row_tombstone() {
    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);
    let schema = create_test_schema();

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::DeleteRow],
        1001000,
        None,
    );

    writer.write_row(&mutation, &schema).unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Verify row flags have HAS_DELETION
    let flags = bytes[0];
    assert_eq!(
        flags & ROW_HAS_DELETION,
        ROW_HAS_DELETION,
        "Should have HAS_DELETION flag"
    );
    // Issue #717: a pure row tombstone carries no primary-key liveness —
    // Cassandra serializes DELETE-d rows without HAS_TIMESTAMP.
    assert_eq!(
        flags & ROW_HAS_TIMESTAMP,
        0,
        "Pure row tombstone must not have HAS_TIMESTAMP"
    );
    assert_eq!(
        flags & ROW_HAS_ALL_COLUMNS,
        0,
        "Row tombstone must not claim all columns"
    );

    // Issue #717: the columns subset must follow the deletion times.
    // Layout: [flags][row_size][prev_size=0][deletion mfda][deletion ldt][subset]
    // With create_test_stats baselines both deletion deltas and the
    // all-missing subset are single-byte VInts.
    let row_size = bytes[1] as usize;
    // Body = prev_size(1) + mfda(vint) + ldt(vint) + subset(vint ≥ 1 byte)
    assert!(
        row_size >= 4,
        "Row tombstone body must include the columns subset (got row_size={})",
        row_size
    );
    // The final body byte is the all-missing subset bitmask: 2 regular
    // columns (name, value) in create_test_schema → 0b11.
    let body_end = 2 + row_size; // flags + row_size byte + body
    assert_eq!(
        bytes[body_end - 1],
        0b11,
        "Columns subset must mark every regular column missing"
    );
}

#[test]
fn test_partition_tombstone() {
    use crate::storage::write_engine::mutation::PartitionTombstone;

    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    let tombstone = PartitionTombstone {
        deletion_time: 1001000,          // microseconds
        local_deletion_time: 1700000010, // seconds
    };

    writer
        .write_partition_header(&key, Some(&tombstone))
        .unwrap();

    let bytes = writer.finish().unwrap();

    // Verify structure (Cassandra BigFormat):
    // [0x00, 0x04] key length (u16 BE)
    // [key bytes]
    // [local_deletion_time: i32 BE]
    // [deletion_timestamp: i64 BE]
    assert_eq!(&bytes[0..2], &[0x00, 0x04], "Key length (u16 BE)");

    // Check local_deletion_time (i32 BE at offset 6)
    let ldt_bytes = &bytes[6..10];
    let ldt = i32::from_be_bytes([ldt_bytes[0], ldt_bytes[1], ldt_bytes[2], ldt_bytes[3]]);
    assert_eq!(ldt, 1700000010, "Local deletion time should match");

    // Check deletion_timestamp (i64 BE at offset 10)
    let ts_bytes = &bytes[10..18];
    let ts = i64::from_be_bytes([
        ts_bytes[0],
        ts_bytes[1],
        ts_bytes[2],
        ts_bytes[3],
        ts_bytes[4],
        ts_bytes[5],
        ts_bytes[6],
        ts_bytes[7],
    ]);
    assert_eq!(ts, 1001000, "Deletion timestamp should match");
}

#[test]
fn test_range_tombstone_inclusive_bounds() {
    use crate::storage::write_engine::mutation::{ClusteringBound, RangeTombstone};

    let mut schema = create_test_schema();
    schema.clustering_keys = vec![ClusteringColumn {
        name: "ts".to_string(),
        data_type: "timestamp".to_string(),
        position: 0,
        order: ClusteringOrder::Asc,
    }];

    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let range = RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ts", Value::Timestamp(1000))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ts", Value::Timestamp(2000))),
        deletion_time: 1001000,
        local_deletion_time: 1700000010,
    };

    let open_size = writer
        .write_range_bound(
            &range.start,
            true,
            range.deletion_time,
            range.local_deletion_time,
            &schema,
            0,
        )
        .unwrap();
    writer
        .write_range_bound(
            &range.end,
            false,
            range.deletion_time,
            range.local_deletion_time,
            &schema,
            open_size as u64,
        )
        .unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Verify opening bound: Cassandra ClusteringPrefix.Kind ordinals
    assert_eq!(bytes[0], IS_MARKER, "Should have IS_MARKER flag");
    assert_eq!(
        bytes[1], INCL_START_BOUND,
        "Should have INCL_START_BOUND kind (ordinal 1)"
    );
    // u16 BE cluster count follows the kind byte
    assert_eq!(
        u16::from_be_bytes([bytes[2], bytes[3]]),
        1,
        "Bound carries one clustering value"
    );

    // Closing bound starts right after the opening marker
    assert_eq!(bytes[open_size], IS_MARKER);
    assert_eq!(
        bytes[open_size + 1],
        INCL_END_BOUND,
        "Should have INCL_END_BOUND kind (ordinal 6)"
    );
}

#[test]
fn test_range_tombstone_exclusive_bounds() {
    use crate::storage::write_engine::mutation::{ClusteringBound, RangeTombstone};

    let mut schema = create_test_schema();
    schema.clustering_keys = vec![ClusteringColumn {
        name: "ts".to_string(),
        data_type: "timestamp".to_string(),
        position: 0,
        order: ClusteringOrder::Asc,
    }];

    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let range = RangeTombstone {
        start: ClusteringBound::Exclusive(ClusteringKey::single("ts", Value::Timestamp(1000))),
        end: ClusteringBound::Exclusive(ClusteringKey::single("ts", Value::Timestamp(2000))),
        deletion_time: 1001000,
        local_deletion_time: 1700000010,
    };

    let open_size = writer
        .write_range_bound(
            &range.start,
            true,
            range.deletion_time,
            range.local_deletion_time,
            &schema,
            0,
        )
        .unwrap();
    writer
        .write_range_bound(
            &range.end,
            false,
            range.deletion_time,
            range.local_deletion_time,
            &schema,
            open_size as u64,
        )
        .unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Verify opening bound: Cassandra ClusteringPrefix.Kind ordinals
    assert_eq!(bytes[0], IS_MARKER, "Should have IS_MARKER flag");
    assert_eq!(
        bytes[1], EXCL_START_BOUND,
        "Should have EXCL_START_BOUND kind (ordinal 7)"
    );
    assert_eq!(
        bytes[open_size + 1],
        EXCL_END_BOUND,
        "Should have EXCL_END_BOUND kind (ordinal 0)"
    );
}

#[test]
fn test_range_tombstone_bottom_top_bounds() {
    use crate::storage::write_engine::mutation::{ClusteringBound, RangeTombstone};

    let mut schema = create_test_schema();
    schema.clustering_keys = vec![ClusteringColumn {
        name: "ts".to_string(),
        data_type: "timestamp".to_string(),
        position: 0,
        order: ClusteringOrder::Asc,
    }];

    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    // Delete everything from start to end of partition
    let range = RangeTombstone {
        start: ClusteringBound::Bottom,
        end: ClusteringBound::Top,
        deletion_time: 1001000,
        local_deletion_time: 1700000010,
    };

    let open_size = writer
        .write_range_bound(
            &range.start,
            true,
            range.deletion_time,
            range.local_deletion_time,
            &schema,
            0,
        )
        .unwrap();
    writer
        .write_range_bound(
            &range.end,
            false,
            range.deletion_time,
            range.local_deletion_time,
            &schema,
            open_size as u64,
        )
        .unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Bottom serializes as an inclusive start bound with zero clustering
    // values (u16 count = 0, no clustering header byte).
    assert_eq!(bytes[0], IS_MARKER, "Should have IS_MARKER flag");
    assert_eq!(
        bytes[1], INCL_START_BOUND,
        "Bottom should serialize as INCL_START_BOUND"
    );
    assert_eq!(
        u16::from_be_bytes([bytes[2], bytes[3]]),
        0,
        "Bottom carries no clustering values"
    );
    // Top serializes as an inclusive end bound with zero values
    assert_eq!(bytes[open_size + 1], INCL_END_BOUND);
}

#[test]
fn test_complete_partition_with_range_tombstone() {
    use crate::storage::write_engine::mutation::{ClusteringBound, RangeTombstone};

    let mut schema = create_test_schema();
    schema.clustering_keys = vec![ClusteringColumn {
        name: "ts".to_string(),
        data_type: "timestamp".to_string(),
        position: 0,
        order: ClusteringOrder::Asc,
    }];

    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x01]);
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // Create mutations
    let mutations = vec![Mutation::new(
        table_id,
        pk,
        Some(ClusteringKey::single("ts", Value::Timestamp(1000))),
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("Alice".to_string()),
        }],
        1001000,
        None,
    )];

    // Create range tombstone
    let range_tombstones = vec![RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ts", Value::Timestamp(500))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ts", Value::Timestamp(1500))),
        deletion_time: 1002000, // Later than row timestamp - will shadow it
        local_deletion_time: 1700000020,
    }];

    let offset = writer
        .write_partition(&key, &mutations, &schema, None, &range_tombstones)
        .unwrap();
    assert_eq!(offset, 0);

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Verify partition header is present (u16 BE key length)
    assert_eq!(&bytes[0..2], &[0x00, 0x04], "Key length (u16 BE)");

    // Range tombstone markers should appear before rows
    // This is validated by the structure of the output
}

#[test]
fn test_write_cell_with_ttl() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1000000;
    stats.min_local_deletion_time = 1700000000;
    stats.min_ttl = 3600;
    let writer = DataWriter::new(stats);

    let mut buf = Vec::new();
    let timestamp = 1001000;
    let ttl_seconds = 7200;

    writer
        .write_cell_with_ttl(
            &mut buf,
            "test_col",
            &Value::Text("test".to_string()),
            timestamp,
            ttl_seconds,
            None,
        )
        .unwrap();

    assert!(!buf.is_empty());

    // First byte should be CELL_IS_EXPIRING flag (0x02)
    let flags = buf[0];
    assert_eq!(
        flags & CELL_IS_EXPIRING,
        CELL_IS_EXPIRING,
        "Should have IS_EXPIRING flag"
    );
    assert_eq!(
        flags & CELL_USE_ROW_TIMESTAMP,
        0,
        "Should NOT have USE_ROW_TIMESTAMP flag"
    );
    assert_eq!(
        flags & CELL_USE_ROW_TTL,
        0,
        "Should NOT have USE_ROW_TTL flag"
    );

    // Should contain timestamp delta, local_deletion_time delta, TTL delta, and value
    assert!(buf.len() > 10, "Should have all TTL cell fields");
}

#[test]
fn test_row_with_ttl_cells() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1000000;
    stats.min_local_deletion_time = 1700000000;
    stats.min_ttl = 3600;
    let mut writer = DataWriter::new(stats);
    let schema = create_test_schema();

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![
            CellOperation::WriteWithTtl {
                column: "name".to_string(),
                value: Value::Text("Alice".to_string()),
                ttl_seconds: 7200,
                local_deletion_time: None,
            },
            CellOperation::Write {
                column: "age".to_string(),
                value: Value::Integer(30),
            },
        ],
        1001000,
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
fn test_row_with_multiple_ttl_cells() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1000000;
    stats.min_local_deletion_time = 1700000000;
    stats.min_ttl = 1800;
    let mut writer = DataWriter::new(stats);
    let schema = create_test_schema();

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![
            CellOperation::WriteWithTtl {
                column: "name".to_string(),
                value: Value::Text("Alice".to_string()),
                ttl_seconds: 3600, // 1 hour
                local_deletion_time: None,
            },
            CellOperation::WriteWithTtl {
                column: "age".to_string(),
                value: Value::Integer(30),
                ttl_seconds: 7200, // 2 hours (different TTL)
                local_deletion_time: None,
            },
        ],
        1001000,
        None,
    );

    writer.write_row(&mutation, &schema).unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Verify both cells were written with their own TTLs
    // The exact validation would require parsing the binary format
}

#[test]
fn test_mixed_ttl_and_regular_cells() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1000000;
    stats.min_local_deletion_time = 1700000000;
    stats.min_ttl = 3600;
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
            CellOperation::WriteWithTtl {
                column: "age".to_string(),
                value: Value::Integer(30),
                ttl_seconds: 7200,
                local_deletion_time: None,
            },
        ],
        1001000,
        None,
    );

    writer.write_row(&mutation, &schema).unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Row should contain both regular and TTL cells
    let flags = bytes[0];
    assert_eq!(flags & ROW_HAS_TIMESTAMP, ROW_HAS_TIMESTAMP);
}

#[test]
fn test_ttl_zero_special_case() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1000000;
    stats.min_local_deletion_time = 1700000000;
    stats.min_ttl = 0;
    let writer = DataWriter::new(stats);

    let mut buf = Vec::new();
    let timestamp = 1001000;
    let ttl_seconds = 0; // Immediate expiration

    writer
        .write_cell_with_ttl(
            &mut buf,
            "test_col",
            &Value::Text("test".to_string()),
            timestamp,
            ttl_seconds,
            None,
        )
        .unwrap();

    assert!(!buf.is_empty());

    // Should have IS_EXPIRING flag even with TTL=0
    let flags = buf[0];
    assert_eq!(flags & CELL_IS_EXPIRING, CELL_IS_EXPIRING);
}

#[test]
fn test_ttl_statistics_tracking() {
    let mut stats = StatisticsMetadata::new();

    // Update with various TTL values
    stats.update_ttl(3600);
    stats.update_ttl(7200);
    stats.update_ttl(1800);
    stats.update_ttl(0); // TTL=0 should be ignored

    assert_eq!(stats.min_ttl, 1800, "min_ttl should be 1800");
    assert_eq!(stats.max_ttl, 7200, "max_ttl should be 7200");
}

#[test]
fn test_ttl_cell_with_null_value() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let mut buf = Vec::new();
    let result =
        writer.write_cell_with_ttl(&mut buf, "test_col", &Value::Null, 1001000, 3600, None);

    assert!(result.is_err(), "NULL values should return error");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("NULL values should not be written"));
}

#[test]
fn test_ttl_cell_local_deletion_time_calculation() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1000000;
    stats.min_local_deletion_time = 1700000000;
    stats.min_ttl = 3600;
    let writer = DataWriter::new(stats);

    let mut buf = Vec::new();
    let timestamp = 1001000;
    let ttl_seconds = 7200; // 2 hours

    // The local_deletion_time should be computed as current_time + ttl_seconds
    writer
        .write_cell_with_ttl(
            &mut buf,
            "test_col",
            &Value::Text("test".to_string()),
            timestamp,
            ttl_seconds,
            None,
        )
        .unwrap();

    assert!(!buf.is_empty());
    // Detailed validation would require parsing the encoded deltas
}

#[test]
fn test_row_ttl_uses_row_ttl_cell_flags() {
    // Regression: when a mutation carries a row-level TTL, every regular cell
    // should be encoded with CELL_IS_EXPIRING | CELL_USE_ROW_TIMESTAMP | CELL_USE_ROW_TTL.
    //
    // Previous implementation used a whole-buffer byte-scan to count how many bytes
    // equalled the flag value 0x1A. That was fragile because the LDT delta field is
    // derived from the wall clock and can produce bytes that collide with 0x1A in
    // roughly 1-2% of CI runs.  We now use a structural parse that walks the row
    // header and then reads each cell's flags byte at its exact offset.
    let mut stats = create_test_stats();
    stats.min_timestamp = 1001000;
    stats.min_ttl = 7200;
    stats.min_local_deletion_time = 1;
    let mut writer = DataWriter::new(stats);
    let schema = create_test_schema();

    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
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
        1001000,
        Some(7200),
    );

    writer.write_row(&mutation, &schema).unwrap();
    let bytes = writer.finish().unwrap();

    // Verify the row header flags first (non-structural byte is safe here).
    assert_eq!(
        bytes[0] & ROW_HAS_TTL,
        ROW_HAS_TTL,
        "row should have TTL flag"
    );

    // Structurally parse the row body to extract each cell's flags byte.
    // Cassandra sorts regular columns by (is_complex, name) — for simple columns,
    // this is plain alphabetical order.  The schema has "age" (int, 4 bytes fixed)
    // and "name" (text, variable), so "age" sorts before "name".
    let cell_flags = parse_simple_row_cell_flags(
        &bytes,
        &[CellValueSizing::Fixed(4), CellValueSizing::Variable],
    );

    let expected = CELL_IS_EXPIRING | CELL_USE_ROW_TIMESTAMP | CELL_USE_ROW_TTL;
    assert_eq!(
        cell_flags.len(),
        2,
        "should have parsed flags for both cells"
    );
    assert!(
        cell_flags.iter().all(|&f| f == expected),
        "expected both cells to inherit row TTL (flags 0x{:02X}), got: {:?}",
        expected,
        cell_flags
    );
}

#[test]
fn test_write_partition_emits_static_row_before_regular_rows() {
    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);
    let schema = create_static_test_schema();
    let key = DecoratedKey::new(1, vec![0, 0, 0, 1]);

    let static_mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "static_val".to_string(),
            value: Value::Text("static".to_string()),
        }],
        1001000,
        None,
    );
    let regular_mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![CellOperation::Write {
            column: "regular_val".to_string(),
            value: Value::Text("regular".to_string()),
        }],
        1002000,
        None,
    );

    writer
        .write_partition(
            &key,
            &[static_mutation, regular_mutation],
            &schema,
            None,
            &[],
        )
        .unwrap();
    let bytes = writer.finish().unwrap();

    let partition_header_len = 2 + key.key.len() + 4 + 8;
    assert_eq!(
        bytes[partition_header_len] & ROW_HAS_EXTENDED_FLAGS,
        ROW_HAS_EXTENDED_FLAGS
    );
    assert_eq!(bytes[partition_header_len + 1], EXTENDED_IS_STATIC);
}

/// Cassandra switches to large-subset encoding when the superset reaches 64 columns.
#[test]
fn test_column_subset_exactly_64_regular_columns_uses_large_subset_encoding() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    // Create schema with exactly 64 regular columns
    let columns: Vec<Column> = (0..64)
        .map(|i| Column {
            name: format!("col_{:03}", i),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        })
        .collect();

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // Only write col_0 and col_63, forcing the large-subset path.
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![
            CellOperation::Write {
                column: "col_000".to_string(),
                value: Value::Text("first".to_string()),
            },
            CellOperation::Write {
                column: "col_063".to_string(),
                value: Value::Text("last".to_string()),
            },
        ],
        1001000,
        None,
    );

    let mut buf = Vec::new();
    writer
        .write_column_bitmap(&mut buf, &mutation, &schema)
        .unwrap();

    // missing_count=62, then present indexes [0, 63]
    assert_eq!(buf, vec![62, 0, 63]);
}
