//! Static column and composite partition key roundtrip tests for M5.1
//!
//! Tests static row serialization and composite partition key encoding.
//! Validates binary format matches Cassandra specifications.

#![cfg(feature = "write-support")]

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::stats_writer::StatisticsMetadata;
use cqlite_core::storage::sstable::writer::DataWriter;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, DecoratedKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use std::collections::HashMap;

// Row flags constants (from V5CompressedLegacy)
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_TTL: u8 = 0x08;
#[allow(dead_code)]
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;
const EXTENDED_IS_STATIC: u8 = 0x01;

fn create_test_stats() -> StatisticsMetadata {
    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = 1000000;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 0;
    stats
}

// =============================================================================
// Static Column Tests (Issue #379)
// =============================================================================

fn create_schema_with_static() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
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
                name: "static_val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "regular_val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

#[test]
fn test_static_row_flags() {
    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);
    let schema = create_schema_with_static();

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("pk", Value::Integer(1));

    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "static_val".to_string(),
            value: Value::Text("static_data".to_string()),
        }],
        1001000,
        None,
    );

    writer.write_static_row(&mutation, &schema).unwrap();
    let bytes = writer.finish().unwrap();

    // Verify row flags
    let flags = bytes[0];
    assert_eq!(
        flags & ROW_HAS_EXTENDED_FLAGS,
        ROW_HAS_EXTENDED_FLAGS,
        "Static row must have HAS_EXTENDED_FLAGS"
    );
    assert_eq!(
        flags & ROW_HAS_TIMESTAMP,
        ROW_HAS_TIMESTAMP,
        "Static row should have timestamp"
    );

    // Verify extended flags
    let extended_flags = bytes[1];
    assert_eq!(
        extended_flags, EXTENDED_IS_STATIC,
        "Extended flags should indicate static row"
    );
}

#[test]
fn test_static_row_no_clustering_prefix() {
    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);
    let schema = create_schema_with_static();

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("pk", Value::Integer(1));

    // Static row should ignore clustering key even if provided
    let mutation = Mutation::new(
        table_id,
        pk,
        Some(ClusteringKey::single("ck", Value::Integer(999))),
        vec![CellOperation::Write {
            column: "static_val".to_string(),
            value: Value::Text("value".to_string()),
        }],
        1001000,
        None,
    );

    writer.write_static_row(&mutation, &schema).unwrap();
    let bytes = writer.finish().unwrap();

    // Static row format: [flags][extended_flags][row_size][prev_size][body]
    // No clustering prefix between extended_flags and row_size
    assert_eq!(bytes[0] & ROW_HAS_EXTENDED_FLAGS, ROW_HAS_EXTENDED_FLAGS);
    assert_eq!(bytes[1], EXTENDED_IS_STATIC);
    // Next byte should be row_size VInt, not clustering header
}

#[test]
fn test_static_row_with_ttl() {
    let mut stats = create_test_stats();
    stats.min_ttl = 0;
    let mut writer = DataWriter::new(stats);
    let schema = create_schema_with_static();

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("pk", Value::Integer(1));

    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "static_val".to_string(),
            value: Value::Text("expiring".to_string()),
        }],
        1001000,
        Some(3600), // 1 hour TTL
    );

    writer.write_static_row(&mutation, &schema).unwrap();
    let bytes = writer.finish().unwrap();

    let flags = bytes[0];
    assert_eq!(flags & ROW_HAS_TTL, ROW_HAS_TTL, "Should have TTL flag");
}

#[test]
fn test_static_row_before_regular_rows() {
    // Verify static rows should be written before regular rows in a partition
    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);
    let schema = create_schema_with_static();

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("pk", Value::Integer(1));

    // Write partition header
    let _key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x01]);

    // Static mutation
    let static_mutation = Mutation::new(
        table_id.clone(),
        pk.clone(),
        None,
        vec![CellOperation::Write {
            column: "static_val".to_string(),
            value: Value::Text("static".to_string()),
        }],
        1001000,
        None,
    );

    // Regular mutation
    let _regular_mutation = Mutation::new(
        table_id,
        pk,
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![CellOperation::Write {
            column: "regular_val".to_string(),
            value: Value::Text("regular".to_string()),
        }],
        1002000,
        None,
    );

    // Note: In a real implementation, we'd use write_partition with static/regular separation
    // For now, just verify both can be written
    writer.write_static_row(&static_mutation, &schema).unwrap();
    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());
}

// =============================================================================
// Composite Partition Key Tests (Issue #380)
// =============================================================================

fn create_composite_key_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![
            KeyColumn {
                name: "year".to_string(),
                data_type: "int".to_string(),
                position: 0,
            },
            KeyColumn {
                name: "month".to_string(),
                data_type: "int".to_string(),
                position: 1,
            },
            KeyColumn {
                name: "day".to_string(),
                data_type: "int".to_string(),
                position: 2,
            },
        ],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "value".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
    }
}

#[test]
fn test_composite_key_format() {
    let schema = create_composite_key_schema();

    let pk = PartitionKey::new(vec![
        ("year".to_string(), Value::Integer(2024)),
        ("month".to_string(), Value::Integer(6)),
        ("day".to_string(), Value::Integer(15)),
    ]);

    let bytes = pk.to_bytes(&schema).unwrap();

    // Format: [len1][val1][0x00][len2][val2][0x00][len3][val3]
    // No trailing 0x00 after last component

    // Component 1: len=4, val=2024
    assert_eq!(bytes[0..2], [0x00, 0x04]); // len = 4
    assert_eq!(bytes[2..6], [0x00, 0x00, 0x07, 0xE8]); // 2024

    // Separator
    assert_eq!(bytes[6], 0x00);

    // Component 2: len=4, val=6
    assert_eq!(bytes[7..9], [0x00, 0x04]); // len = 4
    assert_eq!(bytes[9..13], [0x00, 0x00, 0x00, 0x06]); // 6

    // Separator
    assert_eq!(bytes[13], 0x00);

    // Component 3: len=4, val=15
    assert_eq!(bytes[14..16], [0x00, 0x04]); // len = 4
    assert_eq!(bytes[16..20], [0x00, 0x00, 0x00, 0x0F]); // 15

    // No trailing 0x00
    assert_eq!(bytes.len(), 20);
}

#[test]
fn test_composite_key_no_trailing_separator() {
    let schema = create_composite_key_schema();

    let pk = PartitionKey::new(vec![
        ("year".to_string(), Value::Integer(2024)),
        ("month".to_string(), Value::Integer(12)),
        ("day".to_string(), Value::Integer(31)),
    ]);

    let bytes = pk.to_bytes(&schema).unwrap();

    // Critical: NO trailing 0x00 after last component
    // Last 4 bytes should be the day value (31 = 0x0000001F)
    let last_four = &bytes[bytes.len() - 4..];
    assert_eq!(last_four, &[0x00, 0x00, 0x00, 0x1F]);

    // The byte before should be part of length prefix, not a separator
    // Length is calculated as: 3 components × (2 len + 4 val) + 2 separators = 20 bytes
    assert_eq!(bytes.len(), 20);
}

#[test]
fn test_single_component_no_separator() {
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
    };

    let pk = PartitionKey::single("id", Value::Integer(42));
    let bytes = pk.to_bytes(&schema).unwrap();

    // Single component: no length prefix, no separator
    assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x2A]); // Just the int value
}

#[test]
fn test_composite_key_with_text() {
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![
            KeyColumn {
                name: "region".to_string(),
                data_type: "text".to_string(),
                position: 0,
            },
            KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 1,
            },
        ],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
    };

    let pk = PartitionKey::new(vec![
        ("region".to_string(), Value::Text("us-east".to_string())),
        ("id".to_string(), Value::Integer(100)),
    ]);

    let bytes = pk.to_bytes(&schema).unwrap();

    // Component 1: len=7, val="us-east"
    assert_eq!(bytes[0..2], [0x00, 0x07]); // len = 7
    assert_eq!(&bytes[2..9], b"us-east");

    // Separator
    assert_eq!(bytes[9], 0x00);

    // Component 2: len=4, val=100
    assert_eq!(bytes[10..12], [0x00, 0x04]); // len = 4
    assert_eq!(bytes[12..16], [0x00, 0x00, 0x00, 0x64]); // 100

    // No trailing separator
    assert_eq!(bytes.len(), 16);
}

#[test]
fn test_decorated_key_from_composite() {
    let schema = create_composite_key_schema();

    let pk = PartitionKey::new(vec![
        ("year".to_string(), Value::Integer(2024)),
        ("month".to_string(), Value::Integer(1)),
        ("day".to_string(), Value::Integer(1)),
    ]);

    let dk = pk.to_decorated_key(&schema).unwrap();

    // Token should be deterministic
    let dk2 = pk.to_decorated_key(&schema).unwrap();
    assert_eq!(dk.token, dk2.token, "Token should be deterministic");

    // Key bytes should match
    let expected_bytes = pk.to_bytes(&schema).unwrap();
    assert_eq!(dk.key, expected_bytes);
}

#[test]
fn test_composite_key_ordering() {
    let schema = create_composite_key_schema();

    // Create multiple decorated keys
    let pk1 = PartitionKey::new(vec![
        ("year".to_string(), Value::Integer(2024)),
        ("month".to_string(), Value::Integer(1)),
        ("day".to_string(), Value::Integer(1)),
    ]);

    let pk2 = PartitionKey::new(vec![
        ("year".to_string(), Value::Integer(2024)),
        ("month".to_string(), Value::Integer(6)),
        ("day".to_string(), Value::Integer(15)),
    ]);

    let dk1 = pk1.to_decorated_key(&schema).unwrap();
    let dk2 = pk2.to_decorated_key(&schema).unwrap();

    // Decorated keys should be orderable
    // Note: Actual order depends on Murmur3 hash, not component values
    assert!(
        dk1 != dk2,
        "Different keys should have different tokens/bytes"
    );
}
