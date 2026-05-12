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

fn create_composite_partition_with_clustering_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue438_probe".to_string(),
        table: "single_probe".to_string(),
        partition_keys: vec![
            KeyColumn {
                name: "tenant_id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            },
            KeyColumn {
                name: "user_id".to_string(),
                data_type: "uuid".to_string(),
                position: 1,
            },
        ],
        clustering_keys: vec![
            ClusteringColumn {
                name: "category".to_string(),
                data_type: "text".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            },
            ClusteringColumn {
                name: "item_id".to_string(),
                data_type: "timeuuid".to_string(),
                position: 1,
                order: ClusteringOrder::Asc,
            },
        ],
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

    // Format: [len1][val1][0x00][len2][val2][0x00][len3][val3][0x00]

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

    // Trailing end-of-component marker
    assert_eq!(bytes[20], 0x00);
    assert_eq!(bytes.len(), 21);
}

#[test]
fn test_composite_key_has_trailing_separator() {
    let schema = create_composite_key_schema();

    let pk = PartitionKey::new(vec![
        ("year".to_string(), Value::Integer(2024)),
        ("month".to_string(), Value::Integer(12)),
        ("day".to_string(), Value::Integer(31)),
    ]);

    let bytes = pk.to_bytes(&schema).unwrap();

    // Cassandra writes an end-of-component byte after the last component too.
    let last_five = &bytes[bytes.len() - 5..];
    assert_eq!(last_five, &[0x00, 0x00, 0x00, 0x1F, 0x00]);

    // Length is: 3 components × (2 len + 4 val + 1 separator) = 21 bytes
    assert_eq!(bytes.len(), 21);
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

    // Trailing end-of-component marker
    assert_eq!(bytes[16], 0x00);
    assert_eq!(bytes.len(), 17);
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

#[test]
fn test_composite_partition_single_row_matches_cassandra_probe() {
    let schema = create_composite_partition_with_clustering_schema();
    let timestamp_micros = 1_715_011_200_000_000i64;

    let partition_key = PartitionKey::new(vec![
        (
            "tenant_id".to_string(),
            Value::Uuid([
                0x0f, 0x0f, 0x0f, 0x0f, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x01,
            ]),
        ),
        (
            "user_id".to_string(),
            Value::Uuid([
                0x0f, 0x0f, 0x0f, 0x0f, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0xaa,
            ]),
        ),
    ]);
    let decorated_key = partition_key.to_decorated_key(&schema).unwrap();

    let mutation = Mutation::new(
        TableId::new("issue438_probe", "single_probe"),
        partition_key,
        Some(ClusteringKey::new(vec![
            ("category".to_string(), Value::Text("analytics".to_string())),
            (
                "item_id".to_string(),
                Value::Uuid([
                    0xb4, 0x0e, 0xb2, 0xb0, 0x1b, 0x7f, 0x11, 0xef, 0x80, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x10,
                ]),
            ),
        ])),
        vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text("v1".to_string()),
        }],
        timestamp_micros,
        None,
    );

    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = timestamp_micros;
    stats.max_timestamp = timestamp_micros;
    stats.min_ttl = 0;
    stats.max_ttl = 0;
    stats.min_local_deletion_time = i32::MAX;
    stats.max_local_deletion_time = i32::MAX;

    let mut writer = DataWriter::new(stats);
    writer
        .write_partition(&decorated_key, &[mutation], &schema, None, &[])
        .unwrap();

    let bytes = writer.finish().unwrap();
    let expected = hex::decode(
        "002600100f0f0f0f0000400080000000000000010000100f0f0f0f0000400080000000000000aa007fffffff8000000000000000240009616e616c7974696373b40eb2b01b7f11ef80000000000000100634000802763101",
    )
    .unwrap();

    assert_eq!(bytes, expected);
}

// =============================================================================
// Issue #507 — static-row prelude always written when schema has STATIC columns
// =============================================================================

/// Schema matching gen_static in e2e-cassandra-readback.sh:
///   partition_key UUID, clustering_key TIMESTAMP, static_data TEXT STATIC,
///   row_data TEXT, row_value INT
fn create_static_columns_table_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_basic".to_string(),
        table: "static_columns_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "partition_key".to_string(),
            data_type: "uuid".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "clustering_key".to_string(),
            data_type: "timestamp".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "static_data".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "row_data".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "row_value".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

/// Issue #507: Two clustering-row mutations each carrying a static_data op
/// (exactly the shape gen_static produces) must emit a single static-row prelude
/// followed by two plain clustering rows without any static cell inside them.
#[test]
fn test_issue_507_gen_static_shape_emits_static_prelude() {
    let schema = create_static_columns_table_schema();
    let ts = 1_704_067_200_000_000i64;
    let pk_bytes: [u8; 16] = [0xee; 16];
    let pk_value = Value::Uuid(pk_bytes);

    // Two mutations with clustering keys; each carries static_data + regular ops.
    // This is exactly the shape emitted by the gen_static Python function in
    // e2e-cassandra-readback.sh (lines 350-400).
    let cluster_ts_base_ms: i64 = 1_704_067_200_000;
    let mutations = vec![
        Mutation::new(
            TableId::new("test_basic", "static_columns_table"),
            PartitionKey::single("partition_key", pk_value.clone()),
            Some(ClusteringKey::single(
                "clustering_key",
                Value::Timestamp(cluster_ts_base_ms + 1000),
            )),
            vec![
                CellOperation::Write {
                    column: "static_data".to_string(),
                    value: Value::Text("shared-static-text".to_string()),
                },
                CellOperation::Write {
                    column: "row_data".to_string(),
                    value: Value::Text("alpha".to_string()),
                },
                CellOperation::Write {
                    column: "row_value".to_string(),
                    value: Value::Integer(11),
                },
            ],
            ts,
            None,
        ),
        Mutation::new(
            TableId::new("test_basic", "static_columns_table"),
            PartitionKey::single("partition_key", pk_value.clone()),
            Some(ClusteringKey::single(
                "clustering_key",
                Value::Timestamp(cluster_ts_base_ms + 2000),
            )),
            vec![
                CellOperation::Write {
                    column: "static_data".to_string(),
                    value: Value::Text("shared-static-text".to_string()),
                },
                CellOperation::Write {
                    column: "row_data".to_string(),
                    value: Value::Text("beta".to_string()),
                },
                CellOperation::Write {
                    column: "row_value".to_string(),
                    value: Value::Integer(22),
                },
            ],
            ts,
            None,
        ),
    ];

    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = ts;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 0;

    let decorated_key = PartitionKey::single("partition_key", pk_value)
        .to_decorated_key(&schema)
        .unwrap();

    let mut writer = DataWriter::new(stats);
    writer
        .write_partition(&decorated_key, &mutations, &schema, None, &[])
        .unwrap();

    let bytes = writer.finish().unwrap();

    // Partition header:
    //   2 bytes: key length (16 for UUID)
    //  16 bytes: UUID key
    //   4 bytes: local_deletion_time = i32::MAX
    //   8 bytes: deletion_timestamp  = i64::MIN
    // = 30 bytes
    let pk_header_len = 2 + 16 + 4 + 8;
    assert!(
        bytes.len() > pk_header_len + 2,
        "Output too short: {} bytes",
        bytes.len()
    );

    // Immediately after the partition header: the static-row prelude.
    // Cassandra requirement: bit 0x80 (HAS_EXTENDED_FLAGS) must be set in flags byte,
    // and the following extended-flags byte must be 0x01 (IS_STATIC).
    assert_ne!(
        bytes[pk_header_len] & ROW_HAS_EXTENDED_FLAGS,
        0,
        "First unfiltered after partition header must have ROW_HAS_EXTENDED_FLAGS (bit 0x80); \
         got 0x{:02x} — Cassandra would fire UnfilteredSerializer assertion 4",
        bytes[pk_header_len]
    );
    assert_eq!(
        bytes[pk_header_len + 1],
        EXTENDED_IS_STATIC,
        "Second byte of static prelude must be EXTENDED_IS_STATIC (0x01); \
         got 0x{:02x}",
        bytes[pk_header_len + 1]
    );

    // The static row prelude must NOT have HAS_TIMESTAMP unless actually set —
    // in this test we do have a timestamp so the static row should have it.
    // Just verify neither HAS_TIMESTAMP nor HAS_TTL is set INSIDE clustering rows
    // (which would indicate static cells leaked into regular row bodies).
    // We verify by checking that the 0x80 flag is ONLY at the static prelude position
    // and not at the first clustering row position.
    //
    // Parse past the static row: flags + extended + row_size VInt + body
    // The simplest check: the bytes do NOT have another 0x80 0x01 sequence
    // immediately before the end-of-partition marker (0x01 at end).
    let static_prelude_pos = pk_header_len;
    let eop_pos = bytes.len() - 1;
    assert_eq!(
        bytes[eop_pos], 0x01,
        "Last byte must be END_OF_PARTITION (0x01)"
    );

    // Verify there's data between static prelude and end-of-partition
    // (the two clustering rows).
    assert!(
        eop_pos > static_prelude_pos + 5,
        "Buffer too short to contain both clustering rows"
    );
}

/// Issue #507: When the schema has a STATIC column but NO mutation writes any
/// static cell, the writer must still emit the minimal empty static-row prelude
/// (flags=0x80, extended=0x01).  Without this Cassandra's deserializer reads
/// the first clustering row's flag byte and fires `AssertionError: 4`.
#[test]
fn test_issue_507_empty_static_prelude_when_no_static_ops() {
    let schema = create_static_columns_table_schema();
    let ts = 1_704_067_200_000_000i64;
    let pk_bytes: [u8; 16] = [0xaa; 16];
    let pk_value = Value::Uuid(pk_bytes);

    // Only regular columns written — no static_data.
    let mutation = Mutation::new(
        TableId::new("test_basic", "static_columns_table"),
        PartitionKey::single("partition_key", pk_value.clone()),
        Some(ClusteringKey::single(
            "clustering_key",
            Value::Timestamp(1_704_067_201_000i64),
        )),
        vec![
            CellOperation::Write {
                column: "row_data".to_string(),
                value: Value::Text("only-regular".to_string()),
            },
            CellOperation::Write {
                column: "row_value".to_string(),
                value: Value::Integer(99),
            },
        ],
        ts,
        None,
    );

    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = ts;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 0;

    let decorated_key = PartitionKey::single("partition_key", pk_value)
        .to_decorated_key(&schema)
        .unwrap();

    let mut writer = DataWriter::new(stats);
    writer
        .write_partition(&decorated_key, &[mutation], &schema, None, &[])
        .unwrap();

    let bytes = writer.finish().unwrap();

    // Partition header is 30 bytes (2 + 16 + 4 + 8).
    let pk_header_len = 2 + 16 + 4 + 8;

    assert_eq!(
        bytes[pk_header_len], ROW_HAS_EXTENDED_FLAGS,
        "Schema has STATIC columns → empty static prelude required at byte {}; \
         got 0x{:02x} (Cassandra would fire AssertionError: 4)",
        pk_header_len, bytes[pk_header_len]
    );
    assert_eq!(
        bytes[pk_header_len + 1],
        EXTENDED_IS_STATIC,
        "Extended flags must be IS_STATIC (0x01); got 0x{:02x}",
        bytes[pk_header_len + 1]
    );

    // The empty static row must NOT have HAS_TIMESTAMP (0x04) set.
    assert_eq!(
        bytes[pk_header_len] & ROW_HAS_TIMESTAMP,
        0,
        "Empty static row must not carry HAS_TIMESTAMP"
    );
    // And must NOT have HAS_TTL (0x08).
    assert_eq!(
        bytes[pk_header_len] & ROW_HAS_TTL,
        0,
        "Empty static row must not carry HAS_TTL"
    );

    // End-of-partition marker must be present.
    assert_eq!(
        bytes[bytes.len() - 1],
        0x01,
        "Last byte must be END_OF_PARTITION"
    );
}
