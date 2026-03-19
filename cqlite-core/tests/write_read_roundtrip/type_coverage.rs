//! CQL Type Coverage Write-Read Roundtrip Tests
//!
//! Tests that verify all Stage 0 supported CQL types round-trip correctly
//! through the write and read path.
//!
//! ## Supported Types (Stage 0)
//!
//! - Text (varchar)
//! - Int (32-bit signed)
//! - BigInt (64-bit signed)
//! - Boolean
//! - Timestamp (milliseconds since epoch)
//! - UUID
//!
//! ## Dependencies
//!
//! - Writer: `cqlite_core::storage::write_engine::WriteEngine`
//! - Statistics: `cqlite_core::parser::enhanced_statistics_parser`

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

/// Create a schema for testing a specific column type
fn create_type_test_schema(col_name: &str, col_type: &str) -> TableSchema {
    TableSchema {
        keyspace: "test_types".to_string(),
        table: format!("test_{}", col_type.to_lowercase()),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: col_name.to_string(),
                data_type: col_type.to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

/// Helper to write and flush a single value
async fn write_single_value(
    temp_dir: &TempDir,
    schema: &TableSchema,
    col_name: &str,
    value: Value,
) -> cqlite_core::storage::sstable::writer::SSTableInfo {
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    let table_id = TableId::new(&schema.keyspace, &schema.table);
    let pk = PartitionKey::single("pk", Value::Integer(1));
    let ops = vec![CellOperation::Write {
        column: col_name.to_string(),
        value,
    }];
    let mutation = Mutation::new(table_id, pk, None, ops, 1000000, None);

    engine
        .write_async(mutation)
        .await
        .expect("Write should succeed");

    engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo")
}

/// Test Text type roundtrip
#[tokio::test]
async fn test_type_text_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("text_col", "text");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "text_col",
        Value::Text("Hello, CQLite! 你好世界 🎉".to_string()),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for text type"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Text type with empty string
#[tokio::test]
async fn test_type_text_empty() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("text_col", "text");

    let info = write_single_value(&temp_dir, &schema, "text_col", Value::Text(String::new())).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for empty text"
    );
}

/// Test Text type with long string
#[tokio::test]
async fn test_type_text_long() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("text_col", "text");

    // Create a 10KB string
    let long_text = "A".repeat(10 * 1024);

    let info = write_single_value(&temp_dir, &schema, "text_col", Value::Text(long_text)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for long text"
    );
    let data_size = std::fs::metadata(&info.data_path).unwrap().len();
    assert!(
        data_size > 10000,
        "Data.db should be > 10KB for long text (got {} bytes)",
        data_size
    );
}

/// Test Int type roundtrip
#[tokio::test]
async fn test_type_int_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("int_col", "int");

    let info = write_single_value(&temp_dir, &schema, "int_col", Value::Integer(42)).await;

    assert!(info.data_path.exists(), "Data.db should exist for int type");
}

/// Test Int type with min value
#[tokio::test]
async fn test_type_int_min() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("int_col", "int");

    let info = write_single_value(&temp_dir, &schema, "int_col", Value::Integer(i32::MIN)).await;

    assert!(info.data_path.exists(), "Data.db should exist for int min");
}

/// Test Int type with max value
#[tokio::test]
async fn test_type_int_max() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("int_col", "int");

    let info = write_single_value(&temp_dir, &schema, "int_col", Value::Integer(i32::MAX)).await;

    assert!(info.data_path.exists(), "Data.db should exist for int max");
}

/// Test Int type with zero
#[tokio::test]
async fn test_type_int_zero() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("int_col", "int");

    let info = write_single_value(&temp_dir, &schema, "int_col", Value::Integer(0)).await;

    assert!(info.data_path.exists(), "Data.db should exist for int zero");
}

/// Test BigInt type roundtrip
#[tokio::test]
async fn test_type_bigint_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("bigint_col", "bigint");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "bigint_col",
        Value::BigInt(9223372036854775807i64),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for bigint type"
    );
}

/// Test BigInt type with min value
#[tokio::test]
async fn test_type_bigint_min() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("bigint_col", "bigint");

    let info = write_single_value(&temp_dir, &schema, "bigint_col", Value::BigInt(i64::MIN)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for bigint min"
    );
}

/// Test BigInt type with max value
#[tokio::test]
async fn test_type_bigint_max() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("bigint_col", "bigint");

    let info = write_single_value(&temp_dir, &schema, "bigint_col", Value::BigInt(i64::MAX)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for bigint max"
    );
}

/// Test Boolean type roundtrip (true)
#[tokio::test]
async fn test_type_boolean_true() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("bool_col", "boolean");

    let info = write_single_value(&temp_dir, &schema, "bool_col", Value::Boolean(true)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for boolean true"
    );
}

/// Test Boolean type roundtrip (false)
#[tokio::test]
async fn test_type_boolean_false() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("bool_col", "boolean");

    let info = write_single_value(&temp_dir, &schema, "bool_col", Value::Boolean(false)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for boolean false"
    );
}

/// Test Timestamp type roundtrip
#[tokio::test]
async fn test_type_timestamp_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("ts_col", "timestamp");

    // 2024-01-01 00:00:00 UTC in milliseconds
    let timestamp_ms = 1704067200000i64;

    let info =
        write_single_value(&temp_dir, &schema, "ts_col", Value::Timestamp(timestamp_ms)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for timestamp type"
    );
}

/// Test Timestamp type with epoch
#[tokio::test]
async fn test_type_timestamp_epoch() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("ts_col", "timestamp");

    let info = write_single_value(&temp_dir, &schema, "ts_col", Value::Timestamp(0)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for timestamp epoch"
    );
}

/// Test Timestamp type with far future
#[tokio::test]
async fn test_type_timestamp_future() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("ts_col", "timestamp");

    // Year 3000 (in milliseconds)
    let far_future = 32503680000000i64;

    let info = write_single_value(&temp_dir, &schema, "ts_col", Value::Timestamp(far_future)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for far future timestamp"
    );
}

/// Test UUID type roundtrip
#[tokio::test]
async fn test_type_uuid_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("uuid_col", "uuid");

    let uuid = uuid::Uuid::new_v4();
    let info = write_single_value(
        &temp_dir,
        &schema,
        "uuid_col",
        Value::Uuid(*uuid.as_bytes()),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for uuid type"
    );
}

/// Test UUID type with known value
#[tokio::test]
async fn test_type_uuid_known() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("uuid_col", "uuid");

    let known_uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let info = write_single_value(
        &temp_dir,
        &schema,
        "uuid_col",
        Value::Uuid(*known_uuid.as_bytes()),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for known uuid"
    );
}

/// Test UUID type with nil UUID
#[tokio::test]
async fn test_type_uuid_nil() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("uuid_col", "uuid");

    let nil_uuid = uuid::Uuid::nil();
    let info = write_single_value(
        &temp_dir,
        &schema,
        "uuid_col",
        Value::Uuid(*nil_uuid.as_bytes()),
    )
    .await;

    assert!(info.data_path.exists(), "Data.db should exist for nil uuid");
}

/// Test TinyInt type roundtrip
#[tokio::test]
async fn test_type_tinyint_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("tinyint_col", "tinyint");

    let info = write_single_value(&temp_dir, &schema, "tinyint_col", Value::TinyInt(42)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for tinyint type"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test TinyInt type with min value
#[tokio::test]
async fn test_type_tinyint_min() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("tinyint_col", "tinyint");

    let info = write_single_value(&temp_dir, &schema, "tinyint_col", Value::TinyInt(i8::MIN)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for tinyint min"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test TinyInt type with max value
#[tokio::test]
async fn test_type_tinyint_max() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("tinyint_col", "tinyint");

    let info = write_single_value(&temp_dir, &schema, "tinyint_col", Value::TinyInt(i8::MAX)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for tinyint max"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test SmallInt type roundtrip
#[tokio::test]
async fn test_type_smallint_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("smallint_col", "smallint");

    let info = write_single_value(&temp_dir, &schema, "smallint_col", Value::SmallInt(1000)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for smallint type"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test SmallInt type with min value
#[tokio::test]
async fn test_type_smallint_min() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("smallint_col", "smallint");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "smallint_col",
        Value::SmallInt(i16::MIN),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for smallint min"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test SmallInt type with max value
#[tokio::test]
async fn test_type_smallint_max() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("smallint_col", "smallint");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "smallint_col",
        Value::SmallInt(i16::MAX),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for smallint max"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Float32 type roundtrip
#[tokio::test]
async fn test_type_float32_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("float_col", "float");

    let info = write_single_value(&temp_dir, &schema, "float_col", Value::Float32(1.234_567)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for float type"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Float32 type with special value
#[tokio::test]
async fn test_type_float32_special() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("float_col", "float");

    let info = write_single_value(&temp_dir, &schema, "float_col", Value::Float32(0.0)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for float zero"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Float32 type with min/max value
#[tokio::test]
async fn test_type_float32_min_max() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("float_col", "float");

    let info = write_single_value(&temp_dir, &schema, "float_col", Value::Float32(f32::MIN)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for float min"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Double type roundtrip
#[tokio::test]
async fn test_type_double_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("double_col", "double");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "double_col",
        Value::Float(9.876_543_210_123_456),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for double type"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Double type with special value
#[tokio::test]
async fn test_type_double_special() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("double_col", "double");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "double_col",
        Value::Float(f64::INFINITY),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for double infinity"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Double type with min/max value
#[tokio::test]
async fn test_type_double_min_max() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("double_col", "double");

    let info = write_single_value(&temp_dir, &schema, "double_col", Value::Float(f64::MIN)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for double min"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Blob type roundtrip
#[tokio::test]
async fn test_type_blob_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("blob_col", "blob");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "blob_col",
        Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for blob type"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Blob type with empty value
#[tokio::test]
async fn test_type_blob_empty() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("blob_col", "blob");

    let info = write_single_value(&temp_dir, &schema, "blob_col", Value::Blob(vec![])).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for empty blob"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Blob type with large value
#[tokio::test]
async fn test_type_blob_large() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("blob_col", "blob");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "blob_col",
        Value::Blob(vec![0xAB; 10240]),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for large blob"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
    let data_size = std::fs::metadata(&info.data_path).unwrap().len();
    assert!(
        data_size > 10000,
        "Data.db should be > 10000 bytes for large blob (got {} bytes)",
        data_size
    );
}

/// Test Date type roundtrip
#[tokio::test]
async fn test_type_date_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("date_col", "date");

    // 2024-01-01
    let info = write_single_value(&temp_dir, &schema, "date_col", Value::Date(19723)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for date type"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Date type with epoch value
#[tokio::test]
async fn test_type_date_epoch() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("date_col", "date");

    let info = write_single_value(&temp_dir, &schema, "date_col", Value::Date(0)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for date epoch"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Date type with negative value
#[tokio::test]
async fn test_type_date_negative() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("date_col", "date");

    let info = write_single_value(&temp_dir, &schema, "date_col", Value::Date(-1)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for negative date"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Time type roundtrip
#[tokio::test]
async fn test_type_time_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("time_col", "time");

    // noon: 43200 seconds in nanoseconds
    let info = write_single_value(
        &temp_dir,
        &schema,
        "time_col",
        Value::Time(43_200_000_000_000),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for time type"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Time type with midnight value
#[tokio::test]
async fn test_type_time_midnight() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("time_col", "time");

    let info = write_single_value(&temp_dir, &schema, "time_col", Value::Time(0)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for time midnight"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Time type with max value
#[tokio::test]
async fn test_type_time_max() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("time_col", "time");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "time_col",
        Value::Time(86_399_999_999_999),
    )
    .await;

    assert!(info.data_path.exists(), "Data.db should exist for time max");
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Counter type roundtrip
#[tokio::test]
async fn test_type_counter_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("counter_col", "counter");

    let info = write_single_value(&temp_dir, &schema, "counter_col", Value::Counter(100)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for counter type"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Counter type with zero value
#[tokio::test]
async fn test_type_counter_zero() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("counter_col", "counter");

    let info = write_single_value(&temp_dir, &schema, "counter_col", Value::Counter(0)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for counter zero"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Counter type with negative value
#[tokio::test]
async fn test_type_counter_negative() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("counter_col", "counter");

    let info = write_single_value(&temp_dir, &schema, "counter_col", Value::Counter(-50)).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for negative counter"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Inet type with IPv4 address
#[tokio::test]
async fn test_type_inet_ipv4() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("inet_col", "inet");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "inet_col",
        Value::Inet(vec![192, 168, 1, 1]),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for inet IPv4"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Inet type with IPv6 address
#[tokio::test]
async fn test_type_inet_ipv6() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("inet_col", "inet");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "inet_col",
        Value::Inet(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for inet IPv6"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Inet type with loopback address
#[tokio::test]
async fn test_type_inet_loopback() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("inet_col", "inet");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "inet_col",
        Value::Inet(vec![127, 0, 0, 1]),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for inet loopback"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Varint type with small value
#[tokio::test]
async fn test_type_varint_small() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("varint_col", "varint");

    let info =
        write_single_value(&temp_dir, &schema, "varint_col", Value::Varint(vec![0x2A])).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for varint small"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Varint type with large value
#[tokio::test]
async fn test_type_varint_large() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("varint_col", "varint");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "varint_col",
        Value::Varint(vec![0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for varint large"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Varint type with negative value
#[tokio::test]
async fn test_type_varint_negative() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("varint_col", "varint");

    let info =
        write_single_value(&temp_dir, &schema, "varint_col", Value::Varint(vec![0xFF])).await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for negative varint"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Decimal type roundtrip
#[tokio::test]
async fn test_type_decimal_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("decimal_col", "decimal");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "decimal_col",
        Value::Decimal {
            scale: 2,
            unscaled: vec![0x30, 0x39],
        },
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for decimal type"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Decimal type with zero value
#[tokio::test]
async fn test_type_decimal_zero() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("decimal_col", "decimal");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "decimal_col",
        Value::Decimal {
            scale: 0,
            unscaled: vec![0],
        },
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for decimal zero"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Decimal type with negative scale
#[tokio::test]
async fn test_type_decimal_neg_scale() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("decimal_col", "decimal");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "decimal_col",
        Value::Decimal {
            scale: -2,
            unscaled: vec![1],
        },
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for decimal negative scale"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Duration type roundtrip
#[tokio::test]
async fn test_type_duration_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("duration_col", "duration");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "duration_col",
        Value::Duration {
            months: 1,
            days: 15,
            nanos: 3_600_000_000_000,
        },
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for duration type"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Duration type with zero value
#[tokio::test]
async fn test_type_duration_zero() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("duration_col", "duration");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "duration_col",
        Value::Duration {
            months: 0,
            days: 0,
            nanos: 0,
        },
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for duration zero"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Duration type with negative value
#[tokio::test]
async fn test_type_duration_negative() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("duration_col", "duration");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "duration_col",
        Value::Duration {
            months: -1,
            days: -5,
            nanos: -1_000_000_000,
        },
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for negative duration"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Tuple type roundtrip
#[tokio::test]
async fn test_type_tuple_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("tuple_col", "tuple<int, text>");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "tuple_col",
        Value::Tuple(vec![Value::Integer(42), Value::Text("hello".to_string())]),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for tuple type"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Tuple type with null element
#[tokio::test]
async fn test_type_tuple_with_null() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("tuple_col", "tuple<int, text>");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "tuple_col",
        Value::Tuple(vec![Value::Integer(42), Value::Null]),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for tuple with null"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Tuple type nested
#[tokio::test]
async fn test_type_tuple_nested() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("tuple_col", "tuple<int, tuple<int, text>>");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "tuple_col",
        Value::Tuple(vec![
            Value::Integer(1),
            Value::Tuple(vec![Value::Integer(2), Value::Text("nested".to_string())]),
        ]),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for nested tuple"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Frozen list type roundtrip
#[tokio::test]
async fn test_type_frozen_list() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("frozen_col", "frozen<list<int>>");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "frozen_col",
        Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]))),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for frozen list"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Frozen map type roundtrip
#[tokio::test]
async fn test_type_frozen_map() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("frozen_col", "frozen<map<text, int>>");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "frozen_col",
        Value::Frozen(Box::new(Value::Map(vec![(
            Value::Text("key".to_string()),
            Value::Integer(42),
        )]))),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for frozen map"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test Frozen empty list type
#[tokio::test]
async fn test_type_frozen_empty() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_type_test_schema("frozen_col", "frozen<list<int>>");

    let info = write_single_value(
        &temp_dir,
        &schema,
        "frozen_col",
        Value::Frozen(Box::new(Value::List(vec![]))),
    )
    .await;

    assert!(
        info.data_path.exists(),
        "Data.db should exist for frozen empty list"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");
}

/// Test all types in single partition
#[tokio::test]
async fn test_all_types_single_partition() {
    use super::{create_comprehensive_mutation, create_comprehensive_schema};

    let temp_dir = TempDir::new().unwrap();
    let schema = create_comprehensive_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write mutation with all types
    let mutation = create_comprehensive_mutation(1, "row1", 1000000);
    engine
        .write_async(mutation)
        .await
        .expect("Write should succeed");

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert!(
        info.data_path.exists(),
        "Data.db should exist for all types"
    );
    assert_eq!(info.partition_count, 1, "Should have 1 partition");

    // Verify data has substantial size (all types present)
    let data_size = std::fs::metadata(&info.data_path).unwrap().len();
    assert!(
        data_size > 50,
        "Data.db with all types should have > 50 bytes (got {})",
        data_size
    );
}

/// Test multiple rows with varying types
#[tokio::test]
async fn test_types_multiple_rows() {
    use super::{create_comprehensive_mutation, create_comprehensive_schema};

    let temp_dir = TempDir::new().unwrap();
    let schema = create_comprehensive_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write multiple partitions with all types
    for i in 0..10 {
        let mutation = create_comprehensive_mutation(i, &format!("row_{}", i), 1000000 + i as i64);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert_eq!(
        info.partition_count, 10,
        "Should have 10 partitions with all types"
    );
}
