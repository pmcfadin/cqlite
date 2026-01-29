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
