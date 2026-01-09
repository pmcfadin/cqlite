//! Contract Stability Tests (Issue #265)
//!
//! These tests validate the stability of CQLite's public query result API contracts.
//! Any modification to the structure of these types will cause snapshot test failures,
//! ensuring breaking changes are caught during development.
//!
//! **Why These Tests Matter:**
//! - M3 (Output Writers) depends on these contract types for JSON/CSV/Parquet output
//! - M4 (Language Bindings) depends on these contracts for FFI and WASM interfaces
//! - Breaking changes here silently break ALL downstream consumers
//!
//! **Contract Types Protected:**
//! - `QueryResult` - Contains rows and metadata for query results
//! - `QueryRow` - Individual row with values HashMap and key
//! - `QueryMetadata` - Column info, performance metrics, warnings
//! - `ColumnInfo` - Column name, type, position, nullability
//! - `Value` enum - All 27 CQL type variants
//! - `PerformanceMetrics` - Timing and cache statistics
//!
//! The tests use insta for JSON snapshot testing. Snapshots are stored in:
//! `cqlite-core/tests/snapshots/`

use cqlite_core::query::result::{
    ColumnInfo, PerformanceMetrics, PlanInfo, QueryMetadata, QueryResult, QueryRow, RowMetadata,
};
use cqlite_core::types::{DataType, UdtValue};
use cqlite_core::{RowKey, Value};

// =============================================================================
// QueryResult Contract Tests
// =============================================================================

/// Tests that an empty QueryResult serializes to a stable JSON format.
/// This is the base case for query results.
#[test]
fn contract_query_result_empty() {
    let result = QueryResult::new();
    // Use sort_maps to ensure deterministic HashMap serialization order
    insta::with_settings!({sort_maps => true}, {
        insta::assert_json_snapshot!(result);
    });
}

/// Tests QueryResult with actual row data.
/// Verifies the structure of rows, values, and metadata.
#[test]
fn contract_query_result_with_rows() {
    let mut row = QueryRow::new(RowKey::new(vec![1, 2, 3]));
    row.set("id".to_string(), Value::Integer(42));
    row.set("name".to_string(), Value::Text("test".to_string()));

    let mut result = QueryResult::with_rows(vec![row]);
    result.metadata.columns = vec![
        ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
        ColumnInfo::new("name".to_string(), DataType::Text, true, 1),
    ];

    // Use sort_maps to ensure deterministic HashMap serialization order
    insta::with_settings!({sort_maps => true}, {
        insta::assert_json_snapshot!(result);
    });
}

/// Tests QueryResult for DML operations that return affected row count.
#[test]
fn contract_query_result_with_affected_rows() {
    let result = QueryResult::with_affected_rows(5);
    insta::with_settings!({sort_maps => true}, {
        insta::assert_json_snapshot!(result);
    });
}

// =============================================================================
// QueryRow Contract Tests
// =============================================================================

/// Tests a minimal QueryRow with just a key.
#[test]
fn contract_query_row_minimal() {
    let row = QueryRow::new(RowKey::new(vec![0xFF, 0x00]));
    insta::with_settings!({sort_maps => true}, {
        insta::assert_json_snapshot!(row);
    });
}

/// Tests QueryRow with metadata (version, TTL, tags).
#[test]
fn contract_query_row_with_metadata() {
    let mut row = QueryRow::new(RowKey::new(vec![1]));
    row.set("col1".to_string(), Value::Integer(1));
    row.set_metadata(
        RowMetadata::new()
            .with_version(12345)
            .with_ttl(3600)
            .with_tag("source".to_string(), "import".to_string()),
    );
    insta::with_settings!({sort_maps => true}, {
        insta::assert_json_snapshot!(row);
    });
}

// =============================================================================
// QueryMetadata Contract Tests
// =============================================================================

/// Tests the default QueryMetadata structure.
#[test]
fn contract_query_metadata_default() {
    let metadata = QueryMetadata::default();
    insta::assert_json_snapshot!(metadata);
}

/// Tests QueryMetadata with all fields populated.
#[test]
fn contract_query_metadata_full() {
    let metadata = QueryMetadata {
        columns: vec![ColumnInfo::new(
            "id".to_string(),
            DataType::Integer,
            false,
            0,
        )],
        total_rows: Some(100),
        plan_info: Some(PlanInfo {
            plan_type: "IndexScan".to_string(),
            estimated_cost: 1.5,
            actual_cost: 1.2,
            indexes_used: vec!["idx_id".to_string()],
            steps: vec!["scan".to_string(), "filter".to_string()],
            parallelization: None,
        }),
        performance: PerformanceMetrics::default(),
        warnings: vec!["test warning".to_string()],
    };
    insta::assert_json_snapshot!(metadata);
}

// =============================================================================
// ColumnInfo Contract Tests
// =============================================================================

/// Tests basic ColumnInfo structure.
#[test]
fn contract_column_info_basic() {
    let col = ColumnInfo::new("user_id".to_string(), DataType::Integer, false, 0);
    insta::assert_json_snapshot!(col);
}

/// Tests ColumnInfo with optional table name.
#[test]
fn contract_column_info_with_table() {
    let col = ColumnInfo::new("user_id".to_string(), DataType::Uuid, true, 0)
        .with_table_name("users".to_string());
    insta::assert_json_snapshot!(col);
}

// =============================================================================
// Value Enum Contract Tests (All 27 Variants)
//
// These tests verify that each Value variant serializes to a stable format.
// M3 Output Writers and M4 Language Bindings depend on this serialization.
// =============================================================================

#[test]
fn contract_value_null() {
    insta::assert_json_snapshot!(Value::Null);
}

#[test]
fn contract_value_boolean() {
    insta::assert_json_snapshot!(Value::Boolean(true));
}

#[test]
fn contract_value_integer() {
    insta::assert_json_snapshot!(Value::Integer(42));
}

#[test]
fn contract_value_bigint() {
    insta::assert_json_snapshot!(Value::BigInt(9223372036854775807i64));
}

#[test]
fn contract_value_counter() {
    insta::assert_json_snapshot!(Value::Counter(1000));
}

#[test]
fn contract_value_float() {
    // Use an arbitrary float value (not a mathematical constant to avoid clippy::approx_constant)
    insta::assert_json_snapshot!(Value::Float(1.23456));
}

#[test]
fn contract_value_text() {
    insta::assert_json_snapshot!(Value::Text("hello world".to_string()));
}

#[test]
fn contract_value_blob() {
    insta::assert_json_snapshot!(Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
}

#[test]
fn contract_value_timestamp() {
    // Fixed timestamp: 2024-01-01 00:00:00 UTC
    insta::assert_json_snapshot!(Value::Timestamp(1704067200000));
}

#[test]
fn contract_value_date() {
    // Days since Unix epoch for 2024-01-01
    insta::assert_json_snapshot!(Value::Date(19724));
}

#[test]
fn contract_value_time() {
    // Noon in nanoseconds since midnight
    insta::assert_json_snapshot!(Value::Time(43200000000000i64));
}

#[test]
fn contract_value_uuid() {
    // A fixed UUID for reproducibility
    insta::assert_json_snapshot!(Value::Uuid([
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00
    ]));
}

#[test]
fn contract_value_varint() {
    insta::assert_json_snapshot!(Value::Varint(vec![0x01, 0x00]));
}

#[test]
fn contract_value_decimal() {
    insta::assert_json_snapshot!(Value::Decimal {
        scale: 2,
        unscaled: vec![0x30, 0x39],
    });
}

#[test]
fn contract_value_duration() {
    insta::assert_json_snapshot!(Value::Duration {
        months: 1,
        days: 15,
        nanos: 3600000000000i64,
    });
}

#[test]
fn contract_value_json() {
    insta::assert_json_snapshot!(Value::Json(serde_json::json!({
        "key": "value",
        "number": 42
    })));
}

#[test]
fn contract_value_tinyint() {
    insta::assert_json_snapshot!(Value::TinyInt(127));
}

#[test]
fn contract_value_smallint() {
    insta::assert_json_snapshot!(Value::SmallInt(32767));
}

#[test]
fn contract_value_float32() {
    insta::assert_json_snapshot!(Value::Float32(2.5));
}

#[test]
fn contract_value_list() {
    insta::assert_json_snapshot!(Value::List(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]));
}

#[test]
fn contract_value_set() {
    insta::assert_json_snapshot!(Value::Set(vec![
        Value::Text("a".to_string()),
        Value::Text("b".to_string()),
    ]));
}

#[test]
fn contract_value_map() {
    insta::assert_json_snapshot!(Value::Map(vec![
        (Value::Text("key1".to_string()), Value::Integer(1)),
        (Value::Text("key2".to_string()), Value::Integer(2)),
    ]));
}

#[test]
fn contract_value_tuple() {
    insta::assert_json_snapshot!(Value::Tuple(vec![
        Value::Integer(1),
        Value::Text("test".to_string()),
        Value::Boolean(true),
    ]));
}

#[test]
fn contract_value_udt() {
    let udt = UdtValue::new("Person".to_string(), "test_keyspace".to_string())
        .with_field("name".to_string(), Some(Value::Text("John".to_string())))
        .with_field("age".to_string(), Some(Value::Integer(30)));
    insta::assert_json_snapshot!(Value::Udt(udt));
}

#[test]
fn contract_value_frozen() {
    insta::assert_json_snapshot!(Value::Frozen(Box::new(Value::List(vec![
        Value::Integer(1),
        Value::Integer(2),
    ]))));
}

#[test]
fn contract_value_tombstone() {
    insta::assert_json_snapshot!(Value::row_tombstone(1704067200000));
}

#[test]
fn contract_value_inet_ipv4() {
    insta::assert_json_snapshot!(Value::Inet(vec![192, 168, 1, 1]));
}

#[test]
fn contract_value_inet_ipv6() {
    insta::assert_json_snapshot!(Value::Inet(vec![
        0x20, 0x01, 0x0d, 0xb8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01,
    ]));
}

// =============================================================================
// Column Order Validation (Issue #129)
//
// This test verifies that QueryResult respects column order from metadata,
// NOT HashMap iteration order (which is non-deterministic).
// =============================================================================

#[test]
fn contract_column_order_from_metadata() {
    let mut row1 = QueryRow::new(RowKey::new(vec![1]));
    // Insert in non-alphabetical order to verify ordering
    row1.set("z_col".to_string(), Value::Integer(1));
    row1.set("a_col".to_string(), Value::Integer(2));
    row1.set("m_col".to_string(), Value::Integer(3));

    let mut result = QueryResult::with_rows(vec![row1]);
    // Define explicit column order: a, m, z (custom order)
    result.metadata.columns = vec![
        ColumnInfo::new("a_col".to_string(), DataType::Integer, false, 0),
        ColumnInfo::new("m_col".to_string(), DataType::Integer, false, 1),
        ColumnInfo::new("z_col".to_string(), DataType::Integer, false, 2),
    ];

    // Snapshot the serialized form - to_json() respects metadata column order
    let json = result.to_json();
    insta::with_settings!({sort_maps => true}, {
        insta::assert_json_snapshot!(json);
    });
}

// =============================================================================
// PlanInfo Contract Tests
// =============================================================================

#[test]
fn contract_plan_info() {
    let plan = PlanInfo {
        plan_type: "IndexScan".to_string(),
        estimated_cost: 1.5,
        actual_cost: 1.2,
        indexes_used: vec!["idx_primary".to_string()],
        steps: vec!["scan".to_string(), "filter".to_string()],
        parallelization: None,
    };
    insta::assert_json_snapshot!(plan);
}

// =============================================================================
// ParallelizationInfo and PartitionInfo Contract Tests
// =============================================================================

#[test]
fn contract_partition_info() {
    use cqlite_core::query::result::PartitionInfo;
    let info = PartitionInfo {
        id: 0,
        rows_processed: 5000,
        processing_time_ms: 250,
    };
    insta::assert_json_snapshot!(info);
}

#[test]
fn contract_parallelization_info() {
    use cqlite_core::query::result::{ParallelizationInfo, PartitionInfo};
    let info = ParallelizationInfo {
        threads_used: 4,
        effective: true,
        partitions: vec![
            PartitionInfo {
                id: 0,
                rows_processed: 1000,
                processing_time_ms: 50,
            },
            PartitionInfo {
                id: 1,
                rows_processed: 1200,
                processing_time_ms: 60,
            },
        ],
    };
    insta::assert_json_snapshot!(info);
}

// =============================================================================
// Performance Metrics Contract Tests
// =============================================================================

#[test]
fn contract_performance_metrics_default() {
    let metrics = PerformanceMetrics::default();
    insta::assert_json_snapshot!(metrics);
}

#[test]
fn contract_performance_metrics_populated() {
    let mut metrics = PerformanceMetrics::new();
    metrics.parse_time_us = 100;
    metrics.planning_time_us = 200;
    metrics.execution_time_us = 1000;
    metrics.total_time_us = 1300;
    metrics.memory_usage_bytes = 1048576;
    metrics.io_operations = 5;
    metrics.cache_hits = 8;
    metrics.cache_misses = 2;
    insta::assert_json_snapshot!(metrics);
}

// =============================================================================
// RowMetadata Contract Tests
// =============================================================================

#[test]
fn contract_row_metadata_empty() {
    let metadata = RowMetadata::new();
    insta::with_settings!({sort_maps => true}, {
        insta::assert_json_snapshot!(metadata);
    });
}

#[test]
fn contract_row_metadata_full() {
    let metadata = RowMetadata::new()
        .with_version(123456789)
        .with_ttl(86400)
        .with_tag("source".to_string(), "migration".to_string())
        .with_tag("batch".to_string(), "001".to_string());
    insta::with_settings!({sort_maps => true}, {
        insta::assert_json_snapshot!(metadata);
    });
}
