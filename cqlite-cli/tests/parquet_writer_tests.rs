//! Parquet Writer Unit Tests (Issue #281)
//!
//! Comprehensive tests for the Parquet writer covering all CQL types and edge cases.
//!
//! Epic: #276 (M3 Output Writers)
//! Depends on: #277 (Parquet Writer Core Implementation)
//!
//! # Test Coverage
//!
//! - All 27+ CQL type conversions
//! - Null handling
//! - Nested collections (list of maps, etc.)
//! - UDT serialization
//! - Large value handling (>1MB blobs)
//! - Column ordering matches metadata
//! - Compression verification (Snappy default)
//! - Roundtrip validation with arrow-rs
//! - Empty result sets
//! - Single row, many rows

#![cfg(feature = "state_machine")]

use arrow::array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, ListArray, MapArray, StringArray, TimestampMillisecondArray,
};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use cqlite_cli::config::OutputConfig;
use cqlite_cli::output::ParquetWriter;
use cqlite_core::query::{ColumnInfo, QueryMetadata, QueryResult, QueryRow};
use cqlite_core::types::{DataType, TombstoneInfo, TombstoneType, UdtField, UdtValue};
use cqlite_core::{RowKey, Value};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::error::Error as StdError;

// ============================================================================
// Helper Functions
// ============================================================================

fn default_config() -> OutputConfig {
    OutputConfig::default()
}

/// Helper to create a QueryResult with specified columns and row values
fn create_query_result(
    columns: Vec<(&str, DataType)>,
    rows: Vec<Vec<(&str, Value)>>,
) -> QueryResult {
    let columns_vec: Vec<ColumnInfo> = columns
        .iter()
        .enumerate()
        .map(|(pos, (name, data_type))| ColumnInfo {
            name: name.to_string(),
            data_type: data_type.clone(),
            nullable: true,
            position: pos,
            table_name: None,
            cql_type: None,
        })
        .collect();

    let metadata = QueryMetadata {
        columns: columns_vec.clone(),
        ..Default::default()
    };

    let result_rows: Vec<QueryRow> = rows
        .into_iter()
        .enumerate()
        .map(|(idx, row_values)| {
            let mut values = HashMap::new();
            for (col_name, value) in row_values {
                values.insert(col_name.to_string(), value);
            }
            QueryRow {
                values,
                key: RowKey::new(vec![idx as u8]),
                metadata: Default::default(),
            }
        })
        .collect();

    QueryResult {
        rows: result_rows,
        rows_affected: 0,
        execution_time_ms: 0,
        metadata,
    }
}

/// Helper to create a single-column, single-row QueryResult
fn create_single_value_result(col_name: &str, value: Value, data_type: DataType) -> QueryResult {
    create_query_result(vec![(col_name, data_type)], vec![vec![(col_name, value)]])
}

/// Helper to read Parquet bytes back into a RecordBatch for verification
fn read_parquet_back(bytes: &[u8]) -> Result<RecordBatch, Box<dyn StdError>> {
    let bytes = Bytes::copy_from_slice(bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let mut reader = builder.build()?;
    reader
        .next()
        .ok_or_else(|| "No batches in Parquet file".to_string())?
        .map_err(|e| Box::new(e) as Box<dyn StdError>)
}

/// Verify Parquet file has valid magic bytes (PAR1 at start and end)
fn verify_parquet_magic(bytes: &[u8]) {
    assert!(bytes.len() >= 8, "Parquet file too small");
    assert_eq!(&bytes[0..4], b"PAR1", "Should start with PAR1 magic bytes");
    assert_eq!(
        &bytes[bytes.len() - 4..],
        b"PAR1",
        "Should end with PAR1 magic bytes"
    );
}

// ============================================================================
// Empty Result Set Test (Issue #281)
// ============================================================================

#[test]
fn test_parquet_empty_result_set() {
    // Empty columns, empty rows - should produce valid Parquet
    let result = QueryResult::new();

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    // Empty Parquet has no batches, so we just verify valid structure
    assert!(bytes.len() >= 8, "Should produce valid Parquet file");
}

// ============================================================================
// Missing Primitive Type Tests (Issue #281)
// ============================================================================

#[test]
fn test_parquet_date_values() {
    // Date is stored as i32 days since epoch, handled via DataType::Integer
    // 19000 days since 1970-01-01 = ~2022-01-06
    let result = create_single_value_result("date_col", Value::Date(19000), DataType::Integer);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);

    let col = batch.column(0);
    let int_array = col.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(int_array.value(0), 19000);
}

#[test]
fn test_parquet_time_values() {
    // Time is stored as i64 nanoseconds since midnight, handled via DataType::BigInt
    // 10:00:00.000000000 = 36_000_000_000_000 nanoseconds
    let nanos_10am: i64 = 10 * 60 * 60 * 1_000_000_000;
    let result = create_single_value_result("time_col", Value::Time(nanos_10am), DataType::BigInt);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    let int64_array = col.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(int64_array.value(0), nanos_10am);
}

#[test]
fn test_parquet_json_values() {
    let json_value = serde_json::json!({
        "name": "test",
        "count": 42,
        "nested": {"key": "value"}
    });
    let result =
        create_single_value_result("json_col", Value::Json(json_value.clone()), DataType::Json);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
    let output_str = string_array.value(0);
    // Verify JSON structure is preserved (exact format may vary)
    assert!(output_str.contains("\"name\""));
    assert!(output_str.contains("\"test\""));
    assert!(output_str.contains("42"));
}

#[test]
fn test_parquet_varint_values() {
    // Varint is serialized as string via ValueFormatter fallback
    // Representing a large integer: 123456789012345678901234567890
    let varint_bytes = vec![0x00, 0x5E, 0xCE, 0x0E, 0x6A, 0xEB, 0xBC, 0x22, 0xD2, 0xD2];
    let result =
        create_single_value_result("varint_col", Value::Varint(varint_bytes), DataType::Text);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
    // Varint is serialized as a string representation
    assert!(!string_array.value(0).is_empty());
}

#[test]
fn test_parquet_decimal_values() {
    // Decimal with scale=2 and unscaled value representing 12345.67
    let result = create_single_value_result(
        "decimal_col",
        Value::Decimal {
            scale: 2,
            unscaled: vec![0x01, 0xE2, 0x40, 0x03], // 1234567 in big-endian
        },
        DataType::Text,
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
    // Decimal is serialized via ValueFormatter
    assert!(!string_array.value(0).is_empty());
}

// ============================================================================
// Complex Type Tests (Issue #281)
// ============================================================================

#[test]
fn test_parquet_duration_values() {
    // Duration with months, days, nanos
    // 1 month, 2 days, 3 hours (in nanos)
    let result = create_single_value_result(
        "duration_col",
        Value::Duration {
            months: 1,
            days: 2,
            nanos: 3 * 60 * 60 * 1_000_000_000,
        },
        DataType::Text,
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
    // Duration is serialized via ValueFormatter as string
    let duration_str = string_array.value(0);
    assert!(!duration_str.is_empty());
}

#[test]
fn test_parquet_frozen_values() {
    // Frozen wraps another value (immutable collection marker)
    let frozen_list = Value::Frozen(Box::new(Value::List(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ])));
    let result = create_single_value_result("frozen_col", frozen_list, DataType::Frozen);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
    let frozen_str = string_array.value(0);
    // Frozen is serialized via ValueFormatter
    assert!(frozen_str.contains("1") || frozen_str.contains("["));
}

// ============================================================================
// Collection Type Tests (Issue #281)
// ============================================================================

#[test]
fn test_parquet_set_values() {
    // Set is similar to List but with unique values
    let set_value = Value::Set(vec![
        Value::Text("apple".to_string()),
        Value::Text("banana".to_string()),
        Value::Text("cherry".to_string()),
    ]);
    let result = create_single_value_result("set_col", set_value, DataType::Set);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);

    // Set is converted to List in Parquet
    let col = batch.column(0);
    let list_array = col.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(!list_array.is_null(0));
}

#[test]
fn test_parquet_nested_collections() {
    // List of maps: list<map<text, int>>
    let nested = Value::List(vec![
        Value::Map(vec![
            (Value::Text("a".to_string()), Value::Integer(1)),
            (Value::Text("b".to_string()), Value::Integer(2)),
        ]),
        Value::Map(vec![(Value::Text("c".to_string()), Value::Integer(3))]),
    ]);
    let result = create_single_value_result("nested_col", nested, DataType::List);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    let list_array = col.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(!list_array.is_null(0));
    // Nested collections are serialized as strings within the list
    assert!(list_array.len() > 0);
}

#[test]
fn test_parquet_empty_collections() {
    // Test empty list, set, and map
    let result = create_query_result(
        vec![
            ("empty_list", DataType::List),
            ("empty_set", DataType::Set),
            ("empty_map", DataType::Map),
        ],
        vec![vec![
            ("empty_list", Value::List(vec![])),
            ("empty_set", Value::Set(vec![])),
            ("empty_map", Value::Map(vec![])),
        ]],
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 3);

    // Verify each empty collection column exists
    let list_col = batch.column(0);
    let list_array = list_col.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(!list_array.is_null(0));

    let set_col = batch.column(1);
    let set_array = set_col.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(!set_array.is_null(0));

    let map_col = batch.column(2);
    let map_array = map_col.as_any().downcast_ref::<MapArray>().unwrap();
    assert!(!map_array.is_null(0));
}

// ============================================================================
// UDT Tests (Issue #281)
// ============================================================================

#[test]
fn test_parquet_tuple_values() {
    // Tuple with heterogeneous types
    let tuple = Value::Tuple(vec![
        Value::Integer(42),
        Value::Text("hello".to_string()),
        Value::Boolean(true),
    ]);
    let result = create_single_value_result("tuple_col", tuple, DataType::Tuple);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
    let tuple_str = string_array.value(0);
    // Tuple is serialized as JSON/string via ValueFormatter
    assert!(tuple_str.contains("42") || tuple_str.contains("hello"));
}

#[test]
fn test_parquet_udt_values() {
    // User-defined type with named fields
    let udt = Value::Udt(UdtValue {
        keyspace: "test_ks".to_string(),
        type_name: "address".to_string(),
        fields: vec![
            UdtField {
                name: "street".to_string(),
                value: Some(Value::Text("123 Main St".to_string())),
            },
            UdtField {
                name: "city".to_string(),
                value: Some(Value::Text("Springfield".to_string())),
            },
            UdtField {
                name: "zip".to_string(),
                value: Some(Value::Integer(12345)),
            },
        ],
    });
    let result = create_single_value_result("udt_col", udt, DataType::Udt);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
    let udt_str = string_array.value(0);
    // UDT is serialized via ValueFormatter, should contain field values
    assert!(
        udt_str.contains("Main St")
            || udt_str.contains("Springfield")
            || udt_str.contains("street")
    );
}

#[test]
fn test_parquet_udt_with_collections() {
    // UDT containing a list field
    let udt_with_list = Value::Udt(UdtValue {
        keyspace: "test_ks".to_string(),
        type_name: "user_info".to_string(),
        fields: vec![
            UdtField {
                name: "name".to_string(),
                value: Some(Value::Text("Alice".to_string())),
            },
            UdtField {
                name: "tags".to_string(),
                value: Some(Value::List(vec![
                    Value::Text("admin".to_string()),
                    Value::Text("user".to_string()),
                ])),
            },
        ],
    });
    let result = create_single_value_result("udt_list_col", udt_with_list, DataType::Udt);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
    let udt_str = string_array.value(0);
    assert!(udt_str.contains("Alice") || udt_str.contains("admin"));
}

// ============================================================================
// Edge Case Tests (Issue #281)
// ============================================================================

#[test]
fn test_parquet_single_row() {
    // Verify single row produces valid Parquet
    let result = create_single_value_result("single", Value::Integer(42), DataType::Integer);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
}

#[test]
fn test_parquet_many_rows() {
    // Test with 1000+ rows
    let columns = vec![("id", DataType::Integer), ("value", DataType::Text)];
    let rows: Vec<Vec<(&str, Value)>> = (0..1000)
        .map(|i| {
            vec![
                ("id", Value::Integer(i)),
                ("value", Value::Text(format!("row_{i}"))),
            ]
        })
        .collect();

    let result = create_query_result(columns, rows);
    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1000);
    assert_eq!(batch.num_columns(), 2);

    // Verify first and last row values
    let id_col = batch.column(0);
    let id_array = id_col.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(id_array.value(0), 0);
    assert_eq!(id_array.value(999), 999);
}

#[test]
fn test_parquet_large_blob() {
    // Test with >1MB blob
    let large_blob = vec![0xAB; 1_500_000]; // 1.5 MB of 0xAB bytes
    let result = create_single_value_result(
        "large_blob",
        Value::Blob(large_blob.clone()),
        DataType::Blob,
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    let binary_array = col.as_any().downcast_ref::<BinaryArray>().unwrap();
    let read_blob = binary_array.value(0);
    assert_eq!(read_blob.len(), 1_500_000);
    assert_eq!(read_blob[0], 0xAB);
    assert_eq!(read_blob[1_499_999], 0xAB);
}

#[test]
fn test_parquet_column_order_matches_metadata() {
    // Verify column order in Parquet schema matches QueryMetadata order
    let result = create_query_result(
        vec![
            ("first", DataType::Integer),
            ("second", DataType::Text),
            ("third", DataType::Boolean),
            ("fourth", DataType::Float),
        ],
        vec![vec![
            ("first", Value::Integer(1)),
            ("second", Value::Text("two".to_string())),
            ("third", Value::Boolean(true)),
            ("fourth", Value::Float(4.0)),
        ]],
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    // Verify schema field order
    let schema = batch.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["first", "second", "third", "fourth"]);

    // Verify data in correct columns
    let first_col = batch.column(0);
    let int_array = first_col.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(int_array.value(0), 1);

    let second_col = batch.column(1);
    let str_array = second_col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(str_array.value(0), "two");

    let third_col = batch.column(2);
    let bool_array = third_col.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(bool_array.value(0));

    let fourth_col = batch.column(3);
    let float_array = fourth_col.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((float_array.value(0) - 4.0).abs() < f64::EPSILON);
}

#[test]
fn test_parquet_all_null_column() {
    // Column with only null values
    let result = create_query_result(
        vec![("nullable", DataType::Text)],
        vec![
            vec![("nullable", Value::Null)],
            vec![("nullable", Value::Null)],
            vec![("nullable", Value::Null)],
        ],
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 3);

    let col = batch.column(0);
    let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert!(!string_array.is_valid(0)); // All nulls
    assert!(!string_array.is_valid(1));
    assert!(!string_array.is_valid(2));
}

// ============================================================================
// Compression Test (Issue #281)
// ============================================================================

#[test]
fn test_parquet_compression_snappy_applied() {
    // Verify Snappy compression is applied by checking file is smaller than raw data
    // Create a result with repetitive data that compresses well
    let columns = vec![("data", DataType::Text)];
    let rows: Vec<Vec<(&str, Value)>> = (0..100)
        .map(|_| {
            vec![(
                "data",
                Value::Text(
                    "This is repetitive test data that should compress very well. ".repeat(10),
                ),
            )]
        })
        .collect();

    let result = create_query_result(columns, rows);
    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    // Raw data size: 100 rows * ~610 chars = ~61KB
    // Compressed with Snappy should be significantly smaller
    let raw_data_estimate = 100 * 610;
    assert!(
        bytes.len() < raw_data_estimate,
        "Parquet output ({} bytes) should be smaller than raw data (~{} bytes) due to Snappy compression",
        bytes.len(),
        raw_data_estimate
    );

    // Verify data is readable
    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 100);
}

// ============================================================================
// Roundtrip Validation Tests (Issue #281)
// ============================================================================

#[test]
fn test_parquet_roundtrip_all_primitive_types() {
    // Write all primitive types and verify they can be read back correctly
    let result = create_query_result(
        vec![
            ("bool_col", DataType::Boolean),
            ("tiny_col", DataType::TinyInt),
            ("small_col", DataType::SmallInt),
            ("int_col", DataType::Integer),
            ("big_col", DataType::BigInt),
            ("f32_col", DataType::Float32),
            ("f64_col", DataType::Float),
            ("text_col", DataType::Text),
            ("blob_col", DataType::Blob),
            ("ts_col", DataType::Timestamp),
            ("uuid_col", DataType::Uuid),
        ],
        vec![vec![
            ("bool_col", Value::Boolean(true)),
            ("tiny_col", Value::TinyInt(127)),
            ("small_col", Value::SmallInt(32767)),
            ("int_col", Value::Integer(2147483647)),
            ("big_col", Value::BigInt(9223372036854775807)),
            ("f32_col", Value::Float32(3.125)), // Use exact float, not approx constant
            ("f64_col", Value::Float(2.71)),    // Use exact float, not approx constant
            ("text_col", Value::Text("Hello, Parquet!".to_string())),
            ("blob_col", Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])),
            ("ts_col", Value::Timestamp(1673778645123)), // 2023-01-15 10:30:45.123 UTC
            (
                "uuid_col",
                Value::Uuid([
                    0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55,
                    0x66, 0x77, 0x88,
                ]),
            ),
        ]],
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 11);

    // Verify each column type and value
    let bool_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(bool_col.value(0));

    let tiny_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    assert_eq!(tiny_col.value(0), 127);

    let small_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int16Array>()
        .unwrap();
    assert_eq!(small_col.value(0), 32767);

    let int_col = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(int_col.value(0), 2147483647);

    let big_col = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(big_col.value(0), 9223372036854775807);

    let f32_col = batch
        .column(5)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert!((f32_col.value(0) - 3.125).abs() < 0.001);

    let f64_col = batch
        .column(6)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((f64_col.value(0) - 2.71).abs() < 0.00001);

    let text_col = batch
        .column(7)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(text_col.value(0), "Hello, Parquet!");

    let blob_col = batch
        .column(8)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(blob_col.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);

    let ts_col = batch
        .column(9)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(ts_col.value(0), 1673778645123);

    let uuid_col = batch
        .column(10)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(
        uuid_col.value(0),
        &[
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88
        ]
    );
}

#[test]
fn test_parquet_schema_matches_metadata() {
    // Verify Parquet schema field names and types match QueryMetadata
    let result = create_query_result(
        vec![
            ("alpha", DataType::Integer),
            ("beta", DataType::Text),
            ("gamma", DataType::Boolean),
        ],
        vec![vec![
            ("alpha", Value::Integer(1)),
            ("beta", Value::Text("test".to_string())),
            ("gamma", Value::Boolean(false)),
        ]],
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let schema = batch.schema();

    // Verify field count
    assert_eq!(schema.fields().len(), 3);

    // Verify field names in order
    assert_eq!(schema.field(0).name(), "alpha");
    assert_eq!(schema.field(1).name(), "beta");
    assert_eq!(schema.field(2).name(), "gamma");

    // Verify field types
    assert_eq!(
        schema.field(0).data_type(),
        &arrow::datatypes::DataType::Int32
    );
    assert_eq!(
        schema.field(1).data_type(),
        &arrow::datatypes::DataType::Utf8
    );
    assert_eq!(
        schema.field(2).data_type(),
        &arrow::datatypes::DataType::Boolean
    );
}

// ============================================================================
// Additional Edge Cases
// ============================================================================

#[test]
fn test_parquet_mixed_null_and_values() {
    // Mix of null and non-null values in same column
    let result = create_query_result(
        vec![("mixed", DataType::Integer)],
        vec![
            vec![("mixed", Value::Integer(1))],
            vec![("mixed", Value::Null)],
            vec![("mixed", Value::Integer(3))],
            vec![("mixed", Value::Null)],
            vec![("mixed", Value::Integer(5))],
        ],
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    assert_eq!(batch.num_rows(), 5);

    let col = batch.column(0);
    let int_array = col.as_any().downcast_ref::<Int32Array>().unwrap();

    assert!(int_array.is_valid(0));
    assert_eq!(int_array.value(0), 1);

    assert!(!int_array.is_valid(1)); // null

    assert!(int_array.is_valid(2));
    assert_eq!(int_array.value(2), 3);

    assert!(!int_array.is_valid(3)); // null

    assert!(int_array.is_valid(4));
    assert_eq!(int_array.value(4), 5);
}

#[test]
fn test_parquet_tombstone_value() {
    // Tombstone (deletion marker) should serialize as string
    let tombstone = Value::Tombstone(TombstoneInfo {
        deletion_time: 1673778645,
        tombstone_type: TombstoneType::CellTombstone,
        ttl: None,
        range_start: None,
        range_end: None,
    });
    let result = create_single_value_result("tombstone_col", tombstone, DataType::Tombstone);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0);
    let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
    // Tombstone is serialized via ValueFormatter
    let tombstone_str = string_array.value(0);
    assert!(!tombstone_str.is_empty());
}

#[test]
fn test_parquet_wide_table() {
    // Table with many columns (50+)
    // Use owned strings to avoid memory leak from Box::leak
    let column_names: Vec<String> = (0..50).map(|i| format!("col_{i}")).collect();

    let columns_vec: Vec<ColumnInfo> = column_names
        .iter()
        .enumerate()
        .map(|(pos, name)| ColumnInfo {
            name: name.clone(),
            data_type: DataType::Integer,
            nullable: true,
            position: pos,
            table_name: None,
            cql_type: None,
        })
        .collect();

    let metadata = QueryMetadata {
        columns: columns_vec.clone(),
        ..Default::default()
    };

    let mut values = HashMap::new();
    for (i, name) in column_names.iter().enumerate() {
        values.insert(name.clone(), Value::Integer(i as i32));
    }

    let row = QueryRow {
        values,
        key: RowKey::new(vec![0]),
        metadata: Default::default(),
    };

    let result = QueryResult {
        rows: vec![row],
        rows_affected: 0,
        execution_time_ms: 0,
        metadata,
    };

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    verify_parquet_magic(&bytes);

    let batch = read_parquet_back(&bytes).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 50);

    // Verify a few column values
    let col_0 = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(col_0.value(0), 0);

    let col_49 = batch
        .column(49)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(col_49.value(0), 49);
}
