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
    Array, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, MapArray, StringArray,
    TimestampMillisecondArray,
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

// ============================================================================
// Issue #675: High-fidelity scalar Arrow types
// ============================================================================

/// Helper to build a ColumnInfo with both data_type and cql_type set.
fn col_with_cql_type(
    name: &str,
    data_type: DataType,
    cql_type: cqlite_core::schema::CqlType,
) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable: true,
        position: 0,
        table_name: None,
        cql_type: Some(cql_type),
    }
}

/// Create a QueryResult from a single ColumnInfo and a single value.
fn single_cql_typed_result(col: ColumnInfo, value: Value) -> QueryResult {
    let mut values = HashMap::new();
    values.insert(col.name.clone(), value);
    let row = QueryRow {
        values,
        key: RowKey::new(vec![1]),
        metadata: Default::default(),
    };
    QueryResult {
        rows: vec![row],
        rows_affected: 0,
        execution_time_ms: 0,
        metadata: cqlite_core::query::QueryMetadata {
            columns: vec![col],
            ..Default::default()
        },
    }
}

// ----------------------------------------
// CQL date → Arrow Date32
// ----------------------------------------

#[test]
fn test_cql_date_maps_to_date32_schema() {
    use arrow::datatypes::DataType as ArrowDataType;

    let col = col_with_cql_type(
        "birth_date",
        DataType::Integer,
        cqlite_core::schema::CqlType::Date,
    );
    let result = single_cql_typed_result(col, Value::Date(19358)); // 2023-01-01

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    assert_eq!(batch.num_rows(), 1);
    // Schema field must be Date32
    assert_eq!(batch.schema().field(0).data_type(), &ArrowDataType::Date32);
}

#[test]
fn test_cql_date_roundtrip() {
    use arrow::array::Date32Array;

    let col = col_with_cql_type("d", DataType::Integer, cqlite_core::schema::CqlType::Date);
    // 19358 = days between 1970-01-01 and 2023-01-01
    let result = single_cql_typed_result(col, Value::Date(19358));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(arr.value(0), 19358);
}

#[test]
fn test_cql_date_null() {
    use arrow::array::Date32Array;

    let col = col_with_cql_type("d", DataType::Integer, cqlite_core::schema::CqlType::Date);
    let result = single_cql_typed_result(col, Value::Null);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert!(!arr.is_valid(0));
}

#[test]
fn test_cql_date_epoch() {
    use arrow::array::Date32Array;
    use arrow::datatypes::DataType as ArrowDataType;

    let col = col_with_cql_type("d", DataType::Integer, cqlite_core::schema::CqlType::Date);
    // 0 = 1970-01-01 epoch
    let result = single_cql_typed_result(col, Value::Date(0));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    assert_eq!(batch.schema().field(0).data_type(), &ArrowDataType::Date32);
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(arr.value(0), 0);
}

// ----------------------------------------
// CQL time → Arrow Time64(Nanosecond)
// ----------------------------------------

#[test]
fn test_cql_time_maps_to_time64_ns_schema() {
    use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};

    let col = col_with_cql_type("t", DataType::BigInt, cqlite_core::schema::CqlType::Time);
    let nanos_10am: i64 = 10 * 3600 * 1_000_000_000;
    let result = single_cql_typed_result(col, Value::Time(nanos_10am));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    assert_eq!(
        batch.schema().field(0).data_type(),
        &ArrowDataType::Time64(TimeUnit::Nanosecond)
    );
}

#[test]
fn test_cql_time_roundtrip() {
    use arrow::array::Time64NanosecondArray;

    let col = col_with_cql_type("t", DataType::BigInt, cqlite_core::schema::CqlType::Time);
    let nanos = 10 * 3600 * 1_000_000_000i64 + 30 * 60 * 1_000_000_000 + 45_123_456_789;
    let result = single_cql_typed_result(col, Value::Time(nanos));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()
        .unwrap();
    assert_eq!(arr.value(0), nanos);
}

#[test]
fn test_cql_time_null() {
    use arrow::array::Time64NanosecondArray;

    let col = col_with_cql_type("t", DataType::BigInt, cqlite_core::schema::CqlType::Time);
    let result = single_cql_typed_result(col, Value::Null);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()
        .unwrap();
    assert!(!arr.is_valid(0));
}

// ----------------------------------------
// CQL decimal → Arrow Decimal128
// ----------------------------------------

#[test]
fn test_cql_decimal_schema() {
    use arrow::datatypes::DataType as ArrowDataType;

    let col = col_with_cql_type(
        "price",
        DataType::Text,
        cqlite_core::schema::CqlType::Decimal,
    );
    // Represent 123.45 with scale=2, unscaled = 12345
    let result = single_cql_typed_result(
        col,
        Value::Decimal {
            scale: 2,
            unscaled: 12345i64.to_be_bytes().to_vec(),
        },
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    // Schema: Decimal128(38, 9)
    assert_eq!(
        batch.schema().field(0).data_type(),
        &ArrowDataType::Decimal128(38, 9)
    );
}

#[test]
fn test_cql_decimal_roundtrip_scale_same() {
    use arrow::array::Decimal128Array;

    let col = col_with_cql_type("v", DataType::Text, cqlite_core::schema::CqlType::Decimal);
    // Value: 1_000_000_000 (scale=9 matches DECIMAL_FIXED_SCALE, no rescaling needed)
    // Represents 1.000000000
    let unscaled: i64 = 1_000_000_000;
    let result = single_cql_typed_result(
        col,
        Value::Decimal {
            scale: 9,
            unscaled: unscaled.to_be_bytes().to_vec(),
        },
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(arr.is_valid(0));
    // The stored i128 value should equal unscaled (no rescaling since scale matches)
    assert_eq!(arr.value(0), unscaled as i128);
}

#[test]
fn test_cql_decimal_roundtrip_scale_up() {
    use arrow::array::Decimal128Array;

    let col = col_with_cql_type("v", DataType::Text, cqlite_core::schema::CqlType::Decimal);
    // Value: 12345 with scale=2 (represents 123.45)
    // After rescaling to scale=9: 12345 * 10^7 = 123_450_000_000
    let unscaled: i32 = 12345;
    let result = single_cql_typed_result(
        col,
        Value::Decimal {
            scale: 2,
            unscaled: unscaled.to_be_bytes().to_vec(),
        },
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(arr.is_valid(0));
    assert_eq!(arr.value(0), 123_450_000_000i128);
}

#[test]
fn test_cql_decimal_roundtrip_scale_down() {
    use arrow::array::Decimal128Array;

    let col = col_with_cql_type("v", DataType::Text, cqlite_core::schema::CqlType::Decimal);
    // Value: 1_000_000_000_000 with scale=12 (represents 1.000000000000)
    // After rescaling to scale=9: divide by 10^3 = 1_000_000_000
    let unscaled: i64 = 1_000_000_000_000;
    let result = single_cql_typed_result(
        col,
        Value::Decimal {
            scale: 12,
            unscaled: unscaled.to_be_bytes().to_vec(),
        },
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(arr.is_valid(0));
    assert_eq!(arr.value(0), 1_000_000_000i128);
}

#[test]
fn test_cql_decimal_null() {
    use arrow::array::Decimal128Array;

    let col = col_with_cql_type("v", DataType::Text, cqlite_core::schema::CqlType::Decimal);
    let result = single_cql_typed_result(col, Value::Null);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(!arr.is_valid(0));
}

#[test]
fn test_cql_decimal_negative() {
    use arrow::array::Decimal128Array;

    let col = col_with_cql_type("v", DataType::Text, cqlite_core::schema::CqlType::Decimal);
    // Negative value: -9999 with scale=2 (represents -99.99)
    // Rescale to 9: -9999 * 10^7 = -99_990_000_000
    let bigint = num_bigint::BigInt::from(-9999i32);
    let bytes_val = bigint.to_signed_bytes_be();
    let result = single_cql_typed_result(
        col,
        Value::Decimal {
            scale: 2,
            unscaled: bytes_val,
        },
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(arr.is_valid(0));
    assert_eq!(arr.value(0), -99_990_000_000i128);
}

// ----------------------------------------
// CQL varint → Arrow Decimal128(38, 0)
// ----------------------------------------

#[test]
fn test_cql_varint_schema() {
    use arrow::datatypes::DataType as ArrowDataType;

    let col = col_with_cql_type(
        "big_int",
        DataType::BigInt,
        cqlite_core::schema::CqlType::Varint,
    );
    let bigint = num_bigint::BigInt::from(42i32);
    let result = single_cql_typed_result(col, Value::Varint(bigint.to_signed_bytes_be()));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    assert_eq!(
        batch.schema().field(0).data_type(),
        &ArrowDataType::Decimal128(38, 0)
    );
}

#[test]
fn test_cql_varint_roundtrip_positive() {
    use arrow::array::Decimal128Array;

    let col = col_with_cql_type("n", DataType::BigInt, cqlite_core::schema::CqlType::Varint);
    let bigint = num_bigint::BigInt::from(1_234_567_890i64);
    let result = single_cql_typed_result(col, Value::Varint(bigint.to_signed_bytes_be()));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(arr.is_valid(0));
    assert_eq!(arr.value(0), 1_234_567_890i128);
}

#[test]
fn test_cql_varint_roundtrip_negative() {
    use arrow::array::Decimal128Array;

    let col = col_with_cql_type("n", DataType::BigInt, cqlite_core::schema::CqlType::Varint);
    let bigint = num_bigint::BigInt::from(-999i32);
    let result = single_cql_typed_result(col, Value::Varint(bigint.to_signed_bytes_be()));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(arr.is_valid(0));
    assert_eq!(arr.value(0), -999i128);
}

#[test]
fn test_cql_varint_null() {
    use arrow::array::Decimal128Array;

    let col = col_with_cql_type("n", DataType::BigInt, cqlite_core::schema::CqlType::Varint);
    let result = single_cql_typed_result(col, Value::Null);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(!arr.is_valid(0));
}

// ----------------------------------------
// CQL duration → Arrow Utf8 (parquet crate v53 does not support Interval(MonthDayNano))
//
// The Parquet format's INTERVAL logical type only supports millisecond precision,
// not nanoseconds. The `parquet` crate v53 explicitly rejects writing
// `Interval(MonthDayNano)` to Parquet files.  We therefore serialize CQL duration
// values as their canonical CQL text form (e.g. "1mo2d3ns") stored as `Utf8`.
// When the parquet crate gains MonthDayNano write support, these tests and the
// builder can be upgraded to use the Arrow interval type directly.
// ----------------------------------------

#[test]
fn test_cql_duration_schema_is_utf8() {
    use arrow::datatypes::DataType as ArrowDataType;

    let col = col_with_cql_type(
        "dur",
        DataType::Text,
        cqlite_core::schema::CqlType::Duration,
    );
    let result = single_cql_typed_result(
        col,
        Value::Duration {
            months: 1,
            days: 2,
            nanos: 3_000_000_000,
        },
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    // Duration falls back to Utf8 due to parquet crate limitation.
    assert_eq!(batch.schema().field(0).data_type(), &ArrowDataType::Utf8);
}

#[test]
fn test_cql_duration_roundtrip_as_text() {
    let col = col_with_cql_type(
        "dur",
        DataType::Text,
        cqlite_core::schema::CqlType::Duration,
    );
    let result = single_cql_typed_result(
        col,
        Value::Duration {
            months: 3,
            days: 15,
            nanos: 7_200_000_000_000, // 2 hours
        },
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(arr.is_valid(0));
    // Should produce a non-empty canonical CQL duration string
    let s = arr.value(0);
    assert!(!s.is_empty(), "duration text must not be empty, got: {s:?}");
    // Must encode all three components
    assert!(s.contains("mo") || s.contains('d') || s.contains("ns"));
}

#[test]
fn test_cql_duration_null() {
    let col = col_with_cql_type(
        "dur",
        DataType::Text,
        cqlite_core::schema::CqlType::Duration,
    );
    let result = single_cql_typed_result(col, Value::Null);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!arr.is_valid(0));
}

#[test]
fn test_cql_duration_zero_as_text() {
    let col = col_with_cql_type(
        "dur",
        DataType::Text,
        cqlite_core::schema::CqlType::Duration,
    );
    let result = single_cql_typed_result(
        col,
        Value::Duration {
            months: 0,
            days: 0,
            nanos: 0,
        },
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(arr.is_valid(0));
    // ValueFormatter formats 0-duration as "0ns"
    assert_eq!(arr.value(0), "0ns");
}

// ----------------------------------------
// CQL uuid/timeuuid → FixedSizeBinary(16) + Arrow UUID extension
// ----------------------------------------

#[test]
fn test_cql_uuid_schema_and_extension_metadata() {
    use arrow::datatypes::DataType as ArrowDataType;

    let col = col_with_cql_type(
        "user_id",
        DataType::Uuid,
        cqlite_core::schema::CqlType::Uuid,
    );
    let uuid = [
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
        0x88,
    ];
    let result = single_cql_typed_result(col, Value::Uuid(uuid));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let schema = batch.schema();
    let field = schema.field(0);
    // Arrow type must be FixedSizeBinary(16)
    assert_eq!(field.data_type(), &ArrowDataType::FixedSizeBinary(16));
    // UUID extension metadata must be present
    assert_eq!(
        field
            .metadata()
            .get("ARROW:extension:name")
            .map(|s| s.as_str()),
        Some("arrow.uuid")
    );
}

#[test]
fn test_cql_uuid_roundtrip() {
    use arrow::array::FixedSizeBinaryArray;

    let col = col_with_cql_type("id", DataType::Uuid, cqlite_core::schema::CqlType::Uuid);
    let uuid_bytes = [
        0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99,
    ];
    let result = single_cql_typed_result(col, Value::Uuid(uuid_bytes));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(arr.value(0), &uuid_bytes);
}

#[test]
fn test_cql_timeuuid_extension_metadata() {
    use arrow::datatypes::DataType as ArrowDataType;

    let col = col_with_cql_type(
        "ev_id",
        DataType::Uuid,
        cqlite_core::schema::CqlType::TimeUuid,
    );
    let uuid = [0u8; 16];
    let result = single_cql_typed_result(col, Value::Uuid(uuid));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let schema = batch.schema();
    let field = schema.field(0);
    assert_eq!(field.data_type(), &ArrowDataType::FixedSizeBinary(16));
    assert_eq!(
        field
            .metadata()
            .get("ARROW:extension:name")
            .map(|s| s.as_str()),
        Some("arrow.uuid")
    );
}

#[test]
fn test_cql_uuid_null() {
    use arrow::array::FixedSizeBinaryArray;

    let col = col_with_cql_type("id", DataType::Uuid, cqlite_core::schema::CqlType::Uuid);
    let result = single_cql_typed_result(col, Value::Null);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!(!arr.is_valid(0));
}

// ----------------------------------------
// CQL inet → Arrow Utf8
// ----------------------------------------

#[test]
fn test_cql_inet_ipv4_roundtrip() {
    let col = col_with_cql_type("ip", DataType::Text, cqlite_core::schema::CqlType::Inet);
    // 192.168.1.1
    let result = single_cql_typed_result(col, Value::Inet(vec![192, 168, 1, 1]));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "192.168.1.1");
}

#[test]
fn test_cql_inet_ipv6_roundtrip() {
    // Loopback ::1 = 0000...0001
    let mut ipv6 = [0u8; 16];
    ipv6[15] = 1;
    let col = col_with_cql_type("ip", DataType::Text, cqlite_core::schema::CqlType::Inet);
    let result = single_cql_typed_result(col, Value::Inet(ipv6.to_vec()));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // ::1 is the canonical representation of IPv6 loopback
    assert_eq!(arr.value(0), "::1");
}

#[test]
fn test_cql_inet_null() {
    let col = col_with_cql_type("ip", DataType::Text, cqlite_core::schema::CqlType::Inet);
    let result = single_cql_typed_result(col, Value::Null);

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!arr.is_valid(0));
}

// ----------------------------------------
// CQL counter → Arrow Int64
// ----------------------------------------

#[test]
fn test_cql_counter_maps_to_int64() {
    use arrow::datatypes::DataType as ArrowDataType;

    let col = col_with_cql_type(
        "cnt",
        DataType::BigInt,
        cqlite_core::schema::CqlType::Counter,
    );
    let result = single_cql_typed_result(col, Value::Counter(42_000_000));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    assert_eq!(batch.schema().field(0).data_type(), &ArrowDataType::Int64);
}

#[test]
fn test_cql_counter_roundtrip() {
    let col = col_with_cql_type(
        "cnt",
        DataType::BigInt,
        cqlite_core::schema::CqlType::Counter,
    );
    let result = single_cql_typed_result(col, Value::Counter(9_999_999_999));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 9_999_999_999);
}

// ----------------------------------------
// Fallback: no cql_type → existing mapping unchanged
// ----------------------------------------

#[test]
fn test_no_cql_type_fallback_unchanged() {
    // Without cql_type, BigInt DataType must still produce Int64.
    use arrow::datatypes::DataType as ArrowDataType;

    let col = ColumnInfo {
        name: "big".to_string(),
        data_type: DataType::BigInt,
        nullable: false,
        position: 0,
        table_name: None,
        cql_type: None,
    };
    let mut values = HashMap::new();
    values.insert("big".to_string(), Value::BigInt(1234567890));
    let row = QueryRow {
        values,
        key: RowKey::new(vec![1]),
        metadata: Default::default(),
    };
    let result = QueryResult {
        rows: vec![row],
        rows_affected: 0,
        execution_time_ms: 0,
        metadata: cqlite_core::query::QueryMetadata {
            columns: vec![col],
            ..Default::default()
        },
    };

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    assert_eq!(batch.schema().field(0).data_type(), &ArrowDataType::Int64);
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 1234567890);
}

// ============================================================================
// Issue #676: Typed List/Set arrays via recursive element builder
//
// list<X> and set<X> must export with typed (non-stringified) Arrow element
// arrays.  CqlType::Frozen(inner) is transparent in both schema and value
// recursion.  Null elements and null/empty collections must be preserved.
// ============================================================================

/// Build a ColumnInfo with `cql_type = Some(List(inner))` or `Some(Set(inner))`.
fn list_col_with_cql_type(name: &str, cql_type: cqlite_core::schema::CqlType) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type: cqlite_core::types::DataType::List,
        nullable: true,
        position: 0,
        table_name: None,
        cql_type: Some(cql_type),
    }
}

fn set_col_with_cql_type(name: &str, cql_type: cqlite_core::schema::CqlType) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type: cqlite_core::types::DataType::Set,
        nullable: true,
        position: 0,
        table_name: None,
        cql_type: Some(cql_type),
    }
}

fn single_col_multi_row_result(col: ColumnInfo, values: Vec<Value>) -> QueryResult {
    let rows: Vec<QueryRow> = values
        .into_iter()
        .enumerate()
        .map(|(i, v)| {
            let mut map = HashMap::new();
            map.insert(col.name.clone(), v);
            QueryRow {
                values: map,
                key: RowKey::new(vec![i as u8]),
                metadata: Default::default(),
            }
        })
        .collect();
    QueryResult {
        rows,
        rows_affected: 0,
        execution_time_ms: 0,
        metadata: cqlite_core::query::QueryMetadata {
            columns: vec![col],
            ..Default::default()
        },
    }
}

// ----------------------------------------
// list<int> → List<Int32>
// ----------------------------------------

#[test]
fn test_list_int_schema_is_list_int32() {
    use arrow::datatypes::DataType as ArrowDataType;

    let col = list_col_with_cql_type(
        "nums",
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Int)),
    );
    let result =
        single_cql_typed_result(col, Value::List(vec![Value::Integer(1), Value::Integer(2)]));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let schema = batch.schema();
    let field = schema.field(0);
    assert_eq!(
        field.data_type(),
        &ArrowDataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
            "item",
            ArrowDataType::Int32,
            true
        )))
    );
}

#[test]
fn test_list_int_roundtrip() {
    let col = list_col_with_cql_type(
        "nums",
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Int)),
    );
    let result = single_cql_typed_result(
        col,
        Value::List(vec![
            Value::Integer(10),
            Value::Integer(20),
            Value::Integer(30),
        ]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(!list_col.is_null(0));
    let elements = list_col.value(0);
    let int_arr = elements.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(int_arr.len(), 3);
    assert_eq!(int_arr.value(0), 10);
    assert_eq!(int_arr.value(1), 20);
    assert_eq!(int_arr.value(2), 30);
}

// ----------------------------------------
// list<text> → List<Utf8>
// ----------------------------------------

#[test]
fn test_list_text_roundtrip() {
    let col = list_col_with_cql_type(
        "words",
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Text)),
    );
    let result = single_cql_typed_result(
        col,
        Value::List(vec![
            Value::Text("hello".to_string()),
            Value::Text("world".to_string()),
        ]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(!list_col.is_null(0));
    let elements = list_col.value(0);
    let str_arr = elements.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(str_arr.len(), 2);
    assert_eq!(str_arr.value(0), "hello");
    assert_eq!(str_arr.value(1), "world");
}

#[test]
fn test_list_text_element_is_utf8_not_stringified() {
    use arrow::datatypes::DataType as ArrowDataType;

    // Verify the element type is Utf8, not some other representation.
    let col = list_col_with_cql_type(
        "words",
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Text)),
    );
    let result = single_cql_typed_result(col, Value::List(vec![Value::Text("abc".to_string())]));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let schema = batch.schema();
    match schema.field(0).data_type() {
        ArrowDataType::List(item_field) => {
            assert_eq!(item_field.data_type(), &ArrowDataType::Utf8);
        }
        other => panic!("Expected List, got {other:?}"),
    }
}

// ----------------------------------------
// list<timestamp> → List<Timestamp(ms, UTC)>
// ----------------------------------------

#[test]
fn test_list_timestamp_roundtrip() {
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};

    let col = list_col_with_cql_type(
        "ts_list",
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Timestamp)),
    );
    let ts1: i64 = 1_673_778_645_000;
    let ts2: i64 = 1_673_778_700_000;
    let result = single_cql_typed_result(
        col,
        Value::List(vec![Value::Timestamp(ts1), Value::Timestamp(ts2)]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    // Verify schema: List<Timestamp(ms, UTC)>
    let schema = batch.schema();
    match schema.field(0).data_type() {
        ArrowDataType::List(item_field) => {
            assert_eq!(
                item_field.data_type(),
                &ArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
            );
        }
        other => panic!("Expected List, got {other:?}"),
    }

    // Verify values
    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let elements = list_col.value(0);
    let ts_arr = elements
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(ts_arr.value(0), ts1);
    assert_eq!(ts_arr.value(1), ts2);
}

// ----------------------------------------
// set<int> → List<Int32>  (Arrow has no Set type)
// ----------------------------------------

#[test]
fn test_set_int_schema_is_list_int32() {
    use arrow::datatypes::DataType as ArrowDataType;

    let col = set_col_with_cql_type(
        "ids",
        cqlite_core::schema::CqlType::Set(Box::new(cqlite_core::schema::CqlType::Int)),
    );
    let result =
        single_cql_typed_result(col, Value::Set(vec![Value::Integer(1), Value::Integer(2)]));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let schema = batch.schema();
    let field = schema.field(0);
    assert_eq!(
        field.data_type(),
        &ArrowDataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
            "item",
            ArrowDataType::Int32,
            true
        )))
    );
}

#[test]
fn test_set_int_roundtrip() {
    let col = set_col_with_cql_type(
        "ids",
        cqlite_core::schema::CqlType::Set(Box::new(cqlite_core::schema::CqlType::Int)),
    );
    let result = single_cql_typed_result(
        col,
        Value::Set(vec![Value::Integer(100), Value::Integer(200)]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(!list_col.is_null(0));
    let elements = list_col.value(0);
    let int_arr = elements.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(int_arr.len(), 2);
    assert_eq!(int_arr.value(0), 100);
    assert_eq!(int_arr.value(1), 200);
}

// ----------------------------------------
// set<uuid> → List<FixedSizeBinary(16)>
// ----------------------------------------

#[test]
fn test_set_uuid_schema_is_list_fixedsizebinary16() {
    use arrow::datatypes::DataType as ArrowDataType;

    let col = set_col_with_cql_type(
        "uuids",
        cqlite_core::schema::CqlType::Set(Box::new(cqlite_core::schema::CqlType::Uuid)),
    );
    let uuid1 = [0x01u8; 16];
    let result = single_cql_typed_result(col, Value::Set(vec![Value::Uuid(uuid1)]));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let schema = batch.schema();
    let field = schema.field(0);
    assert_eq!(
        field.data_type(),
        &ArrowDataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
            "item",
            ArrowDataType::FixedSizeBinary(16),
            true
        )))
    );
}

#[test]
fn test_set_uuid_roundtrip() {
    let col = set_col_with_cql_type(
        "uuids",
        cqlite_core::schema::CqlType::Set(Box::new(cqlite_core::schema::CqlType::Uuid)),
    );
    let uuid1 = [0xAAu8; 16];
    let uuid2 = [0xBBu8; 16];
    let result = single_cql_typed_result(
        col,
        Value::Set(vec![Value::Uuid(uuid1), Value::Uuid(uuid2)]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(!list_col.is_null(0));
    let elements = list_col.value(0);
    let fsb_arr = elements
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(fsb_arr.len(), 2);
    assert_eq!(fsb_arr.value(0), &uuid1);
    assert_eq!(fsb_arr.value(1), &uuid2);
}

// ----------------------------------------
// Null list, empty list, and null elements
// ----------------------------------------

#[test]
fn test_list_int_null_list_roundtrip() {
    // A null list (Value::Null) is distinct from an empty list (Value::List(vec![])).
    let col = list_col_with_cql_type(
        "nums",
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Int)),
    );
    let result = single_col_multi_row_result(
        col,
        vec![
            Value::List(vec![Value::Integer(1)]), // row 0: non-null list
            Value::Null,                          // row 1: null list
            Value::List(vec![]),                  // row 2: empty (non-null) list
        ],
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list_col.len(), 3);

    // Row 0: non-null, has one element.
    assert!(!list_col.is_null(0));
    let row0 = list_col.value(0);
    let row0_arr = row0.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(row0_arr.len(), 1);
    assert_eq!(row0_arr.value(0), 1);

    // Row 1: null list.
    assert!(list_col.is_null(1));

    // Row 2: non-null empty list.
    assert!(!list_col.is_null(2));
    let row2 = list_col.value(2);
    let row2_arr = row2.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(row2_arr.len(), 0);
}

// ----------------------------------------
// Nested: list<frozen<list<int>>> → List<List<Int32>>
// ----------------------------------------

#[test]
fn test_list_frozen_list_int_schema() {
    use arrow::datatypes::DataType as ArrowDataType;

    // list<frozen<list<int>>>
    let inner_list_type =
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Int));
    let frozen_inner = cqlite_core::schema::CqlType::Frozen(Box::new(inner_list_type));
    let outer_type = cqlite_core::schema::CqlType::List(Box::new(frozen_inner));

    let col = list_col_with_cql_type("nested", outer_type);

    // Value: [[1, 2], [3]]
    let result = single_cql_typed_result(
        col,
        Value::List(vec![
            Value::List(vec![Value::Integer(1), Value::Integer(2)]),
            Value::List(vec![Value::Integer(3)]),
        ]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    // Schema should be List<List<Int32>>
    let schema = batch.schema();
    let field = schema.field(0);
    match field.data_type() {
        ArrowDataType::List(outer_item) => match outer_item.data_type() {
            ArrowDataType::List(inner_item) => {
                assert_eq!(inner_item.data_type(), &ArrowDataType::Int32);
            }
            other => panic!("Expected inner List, got {other:?}"),
        },
        other => panic!("Expected outer List, got {other:?}"),
    }
}

#[test]
fn test_list_frozen_list_int_roundtrip() {
    // list<frozen<list<int>>>: values [[10, 20], [30]] — full roundtrip.
    let inner_list_type =
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Int));
    let frozen_inner = cqlite_core::schema::CqlType::Frozen(Box::new(inner_list_type));
    let outer_type = cqlite_core::schema::CqlType::List(Box::new(frozen_inner));

    let col = list_col_with_cql_type("nested", outer_type);

    let result = single_cql_typed_result(
        col,
        Value::List(vec![
            Value::List(vec![Value::Integer(10), Value::Integer(20)]),
            Value::List(vec![Value::Integer(30)]),
        ]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    // Outer list for row 0
    let outer = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(!outer.is_null(0));
    let outer_row = outer.value(0); // the two inner lists

    let inner_lists = outer_row.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(inner_lists.len(), 2);

    // First inner list: [10, 20]
    let first_inner = inner_lists.value(0);
    let first_arr = first_inner.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(first_arr.len(), 2);
    assert_eq!(first_arr.value(0), 10);
    assert_eq!(first_arr.value(1), 20);

    // Second inner list: [30]
    let second_inner = inner_lists.value(1);
    let second_arr = second_inner.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(second_arr.len(), 1);
    assert_eq!(second_arr.value(0), 30);
}

#[test]
fn test_list_frozen_list_int_with_frozen_value() {
    // Runtime Value::Frozen must be unwrapped transparently.
    // list<frozen<list<int>>>: values [[1], frozen([2, 3])]
    let inner_list_type =
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Int));
    let frozen_inner = cqlite_core::schema::CqlType::Frozen(Box::new(inner_list_type));
    let outer_type = cqlite_core::schema::CqlType::List(Box::new(frozen_inner));

    let col = list_col_with_cql_type("nested", outer_type);

    let result = single_cql_typed_result(
        col,
        Value::List(vec![
            Value::List(vec![Value::Integer(1)]),
            Value::Frozen(Box::new(Value::List(vec![
                Value::Integer(2),
                Value::Integer(3),
            ]))),
        ]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let outer = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let outer_row = outer.value(0);
    let inner_lists = outer_row.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(inner_lists.len(), 2);

    // First inner list: [1]
    let first = inner_lists.value(0);
    let first_arr = first.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(first_arr.value(0), 1);

    // Second inner list (was Frozen-wrapped): [2, 3]
    let second = inner_lists.value(1);
    let second_arr = second.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(second_arr.len(), 2);
    assert_eq!(second_arr.value(0), 2);
    assert_eq!(second_arr.value(1), 3);
}

// ----------------------------------------
// list<date> → List<Date32>  (high-fidelity scalar)
// ----------------------------------------

#[test]
fn test_list_date_roundtrip() {
    let col = list_col_with_cql_type(
        "dates",
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Date)),
    );
    let result = single_cql_typed_result(
        col,
        Value::List(vec![Value::Date(19000), Value::Date(19001)]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let elements = list_col.value(0);
    let date_arr = elements.as_any().downcast_ref::<Date32Array>().unwrap();
    assert_eq!(date_arr.len(), 2);
    assert_eq!(date_arr.value(0), 19000);
    assert_eq!(date_arr.value(1), 19001);
}

// ----------------------------------------
// set<text> → List<Utf8>  (set uses Value::Set)
// ----------------------------------------

#[test]
fn test_set_text_roundtrip() {
    let col = set_col_with_cql_type(
        "tags",
        cqlite_core::schema::CqlType::Set(Box::new(cqlite_core::schema::CqlType::Text)),
    );
    let result = single_cql_typed_result(
        col,
        Value::Set(vec![
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(!list_col.is_null(0));
    let elements = list_col.value(0);
    let str_arr = elements.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(str_arr.len(), 2);
    assert_eq!(str_arr.value(0), "alpha");
    assert_eq!(str_arr.value(1), "beta");
}

// ----------------------------------------
// Multiple rows with mixed null/empty lists
// ----------------------------------------

#[test]
fn test_list_int_multi_row_null_and_values() {
    let col = list_col_with_cql_type(
        "nums",
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Int)),
    );
    let result = single_col_multi_row_result(
        col,
        vec![
            Value::List(vec![Value::Integer(1), Value::Integer(2)]),
            Value::Null,
            Value::List(vec![Value::Integer(99)]),
        ],
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list_col.len(), 3);

    // Row 0: [1, 2]
    assert!(!list_col.is_null(0));
    let r0 = list_col.value(0);
    let r0_arr = r0.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(r0_arr.len(), 2);
    assert_eq!(r0_arr.value(0), 1);
    assert_eq!(r0_arr.value(1), 2);

    // Row 1: null
    assert!(list_col.is_null(1));

    // Row 2: [99]
    assert!(!list_col.is_null(2));
    let r2 = list_col.value(2);
    let r2_arr = r2.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(r2_arr.len(), 1);
    assert_eq!(r2_arr.value(0), 99);
}

// ----------------------------------------
// Frozen top-level column (Frozen<list<int>>)
// ----------------------------------------

#[test]
fn test_frozen_list_int_schema_and_roundtrip() {
    use arrow::datatypes::DataType as ArrowDataType;

    // Column type is Frozen<list<int>> — should be transparent, same as list<int>
    let col = ColumnInfo {
        name: "frozen_nums".to_string(),
        data_type: cqlite_core::types::DataType::Frozen,
        nullable: true,
        position: 0,
        table_name: None,
        cql_type: Some(cqlite_core::schema::CqlType::Frozen(Box::new(
            cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Int)),
        ))),
    };

    // Value can be either List or Frozen(List) at runtime.
    let result = single_cql_typed_result(
        col,
        Value::List(vec![
            Value::Integer(7),
            Value::Integer(8),
            Value::Integer(9),
        ]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    // Schema should be List<Int32> (Frozen is transparent)
    let schema = batch.schema();
    let field = schema.field(0);
    assert_eq!(
        field.data_type(),
        &ArrowDataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
            "item",
            ArrowDataType::Int32,
            true
        )))
    );

    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let elements = list_col.value(0);
    let int_arr = elements.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(int_arr.len(), 3);
    assert_eq!(int_arr.value(0), 7);
    assert_eq!(int_arr.value(1), 8);
    assert_eq!(int_arr.value(2), 9);
}

// ----------------------------------------
// list<bigint> — verify BigInt elements are correctly typed
// ----------------------------------------

#[test]
fn test_list_bigint_roundtrip() {
    let col = list_col_with_cql_type(
        "big_nums",
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::BigInt)),
    );
    let result = single_cql_typed_result(
        col,
        Value::List(vec![
            Value::BigInt(i64::MAX),
            Value::BigInt(i64::MIN),
            Value::BigInt(0),
        ]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let elements = list_col.value(0);
    let int64_arr = elements.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(int64_arr.len(), 3);
    assert_eq!(int64_arr.value(0), i64::MAX);
    assert_eq!(int64_arr.value(1), i64::MIN);
    assert_eq!(int64_arr.value(2), 0);
}

// ----------------------------------------
// list<set<frozen<set<text>>>> — verify set<frozen<set<text>>> works
// ----------------------------------------

#[test]
fn test_set_frozen_set_text_roundtrip() {
    // set<frozen<set<text>>>: value = {{"a", "b"}, {"c"}}
    let inner_set = cqlite_core::schema::CqlType::Set(Box::new(cqlite_core::schema::CqlType::Text));
    let frozen_inner = cqlite_core::schema::CqlType::Frozen(Box::new(inner_set));
    let outer_type = cqlite_core::schema::CqlType::Set(Box::new(frozen_inner));

    let col = set_col_with_cql_type("nested_sets", outer_type);

    let result = single_cql_typed_result(
        col,
        Value::Set(vec![
            Value::Set(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
            ]),
            Value::Set(vec![Value::Text("c".to_string())]),
        ]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let outer = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let outer_row = outer.value(0);
    let inner_lists = outer_row.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(inner_lists.len(), 2);

    let first_inner = inner_lists.value(0);
    let first_arr = first_inner.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(first_arr.len(), 2);
    assert_eq!(first_arr.value(0), "a");
    assert_eq!(first_arr.value(1), "b");

    let second_inner = inner_lists.value(1);
    let second_arr = second_inner.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(second_arr.len(), 1);
    assert_eq!(second_arr.value(0), "c");
}

// ----------------------------------------
// Elements that are Value::Null within a non-null list
// ----------------------------------------

#[test]
fn test_list_int_with_null_element() {
    // list<int> = [1, NULL, 3] — null elements within the list
    let col = list_col_with_cql_type(
        "nums",
        cqlite_core::schema::CqlType::List(Box::new(cqlite_core::schema::CqlType::Int)),
    );
    let result = single_cql_typed_result(
        col,
        Value::List(vec![Value::Integer(1), Value::Null, Value::Integer(3)]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let list_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(!list_col.is_null(0)); // The list itself is not null
    let elements = list_col.value(0);
    let int_arr = elements.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(int_arr.len(), 3);
    assert!(int_arr.is_valid(0));
    assert_eq!(int_arr.value(0), 1);
    assert!(!int_arr.is_valid(1)); // null element
    assert!(int_arr.is_valid(2));
    assert_eq!(int_arr.value(2), 3);
}
