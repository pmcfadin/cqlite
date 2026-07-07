//! Output Format Tests for M3 Milestone (Issue #269)
//!
//! These tests ensure output writers produce correct, consistent format in preparation
//! for M3 (Output Writers) milestone.
//!
//! Philosophy: Output format is a contract with users. Tests ensure format stability
//! and correctness.
//!
//! # Tests Implemented
//!
//! 1. JSON deterministic key ordering (verifies existing implementation)
//! 2. CSV special character escaping (verifies existing implementation)
//! 3. All 28 Value variants serialize without error
//! 4. NULL representation consistent across formats
//! 5. Nested collections render correct structure
//! 6. UDT fields render with names, not indices
//! 7. Column order matches metadata.columns

#![cfg(feature = "state_machine")]

use cqlite_cli::config::OutputConfig;
use cqlite_cli::output::{CSVWriter, JSONWriter};
use cqlite_core::query::{ColumnInfo, QueryMetadata, QueryResult, QueryRow};
use cqlite_core::types::{DataType, TombstoneInfo, TombstoneType, UdtField, UdtValue};
use cqlite_core::{RowKey, Value};
use std::collections::HashMap;

// ============================================================================
// Helper Functions
// ============================================================================

fn default_config() -> OutputConfig {
    OutputConfig::default()
}

fn create_single_value_result(col_name: &str, value: Value, data_type: DataType) -> QueryResult {
    let columns = vec![ColumnInfo {
        name: col_name.to_string(),
        data_type,
        nullable: true,
        position: 0,
        table_name: None,
        cql_type: None,
    }];

    let metadata = QueryMetadata {
        columns,
        ..Default::default()
    };

    let mut values = HashMap::new();
    values.insert(col_name.into(), value);

    let row = QueryRow {
        values,
        key: RowKey::new(vec![1]),
        metadata: Default::default(),
        cell_metadata: None,
    };

    QueryResult {
        rows: vec![row],
        rows_affected: 0,
        execution_time_ms: 0,
        metadata,
    }
}

fn create_result_with_columns(
    columns: Vec<(&str, DataType)>,
    row_values: Vec<(&str, Value)>,
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
        columns: columns_vec,
        ..Default::default()
    };

    let mut values = HashMap::new();
    for (col_name, value) in row_values {
        values.insert(col_name.into(), value);
    }

    let row = QueryRow {
        values,
        key: RowKey::new(vec![1]),
        metadata: Default::default(),
        cell_metadata: None,
    };

    QueryResult {
        rows: vec![row],
        rows_affected: 0,
        execution_time_ms: 0,
        metadata,
    }
}

// ============================================================================
// Test 1: JSON Deterministic Key Ordering
// ============================================================================

#[test]
fn test_json_output_key_order_deterministic() {
    // Issue #269: JSON output must have consistent key order for diffing
    // Run the same serialization twice and verify identical output

    let result = create_result_with_columns(
        vec![
            ("zebra", DataType::Integer),
            ("alpha", DataType::Integer),
            ("mango", DataType::Integer),
        ],
        vec![
            ("zebra", Value::Integer(3)),
            ("alpha", Value::Integer(1)),
            ("mango", Value::Integer(2)),
        ],
    );

    let json1 = JSONWriter::write(&result, &default_config()).unwrap();
    let json2 = JSONWriter::write(&result, &default_config()).unwrap();

    assert_eq!(json1, json2, "JSON output must be deterministic");

    // Verify key order matches column order (zebra, alpha, mango), NOT alphabetical
    let zebra_pos = json1.find("\"zebra\"").unwrap();
    let alpha_pos = json1.find("\"alpha\"").unwrap();
    let mango_pos = json1.find("\"mango\"").unwrap();

    assert!(
        zebra_pos < alpha_pos,
        "Column 'zebra' must appear before 'alpha' (column order, not alphabetical)"
    );
    assert!(
        alpha_pos < mango_pos,
        "Column 'alpha' must appear before 'mango'"
    );
}

// ============================================================================
// Test 2: CSV Special Character Escaping
// ============================================================================

#[test]
fn test_csv_escapes_commas_in_values() {
    let result = create_single_value_result(
        "text_col",
        Value::Text("hello, world".to_string()),
        DataType::Text,
    );

    let csv = CSVWriter::write(&result, &default_config()).unwrap();

    // Value containing comma must be quoted per RFC 4180
    assert!(
        csv.contains("\"hello, world\""),
        "CSV must quote values containing commas. Got: {csv}"
    );
}

#[test]
fn test_csv_escapes_quotes_in_values() {
    let result = create_single_value_result(
        "text_col",
        Value::Text("say \"hello\"".to_string()),
        DataType::Text,
    );

    let csv = CSVWriter::write(&result, &default_config()).unwrap();

    // Quotes must be escaped as "" per RFC 4180
    assert!(
        csv.contains("\"say \"\"hello\"\"\""),
        "CSV must escape quotes by doubling them. Got: {csv}"
    );
}

#[test]
fn test_csv_escapes_newlines_in_values() {
    let result = create_single_value_result(
        "text_col",
        Value::Text("line1\nline2".to_string()),
        DataType::Text,
    );

    let csv = CSVWriter::write(&result, &default_config()).unwrap();

    // Value containing newline must be quoted
    assert!(
        csv.contains("\"line1\nline2\""),
        "CSV must quote values containing newlines. Got: {csv}"
    );
}

// ============================================================================
// Test 3: All 28 Value Variants Serialize
// ============================================================================

#[test]
fn test_json_serializes_all_value_variants() {
    // Test each of the 27 Value variants produces valid JSON
    // Note: Issue #269 states 26, but actual count is 27 variants in Value enum
    let variants: Vec<(&str, Value, DataType)> = vec![
        // Primitive types
        ("null_val", Value::Null, DataType::Null),
        ("bool_val", Value::Boolean(true), DataType::Boolean),
        ("int_val", Value::Integer(42), DataType::Integer),
        ("bigint_val", Value::BigInt(i64::MAX), DataType::BigInt),
        ("counter_val", Value::Counter(100), DataType::Integer), // Counter uses Integer display
        ("float_val", Value::Float(1.23456), DataType::Float),
        ("float32_val", Value::Float32(9.87), DataType::Float32),
        ("text_val", Value::Text("hello".to_string()), DataType::Text),
        (
            "blob_val",
            Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            DataType::Blob,
        ),
        ("tinyint_val", Value::TinyInt(127), DataType::TinyInt),
        ("smallint_val", Value::SmallInt(32767), DataType::SmallInt),
        // Date/Time types
        (
            "timestamp_val",
            Value::Timestamp(1673778645123),
            DataType::Timestamp,
        ),
        ("date_val", Value::Date(19358), DataType::Text), // Date uses Text for display
        ("time_val", Value::Time(52245123456789), DataType::Text), // Time uses Text for display
        (
            "duration_val",
            Value::Duration {
                months: 2,
                days: 15,
                nanos: 123456789,
            },
            DataType::Text, // Duration uses Text for display
        ),
        // UUID/ID types
        (
            "uuid_val",
            Value::Uuid([
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
                0x77, 0x88,
            ]),
            DataType::Uuid,
        ),
        (
            "inet_val",
            Value::Inet(vec![192, 168, 1, 1]),
            DataType::Text, // Inet uses Text for display
        ),
        // Numeric extended types
        (
            "varint_val",
            Value::Varint(vec![0x01, 0x00]),
            DataType::Text, // Varint uses Text for display
        ),
        (
            "decimal_val",
            Value::Decimal {
                scale: 2,
                unscaled: vec![0x30, 0x39],
            },
            DataType::Text, // Decimal uses Text for display
        ),
        (
            "json_val",
            Value::Json(Box::new(serde_json::json!({"key": "value"}))),
            DataType::Json,
        ),
        // Collection types - DataType::List/Set/Map are unit variants
        (
            "list_val",
            Value::List(vec![Value::Integer(1), Value::Integer(2)]),
            DataType::List,
        ),
        (
            "set_val",
            Value::Set(vec![Value::Integer(3), Value::Integer(4)]),
            DataType::Set,
        ),
        (
            "map_val",
            Value::Map(vec![(Value::Text("key".to_string()), Value::Integer(5))]),
            DataType::Map,
        ),
        (
            "tuple_val",
            Value::Tuple(vec![Value::Integer(1), Value::Text("two".to_string())]),
            DataType::Tuple,
        ),
        // Complex types
        (
            "udt_val",
            Value::Udt(Box::new(UdtValue {
                type_name: "address".to_string(),
                keyspace: "test".to_string(),
                fields: vec![
                    UdtField {
                        name: "street".to_string(),
                        value: Some(Value::Text("123 Main St".to_string())),
                    },
                    UdtField {
                        name: "city".to_string(),
                        value: Some(Value::Text("Springfield".to_string())),
                    },
                ],
            })),
            DataType::Udt,
        ),
        (
            "frozen_val",
            Value::Frozen(Box::new(Value::List(vec![Value::Integer(1)]))),
            DataType::Frozen,
        ),
        (
            "tombstone_val",
            Value::Tombstone(Box::new(TombstoneInfo {
                deletion_time: 1673778645000000,
                tombstone_type: TombstoneType::RowTombstone,
                local_deletion_time: 0,
                ttl: None,
                range_start: None,
                range_end: None,
            })),
            DataType::Tombstone,
        ),
    ];

    // Verify we're testing all 27 variants
    // Note: Issue #269 states 26, but actual count is 27 variants in Value enum
    assert_eq!(variants.len(), 27, "Must test all 27 Value variants");

    for (name, value, data_type) in variants {
        let result = create_single_value_result(name, value.clone(), data_type);
        let json_result = JSONWriter::write(&result, &default_config());

        assert!(
            json_result.is_ok(),
            "Failed to serialize Value variant '{}': {:?}. Value: {:?}",
            name,
            json_result.err(),
            value
        );

        // Verify it's valid JSON by parsing
        let json_str = json_result.unwrap();
        let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(&json_str);
        assert!(
            parsed.is_ok(),
            "JSON output for '{name}' is not valid JSON: {json_str}"
        );
    }
}

#[test]
fn test_csv_serializes_all_value_variants() {
    // Same 28 variants for CSV
    let variants: Vec<(&str, Value, DataType)> = vec![
        ("null_val", Value::Null, DataType::Null),
        ("bool_val", Value::Boolean(true), DataType::Boolean),
        ("int_val", Value::Integer(42), DataType::Integer),
        ("bigint_val", Value::BigInt(i64::MAX), DataType::BigInt),
        ("counter_val", Value::Counter(100), DataType::Integer),
        ("float_val", Value::Float(1.23456), DataType::Float),
        ("float32_val", Value::Float32(9.87), DataType::Float32),
        ("text_val", Value::Text("hello".to_string()), DataType::Text),
        (
            "blob_val",
            Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            DataType::Blob,
        ),
        ("tinyint_val", Value::TinyInt(127), DataType::TinyInt),
        ("smallint_val", Value::SmallInt(32767), DataType::SmallInt),
        (
            "timestamp_val",
            Value::Timestamp(1673778645123),
            DataType::Timestamp,
        ),
        ("date_val", Value::Date(19358), DataType::Text),
        ("time_val", Value::Time(52245123456789), DataType::Text),
        (
            "duration_val",
            Value::Duration {
                months: 2,
                days: 15,
                nanos: 123456789,
            },
            DataType::Text,
        ),
        (
            "uuid_val",
            Value::Uuid([
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
                0x77, 0x88,
            ]),
            DataType::Uuid,
        ),
        (
            "inet_val",
            Value::Inet(vec![192, 168, 1, 1]),
            DataType::Text,
        ),
        (
            "varint_val",
            Value::Varint(vec![0x01, 0x00]),
            DataType::Text,
        ),
        (
            "decimal_val",
            Value::Decimal {
                scale: 2,
                unscaled: vec![0x30, 0x39],
            },
            DataType::Text,
        ),
        (
            "json_val",
            Value::Json(Box::new(serde_json::json!({"key": "value"}))),
            DataType::Json,
        ),
        (
            "list_val",
            Value::List(vec![Value::Integer(1), Value::Integer(2)]),
            DataType::List,
        ),
        (
            "set_val",
            Value::Set(vec![Value::Integer(3), Value::Integer(4)]),
            DataType::Set,
        ),
        (
            "map_val",
            Value::Map(vec![(Value::Text("key".to_string()), Value::Integer(5))]),
            DataType::Map,
        ),
        (
            "tuple_val",
            Value::Tuple(vec![Value::Integer(1), Value::Text("two".to_string())]),
            DataType::Tuple,
        ),
        (
            "udt_val",
            Value::Udt(Box::new(UdtValue {
                type_name: "address".to_string(),
                keyspace: "test".to_string(),
                fields: vec![
                    UdtField {
                        name: "street".to_string(),
                        value: Some(Value::Text("123 Main St".to_string())),
                    },
                    UdtField {
                        name: "city".to_string(),
                        value: Some(Value::Text("Springfield".to_string())),
                    },
                ],
            })),
            DataType::Udt,
        ),
        (
            "frozen_val",
            Value::Frozen(Box::new(Value::List(vec![Value::Integer(1)]))),
            DataType::Frozen,
        ),
        (
            "tombstone_val",
            Value::Tombstone(Box::new(TombstoneInfo {
                deletion_time: 1673778645000000,
                tombstone_type: TombstoneType::RowTombstone,
                local_deletion_time: 0,
                ttl: None,
                range_start: None,
                range_end: None,
            })),
            DataType::Tombstone,
        ),
    ];

    assert_eq!(variants.len(), 27, "Must test all 27 Value variants");

    for (name, value, data_type) in variants {
        let result = create_single_value_result(name, value.clone(), data_type);
        let csv_result = CSVWriter::write(&result, &default_config());

        assert!(
            csv_result.is_ok(),
            "Failed to serialize Value variant '{}' to CSV: {:?}. Value: {:?}",
            name,
            csv_result.err(),
            value
        );

        // Verify output has header and data row
        let csv_str = csv_result.unwrap();
        let lines: Vec<&str> = csv_str.lines().collect();
        assert!(
            lines.len() >= 2,
            "CSV for '{name}' should have header and data row. Got: {csv_str}"
        );
    }
}

// ============================================================================
// Test 4: NULL Values Consistent Across Formats
// ============================================================================

#[test]
fn test_null_representation_consistent() {
    // Test explicit Value::Null
    let result = create_single_value_result("nullable_col", Value::Null, DataType::Text);

    // JSON: must use null keyword
    let json = JSONWriter::write(&result, &default_config()).unwrap();
    assert!(
        json.contains("null"),
        "JSON must represent NULL as 'null' keyword. Got: {json}"
    );
    // Verify it's not the string "null"
    assert!(
        !json.contains("\"null\""),
        "JSON NULL should not be quoted string. Got: {json}"
    );

    // CSV: empty field (may be quoted "" or unquoted empty per RFC 4180)
    let csv = CSVWriter::write(&result, &default_config()).unwrap();
    let lines: Vec<&str> = csv.lines().collect();
    assert!(lines.len() >= 2);
    let data_line = lines[1];
    // Empty field can be "" (quoted empty) or truly empty - both are valid NULL representations
    assert!(
        data_line.is_empty() || data_line == "\"\"" || data_line.trim().is_empty(),
        "CSV NULL should be empty field or quoted empty. Got data line: '{data_line}'"
    );
}

#[test]
fn test_missing_column_treated_as_null() {
    // Create result with column defined but no value in row
    let columns = vec![ColumnInfo {
        name: "missing_col".to_string(),
        data_type: DataType::Text,
        nullable: true,
        position: 0,
        table_name: None,
        cql_type: None,
    }];

    let metadata = QueryMetadata {
        columns,
        ..Default::default()
    };

    // Row with empty HashMap - no value for missing_col
    let row = QueryRow {
        values: HashMap::new(),
        key: RowKey::new(vec![1]),
        metadata: Default::default(),
        cell_metadata: None,
    };

    let result = QueryResult {
        rows: vec![row],
        rows_affected: 0,
        execution_time_ms: 0,
        metadata,
    };

    // JSON: missing column should appear as null
    let json = JSONWriter::write(&result, &default_config()).unwrap();
    assert!(
        json.contains("null"),
        "JSON must include null for missing column. Got: {json}"
    );

    // CSV: should have empty field
    let csv = CSVWriter::write(&result, &default_config()).unwrap();
    let lines: Vec<&str> = csv.lines().collect();
    assert!(lines.len() >= 2);
}

// ============================================================================
// Test 5: Nested Collection Renders Correctly
// ============================================================================

#[test]
fn test_nested_collection_json_structure() {
    // List<Map<Text, Set<Int>>> - deeply nested structure from issue
    let nested = Value::List(vec![Value::Map(vec![(
        Value::Text("key".to_string()),
        Value::Set(vec![Value::Integer(1), Value::Integer(2)]),
    )])]);

    let result = create_single_value_result("nested_col", nested, DataType::List);

    let json = JSONWriter::write(&result, &default_config()).unwrap();

    // Parse and verify valid structure
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.len(), 1);

    // Navigate the nested structure
    let row = &parsed[0];
    let nested_col = row.get("nested_col").expect("nested_col should exist");
    assert!(nested_col.is_array(), "List should be JSON array");

    let outer_list = nested_col.as_array().unwrap();
    assert_eq!(outer_list.len(), 1);

    // Map is serialized as array of {key, value} objects
    let map_entry = &outer_list[0];
    assert!(map_entry.is_array(), "Map should be array of entries");
}

#[test]
fn test_deeply_nested_map_of_lists() {
    // Map<Text, List<Map<Text, Int>>> - another deep nesting pattern
    let nested = Value::Map(vec![(
        Value::Text("outer_key".to_string()),
        Value::List(vec![Value::Map(vec![(
            Value::Text("inner_key".to_string()),
            Value::Integer(42),
        )])]),
    )]);

    let result = create_single_value_result("deep_col", nested, DataType::Map);

    let json_result = JSONWriter::write(&result, &default_config());
    assert!(
        json_result.is_ok(),
        "Deep nesting should serialize: {:?}",
        json_result.err()
    );

    let json = json_result.unwrap();
    let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(&json);
    assert!(parsed.is_ok(), "Deep nested JSON should be valid");
}

// ============================================================================
// Test 6: UDT Fields Render With Names
// ============================================================================

#[test]
fn test_udt_renders_field_names_not_indices() {
    let udt = UdtValue {
        type_name: "address".to_string(),
        keyspace: "test_keyspace".to_string(),
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
                name: "zip_code".to_string(),
                value: Some(Value::Integer(12345)),
            },
        ],
    };

    let result = create_single_value_result("address_col", Value::Udt(Box::new(udt)), DataType::Udt);

    let json = JSONWriter::write(&result, &default_config()).unwrap();

    // UDT must contain field names, not numeric indices
    assert!(
        json.contains("street"),
        "UDT JSON must contain field name 'street'. Got: {json}"
    );
    assert!(
        json.contains("city"),
        "UDT JSON must contain field name 'city'. Got: {json}"
    );
    assert!(
        json.contains("zip_code"),
        "UDT JSON must contain field name 'zip_code'. Got: {json}"
    );

    // Should NOT contain numeric indices like "0", "1", "2" as field names
    // (Parse to verify structure)
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
    let row = &parsed[0];
    let address = row.get("address_col").unwrap();

    // UDT should be object with named fields
    if let Some(obj) = address.as_object() {
        // Check that keys are field names, not indices
        let keys: Vec<&String> = obj.keys().collect();
        assert!(
            !keys.contains(&&"0".to_string()),
            "UDT should use field names, not indices"
        );
        assert!(
            !keys.contains(&&"1".to_string()),
            "UDT should use field names, not indices"
        );
    }
}

#[test]
fn test_udt_with_null_field() {
    let udt = UdtValue {
        type_name: "person".to_string(),
        keyspace: "test".to_string(),
        fields: vec![
            UdtField {
                name: "name".to_string(),
                value: Some(Value::Text("John".to_string())),
            },
            UdtField {
                name: "nickname".to_string(),
                value: None, // Null field
            },
        ],
    };

    let result = create_single_value_result("person_col", Value::Udt(Box::new(udt)), DataType::Udt);

    let json = JSONWriter::write(&result, &default_config()).unwrap();

    // Field name should appear even if value is null
    assert!(
        json.contains("nickname"),
        "UDT should include null field name. Got: {json}"
    );
}

// ============================================================================
// Test 7: Column Order Matches Metadata
// ============================================================================

#[test]
fn test_csv_column_order_matches_metadata() {
    let result = create_result_with_columns(
        vec![
            ("zebra", DataType::Integer),
            ("apple", DataType::Integer),
            ("mango", DataType::Integer),
        ],
        vec![
            ("zebra", Value::Integer(3)),
            ("apple", Value::Integer(1)),
            ("mango", Value::Integer(2)),
        ],
    );

    let csv = CSVWriter::write(&result, &default_config()).unwrap();
    let header = csv.lines().next().unwrap();

    assert_eq!(
        header, "zebra,apple,mango",
        "CSV header must match metadata.columns order, not alphabetical"
    );
}

#[test]
fn test_csv_data_order_matches_header() {
    let result = create_result_with_columns(
        vec![
            ("col_z", DataType::Integer),
            ("col_a", DataType::Integer),
            ("col_m", DataType::Integer),
        ],
        vec![
            ("col_z", Value::Integer(100)),
            ("col_a", Value::Integer(200)),
            ("col_m", Value::Integer(300)),
        ],
    );

    let csv = CSVWriter::write(&result, &default_config()).unwrap();
    let lines: Vec<&str> = csv.lines().collect();

    assert!(lines.len() >= 2);
    let header = lines[0];
    let data = lines[1];

    assert_eq!(header, "col_z,col_a,col_m");
    assert_eq!(data, "100,200,300", "Data order must match header order");
}

#[test]
fn test_json_column_order_matches_metadata() {
    let result = create_result_with_columns(
        vec![
            ("third", DataType::Text),
            ("first", DataType::Text),
            ("second", DataType::Text),
        ],
        vec![
            ("third", Value::Text("c".to_string())),
            ("first", Value::Text("a".to_string())),
            ("second", Value::Text("b".to_string())),
        ],
    );

    let json = JSONWriter::write(&result, &default_config()).unwrap();

    // Parse and verify key order
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
    let row = parsed[0].as_object().unwrap();
    let keys: Vec<&String> = row.keys().collect();

    assert_eq!(
        keys,
        vec!["third", "first", "second"],
        "JSON keys must follow metadata.columns order"
    );
}
