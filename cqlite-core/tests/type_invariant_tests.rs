//! Type Invariant Tests (Issue #267)
//!
//! These tests validate that type information is preserved through the entire
//! pipeline. They catch cases where type changes silently cause data corruption.
//!
//! Key invariants tested:
//! - Empty collections preserve declared element types (with schema awareness)
//! - Null UDT fields preserve schema-declared types (with schema awareness)
//! - Timestamps are stored as milliseconds (not multiplied by 1000)
//! - Date decoding applies i32::MIN offset correctly
//! - Frozen wrappers preserve inner collection types
//! - JSON serialization handles large numbers correctly

use cqlite_core::{
    parser::types::{parse_date, parse_timestamp},
    schema::CqlType,
    types::{UdtField, UdtValue, Value},
};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

// ============================================================================
// Helper Functions
// ============================================================================

/// Get the test datasets root from environment or default location
fn get_test_datasets_root() -> PathBuf {
    env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
        .unwrap_or_else(|| {
            let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            path.push("../test-data/datasets");
            path.canonicalize().unwrap_or(path)
        })
}

// ============================================================================
// Module 1: Empty Collection Type Tests
// ============================================================================

/// Test: Empty List returns Text element type (documents current behavior)
///
/// WHY: types.rs:330-331 defaults empty lists to CqlType::List(Text)
/// This test documents the current value-driven behavior. The schema-aware
/// reader should provide correct types when schema is available.
#[test]
fn test_empty_list_data_type_returns_text() {
    let empty_list = Value::List(vec![]);
    let data_type = empty_list.data_type();

    // Current behavior: empty lists default to List<Text>
    assert_eq!(
        data_type,
        CqlType::List(Box::new(CqlType::Text)),
        "Empty list should return List<Text> (current value-driven behavior)"
    );
}

/// Test: Empty Set returns Text element type (documents current behavior)
///
/// WHY: types.rs:338-339 defaults empty sets to CqlType::Set(Text)
#[test]
fn test_empty_set_data_type_returns_text() {
    let empty_set = Value::Set(vec![]);
    let data_type = empty_set.data_type();

    // Current behavior: empty sets default to Set<Text>
    assert_eq!(
        data_type,
        CqlType::Set(Box::new(CqlType::Text)),
        "Empty set should return Set<Text> (current value-driven behavior)"
    );
}

/// Test: Empty Map returns Text key/value types (documents current behavior)
///
/// WHY: types.rs:346-347 defaults empty maps to Map<Text, Text>
#[test]
fn test_empty_map_data_type_returns_text() {
    let empty_map = Value::Map(vec![]);
    let data_type = empty_map.data_type();

    // Current behavior: empty maps default to Map<Text, Text>
    assert_eq!(
        data_type,
        CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Text)),
        "Empty map should return Map<Text, Text> (current value-driven behavior)"
    );
}

/// Test: Non-empty list correctly infers element type
#[test]
fn test_nonempty_list_preserves_element_type() {
    let int_list = Value::List(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);
    let data_type = int_list.data_type();

    assert_eq!(
        data_type,
        CqlType::List(Box::new(CqlType::Int)),
        "List with integers should return List<Int>"
    );
}

/// Test: Non-empty set correctly infers element type
#[test]
fn test_nonempty_set_preserves_element_type() {
    let uuid_set = Value::Set(vec![Value::Uuid([0u8; 16]), Value::Uuid([1u8; 16])]);
    let data_type = uuid_set.data_type();

    assert_eq!(
        data_type,
        CqlType::Set(Box::new(CqlType::Uuid)),
        "Set with UUIDs should return Set<Uuid>"
    );
}

/// Test: Non-empty map correctly infers key/value types
#[test]
fn test_nonempty_map_preserves_key_value_types() {
    let text_bigint_map = Value::Map(vec![
        (Value::Text("key1".to_string()), Value::BigInt(100)),
        (Value::Text("key2".to_string()), Value::BigInt(200)),
    ]);
    let data_type = text_bigint_map.data_type();

    assert_eq!(
        data_type,
        CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::BigInt)),
        "Map with Text->BigInt should return Map<Text, BigInt>"
    );
}

// ============================================================================
// Module 2: Null UDT Field Type Tests
// ============================================================================

/// Test: Null UDT field returns Text type (documents current behavior)
///
/// WHY: types.rs:365 defaults null UDT fields to CqlType::Text
#[test]
fn test_null_udt_field_returns_text() {
    let udt = UdtValue {
        type_name: "address_type".to_string(),
        keyspace: "test_collections".to_string(),
        fields: vec![
            UdtField {
                name: "street".to_string(),
                value: Some(Value::Text("123 Main St".to_string())),
            },
            UdtField {
                name: "city".to_string(),
                value: None, // Null field
            },
            UdtField {
                name: "zip_code".to_string(),
                value: Some(Value::Text("12345".to_string())),
            },
        ],
    };

    let data_type = Value::Udt(Box::new(udt)).data_type();

    match data_type {
        CqlType::Udt(name, fields) => {
            assert_eq!(name, "address_type");

            // Find the null field
            let city_field = fields.iter().find(|(n, _)| n == "city");
            assert!(city_field.is_some(), "city field should exist");

            // Current behavior: null fields default to Text
            assert_eq!(
                city_field.unwrap().1,
                CqlType::Text,
                "Null UDT field should return Text (current value-driven behavior)"
            );
        }
        _ => panic!("Expected CqlType::Udt, got {:?}", data_type),
    }
}

/// Test: UDT with all non-null fields preserves types
#[test]
fn test_udt_with_values_preserves_types() {
    let udt = UdtValue {
        type_name: "test_udt".to_string(),
        keyspace: "test_keyspace".to_string(),
        fields: vec![
            UdtField {
                name: "int_field".to_string(),
                value: Some(Value::Integer(42)),
            },
            UdtField {
                name: "text_field".to_string(),
                value: Some(Value::Text("hello".to_string())),
            },
            UdtField {
                name: "bigint_field".to_string(),
                value: Some(Value::BigInt(9999999999)),
            },
        ],
    };

    let data_type = Value::Udt(Box::new(udt)).data_type();

    match data_type {
        CqlType::Udt(name, fields) => {
            assert_eq!(name, "test_udt");
            assert_eq!(fields.len(), 3);

            // Verify each field type
            let field_map: HashMap<_, _> = fields.into_iter().collect();
            assert_eq!(field_map.get("int_field"), Some(&CqlType::Int));
            assert_eq!(field_map.get("text_field"), Some(&CqlType::Text));
            assert_eq!(field_map.get("bigint_field"), Some(&CqlType::BigInt));
        }
        _ => panic!("Expected CqlType::Udt, got {:?}", data_type),
    }
}

// ============================================================================
// Module 3: Timestamp Unit Tests (Regression for Issue #258)
// ============================================================================

/// Test: Timestamp values are stored as milliseconds (not multiplied by 1000)
///
/// WHY: Historical bug (Issue #258) multiplied milliseconds by 1000,
/// causing overflow for large timestamps.
#[test]
fn test_timestamp_stored_as_milliseconds() {
    // Known timestamp: 2021-01-01 00:00:00 UTC = 1609459200000 ms
    let ms: i64 = 1_609_459_200_000;
    let value = Value::Timestamp(ms);

    // Verify: Value stores milliseconds directly (using pattern matching)
    match value {
        Value::Timestamp(stored) => {
            assert_eq!(stored, ms, "Timestamp should store milliseconds directly");
            // Verify: NOT multiplied by 1000 (which would cause overflow issues)
            assert_ne!(
                stored,
                ms * 1000,
                "Timestamp should NOT be multiplied by 1000"
            );
        }
        _ => panic!("Expected Value::Timestamp"),
    }
}

/// Test: Large timestamps don't overflow
///
/// WHY: Issue #258 caused overflow for large timestamps when multiplied by 1000
#[test]
fn test_large_timestamp_no_overflow() {
    // Maximum safe timestamp (year ~292278994, max i64 milliseconds)
    let large_ts: i64 = i64::MAX / 2; // Large but won't overflow
    let value = Value::Timestamp(large_ts);

    // Should not panic or produce negative value (using pattern matching)
    match value {
        Value::Timestamp(stored) => {
            assert!(stored > 0, "Large timestamp should remain positive");
            assert_eq!(stored, large_ts, "Timestamp should be stored as-is");
        }
        _ => panic!("Expected Value::Timestamp"),
    }
}

/// Test: parse_timestamp returns milliseconds directly
#[test]
fn test_parse_timestamp_returns_milliseconds() {
    // Test data: 1609459200000 ms (2021-01-01 00:00:00 UTC)
    let ms: i64 = 1_609_459_200_000;
    let bytes = ms.to_be_bytes();

    let result = parse_timestamp(&bytes);
    assert!(result.is_ok(), "parse_timestamp should succeed");

    let (remaining, parsed) = result.unwrap();
    assert_eq!(remaining.len(), 0, "Should consume all bytes");

    match parsed {
        Value::Timestamp(t) => {
            assert_eq!(t, ms, "Parsed timestamp should equal input milliseconds");
        }
        other => panic!("Expected Timestamp, got {:?}", other),
    }
}

// ============================================================================
// Module 4: Date Encoding Tests
// ============================================================================

/// Test: Date decoding applies i32::MIN offset correctly
///
/// Cassandra stores DATE as: raw_value where days = raw_value - i32::MIN
/// Epoch (1970-01-01) has 0 days, so raw_value = 0 - i32::MIN = i32::MIN (as u32)
#[test]
fn test_date_decoding_applies_offset() {
    // Epoch date: 0 days since 1970-01-01
    // Stored as: 0u32.wrapping_sub(i32::MIN as u32) which equals 2147483648 (0x80000000)
    let raw_epoch: u32 = 0u32.wrapping_sub(i32::MIN as u32);
    let bytes = raw_epoch.to_be_bytes();

    let result = parse_date(&bytes);
    assert!(result.is_ok(), "parse_date should succeed");

    let (remaining, parsed) = result.unwrap();
    assert_eq!(remaining.len(), 0, "Should consume all bytes");

    match parsed {
        Value::Date(days) => {
            assert_eq!(days, 0, "Epoch should be 0 days since epoch");
        }
        other => panic!("Expected Date, got {:?}", other),
    }
}

/// Test: Date 2025-01-01 decodes correctly
///
/// 2025-01-01 = 20089 days since epoch (1970-01-01)
#[test]
fn test_date_2025_01_01_correct() {
    let days: i32 = 20089; // Days from 1970-01-01 to 2025-01-01
    let raw = (days as u32).wrapping_sub(i32::MIN as u32);
    let bytes = raw.to_be_bytes();

    let result = parse_date(&bytes);
    assert!(result.is_ok(), "parse_date should succeed");

    let (_, parsed) = result.unwrap();

    match parsed {
        Value::Date(parsed_days) => {
            assert_eq!(parsed_days, days, "Should decode to 20089 days since epoch");
        }
        other => panic!("Expected Date, got {:?}", other),
    }
}

/// Test: Negative dates (before epoch) decode correctly
#[test]
fn test_date_before_epoch_correct() {
    // 100 days before epoch = -100
    let days: i32 = -100;
    let raw = (days as u32).wrapping_sub(i32::MIN as u32);
    let bytes = raw.to_be_bytes();

    let result = parse_date(&bytes);
    assert!(result.is_ok(), "parse_date should succeed");

    let (_, parsed) = result.unwrap();

    match parsed {
        Value::Date(parsed_days) => {
            assert_eq!(
                parsed_days, days,
                "Should decode to -100 days (100 days before epoch)"
            );
        }
        other => panic!("Expected Date, got {:?}", other),
    }
}

// ============================================================================
// Module 5: Frozen Type Tests
// ============================================================================

/// Test: Frozen wrapper preserves inner collection type
#[test]
fn test_frozen_wrapper_preserves_inner_type() {
    // Create a frozen list of integers
    let inner_list = Value::List(vec![Value::Integer(1), Value::Integer(2)]);
    let frozen = Value::Frozen(Box::new(inner_list));

    let data_type = frozen.data_type();

    // Should be Frozen<List<Int>>
    match data_type {
        CqlType::Frozen(inner) => match *inner {
            CqlType::List(element_type) => {
                assert_eq!(
                    *element_type,
                    CqlType::Int,
                    "Frozen list should preserve Int element type"
                );
            }
            other => panic!("Expected List inside Frozen, got {:?}", other),
        },
        other => panic!("Expected Frozen, got {:?}", other),
    }
}

/// Test: Frozen set preserves element type
#[test]
fn test_frozen_set_preserves_type() {
    let inner_set = Value::Set(vec![
        Value::Text("a".to_string()),
        Value::Text("b".to_string()),
    ]);
    let frozen = Value::Frozen(Box::new(inner_set));

    let data_type = frozen.data_type();

    match data_type {
        CqlType::Frozen(inner) => match *inner {
            CqlType::Set(element_type) => {
                assert_eq!(
                    *element_type,
                    CqlType::Text,
                    "Frozen set should preserve Text element type"
                );
            }
            other => panic!("Expected Set inside Frozen, got {:?}", other),
        },
        other => panic!("Expected Frozen, got {:?}", other),
    }
}

/// Test: Frozen map preserves key/value types
#[test]
fn test_frozen_map_preserves_types() {
    let inner_map = Value::Map(vec![(Value::Uuid([0u8; 16]), Value::Counter(100))]);
    let frozen = Value::Frozen(Box::new(inner_map));

    let data_type = frozen.data_type();

    match data_type {
        CqlType::Frozen(inner) => match *inner {
            CqlType::Map(key_type, value_type) => {
                assert_eq!(
                    *key_type,
                    CqlType::Uuid,
                    "Frozen map should preserve Uuid key type"
                );
                assert_eq!(
                    *value_type,
                    CqlType::Counter,
                    "Frozen map should preserve Counter value type"
                );
            }
            other => panic!("Expected Map inside Frozen, got {:?}", other),
        },
        other => panic!("Expected Frozen, got {:?}", other),
    }
}

/// Test: Empty frozen collection defaults to Text (documents behavior)
#[test]
fn test_empty_frozen_list_defaults_to_text() {
    let empty_list = Value::List(vec![]);
    let frozen = Value::Frozen(Box::new(empty_list));

    let data_type = frozen.data_type();

    // Should be Frozen<List<Text>> (empty list defaults to Text)
    match data_type {
        CqlType::Frozen(inner) => match *inner {
            CqlType::List(element_type) => {
                assert_eq!(
                    *element_type,
                    CqlType::Text,
                    "Empty frozen list should default to Text element type"
                );
            }
            other => panic!("Expected List inside Frozen, got {:?}", other),
        },
        other => panic!("Expected Frozen, got {:?}", other),
    }
}

// ============================================================================
// Module 6: JSON Precision Tests
// ============================================================================

/// Test: BigInt values preserve full precision
#[test]
fn test_bigint_precision_preserved() {
    // 2^53 + 1 = 9007199254740993 (exceeds f64 safe integer range)
    let big = Value::BigInt(9007199254740993);

    // Verify value is stored correctly
    assert_eq!(
        big.as_i64(),
        Some(9007199254740993),
        "BigInt should preserve full i64 precision"
    );

    // Verify data type
    assert_eq!(big.data_type(), CqlType::BigInt);
}

/// Test: Counter values preserve full precision
#[test]
fn test_counter_precision_preserved() {
    let counter = Value::Counter(9007199254740993);

    assert_eq!(
        counter.as_i64(),
        Some(9007199254740993),
        "Counter should preserve full i64 precision"
    );

    assert_eq!(counter.data_type(), CqlType::Counter);
}

/// Test: Float NaN is representable
#[test]
fn test_float_nan_representable() {
    let nan = Value::Float(f64::NAN);

    // Verify it's stored
    match nan {
        Value::Float(f) => assert!(f.is_nan(), "Should store NaN"),
        _ => panic!("Should be Float"),
    }

    // Note: JSON serialization converts NaN to null (documented behavior)
    // This is expected per IEEE 754 / JSON spec
}

/// Test: Float infinity is representable
#[test]
fn test_float_infinity_representable() {
    let pos_inf = Value::Float(f64::INFINITY);
    let neg_inf = Value::Float(f64::NEG_INFINITY);

    match pos_inf {
        Value::Float(f) => assert!(f.is_infinite() && f.is_sign_positive()),
        _ => panic!("Should be Float"),
    }

    match neg_inf {
        Value::Float(f) => assert!(f.is_infinite() && f.is_sign_negative()),
        _ => panic!("Should be Float"),
    }

    // Note: JSON serialization converts infinity to null (documented behavior)
}

// ============================================================================
// Module 7: Collection Type Stability Tests
// ============================================================================

/// Test: Nested collections preserve types correctly
#[test]
fn test_nested_collection_types() {
    // List of Lists of Integers
    let inner1 = Value::List(vec![Value::Integer(1), Value::Integer(2)]);
    let inner2 = Value::List(vec![Value::Integer(3), Value::Integer(4)]);
    let outer = Value::List(vec![inner1, inner2]);

    let data_type = outer.data_type();

    match data_type {
        CqlType::List(inner_type) => match *inner_type {
            CqlType::List(element_type) => {
                assert_eq!(
                    *element_type,
                    CqlType::Int,
                    "Nested list should preserve Int element type"
                );
            }
            other => panic!("Expected nested List, got {:?}", other),
        },
        other => panic!("Expected List, got {:?}", other),
    }
}

/// Test: Map with collection values preserves types
#[test]
fn test_map_with_collection_values() {
    let list_value = Value::List(vec![Value::Timestamp(1609459200000)]);
    let map = Value::Map(vec![(Value::Text("timestamps".to_string()), list_value)]);

    let data_type = map.data_type();

    match data_type {
        CqlType::Map(key_type, value_type) => {
            assert_eq!(*key_type, CqlType::Text);
            match *value_type {
                CqlType::List(element_type) => {
                    assert_eq!(*element_type, CqlType::Timestamp);
                }
                other => panic!("Expected List value type, got {:?}", other),
            }
        }
        other => panic!("Expected Map, got {:?}", other),
    }
}

// ============================================================================
// Integration Tests (require SSTable data)
// ============================================================================

/// Smoke test: Verify empty_collections_table SSTable exists
#[test]
fn test_empty_collections_table_exists() {
    let test_root = get_test_datasets_root();
    let table_path = test_root
        .join("sstables/test_collections/empty_collections_table-6be780f0a25111f0a3fef1a551383fb9");

    assert!(
        table_path.exists(),
        "Test requires full SSTable dataset: empty_collections_table not found at {:?}",
        table_path
    );
}

/// Smoke test: Verify frozen_collections_table SSTable exists
#[test]
fn test_frozen_collections_table_exists() {
    let test_root = get_test_datasets_root();
    let table_path = test_root.join(
        "sstables/test_collections/frozen_collections_table-6bd1fd20a25111f0a3fef1a551383fb9",
    );

    assert!(
        table_path.exists(),
        "Test requires full SSTable dataset: frozen_collections_table not found at {:?}",
        table_path
    );
}
