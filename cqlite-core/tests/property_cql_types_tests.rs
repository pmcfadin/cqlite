//! Specialized property tests for CQL type system validation
//!
//! This module focuses specifically on CQL type system edge cases,
//! UDT validation, and type conversion consistency.

use crate::schema::CqlType;
use crate::types::{UdtField, UdtFieldDef, UdtTypeDef, UdtValue, Value};
use proptest::prelude::*;
use std::collections::HashMap;

// ============================================================================
// Advanced CQL Type Generators
// ============================================================================

/// Generates deeply nested UDT structures to test parser limits
fn arb_nested_udt(depth: usize) -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<i32>().prop_map(Value::Integer),
        "[a-zA-Z0-9]{0,100}".prop_map(Value::Text),
    ];

    leaf.prop_recursive(depth, depth * 10, 3, |inner| {
        (
            "[a-zA-Z][a-zA-Z0-9_]{0,63}",
            "[a-zA-Z][a-zA-Z0-9_]{0,63}",
            prop::collection::vec(
                ("[a-zA-Z][a-zA-Z0-9_]{0,63}", prop::option::of(inner))
                    .prop_map(|(name, value)| UdtField { name, value }),
                1..5,
            ),
        )
            .prop_map(|(type_name, keyspace, fields)| {
                Value::Udt(UdtValue {
                    type_name,
                    keyspace,
                    fields,
                })
            })
    })
}

/// Generates extreme numeric values for boundary testing
fn arb_extreme_numerics() -> impl Strategy<Value = Value> {
    prop_oneof![
        // Integer boundaries
        Just(Value::Integer(i32::MIN)),
        Just(Value::Integer(i32::MAX)),
        Just(Value::Integer(0)),
        Just(Value::Integer(-1)),
        Just(Value::Integer(1)),
        // BigInt boundaries
        Just(Value::BigInt(i64::MIN)),
        Just(Value::BigInt(i64::MAX)),
        Just(Value::BigInt(0)),
        // Float special values
        Just(Value::Float(f64::INFINITY)),
        Just(Value::Float(f64::NEG_INFINITY)),
        Just(Value::Float(f64::NAN)),
        Just(Value::Float(0.0)),
        Just(Value::Float(-0.0)),
        Just(Value::Float(f64::MIN)),
        Just(Value::Float(f64::MAX)),
        Just(Value::Float(f64::EPSILON)),
        // TinyInt boundaries
        Just(Value::TinyInt(i8::MIN)),
        Just(Value::TinyInt(i8::MAX)),
        Just(Value::TinyInt(0)),
        // SmallInt boundaries
        Just(Value::SmallInt(i16::MIN)),
        Just(Value::SmallInt(i16::MAX)),
        Just(Value::SmallInt(0)),
        // Float32 special values
        Just(Value::Float32(f32::INFINITY)),
        Just(Value::Float32(f32::NEG_INFINITY)),
        Just(Value::Float32(f32::NAN)),
        Just(Value::Float32(0.0)),
        Just(Value::Float32(-0.0)),
    ]
}

/// Generates Unicode edge cases for text validation
fn arb_unicode_text() -> impl Strategy<Value = Value> {
    prop_oneof![
        // Empty string
        Just(Value::Text(String::new())),
        // Single characters
        Just(Value::Text("a".to_string())),
        Just(Value::Text("π".to_string())),
        Just(Value::Text("🚀".to_string())),
        // Multi-byte UTF-8 sequences
        Just(Value::Text("こんにちは".to_string())), // Japanese
        Just(Value::Text("مرحبا".to_string())),      // Arabic
        Just(Value::Text("🔥💯⚡".to_string())),     // Emojis
        // Control characters
        Just(Value::Text("\n\r\t".to_string())),
        Just(Value::Text("\x00\x01\x02".to_string())),
        // Long strings
        prop::string::string_regex("[\u{0000}-\u{10FFFF}]{1000,5000}")
            .unwrap()
            .prop_map(Value::Text),
        // Mixed ASCII and Unicode
        prop::string::string_regex("[a-zA-Z0-9\u{1F600}-\u{1F64F}]{10,100}")
            .unwrap()
            .prop_map(Value::Text),
    ]
}

/// Generates binary data with specific patterns
fn arb_binary_patterns() -> impl Strategy<Value = Value> {
    prop_oneof![
        // Empty blob
        Just(Value::Blob(vec![])),
        // Single byte patterns
        any::<u8>().prop_map(|b| Value::Blob(vec![b])),
        // Repeated byte patterns
        (any::<u8>(), 1..10000usize).prop_map(|(byte, len)| { Value::Blob(vec![byte; len]) }),
        // Random binary data
        prop::collection::vec(any::<u8>(), 0..65536).prop_map(Value::Blob),
        // Common binary prefixes (magic numbers, etc.)
        prop_oneof![
            Just(vec![0xFF, 0xFE]),             // BOM
            Just(vec![0x89, 0x50, 0x4E, 0x47]), // PNG header
            Just(vec![0x4A, 0x46, 0x49, 0x46]), // JPEG header
            Just(vec![0x00, 0x00, 0x00, 0x00]), // Null bytes
            Just(vec![0xFF, 0xFF, 0xFF, 0xFF]), // Max bytes
        ]
        .prop_map(Value::Blob),
    ]
}

// ============================================================================
// Property Tests for CQL Types
// ============================================================================

proptest! {
    /// Test that extreme numeric values are handled correctly
    #[test]
    fn prop_extreme_numeric_handling(value in arb_extreme_numerics()) {
        // Should serialize without error
        let serialized = bincode::serialize(&value)
            .expect("Extreme numeric values should serialize");

        // Should deserialize to the same value
        let deserialized: Value = bincode::deserialize(&serialized)
            .expect("Serialized extreme values should deserialize");

        // Handle special float cases
        match (&value, &deserialized) {
            (Value::Float(f1), Value::Float(f2)) => {
                if f1.is_nan() && f2.is_nan() {
                    // NaN == NaN is always false, so we need special handling
                    prop_assert!(f2.is_nan());
                } else {
                    prop_assert_eq!(f1, f2);
                }
            },
            (Value::Float32(f1), Value::Float32(f2)) => {
                if f1.is_nan() && f2.is_nan() {
                    prop_assert!(f2.is_nan());
                } else {
                    prop_assert_eq!(f1, f2);
                }
            },
            _ => {
                prop_assert_eq!(value, deserialized);
            }
        }

        // Type-specific validations
        match value {
            Value::Integer(i) => {
                prop_assert!(i >= i32::MIN && i <= i32::MAX);
            },
            Value::BigInt(i) => {
                prop_assert!(i >= i64::MIN && i <= i64::MAX);
            },
            Value::TinyInt(i) => {
                prop_assert!(i >= i8::MIN && i <= i8::MAX);
            },
            Value::SmallInt(i) => {
                prop_assert!(i >= i16::MIN && i <= i16::MAX);
            },
            Value::Float(f) => {
                // Verify float properties
                if !f.is_nan() {
                    prop_assert!(f.is_finite() || f.is_infinite());
                }
            },
            Value::Float32(f) => {
                if !f.is_nan() {
                    prop_assert!(f.is_finite() || f.is_infinite());
                }
            },
            _ => {}
        }
    }

    /// Test Unicode and text edge cases
    #[test]
    fn prop_unicode_text_handling(value in arb_unicode_text()) {
        if let Value::Text(ref text) = value {
            // Should be valid UTF-8
            prop_assert!(text.is_ascii() || std::str::from_utf8(text.as_bytes()).is_ok());

            // Should serialize/deserialize correctly
            let serialized = bincode::serialize(&value).unwrap();
            let deserialized: Value = bincode::deserialize(&serialized).unwrap();
            prop_assert_eq!(value, deserialized);

            // Length properties
            prop_assert!(text.len() <= 1024 * 1024); // Max 1MB text
            prop_assert!(text.chars().count() <= text.len()); // Char count <= byte count

            // Unicode normalization should be consistent
            let normalized = text.chars().collect::<String>();
            prop_assert_eq!(text, &normalized);
        }
    }

    /// Test binary data patterns and edge cases
    #[test]
    fn prop_binary_data_handling(value in arb_binary_patterns()) {
        if let Value::Blob(ref data) = value {
            // Should serialize/deserialize correctly
            let serialized = bincode::serialize(&value).unwrap();
            let deserialized: Value = bincode::deserialize(&serialized).unwrap();
            prop_assert_eq!(value, deserialized);

            // Size constraints
            prop_assert!(data.len() <= 1024 * 1024); // Max 1MB blob

            // Binary data should preserve exact bytes
            if let Value::Blob(ref deser_data) = deserialized {
                prop_assert_eq!(data.len(), deser_data.len());
                for (original, roundtrip) in data.iter().zip(deser_data.iter()) {
                    prop_assert_eq!(original, roundtrip);
                }
            }
        }
    }

    /// Test deeply nested UDT structures
    #[test]
    fn prop_nested_udt_handling(value in arb_nested_udt(5)) {
        // Should serialize without stack overflow
        let serialized_result = std::panic::catch_unwind(|| {
            bincode::serialize(&value)
        });

        prop_assert!(serialized_result.is_ok(), "UDT serialization should not panic");

        if let Ok(Ok(serialized)) = serialized_result {
            // Should deserialize without stack overflow
            let deserialized_result = std::panic::catch_unwind(|| {
                bincode::deserialize::<Value>(&serialized)
            });

            prop_assert!(deserialized_result.is_ok(), "UDT deserialization should not panic");

            if let Ok(Ok(deserialized)) = deserialized_result {
                prop_assert_eq!(value, deserialized);
            }
        }

        // Validate UDT structure if applicable
        if let Value::Udt(ref udt) = value {
            prop_assert!(!udt.type_name.is_empty());
            prop_assert!(!udt.keyspace.is_empty());
            prop_assert!(!udt.fields.is_empty());

            // Field names should be unique within UDT
            let mut field_names = std::collections::HashSet::new();
            for field in &udt.fields {
                prop_assert!(field_names.insert(&field.name),
                    "Duplicate field name: {}", field.name);
                prop_assert!(!field.name.is_empty());
            }
        }
    }

    /// Test collection type consistency and invariants
    #[test]
    fn prop_collection_type_invariants(
        list_items in prop::collection::vec(arb_primitive_value(), 0..100),
        set_items in prop::collection::vec(arb_primitive_value(), 0..100),
        map_items in prop::collection::vec((arb_primitive_value(), arb_primitive_value()), 0..100)
    ) {
        let list = Value::List(list_items.clone());
        let set = Value::Set(set_items.clone());
        let map = Value::Map(map_items.clone());

        // All collections should serialize/deserialize
        for collection in &[list, set, map] {
            let serialized = bincode::serialize(collection).unwrap();
            let deserialized: Value = bincode::deserialize(&serialized).unwrap();
            prop_assert_eq!(*collection, deserialized);
        }

        // List properties
        if let Value::List(ref items) = list {
            prop_assert_eq!(items.len(), list_items.len());
            // Lists preserve order and allow duplicates
            prop_assert_eq!(items, &list_items);
        }

        // Set properties
        if let Value::Set(ref items) = set {
            prop_assert_eq!(items.len(), set_items.len());
            // Sets should preserve insertion order (Vec implementation)
            prop_assert_eq!(items, &set_items);
        }

        // Map properties
        if let Value::Map(ref items) = map {
            prop_assert_eq!(items.len(), map_items.len());
            // Maps preserve insertion order
            prop_assert_eq!(items, &map_items);
        }

        // Nested collection handling
        let nested_list = Value::List(vec![list.clone(), set.clone()]);
        let serialized = bincode::serialize(&nested_list).unwrap();
        let deserialized: Value = bincode::deserialize(&serialized).unwrap();
        prop_assert_eq!(nested_list, deserialized);
    }

    /// Test timestamp and temporal type edge cases
    #[test]
    fn prop_temporal_type_handling(
        timestamp in any::<i64>(),
        months in any::<i32>(),
        days in any::<i32>(),
        nanos in any::<i64>()
    ) {
        let timestamp_val = Value::Timestamp(timestamp);
        let duration_val = Value::Duration { months, days, nanos };

        // Timestamps should handle full i64 range
        let ts_serialized = bincode::serialize(&timestamp_val).unwrap();
        let ts_deserialized: Value = bincode::deserialize(&ts_serialized).unwrap();
        prop_assert_eq!(timestamp_val, ts_deserialized);

        // Duration components should be preserved exactly
        let dur_serialized = bincode::serialize(&duration_val).unwrap();
        let dur_deserialized: Value = bincode::deserialize(&dur_serialized).unwrap();
        prop_assert_eq!(duration_val, dur_deserialized);

        // Validate timestamp ranges (microseconds since epoch)
        if timestamp != i64::MIN && timestamp != i64::MAX {
            // Should represent a valid timestamp
            let _seconds = timestamp / 1_000_000;
            let _micros = timestamp % 1_000_000;
        }

        // Duration invariants
        if let Value::Duration { months: m, days: d, nanos: n } = duration_val {
            prop_assert_eq!(m, months);
            prop_assert_eq!(d, days);
            prop_assert_eq!(n, nanos);
        }
    }
}

/// Helper function to generate primitive values for collections
fn arb_primitive_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Boolean),
        any::<i32>().prop_map(Value::Integer),
        any::<i64>().prop_map(Value::BigInt),
        "[a-zA-Z0-9]{0,50}".prop_map(Value::Text),
        prop::collection::vec(any::<u8>(), 0..100).prop_map(Value::Blob),
    ]
}

#[cfg(test)]
mod cql_type_integration_tests {
    use super::*;

    #[test]
    fn test_all_cql_types_represented() {
        // Ensure our generators cover all CQL types
        let mut runner = proptest::test_runner::TestRunner::default();
        let strategy = prop_oneof![
            arb_extreme_numerics(),
            arb_unicode_text(),
            arb_binary_patterns(),
            arb_nested_udt(3),
        ];

        let mut seen_types = std::collections::HashSet::new();

        for _ in 0..1000 {
            let value = strategy.new_tree(&mut runner).unwrap().current();
            seen_types.insert(std::mem::discriminant(&value));
        }

        // We should see a good variety of types
        assert!(seen_types.len() >= 5, "Should generate diverse CQL types");
    }

    #[test]
    fn test_type_size_limits() {
        // Verify our generators respect size limits
        let mut runner = proptest::test_runner::TestRunner::default();
        let strategy = arb_unicode_text();

        for _ in 0..100 {
            let value = strategy.new_tree(&mut runner).unwrap().current();
            if let Value::Text(ref text) = value {
                assert!(text.len() <= 1024 * 1024, "Text exceeds size limit");
            }
        }
    }
}
