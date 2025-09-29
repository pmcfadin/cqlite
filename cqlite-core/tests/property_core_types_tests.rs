//! Standalone property-based tests for core CQL types
//!
//! This module provides comprehensive property-based testing for the core Value
//! enum and related types without dependencies on potentially problematic modules.

use cqlite_core::types::{TombstoneInfo, UdtField, UdtValue, Value};
use proptest::prelude::*;
use std::collections::HashMap;

// ============================================================================
// Core Type Generators
// ============================================================================

/// Generates arbitrary primitive Value instances
fn arb_primitive_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Boolean),
        any::<i32>().prop_map(Value::Integer),
        any::<i64>().prop_map(Value::BigInt),
        any::<f64>().prop_map(Value::Float),
        any::<f32>().prop_map(Value::Float32),
        any::<i8>().prop_map(Value::TinyInt),
        any::<i16>().prop_map(Value::SmallInt),
        "[a-zA-Z0-9 ]{0,1000}".prop_map(Value::Text),
        prop::collection::vec(any::<u8>(), 0..1000).prop_map(Value::Blob),
        any::<i64>().prop_map(Value::Timestamp),
        any::<[u8; 16]>().prop_map(Value::Uuid),
        prop::collection::vec(any::<u8>(), 1..32).prop_map(Value::Varint),
        arb_decimal(),
        arb_duration(),
        arb_json(),
    ]
}

/// Generates arbitrary collection Value instances
fn arb_collection_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        prop::collection::vec(arb_primitive_value(), 0..10).prop_map(Value::List),
        prop::collection::vec(arb_primitive_value(), 0..10).prop_map(Value::Set),
        prop::collection::vec((arb_primitive_value(), arb_primitive_value()), 0..10)
            .prop_map(Value::Map),
        prop::collection::vec(arb_primitive_value(), 0..5).prop_map(Value::Tuple),
        arb_udt(),
        arb_primitive_value().prop_map(|v| Value::Frozen(Box::new(v))),
        arb_tombstone(),
    ]
}

/// Generates any Value type
fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![arb_primitive_value(), arb_collection_value(),]
}

/// Generates decimal values
fn arb_decimal() -> impl Strategy<Value = Value> {
    (any::<i32>(), prop::collection::vec(any::<u8>(), 1..32))
        .prop_map(|(scale, unscaled)| Value::Decimal { scale, unscaled })
}

/// Generates duration values
fn arb_duration() -> impl Strategy<Value = Value> {
    (any::<i32>(), any::<i32>(), any::<i64>()).prop_map(|(months, days, nanos)| Value::Duration {
        months,
        days,
        nanos,
    })
}

/// Generates JSON values
fn arb_json() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::Bool),
        any::<i64>().prop_map(|n| serde_json::Value::Number(n.into())),
        "[a-zA-Z0-9 ]{0,100}".prop_map(serde_json::Value::String),
    ]
    .prop_map(Value::Json)
}

/// Generates UDT values
fn arb_udt() -> impl Strategy<Value = Value> {
    (
        "[a-zA-Z][a-zA-Z0-9_]{0,63}",
        "[a-zA-Z][a-zA-Z0-9_]{0,63}",
        prop::collection::vec(arb_udt_field(), 0..5),
    )
        .prop_map(|(type_name, keyspace, fields)| {
            Value::Udt(UdtValue {
                type_name,
                keyspace,
                fields,
            })
        })
}

/// Generates UDT fields
fn arb_udt_field() -> impl Strategy<Value = UdtField> {
    (
        "[a-zA-Z][a-zA-Z0-9_]{0,63}",
        prop::option::of(arb_primitive_value()),
    )
        .prop_map(|(name, value)| UdtField { name, value })
}

/// Generates tombstone values
fn arb_tombstone() -> impl Strategy<Value = Value> {
    (any::<i64>(), any::<i32>()).prop_map(|(deletion_time, local_deletion_time)| {
        Value::Tombstone(TombstoneInfo {
            deletion_time,
            local_deletion_time,
        })
    })
}

// ============================================================================
// Property Tests
// ============================================================================

proptest! {
    /// Test that all CQL types can roundtrip through serialization/deserialization
    #[test]
    fn prop_all_cql_types_roundtrip(value in arb_value()) {
        // Serialize to bytes
        let serialized = bincode::serialize(&value)
            .expect("All CQL values should serialize successfully");

        // Deserialize back
        let deserialized: Value = bincode::deserialize(&serialized)
            .expect("Serialized CQL values should deserialize successfully");

        // Values should be identical (with special handling for NaN)
        match (&value, &deserialized) {
            (Value::Float(f1), Value::Float(f2)) => {
                if f1.is_nan() && f2.is_nan() {
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

        // Additional type-specific validation
        validate_value_invariants(&value)?;
    }

    /// Test numeric boundary values and special cases
    #[test]
    fn prop_numeric_boundaries(
        int_val in prop_oneof![
            Just(i32::MIN), Just(i32::MAX), Just(0i32), Just(-1i32), Just(1i32)
        ],
        bigint_val in prop_oneof![
            Just(i64::MIN), Just(i64::MAX), Just(0i64)
        ],
        float_val in prop_oneof![
            Just(f64::INFINITY), Just(f64::NEG_INFINITY), Just(f64::NAN),
            Just(0.0f64), Just(-0.0f64), Just(f64::MIN), Just(f64::MAX)
        ]
    ) {
        let values = vec![
            Value::Integer(int_val),
            Value::BigInt(bigint_val),
            Value::Float(float_val),
        ];

        for value in values {
            // Should serialize without error
            let serialized = bincode::serialize(&value)?;

            // Should deserialize correctly
            let deserialized: Value = bincode::deserialize(&serialized)?;

            // Verify correctness with NaN handling
            match (&value, &deserialized) {
                (Value::Float(f1), Value::Float(f2)) if f1.is_nan() => {
                    prop_assert!(f2.is_nan());
                },
                _ => {
                    prop_assert_eq!(value, deserialized);
                }
            }
        }
    }

    /// Test collection type invariants
    #[test]
    fn prop_collection_invariants(
        list_items in prop::collection::vec(arb_primitive_value(), 0..20),
        set_items in prop::collection::vec(arb_primitive_value(), 0..20),
        map_items in prop::collection::vec(
            (arb_primitive_value(), arb_primitive_value()),
            0..20
        )
    ) {
        let collections = vec![
            Value::List(list_items.clone()),
            Value::Set(set_items.clone()),
            Value::Map(map_items.clone()),
        ];

        for collection in collections {
            // Serialize/deserialize roundtrip
            let serialized = bincode::serialize(&collection)?;
            let deserialized: Value = bincode::deserialize(&serialized)?;
            prop_assert_eq!(collection, deserialized);

            // Collection-specific invariants
            match &collection {
                Value::List(items) => {
                    prop_assert_eq!(items.len(), list_items.len());
                    prop_assert!(items.len() <= 20);
                },
                Value::Set(items) => {
                    prop_assert_eq!(items.len(), set_items.len());
                    prop_assert!(items.len() <= 20);
                },
                Value::Map(items) => {
                    prop_assert_eq!(items.len(), map_items.len());
                    prop_assert!(items.len() <= 20);
                },
                _ => {}
            }
        }
    }

    /// Test UDT structure validation
    #[test]
    fn prop_udt_validation(udt in arb_udt()) {
        if let Value::Udt(ref udt_value) = udt {
            // UDT must have non-empty type name and keyspace
            prop_assert!(!udt_value.type_name.is_empty());
            prop_assert!(!udt_value.keyspace.is_empty());

            // Field names should be unique
            let mut field_names = std::collections::HashSet::new();
            for field in &udt_value.fields {
                prop_assert!(!field.name.is_empty());
                prop_assert!(field_names.insert(&field.name),
                    "Duplicate field name: {}", field.name);
            }

            // Should serialize/deserialize correctly
            let serialized = bincode::serialize(&udt)?;
            let deserialized: Value = bincode::deserialize(&serialized)?;
            prop_assert_eq!(udt, deserialized);
        }
    }

    /// Test text encoding and Unicode handling
    #[test]
    fn prop_text_encoding(
        text in prop_oneof![
            ".*",                              // ASCII
            "[\u{0000}-\u{007F}]{0,1000}",    // ASCII only
            "[\u{0080}-\u{07FF}]{0,500}",     // 2-byte UTF-8
            "[\u{0800}-\u{FFFF}]{0,333}",     // 3-byte UTF-8
            "[\u{10000}-\u{10FFFF}]{0,250}",  // 4-byte UTF-8
        ]
    ) {
        let value = Value::Text(text.clone());

        // Should be valid UTF-8
        prop_assert!(text.is_ascii() || std::str::from_utf8(text.as_bytes()).is_ok());

        // Should serialize/deserialize correctly
        let serialized = bincode::serialize(&value)?;
        let deserialized: Value = bincode::deserialize(&serialized)?;
        prop_assert_eq!(value, deserialized);

        // Text length constraints
        prop_assert!(text.len() <= 4000); // Reasonable upper bound
        prop_assert!(text.chars().count() <= text.len()); // Char count <= byte count
    }

    /// Test binary data handling
    #[test]
    fn prop_binary_data(data in prop::collection::vec(any::<u8>(), 0..10000)) {
        let value = Value::Blob(data.clone());

        // Should serialize/deserialize correctly
        let serialized = bincode::serialize(&value)?;
        let deserialized: Value = bincode::deserialize(&serialized)?;
        prop_assert_eq!(value, deserialized);

        // Binary data should preserve exact bytes
        if let Value::Blob(ref deser_data) = deserialized {
            prop_assert_eq!(data.len(), deser_data.len());
            for (original, roundtrip) in data.iter().zip(deser_data.iter()) {
                prop_assert_eq!(original, roundtrip);
            }
        }
    }

    /// Test temporal types (timestamps and durations)
    #[test]
    fn prop_temporal_types(
        timestamp in any::<i64>(),
        months in any::<i32>(),
        days in any::<i32>(),
        nanos in any::<i64>()
    ) {
        let timestamp_val = Value::Timestamp(timestamp);
        let duration_val = Value::Duration { months, days, nanos };

        // Both should serialize/deserialize correctly
        for value in &[timestamp_val, duration_val] {
            let serialized = bincode::serialize(value)?;
            let deserialized: Value = bincode::deserialize(&serialized)?;
            prop_assert_eq!(*value, deserialized);
        }

        // Duration component validation
        if let Value::Duration { months: m, days: d, nanos: n } = duration_val {
            prop_assert_eq!(m, months);
            prop_assert_eq!(d, days);
            prop_assert_eq!(n, nanos);
        }
    }

    /// Test decimal value handling
    #[test]
    fn prop_decimal_values(
        scale in any::<i32>(),
        unscaled in prop::collection::vec(any::<u8>(), 1..32)
    ) {
        let value = Value::Decimal { scale, unscaled: unscaled.clone() };

        // Should serialize/deserialize correctly
        let serialized = bincode::serialize(&value)?;
        let deserialized: Value = bincode::deserialize(&serialized)?;
        prop_assert_eq!(value, deserialized);

        // Decimal invariants
        if let Value::Decimal { scale: s, unscaled: u } = value {
            prop_assert_eq!(s, scale);
            prop_assert_eq!(u, unscaled);
            prop_assert!(!u.is_empty());
        }
    }

    /// Test performance bounds for serialization
    #[test]
    fn prop_serialization_performance(value in arb_value()) {
        use std::time::Instant;

        // Measure serialization time
        let start = Instant::now();
        let serialized = bincode::serialize(&value)?;
        let serialize_time = start.elapsed();

        // Measure deserialization time
        let start = Instant::now();
        let _deserialized: Value = bincode::deserialize(&serialized)?;
        let deserialize_time = start.elapsed();

        // Performance bounds (generous to avoid flaky tests)
        let max_time = std::time::Duration::from_millis(100);
        prop_assert!(serialize_time <= max_time,
            "Serialization took too long: {:?}", serialize_time);
        prop_assert!(deserialize_time <= max_time,
            "Deserialization took too long: {:?}", deserialize_time);

        // Serialized size should be reasonable
        let size_estimate = estimate_value_size(&value);
        prop_assert!(serialized.len() <= size_estimate * 4,
            "Serialized size {} too large for estimated size {}",
            serialized.len(), size_estimate);
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Validates invariants for a Value
fn validate_value_invariants(value: &Value) -> Result<(), proptest::test_runner::TestCaseError> {
    match value {
        Value::Text(s) => {
            prop_assert!(s.len() <= 1_000_000, "Text too long");
            prop_assert!(
                s.is_ascii() || std::str::from_utf8(s.as_bytes()).is_ok(),
                "Text must be valid UTF-8"
            );
        }
        Value::Blob(b) => {
            prop_assert!(b.len() <= 1_000_000, "Blob too large");
        }
        Value::List(items) => {
            prop_assert!(items.len() <= 1000, "List too large");
            for item in items {
                validate_value_invariants(item)?;
            }
        }
        Value::Set(items) => {
            prop_assert!(items.len() <= 1000, "Set too large");
            for item in items {
                validate_value_invariants(item)?;
            }
        }
        Value::Map(items) => {
            prop_assert!(items.len() <= 1000, "Map too large");
            for (key, value) in items {
                validate_value_invariants(key)?;
                validate_value_invariants(value)?;
            }
        }
        Value::Tuple(items) => {
            prop_assert!(items.len() <= 100, "Tuple too large");
            for item in items {
                validate_value_invariants(item)?;
            }
        }
        Value::Udt(udt) => {
            prop_assert!(!udt.type_name.is_empty(), "UDT type name cannot be empty");
            prop_assert!(!udt.keyspace.is_empty(), "UDT keyspace cannot be empty");
            prop_assert!(udt.fields.len() <= 100, "UDT has too many fields");

            let mut field_names = std::collections::HashSet::new();
            for field in &udt.fields {
                prop_assert!(!field.name.is_empty(), "UDT field name cannot be empty");
                prop_assert!(
                    field_names.insert(&field.name),
                    "Duplicate UDT field name: {}",
                    field.name
                );
                if let Some(ref value) = field.value {
                    validate_value_invariants(value)?;
                }
            }
        }
        Value::Frozen(boxed_value) => {
            validate_value_invariants(boxed_value)?;
        }
        Value::Decimal { scale: _, unscaled } => {
            prop_assert!(
                !unscaled.is_empty(),
                "Decimal unscaled value cannot be empty"
            );
            prop_assert!(unscaled.len() <= 32, "Decimal unscaled value too large");
        }
        _ => {
            // Other types have no special invariants to check
        }
    }
    Ok(())
}

/// Estimates the serialized size of a value
fn estimate_value_size(value: &Value) -> usize {
    match value {
        Value::Null => 1,
        Value::Boolean(_) => 2,
        Value::Integer(_) => 5,
        Value::BigInt(_) => 9,
        Value::Float(_) => 9,
        Value::Float32(_) => 5,
        Value::TinyInt(_) => 2,
        Value::SmallInt(_) => 3,
        Value::Text(s) => s.len() + 8,
        Value::Blob(b) => b.len() + 8,
        Value::Timestamp(_) => 9,
        Value::Uuid(_) => 17,
        Value::Varint(v) => v.len() + 8,
        Value::Decimal { unscaled, .. } => unscaled.len() + 12,
        Value::Duration { .. } => 16,
        Value::Json(j) => j.to_string().len() + 8,
        Value::List(items) => items.iter().map(estimate_value_size).sum::<usize>() + 8,
        Value::Set(items) => items.iter().map(estimate_value_size).sum::<usize>() + 8,
        Value::Map(items) => {
            items
                .iter()
                .map(|(k, v)| estimate_value_size(k) + estimate_value_size(v))
                .sum::<usize>()
                + 8
        }
        Value::Tuple(items) => items.iter().map(estimate_value_size).sum::<usize>() + 8,
        Value::Udt(udt) => {
            udt.type_name.len()
                + udt.keyspace.len()
                + udt
                    .fields
                    .iter()
                    .map(|f| f.name.len() + f.value.as_ref().map(estimate_value_size).unwrap_or(0))
                    .sum::<usize>()
                + 16
        }
        Value::Frozen(boxed) => estimate_value_size(boxed) + 8,
        Value::Tombstone(_) => 16,
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_generators_produce_valid_values() {
        use proptest::test_runner::TestRunner;

        let mut runner = TestRunner::default();
        let strategy = arb_value();

        // Generate several values to ensure generators work
        for _ in 0..50 {
            let value = strategy.new_tree(&mut runner).unwrap().current();

            // Basic validation
            assert!(validate_value_invariants(&value).is_ok());

            // Serialization should work
            let serialized = bincode::serialize(&value).unwrap();
            let deserialized: Value = bincode::deserialize(&serialized).unwrap();

            // Handle NaN equality
            match (&value, &deserialized) {
                (Value::Float(f1), Value::Float(f2)) if f1.is_nan() => {
                    assert!(f2.is_nan());
                }
                (Value::Float32(f1), Value::Float32(f2)) if f1.is_nan() => {
                    assert!(f2.is_nan());
                }
                _ => {
                    assert_eq!(value, deserialized);
                }
            }
        }
    }

    #[test]
    fn test_all_value_variants_covered() {
        // Ensure our generators can produce all Value variants
        use proptest::test_runner::TestRunner;

        let mut runner = TestRunner::default();
        let strategy = arb_value();

        let mut seen_variants = std::collections::HashSet::new();

        for _ in 0..1000 {
            let value = strategy.new_tree(&mut runner).unwrap().current();
            seen_variants.insert(std::mem::discriminant(&value));
        }

        // Should see a good variety of Value variants
        assert!(
            seen_variants.len() >= 10,
            "Should generate diverse Value types, got {}",
            seen_variants.len()
        );
    }
}
