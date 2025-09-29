//! Validation functions for property testing

use crate::types::*;
use proptest::test_runner::TestCaseError;
use proptest::{prop_assert, prop_assert_eq};

/// Validates invariants for CQL values
pub fn validate_cql_value_invariants(value: &CqlValue) -> Result<(), TestCaseError> {
    match value {
        CqlValue::Text(s) => {
            prop_assert!(s.len() <= 1_000_000, "Text too long: {} chars", s.len());
            prop_assert!(s.is_ascii() || std::str::from_utf8(s.as_bytes()).is_ok(),
                "Text must be valid UTF-8");
        },
        CqlValue::Blob(b) => {
            prop_assert!(b.len() <= 1_000_000, "Blob too large: {} bytes", b.len());
        },
        CqlValue::List(items) => {
            prop_assert!(items.len() <= 10_000, "List too large: {} items", items.len());
            for (i, item) in items.iter().enumerate() {
                validate_cql_value_invariants(item)
                    .map_err(|e| TestCaseError::fail(format!("List item {}: {}", i, e)))?;
            }
        },
        CqlValue::Set(items) => {
            prop_assert!(items.len() <= 10_000, "Set too large: {} items", items.len());
            for (i, item) in items.iter().enumerate() {
                validate_cql_value_invariants(item)
                    .map_err(|e| TestCaseError::fail(format!("Set item {}: {}", i, e)))?;
            }
        },
        CqlValue::Map(items) => {
            prop_assert!(items.len() <= 10_000, "Map too large: {} items", items.len());
            for (i, (key, value)) in items.iter().enumerate() {
                validate_cql_value_invariants(key)
                    .map_err(|e| TestCaseError::fail(format!("Map key {}: {}", i, e)))?;
                validate_cql_value_invariants(value)
                    .map_err(|e| TestCaseError::fail(format!("Map value {}: {}", i, e)))?;
            }
        },
        CqlValue::Tuple(items) => {
            prop_assert!(items.len() <= 1000, "Tuple too large: {} items", items.len());
            for (i, item) in items.iter().enumerate() {
                validate_cql_value_invariants(item)
                    .map_err(|e| TestCaseError::fail(format!("Tuple item {}: {}", i, e)))?;
            }
        },
        CqlValue::Udt(udt) => {
            prop_assert!(!udt.type_name.is_empty(), "UDT type name cannot be empty");
            prop_assert!(!udt.keyspace.is_empty(), "UDT keyspace cannot be empty");
            prop_assert!(udt.fields.len() <= 100, "UDT has too many fields: {}", udt.fields.len());

            let mut field_names = std::collections::HashSet::new();
            for field in &udt.fields {
                prop_assert!(!field.name.is_empty(), "UDT field name cannot be empty");
                prop_assert!(field_names.insert(&field.name),
                    "Duplicate UDT field name: {}", field.name);
                if let Some(ref value) = field.value {
                    validate_cql_value_invariants(value)
                        .map_err(|e| TestCaseError::fail(format!("UDT field {}: {}", field.name, e)))?;
                }
            }
        },
        CqlValue::Frozen(boxed_value) => {
            validate_cql_value_invariants(boxed_value)
                .map_err(|e| TestCaseError::fail(format!("Frozen value: {}", e)))?;
        },
        CqlValue::Decimal { scale: _, unscaled } => {
            prop_assert!(!unscaled.is_empty(), "Decimal unscaled value cannot be empty");
            prop_assert!(unscaled.len() <= 32, "Decimal unscaled value too large: {} bytes", unscaled.len());
        },
        CqlValue::Varint(v) => {
            prop_assert!(!v.is_empty(), "Varint cannot be empty");
            prop_assert!(v.len() <= 32, "Varint too large: {} bytes", v.len());
        },
        CqlValue::Float(OrderedFloat(f)) => {
            prop_assert!(f.is_finite() || f.is_infinite() || f.is_nan(),
                "Float should be finite, infinite, or NaN");
        },
        CqlValue::Float32(OrderedFloat32(f)) => {
            prop_assert!(f.is_finite() || f.is_infinite() || f.is_nan(),
                "Float32 should be finite, infinite, or NaN");
        },
        CqlValue::Duration { months, days, nanos } => {
            // Duration components should be within reasonable bounds
            prop_assert!(months.abs() < 1_000_000, "Duration months too large: {}", months);
            prop_assert!(days.abs() < 1_000_000, "Duration days too large: {}", days);
            prop_assert!(nanos.abs() < 86_400_000_000_000, "Duration nanos too large: {}", nanos);
        },
        CqlValue::Timestamp(ts) => {
            // Timestamp should be within reasonable bounds (1970-2070 range in microseconds)
            let min_timestamp = 0i64; // 1970-01-01
            let max_timestamp = 3_155_760_000_000_000i64; // ~2070-01-01
            prop_assert!(*ts >= min_timestamp && *ts <= max_timestamp,
                "Timestamp out of reasonable range: {}", ts);
        },
        _ => {
            // Other types have no special invariants to check
        }
    }
    Ok(())
}

/// Validates schema invariants
pub fn validate_schema_invariants(schema: &Schema) -> Result<(), TestCaseError> {
    prop_assert!(!schema.keyspace.is_empty(), "Keyspace cannot be empty");
    prop_assert!(!schema.table.is_empty(), "Table name cannot be empty");
    prop_assert!(!schema.partition_keys.is_empty(), "Must have at least one partition key");

    // Check for duplicate column names across all column types
    let mut all_column_names = std::collections::HashSet::new();

    for key in &schema.partition_keys {
        prop_assert!(!key.name.is_empty(), "Partition key name cannot be empty");
        prop_assert!(!key.data_type.is_empty(), "Partition key data type cannot be empty");
        prop_assert!(all_column_names.insert(&key.name),
            "Duplicate column name: {}", key.name);
    }

    for key in &schema.clustering_keys {
        prop_assert!(!key.name.is_empty(), "Clustering key name cannot be empty");
        prop_assert!(!key.data_type.is_empty(), "Clustering key data type cannot be empty");
        prop_assert!(all_column_names.insert(&key.name),
            "Duplicate column name: {}", key.name);
    }

    for column in &schema.columns {
        prop_assert!(!column.name.is_empty(), "Column name cannot be empty");
        prop_assert!(!column.data_type.is_empty(), "Column data type cannot be empty");
        prop_assert!(all_column_names.insert(&column.name),
            "Duplicate column name: {}", column.name);
    }

    // Validate data types
    for key in schema.partition_keys.iter().chain(schema.clustering_keys.iter()) {
        validate_data_type(&key.data_type)?;
    }

    for column in &schema.columns {
        validate_data_type(&column.data_type)?;
    }

    Ok(())
}

/// Validates that a data type string is valid
fn validate_data_type(data_type: &str) -> Result<(), TestCaseError> {
    let valid_types = [
        "text", "varchar", "ascii",
        "int", "bigint", "smallint", "tinyint", "varint",
        "float", "double", "decimal",
        "boolean", "blob",
        "timestamp", "date", "time", "timeuuid", "uuid",
        "inet", "duration",
        "counter",
        "json",
    ];

    let valid_collection_prefixes = [
        "list<", "set<", "map<", "tuple<", "frozen<"
    ];

    if valid_types.contains(&data_type) {
        return Ok(());
    }

    for prefix in &valid_collection_prefixes {
        if data_type.starts_with(prefix) && data_type.ends_with('>') {
            return Ok(()); // Simplified validation for collections
        }
    }

    prop_assert!(false, "Invalid data type: {}", data_type);
    Ok(())
}

/// Validates memory usage bounds
pub fn validate_memory_bounds(size: usize, max_size: usize, description: &str) -> Result<(), TestCaseError> {
    prop_assert!(size <= max_size,
        "{} memory usage {} exceeds maximum {}", description, size, max_size);
    Ok(())
}

/// Validates performance bounds
pub fn validate_performance_bounds(
    duration: std::time::Duration,
    max_duration: std::time::Duration,
    operation: &str
) -> Result<(), TestCaseError> {
    prop_assert!(duration <= max_duration,
        "{} took {:?}, expected <= {:?}", operation, duration, max_duration);
    Ok(())
}

/// Validates compression ratio bounds
pub fn validate_compression_ratio(
    original_size: usize,
    compressed_size: usize,
    min_ratio: f64,
    max_ratio: f64,
    algorithm: &str
) -> Result<(), TestCaseError> {
    if original_size == 0 {
        return Ok(()); // Skip validation for empty data
    }

    let ratio = compressed_size as f64 / original_size as f64;
    prop_assert!(ratio >= min_ratio && ratio <= max_ratio,
        "{} compression ratio {:.3} not in range [{:.3}, {:.3}]",
        algorithm, ratio, min_ratio, max_ratio);
    Ok(())
}

/// Validates that floating point operations preserve special values correctly
pub fn validate_float_special_values(original: f64, roundtrip: f64) -> Result<(), TestCaseError> {
    if original.is_nan() {
        prop_assert!(roundtrip.is_nan(), "NaN should remain NaN after roundtrip");
    } else if original.is_infinite() {
        prop_assert!(roundtrip.is_infinite(), "Infinity should remain infinity after roundtrip");
        prop_assert_eq!(original.is_sign_positive(), roundtrip.is_sign_positive(),
            "Infinity sign should be preserved");
    } else {
        prop_assert_eq!(original, roundtrip, "Finite float should be preserved exactly");
    }
    Ok(())
}

/// Validates that floating point operations preserve special values correctly for f32
pub fn validate_float32_special_values(original: f32, roundtrip: f32) -> Result<(), TestCaseError> {
    if original.is_nan() {
        prop_assert!(roundtrip.is_nan(), "NaN should remain NaN after roundtrip");
    } else if original.is_infinite() {
        prop_assert!(roundtrip.is_infinite(), "Infinity should remain infinity after roundtrip");
        prop_assert_eq!(original.is_sign_positive(), roundtrip.is_sign_positive(),
            "Infinity sign should be preserved");
    } else {
        prop_assert_eq!(original, roundtrip, "Finite float should be preserved exactly");
    }
    Ok(())
}

/// Validates concurrent operation results
pub fn validate_concurrent_results<T: PartialEq + std::fmt::Debug>(
    original: &[T],
    results: &[T],
    operation: &str
) -> Result<(), TestCaseError> {
    prop_assert_eq!(original.len(), results.len(),
        "Concurrent {} changed number of items", operation);

    for (i, (orig, result)) in original.iter().zip(results.iter()).enumerate() {
        prop_assert_eq!(orig, result,
            "Concurrent {} corrupted item {} ({:?} != {:?})", operation, i, orig, result);
    }

    Ok(())
}

/// Validates that a collection maintains its ordering properties
pub fn validate_collection_ordering<T: PartialEq + std::fmt::Debug>(
    original: &[T],
    roundtrip: &[T],
    should_preserve_order: bool,
    collection_type: &str
) -> Result<(), TestCaseError> {
    prop_assert_eq!(original.len(), roundtrip.len(),
        "{} length changed during roundtrip", collection_type);

    if should_preserve_order {
        for (i, (orig, trip)) in original.iter().zip(roundtrip.iter()).enumerate() {
            prop_assert_eq!(orig, trip,
                "{} item {} changed during roundtrip ({:?} != {:?})",
                collection_type, i, orig, trip);
        }
    } else {
        // For unordered collections, just check that all elements are present
        for (i, orig) in original.iter().enumerate() {
            prop_assert!(roundtrip.contains(orig),
                "{} missing item {} ({:?}) after roundtrip",
                collection_type, i, orig);
        }
    }

    Ok(())
}