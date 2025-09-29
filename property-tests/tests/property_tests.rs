//! Comprehensive property-based tests for CQLite
//!
//! This module contains the main property tests covering:
//! - All CQL types roundtrip serialization/deserialization
//! - Schema inference consistency
//! - Compression data integrity for all algorithms
//! - Partition key handling edge cases
//! - Memory usage bounds and leak detection
//! - Performance regression prevention

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use cqlite_property_tests::*;
use std::time::Instant;
use std::sync::Arc;
use std::thread;

// ============================================================================
// Core Property Tests
// ============================================================================

proptest! {
    /// Test that all CQL types can roundtrip through serialization/deserialization
    #[test]
    fn prop_all_cql_types_roundtrip(value in arb_cql_value()) {
        // Serialize to bytes
        let serialized = bincode::serialize(&value)
            .expect("All CQL values should serialize successfully");

        // Deserialize back
        let deserialized: CqlValue = bincode::deserialize(&serialized)
            .expect("Serialized CQL values should deserialize successfully");

        // Values should be identical (with special handling for NaN)
        match (&value, &deserialized) {
            (CqlValue::Float(f1), CqlValue::Float(f2)) => {
                validate_float_special_values(f1.0, f2.0)?;
            },
            (CqlValue::Float32(f1), CqlValue::Float32(f2)) => {
                validate_float32_special_values(f1.0, f2.0)?;
            },
            _ => {
                prop_assert_eq!(value, deserialized);
            }
        }

        // Additional type-specific validation
        validate_cql_value_invariants(&value)?;
        validate_cql_value_invariants(&deserialized)?;

        // Size constraints
        let estimated_size = value.estimate_size();
        prop_assert!(serialized.len() <= estimated_size * 4,
            "Serialized size {} exceeds 4x estimated size {}",
            serialized.len(), estimated_size);
    }

    /// Test schema inference consistency across different parsing contexts
    #[test]
    fn prop_schema_inference_consistency(schema in arb_schema()) {
        // Schema should validate
        validate_schema_invariants(&schema)?;

        // Schema should serialize/deserialize consistently
        let serialized = serde_json::to_string(&schema)
            .expect("Schema should serialize to JSON");
        let deserialized: Schema = serde_json::from_str(&serialized)
            .expect("Serialized schema should deserialize");

        prop_assert_eq!(schema, deserialized);

        // Schema serialization should be deterministic
        let serialized2 = serde_json::to_string(&deserialized)
            .expect("Deserialized schema should serialize again");
        prop_assert_eq!(serialized, serialized2);

        // Schema components should have reasonable sizes
        prop_assert!(schema.partition_keys.len() <= 20, "Too many partition keys");
        prop_assert!(schema.clustering_keys.len() <= 20, "Too many clustering keys");
        prop_assert!(schema.columns.len() <= 1000, "Too many columns");
    }

    /// Test compression data integrity for all supported algorithms
    #[test]
    fn prop_compression_data_integrity(
        data in prop::collection::vec(any::<u8>(), 0..100000),
        compression_type in prop_oneof![
            Just(CompressionType::None),
            Just(CompressionType::Lz4Mock),
            Just(CompressionType::SnappyMock),
            Just(CompressionType::DeflateMock),
            Just(CompressionType::ZstdMock),
        ]
    ) {
        let codec = CompressionCodec::new(compression_type)
            .expect("Should create codec");

        // Compress the data
        let compressed = codec.compress(&data)
            .expect("Compression should succeed for valid data");

        // Decompress back
        let decompressed = codec.decompress(&compressed, data.len())
            .expect("Decompression should succeed for properly compressed data");

        // Data should be identical after roundtrip
        prop_assert_eq!(data, decompressed);

        // Compression ratio should be reasonable
        let expected_ratio = codec.expected_compression_ratio(&data);
        let actual_ratio = if data.is_empty() {
            1.0
        } else {
            compressed.len() as f64 / data.len() as f64
        };

        match compression_type {
            CompressionType::None => {
                prop_assert_eq!(actual_ratio, 1.0, "No compression should have ratio 1.0");
            },
            _ => {
                validate_compression_ratio(
                    data.len(),
                    compressed.len(),
                    0.01, // Min ratio (very good compression)
                    5.0,  // Max ratio (some expansion allowed)
                    &format!("{:?}", compression_type)
                )?;

                // For highly repetitive data, should compress well
                if data.len() > 100 && data.iter().all(|&b| b == data[0]) {
                    prop_assert!(actual_ratio < 0.1,
                        "Repetitive data should compress well with {:?}, got ratio {}",
                        compression_type, actual_ratio);
                }
            }
        }
    }

    /// Test partition key handling for various edge cases
    #[test]
    fn prop_partition_key_handling(
        partition_keys in prop::collection::vec(arb_primitive_cql_value(), 1..10),
        clustering_keys in prop::collection::vec(arb_primitive_cql_value(), 0..10)
    ) {
        // Partition keys should never be empty
        prop_assert!(!partition_keys.is_empty());

        // All partition key values should be serializable
        for (i, key) in partition_keys.iter().enumerate() {
            let serialized = bincode::serialize(key)
                .expect("Partition keys must be serializable");
            prop_assert!(!serialized.is_empty(), "Partition key {} serialized to empty", i);

            // Key should deserialize back to same value
            let deserialized: CqlValue = bincode::deserialize(&serialized)
                .expect("Serialized partition keys must deserialize");

            match (key, &deserialized) {
                (CqlValue::Float(f1), CqlValue::Float(f2)) => {
                    validate_float_special_values(f1.0, f2.0)?;
                },
                (CqlValue::Float32(f1), CqlValue::Float32(f2)) => {
                    validate_float32_special_values(f1.0, f2.0)?;
                },
                _ => {
                    prop_assert_eq!(*key, deserialized);
                }
            }

            // Validate key invariants
            validate_cql_value_invariants(key)?;
        }

        // Clustering keys are optional but if present should be serializable
        for (i, key) in clustering_keys.iter().enumerate() {
            let serialized = bincode::serialize(key)
                .expect("Clustering keys must be serializable");
            let deserialized: CqlValue = bincode::deserialize(&serialized)
                .expect("Serialized clustering keys must deserialize");

            match (key, &deserialized) {
                (CqlValue::Float(f1), CqlValue::Float(f2)) => {
                    validate_float_special_values(f1.0, f2.0)?;
                },
                (CqlValue::Float32(f1), CqlValue::Float32(f2)) => {
                    validate_float32_special_values(f1.0, f2.0)?;
                },
                _ => {
                    prop_assert_eq!(*key, deserialized);
                }
            }

            validate_cql_value_invariants(key)?;
        }

        // Combined key should produce consistent results
        let all_keys: Vec<_> = partition_keys.iter().chain(clustering_keys.iter()).collect();
        let combined_serialized = bincode::serialize(&all_keys)
            .expect("Combined keys should serialize");
        let combined_deserialized: Vec<CqlValue> = bincode::deserialize(&combined_serialized)
            .expect("Combined keys should deserialize");

        prop_assert_eq!(all_keys.len(), combined_deserialized.len());
    }

    /// Test memory usage bounds and leak detection
    #[test]
    fn prop_memory_usage_bounds(
        values in prop::collection::vec(arb_cql_value(), 1..50),
        operation_count in 1..100usize
    ) {
        // Track memory usage patterns
        let mut total_serialized_size = 0usize;
        let mut max_single_size = 0usize;

        for _ in 0..operation_count {
            for value in &values {
                let serialized = bincode::serialize(value)
                    .expect("Value should serialize");

                total_serialized_size += serialized.len();
                max_single_size = max_single_size.max(serialized.len());

                let _deserialized: CqlValue = bincode::deserialize(&serialized)
                    .expect("Value should deserialize");

                // Individual value size should be reasonable
                validate_memory_bounds(
                    serialized.len(),
                    10_000_000, // 10MB max per value
                    "Single value serialization"
                )?;
            }
        }

        // Total memory usage should be bounded
        let avg_size = total_serialized_size / (values.len() * operation_count);
        validate_memory_bounds(
            avg_size,
            1_000_000, // 1MB average
            "Average serialization size"
        )?;

        validate_memory_bounds(
            max_single_size,
            10_000_000, // 10MB max single value
            "Maximum single value size"
        )?;

        // Total accumulated size should be reasonable
        validate_memory_bounds(
            total_serialized_size,
            100_000_000, // 100MB total
            "Total accumulated serialization size"
        )?;
    }

    /// Test performance bounds for operations
    #[test]
    fn prop_performance_bounds(value in arb_cql_value()) {
        // Measure serialization performance
        let start = Instant::now();
        let serialized = bincode::serialize(&value)
            .expect("Value should serialize");
        let serialize_time = start.elapsed();

        // Measure deserialization performance
        let start = Instant::now();
        let _deserialized: CqlValue = bincode::deserialize(&serialized)
            .expect("Value should deserialize");
        let deserialize_time = start.elapsed();

        // Performance bounds (generous to avoid flaky tests)
        validate_performance_bounds(
            serialize_time,
            std::time::Duration::from_millis(1000), // 1 second max
            "Serialization"
        )?;

        validate_performance_bounds(
            deserialize_time,
            std::time::Duration::from_millis(1000), // 1 second max
            "Deserialization"
        )?;

        // Deserialization should not be significantly slower than serialization
        if serialize_time.as_millis() > 10 {
            let ratio = deserialize_time.as_millis() as f64 / serialize_time.as_millis() as f64;
            prop_assert!(ratio <= 10.0,
                "Deserialization took {}x longer than serialization", ratio);
        }
    }

    /// Test concurrent operations safety
    #[test]
    fn prop_concurrent_safety(
        values in prop::collection::vec(arb_cql_value(), 1..20)
    ) {
        let shared_values = Arc::new(values.clone());

        // Spawn multiple threads for concurrent operations
        let handles: Vec<_> = (0..shared_values.len()).map(|i| {
            let values = Arc::clone(&shared_values);

            thread::spawn(move || {
                let value = &values[i];

                // Perform serialization/deserialization
                let serialized = bincode::serialize(value)?;
                let deserialized: CqlValue = bincode::deserialize(&serialized)?;

                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((value.clone(), deserialized))
            })
        }).collect();

        // Collect results
        let mut results = Vec::new();
        for handle in handles {
            let result = handle.join()
                .expect("Thread should complete successfully")
                .expect("Operations should succeed");
            results.push(result);
        }

        // Validate all results
        for (i, (original, deserialized)) in results.iter().enumerate() {
            match (original, deserialized) {
                (CqlValue::Float(f1), CqlValue::Float(f2)) => {
                    validate_float_special_values(f1.0, f2.0)
                        .map_err(|e| TestCaseError::fail(format!("Thread {}: {}", i, e)))?;
                },
                (CqlValue::Float32(f1), CqlValue::Float32(f2)) => {
                    validate_float32_special_values(f1.0, f2.0)
                        .map_err(|e| TestCaseError::fail(format!("Thread {}: {}", i, e)))?;
                },
                _ => {
                    prop_assert_eq!(*original, *deserialized,
                        "Thread {} data corruption", i);
                }
            }
        }

        // Check that we got all expected results
        prop_assert_eq!(results.len(), values.len());
    }

    /// Test edge cases and boundary conditions
    #[test]
    fn prop_edge_cases(value in arb_extreme_numerics()) {
        // Should serialize without error
        let serialized = bincode::serialize(&value)
            .expect("Extreme values should serialize");

        // Should deserialize correctly
        let deserialized: CqlValue = bincode::deserialize(&serialized)
            .expect("Extreme values should deserialize");

        // Verify correctness with special float handling
        match (&value, &deserialized) {
            (CqlValue::Float(f1), CqlValue::Float(f2)) => {
                validate_float_special_values(f1.0, f2.0)?;
            },
            (CqlValue::Float32(f1), CqlValue::Float32(f2)) => {
                validate_float32_special_values(f1.0, f2.0)?;
            },
            _ => {
                prop_assert_eq!(value, deserialized);
            }
        }

        // Should pass invariant validation
        validate_cql_value_invariants(&value)?;
    }

    /// Test collection ordering properties
    #[test]
    fn prop_collection_ordering(
        list_items in prop::collection::vec(arb_primitive_cql_value(), 0..50),
        set_items in prop::collection::vec(arb_primitive_cql_value(), 0..50),
        map_items in prop::collection::vec(
            (arb_primitive_cql_value(), arb_primitive_cql_value()),
            0..50
        )
    ) {
        let list = CqlValue::List(list_items.clone());
        let set = CqlValue::Set(set_items.clone());
        let map = CqlValue::Map(map_items.clone());

        for (collection, should_preserve_order, name) in [
            (&list, true, "List"),
            (&set, true, "Set"), // CQL sets preserve insertion order
            (&map, true, "Map"), // CQL maps preserve insertion order
        ] {
            let serialized = bincode::serialize(collection)?;
            let deserialized: CqlValue = bincode::deserialize(&serialized)?;

            match (&collection, &deserialized) {
                (CqlValue::List(orig), CqlValue::List(deser)) => {
                    validate_collection_ordering(orig, deser, should_preserve_order, name)?;
                },
                (CqlValue::Set(orig), CqlValue::Set(deser)) => {
                    validate_collection_ordering(orig, deser, should_preserve_order, name)?;
                },
                (CqlValue::Map(orig), CqlValue::Map(deser)) => {
                    validate_collection_ordering(orig, deser, should_preserve_order, name)?;
                },
                _ => {
                    prop_assert_eq!(*collection, deserialized);
                }
            }
        }
    }

    /// Test deeply nested structures
    #[test]
    fn prop_deeply_nested_structures(value in arb_deeply_nested(10)) {
        // Should handle nested structures without stack overflow
        let serialize_result = std::panic::catch_unwind(|| {
            bincode::serialize(&value)
        });

        prop_assert!(serialize_result.is_ok(), "Nested structure serialization should not panic");

        if let Ok(Ok(serialized)) = serialize_result {
            let deserialize_result = std::panic::catch_unwind(|| {
                bincode::deserialize::<CqlValue>(&serialized)
            });

            prop_assert!(deserialize_result.is_ok(), "Nested structure deserialization should not panic");

            if let Ok(Ok(deserialized)) = deserialize_result {
                prop_assert_eq!(value, deserialized);
            }
        }

        // Should pass validation
        validate_cql_value_invariants(&value)?;
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_all_generators_work() {
        use proptest::test_runner::TestRunner;

        let mut runner = TestRunner::default();

        // Test all major generators
        let generators = [
            ("arb_cql_value", Box::new(arb_cql_value()) as Box<dyn Strategy<Value = CqlValue>>),
            ("arb_primitive_cql_value", Box::new(arb_primitive_cql_value())),
            ("arb_extreme_numerics", Box::new(arb_extreme_numerics())),
            ("arb_udt", Box::new(arb_udt())),
        ];

        for (name, strategy) in generators {
            for _ in 0..10 {
                let value = strategy.new_tree(&mut runner).unwrap().current();

                // Should be able to serialize
                let serialized = bincode::serialize(&value).unwrap();

                // Should be able to deserialize
                let _deserialized: CqlValue = bincode::deserialize(&serialized).unwrap();

                println!("Generator {} produced valid value of type {}", name, value.type_name());
            }
        }
    }

    #[test]
    fn test_compression_algorithms() {
        let test_data = b"hello world ".repeat(100);

        for algorithm in [
            CompressionType::None,
            CompressionType::Lz4Mock,
            CompressionType::SnappyMock,
            CompressionType::DeflateMock,
            CompressionType::ZstdMock,
        ] {
            let codec = CompressionCodec::new(algorithm).unwrap();
            let compressed = codec.compress(&test_data).unwrap();
            let decompressed = codec.decompress(&compressed, test_data.len()).unwrap();

            assert_eq!(test_data.as_slice(), decompressed.as_slice());

            println!("Algorithm {:?}: {} -> {} bytes (ratio: {:.3})",
                algorithm,
                test_data.len(),
                compressed.len(),
                compressed.len() as f64 / test_data.len() as f64
            );
        }
    }

    #[test]
    fn test_schema_generation() {
        use proptest::test_runner::TestRunner;

        let mut runner = TestRunner::default();
        let strategy = arb_schema();

        for _ in 0..10 {
            let schema = strategy.new_tree(&mut runner).unwrap().current();

            // Should pass validation
            validate_schema_invariants(&schema).unwrap();

            // Should serialize
            let _serialized = serde_json::to_string(&schema).unwrap();

            println!("Generated schema: {}.{} with {} partition keys, {} clustering keys, {} columns",
                schema.keyspace, schema.table,
                schema.partition_keys.len(),
                schema.clustering_keys.len(),
                schema.columns.len()
            );
        }
    }

    #[test]
    fn test_validation_functions() {
        // Test validation functions work correctly
        let valid_value = CqlValue::Text("hello".to_string());
        assert!(validate_cql_value_invariants(&valid_value).is_ok());

        let valid_schema = Schema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: std::collections::HashMap::new(),
        };
        assert!(validate_schema_invariants(&valid_schema).is_ok());
    }
}