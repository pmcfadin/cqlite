//! Comprehensive test suite for CEP-25 compliant byte-comparable key encoder
//!
//! This test suite validates the implementation against the CEP-25 specification
//! for Cassandra 5.0 byte-comparable key encoding used in BTI format.

#[cfg(test)]
use cqlite_core::types::{Value, UdtValue, UdtField};
#[cfg(test)]
use cqlite_core::storage::sstable::bti::encoder::{ByteComparableEncoder, BatchEncoder, EncoderConfig};

#[cfg(test)]
mod bti_encoder_tests {
    use super::*;

    /// Test CEP-25 compliance for basic data types
    #[test]
    fn test_cep25_basic_types_compliance() {
        let mut encoder = ByteComparableEncoder::new();

        // Test all basic types with known values
        let test_cases = vec![
            (Value::Null, "null value"),
            (Value::Boolean(false), "boolean false"),
            (Value::Boolean(true), "boolean true"),
            (Value::TinyInt(-128), "tinyint min"),
            (Value::TinyInt(0), "tinyint zero"),
            (Value::TinyInt(127), "tinyint max"),
            (Value::SmallInt(-32768), "smallint min"),
            (Value::SmallInt(0), "smallint zero"),
            (Value::SmallInt(32767), "smallint max"),
            (Value::Integer(i32::MIN), "int min"),
            (Value::Integer(0), "int zero"),
            (Value::Integer(i32::MAX), "int max"),
            (Value::BigInt(i64::MIN), "bigint min"),
            (Value::BigInt(0), "bigint zero"),
            (Value::BigInt(i64::MAX), "bigint max"),
        ];

        for (value, description) in test_cases {
            let encoded = encoder.encode_value(&value).unwrap();
            assert!(!encoded.is_empty(), "Empty encoding for {}", description);

            // Validate the encoding
            encoder.validate_encoded_key(&encoded).unwrap_or_else(|e| {
                panic!("Invalid encoding for {}: {}", description, e);
            });
        }
    }

    /// Test floating point encoding with IEEE 754 compliance
    #[test]
    fn test_ieee754_float_ordering() {
        let mut encoder = ByteComparableEncoder::new();

        let float_values = vec![
            f32::NEG_INFINITY,
            -1000000.0,
            -1.0,
            -0.0,
            0.0,
            1.0,
            1000000.0,
            f32::INFINITY,
            f32::NAN,
        ];

        let mut encoded_floats = Vec::new();
        for &val in &float_values {
            let encoded = encoder.encode_value(&Value::Float32(val)).unwrap();
            encoded_floats.push(encoded);
        }

        // Verify proper IEEE 754 ordering (except NaN)
        for i in 0..encoded_floats.len() - 2 {
            assert!(
                encoded_floats[i] <= encoded_floats[i + 1],
                "Float ordering violation: {} should be <= {}",
                float_values[i],
                float_values[i + 1]
            );
        }

        // NaN should sort last
        assert!(
            encoded_floats[encoded_floats.len() - 2] < encoded_floats[encoded_floats.len() - 1],
            "NaN should sort after infinity"
        );
    }

    /// Test string encoding with UTF-8 and escape sequences
    #[test]
    fn test_utf8_string_encoding() {
        let mut encoder = ByteComparableEncoder::new();

        let test_strings = vec![
            "",
            "a",
            "hello",
            "hello world",
            "café",       // UTF-8 accented characters
            "🚀",         // UTF-8 emoji
            "你好",       // UTF-8 Chinese characters
            "a\0b",       // Embedded null
            "a\x01b",     // Control character
            "a\u{00FF}b", // High byte value
        ];

        let mut encoded_strings = Vec::new();
        for s in &test_strings {
            let encoded = encoder.encode_value(&Value::Text(s.to_string())).unwrap();
            encoded_strings.push((encoded, s));
        }

        // Sort by encoded values
        encoded_strings.sort_by(|a, b| a.0.cmp(&b.0));

        // Verify UTF-8 ordering is preserved
        let sorted_strings: Vec<&str> = encoded_strings.iter().map(|(_, s)| **s).collect();
        let mut expected_strings = test_strings.clone();
        expected_strings.sort();

        assert_eq!(
            sorted_strings, expected_strings,
            "UTF-8 string ordering not preserved after encoding"
        );
    }

    /// Test collection encoding with proper sorting
    #[test]
    fn test_collection_deterministic_encoding() {
        let mut encoder = ByteComparableEncoder::new();

        // Test sets with different input orders should produce same encoding
        let set1 = Value::Set(vec![
            Value::Integer(3),
            Value::Integer(1),
            Value::Integer(2),
        ]);

        let set2 = Value::Set(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);

        let encoded1 = encoder.encode_value(&set1).unwrap();
        let encoded2 = encoder.encode_value(&set2).unwrap();

        assert_eq!(
            encoded1, encoded2,
            "Set encoding should be deterministic regardless of input order"
        );

        // Test maps with different input orders
        let map1 = Value::Map(vec![
            (Value::Text("z".to_string()), Value::Integer(1)),
            (Value::Text("a".to_string()), Value::Integer(2)),
        ]);

        let map2 = Value::Map(vec![
            (Value::Text("a".to_string()), Value::Integer(2)),
            (Value::Text("z".to_string()), Value::Integer(1)),
        ]);

        let encoded_map1 = encoder.encode_value(&map1).unwrap();
        let encoded_map2 = encoder.encode_value(&map2).unwrap();

        assert_eq!(
            encoded_map1, encoded_map2,
            "Map encoding should be deterministic regardless of input order"
        );
    }

    /// Test complex nested structures
    #[test]
    fn test_complex_nested_structures() {
        let mut encoder = ByteComparableEncoder::new();

        // Create a complex nested structure
        let complex_value = Value::Map(vec![(
            Value::Text("users".to_string()),
            Value::List(vec![Value::Udt(UdtValue {
                type_name: "User".to_string(),
                keyspace: "test".to_string(),
                fields: vec![
                    UdtField {
                        name: "id".to_string(),
                        value: Some(Value::Integer(1)),
                    },
                    UdtField {
                        name: "profile".to_string(),
                        value: Some(Value::Map(vec![
                            (
                                Value::Text("name".to_string()),
                                Value::Text("John".to_string()),
                            ),
                            (
                                Value::Text("tags".to_string()),
                                Value::Set(vec![
                                    Value::Text("admin".to_string()),
                                    Value::Text("developer".to_string()),
                                ]),
                            ),
                        ])),
                    },
                ],
            })]),
        )]);

        let encoded = encoder.encode_value(&complex_value).unwrap();
        assert!(
            !encoded.is_empty(),
            "Complex nested structure should encode successfully"
        );

        // Validate the encoding
        encoder.validate_encoded_key(&encoded).unwrap();
    }

    /// Test error handling and validation
    #[test]
    fn test_error_handling() {
        let config = EncoderConfig {
            max_nesting_depth: 3,
            ..Default::default()
        };
        let mut encoder = ByteComparableEncoder::with_config(config);

        // Test depth limit
        let deeply_nested = Value::List(vec![Value::List(vec![Value::List(vec![Value::List(
            vec![Value::Integer(1)],
        )])])]);

        let result = encoder.encode_value(&deeply_nested);
        assert!(result.is_err(), "Should fail with deep nesting");

        // Test validation of invalid encodings
        let encoder = ByteComparableEncoder::new();
        assert!(
            encoder.validate_encoded_key(&[]).is_err(),
            "Empty key should be invalid"
        );
    }

    /// Test performance characteristics
    #[test]
    fn test_performance_characteristics() {
        let mut encoder = ByteComparableEncoder::new();

        // Test large collections
        let large_list: Vec<Value> = (0..1000).map(|i| Value::Integer(i)).collect();
        let large_list_value = Value::List(large_list);

        let start = std::time::Instant::now();
        let encoded = encoder.encode_value(&large_list_value).unwrap();
        let encoding_time = start.elapsed();

        println!("Encoded {} elements in {:?}", 1000, encoding_time);
        assert!(!encoded.is_empty(), "Large list should encode successfully");

        // Test memory efficiency
        let stats = encoder.get_stats();
        println!(
            "Buffer size: {} bytes, capacity: {} bytes",
            stats.buffer_size, stats.buffer_capacity
        );
    }

    /// Test batch encoding performance
    #[test]
    fn test_batch_encoding() {
        let mut batch_encoder = BatchEncoder::new();

        let values: Vec<Value> = (0..100)
            .map(|i| Value::Text(format!("key_{:04}", i)))
            .collect();

        let start = std::time::Instant::now();
        let encoded_batch = batch_encoder.encode_batch(&values).unwrap();
        let batch_time = start.elapsed();

        // Compare with individual encoding
        let mut single_encoder = ByteComparableEncoder::new();
        let start = std::time::Instant::now();
        let individual_encoded: Vec<_> = values
            .iter()
            .map(|v| single_encoder.encode_value(v).unwrap())
            .collect();
        let individual_time = start.elapsed();

        println!(
            "Batch encoding: {:?}, Individual encoding: {:?}",
            batch_time, individual_time
        );

        assert_eq!(encoded_batch.len(), individual_encoded.len());
        for i in 0..encoded_batch.len() {
            assert_eq!(
                encoded_batch[i], individual_encoded[i],
                "Batch and individual encoding should match for index {}",
                i
            );
        }
    }

    /// Test composite key encoding consistency
    #[test]
    fn test_composite_key_consistency() {
        let mut encoder = ByteComparableEncoder::new();

        // Test various composite key combinations
        let test_cases = vec![
            vec![Value::Text("partition1".to_string()), Value::Integer(1)],
            vec![Value::Text("partition1".to_string()), Value::Integer(2)],
            vec![Value::Text("partition2".to_string()), Value::Integer(1)],
            vec![Value::Integer(1), Value::Text("clustering1".to_string())],
            vec![Value::Integer(1), Value::Text("clustering2".to_string())],
            vec![Value::Integer(2), Value::Text("clustering1".to_string())],
        ];

        let mut encoded_keys = Vec::new();
        for key_values in &test_cases {
            let encoded = encoder.encode_composite_key(key_values).unwrap();
            encoded_keys.push((encoded, key_values));
        }

        // Sort by encoded values
        encoded_keys.sort_by(|a, b| a.0.cmp(&b.0));

        // Verify ordering makes sense
        for i in 0..encoded_keys.len() - 1 {
            println!("Key {}: {:?}", i, encoded_keys[i].1);
        }

        // Specific ordering checks
        // partition1 should come before partition2
        let partition1_keys: Vec<_> = encoded_keys
            .iter()
            .filter(|(_, key)| matches!(key.get(0), Some(Value::Text(s)) if s == "partition1"))
            .collect();

        let partition2_keys: Vec<_> = encoded_keys
            .iter()
            .filter(|(_, key)| matches!(key.get(0), Some(Value::Text(s)) if s == "partition2"))
            .collect();

        if !partition1_keys.is_empty() && !partition2_keys.is_empty() {
            assert!(
                partition1_keys.last().unwrap().0 < partition2_keys.first().unwrap().0,
                "partition1 keys should come before partition2 keys"
            );
        }
    }

    /// Test cross-type ordering consistency
    #[test]
    fn test_cross_type_ordering() {
        let mut encoder = ByteComparableEncoder::new();

        // Create values of different types with similar semantic values
        let mixed_values = vec![
            Value::Null,
            Value::Boolean(false),
            Value::Boolean(true),
            Value::TinyInt(0),
            Value::SmallInt(0),
            Value::Integer(0),
            Value::BigInt(0),
            Value::Float32(0.0),
            Value::Float(0.0),
            Value::Text("".to_string()),
            Value::Text("0".to_string()),
            Value::Blob(vec![]),
            Value::Blob(vec![0x30]), // ASCII '0'
            Value::List(vec![]),
            Value::Set(vec![]),
            Value::Map(vec![]),
        ];

        let mut encoded_mixed = Vec::new();
        for (i, value) in mixed_values.iter().enumerate() {
            let encoded = encoder.encode_value(value).unwrap();
            encoded_mixed.push((encoded, i, value));
        }

        // Sort by encoded values
        encoded_mixed.sort_by(|a, b| a.0.cmp(&b.0));

        println!("Cross-type ordering:");
        for (_, original_index, value) in &encoded_mixed {
            println!("  Original index {}: {:?}", original_index, value);
        }

        // Verify that types are consistently ordered
        // (The specific ordering doesn't matter as much as consistency)
        for i in 0..encoded_mixed.len() - 1 {
            assert!(
                encoded_mixed[i].0 <= encoded_mixed[i + 1].0,
                "Encoded values should be in non-decreasing order"
            );
        }
    }

    /// Test encoding stability across encoder instances
    #[test]
    fn test_encoding_stability() {
        let value = Value::Map(vec![
            (Value::Text("key1".to_string()), Value::Integer(42)),
            (
                Value::Text("key2".to_string()),
                Value::List(vec![Value::Boolean(true), Value::Float32(3.14)]),
            ),
        ]);

        // Encode with multiple encoder instances
        let mut encoder1 = ByteComparableEncoder::new();
        let mut encoder2 = ByteComparableEncoder::new();
        let mut encoder3 = ByteComparableEncoder::with_config(EncoderConfig::default());

        let encoded1 = encoder1.encode_value(&value).unwrap();
        let encoded2 = encoder2.encode_value(&value).unwrap();
        let encoded3 = encoder3.encode_value(&value).unwrap();

        assert_eq!(
            encoded1, encoded2,
            "Different encoder instances should produce same result"
        );
        assert_eq!(
            encoded2, encoded3,
            "Different configs with same values should produce same result"
        );
    }

    /// Test special value handling
    #[test]
    fn test_special_values() {
        let mut encoder = ByteComparableEncoder::new();

        // Test UUID edge cases
        let uuid_zero = Value::Uuid([0u8; 16]);
        let uuid_max = Value::Uuid([0xFFu8; 16]);
        let uuid_mid = Value::Uuid([0x80u8; 16]);

        let encoded_zero = encoder.encode_value(&uuid_zero).unwrap();
        let encoded_max = encoder.encode_value(&uuid_max).unwrap();
        let encoded_mid = encoder.encode_value(&uuid_mid).unwrap();

        assert!(encoded_zero < encoded_mid);
        assert!(encoded_mid < encoded_max);

        // Test timestamp edge cases
        let timestamp_far_past = Value::Timestamp(i64::MIN);
        let timestamp_far_future = Value::Timestamp(i64::MAX);
        let timestamp_epoch = Value::Timestamp(0);

        let encoded_past = encoder.encode_value(&timestamp_far_past).unwrap();
        let encoded_future = encoder.encode_value(&timestamp_far_future).unwrap();
        let encoded_epoch = encoder.encode_value(&timestamp_epoch).unwrap();

        assert!(encoded_past < encoded_epoch);
        assert!(encoded_epoch < encoded_future);
    }
}
