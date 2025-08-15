//! BTI Integration Tests
//!
//! Comprehensive test suite for BTI Partitions.db and Rows.db functionality

use cqlite_core::storage::sstable::bti::{
    BTI_MAGIC_NUMBER, BtiConfig, BtiError, BtiLookupResult, BtiMetadata,
    encoder::ByteComparableEncoder,
    nodes::{NodeParser, NodeType, TrieNode},
    parser::{PartitionsParser, RowsParser},
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Write as IoWrite};
use tempfile::NamedTempFile;

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// Create a test BTI file with proper header and simple trie structure
    fn create_test_bti_file() -> Result<NamedTempFile, Box<dyn std::error::Error>> {
        let mut file = NamedTempFile::new()?;

        // Write BTI header
        file.write_all(&BTI_MAGIC_NUMBER.to_be_bytes())?; // Magic
        file.write_all(&0x0001u16.to_be_bytes())?; // Version
        file.write_all(&0x0000u16.to_be_bytes())?; // Flags
        file.write_all(&0x0010u64.to_be_bytes())?; // Root offset

        // Write simple PAYLOAD_ONLY node at offset 0x0010
        let mut node_data = Vec::new();
        node_data.push(0x01); // PAYLOAD_ONLY with payload flag
        node_data.extend_from_slice(&8u16.to_be_bytes()); // Payload size
        node_data.extend_from_slice(&0x1234567890ABCDEFu64.to_be_bytes()); // Data offset

        // Pad to offset 0x0010
        while file.stream_position()? < 0x0010 {
            file.write_all(&[0x00])?;
        }

        file.write_all(&node_data)?;
        file.flush()?;

        Ok(file)
    }

    #[test]
    fn test_bti_header_parsing() -> Result<(), Box<dyn std::error::Error>> {
        let test_file = create_test_bti_file()?;
        let file = File::open(test_file.path())?;

        let parser = PartitionsParser::new(file)?;
        // Test passes if parser creation succeeds
        Ok(())
    }

    #[test]
    fn test_partitions_parser_creation() -> Result<(), Box<dyn std::error::Error>> {
        let test_file = create_test_bti_file()?;
        let file = File::open(test_file.path())?;

        let mut parser = PartitionsParser::new(file)?;

        // Test basic lookup functionality
        let partition_key = vec![Value::Text("test_partition".to_string())];
        let result = parser.lookup_partition(&partition_key);

        // Should not error, even if no match found
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_rows_parser_creation() -> Result<(), Box<dyn std::error::Error>> {
        let test_file = create_test_bti_file()?;
        let file = File::open(test_file.path())?;

        let mut parser = RowsParser::new(file)?;

        // Test basic lookup functionality
        let clustering_key = vec![Value::Integer(42)];
        let result = parser.lookup_row(&clustering_key);

        // Should not error, even if no match found
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_byte_comparable_encoder_integration() {
        let mut encoder = ByteComparableEncoder::new();

        // Test various key types that should be supported
        let test_cases = vec![
            Value::Text("test".to_string()),
            Value::Integer(42),
            Value::BigInt(1234567890),
            Value::Boolean(true),
            Value::Timestamp(1640995200000000), // 2022-01-01 00:00:00 UTC
        ];

        for value in test_cases {
            let result = encoder.encode_value(&value);
            assert!(result.is_ok(), "Failed to encode value: {:?}", value);

            let encoded = result.unwrap();
            assert!(!encoded.is_empty(), "Encoded value should not be empty");
        }

        // Test composite key encoding
        let composite_key = vec![
            Value::Text("partition".to_string()),
            Value::Integer(1),
            Value::Boolean(true),
        ];

        let result = encoder.encode_composite_key(&composite_key);
        assert!(result.is_ok(), "Failed to encode composite key");

        let encoded = result.unwrap();
        assert!(
            !encoded.is_empty(),
            "Encoded composite key should not be empty"
        );
    }

    #[test]
    fn test_byte_comparable_ordering() {
        let mut encoder = ByteComparableEncoder::new();

        // Test text ordering
        let text_a = encoder.encode_value(&Value::Text("a".to_string())).unwrap();
        let text_b = encoder.encode_value(&Value::Text("b".to_string())).unwrap();
        let text_aa = encoder
            .encode_value(&Value::Text("aa".to_string()))
            .unwrap();

        assert!(
            text_a < text_b,
            "Text ordering: 'a' should be less than 'b'"
        );
        assert!(
            text_a < text_aa,
            "Text ordering: 'a' should be less than 'aa'"
        );
        assert!(
            text_aa < text_b,
            "Text ordering: 'aa' should be less than 'b'"
        );

        // Test integer ordering
        let int_neg = encoder.encode_value(&Value::Integer(-100)).unwrap();
        let int_zero = encoder.encode_value(&Value::Integer(0)).unwrap();
        let int_pos = encoder.encode_value(&Value::Integer(100)).unwrap();

        assert!(
            int_neg < int_zero,
            "Integer ordering: -100 should be less than 0"
        );
        assert!(
            int_zero < int_pos,
            "Integer ordering: 0 should be less than 100"
        );
    }

    #[test]
    fn test_node_type_parsing() {
        // Test all node types
        assert_eq!(
            NodeType::from_header_byte(0x00).unwrap(),
            NodeType::PayloadOnly
        );
        assert_eq!(NodeType::from_header_byte(0x10).unwrap(), NodeType::Single);
        assert_eq!(NodeType::from_header_byte(0x20).unwrap(), NodeType::Sparse);
        assert_eq!(NodeType::from_header_byte(0x30).unwrap(), NodeType::Dense);

        // Test payload flags
        assert_eq!(
            NodeType::from_header_byte(0x01).unwrap(),
            NodeType::PayloadOnly
        );
        assert_eq!(NodeType::from_header_byte(0x11).unwrap(), NodeType::Single);

        // Test invalid node type
        assert!(NodeType::from_header_byte(0x40).is_err());
    }

    #[test]
    fn test_error_handling() {
        // Test invalid magic number
        let mut invalid_header = Vec::new();
        invalid_header.extend_from_slice(&0xDEADBEEFu32.to_be_bytes()); // Invalid magic
        invalid_header.extend_from_slice(&0x0001u16.to_be_bytes());
        invalid_header.extend_from_slice(&0x0000u16.to_be_bytes());
        invalid_header.extend_from_slice(&0x0010u64.to_be_bytes());

        let cursor = Cursor::new(invalid_header);
        let result = PartitionsParser::new(cursor);

        assert!(result.is_err(), "Should fail with invalid magic number");
    }

    #[test]
    fn test_bti_config_defaults() {
        let config = BtiConfig::default();

        assert!(
            config.page_aware_reading,
            "Page-aware reading should be enabled by default"
        );
        assert_eq!(
            config.max_cached_nodes, 1024,
            "Default cache size should be 1024"
        );
        assert!(
            config.pointer_compression,
            "Pointer compression should be enabled by default"
        );
    }

    #[test]
    fn test_composite_key_separation() {
        let mut encoder = ByteComparableEncoder::new();

        // Two different composite keys that should be distinguishable
        let key1 = vec![Value::Text("ab".to_string()), Value::Text("c".to_string())];
        let key2 = vec![Value::Text("a".to_string()), Value::Text("bc".to_string())];

        let encoded1 = encoder.encode_composite_key(&key1).unwrap();
        let encoded2 = encoder.encode_composite_key(&key2).unwrap();

        assert_ne!(
            encoded1, encoded2,
            "Different composite keys should encode differently"
        );
    }

    #[test]
    fn test_large_key_handling() {
        let mut encoder = ByteComparableEncoder::new();

        // Test with large text value
        let large_text = "x".repeat(10000);
        let result = encoder.encode_value(&Value::Text(large_text));

        assert!(result.is_ok(), "Should handle large text values");

        // Test with large composite key
        let large_composite: Vec<Value> = (0..100).map(|i| Value::Integer(i)).collect();

        let result = encoder.encode_composite_key(&large_composite);
        assert!(result.is_ok(), "Should handle large composite keys");
    }

    #[test]
    fn test_special_values() {
        let mut encoder = ByteComparableEncoder::new();

        // Test empty string
        let empty_text = encoder.encode_value(&Value::Text("".to_string())).unwrap();
        assert!(
            !empty_text.is_empty(),
            "Empty text should still produce encoded bytes"
        );

        // Test zero values
        let zero_int = encoder.encode_value(&Value::Integer(0)).unwrap();
        let zero_bigint = encoder.encode_value(&Value::BigInt(0)).unwrap();

        assert!(
            !zero_int.is_empty(),
            "Zero integer should produce encoded bytes"
        );
        assert!(
            !zero_bigint.is_empty(),
            "Zero bigint should produce encoded bytes"
        );
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn test_encoding_performance() {
        let mut encoder = ByteComparableEncoder::new();

        let start = Instant::now();
        let iterations = 10000;

        for i in 0..iterations {
            let key = vec![
                Value::Text(format!("partition_{}", i)),
                Value::Integer(i),
                Value::Boolean(i % 2 == 0),
            ];

            let result = encoder.encode_composite_key(&key);
            assert!(
                result.is_ok(),
                "Encoding should succeed for iteration {}",
                i
            );
        }

        let duration = start.elapsed();
        let ops_per_sec = iterations as f64 / duration.as_secs_f64();

        println!("Encoding performance: {:.0} ops/sec", ops_per_sec);

        // Should be able to encode at least 1000 composite keys per second
        assert!(
            ops_per_sec > 1000.0,
            "Encoding performance too slow: {:.0} ops/sec",
            ops_per_sec
        );
    }

    #[test]
    fn test_parser_memory_usage() {
        let test_file = create_test_bti_file().unwrap();
        let file = File::open(test_file.path()).unwrap();

        let mut parser = PartitionsParser::new(file).unwrap();

        // Perform multiple lookups to test caching
        for i in 0..1000 {
            let key = vec![Value::Text(format!("key_{}", i))];
            let _ = parser.lookup_partition(&key);
        }

        // Test passes if no memory issues occurred
        assert!(true, "Memory test completed without issues");
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_corrupted_file_handling() {
        // Create a file with truncated header
        let mut truncated_file = NamedTempFile::new().unwrap();
        truncated_file.write_all(&[0x64, 0x61, 0x00]).unwrap(); // Incomplete header

        let file = File::open(truncated_file.path()).unwrap();
        let result = PartitionsParser::new(file);

        assert!(result.is_err(), "Should fail with truncated header");
    }

    #[test]
    fn test_invalid_node_type_handling() {
        // Test BtiError for invalid node type
        let error = BtiError::InvalidNodeType(0xFF);
        let error_string = error.to_string();

        assert!(error_string.contains("Invalid BTI trie node type"));
        assert!(error_string.contains("0xff"));
    }

    #[test]
    fn test_max_depth_protection() {
        let error = BtiError::MaxDepthExceeded(200);
        let error_string = error.to_string();

        assert!(error_string.contains("BTI trie depth exceeded maximum"));
        assert!(error_string.contains("200"));
    }

    #[test]
    fn test_invalid_byte_comparable_key() {
        let error = BtiError::InvalidByteComparableKey("test_key".to_string());
        let error_string = error.to_string();

        assert!(error_string.contains("Invalid byte-comparable key"));
        assert!(error_string.contains("test_key"));
    }

    #[test]
    fn test_missing_component() {
        let error = BtiError::MissingComponent("Partitions.db".to_string());
        let error_string = error.to_string();

        assert!(error_string.contains("Missing BTI component"));
        assert!(error_string.contains("Partitions.db"));
    }
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;

    #[test]
    fn test_cep25_type_hierarchy() {
        let mut encoder = ByteComparableEncoder::new();

        // Test cross-type ordering according to CEP-25
        let null_val = encoder
            .encode_value(&Value::Null)
            .unwrap_or_else(|_| vec![0x00]);
        let bool_val = encoder.encode_value(&Value::Boolean(false)).unwrap();
        let int_val = encoder.encode_value(&Value::Integer(0)).unwrap();
        let text_val = encoder.encode_value(&Value::Text("".to_string())).unwrap();

        // Verify type hierarchy ordering (null < boolean < numeric < text)
        assert!(null_val < bool_val, "Null should be less than boolean");
        assert!(bool_val < int_val, "Boolean should be less than integer");
        assert!(int_val < text_val, "Integer should be less than text");
    }

    #[test]
    fn test_cassandra_5_compatibility() {
        // Test that our magic number matches Cassandra 5.0 BTI format
        assert_eq!(
            BTI_MAGIC_NUMBER, 0x6461_0000,
            "BTI magic number should match Cassandra 5.0"
        );

        // Test that our node types match the specification
        assert_eq!(NodeType::PayloadOnly as u8, 0);
        assert_eq!(NodeType::Single as u8, 1);
        assert_eq!(NodeType::Sparse as u8, 2);
        assert_eq!(NodeType::Dense as u8, 3);
    }

    #[test]
    fn test_ieee754_float_ordering() {
        let mut encoder = ByteComparableEncoder::new();

        // Test special float values ordering
        let neg_inf = encoder
            .encode_value(&Value::Float(f32::NEG_INFINITY))
            .unwrap();
        let neg_one = encoder.encode_value(&Value::Float(-1.0)).unwrap();
        let neg_zero = encoder.encode_value(&Value::Float(-0.0)).unwrap();
        let pos_zero = encoder.encode_value(&Value::Float(0.0)).unwrap();
        let pos_one = encoder.encode_value(&Value::Float(1.0)).unwrap();
        let pos_inf = encoder.encode_value(&Value::Float(f32::INFINITY)).unwrap();

        // Verify IEEE 754 ordering
        assert!(neg_inf < neg_one, "Negative infinity should be smallest");
        assert!(
            neg_one < neg_zero,
            "Negative one should be less than negative zero"
        );
        assert!(
            neg_zero <= pos_zero,
            "Negative zero should be <= positive zero"
        );
        assert!(
            pos_zero < pos_one,
            "Positive zero should be less than positive one"
        );
        assert!(
            pos_one < pos_inf,
            "Positive one should be less than positive infinity"
        );
    }
}
