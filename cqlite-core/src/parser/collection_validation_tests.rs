//! Validation tests for collection parsing with real Cassandra 5.0 format compliance
//!
//! These tests validate that our collection parsing implementation works correctly
//! with the actual Cassandra 5.0 format and the collections_table schema.

use super::*;
use crate::types::Value;
use super::types::{CqlTypeId, parse_list, parse_set, parse_map};
use super::vint::encode_vint;
use crate::schema::{TableSchema, CqlType};

#[cfg(test)]
mod cassandra_format_tests {
    use super::*;

    #[test]
    fn test_collections_table_schema_compatibility() {
        // Test that our parsing is compatible with the collections_table schema
        let schema_json = r#"{
            "keyspace": "test_keyspace",
            "table": "collections_table",
            "partition_keys": [
                {"name": "id", "type": "uuid", "position": 0}
            ],
            "clustering_keys": [],
            "columns": [
                {"name": "id", "type": "uuid", "nullable": false},
                {"name": "list_col", "type": "list<text>", "nullable": true},
                {"name": "set_col", "type": "set<int>", "nullable": true},
                {"name": "map_col", "type": "map<text, int>", "nullable": true},
                {"name": "frozen_list", "type": "frozen<list<text>>", "nullable": true},
                {"name": "frozen_set", "type": "frozen<set<int>>", "nullable": true},
                {"name": "frozen_map", "type": "frozen<map<text, int>>", "nullable": true}
            ]
        }"#;
        
        let schema = TableSchema::from_json(schema_json).unwrap();
        
        // Validate that our schema parsing understands the collection types
        let list_col = schema.columns.iter().find(|c| c.name == "list_col").unwrap();
        assert_eq!(list_col.data_type, "list<text>");
        
        let set_col = schema.columns.iter().find(|c| c.name == "set_col").unwrap();
        assert_eq!(set_col.data_type, "set<int>");
        
        let map_col = schema.columns.iter().find(|c| c.name == "map_col").unwrap();
        assert_eq!(map_col.data_type, "map<text, int>");
    }

    #[test]
    fn test_list_text_parsing_cassandra_format() {
        // Test list<text> parsing with actual Cassandra format
        // Format: [count:vint][element_type:u8][elements...]
        let test_strings = vec!["hello", "world", "cassandra"];
        let mut data = Vec::new();
        
        // Count
        data.extend_from_slice(&encode_vint(test_strings.len() as i64));
        // Element type
        data.push(CqlTypeId::Varchar as u8);
        
        // Elements (text elements have their own length prefixes)
        for s in &test_strings {
            data.extend_from_slice(&encode_vint(s.len() as i64));
            data.extend_from_slice(s.as_bytes());
        }
        
        let (remaining, value) = parse_list(&data).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::List(parsed_list) = value {
            assert_eq!(parsed_list.len(), 3);
            for (i, item) in parsed_list.iter().enumerate() {
                if let Value::Text(text) = item {
                    assert_eq!(text, test_strings[i]);
                } else {
                    panic!("Expected text value at index {}", i);
                }
            }
        } else {
            panic!("Expected list value");
        }
    }

    #[test]
    fn test_set_int_parsing_cassandra_format() {
        // Test set<int> parsing with actual Cassandra format
        let test_ints = vec![1i32, 42, 100, -5];
        let mut data = Vec::new();
        
        // Count
        data.extend_from_slice(&encode_vint(test_ints.len() as i64));
        // Element type
        data.push(CqlTypeId::Int as u8);
        
        // Elements (int elements are fixed 4 bytes each)
        for &i in &test_ints {
            data.extend_from_slice(&i.to_be_bytes());
        }
        
        let (remaining, value) = parse_set(&data).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::Set(parsed_set) = value {
            assert_eq!(parsed_set.len(), 4);
            for (i, item) in parsed_set.iter().enumerate() {
                if let Value::Integer(int_val) = item {
                    assert_eq!(*int_val, test_ints[i]);
                } else {
                    panic!("Expected integer value at index {}", i);
                }
            }
        } else {
            panic!("Expected set value");
        }
    }

    #[test]
    fn test_map_text_int_parsing_cassandra_format() {
        // Test map<text, int> parsing with actual Cassandra format
        let test_pairs = vec![("key1", 10i32), ("key2", 20i32), ("key3", 30i32)];
        let mut data = Vec::new();
        
        // Count
        data.extend_from_slice(&encode_vint(test_pairs.len() as i64));
        // Key type
        data.push(CqlTypeId::Varchar as u8);
        // Value type
        data.push(CqlTypeId::Int as u8);
        
        // Key-value pairs
        for (key, value) in &test_pairs {
            // Key (text with length prefix)
            data.extend_from_slice(&encode_vint(key.len() as i64));
            data.extend_from_slice(key.as_bytes());
            
            // Value (fixed 4-byte int)
            data.extend_from_slice(&value.to_be_bytes());
        }
        
        let (remaining, value) = parse_map(&data).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::Map(parsed_map) = value {
            assert_eq!(parsed_map.len(), 3);
            for (i, (key, value)) in parsed_map.iter().enumerate() {
                if let (Value::Text(key_text), Value::Integer(value_int)) = (key, value) {
                    assert_eq!(key_text, test_pairs[i].0);
                    assert_eq!(*value_int, test_pairs[i].1);
                } else {
                    panic!("Expected text key and integer value at index {}", i);
                }
            }
        } else {
            panic!("Expected map value");
        }
    }

    #[test]
    fn test_memory_safety_large_collections() {
        // Test that we properly reject oversized collections to prevent memory exhaustion
        
        // Test oversized list
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(2_000_000)); // > 1M limit
        data.push(CqlTypeId::Int as u8);
        
        let result = parse_list(&data);
        assert!(result.is_err(), "Should reject lists with > 1M elements");
        
        // Test oversized map
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(2_000_000)); // > 1M limit
        data.push(CqlTypeId::Varchar as u8);
        data.push(CqlTypeId::Int as u8);
        
        let result = parse_map(&data);
        assert!(result.is_err(), "Should reject maps with > 1M elements");
    }

    #[test]
    fn test_empty_collections_edge_cases() {
        // Test empty collections
        let empty_count_data = encode_vint(0);
        
        // Empty list
        let (remaining, value) = parse_list(&empty_count_data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::List(Vec::new()));
        
        // Empty set
        let (remaining, value) = parse_set(&empty_count_data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Set(Vec::new()));
        
        // Empty map
        let (remaining, value) = parse_map(&empty_count_data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Map(Vec::new()));
    }

    #[test]
    fn test_collection_type_identification() {
        // Test that we can correctly identify collection types from type IDs
        assert_eq!(CqlTypeId::try_from(0x20).unwrap(), CqlTypeId::List);
        assert_eq!(CqlTypeId::try_from(0x21).unwrap(), CqlTypeId::Map);
        assert_eq!(CqlTypeId::try_from(0x22).unwrap(), CqlTypeId::Set);
    }

    #[test]
    fn test_mixed_type_validation() {
        // Test that collections maintain type consistency as expected by Cassandra
        // This validates our type system integration
        
        // Create a list with consistent types
        let int_list = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        
        // Validate this would be serializable
        assert!(int_list.validate_collection_types().is_ok());
        
        // Check that the data type is correctly identified
        if let CqlType::List(element_type) = int_list.data_type() {
            assert_eq!(**element_type, CqlType::Int);
        } else {
            panic!("Expected List type");
        }
    }

    #[test]
    fn test_sstable_compatibility_format() {
        // Test that our parsing format is compatible with what SSTable reading expects
        // This ensures integration with the SSTable parser
        
        // Simulate what an SSTable might contain for a list<text> column
        let mut sstable_data = Vec::new();
        
        // Column value length (for the entire collection)
        let collection_content = {
            let mut content = Vec::new();
            content.extend_from_slice(&encode_vint(2)); // count
            content.push(CqlTypeId::Varchar as u8); // element type
            content.extend_from_slice(&encode_vint(5)); // "hello" length
            content.extend_from_slice(b"hello");
            content.extend_from_slice(&encode_vint(5)); // "world" length
            content.extend_from_slice(b"world");
            content
        };
        
        sstable_data.extend_from_slice(&encode_vint(collection_content.len() as i64));
        sstable_data.extend_from_slice(&collection_content);
        
        // Parse as if reading from SSTable
        let (remaining, collection_length) = super::vint::parse_vint_length(&sstable_data).unwrap();
        let (remaining, collection_data) = nom::bytes::complete::take::<_, _, nom::error::Error<_>>(collection_length)(remaining).unwrap();
        
        // Now parse the collection itself
        let (_, value) = parse_list(collection_data).unwrap();
        
        if let Value::List(elements) = value {
            assert_eq!(elements.len(), 2);
            assert_eq!(elements[0], Value::Text("hello".to_string()));
            assert_eq!(elements[1], Value::Text("world".to_string()));
        } else {
            panic!("Expected list value");
        }
        
        assert!(remaining.is_empty());
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    
    #[test]
    fn test_collection_parsing_performance() {
        // Test that collection parsing performs reasonably with moderate sizes
        let start = std::time::Instant::now();
        
        // Create a moderately large list (1000 elements)
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(1000));
        data.push(CqlTypeId::Int as u8);
        
        for i in 0..1000i32 {
            data.extend_from_slice(&i.to_be_bytes());
        }
        
        let (_, value) = parse_list(&data).unwrap();
        
        let duration = start.elapsed();
        assert!(duration.as_millis() < 100, "Parsing 1000 elements should take < 100ms, took {:?}", duration);
        
        if let Value::List(elements) = value {
            assert_eq!(elements.len(), 1000);
            // Spot check a few elements
            assert_eq!(elements[0], Value::Integer(0));
            assert_eq!(elements[500], Value::Integer(500));
            assert_eq!(elements[999], Value::Integer(999));
        } else {
            panic!("Expected list value");
        }
    }
}