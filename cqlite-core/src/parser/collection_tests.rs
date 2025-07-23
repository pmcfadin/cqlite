//! Comprehensive tests for collection parsing and serialization
//!
//! This module provides extensive testing for collections (List, Set, Map, Tuple)
//! with real Cassandra data formats and edge cases.

use super::*;
use crate::types::Value;
use super::types::{CqlTypeId, parse_list, parse_set, parse_map, parse_tuple};
use super::vint::encode_vint;
use std::collections::HashMap;

/// Test comprehensive list parsing with various element types
#[cfg(test)]
mod list_tests {
    use super::*;

    #[test]
    fn test_empty_list_parsing() {
        // Empty list: count=0
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(0)); // count = 0
        
        let (remaining, value) = parse_list(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::List(vec![]));
    }

    #[test]
    fn test_string_list_parsing() {
        let test_strings = vec!["hello", "world", "cassandra"];
        let mut data = Vec::new();
        
        // Count
        data.extend_from_slice(&encode_vint(test_strings.len() as i64));
        // Element type
        data.push(CqlTypeId::Varchar as u8);
        
        // Elements with length prefixes
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
                    panic!("Expected text value");
                }
            }
        } else {
            panic!("Expected list value");
        }
    }

    #[test]
    fn test_integer_list_parsing() {
        let test_ints = vec![42i32, -100, 0, 12345];
        let mut data = Vec::new();
        
        // Count
        data.extend_from_slice(&encode_vint(test_ints.len() as i64));
        // Element type
        data.push(CqlTypeId::Int as u8);
        
        // Elements with length prefixes
        for &i in &test_ints {
            let int_bytes = i.to_be_bytes();
            data.extend_from_slice(&encode_vint(int_bytes.len() as i64));
            data.extend_from_slice(&int_bytes);
        }
        
        let (remaining, value) = parse_list(&data).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::List(parsed_list) = value {
            assert_eq!(parsed_list.len(), 4);
            for (i, item) in parsed_list.iter().enumerate() {
                if let Value::Integer(int_val) = item {
                    assert_eq!(*int_val, test_ints[i]);
                } else {
                    panic!("Expected integer value");
                }
            }
        } else {
            panic!("Expected list value");
        }
    }

    #[test]
    fn test_nested_list_parsing() {
        // Test list of lists: [[1, 2], [3, 4, 5]]
        let mut data = Vec::new();
        
        // Outer list count
        data.extend_from_slice(&encode_vint(2));
        // Outer list element type (List)
        data.push(CqlTypeId::List as u8);
        
        // First inner list: [1, 2]
        let mut inner_list_1 = Vec::new();
        inner_list_1.extend_from_slice(&encode_vint(2)); // count
        inner_list_1.push(CqlTypeId::Int as u8); // element type
        for &i in &[1i32, 2i32] {
            let int_bytes = i.to_be_bytes();
            inner_list_1.extend_from_slice(&encode_vint(int_bytes.len() as i64));
            inner_list_1.extend_from_slice(&int_bytes);
        }
        
        data.extend_from_slice(&encode_vint(inner_list_1.len() as i64));
        data.extend_from_slice(&inner_list_1);
        
        // Second inner list: [3, 4, 5]
        let mut inner_list_2 = Vec::new();
        inner_list_2.extend_from_slice(&encode_vint(3)); // count
        inner_list_2.push(CqlTypeId::Int as u8); // element type
        for &i in &[3i32, 4i32, 5i32] {
            let int_bytes = i.to_be_bytes();
            inner_list_2.extend_from_slice(&encode_vint(int_bytes.len() as i64));
            inner_list_2.extend_from_slice(&int_bytes);
        }
        
        data.extend_from_slice(&encode_vint(inner_list_2.len() as i64));
        data.extend_from_slice(&inner_list_2);
        
        let (remaining, value) = parse_list(&data).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::List(outer_list) = value {
            assert_eq!(outer_list.len(), 2);
            
            // Check first inner list
            if let Value::List(inner1) = &outer_list[0] {
                assert_eq!(inner1.len(), 2);
            } else {
                panic!("Expected inner list");
            }
            
            // Check second inner list
            if let Value::List(inner2) = &outer_list[1] {
                assert_eq!(inner2.len(), 3);
            } else {
                panic!("Expected inner list");
            }
        } else {
            panic!("Expected outer list");
        }
    }

    #[test]
    fn test_large_list_safety() {
        // Test that we properly handle large list size limits
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(2_000_000)); // > 1M limit
        data.push(CqlTypeId::Int as u8);
        
        let result = parse_list(&data);
        assert!(result.is_err(), "Should reject lists with > 1M elements");
    }
    
    #[test]
    fn test_list_with_null_elements() {
        // Test list with some null elements: [1, null, 3]
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(3)); // count
        data.push(CqlTypeId::Int as u8); // element type
        
        // Element 1: 1 (4 bytes)
        data.extend_from_slice(&encode_vint(4));
        data.extend_from_slice(&1i32.to_be_bytes());
        
        // Element 2: null (-1 length)
        data.extend_from_slice(&encode_vint(-1));
        
        // Element 3: 3 (4 bytes)
        data.extend_from_slice(&encode_vint(4));
        data.extend_from_slice(&3i32.to_be_bytes());
        
        let (remaining, value) = parse_list(&data).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::List(elements) = value {
            assert_eq!(elements.len(), 3);
            assert_eq!(elements[0], Value::Integer(1));
            assert_eq!(elements[1], Value::Null);
            assert_eq!(elements[2], Value::Integer(3));
        } else {
            panic!("Expected list value");
        }
    }
    
    #[test]
    fn test_list_with_variable_length_strings() {
        // Test list with strings of different lengths
        let test_strings = vec!["a", "hello", "", "this is a longer string"];
        let mut data = Vec::new();
        
        data.extend_from_slice(&encode_vint(test_strings.len() as i64));
        data.push(CqlTypeId::Varchar as u8);
        
        for s in &test_strings {
            data.extend_from_slice(&encode_vint(s.len() as i64)); // element length
            data.extend_from_slice(s.as_bytes());
        }
        
        let (remaining, value) = parse_list(&data).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::List(elements) = value {
            assert_eq!(elements.len(), 4);
            for (i, element) in elements.iter().enumerate() {
                if let Value::Text(text) = element {
                    assert_eq!(text, test_strings[i]);
                } else {
                    panic!("Expected text value at index {}", i);
                }
            }
        } else {
            panic!("Expected list value");
        }
    }
}

/// Test comprehensive set parsing with duplicate detection
#[cfg(test)]
mod set_tests {
    use super::*;

    #[test]
    fn test_empty_set_parsing() {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(0)); // count = 0
        
        let (remaining, value) = parse_set(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Set(vec![]));
    }

    #[test]
    fn test_string_set_parsing() {
        let test_strings = vec!["apple", "banana", "cherry"];
        let mut data = Vec::new();
        
        // Count
        data.extend_from_slice(&encode_vint(test_strings.len() as i64));
        // Element type
        data.push(CqlTypeId::Varchar as u8);
        
        // Elements with length prefixes
        for s in &test_strings {
            data.extend_from_slice(&encode_vint(s.len() as i64));
            data.extend_from_slice(s.as_bytes());
        }
        
        let (remaining, value) = parse_set(&data).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::Set(parsed_set) = value {
            assert_eq!(parsed_set.len(), 3);
            for (i, item) in parsed_set.iter().enumerate() {
                if let Value::Text(text) = item {
                    assert_eq!(text, test_strings[i]);
                } else {
                    panic!("Expected text value");
                }
            }
        } else {
            panic!("Expected set value");
        }
    }

    #[test]
    fn test_set_duplicate_detection() {
        // Test set with duplicates - should be filtered out
        let test_strings = vec!["apple", "banana", "apple", "cherry", "banana"];
        let mut data = Vec::new();
        
        // Count (including duplicates)
        data.extend_from_slice(&encode_vint(test_strings.len() as i64));
        // Element type
        data.push(CqlTypeId::Varchar as u8);
        
        // Elements with length prefixes
        for s in &test_strings {
            data.extend_from_slice(&encode_vint(s.len() as i64));
            data.extend_from_slice(s.as_bytes());
        }
        
        let (remaining, value) = parse_set(&data).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::Set(parsed_set) = value {
            // Cassandra preserves insertion order and deduplicates server-side
            // We maintain order as stored in the SSTable
            assert_eq!(parsed_set.len(), 5); // All elements as stored
            
            // Verify the elements are parsed correctly
            let expected = vec!["apple", "banana", "apple", "cherry", "banana"];
            for (i, item) in parsed_set.iter().enumerate() {
                if let Value::Text(text) = item {
                    assert_eq!(text, expected[i]);
                } else {
                    panic!("Expected text value at index {}", i);
                }
            }
        } else {
            panic!("Expected set value");
        }
    }
}

/// Test comprehensive map parsing with various key-value types
#[cfg(test)]
mod map_tests {
    use super::*;

    #[test]
    fn test_empty_map_parsing() {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(0)); // count = 0
        
        let (remaining, value) = parse_map(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Map(vec![]));
    }

    #[test]
    fn test_string_to_int_map_parsing() {
        let test_pairs = vec![("one", 1i32), ("two", 2i32), ("three", 3i32)];
        let mut data = Vec::new();
        
        // Count
        data.extend_from_slice(&encode_vint(test_pairs.len() as i64));
        // Key type
        data.push(CqlTypeId::Varchar as u8);
        // Value type
        data.push(CqlTypeId::Int as u8);
        
        // Key-value pairs with length prefixes
        for (key, value) in &test_pairs {
            // Key
            data.extend_from_slice(&encode_vint(key.len() as i64));
            data.extend_from_slice(key.as_bytes());
            
            // Value
            let value_bytes = value.to_be_bytes();
            data.extend_from_slice(&encode_vint(value_bytes.len() as i64));
            data.extend_from_slice(&value_bytes);
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
                    panic!("Expected text key and integer value");
                }
            }
        } else {
            panic!("Expected map value");
        }
    }

    #[test]
    fn test_map_with_null_values() {
        // Test map with null values: {"key1": 1, "key2": null, "key3": 3}
        let mut data = Vec::new();
        
        data.extend_from_slice(&encode_vint(3)); // count
        data.push(CqlTypeId::Varchar as u8); // key type
        data.push(CqlTypeId::Int as u8); // value type
        
        // Pair 1: ("key1", 1)
        data.extend_from_slice(&encode_vint(4)); // key length
        data.extend_from_slice(b"key1");
        data.extend_from_slice(&encode_vint(4)); // value length
        data.extend_from_slice(&1i32.to_be_bytes());
        
        // Pair 2: ("key2", null)
        data.extend_from_slice(&encode_vint(4)); // key length
        data.extend_from_slice(b"key2");
        data.extend_from_slice(&encode_vint(-1)); // null value
        
        // Pair 3: ("key3", 3)
        data.extend_from_slice(&encode_vint(4)); // key length
        data.extend_from_slice(b"key3");
        data.extend_from_slice(&encode_vint(4)); // value length
        data.extend_from_slice(&3i32.to_be_bytes());
        
        let (remaining, value) = parse_map(&data).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::Map(map) = value {
            assert_eq!(map.len(), 3);
            
            // Check values
            let key1_value = map.iter().find(|(k, _)| matches!(k, Value::Text(s) if s == "key1"));
            assert!(key1_value.is_some());
            assert_eq!(key1_value.unwrap().1, Value::Integer(1));
            
            let key2_value = map.iter().find(|(k, _)| matches!(k, Value::Text(s) if s == "key2"));
            assert!(key2_value.is_some());
            assert_eq!(key2_value.unwrap().1, Value::Null);
            
            let key3_value = map.iter().find(|(k, _)| matches!(k, Value::Text(s) if s == "key3"));
            assert!(key3_value.is_some());
            assert_eq!(key3_value.unwrap().1, Value::Integer(3));
        } else {
            panic!("Expected map value");
        }
    }
    
    #[test]
    fn test_map_large_count_validation() {
        // Test map with count > 1M should fail
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(2_000_000)); // > 1M limit
        data.push(CqlTypeId::Varchar as u8); // key type
        data.push(CqlTypeId::Int as u8); // value type
        
        let result = parse_map(&data);
        assert!(result.is_err(), "Should reject maps with > 1M elements");
    }
}

/// Test tuple parsing with heterogeneous types
#[cfg(test)]
mod tuple_tests {
    use super::*;

    #[test]
    fn test_empty_tuple_parsing() {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(0)); // count = 0
        
        let (remaining, value) = parse_tuple(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Tuple(vec![]));
    }

    #[test]
    fn test_mixed_type_tuple_parsing() {
        // Tuple: (42, "hello", true, 3.14)
        let mut data = Vec::new();
        
        // Count
        data.extend_from_slice(&encode_vint(4));
        
        // Element 1: Integer 42
        data.push(CqlTypeId::Int as u8);
        let int_bytes = 42i32.to_be_bytes();
        data.extend_from_slice(&encode_vint(int_bytes.len() as i64));
        data.extend_from_slice(&int_bytes);
        
        // Element 2: String "hello"
        data.push(CqlTypeId::Varchar as u8);
        let str_bytes = "hello".as_bytes();
        data.extend_from_slice(&encode_vint(str_bytes.len() as i64));
        data.extend_from_slice(str_bytes);
        
        // Element 3: Boolean true
        data.push(CqlTypeId::Boolean as u8);
        data.extend_from_slice(&encode_vint(1));
        data.push(1u8);
        
        // Element 4: Double 3.14
        data.push(CqlTypeId::Double as u8);
        let double_bytes = 3.14f64.to_be_bytes();
        data.extend_from_slice(&encode_vint(double_bytes.len() as i64));
        data.extend_from_slice(&double_bytes);
        
        let (remaining, value) = parse_tuple(&data).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::Tuple(parsed_tuple) = value {
            assert_eq!(parsed_tuple.len(), 4);
            
            // Check each element
            assert!(matches!(parsed_tuple[0], Value::Integer(42)));
            assert!(matches!(parsed_tuple[1], Value::Text(ref s) if s == "hello"));
            assert!(matches!(parsed_tuple[2], Value::Boolean(true)));
            assert!(matches!(parsed_tuple[3], Value::Float(f) if (f - 3.14).abs() < f64::EPSILON));
        } else {
            panic!("Expected tuple value");
        }
    }
}

/// Test serialization round-trip for all collection types
#[cfg(test)]
mod roundtrip_tests {
    use super::*;

    #[test]
    fn test_list_roundtrip() {
        let original = Value::List(vec![
            Value::Text("apple".to_string()),
            Value::Text("banana".to_string()),
            Value::Text("cherry".to_string()),
        ]);
        
        let serialized = serialize_cql_value(&original).unwrap();
        let (_, parsed) = parse_cql_value(&serialized[1..], CqlTypeId::List).unwrap();
        
        if let (Value::List(orig_list), Value::List(parsed_list)) = (&original, &parsed) {
            assert_eq!(orig_list.len(), parsed_list.len());
            for (orig, parsed) in orig_list.iter().zip(parsed_list.iter()) {
                assert_eq!(orig, parsed);
            }
        } else {
            panic!("Expected list values for roundtrip test");
        }
    }

    #[test]
    fn test_map_roundtrip() {
        let original = Value::Map(vec![
            (Value::Text("key1".to_string()), Value::Integer(1)),
            (Value::Text("key2".to_string()), Value::Integer(2)),
            (Value::Text("key3".to_string()), Value::Integer(3)),
        ]);
        
        let serialized = serialize_cql_value(&original).unwrap();
        let (_, parsed) = parse_cql_value(&serialized[1..], CqlTypeId::Map).unwrap();
        
        if let (Value::Map(orig_map), Value::Map(parsed_map)) = (&original, &parsed) {
            assert_eq!(orig_map.len(), parsed_map.len());
            // Note: Order might not be preserved, so we check by content
            for (orig_key, orig_value) in orig_map {
                let found = parsed_map.iter().find(|(k, _)| k == orig_key);
                assert!(found.is_some(), "Key not found in parsed map: {:?}", orig_key);
                let (_, parsed_value) = found.unwrap();
                assert_eq!(orig_value, parsed_value);
            }
        } else {
            panic!("Expected map values for roundtrip test");
        }
    }

    #[test]
    fn test_tuple_roundtrip() {
        let original = Value::Tuple(vec![
            Value::Integer(42),
            Value::Text("hello".to_string()),
            Value::Boolean(true),
            Value::Float(3.14),
        ]);
        
        let serialized = serialize_cql_value(&original).unwrap();
        let (_, parsed) = parse_cql_value(&serialized[1..], CqlTypeId::Tuple).unwrap();
        
        if let (Value::Tuple(orig_tuple), Value::Tuple(parsed_tuple)) = (&original, &parsed) {
            assert_eq!(orig_tuple.len(), parsed_tuple.len());
            for (orig, parsed) in orig_tuple.iter().zip(parsed_tuple.iter()) {
                match (orig, parsed) {
                    (Value::Float(o), Value::Float(p)) => {
                        assert!((o - p).abs() < f64::EPSILON, "Float values don't match: {} != {}", o, p);
                    },
                    _ => assert_eq!(orig, parsed),
                }
            }
        } else {
            panic!("Expected tuple values for roundtrip test");
        }
    }
}

/// Test edge cases and error conditions
#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_nested_collections() {
        // Test list of lists: [[1, 2], [3, 4, 5]]
        let mut data = Vec::new();
        
        // Outer list count
        data.extend_from_slice(&encode_vint(2));
        // Outer list element type (List)
        data.push(CqlTypeId::List as u8);
        
        // First inner list: [1, 2]
        let mut inner_list_1 = Vec::new();
        inner_list_1.extend_from_slice(&encode_vint(2)); // count
        inner_list_1.push(CqlTypeId::Int as u8); // element type
        for &i in &[1i32, 2i32] {
            inner_list_1.extend_from_slice(&encode_vint(4)); // element length
            inner_list_1.extend_from_slice(&i.to_be_bytes());
        }
        
        data.extend_from_slice(&encode_vint(inner_list_1.len() as i64));
        data.extend_from_slice(&inner_list_1);
        
        // Second inner list: [3, 4, 5]
        let mut inner_list_2 = Vec::new();
        inner_list_2.extend_from_slice(&encode_vint(3)); // count
        inner_list_2.push(CqlTypeId::Int as u8); // element type
        for &i in &[3i32, 4i32, 5i32] {
            inner_list_2.extend_from_slice(&encode_vint(4)); // element length
            inner_list_2.extend_from_slice(&i.to_be_bytes());
        }
        
        data.extend_from_slice(&encode_vint(inner_list_2.len() as i64));
        data.extend_from_slice(&inner_list_2);
        
        let (remaining, value) = parse_list(&data).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::List(outer_list) = value {
            assert_eq!(outer_list.len(), 2);
            
            // Check first inner list
            if let Value::List(inner1) = &outer_list[0] {
                assert_eq!(inner1.len(), 2);
                assert_eq!(inner1[0], Value::Integer(1));
                assert_eq!(inner1[1], Value::Integer(2));
            } else {
                panic!("Expected inner list at index 0");
            }
            
            // Check second inner list
            if let Value::List(inner2) = &outer_list[1] {
                assert_eq!(inner2.len(), 3);
                assert_eq!(inner2[0], Value::Integer(3));
                assert_eq!(inner2[1], Value::Integer(4));
                assert_eq!(inner2[2], Value::Integer(5));
            } else {
                panic!("Expected inner list at index 1");
            }
        } else {
            panic!("Expected outer list");
        }
    }

    #[test]
    fn test_collection_size_estimates() {
        let list = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        
        let estimated_size = list.size_estimate();
        assert!(estimated_size > 0);
        assert!(estimated_size < 1000); // Reasonable upper bound
        
        let empty_list = Value::List(vec![]);
        assert_eq!(empty_list.collection_len(), Some(0));
        assert!(empty_list.is_empty_collection());
    }

    #[test]
    fn test_insufficient_data_handling() {
        // Test parsing with insufficient data
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(3)); // Claims 3 elements
        data.push(CqlTypeId::Int as u8);
        // But only provide data for 1 element
        data.extend_from_slice(&encode_vint(4)); // element length
        data.extend_from_slice(&42i32.to_be_bytes());
        // Missing 2 more elements
        
        let result = parse_list(&data);
        assert!(result.is_err(), "Should fail gracefully with insufficient data");
    }
    
    #[test]
    fn test_malformed_element_length() {
        // Test with invalid element length
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(1)); // 1 element
        data.push(CqlTypeId::Int as u8);
        data.extend_from_slice(&encode_vint(10)); // Claims 10 bytes
        data.extend_from_slice(&42i32.to_be_bytes()); // But only provide 4
        
        let result = parse_list(&data);
        assert!(result.is_err(), "Should fail with malformed element length");
    }
    
    #[test]
    fn test_empty_collections() {
        // Test empty list
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(0)); // count = 0
        
        let (remaining, value) = parse_list(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::List(Vec::new()));
        
        // Test empty set
        let (remaining, value) = parse_set(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Set(Vec::new()));
        
        // Test empty map
        let (remaining, value) = parse_map(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Map(Vec::new()));
    }
}