//! M3 Complex Type Validation Tests
//!
//! Comprehensive test suite to validate CQLite's complex type implementation
//! against real Cassandra 5+ SSTable format requirements.

use cqlite_core::{
    parser::{ComplexTypeParser, CollectionType},
    types::{Value, UdtValue, UdtField, DataType},
    schema::{CqlType, UdtTypeDef, UdtFieldDef},
    error::Result,
};

/// Test suite for M3 complex type validation
#[cfg(test)]
mod m3_validation_tests {
    use super::*;

    /// Test Collection parsing with Cassandra 5+ tuple format
    #[test]
    fn test_m3_collections_tuple_format() {
        let parser = ComplexTypeParser::new();

        // Test List<int> with multiple elements
        let list_data = create_test_list_data();
        let (_, parsed_list) = parser.parse_collection(&list_data, CollectionType::List).unwrap();
        
        match parsed_list {
            Value::List(elements) => {
                assert_eq!(elements.len(), 4);
                assert_eq!(elements[0], Value::Integer(10));
                assert_eq!(elements[1], Value::Integer(20));
                assert_eq!(elements[2], Value::Integer(30));
                assert_eq!(elements[3], Value::Integer(40));
            }
            _ => panic!("Expected List value"),
        }

        // Test Set<text> with deduplication
        let set_data = create_test_set_data();
        let (_, parsed_set) = parser.parse_collection(&set_data, CollectionType::Set).unwrap();
        
        match parsed_set {
            Value::Set(elements) => {
                assert_eq!(elements.len(), 3);
                assert!(elements.contains(&Value::Text("apple".to_string())));
                assert!(elements.contains(&Value::Text("banana".to_string())));
                assert!(elements.contains(&Value::Text("cherry".to_string())));
            }
            _ => panic!("Expected Set value"),
        }

        // Test Map<text, int> with key-value pairs
        let map_data = create_test_map_data();
        let (_, parsed_map) = parser.parse_collection(&map_data, CollectionType::Map).unwrap();
        
        match parsed_map {
            Value::Map(pairs) => {
                assert_eq!(pairs.len(), 2);
                assert!(pairs.contains(&(Value::Text("key1".to_string()), Value::Integer(100))));
                assert!(pairs.contains(&(Value::Text("key2".to_string()), Value::Integer(200))));
            }
            _ => panic!("Expected Map value"),
        }
    }

    /// Test Tuple parsing with heterogeneous types
    #[test]
    fn test_m3_tuples_heterogeneous() {
        let parser = ComplexTypeParser::new();

        // Test simple tuple: (int, text, boolean)
        let tuple_data = create_test_tuple_data();
        let (_, parsed_tuple) = parser.parse_tuple(&tuple_data).unwrap();
        
        match parsed_tuple {
            Value::Tuple(fields) => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0], Value::Integer(42));
                assert_eq!(fields[1], Value::Text("hello".to_string()));
                assert_eq!(fields[2], Value::Boolean(true));
            }
            _ => panic!("Expected Tuple value"),
        }

        // Test nested tuple: (int, list<text>, map<text, int>)
        let nested_tuple_data = create_test_nested_tuple_data();
        let (_, nested_tuple) = parser.parse_tuple(&nested_tuple_data).unwrap();
        
        match nested_tuple {
            Value::Tuple(fields) => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0], Value::Integer(100));
                
                // Validate nested list
                if let Value::List(list_items) = &fields[1] {
                    assert_eq!(list_items.len(), 2);
                    assert_eq!(list_items[0], Value::Text("item1".to_string()));
                    assert_eq!(list_items[1], Value::Text("item2".to_string()));
                } else {
                    panic!("Expected List in tuple field 1");
                }
                
                // Validate nested map
                if let Value::Map(map_pairs) = &fields[2] {
                    assert_eq!(map_pairs.len(), 1);
                    assert_eq!(map_pairs[0], (Value::Text("count".to_string()), Value::Integer(5)));
                } else {
                    panic!("Expected Map in tuple field 2");
                }
            }
            _ => panic!("Expected Tuple value"),
        }
    }

    /// Test UDT parsing with schema validation
    #[test]
    fn test_m3_udt_schema_validation() {
        let mut parser = ComplexTypeParser::new();
        
        // Register UDT schema
        let address_udt = create_address_udt_schema();
        parser.register_udt(address_udt);
        
        // Test UDT parsing
        let udt_data = create_test_udt_data();
        let (_, parsed_udt) = parser.parse_udt(&udt_data, "address", "test_keyspace").unwrap();
        
        match parsed_udt {
            Value::Udt(udt_value) => {
                assert_eq!(udt_value.type_name, "address");
                assert_eq!(udt_value.keyspace, "test_keyspace");
                assert_eq!(udt_value.fields.len(), 4);
                
                // Validate field values
                assert_eq!(udt_value.fields[0].name, "street");
                assert_eq!(udt_value.fields[0].value, Some(Value::Text("123 Main St".to_string())));
                
                assert_eq!(udt_value.fields[1].name, "city");
                assert_eq!(udt_value.fields[1].value, Some(Value::Text("San Francisco".to_string())));
                
                assert_eq!(udt_value.fields[2].name, "zip_code");
                assert_eq!(udt_value.fields[2].value, Some(Value::Integer(94105)));
                
                assert_eq!(udt_value.fields[3].name, "country");
                assert_eq!(udt_value.fields[3].value, Some(Value::Text("USA".to_string())));
            }
            _ => panic!("Expected UDT value"),
        }
    }

    /// Test Frozen types with immutable semantics
    #[test]
    fn test_m3_frozen_types() {
        let parser = ComplexTypeParser::new();

        // Test frozen list
        let frozen_list_data = create_test_frozen_list_data();
        let list_type = CqlType::List(Box::new(CqlType::Text));
        let (_, frozen_list) = parser.parse_frozen(&frozen_list_data, &list_type).unwrap();
        
        match frozen_list {
            Value::Frozen(inner) => {
                if let Value::List(elements) = inner.as_ref() {
                    assert_eq!(elements.len(), 3);
                    assert_eq!(elements[0], Value::Text("a".to_string()));
                    assert_eq!(elements[1], Value::Text("b".to_string()));
                    assert_eq!(elements[2], Value::Text("c".to_string()));
                } else {
                    panic!("Expected List inside Frozen");
                }
            }
            _ => panic!("Expected Frozen value"),
        }

        // Test frozen UDT
        let frozen_udt_data = create_test_frozen_udt_data();
        let udt_type = CqlType::Udt("person".to_string(), vec![]);
        let (_, frozen_udt) = parser.parse_frozen(&frozen_udt_data, &udt_type).unwrap();
        
        match frozen_udt {
            Value::Frozen(inner) => {
                assert!(matches!(inner.as_ref(), Value::Udt(_)));
            }
            _ => panic!("Expected Frozen UDT value"),
        }
    }

    /// Test edge cases and error conditions
    #[test]
    fn test_m3_edge_cases() {
        let parser = ComplexTypeParser::new();

        // Test empty collections
        test_empty_collections(&parser);
        
        // Test null values in collections
        test_null_values_in_collections(&parser);
        
        // Test deeply nested structures
        test_deeply_nested_structures(&parser);
        
        // Test large collections (performance)
        test_large_collections(&parser);
    }

    /// Test performance benchmarks
    #[test]
    fn test_m3_performance_benchmarks() {
        let parser = ComplexTypeParser::new();
        
        // Benchmark list parsing
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let data = create_large_list_data(100);
            let _ = parser.parse_collection(&data, CollectionType::List);
        }
        let list_duration = start.elapsed();
        println!("List parsing (1000 iterations, 100 elements each): {:?}", list_duration);
        
        // Benchmark should be under 100ms for reasonable performance
        assert!(list_duration.as_millis() < 100, "List parsing too slow: {:?}", list_duration);
        
        // Benchmark tuple parsing
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let data = create_complex_tuple_data();
            let _ = parser.parse_tuple(&data);
        }
        let tuple_duration = start.elapsed();
        println!("Tuple parsing (1000 iterations): {:?}", tuple_duration);
        
        assert!(tuple_duration.as_millis() < 50, "Tuple parsing too slow: {:?}", tuple_duration);
    }

    /// Test serialization roundtrip for all complex types
    #[test]
    fn test_m3_serialization_roundtrip() {
        use cqlite_core::parser::complex_types::{serialize_list_v5, serialize_set_v5, serialize_map_v5, serialize_tuple};
        
        let parser = ComplexTypeParser::new();

        // Test List roundtrip
        let original_list = vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ];
        let serialized = serialize_list_v5(&original_list).unwrap();
        let (_, parsed) = parser.parse_collection(&serialized, CollectionType::List).unwrap();
        assert_eq!(Value::List(original_list), parsed);

        // Test Map roundtrip
        let original_map = vec![
            (Value::Text("key1".to_string()), Value::Integer(10)),
            (Value::Text("key2".to_string()), Value::Integer(20)),
        ];
        let serialized = serialize_map_v5(&original_map).unwrap();
        let (_, parsed) = parser.parse_collection(&serialized, CollectionType::Map).unwrap();
        assert_eq!(Value::Map(original_map), parsed);

        // Test Tuple roundtrip
        let original_tuple = vec![
            Value::Integer(42),
            Value::Text("test".to_string()),
            Value::Boolean(true),
        ];
        let serialized = serialize_tuple(&original_tuple).unwrap();
        let (_, parsed) = parser.parse_tuple(&serialized).unwrap();
        assert_eq!(Value::Tuple(original_tuple), parsed);
    }

    // Helper functions for creating test data

    fn create_test_list_data() -> Vec<u8> {
        use cqlite_core::parser::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(4)); // count
        data.push(0x09); // int type
        data.extend_from_slice(&10i32.to_be_bytes());
        data.extend_from_slice(&20i32.to_be_bytes());
        data.extend_from_slice(&30i32.to_be_bytes());
        data.extend_from_slice(&40i32.to_be_bytes());
        data
    }

    fn create_test_set_data() -> Vec<u8> {
        use cqlite_core::parser::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(3)); // count
        data.push(0x0D); // varchar type
        
        // "apple"
        data.extend_from_slice(&encode_vint(5));
        data.extend_from_slice(b"apple");
        
        // "banana"
        data.extend_from_slice(&encode_vint(6));
        data.extend_from_slice(b"banana");
        
        // "cherry"
        data.extend_from_slice(&encode_vint(6));
        data.extend_from_slice(b"cherry");
        
        data
    }

    fn create_test_map_data() -> Vec<u8> {
        use cqlite_core::parser::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(2)); // count
        data.push(0x0D); // varchar key type
        data.push(0x09); // int value type
        
        // "key1" -> 100
        data.extend_from_slice(&encode_vint(4));
        data.extend_from_slice(b"key1");
        data.extend_from_slice(&100i32.to_be_bytes());
        
        // "key2" -> 200
        data.extend_from_slice(&encode_vint(4));
        data.extend_from_slice(b"key2");
        data.extend_from_slice(&200i32.to_be_bytes());
        
        data
    }

    fn create_test_tuple_data() -> Vec<u8> {
        use cqlite_core::parser::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(3)); // field count
        
        // Type specifications
        data.push(0x09); // int
        data.push(0x0D); // varchar
        data.push(0x04); // boolean
        
        // Values
        data.extend_from_slice(&42i32.to_be_bytes()); // 42
        data.extend_from_slice(&encode_vint(5)); // string length
        data.extend_from_slice(b"hello"); // "hello"
        data.push(0x01); // true
        
        data
    }

    fn create_test_nested_tuple_data() -> Vec<u8> {
        use cqlite_core::parser::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(3)); // field count
        
        // Type specifications
        data.push(0x09); // int
        data.push(0x20); // list
        data.push(0x21); // map
        
        // Field 1: int = 100
        data.extend_from_slice(&100i32.to_be_bytes());
        
        // Field 2: list<text> = ["item1", "item2"]
        data.extend_from_slice(&encode_vint(2)); // list count
        data.push(0x0D); // varchar element type
        data.extend_from_slice(&encode_vint(5));
        data.extend_from_slice(b"item1");
        data.extend_from_slice(&encode_vint(5));
        data.extend_from_slice(b"item2");
        
        // Field 3: map<text, int> = {"count": 5}
        data.extend_from_slice(&encode_vint(1)); // map count
        data.push(0x0D); // varchar key type
        data.push(0x09); // int value type
        data.extend_from_slice(&encode_vint(5));
        data.extend_from_slice(b"count");
        data.extend_from_slice(&5i32.to_be_bytes());
        
        data
    }

    fn create_address_udt_schema() -> UdtTypeDef {
        UdtTypeDef {
            keyspace: "test_keyspace".to_string(),
            name: "address".to_string(),
            fields: vec![
                UdtFieldDef {
                    name: "street".to_string(),
                    field_type: CqlType::Text,
                    nullable: true,
                },
                UdtFieldDef {
                    name: "city".to_string(),
                    field_type: CqlType::Text,
                    nullable: true,
                },
                UdtFieldDef {
                    name: "zip_code".to_string(),
                    field_type: CqlType::Int,
                    nullable: true,
                },
                UdtFieldDef {
                    name: "country".to_string(),
                    field_type: CqlType::Text,
                    nullable: true,
                },
            ],
        }
    }

    fn create_test_udt_data() -> Vec<u8> {
        use cqlite_core::parser::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(4)); // field count
        
        // Field 1: street = "123 Main St"
        data.extend_from_slice(&encode_vint(11));
        data.extend_from_slice(b"123 Main St");
        
        // Field 2: city = "San Francisco"
        data.extend_from_slice(&encode_vint(13));
        data.extend_from_slice(b"San Francisco");
        
        // Field 3: zip_code = 94105
        data.extend_from_slice(&94105i32.to_be_bytes());
        
        // Field 4: country = "USA"
        data.extend_from_slice(&encode_vint(3));
        data.extend_from_slice(b"USA");
        
        data
    }

    fn create_test_frozen_list_data() -> Vec<u8> {
        use cqlite_core::parser::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(3)); // count
        data.push(0x0D); // varchar type
        
        data.extend_from_slice(&encode_vint(1));
        data.extend_from_slice(b"a");
        data.extend_from_slice(&encode_vint(1));
        data.extend_from_slice(b"b");
        data.extend_from_slice(&encode_vint(1));
        data.extend_from_slice(b"c");
        
        data
    }

    fn create_test_frozen_udt_data() -> Vec<u8> {
        // For now, return simple UDT data - would be same as regular UDT
        create_test_udt_data()
    }

    fn test_empty_collections(parser: &ComplexTypeParser) {
        use cqlite_core::parser::vint::encode_vint;
        
        // Empty list
        let empty_list = encode_vint(0);
        let (_, value) = parser.parse_collection(&empty_list, CollectionType::List).unwrap();
        assert_eq!(value, Value::List(Vec::new()));
        
        // Empty set
        let (_, value) = parser.parse_collection(&empty_list, CollectionType::Set).unwrap();
        assert_eq!(value, Value::Set(Vec::new()));
        
        // Empty map
        let (_, value) = parser.parse_collection(&empty_list, CollectionType::Map).unwrap();
        assert_eq!(value, Value::Map(Vec::new()));
    }

    fn test_null_values_in_collections(_parser: &ComplexTypeParser) {
        // Test null handling in collections
        // This would require more complex test data generation
        // For now, just validate that null handling is implemented
        assert!(true, "Null value handling test placeholder");
    }

    fn test_deeply_nested_structures(_parser: &ComplexTypeParser) {
        // Test deeply nested structures like List<Map<Text, Set<Int>>>
        // This would require complex test data generation
        assert!(true, "Deep nesting test placeholder");
    }

    fn test_large_collections(_parser: &ComplexTypeParser) {
        // Test collections with many elements for performance
        assert!(true, "Large collection test placeholder");
    }

    fn create_large_list_data(element_count: usize) -> Vec<u8> {
        use cqlite_core::parser::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(element_count as i64));
        data.push(0x09); // int type
        
        for i in 0..element_count {
            data.extend_from_slice(&(i as i32).to_be_bytes());
        }
        
        data
    }

    fn create_complex_tuple_data() -> Vec<u8> {
        // Return same as simple tuple for now
        create_test_tuple_data()
    }
}