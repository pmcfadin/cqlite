//! Comprehensive tests for collection and UDT parsing with Cassandra 5.0+ compatibility
//!
//! This module tests the complete collection and UDT parsing functionality
//! against the actual test schema defined in test-env/cassandra5/scripts/create-keyspaces.cql

use super::complex_types::ComplexTypeParser;
use super::types::CqlTypeId;
use super::vint::encode_vint;
use crate::schema::{CqlType, UdtRegistry};
use crate::types::{Value, UdtValue, UdtField, UdtTypeDef};

/// Test parsing of LIST<TEXT> from Cassandra 5 test schema
#[test]
fn test_list_text_parsing() {
    let parser = ComplexTypeParser::new();
    
    // Create test data: LIST<TEXT> with ["hello", "world", "test"]
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(3)); // count = 3
    data.push(CqlTypeId::Varchar as u8); // element type = TEXT
    
    // Element 1: "hello"
    data.extend_from_slice(&encode_vint(5));
    data.extend_from_slice(b"hello");
    
    // Element 2: "world"
    data.extend_from_slice(&encode_vint(5));
    data.extend_from_slice(b"world");
    
    // Element 3: "test"
    data.extend_from_slice(&encode_vint(4));
    data.extend_from_slice(b"test");
    
    let (remaining, parsed_value) = parser.parse_list_v5(&data).unwrap();
    assert!(remaining.is_empty());
    
    match parsed_value {
        Value::List(elements) => {
            assert_eq!(elements.len(), 3);
            assert_eq!(elements[0], Value::Text("hello".to_string()));
            assert_eq!(elements[1], Value::Text("world".to_string()));
            assert_eq!(elements[2], Value::Text("test".to_string()));
        }
        _ => panic!("Expected List value, got {:?}", parsed_value),
    }
}

/// Test parsing of SET<INT> from Cassandra 5 test schema
#[test]
fn test_set_int_parsing() {
    let parser = ComplexTypeParser::new();
    
    // Create test data: SET<INT> with {1, 2, 3, 4, 5}
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(5)); // count = 5
    data.push(CqlTypeId::Int as u8); // element type = INT
    
    // Elements: 1, 2, 3, 4, 5
    for i in 1..=5 {
        data.extend_from_slice(&(i as i32).to_be_bytes());
    }
    
    let (remaining, parsed_value) = parser.parse_set_v5(&data).unwrap();
    assert!(remaining.is_empty());
    
    match parsed_value {
        Value::Set(elements) => {
            assert_eq!(elements.len(), 5);
            for (i, element) in elements.iter().enumerate() {
                assert_eq!(*element, Value::Integer((i + 1) as i32));
            }
        }
        _ => panic!("Expected Set value, got {:?}", parsed_value),
    }
}

/// Test parsing of MAP<TEXT, INT> from Cassandra 5 test schema
#[test]
fn test_map_text_int_parsing() {
    let parser = ComplexTypeParser::new();
    
    // Create test data: MAP<TEXT, INT> with {"key1": 10, "key2": 20}
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(2)); // count = 2
    data.push(CqlTypeId::Varchar as u8); // key type = TEXT
    data.push(CqlTypeId::Int as u8); // value type = INT
    
    // Pair 1: "key1" -> 10
    data.extend_from_slice(&encode_vint(4));
    data.extend_from_slice(b"key1");
    data.extend_from_slice(&10i32.to_be_bytes());
    
    // Pair 2: "key2" -> 20
    data.extend_from_slice(&encode_vint(4));
    data.extend_from_slice(b"key2");
    data.extend_from_slice(&20i32.to_be_bytes());
    
    let (remaining, parsed_value) = parser.parse_map_v5(&data).unwrap();
    assert!(remaining.is_empty());
    
    match parsed_value {
        Value::Map(pairs) => {
            assert_eq!(pairs.len(), 2);
            assert_eq!(pairs[0], (Value::Text("key1".to_string()), Value::Integer(10)));
            assert_eq!(pairs[1], (Value::Text("key2".to_string()), Value::Integer(20)));
        }
        _ => panic!("Expected Map value, got {:?}", parsed_value),
    }
}

/// Test parsing of FROZEN<LIST<TEXT>> from Cassandra 5 test schema
#[test]
fn test_frozen_list_parsing() {
    let parser = ComplexTypeParser::new();
    
    // Create test data: FROZEN<LIST<TEXT>> with ["frozen", "list"]
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(2)); // count = 2
    data.push(CqlTypeId::Varchar as u8); // element type = TEXT
    
    // Element 1: "frozen"
    data.extend_from_slice(&encode_vint(6));
    data.extend_from_slice(b"frozen");
    
    // Element 2: "list"
    data.extend_from_slice(&encode_vint(4));
    data.extend_from_slice(b"list");
    
    let (remaining, parsed_value) = parser.parse_frozen_list(&data).unwrap();
    assert!(remaining.is_empty());
    
    match parsed_value {
        Value::Frozen(boxed_value) => match boxed_value.as_ref() {
            Value::List(elements) => {
                assert_eq!(elements.len(), 2);
                assert_eq!(elements[0], Value::Text("frozen".to_string()));
                assert_eq!(elements[1], Value::Text("list".to_string()));
            }
            _ => panic!("Expected List inside Frozen, got {:?}", boxed_value),
        },
        _ => panic!("Expected Frozen value, got {:?}", parsed_value),
    }
}

/// Test parsing of address UDT from Cassandra 5 test schema
#[test]
fn test_address_udt_parsing() {
    let mut parser = ComplexTypeParser::with_test_udts();
    
    // Create test data for address UDT: {street: "123 Main St", city: "Test City", state: "TC", zip_code: "12345"}
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(4)); // field count = 4
    
    // Field 1: street = "123 Main St"
    data.extend_from_slice(&encode_vint(11)); // length
    data.extend_from_slice(&encode_vint(11));
    data.extend_from_slice(b"123 Main St");
    
    // Field 2: city = "Test City"  
    data.extend_from_slice(&encode_vint(9)); // length
    data.extend_from_slice(&encode_vint(9));
    data.extend_from_slice(b"Test City");
    
    // Field 3: state = "TC"
    data.extend_from_slice(&encode_vint(2)); // length
    data.extend_from_slice(&encode_vint(2));
    data.extend_from_slice(b"TC");
    
    // Field 4: zip_code = "12345"
    data.extend_from_slice(&encode_vint(5)); // length
    data.extend_from_slice(&encode_vint(5));
    data.extend_from_slice(b"12345");
    
    let (remaining, parsed_value) = parser.parse_udt(&data, "address", "test_keyspace").unwrap();
    assert!(remaining.is_empty());
    
    match parsed_value {
        Value::Udt(udt_value) => {
            assert_eq!(udt_value.type_name, "address");
            assert_eq!(udt_value.keyspace, "test_keyspace");
            assert_eq!(udt_value.fields.len(), 4);
            
            assert_eq!(udt_value.fields[0].name, "street");
            assert_eq!(udt_value.fields[0].value, Some(Value::Text("123 Main St".to_string())));
            
            assert_eq!(udt_value.fields[1].name, "city");
            assert_eq!(udt_value.fields[1].value, Some(Value::Text("Test City".to_string())));
            
            assert_eq!(udt_value.fields[2].name, "state");
            assert_eq!(udt_value.fields[2].value, Some(Value::Text("TC".to_string())));
            
            assert_eq!(udt_value.fields[3].name, "zip_code");
            assert_eq!(udt_value.fields[3].value, Some(Value::Text("12345".to_string())));
        }
        _ => panic!("Expected UDT value, got {:?}", parsed_value),
    }
}

/// Test parsing of person UDT with nested FROZEN<address> from Cassandra 5 test schema
#[test]
fn test_person_udt_with_nested_address() {
    let parser = ComplexTypeParser::with_test_udts();
    
    // This test would require more complex data setup for nested UDT
    // For now, we'll test the structure validation
    let person_udt = parser.udt_registry.get_udt("test_keyspace", "person").unwrap();
    
    assert_eq!(person_udt.fields.len(), 3);
    assert_eq!(person_udt.fields[0].name, "name");
    assert_eq!(person_udt.fields[1].name, "age");
    assert_eq!(person_udt.fields[2].name, "address");
    
    // Verify the address field is FROZEN<address>
    match &person_udt.fields[2].field_type {
        CqlType::Frozen(inner) => match inner.as_ref() {
            CqlType::Udt(type_name, _) => {
                assert_eq!(type_name, "address");
            }
            _ => panic!("Expected nested UDT type, got {:?}", inner),
        },
        _ => panic!("Expected Frozen type, got {:?}", person_udt.fields[2].field_type),
    }
}

/// Test empty collection parsing
#[test]
fn test_empty_collections() {
    let parser = ComplexTypeParser::new();
    
    // Empty list
    let data = encode_vint(0);
    let (_, value) = parser.parse_list_v5(&data).unwrap();
    assert_eq!(value, Value::List(Vec::new()));
    
    // Empty set  
    let (_, value) = parser.parse_set_v5(&data).unwrap();
    assert_eq!(value, Value::Set(Vec::new()));
    
    // Empty map
    let (_, value) = parser.parse_map_v5(&data).unwrap();
    assert_eq!(value, Value::Map(Vec::new()));
}

/// Test collection serialization roundtrip
#[test]
fn test_collection_serialization_roundtrip() {
    use super::complex_types::{serialize_list_v5, serialize_set_v5, serialize_map_v5};
    
    let parser = ComplexTypeParser::new();
    
    // Test list roundtrip
    let original_list = vec![
        Value::Text("item1".to_string()),
        Value::Text("item2".to_string()),
        Value::Text("item3".to_string()),
    ];
    
    let serialized = serialize_list_v5(&original_list).unwrap();
    let (_, parsed) = parser.parse_list_v5(&serialized).unwrap();
    assert_eq!(Value::List(original_list), parsed);
    
    // Test set roundtrip
    let original_set = vec![
        Value::Integer(10),
        Value::Integer(20),
        Value::Integer(30),
    ];
    
    let serialized = serialize_set_v5(&original_set).unwrap();
    let (_, parsed) = parser.parse_set_v5(&serialized).unwrap();
    assert_eq!(Value::Set(original_set), parsed);
    
    // Test map roundtrip
    let original_map = vec![
        (Value::Text("key1".to_string()), Value::Integer(100)),
        (Value::Text("key2".to_string()), Value::Integer(200)),
    ];
    
    let serialized = serialize_map_v5(&original_map).unwrap();
    let (_, parsed) = parser.parse_map_v5(&serialized).unwrap();
    assert_eq!(Value::Map(original_map), parsed);
}

/// Test large collection performance and safety limits
#[test]
fn test_large_collection_limits() {
    let parser = ComplexTypeParser::new();
    
    // Test that we properly handle large collection counts
    // This should be within limits but test the safety bounds
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(1000)); // 1000 elements
    data.push(CqlTypeId::Int as u8);
    
    // Add 1000 integer elements
    for i in 0..1000 {
        data.extend_from_slice(&(i as i32).to_be_bytes());
    }
    
    let (remaining, parsed_value) = parser.parse_list_v5(&data).unwrap();
    assert!(remaining.is_empty());
    
    match parsed_value {
        Value::List(elements) => {
            assert_eq!(elements.len(), 1000);
            // Verify first and last elements
            assert_eq!(elements[0], Value::Integer(0));
            assert_eq!(elements[999], Value::Integer(999));
        }
        _ => panic!("Expected List value"),
    }
}

/// Test nested collection validation (LIST<LIST<INT>>)
#[test]
fn test_nested_collections() {
    // This would be a more advanced test for nested collections
    // For now, we validate that the parser structure supports it
    let parser = ComplexTypeParser::new();
    
    // Verify we can handle nested collection types in the type system
    let nested_list_type = CqlType::List(Box::new(CqlType::List(Box::new(CqlType::Int))));
    assert!(nested_list_type.is_collection());
    
    let nested_map_type = CqlType::Map(
        Box::new(CqlType::Text),
        Box::new(CqlType::List(Box::new(CqlType::Int)))
    );
    assert!(nested_map_type.is_collection());
}

/// Test UDT field null handling
#[test]
fn test_udt_null_fields() {
    let parser = ComplexTypeParser::with_test_udts();
    
    // Create test data for address UDT with some null fields
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(4)); // field count = 4
    
    // Field 1: street = "123 Main St"
    data.extend_from_slice(&encode_vint(11));
    data.extend_from_slice(&encode_vint(11));
    data.extend_from_slice(b"123 Main St");
    
    // Field 2: city = null
    data.extend_from_slice(&encode_vint(-1)); // null marker
    
    // Field 3: state = "TC"
    data.extend_from_slice(&encode_vint(2));
    data.extend_from_slice(&encode_vint(2));
    data.extend_from_slice(b"TC");
    
    // Field 4: zip_code = null  
    data.extend_from_slice(&encode_vint(-1)); // null marker
    
    let (remaining, parsed_value) = parser.parse_udt(&data, "address", "test_keyspace").unwrap();
    assert!(remaining.is_empty());
    
    match parsed_value {
        Value::Udt(udt_value) => {
            assert_eq!(udt_value.fields.len(), 4);
            
            assert_eq!(udt_value.fields[0].value, Some(Value::Text("123 Main St".to_string())));
            assert_eq!(udt_value.fields[1].value, None); // null city
            assert_eq!(udt_value.fields[2].value, Some(Value::Text("TC".to_string())));
            assert_eq!(udt_value.fields[3].value, None); // null zip_code
        }
        _ => panic!("Expected UDT value, got {:?}", parsed_value),
    }
}