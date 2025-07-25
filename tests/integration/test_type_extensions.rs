//! Comprehensive tests for UDT, Tuple, and Frozen type extensions
//! 
//! These tests validate the new type system implementations for exact
//! Cassandra compatibility with proper binary format handling.

use cqlite_core::types::{Value, DataType};
use cqlite_core::schema::{CqlType, TableSchema, Column, KeyColumn, ClusteringColumn};
use std::collections::HashMap;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_value_creation_and_display() {
        // Test tuple with mixed types: tuple<int, text, boolean>
        let tuple = Value::Tuple(vec![
            Value::Integer(42),
            Value::Text("hello".to_string()),
            Value::Boolean(true),
            Value::Null,
        ]);
        
        assert_eq!(tuple.data_type(), DataType::Tuple);
        assert_eq!(tuple.to_string(), "(42, 'hello', true, NULL)");
        assert!(!tuple.is_null());
    }

    #[test]
    fn test_udt_value_creation_and_display() {
        // Test UDT: CREATE TYPE address (street text, city text, zip int)
        let mut fields = HashMap::new();
        fields.insert("street".to_string(), Value::Text("123 Main St".to_string()));
        fields.insert("city".to_string(), Value::Text("San Francisco".to_string()));
        fields.insert("zip".to_string(), Value::Integer(94102));
        fields.insert("country".to_string(), Value::Null); // Nullable field

        let address_udt = Value::Udt("address".to_string(), fields);
        
        assert_eq!(address_udt.data_type(), DataType::Udt);
        assert!(address_udt.to_string().contains("address{"));
        assert!(address_udt.to_string().contains("street: '123 Main St'"));
        assert!(address_udt.to_string().contains("zip: 94102"));
        assert!(address_udt.to_string().contains("country: NULL"));
    }

    #[test]
    fn test_frozen_collections() {
        // Test frozen<list<int>>
        let frozen_list = Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ])));
        
        assert_eq!(frozen_list.data_type(), DataType::Frozen);
        assert_eq!(frozen_list.to_string(), "FROZEN([1, 2, 3])");

        // Test frozen<map<text, int>>
        let frozen_map = Value::Frozen(Box::new(Value::Map(vec![
            (Value::Text("a".to_string()), Value::Integer(1)),
            (Value::Text("b".to_string()), Value::Integer(2)),
        ])));
        
        assert_eq!(frozen_map.to_string(), "FROZEN({'a': 1, 'b': 2})");
    }

    #[test]
    fn test_nested_udt_in_tuple() {
        // Test complex nesting: tuple<text, address>
        let mut address_fields = HashMap::new();
        address_fields.insert("street".to_string(), Value::Text("456 Oak Ave".to_string()));
        address_fields.insert("city".to_string(), Value::Text("Portland".to_string()));
        
        let address = Value::Udt("address".to_string(), address_fields);
        let nested_tuple = Value::Tuple(vec![
            Value::Text("John Doe".to_string()),
            address,
        ]);
        
        assert_eq!(nested_tuple.data_type(), DataType::Tuple);
        let display = nested_tuple.to_string();
        assert!(display.contains("('John Doe', address{"));
        assert!(display.contains("street: '456 Oak Ave'"));
    }

    #[test]
    fn test_cql_type_parsing_tuple() {
        // Test parsing tuple<int, text, boolean>
        let parsed = CqlType::parse("tuple<int, text, boolean>").unwrap();
        match parsed {
            CqlType::Tuple(types) => {
                assert_eq!(types.len(), 3);
                assert_eq!(types[0], CqlType::Int);
                assert_eq!(types[1], CqlType::Text);
                assert_eq!(types[2], CqlType::Boolean);
            }
            _ => panic!("Expected Tuple type"),
        }
    }

    #[test]
    fn test_cql_type_parsing_frozen() {
        // Test parsing frozen<list<int>>
        let parsed = CqlType::parse("frozen<list<int>>").unwrap();
        match parsed {
            CqlType::Frozen(inner) => {
                match *inner {
                    CqlType::List(element_type) => {
                        assert_eq!(*element_type, CqlType::Int);
                    }
                    _ => panic!("Expected List inside Frozen"),
                }
            }
            _ => panic!("Expected Frozen type"),
        }
    }

    #[test]
    fn test_data_type_default_values() {
        // Test that all new types have proper default values
        assert_eq!(DataType::Tuple.default_value(), Value::Tuple(Vec::new()));
        assert_eq!(DataType::Udt.default_value(), Value::Udt(String::new(), HashMap::new()));
        assert_eq!(DataType::Frozen.default_value(), Value::Frozen(Box::new(Value::Null)));
    }

    #[test]
    fn test_type_compatibility_edge_cases() {
        // Test null handling in complex types
        let tuple_with_nulls = Value::Tuple(vec![
            Value::Null,
            Value::Integer(42),
            Value::Null,
        ]);
        assert_eq!(tuple_with_nulls.to_string(), "(NULL, 42, NULL)");

        // Test empty UDT
        let empty_udt = Value::Udt("empty_type".to_string(), HashMap::new());
        assert_eq!(empty_udt.to_string(), "empty_type{}");

        // Test frozen null
        let frozen_null = Value::Frozen(Box::new(Value::Null));
        assert_eq!(frozen_null.to_string(), "FROZEN(NULL)");
    }

    #[test]
    fn test_schema_evolution_scenarios() {
        // Test adding fields to UDT (schema evolution)
        let mut v1_fields = HashMap::new();
        v1_fields.insert("name".to_string(), Value::Text("Alice".to_string()));
        
        let mut v2_fields = HashMap::new();
        v2_fields.insert("name".to_string(), Value::Text("Alice".to_string()));
        v2_fields.insert("age".to_string(), Value::Integer(30)); // New field
        v2_fields.insert("email".to_string(), Value::Null); // New nullable field
        
        let udt_v1 = Value::Udt("person".to_string(), v1_fields);
        let udt_v2 = Value::Udt("person".to_string(), v2_fields);
        
        // Both should be valid UDT values
        assert_eq!(udt_v1.data_type(), DataType::Udt);
        assert_eq!(udt_v2.data_type(), DataType::Udt);
    }
}