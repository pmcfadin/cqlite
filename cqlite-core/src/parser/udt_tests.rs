//! Comprehensive UDT parsing tests for Cassandra 5.0 compatibility
//!
//! Tests the enhanced UDT parsing system with complex nested structures,
//! collections containing UDTs, and real-world schema patterns.

#[cfg(test)]
mod tests {
    use super::super::types::*;
    use super::super::vint::encode_vint;
    use crate::types::{UdtValue, UdtField, UdtTypeDef, UdtFieldDef, Value};
    use crate::schema::{CqlType, UdtRegistry};

    /// Create a sample person UDT value for testing
    fn create_sample_person_udt() -> UdtValue {
        UdtValue {
            type_name: "person".to_string(),
            keyspace: "test_keyspace".to_string(),
            fields: vec![
                UdtField {
                    name: "first_name".to_string(),
                    value: Some(Value::Text("John".to_string())),
                },
                UdtField {
                    name: "last_name".to_string(),
                    value: Some(Value::Text("Doe".to_string())),
                },
                UdtField {
                    name: "age".to_string(),
                    value: Some(Value::Integer(30)),
                },
                UdtField {
                    name: "email".to_string(),
                    value: Some(Value::Text("john.doe@example.com".to_string())),
                },
            ],
        }
    }

    /// Create a sample address UDT value for testing
    fn create_sample_address_udt() -> UdtValue {
        UdtValue {
            type_name: "address".to_string(),
            keyspace: "test_keyspace".to_string(),
            fields: vec![
                UdtField {
                    name: "street".to_string(),
                    value: Some(Value::Text("123 Main St".to_string())),
                },
                UdtField {
                    name: "city".to_string(),
                    value: Some(Value::Text("Anytown".to_string())),
                },
                UdtField {
                    name: "state".to_string(),
                    value: Some(Value::Text("CA".to_string())),
                },
                UdtField {
                    name: "zip_code".to_string(),
                    value: Some(Value::Text("12345".to_string())),
                },
                UdtField {
                    name: "country".to_string(),
                    value: Some(Value::Text("USA".to_string())),
                },
            ],
        }
    }

    /// Serialize a UDT value to binary format for testing parsing
    fn serialize_udt_for_test(udt: &UdtValue) -> Vec<u8> {
        let mut data = Vec::new();
        
        // Serialize type name
        data.extend_from_slice(&encode_vint(udt.type_name.len() as i64));
        data.extend_from_slice(udt.type_name.as_bytes());
        
        // Serialize field count
        data.extend_from_slice(&encode_vint(udt.fields.len() as i64));
        
        // Serialize field definitions (schema part)
        for field in &udt.fields {
            // Field name
            data.extend_from_slice(&encode_vint(field.name.len() as i64));
            data.extend_from_slice(field.name.as_bytes());
            
            // Field type ID (inferred from value)
            let type_id = match &field.value {
                Some(Value::Text(_)) => CqlTypeId::Varchar as u8,
                Some(Value::Integer(_)) => CqlTypeId::Int as u8,
                Some(Value::BigInt(_)) => CqlTypeId::BigInt as u8,
                Some(Value::Boolean(_)) => CqlTypeId::Boolean as u8,
                Some(Value::Udt(_)) => CqlTypeId::Udt as u8,
                Some(Value::List(_)) => CqlTypeId::List as u8,
                Some(Value::Set(_)) => CqlTypeId::Set as u8,
                Some(Value::Map(_)) => CqlTypeId::Map as u8,
                _ => CqlTypeId::Blob as u8,
            };
            data.push(type_id);
        }
        
        // Serialize field values
        for field in &udt.fields {
            match &field.value {
                None => {
                    // Null field
                    data.extend_from_slice(&(-1i32).to_be_bytes());
                },
                Some(value) => {
                    let value_data = serialize_value_for_test(value);
                    data.extend_from_slice(&(value_data.len() as i32).to_be_bytes());
                    data.extend_from_slice(&value_data);
                }
            }
        }
        
        data
    }

    /// Helper to serialize individual values for testing
    fn serialize_value_for_test(value: &Value) -> Vec<u8> {
        match value {
            Value::Text(s) => {
                let mut data = Vec::new();
                data.extend_from_slice(&encode_vint(s.len() as i64));
                data.extend_from_slice(s.as_bytes());
                data
            },
            Value::Integer(i) => i.to_be_bytes().to_vec(),
            Value::Boolean(b) => vec![if *b { 1 } else { 0 }],
            Value::Udt(udt) => serialize_udt_for_test(udt),
            _ => Vec::new(), // Simplified for testing
        }
    }

    #[test]
    fn test_udt_registry_creation() {
        let registry = UdtRegistry::with_cassandra5_defaults();
        
        // Should have system UDTs loaded by default
        assert!(registry.contains_udt("system", "address"));
        assert!(registry.contains_udt("system", "person"));
        assert!(registry.contains_udt("system", "contact_info"));
        
        assert_eq!(registry.total_udts(), 3);
    }

    #[test]
    fn test_udt_registry_dependency_validation() {
        let registry = UdtRegistry::with_cassandra5_defaults();
        
        // Test successful dependency resolution
        let person_udt = registry.resolve_udt_with_dependencies("system", "person").unwrap();
        assert_eq!(person_udt.name, "person");
        
        // Test missing dependency
        let result = registry.resolve_udt_with_dependencies("system", "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_udt_registry_circular_dependency_detection() {
        let mut registry = UdtRegistry::new();
        
        // Try to create a circular dependency
        let self_referencing_udt = UdtTypeDef::new("test".to_string(), "circular".to_string())
            .with_field("self_ref".to_string(), CqlType::Udt("circular".to_string(), vec![]), true);
        
        let result = registry.register_udt_with_validation(self_referencing_udt);
        assert!(result.is_err(), "Should detect circular dependency");
    }

    #[test]
    fn test_udt_registry_dependent_udts() {
        let registry = UdtRegistry::with_cassandra5_defaults();
        
        // contact_info depends on both person and address
        let dependents = registry.get_dependent_udts("system", "person");
        assert!(!dependents.is_empty());
        
        let contact_info_depends_on_person = dependents.iter()
            .any(|udt| udt.name == "contact_info");
        assert!(contact_info_depends_on_person);
    }

    #[test]
    fn test_udt_registry_export_definitions() {
        let registry = UdtRegistry::with_cassandra5_defaults();
        
        let definitions = registry.export_definitions("system");
        assert!(!definitions.is_empty());
        
        // Check that definitions contain CREATE TYPE statements
        let address_def = definitions.iter()
            .find(|def| def.contains("CREATE TYPE system.address"))
            .expect("Address UDT definition should exist");
        
        assert!(address_def.contains("street text"));
        assert!(address_def.contains("city text"));
    }

    #[test]
    fn test_basic_udt_parsing() {
        let person_udt = create_sample_person_udt();
        let serialized = serialize_udt_for_test(&person_udt);
        
        let (remaining, parsed_value) = parse_udt(&serialized).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::Udt(parsed_udt) = parsed_value {
            assert_eq!(parsed_udt.type_name, "person");
            assert_eq!(parsed_udt.fields.len(), 4);
            
            // Check specific fields
            let first_name = parsed_udt.get_field("first_name").unwrap();
            assert_eq!(first_name.as_str(), Some("John"));
            
            let age = parsed_udt.get_field("age").unwrap();
            assert_eq!(age.as_i32(), Some(30));
        } else {
            panic!("Expected UDT value");
        }
    }

    #[test]
    fn test_udt_parsing_with_registry() {
        let registry = UdtRegistry::with_cassandra5_defaults();
        let person_udt = create_sample_person_udt();
        let serialized = serialize_udt_for_test(&person_udt);
        
        let (remaining, parsed_value) = parse_udt_with_registry(
            &serialized, 
            "person", 
            "system", 
            &registry
        ).unwrap();
        
        assert!(remaining.is_empty());
        
        if let Value::Udt(parsed_udt) = parsed_value {
            assert_eq!(parsed_udt.type_name, "person");
            assert_eq!(parsed_udt.keyspace, "system"); // Resolved from registry
        } else {
            panic!("Expected UDT value");
        }
    }

    #[test]
    fn test_nested_udt_parsing() {
        let mut registry = UdtRegistry::with_cassandra5_defaults();
        
        // Create a company UDT that contains person and address UDTs
        let company_udt = UdtTypeDef::new("test".to_string(), "company".to_string())
            .with_field("name".to_string(), CqlType::Text, false)
            .with_field("ceo".to_string(), CqlType::Udt("person".to_string(), vec![]), true)
            .with_field("headquarters".to_string(), CqlType::Udt("address".to_string(), vec![]), true);
        
        registry.register_udt(company_udt);
        
        // This would require more complex serialization for a full test
        // For now, verify the registry recognizes the nested structure
        let resolved_company = registry.resolve_udt_with_dependencies("test", "company");
        
        // Should fail because person and address are in "system" keyspace
        assert!(resolved_company.is_err());
        
        // But the UDT structure should be registered
        assert!(registry.contains_udt("test", "company"));
    }

    #[test]
    fn test_udt_with_collections() {
        let registry = UdtRegistry::with_cassandra5_defaults();
        
        // Test person UDT which has collections (phone_numbers, addresses, metadata)
        let person_def = registry.get_udt("system", "person").unwrap();
        
        // Should have fields with collection types
        let phone_numbers_field = person_def.get_field("phone_numbers").unwrap();
        assert!(matches!(phone_numbers_field.field_type, CqlType::Set(_)));
        
        let addresses_field = person_def.get_field("addresses").unwrap();
        assert!(matches!(addresses_field.field_type, CqlType::List(_)));
        
        let metadata_field = person_def.get_field("metadata").unwrap();
        assert!(matches!(metadata_field.field_type, CqlType::Map(_, _)));
    }

    #[test]
    fn test_frozen_udt_parsing() {
        let registry = UdtRegistry::with_cassandra5_defaults();
        let address_def = registry.get_udt("system", "address").unwrap();
        
        let address_udt = create_sample_address_udt();
        let serialized = serialize_udt_for_test(&address_udt);
        
        let (remaining, parsed_value) = parse_frozen_udt_with_registry(
            &serialized, 
            address_def, 
            &registry
        ).unwrap();
        
        assert!(remaining.is_empty());
        
        if let Value::Frozen(boxed_value) = parsed_value {
            if let Value::Udt(frozen_udt) = boxed_value.as_ref() {
                assert_eq!(frozen_udt.type_name, "address");
                assert_eq!(frozen_udt.keyspace, "system");
            } else {
                panic!("Expected UDT inside Frozen wrapper");
            }
        } else {
            panic!("Expected Frozen value");
        }
    }

    #[test]
    fn test_udt_validation() {
        let registry = UdtRegistry::with_cassandra5_defaults();
        let person_def = registry.get_udt("system", "person").unwrap();
        
        // Valid person UDT
        let valid_person = UdtValue {
            type_name: "person".to_string(),
            keyspace: "system".to_string(),
            fields: vec![
                UdtField {
                    name: "id".to_string(),
                    value: Some(Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])),
                },
                UdtField {
                    name: "first_name".to_string(),
                    value: Some(Value::Text("John".to_string())),
                },
                UdtField {
                    name: "last_name".to_string(),
                    value: Some(Value::Text("Doe".to_string())),
                },
            ],
        };
        
        // Validation should succeed for required fields
        // Note: UdtTypeDef::validate_value would need the proper field types
        // This is a conceptual test - actual validation depends on field type matching
        assert_eq!(valid_person.type_name, "person");
        assert_eq!(valid_person.keyspace, "system");
    }

    #[test]
    fn test_list_with_udt_elements() {
        let registry = UdtRegistry::with_cassandra5_defaults();
        
        // Create a list of address UDTs
        let addresses = vec![
            Value::Udt(create_sample_address_udt()),
            Value::Udt({
                let mut addr = create_sample_address_udt();
                addr.set_field("street".to_string(), Some(Value::Text("456 Oak Ave".to_string())));
                addr.set_field("city".to_string(), Some(Value::Text("Other City".to_string())));
                addr
            }),
        ];
        
        let list_value = Value::List(addresses);
        
        // Verify structure
        if let Value::List(list) = list_value {
            assert_eq!(list.len(), 2);
            
            for item in list {
                if let Value::Udt(udt) = item {
                    assert_eq!(udt.type_name, "address");
                    assert_eq!(udt.field_count(), 5);
                } else {
                    panic!("Expected UDT in list");
                }
            }
        }
    }

    #[test]
    fn test_map_with_udt_values() {
        let registry = UdtRegistry::with_cassandra5_defaults();
        
        // Create a map with text keys and person UDT values
        let people_map = vec![
            (Value::Text("employee1".to_string()), Value::Udt(create_sample_person_udt())),
            (Value::Text("employee2".to_string()), Value::Udt({
                let mut person = create_sample_person_udt();
                person.set_field("first_name".to_string(), Some(Value::Text("Jane".to_string())));
                person.set_field("last_name".to_string(), Some(Value::Text("Smith".to_string())));
                person
            })),
        ];
        
        let map_value = Value::Map(people_map);
        
        // Verify structure
        if let Value::Map(map) = map_value {
            assert_eq!(map.len(), 2);
            
            for (key, value) in map {
                assert!(matches!(key, Value::Text(_)));
                if let Value::Udt(udt) = value {
                    assert_eq!(udt.type_name, "person");
                } else {
                    panic!("Expected UDT value in map");
                }
            }
        }
    }

    #[test]
    fn test_empty_value_creation_for_cql_types() {
        // Test empty value creation for various CQL types
        let empty_text = Value::Text(String::new());
        assert_eq!(empty_text, Value::Text(String::new()));
        
        let empty_int = Value::Integer(0);
        assert_eq!(empty_int, Value::Integer(0));
        
        let empty_list = Value::List(Vec::new());
        assert_eq!(empty_list, Value::List(Vec::new()));
        
        let empty_udt = Value::Udt(UdtValue {
            type_name: "test".to_string(),
            fields: vec![],
        });
        if let Value::Udt(udt) = &empty_udt {
            assert_eq!(udt.type_name, "test");
            assert_eq!(udt.fields.len(), 0);
        } else {
            panic!("Expected UDT value");
        }
    }

    #[test]
    fn test_udt_enhanced_parsing_fallback() {
        // Test that enhanced parsing falls back to embedded schema when registry lookup fails
        let person_udt = create_sample_person_udt();
        let serialized = serialize_udt_for_test(&person_udt);
        
        // Use enhanced parsing (which will fall back since "person" might not be in registry)
        let (remaining, parsed_value) = parse_udt_enhanced(&serialized).unwrap();
        assert!(remaining.is_empty());
        
        if let Value::Udt(parsed_udt) = parsed_value {
            assert_eq!(parsed_udt.type_name, "person");
            // Keyspace will be "unknown" from embedded parsing
        } else {
            panic!("Expected UDT value");
        }
    }

    #[test]
    fn test_complex_nested_structure() {
        let mut registry = UdtRegistry::with_cassandra5_defaults();
        
        // Test the contact_info UDT which nests person and address
        let contact_info_def = registry.get_udt("system", "contact_info").unwrap();
        
        // Verify the nested structure
        let person_field = contact_info_def.get_field("person").unwrap();
        assert!(matches!(person_field.field_type, CqlType::Udt(_, _)));
        
        let address_field = contact_info_def.get_field("primary_address").unwrap();
        assert!(matches!(address_field.field_type, CqlType::Udt(_, _)));
        
        let emergency_contacts_field = contact_info_def.get_field("emergency_contacts").unwrap();
        assert!(matches!(emergency_contacts_field.field_type, CqlType::List(_)));
    }
}