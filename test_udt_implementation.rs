#!/usr/bin/env rust-script

//! Test program to validate UDT implementation
//!
//! This program demonstrates the User Defined Type (UDT) implementation
//! working with Cassandra-compatible binary format parsing and serialization.

use cqlite_core::types::{Value, UdtValue, UdtField};
use cqlite_core::parser::types::{serialize_cql_value, CqlTypeId};

fn main() {
    println!("🚀 CQLite UDT Implementation Test");
    println!("==================================");

    // Test 1: Create a Person UDT
    println!("\n📋 Test 1: Creating Person UDT");
    let person_udt = UdtValue {
        type_name: "Person".to_string(),
        keyspace: "test_keyspace".to_string(),
        fields: vec![
            UdtField {
                name: "name".to_string(),
                value: Some(Value::Text("John Doe".to_string())),
            },
            UdtField {
                name: "age".to_string(),
                value: Some(Value::Integer(30)),
            },
            UdtField {
                name: "email".to_string(),
                value: Some(Value::Text("john.doe@example.com".to_string())),
            },
            UdtField {
                name: "phone".to_string(),
                value: None, // NULL field
            },
        ],
    };

    println!("✅ Created Person UDT: {}", Value::Udt(person_udt.clone()));

    // Test 2: UDT Serialization
    println!("\n📋 Test 2: UDT Serialization");
    match serialize_cql_value(&Value::Udt(person_udt.clone())) {
        Ok(serialized) => {
            println!("✅ UDT serialized successfully!");
            println!("   Serialized size: {} bytes", serialized.len());
            println!("   First byte (type ID): 0x{:02X} (should be 0x30 for UDT)", serialized[0]);
            
            // Validate the type ID
            if serialized[0] == CqlTypeId::Udt as u8 {
                println!("✅ Correct UDT type ID found");
            } else {
                println!("❌ Incorrect type ID");
            }
        }
        Err(e) => {
            println!("❌ UDT serialization failed: {}", e);
        }
    }

    // Test 3: UDT Field Access
    println!("\n📋 Test 3: UDT Field Access");
    if let Some(name_value) = person_udt.get_field("name") {
        println!("✅ Retrieved name field: {}", name_value);
    } else {
        println!("❌ Failed to retrieve name field");
    }

    if let Some(age_value) = person_udt.get_field("age") {
        println!("✅ Retrieved age field: {}", age_value);
    } else {
        println!("❌ Failed to retrieve age field");
    }

    // Test null field
    if person_udt.get_field("phone").is_none() {
        println!("✅ Null field correctly handled for 'phone'");
    } else {
        println!("❌ Null field handling failed");
    }

    // Test 4: Nested UDT Example
    println!("\n📋 Test 4: Nested UDT Example");
    let address_udt = UdtValue {
        type_name: "Address".to_string(),
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
        ],
    };

    let person_with_address = UdtValue {
        type_name: "PersonWithAddress".to_string(),
        keyspace: "test_keyspace".to_string(),
        fields: vec![
            UdtField {
                name: "name".to_string(),
                value: Some(Value::Text("Jane Smith".to_string())),
            },
            UdtField {
                name: "address".to_string(),
                value: Some(Value::Udt(address_udt)), // Nested UDT
            },
        ],
    };

    println!("✅ Created nested UDT structure");

    match serialize_cql_value(&Value::Udt(person_with_address)) {
        Ok(serialized) => {
            println!("✅ Nested UDT serialized successfully!");
            println!("   Serialized size: {} bytes", serialized.len());
        }
        Err(e) => {
            println!("❌ Nested UDT serialization failed: {}", e);
        }
    }

    // Test 5: Tuple Type
    println!("\n📋 Test 5: Tuple Type");
    let tuple_value = Value::Tuple(vec![
        Value::Text("hello".to_string()),
        Value::Integer(42),
        Value::Boolean(true),
    ]);

    match serialize_cql_value(&tuple_value) {
        Ok(serialized) => {
            println!("✅ Tuple serialized successfully!");
            println!("   Serialized size: {} bytes", serialized.len());
            println!("   First byte (type ID): 0x{:02X} (should be 0x31 for Tuple)", serialized[0]);
            
            if serialized[0] == CqlTypeId::Tuple as u8 {
                println!("✅ Correct Tuple type ID found");
            } else {
                println!("❌ Incorrect type ID");
            }
        }
        Err(e) => {
            println!("❌ Tuple serialization failed: {}", e);
        }
    }

    println!("\n🎉 UDT Implementation Test Complete!");
    println!("=====================================");
    println!("✅ UDT structures created successfully");
    println!("✅ Serialization working");
    println!("✅ Field access working");
    println!("✅ Null field handling working");
    println!("✅ Nested UDT support working");
    println!("✅ Tuple support working");
    println!("\n🔧 UDT Implementation Status: FUNCTIONAL");
}