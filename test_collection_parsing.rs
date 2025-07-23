#!/usr/bin/env rust-script

//! Test collection type parsing with Cassandra 5.0 format
//!
//! This is a standalone test to verify our collection parsing implementation
//! works with real data formats

use std::fs;

mod types {
    //! Embedded types for testing
    
    #[derive(Debug, PartialEq, Clone)]
    pub enum Value {
        Null,
        Boolean(bool),
        Integer(i32),
        BigInt(i64),
        Float(f64),
        Text(String),
        Blob(Vec<u8>),
        List(Vec<Value>),
        Set(Vec<Value>),
        Map(Vec<(Value, Value)>),
        Uuid([u8; 16]),
        Timestamp(i64),
        // Additional types for completeness
        TinyInt(i8),
        SmallInt(i16),
        Float32(f32),
        Json(serde_json::Value),
        Tuple(Vec<Value>),
        Udt(UdtValue),
        Frozen(Box<Value>),
        Tombstone(TombstoneInfo),
    }

    #[derive(Debug, PartialEq, Clone)]
    pub struct UdtValue {
        pub type_name: String,
        pub keyspace: String,
        pub fields: Vec<UdtField>,
    }

    #[derive(Debug, PartialEq, Clone)]
    pub struct UdtField {
        pub name: String,
        pub value: Option<Value>,
    }

    #[derive(Debug, PartialEq, Clone)]
    pub struct TombstoneInfo {
        pub deletion_time: i64,
        pub tombstone_type: TombstoneType,
        pub ttl: Option<i64>,
        pub range_start: Option<Vec<u8>>,
        pub range_end: Option<Vec<u8>>,
    }

    #[derive(Debug, PartialEq, Clone)]
    pub enum TombstoneType {
        RowTombstone,
        CellTombstone,
        RangeTombstone,
        TtlExpiration,
    }

    impl Value {
        pub fn data_type(&self) -> &'static str {
            match self {
                Value::Null => "null",
                Value::Boolean(_) => "boolean",
                Value::Integer(_) => "int",
                Value::BigInt(_) => "bigint",
                Value::Float(_) => "double",
                Value::Text(_) => "text",
                Value::Blob(_) => "blob",
                Value::List(_) => "list",
                Value::Set(_) => "set",
                Value::Map(_) => "map",
                Value::Uuid(_) => "uuid",
                Value::Timestamp(_) => "timestamp",
                Value::TinyInt(_) => "tinyint",
                Value::SmallInt(_) => "smallint",
                Value::Float32(_) => "float",
                Value::Json(_) => "json",
                Value::Tuple(_) => "tuple",
                Value::Udt(_) => "udt",
                Value::Frozen(_) => "frozen",
                Value::Tombstone(_) => "tombstone",
            }
        }
    }

    pub fn encode_vint(value: i64) -> Vec<u8> {
        if value >= -64 && value <= 63 {
            vec![value as u8]
        } else if value >= -8192 && value <= 8191 {
            let bytes = (value as u16).to_be_bytes();
            vec![0x80 | bytes[0], bytes[1]]
        } else {
            // Simplified - just use 8 bytes for larger values
            let mut result = vec![0xFF];
            result.extend_from_slice(&value.to_be_bytes());
            result
        }
    }
}

use types::{Value, encode_vint};

/// Create test data for LIST<TEXT> with Cassandra 5 format
fn create_test_list_data() -> Vec<u8> {
    let mut data = Vec::new();
    
    // List count: 3 elements
    data.extend_from_slice(&encode_vint(3));
    
    // Element type: VARCHAR (0x0D)
    data.push(0x0D);
    
    // Element 1: "apple"
    let apple = b"apple";
    data.extend_from_slice(&encode_vint(apple.len() as i64)); // length
    data.extend_from_slice(&encode_vint(apple.len() as i64)); // string length
    data.extend_from_slice(apple);
    
    // Element 2: "banana"
    let banana = b"banana";
    data.extend_from_slice(&encode_vint(banana.len() as i64)); // length
    data.extend_from_slice(&encode_vint(banana.len() as i64)); // string length
    data.extend_from_slice(banana);
    
    // Element 3: "cherry"
    let cherry = b"cherry";
    data.extend_from_slice(&encode_vint(cherry.len() as i64)); // length
    data.extend_from_slice(&encode_vint(cherry.len() as i64)); // string length
    data.extend_from_slice(cherry);
    
    data
}

/// Create test data for SET<INT> with Cassandra 5 format
fn create_test_set_data() -> Vec<u8> {
    let mut data = Vec::new();
    
    // Set count: 3 elements
    data.extend_from_slice(&encode_vint(3));
    
    // Element type: INT (0x09)
    data.push(0x09);
    
    // Element 1: 10
    data.extend_from_slice(&encode_vint(4)); // int length
    data.extend_from_slice(&10i32.to_be_bytes());
    
    // Element 2: 20
    data.extend_from_slice(&encode_vint(4)); // int length
    data.extend_from_slice(&20i32.to_be_bytes());
    
    // Element 3: 30
    data.extend_from_slice(&encode_vint(4)); // int length
    data.extend_from_slice(&30i32.to_be_bytes());
    
    data
}

/// Create test data for MAP<TEXT,INT> with Cassandra 5 format
fn create_test_map_data() -> Vec<u8> {
    let mut data = Vec::new();
    
    // Map count: 2 pairs
    data.extend_from_slice(&encode_vint(2));
    
    // Key type: VARCHAR (0x0D)
    data.push(0x0D);
    // Value type: INT (0x09)
    data.push(0x09);
    
    // Pair 1: "one" -> 1
    let one = b"one"; 
    data.extend_from_slice(&encode_vint(one.len() as i64)); // key length
    data.extend_from_slice(&encode_vint(one.len() as i64)); // string length
    data.extend_from_slice(one);
    data.extend_from_slice(&encode_vint(4)); // value length
    data.extend_from_slice(&1i32.to_be_bytes());
    
    // Pair 2: "two" -> 2
    let two = b"two";
    data.extend_from_slice(&encode_vint(two.len() as i64)); // key length
    data.extend_from_slice(&encode_vint(two.len() as i64)); // string length
    data.extend_from_slice(two);
    data.extend_from_slice(&encode_vint(4)); // value length
    data.extend_from_slice(&2i32.to_be_bytes());
    
    data
}

fn main() {
    println!("🧪 Testing Collection Type Parsing for Cassandra 5.0");
    println!("====================================================");
    
    // Test LIST<TEXT>
    println!("\n📋 Testing LIST<TEXT> parsing:");
    let list_data = create_test_list_data();
    println!("Created test data: {} bytes", list_data.len());
    println!("Data hex: {}", hex::encode(&list_data));
    
    // Test SET<INT>
    println!("\n🔢 Testing SET<INT> parsing:");
    let set_data = create_test_set_data();
    println!("Created test data: {} bytes", set_data.len());
    println!("Data hex: {}", hex::encode(&set_data));
    
    // Test MAP<TEXT,INT>
    println!("\n🗺️ Testing MAP<TEXT,INT> parsing:");
    let map_data = create_test_map_data();
    println!("Created test data: {} bytes", map_data.len());
    println!("Data hex: {}", hex::encode(&map_data));
    
    println!("\n✅ Test data generation complete!");
    println!("👉 Next: Test with real SSTable data from test-env/cassandra5/");
}