//! SSTable Test Data Generator
//!
//! This binary creates real Cassandra-compatible SSTable files with complex types
//! for testing CQLite's parsing and query capabilities.

use cqlite_core::{
    Config, Value,
    parser::{
        complex_types::{ComplexTypeParser, serialize_list_v5, serialize_map_v5, serialize_tuple},
        header::{SSTableHeader, CompressionInfo, SSTableStats},
        types::{serialize_cql_value, CqlTypeId},
        vint::encode_vint,
    },
    platform::Platform,
    storage::sstable::{SSTableManager, writer::SSTableWriter},
    types::{TableId, RowKey, UdtValue, UdtField},
};
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🏗️  SSTable Test Data Generator");
    println!("==============================");
    
    // Create output directory
    let output_dir = "test_sstables";
    fs::create_dir_all(output_dir).await?;
    
    // Generate different types of test SSTable files
    generate_simple_sstable(&output_dir).await?;
    generate_complex_types_sstable(&output_dir).await?;
    generate_large_dataset_sstable(&output_dir).await?;
    generate_cassandra_compatible_sstable(&output_dir).await?;
    
    println!("\n✅ All test SSTable files generated in '{}'", output_dir);
    println!("🔍 Use these files to test CQLite parsing capabilities");
    
    Ok(())
}

/// Generate simple SSTable with basic types
async fn generate_simple_sstable(output_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📝 Generating simple SSTable...");
    
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let file_path = Path::new(output_dir).join("simple_test.sst");
    
    let mut writer = SSTableWriter::create(&file_path, &config, platform).await?;
    
    // Simple test data
    let test_data = vec![
        (TableId::new("users".to_string()), RowKey::from("user1"), create_simple_user_record(1, "Alice", 25, true)),
        (TableId::new("users".to_string()), RowKey::from("user2"), create_simple_user_record(2, "Bob", 30, false)),
        (TableId::new("users".to_string()), RowKey::from("user3"), create_simple_user_record(3, "Carol", 28, true)),
        (TableId::new("users".to_string()), RowKey::from("user4"), create_simple_user_record(4, "David", 35, true)),
        (TableId::new("users".to_string()), RowKey::from("user5"), create_simple_user_record(5, "Eve", 22, false)),
    ];
    
    for (table_id, key, value) in test_data {
        writer.add_entry(&table_id, key, value).await?;
    }
    
    writer.finish().await?;
    
    println!("   ✓ Created simple_test.sst with {} records", 5);
    Ok(())
}

/// Generate SSTable with complex types (Lists, Sets, Maps, Tuples, UDTs)
async fn generate_complex_types_sstable(output_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔧 Generating complex types SSTable...");
    
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let file_path = Path::new(output_dir).join("complex_types_test.sst");
    
    let mut writer = SSTableWriter::create(&file_path, &config, platform).await?;
    
    // Complex test data
    let test_data = vec![
        (
            TableId::new("complex_data".to_string()), 
            RowKey::from("record1"), 
            create_complex_record_1()
        ),
        (
            TableId::new("complex_data".to_string()), 
            RowKey::from("record2"), 
            create_complex_record_2()
        ),
        (
            TableId::new("complex_data".to_string()), 
            RowKey::from("record3"), 
            create_complex_record_3()
        ),
    ];
    
    for (table_id, key, value) in test_data {
        writer.add_entry(&table_id, key, value).await?;
    }
    
    writer.finish().await?;
    
    println!("   ✓ Created complex_types_test.sst with complex data types");
    Ok(())
}

/// Generate large dataset for performance testing
async fn generate_large_dataset_sstable(output_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 Generating large dataset SSTable...");
    
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let file_path = Path::new(output_dir).join("large_dataset_test.sst");
    
    let mut writer = SSTableWriter::create(&file_path, &config, platform).await?;
    
    let record_count = 10000;
    
    for i in 1..=record_count {
        let table_id = TableId::new("large_table".to_string());
        let key = RowKey::from(format!("key_{:06}", i));
        let value = create_performance_test_record(i);
        
        writer.add_entry(&table_id, key, value).await?;
        
        if i % 1000 == 0 {
            println!("   📈 Progress: {}/{} records", i, record_count);
        }
    }
    
    writer.finish().await?;
    
    println!("   ✓ Created large_dataset_test.sst with {} records", record_count);
    Ok(())
}

/// Generate Cassandra-compatible SSTable with proper headers
async fn generate_cassandra_compatible_sstable(output_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🗄️  Generating Cassandra-compatible SSTable...");
    
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let file_path = Path::new(output_dir).join("cassandra_compatible.sst");
    
    // Create SSTable with proper Cassandra 5+ header
    let header = create_cassandra_5_header();
    let mut writer = SSTableWriter::create_with_header(&file_path, &config, platform, header).await?;
    
    // Add realistic e-commerce data
    let test_data = create_ecommerce_test_data();
    
    for (table_id, key, value) in test_data {
        writer.add_entry(&table_id, key, value).await?;
    }
    
    writer.finish().await?;
    
    println!("   ✓ Created cassandra_compatible.sst with proper headers");
    Ok(())
}

/// Create simple user record
fn create_simple_user_record(id: i32, name: &str, age: i32, active: bool) -> Value {
    // Simple tuple representing: (id, name, age, active)
    Value::Tuple(vec![
        Value::Integer(id),
        Value::Text(name.to_string()),
        Value::Integer(age),
        Value::Boolean(active),
    ])
}

/// Create complex record with Lists, Sets, Maps
fn create_complex_record_1() -> Value {
    let scores_list = Value::List(vec![
        Value::Integer(95),
        Value::Integer(87),
        Value::Integer(92),
        Value::Integer(88),
    ]);
    
    let tags_set = Value::Set(vec![
        Value::Text("developer".to_string()),
        Value::Text("rust".to_string()),
        Value::Text("database".to_string()),
    ]);
    
    let metadata_map = Value::Map(vec![
        (Value::Text("projects".to_string()), Value::Integer(12)),
        (Value::Text("years_exp".to_string()), Value::Integer(5)),
        (Value::Text("team_size".to_string()), Value::Integer(8)),
    ]);
    
    let location_tuple = Value::Tuple(vec![
        Value::Float(37.7749),   // latitude
        Value::Float(-122.4194), // longitude
        Value::Text("San Francisco".to_string()),
    ]);
    
    // Main record tuple
    Value::Tuple(vec![
        Value::Integer(1),
        Value::Text("Alice Johnson".to_string()),
        scores_list,
        tags_set,
        metadata_map,
        location_tuple,
        Value::Boolean(true),
    ])
}

/// Create complex record with nested structures
fn create_complex_record_2() -> Value {
    let nested_list = Value::List(vec![
        Value::List(vec![Value::Integer(1), Value::Integer(2)]),
        Value::List(vec![Value::Integer(3), Value::Integer(4)]),
        Value::List(vec![Value::Integer(5), Value::Integer(6)]),
    ]);
    
    let complex_map = Value::Map(vec![
        (
            Value::Text("config".to_string()), 
            Value::Map(vec![
                (Value::Text("enabled".to_string()), Value::Boolean(true)),
                (Value::Text("timeout".to_string()), Value::Integer(30)),
            ])
        ),
        (
            Value::Text("metrics".to_string()),
            Value::List(vec![
                Value::Float(0.95),
                Value::Float(0.87),
                Value::Float(0.92),
            ])
        ),
    ]);
    
    let address_udt = create_address_udt("456 Broadway", "New York", 10013, (40.7128, -74.0060));
    
    Value::Tuple(vec![
        Value::Integer(2),
        Value::Text("Bob Smith".to_string()),
        nested_list,
        complex_map,
        address_udt,
        Value::Boolean(true),
    ])
}

/// Create complex record with UDTs and frozen types
fn create_complex_record_3() -> Value {
    let history_list = Value::List(vec![
        Value::Tuple(vec![
            Value::Timestamp(1640995200000), // 2022-01-01
            Value::Text("joined".to_string()),
            Value::Integer(1),
        ]),
        Value::Tuple(vec![
            Value::Timestamp(1672531200000), // 2023-01-01
            Value::Text("promoted".to_string()),
            Value::Integer(2),
        ]),
    ]);
    
    let frozen_history = Value::Frozen(Box::new(history_list));
    let address_udt = create_address_udt("789 Baker St", "London", 12345, (51.5074, -0.1278));
    let frozen_address = Value::Frozen(Box::new(address_udt));
    
    let skills_set = Value::Set(vec![
        Value::Text("machine_learning".to_string()),
        Value::Text("python".to_string()),
        Value::Text("tensorflow".to_string()),
        Value::Text("data_analysis".to_string()),
    ]);
    
    Value::Tuple(vec![
        Value::Integer(3),
        Value::Text("Carol Davis".to_string()),
        skills_set,
        frozen_history,
        frozen_address,
        Value::Boolean(false),
    ])
}

/// Create address UDT value
fn create_address_udt(street: &str, city: &str, zipcode: i32, coordinates: (f64, f64)) -> Value {
    let fields = vec![
        UdtField {
            name: "street".to_string(),
            value: Some(Value::Text(street.to_string())),
        },
        UdtField {
            name: "city".to_string(),
            value: Some(Value::Text(city.to_string())),
        },
        UdtField {
            name: "zipcode".to_string(),
            value: Some(Value::Integer(zipcode)),
        },
        UdtField {
            name: "coordinates".to_string(),
            value: Some(Value::Tuple(vec![
                Value::Float(coordinates.0),
                Value::Float(coordinates.1),
            ])),
        },
    ];
    
    Value::Udt(UdtValue {
        type_name: "address".to_string(),
        keyspace: "ecommerce".to_string(),
        fields,
    })
}

/// Create performance test record with reasonable complexity
fn create_performance_test_record(id: i32) -> Value {
    let scores = Value::List((1..=10).map(|i| Value::Integer(id % 100 + i)).collect());
    
    let tags = Value::Set(vec![
        Value::Text(format!("tag_{}", id % 5)),
        Value::Text(format!("category_{}", id % 3)),
        Value::Text("performance_test".to_string()),
    ]);
    
    let metadata = Value::Map(vec![
        (Value::Text("batch".to_string()), Value::Integer(id / 1000)),
        (Value::Text("checksum".to_string()), Value::Integer(id * 31 % 1000)),
    ]);
    
    Value::Tuple(vec![
        Value::Integer(id),
        Value::Text(format!("record_{}", id)),
        scores,
        tags,
        metadata,
        Value::Boolean(id % 2 == 0),
        Value::Timestamp(1640995200000 + (id as i64 * 1000)),
    ])
}

/// Create realistic e-commerce test data
fn create_ecommerce_test_data() -> Vec<(TableId, RowKey, Value)> {
    vec![
        // Product table
        (
            TableId::new("products".to_string()),
            RowKey::from("prod_1"),
            create_product_record(1, "Laptop", 999.99, vec!["electronics", "computers"], true)
        ),
        (
            TableId::new("products".to_string()),
            RowKey::from("prod_2"),
            create_product_record(2, "Smartphone", 699.99, vec!["electronics", "mobile"], true)
        ),
        (
            TableId::new("products".to_string()),
            RowKey::from("prod_3"),
            create_product_record(3, "Book", 29.99, vec!["books", "education"], false)
        ),
        
        // Orders table
        (
            TableId::new("orders".to_string()),
            RowKey::from("order_1"),
            create_order_record(1, "user_1", vec![(1, 2), (3, 1)], 2059.97)
        ),
        (
            TableId::new("orders".to_string()),
            RowKey::from("order_2"),
            create_order_record(2, "user_2", vec![(2, 1)], 699.99)
        ),
        
        // Users table
        (
            TableId::new("users".to_string()),
            RowKey::from("user_1"),
            create_user_record(1, "alice@example.com", "Alice Johnson", "premium")
        ),
        (
            TableId::new("users".to_string()),
            RowKey::from("user_2"),
            create_user_record(2, "bob@example.com", "Bob Smith", "basic")
        ),
    ]
}

fn create_product_record(id: i32, name: &str, price: f64, categories: Vec<&str>, available: bool) -> Value {
    let categories_set = Value::Set(
        categories.into_iter().map(|cat| Value::Text(cat.to_string())).collect()
    );
    
    let specifications = Value::Map(vec![
        (Value::Text("weight".to_string()), Value::Float(2.5)),
        (Value::Text("warranty_months".to_string()), Value::Integer(24)),
        (Value::Text("rating".to_string()), Value::Float(4.5)),
    ]);
    
    Value::Tuple(vec![
        Value::Integer(id),
        Value::Text(name.to_string()),
        Value::Float(price),
        categories_set,
        specifications,
        Value::Boolean(available),
        Value::Timestamp(1640995200000),
    ])
}

fn create_order_record(id: i32, user_id: &str, items: Vec<(i32, i32)>, total: f64) -> Value {
    let order_items = Value::List(
        items.into_iter().map(|(product_id, quantity)| {
            Value::Tuple(vec![
                Value::Integer(product_id),
                Value::Integer(quantity),
            ])
        }).collect()
    );
    
    let order_metadata = Value::Map(vec![
        (Value::Text("shipping_method".to_string()), Value::Text("standard".to_string())),
        (Value::Text("payment_method".to_string()), Value::Text("credit_card".to_string())),
        (Value::Text("discount_applied".to_string()), Value::Boolean(false)),
    ]);
    
    Value::Tuple(vec![
        Value::Integer(id),
        Value::Text(user_id.to_string()),
        order_items,
        Value::Float(total),
        order_metadata,
        Value::Timestamp(1640995200000 + (id as i64 * 86400000)), // One day apart
    ])
}

fn create_user_record(id: i32, email: &str, name: &str, tier: &str) -> Value {
    let preferences = Value::Map(vec![
        (Value::Text("newsletter".to_string()), Value::Boolean(true)),
        (Value::Text("notifications".to_string()), Value::Boolean(false)),
        (Value::Text("theme".to_string()), Value::Text("dark".to_string())),
    ]);
    
    let purchase_history = Value::List(vec![
        Value::Integer(1),
        Value::Integer(3),
        Value::Integer(7),
    ]);
    
    let address = create_address_udt("123 Main St", "San Francisco", 94102, (37.7749, -122.4194));
    
    Value::Tuple(vec![
        Value::Integer(id),
        Value::Text(email.to_string()),
        Value::Text(name.to_string()),
        Value::Text(tier.to_string()),
        preferences,
        purchase_history,
        address,
        Value::Timestamp(1609459200000), // Account creation
    ])
}

/// Create proper Cassandra 5+ SSTable header
fn create_cassandra_5_header() -> SSTableHeader {
    use std::collections::HashMap;
    
    let mut properties = HashMap::new();
    properties.insert("format_version".to_string(), "oa".to_string());
    properties.insert("cassandra_version".to_string(), "5.0.0".to_string());
    properties.insert("created_by".to_string(), "CQLite Test Generator".to_string());
    properties.insert("bloom_filter_fp_chance".to_string(), "0.01".to_string());
    properties.insert("compression".to_string(), "LZ4Compressor".to_string());
    
    SSTableHeader {
        magic: 0x5354414C, // "STAL" in hex
        format_version: "oa".to_string(),
        properties,
        compression: CompressionInfo {
            algorithm: "LZ4".to_string(),
            chunk_length: 65536,
            data_length: 0, // Will be set by writer
            compressed_length: 0, // Will be set by writer
        },
        stats: SSTableStats {
            estimated_row_size: 512,
            estimated_column_count: 8,
            row_count: 0, // Will be updated by writer
            compression_ratio: 0.7,
        },
    }
}