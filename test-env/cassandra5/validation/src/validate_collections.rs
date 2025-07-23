//! Validation program for collections_table SSTable
//! Tests parsing of CQL collection types: list, set, map

use std::path::Path;
use std::sync::Arc;
use cqlite_core::{
    platform::Platform,
    storage::sstable::reader::SSTableReader,
    types::TableId,
    Config, Result, RowKey, Value,
};

#[tokio::main]
async fn main() -> Result<()> {
    println!("🧪 Collections Table SSTable Validation");
    println!("========================================");

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    // Path to the collections_table SSTable
    let sstable_path = Path::new("../sstables").join("collections_table-86aef010669411f0acab47cdf782cef5");
    let data_file = sstable_path.join("nb-1-big-Data.db");

    if !data_file.exists() {
        eprintln!("❌ SSTable data file not found: {:?}", data_file);
        std::process::exit(1);
    }

    println!("📁 Opening SSTable from: {:?}", sstable_path);

    // Open the SSTable reader
    let reader = match SSTableReader::open(&sstable_path, &config, platform.clone()).await {
        Ok(reader) => {
            println!("✅ Successfully opened SSTable reader");
            reader
        }
        Err(e) => {
            eprintln!("❌ Failed to open SSTable reader: {}", e);
            std::process::exit(1);
        }
    };

    // Get reader statistics
    let stats = reader.get_stats().await?;
    println!("📊 SSTable Statistics:");
    println!("   • File size: {} bytes", stats.file_size);
    println!("   • Entry count: {}", stats.entry_count);
    println!("   • Table count: {}", stats.table_count);
    println!("   • Block count: {}", stats.block_count);
    println!("   • Index size: {} bytes", stats.index_size);
    println!("   • Bloom filter size: {} bytes", stats.bloom_filter_size);
    println!("   • Compression ratio: {:.2}", stats.compression_ratio);

    // Test collection types we expect in the collections_table
    let table_id = TableId::new("collections_table");
    let mut validation_results = Vec::new();

    // Test cases for different collection types
    let test_cases = vec![
        ("list_field", CollectionType::List),
        ("set_field", CollectionType::Set),
        ("map_field", CollectionType::Map),
        ("frozen_list", CollectionType::List),
        ("frozen_set", CollectionType::Set),
        ("frozen_map", CollectionType::Map),
    ];

    println!("\n🔍 Testing collection type parsing:");

    for (test_key, expected_type) in test_cases {
        let key = RowKey::from(test_key);
        
        match reader.get(&table_id, &key).await {
            Ok(Some(value)) => {
                let actual_type = get_collection_type(&value);
                let matches = actual_type == expected_type;
                
                validation_results.push(ValidationResult {
                    key: test_key.to_string(),
                    expected_type: format!("{:?}", expected_type),
                    actual_type: format!("{:?}", actual_type),
                    value: format!("{:?}", value),
                    matches,
                    collection_details: analyze_collection(&value),
                });

                let status = if matches { "✅" } else { "❌" };
                println!("   {} {}: {} -> {:?}", status, test_key, format!("{:?}", expected_type), value);
                
                // Additional collection analysis
                if let Some(details) = analyze_collection(&value) {
                    println!("      └─ Collection details: {}", details);
                }
            }
            Ok(None) => {
                println!("   ⚠️  {}: No value found", test_key);
                validation_results.push(ValidationResult {
                    key: test_key.to_string(),
                    expected_type: format!("{:?}", expected_type),
                    actual_type: "None".to_string(),
                    value: "None".to_string(),
                    matches: false,
                    collection_details: None,
                });
            }
            Err(e) => {
                println!("   ❌ {}: Error reading value: {}", test_key, e);
                validation_results.push(ValidationResult {
                    key: test_key.to_string(),
                    expected_type: format!("{:?}", expected_type),
                    actual_type: format!("Error: {}", e),
                    value: "Error".to_string(),
                    matches: false,
                    collection_details: None,
                });
            }
        }
    }

    // Scan all entries in the collections table to see what's actually there
    println!("\n🔎 Scanning all entries in collections table:");
    match reader.scan_table(&table_id).await {
        Ok(entries) => {
            println!("   Found {} entries total", entries.len());
            for (i, (key, value)) in entries.iter().take(10).enumerate() {
                println!("   [{:2}] {:?} -> {:?}", i + 1, key, value);
                
                // Show collection analysis for each entry
                if let Some(details) = analyze_collection(value) {
                    println!("        └─ {}", details);
                }
            }
            if entries.len() > 10 {
                println!("   ... and {} more entries", entries.len() - 10);
            }
        }
        Err(e) => {
            println!("   ❌ Error scanning table: {}", e);
        }
    }

    // Test specific collection operations
    println!("\n🧪 Testing collection operations:");
    test_collection_operations(&reader, &table_id).await?;

    // Generate validation report
    generate_validation_report(&validation_results)?;

    let successful_validations = validation_results.iter().filter(|r| r.matches).count();
    let total_validations = validation_results.len();

    println!("\n📋 Validation Summary:");
    println!("   • Total tests: {}", total_validations);
    println!("   • Successful: {}", successful_validations);
    println!("   • Failed: {}", total_validations - successful_validations);
    println!("   • Success rate: {:.1}%", (successful_validations as f64 / total_validations as f64) * 100.0);

    if successful_validations == total_validations {
        println!("\n🎉 All collection type validations passed!");
    } else {
        println!("\n⚠️  Some validations failed. Check validation report for details.");
    }

    Ok(())
}

#[derive(Debug, PartialEq)]
enum CollectionType {
    List,
    Set,
    Map,
    Unknown,
}

fn get_collection_type(value: &Value) -> CollectionType {
    match value {
        Value::List(_) => CollectionType::List,
        Value::Set(_) => CollectionType::Set,
        Value::Map(_) => CollectionType::Map,
        _ => CollectionType::Unknown,
    }
}

fn analyze_collection(value: &Value) -> Option<String> {
    match value {
        Value::List(items) => {
            Some(format!("List with {} items", items.len()))
        }
        Value::Set(items) => {
            Some(format!("Set with {} items", items.len()))
        }
        Value::Map(items) => {
            Some(format!("Map with {} key-value pairs", items.len()))
        }
        _ => None,
    }
}

async fn test_collection_operations(reader: &SSTableReader, table_id: &TableId) -> Result<()> {
    // Test collection-specific operations like element access, size checks, etc.
    println!("   🔍 Testing collection element access...");
    
    // Try to access first few entries and examine their collection properties
    match reader.scan_table(table_id).await {
        Ok(entries) => {
            for (key, value) in entries.iter().take(3) {
                match value {
                    Value::List(items) => {
                        println!("      • List at {:?}: {} elements", key, items.len());
                        if !items.is_empty() {
                            println!("         └─ First element: {:?}", items[0]);
                        }
                    }
                    Value::Set(items) => {
                        println!("      • Set at {:?}: {} elements", key, items.len());
                        if !items.is_empty() {
                            println!("         └─ Sample element: {:?}", items.iter().next().unwrap());
                        }
                    }
                    Value::Map(items) => {
                        println!("      • Map at {:?}: {} key-value pairs", key, items.len());
                        if !items.is_empty() {
                            let (first_key, first_value) = items.iter().next().unwrap();
                            println!("         └─ Sample entry: {:?} -> {:?}", first_key, first_value);
                        }
                    }
                    _ => {
                        println!("      • Non-collection at {:?}: {:?}", key, value);
                    }
                }
            }
        }
        Err(e) => {
            println!("      ❌ Error during collection operations test: {}", e);
        }
    }
    
    Ok(())
}

#[derive(Debug)]
struct ValidationResult {
    key: String,
    expected_type: String,
    actual_type: String,
    value: String,
    matches: bool,
    collection_details: Option<String>,
}

fn generate_validation_report(results: &[ValidationResult]) -> Result<()> {
    use std::fs::File;
    use std::io::Write;

    let mut report = File::create("validation_report_collections.json")?;
    
    let json_report = serde_json::json!({
        "test_name": "collections_validation",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "total_tests": results.len(),
        "successful_tests": results.iter().filter(|r| r.matches).count(),
        "failed_tests": results.iter().filter(|r| !r.matches).count(),
        "results": results.iter().map(|r| {
            serde_json::json!({
                "key": r.key,
                "expected_type": r.expected_type,
                "actual_type": r.actual_type,
                "value": r.value,
                "matches": r.matches,
                "collection_details": r.collection_details
            })
        }).collect::<Vec<_>>()
    });

    writeln!(report, "{}", serde_json::to_string_pretty(&json_report)?)?;
    println!("📄 Validation report saved to: validation_report_collections.json");

    Ok(())
}