//! Validation program for users SSTable  
//! Tests parsing of User Defined Types (UDTs)

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
    println!("🧪 Users Table SSTable Validation (UDTs)");
    println!("=========================================");

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    // Path to the users SSTable
    let sstable_path = Path::new("../sstables").join("users-86c166a0669411f0acab47cdf782cef5");
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

    // Test UDT fields we expect in the users table
    let table_id = TableId::new("users");
    let mut validation_results = Vec::new();

    // Test cases for UDT fields - assuming users table has address and profile UDTs
    let test_cases = vec![
        ("user_id", FieldType::Primitive),
        ("name", FieldType::Primitive), 
        ("email", FieldType::Primitive),
        ("address", FieldType::UDT),
        ("profile", FieldType::UDT),
        ("preferences", FieldType::UDT),
        ("metadata", FieldType::UDT),
    ];

    println!("\n🔍 Testing UDT field parsing:");

    for (test_key, expected_type) in test_cases {
        let key = RowKey::from(test_key);
        
        match reader.get(&table_id, &key).await {
            Ok(Some(value)) => {
                let actual_type = get_field_type(&value);
                let matches = actual_type == expected_type;
                
                let udt_analysis = if matches && expected_type == FieldType::UDT {
                    analyze_udt(&value)
                } else {
                    None
                };
                
                validation_results.push(ValidationResult {
                    key: test_key.to_string(),
                    expected_type: format!("{:?}", expected_type),
                    actual_type: format!("{:?}", actual_type),
                    value: format!("{:?}", value),
                    matches,
                    udt_details: udt_analysis,
                });

                let status = if matches { "✅" } else { "❌" };
                println!("   {} {}: {} -> {:?}", status, test_key, format!("{:?}", expected_type), value);
                
                // Show UDT structure analysis
                if let Some(details) = &udt_analysis {
                    println!("      └─ UDT structure: {}", details);
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
                    udt_details: None,
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
                    udt_details: None,
                });
            }
        }
    }

    // Scan all entries in the users table to see the full structure
    println!("\n🔎 Scanning all entries in users table:");
    match reader.scan_table(&table_id).await {
        Ok(entries) => {
            println!("   Found {} entries total", entries.len());
            for (i, (key, value)) in entries.iter().take(5).enumerate() {
                println!("   [{:2}] {:?} -> {:?}", i + 1, key, value);
                
                // Show UDT analysis for each entry
                if let Some(details) = analyze_udt(value) {
                    println!("        └─ UDT: {}", details);
                }
            }
            if entries.len() > 5 {
                println!("   ... and {} more entries", entries.len() - 5);
            }
        }
        Err(e) => {
            println!("   ❌ Error scanning table: {}", e);
        }
    }

    // Test UDT-specific operations
    println!("\n🧪 Testing UDT operations:");
    test_udt_operations(&reader, &table_id).await?;

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
        println!("\n🎉 All UDT validations passed!");
    } else {
        println!("\n⚠️  Some validations failed. Check validation report for details.");
    }

    Ok(())
}

#[derive(Debug, PartialEq)]
enum FieldType {
    Primitive,
    UDT,
    Unknown,
}

fn get_field_type(value: &Value) -> FieldType {
    match value {
        Value::UserDefinedType(_) => FieldType::UDT,
        Value::Text(_) | Value::Integer(_) | Value::BigInt(_) | Value::Boolean(_) |
        Value::Float(_) | Value::Double(_) | Value::Timestamp(_) | Value::Uuid(_) => FieldType::Primitive,
        _ => FieldType::Unknown,
    }
}

fn analyze_udt(value: &Value) -> Option<String> {
    match value {
        Value::UserDefinedType(fields) => {
            let field_count = fields.len();
            let field_names: Vec<String> = fields.keys().cloned().collect();
            Some(format!("UDT with {} fields: [{}]", field_count, field_names.join(", ")))
        }
        _ => None,
    }
}

async fn test_udt_operations(reader: &SSTableReader, table_id: &TableId) -> Result<()> {
    println!("   🔍 Testing UDT field access...");
    
    // Try to access first few entries and examine their UDT properties
    match reader.scan_table(table_id).await {
        Ok(entries) => {
            for (key, value) in entries.iter().take(3) {
                match value {
                    Value::UserDefinedType(fields) => {
                        println!("      • UDT at {:?}: {} fields", key, fields.len());
                        for (field_name, field_value) in fields.iter().take(3) {
                            println!("         └─ {}: {:?}", field_name, field_value);
                        }
                        if fields.len() > 3 {
                            println!("         └─ ... and {} more fields", fields.len() - 3);
                        }
                    }
                    _ => {
                        println!("      • Non-UDT at {:?}: {:?}", key, value);
                    }
                }
            }
        }
        Err(e) => {
            println!("      ❌ Error during UDT operations test: {}", e);
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
    udt_details: Option<String>,
}

fn generate_validation_report(results: &[ValidationResult]) -> Result<()> {
    use std::fs::File;
    use std::io::Write;

    let mut report = File::create("validation_report_users.json")?;
    
    let json_report = serde_json::json!({
        "test_name": "users_udt_validation",
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
                "udt_details": r.udt_details
            })
        }).collect::<Vec<_>>()
    });

    writeln!(report, "{}", serde_json::to_string_pretty(&json_report)?)?;
    println!("📄 Validation report saved to: validation_report_users.json");

    Ok(())
}