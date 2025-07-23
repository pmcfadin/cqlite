//! Validation program for all_types SSTable
//! Tests parsing of all primitive CQL types: text, int, bigint, float, double, boolean, timestamp, uuid, etc.

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
    println!("🧪 All Types SSTable Validation");
    println!("================================");

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    // Path to the all_types SSTable
    let sstable_path = Path::new("../sstables/all_types-86a52c10669411f0acab47cdf782cef5");
    let data_file = sstable_path.join("nb-1-big-Data.db");

    if !data_file.exists() {
        eprintln!("❌ SSTable data file not found: {:?}", data_file);
        std::process::exit(1);
    }

    println!("📁 Opening SSTable from: {:?}", sstable_path);

    // Open the SSTable reader
    let reader = match SSTableReader::open(&data_file, &config, platform.clone()).await {
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
    let stats = reader.stats().await?;
    println!("📊 SSTable Statistics:");
    println!("   • File size: {} bytes", stats.file_size);
    println!("   • Entry count: {}", stats.entry_count);
    println!("   • Table count: {}", stats.table_count);
    println!("   • Block count: {}", stats.block_count);
    println!("   • Index size: {} bytes", stats.index_size);
    println!("   • Bloom filter size: {} bytes", stats.bloom_filter_size);
    println!("   • Compression ratio: {:.2}", stats.compression_ratio);

    // Test all primitive types we expect in the all_types table
    let table_id = TableId::new("all_types");
    let mut validation_results = Vec::new();

    // Test cases for different types
    let test_cases = vec![
        ("text_value", ValueType::Text),
        ("int_value", ValueType::Integer),
        ("bigint_value", ValueType::BigInt),
        ("float_value", ValueType::Float),
        ("double_value", ValueType::Float),
        ("boolean_value", ValueType::Boolean),
        ("timestamp_value", ValueType::Timestamp),
        ("uuid_value", ValueType::Uuid),
        ("blob_value", ValueType::Blob),
        ("decimal_value", ValueType::Text),
        ("inet_value", ValueType::Text),
        ("varint_value", ValueType::BigInt),
    ];

    println!("\n🔍 Testing primitive type parsing:");

    for (test_key, expected_type) in test_cases {
        let key = RowKey::from(test_key);
        
        match reader.get(&table_id, &key).await {
            Ok(Some(value)) => {
                let actual_type = get_value_type(&value);
                let matches = actual_type == expected_type;
                
                validation_results.push(ValidationResult {
                    key: test_key.to_string(),
                    expected_type: format!("{:?}", expected_type),
                    actual_type: format!("{:?}", actual_type),
                    value: format!("{:?}", value),
                    matches,
                });

                let status = if matches { "✅" } else { "❌" };
                println!("   {} {}: {} -> {:?}", status, test_key, format!("{:?}", expected_type), value);
            }
            Ok(None) => {
                println!("   ⚠️  {}: No value found", test_key);
                validation_results.push(ValidationResult {
                    key: test_key.to_string(),
                    expected_type: format!("{:?}", expected_type),
                    actual_type: "None".to_string(),
                    value: "None".to_string(),
                    matches: false,
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
                });
            }
        }
    }

    // Try to scan all entries to see what's actually in the table
    println!("\n🔎 Scanning all entries in table:");
    match reader.get_all_entries().await {
        Ok(entries) => {
            // Filter entries for our table_id
            let table_entries: Vec<_> = entries.iter()
                .filter(|(tid, _, _)| tid == &table_id)
                .collect();
            println!("   Found {} entries for table", table_entries.len());
            for (i, (tid, key, value)) in table_entries.iter().take(10).enumerate() {
                println!("   [{:2}] {:?} -> {:?}", i + 1, key, value);
            }
            if table_entries.len() > 10 {
                println!("   ... and {} more entries", table_entries.len() - 10);
            }
        }
        Err(e) => {
            println!("   ❌ Error scanning table: {}", e);
        }
    }

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
        println!("\n🎉 All primitive type validations passed!");
    } else {
        println!("\n⚠️  Some validations failed. Check validation report for details.");
    }

    Ok(())
}

#[derive(Debug, PartialEq)]
enum ValueType {
    Text,
    Integer,
    BigInt,
    Float,
    Boolean,
    Timestamp,
    Uuid,
    Blob,
    Unknown,
}

fn get_value_type(value: &Value) -> ValueType {
    match value {
        Value::Text(_) => ValueType::Text,
        Value::Integer(_) => ValueType::Integer,
        Value::BigInt(_) => ValueType::BigInt,
        Value::Float(_) => ValueType::Float,
        Value::Boolean(_) => ValueType::Boolean,
        Value::Timestamp(_) => ValueType::Timestamp,
        Value::Uuid(_) => ValueType::Uuid,
        Value::Blob(_) => ValueType::Blob,
        _ => ValueType::Unknown,
    }
}

#[derive(Debug)]
struct ValidationResult {
    key: String,
    expected_type: String,
    actual_type: String,
    value: String,
    matches: bool,
}

fn generate_validation_report(results: &[ValidationResult]) -> Result<()> {
    use std::fs::File;
    use std::io::Write;

    let mut report = File::create("validation_report_all_types.json")?;
    
    let json_report = serde_json::json!({
        "test_name": "all_types_validation",
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
                "matches": r.matches
            })
        }).collect::<Vec<_>>()
    });

    writeln!(report, "{}", serde_json::to_string_pretty(&json_report)?)?;
    println!("📄 Validation report saved to: validation_report_all_types.json");

    Ok(())
}