//! Validation program for time_series SSTable
//! Tests parsing of tables with clustering columns and time-based data

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
    println!("🧪 Time Series SSTable Validation");
    println!("==================================");

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    // Path to the time_series SSTable
    let sstable_path = Path::new("../sstables").join("time_series-86ca4040669411f0acab47cdf782cef5");
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

    // Test time series fields we expect
    let table_id = TableId::new("time_series");
    let mut validation_results = Vec::new();

    // Test cases for time series fields - partition key + clustering columns
    let test_cases = vec![
        ("sensor_id", FieldType::PartitionKey),
        ("timestamp", FieldType::ClusteringColumn),
        ("value", FieldType::RegularColumn),
        ("metric_type", FieldType::RegularColumn),
        ("metadata", FieldType::RegularColumn),
    ];

    println!("\n🔍 Testing time series column parsing:");

    for (test_key, expected_type) in test_cases {
        let key = RowKey::from(test_key);
        
        match reader.get(&table_id, &key).await {
            Ok(Some(value)) => {
                let actual_type = get_column_type(&value);
                let matches = actual_type == expected_type;
                
                let time_analysis = if is_time_related(&value) {
                    analyze_time_value(&value)
                } else {
                    None
                };
                
                validation_results.push(ValidationResult {
                    key: test_key.to_string(),
                    expected_type: format!("{:?}", expected_type),
                    actual_type: format!("{:?}", actual_type),
                    value: format!("{:?}", value),
                    matches,
                    time_details: time_analysis,
                });

                let status = if matches { "✅" } else { "❌" };
                println!("   {} {}: {} -> {:?}", status, test_key, format!("{:?}", expected_type), value);
                
                // Show time-based analysis
                if let Some(details) = &time_analysis {
                    println!("      └─ Time analysis: {}", details);
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
                    time_details: None,
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
                    time_details: None,
                });
            }
        }
    }

    // Scan all entries to analyze time series patterns
    println!("\n🔎 Scanning time series entries:");
    match reader.scan_table(&table_id).await {
        Ok(entries) => {
            println!("   Found {} entries total", entries.len());
            
            // Group entries by timestamp for analysis
            let mut time_distribution = std::collections::HashMap::new();
            let mut sensor_distribution = std::collections::HashMap::new();
            
            for (key, value) in &entries {
                // Try to extract time and sensor info from the data
                if let Some(time_info) = extract_time_info(key, value) {
                    *time_distribution.entry(time_info.0).or_insert(0) += 1;
                    *sensor_distribution.entry(time_info.1).or_insert(0) += 1;
                }
            }
            
            // Show sample entries
            for (i, (key, value)) in entries.iter().take(5).enumerate() {
                println!("   [{:2}] {:?} -> {:?}", i + 1, key, value);
                if let Some(time_details) = analyze_time_value(value) {
                    println!("        └─ {}", time_details);
                }
            }
            
            if entries.len() > 5 {
                println!("   ... and {} more entries", entries.len() - 5);
            }
            
            // Show distribution analysis
            println!("\n📊 Time series distribution analysis:");
            println!("   • Unique time periods: {}", time_distribution.len());
            println!("   • Unique sensors: {}", sensor_distribution.len());
            
            if !time_distribution.is_empty() {
                let avg_entries_per_time = entries.len() as f64 / time_distribution.len() as f64;
                println!("   • Average entries per time period: {:.1}", avg_entries_per_time);
            }
            
        }
        Err(e) => {
            println!("   ❌ Error scanning table: {}", e);
        }
    }

    // Test clustering column ordering
    println!("\n🧪 Testing clustering column operations:");
    test_clustering_operations(&reader, &table_id).await?;

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
        println!("\n🎉 All time series validations passed!");
    } else {
        println!("\n⚠️  Some validations failed. Check validation report for details.");
    }

    Ok(())
}

#[derive(Debug, PartialEq)]
enum FieldType {
    PartitionKey,
    ClusteringColumn,
    RegularColumn,
    Unknown,
}

fn get_column_type(value: &Value) -> FieldType {
    // This is a heuristic - in a real implementation we'd need schema information
    match value {
        Value::Timestamp(_) => FieldType::ClusteringColumn,
        Value::Text(s) if s.starts_with("sensor_") => FieldType::PartitionKey,
        _ => FieldType::RegularColumn,
    }
}

fn is_time_related(value: &Value) -> bool {
    matches!(value, Value::Timestamp(_))
}

fn analyze_time_value(value: &Value) -> Option<String> {
    match value {
        Value::Timestamp(ts) => {
            // Convert timestamp to human readable format
            let datetime = chrono::DateTime::from_timestamp(*ts / 1000, ((*ts % 1000) * 1_000_000) as u32);
            if let Some(dt) = datetime {
                Some(format!("Timestamp: {} ({})", ts, dt.format("%Y-%m-%d %H:%M:%S UTC")))
            } else {
                Some(format!("Timestamp: {} (invalid)", ts))
            }
        }
        _ => None,
    }
}

fn extract_time_info(key: &RowKey, value: &Value) -> Option<(String, String)> {
    // Extract time period and sensor ID for distribution analysis
    let time_period = match value {
        Value::Timestamp(ts) => {
            let datetime = chrono::DateTime::from_timestamp(*ts / 1000, 0);
            if let Some(dt) = datetime {
                dt.format("%Y-%m-%d %H").to_string() // Group by hour
            } else {
                "unknown".to_string()
            }
        }
        _ => "non-time".to_string(),
    };
    
    let sensor_id = match key {
        RowKey::Single(bytes) => {
            String::from_utf8_lossy(bytes).to_string()
        }
        RowKey::Composite(parts) => {
            if !parts.is_empty() {
                String::from_utf8_lossy(&parts[0]).to_string()
            } else {
                "unknown".to_string()
            }
        }
    };
    
    Some((time_period, sensor_id))
}

async fn test_clustering_operations(reader: &SSTableReader, table_id: &TableId) -> Result<()> {
    println!("   🔍 Testing clustering column ordering...");
    
    // Test if entries are properly ordered by clustering columns
    match reader.scan_table(table_id).await {
        Ok(entries) => {
            let mut timestamps = Vec::new();
            
            for (_key, value) in &entries {
                if let Value::Timestamp(ts) = value {
                    timestamps.push(*ts);
                }
            }
            
            if timestamps.len() > 1 {
                let is_sorted_asc = timestamps.windows(2).all(|w| w[0] <= w[1]);
                let is_sorted_desc = timestamps.windows(2).all(|w| w[0] >= w[1]);
                
                if is_sorted_asc {
                    println!("      ✅ Timestamps are sorted in ascending order");
                } else if is_sorted_desc {
                    println!("      ✅ Timestamps are sorted in descending order");
                } else {
                    println!("      ⚠️  Timestamps are not consistently sorted");
                }
                
                println!("      📊 Time range: {} to {}", 
                    timestamps.iter().min().unwrap(),
                    timestamps.iter().max().unwrap());
            } else {
                println!("      ℹ️  Not enough timestamp data for ordering analysis");
            }
        }
        Err(e) => {
            println!("      ❌ Error during clustering operations test: {}", e);
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
    time_details: Option<String>,
}

fn generate_validation_report(results: &[ValidationResult]) -> Result<()> {
    use std::fs::File;
    use std::io::Write;

    let mut report = File::create("validation_report_time_series.json")?;
    
    let json_report = serde_json::json!({
        "test_name": "time_series_validation",
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
                "time_details": r.time_details
            })
        }).collect::<Vec<_>>()
    });

    writeln!(report, "{}", serde_json::to_string_pretty(&json_report)?)?;
    println!("📄 Validation report saved to: validation_report_time_series.json");

    Ok(())
}