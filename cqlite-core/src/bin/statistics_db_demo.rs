//! Statistics.db analysis demo for CQLite
//!
//! This binary demonstrates the enhanced Statistics.db parsing capabilities
//! for real Cassandra 5.0 'nb' format files.

use cqlite_core::{
    parser::enhanced_statistics_parser::parse_enhanced_statistics_file,
    parser::statistics::StatisticsAnalyzer,
    Config, Platform,
};
use std::env;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        println!("Usage: {} <Statistics.db file path>", args[0]);
        println!("\nExample Statistics.db files in test environment:");
        println!("  test-env/cassandra5/sstables/users-*/nb-1-big-Statistics.db");
        println!("  test-env/cassandra5/sstables/all_types-*/nb-1-big-Statistics.db");
        println!("  test-env/cassandra5/sstables/collections_table-*/nb-1-big-Statistics.db");
        return Ok(());
    }

    let file_path = &args[1];
    let path = Path::new(file_path);
    
    if !path.exists() {
        eprintln!("❌ File not found: {}", file_path);
        return Ok(());
    }

    println!("🔍 Analyzing Statistics.db file: {}", file_path);
    println!("📊 File: {}", path.file_name().unwrap().to_string_lossy());
    
    // Read the file
    let file_data = fs::read(path).await?;
    println!("📏 Size: {} bytes", file_data.len());
    
    // Show hex dump of first 64 bytes for analysis
    println!("\n🔍 Binary Header Analysis:");
    for (i, chunk) in file_data[..64.min(file_data.len())].chunks(16).enumerate() {
        print!("  {:04x}: ", i * 16);
        for byte in chunk {
            print!("{:02x} ", byte);
        }
        // Print ASCII representation
        print!(" |");
        for byte in chunk {
            if *byte >= 32 && *byte <= 126 {
                print!("{}", *byte as char);
            } else {
                print!(".");
            }
        }
        println!("|");
    }
    
    // Parse using enhanced parser
    match parse_enhanced_statistics_file(&file_data) {
        Ok((_, statistics)) => {
            println!("\n✅ Successfully parsed with enhanced parser!");
            
            // Display header information
            println!("\n📋 Header Information:");
            println!("  Version: {} ({})", 
                statistics.header.version,
                if statistics.header.version == 4 { "Cassandra 5.0 'nb' format" } else { "Other format" }
            );
            println!("  Statistics Kind: 0x{:08X}", statistics.header.statistics_kind);
            println!("  Data Length: {} bytes", statistics.header.data_length);
            println!("  Metadata: [{}, {}, {}]", 
                statistics.header.metadata1, 
                statistics.header.metadata2, 
                statistics.header.metadata3
            );
            println!("  Checksum: 0x{:08X}", statistics.header.checksum);
            
            if let Some(table_id) = statistics.header.table_id {
                println!("  Table ID: {}", 
                    table_id.iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(""));
            }
            
            // Display statistics
            println!("\n📊 Row Statistics:");
            println!("  Total Rows: {}", statistics.row_stats.total_rows);
            println!("  Live Rows: {} ({:.1}%)", 
                statistics.row_stats.live_rows,
                if statistics.row_stats.total_rows > 0 {
                    (statistics.row_stats.live_rows as f64 / statistics.row_stats.total_rows as f64) * 100.0
                } else { 0.0 }
            );
            println!("  Tombstones: {}", statistics.row_stats.tombstone_count);
            println!("  Partitions: {}", statistics.row_stats.partition_count);
            println!("  Avg Rows/Partition: {:.1}", statistics.row_stats.avg_rows_per_partition);
            
            println!("\n💾 Table Statistics:");
            println!("  Disk Size: {:.2} KB", statistics.table_stats.disk_size as f64 / 1024.0);
            println!("  Uncompressed Size: {:.2} KB", statistics.table_stats.uncompressed_size as f64 / 1024.0);
            println!("  Compression Ratio: {:.1}%", statistics.table_stats.compression_ratio * 100.0);
            println!("  Block Count: {}", statistics.table_stats.block_count);
            println!("  Index Size: {:.2} KB", statistics.table_stats.index_size as f64 / 1024.0);
            println!("  Bloom Filter Size: {:.2} KB", statistics.table_stats.bloom_filter_size as f64 / 1024.0);
            
            println!("\n🗂️ Partition Statistics:");
            println!("  Avg Partition Size: {:.1} bytes", statistics.partition_stats.avg_partition_size);
            println!("  Min Partition Size: {} bytes", statistics.partition_stats.min_partition_size);
            println!("  Max Partition Size: {:.1} KB", statistics.partition_stats.max_partition_size as f64 / 1024.0);
            println!("  Large Partitions: {:.1}%", statistics.partition_stats.large_partition_percentage);
            
            println!("\n🗜️ Compression Statistics:");
            println!("  Algorithm: {}", statistics.compression_stats.algorithm);
            println!("  Original Size: {:.2} KB", statistics.compression_stats.original_size as f64 / 1024.0);
            println!("  Compressed Size: {:.2} KB", statistics.compression_stats.compressed_size as f64 / 1024.0);
            println!("  Compression Speed: {:.1} MB/s", statistics.compression_stats.compression_speed);
            println!("  Decompression Speed: {:.1} MB/s", statistics.compression_stats.decompression_speed);
            
            // Analyze health and provide insights
            let analysis = StatisticsAnalyzer::analyze(&statistics);
            println!("\n🎯 Analysis Summary:");
            println!("  Health Score: {:.1}/100", analysis.health_score);
            println!("  Data Efficiency: {:.1}%", analysis.data_efficiency);
            println!("  Live Data: {:.1}%", analysis.live_data_percentage);
            println!("  Compression Efficiency: {:.1}%", analysis.compression_efficiency);
            println!("  Timestamp Range: {:.1} days", analysis.timestamp_range_days);
            
            // Status assessment
            if analysis.health_score >= 90.0 {
                println!("  Status: 🎯 Excellent");
            } else if analysis.health_score >= 70.0 {
                println!("  Status: ✅ Good");
            } else if analysis.health_score >= 50.0 {
                println!("  Status: ⚠️  Needs attention");
            } else {
                println!("  Status: ❌ Poor - requires immediate attention");
            }
            
            // Performance hints
            if !analysis.query_performance_hints.is_empty() {
                println!("\n💡 Performance Hints:");
                for hint in &analysis.query_performance_hints {
                    println!("  • {}", hint);
                }
            }
            
            // Storage recommendations
            if !analysis.storage_recommendations.is_empty() {
                println!("\n📝 Storage Recommendations:");
                for rec in &analysis.storage_recommendations {
                    println!("  • {}", rec);
                }
            }
            
            // Metadata
            if !statistics.metadata.is_empty() {
                println!("\n🏷️ Metadata:");
                for (key, value) in &statistics.metadata {
                    println!("  {}: {}", key, value);
                }
            }
            
            println!("\n✅ Analysis complete!");
            
        }
        Err(e) => {
            println!("\n❌ Failed to parse Statistics.db file: {:?}", e);
            println!("This might be a format not yet supported by the enhanced parser.");
        }
    }
    
    Ok(())
}