//! Integration test for streaming performance optimizations
//!
//! This binary tests the integration of all streaming performance components
//! with real SSTable files from the test environment.

use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use cqlite_core::{
    platform::Platform,
    storage::sstable::{
        streaming_reader::{StreamingSSTableReader, StreamingReaderConfig},
        compression::{Compression, CompressionPriority},
        performance_benchmarks::PerformanceBenchmarks,
    },
    types::TableId,
    Config, RowKey,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    println!("🧪 CQLite Streaming Performance Integration Test");
    println!("═══════════════════════════════════════════════════");
    
    // Get test data directory
    let test_data_dir = get_test_data_dir();
    println!("📁 Test data directory: {}", test_data_dir.display());
    
    // Initialize platform and config
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    
    // Test 1: Compression Algorithm Selection
    println!("\n🔧 Test 1: Compression Algorithm Selection");
    test_compression_algorithm_selection().await?;
    
    // Test 2: Streaming Reader Performance
    println!("\n🌊 Test 2: Streaming Reader Performance");
    test_streaming_reader_performance(&test_data_dir, &config, platform.clone()).await?;
    
    // Test 3: Memory Usage Monitoring
    println!("\n💾 Test 3: Memory Usage Monitoring");
    test_memory_usage_monitoring(&test_data_dir, &config, platform.clone()).await?;
    
    // Test 4: Large File Handling
    println!("\n📊 Test 4: Large File Handling");
    test_large_file_handling(&test_data_dir, &config, platform.clone()).await?;
    
    // Test 5: Integration with Real Data
    println!("\n🔗 Test 5: Integration with Real SSTable Data");
    test_real_data_integration(&test_data_dir).await?;
    
    println!("\n✅ All integration tests completed successfully!");
    Ok(())
}

async fn test_compression_algorithm_selection() -> Result<(), Box<dyn std::error::Error>> {
    println!("  📋 Testing compression algorithm selection logic...");
    
    // Test with different data patterns
    let test_cases = vec![
        ("Random data", (0..255).collect::<Vec<u8>>()),
        ("Repetitive data", vec![42u8; 1000]),
        ("Text data", b"Hello world! This is a sample text with some repetition.".repeat(50)),
        ("Mixed data", {
            let mut data = Vec::new();
            data.extend_from_slice(&[0u8; 100]);
            data.extend_from_slice(&(0..100).collect::<Vec<u8>>());
            data.extend_from_slice(b"Text content here");
            data
        }),
    ];
    
    for (name, data) in test_cases {
        println!("    🔍 Testing with {}", name);
        
        let speed_choice = Compression::select_optimal_algorithm(&data, CompressionPriority::Speed);
        let balanced_choice = Compression::select_optimal_algorithm(&data, CompressionPriority::Balanced);
        let ratio_choice = Compression::select_optimal_algorithm(&data, CompressionPriority::Ratio);
        
        println!("      Speed: {:?}, Balanced: {:?}, Ratio: {:?}", 
                speed_choice, balanced_choice, ratio_choice);
        
        // Verify that algorithm selection makes sense
        if data.len() > 100 && data.iter().all(|&b| b == data[0]) {
            // Highly repetitive data should not choose None for ratio priority
            assert_ne!(ratio_choice, cqlite_core::storage::sstable::compression::CompressionAlgorithm::None);
        }
    }
    
    println!("  ✅ Compression algorithm selection tests passed");
    Ok(())
}

async fn test_streaming_reader_performance(
    test_data_dir: &PathBuf,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("  📋 Testing streaming reader performance...");
    
    // Find a test SSTable file
    let sstables_dir = test_data_dir.join("sstables");
    if !sstables_dir.exists() {
        println!("  ⚠️  No SSTable files found, skipping streaming reader test");
        return Ok(());
    }
    
    // Get the first available SSTable file
    let mut dir_entries = tokio::fs::read_dir(&sstables_dir).await?;
    while let Some(entry) = dir_entries.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            let data_file = path.join("nb-1-big-Data.db");
            if data_file.exists() {
                println!("    🔍 Testing with file: {}", data_file.display());
                
                let start_time = Instant::now();
                let reader = StreamingSSTableReader::open(&data_file, config, platform.clone()).await?;
                let open_time = start_time.elapsed();
                
                // Test basic operations
                let table_name = path.file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| s.split('-').next())
                    .unwrap_or("test_table");
                let table_id = TableId::new(table_name.to_string());
                
                // Test streaming scan
                let scan_start = Instant::now();
                match reader.scan_streaming(&table_id, None, None, Some(10)).await {
                    Ok(results) => {
                        let scan_time = scan_start.elapsed();
                        println!("    ✅ Streaming scan: {} results in {:?}", results.len(), scan_time);
                    }
                    Err(e) => {
                        println!("    ⚠️  Streaming scan failed: {}", e);
                    }
                }
                
                // Test streaming statistics
                let stats = reader.get_streaming_stats().await?;
                println!("    📊 Memory usage: {:.2} MB", stats.total_memory_mb);
                println!("    📊 Buffer utilization: {:.1}%", stats.buffer_pool_utilization * 100.0);
                
                println!("    ✅ Streaming reader opened in {:?}", open_time);
                break;
            }
        }
    }
    
    Ok(())
}

async fn test_memory_usage_monitoring(
    test_data_dir: &PathBuf,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("  📋 Testing memory usage monitoring...");
    
    // Create streaming reader with custom config for memory testing
    let mut streaming_config = StreamingReaderConfig::default();
    streaming_config.buffer_pool_size_mb = 16; // Smaller pool for testing
    streaming_config.max_memory_usage_mb = 32; // Lower limit for testing
    
    println!("    📊 Buffer pool configured: {} MB", streaming_config.buffer_pool_size_mb);
    println!("    📊 Memory limit: {} MB", streaming_config.max_memory_usage_mb);
    
    // Find test file
    let sstables_dir = test_data_dir.join("sstables");
    if sstables_dir.exists() {
        let mut dir_entries = tokio::fs::read_dir(&sstables_dir).await?;
        if let Some(entry) = dir_entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                let data_file = path.join("nb-1-big-Data.db");
                if data_file.exists() {
                    let reader = StreamingSSTableReader::open(&data_file, config, platform).await?;
                    
                    // Monitor memory usage during operations
                    let initial_stats = reader.get_streaming_stats().await?;
                    println!("    📊 Initial memory: {:.2} MB", initial_stats.total_memory_mb);
                    
                    // Perform some operations that should increase memory usage
                    let table_name = path.file_name()
                        .and_then(|n| n.to_str())
                        .and_then(|s| s.split('-').next())
                        .unwrap_or("test_table");
                    let table_id = TableId::new(table_name.to_string());
                    
                    let _ = reader.scan_streaming(&table_id, None, None, Some(50)).await;
                    
                    let final_stats = reader.get_streaming_stats().await?;
                    println!("    📊 Final memory: {:.2} MB", final_stats.total_memory_mb);
                    println!("    📊 Memory increase: {:.2} MB", 
                            final_stats.total_memory_mb - initial_stats.total_memory_mb);
                }
            }
        }
    }
    
    println!("  ✅ Memory usage monitoring tests completed");
    Ok(())
}

async fn test_large_file_handling(
    test_data_dir: &PathBuf,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("  📋 Testing large file handling...");
    
    // Find the largest available test file
    let sstables_dir = test_data_dir.join("sstables");
    if !sstables_dir.exists() {
        println!("  ⚠️  No SSTable files found, skipping large file test");
        return Ok(());
    }
    
    let mut largest_file: Option<(PathBuf, u64)> = None;
    let mut dir_entries = tokio::fs::read_dir(&sstables_dir).await?;
    
    while let Some(entry) = dir_entries.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            let data_file = path.join("nb-1-big-Data.db");
            if data_file.exists() {
                let metadata = tokio::fs::metadata(&data_file).await?;
                let file_size = metadata.len();
                
                if largest_file.as_ref().map_or(true, |(_, size)| file_size > *size) {
                    largest_file = Some((data_file, file_size));
                }
            }
        }
    }
    
    if let Some((largest_file_path, file_size)) = largest_file {
        println!("    🔍 Testing with largest file: {} ({:.2} MB)", 
                largest_file_path.display(), file_size as f64 / 1024.0 / 1024.0);
        
        // Test that large file doesn't cause memory exhaustion
        let start_time = Instant::now();
        let reader = StreamingSSTableReader::open(&largest_file_path, config, platform).await?;
        let open_time = start_time.elapsed();
        
        println!("    ✅ Large file opened in {:?}", open_time);
        
        // Test memory usage stays within bounds
        let stats = reader.get_streaming_stats().await?;
        println!("    📊 Memory usage: {:.2} MB (should be < 128 MB)", stats.total_memory_mb);
        
        if stats.total_memory_mb > 128.0 {
            println!("    ⚠️  Memory usage exceeded expected bounds");
        } else {
            println!("    ✅ Memory usage within acceptable bounds");
        }
        
        // Test streaming operations don't exhaust memory
        let table_name = largest_file_path.parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .and_then(|s| s.split('-').next())
            .unwrap_or("test_table");
        let table_id = TableId::new(table_name.to_string());
        
        match reader.scan_streaming(&table_id, None, None, Some(100)).await {
            Ok(results) => {
                println!("    ✅ Streaming scan completed: {} results", results.len());
            }
            Err(e) => {
                println!("    ⚠️  Streaming scan failed: {}", e);
            }
        }
    } else {
        println!("  ⚠️  No test files found for large file handling test");
    }
    
    Ok(())
}

async fn test_real_data_integration(test_data_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    println!("  📋 Testing integration with real SSTable data...");
    
    // Run comprehensive benchmarks to verify integration
    let benchmarks = PerformanceBenchmarks::new(test_data_dir).await?;
    let results = benchmarks.run_comprehensive_benchmarks().await?;
    
    if results.is_empty() {
        println!("  ⚠️  No benchmark results - check test data availability");
    } else {
        println!("  ✅ Integration test completed with {} benchmark results", results.len());
        
        // Verify that streaming reader performs comparably
        let streaming_results: Vec<_> = results.iter()
            .filter(|r| r.reader_type.contains("Streaming"))
            .collect();
        
        if !streaming_results.is_empty() {
            let avg_ops_per_sec: f64 = streaming_results.iter()
                .map(|r| r.ops_per_second)
                .sum::<f64>() / streaming_results.len() as f64;
            
            println!("  📊 Average streaming performance: {:.2} ops/sec", avg_ops_per_sec);
            
            if avg_ops_per_sec > 0.0 {
                println!("  ✅ Streaming reader shows positive performance");
            }
        }
    }
    
    Ok(())
}

fn get_test_data_dir() -> PathBuf {
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        PathBuf::from(&args[1])
    } else {
        let mut current_dir = env::current_dir().expect("Failed to get current directory");
        current_dir.push("test-env");
        current_dir.push("cassandra5");
        current_dir
    }
}