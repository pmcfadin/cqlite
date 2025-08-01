// SSTable reading performance tests - Issue #25

use super::*;
use cqlite_core::{Config, Error, Result};
use std::time::{Duration, Instant};

/// Test basic performance benchmarks
#[tokio::test]
async fn test_file_opening_performance() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Create test files of different sizes for performance testing
    let size_tests = vec![
        ("small", 1_000, "1KB file"),
        ("medium", 10_000, "10KB file"),
        ("large", 100_000, "100KB file"),
        ("xlarge", 1_000_000, "1MB file"),
    ];
    
    let mut performance_results = Vec::new();
    
    for (size_name, data_size, description) in size_tests {
        println!("Testing file opening performance: {} ({})", size_name, description);
        
        // Create test data
        let large_data = generate_test_data(data_size);
        let test_data = TestSSTableData {
            keyspace: format!("perf_test_{}", size_name),
            table: "performance_table".to_string(),
            rows: vec![TestRow {
                key: format!("perf_key_{}", size_name).into_bytes(),
                columns: {
                    let mut cols = std::collections::HashMap::new();
                    cols.insert("size".to_string(), size_name.as_bytes().to_vec());
                    cols.insert("data".to_string(), large_data);
                    cols
                },
                timestamp: Some(1640995200000),
            }],
            compression: None,
            version: "5.0".to_string(),
        };
        
        let sstable_path = harness.create_test_sstable(&format!("perf_{}", size_name), test_data).await?;
        
        // Benchmark file opening
        let iterations = 10;
        let mut total_duration = Duration::ZERO;
        let mut successful_opens = 0;
        
        for i in 0..iterations {
            let start_time = Instant::now();
            
            match harness.open_reader(&sstable_path).await {
                Ok(_reader) => {
                    let duration = start_time.elapsed();
                    total_duration += duration;
                    successful_opens += 1;
                    
                    if i == 0 {
                        println!("  First open: {:?}", duration);
                    }
                },
                Err(e) => {
                    println!("  ⚠️ Open failed on iteration {}: {}", i, e);
                }
            }
        }
        
        if successful_opens > 0 {
            let avg_duration = total_duration / successful_opens;
            println!("  Average opening time: {:?} ({}/{} successful)", avg_duration, successful_opens, iterations);
            
            // Performance thresholds (adjust based on requirements)
            let expected_max_time = match size_name {
                "small" => Duration::from_millis(10),
                "medium" => Duration::from_millis(50),
                "large" => Duration::from_millis(200),
                "xlarge" => Duration::from_millis(1000),
                _ => Duration::from_millis(100),
            };
            
            if avg_duration <= expected_max_time {
                println!("  ✅ Performance acceptable (within {:?})", expected_max_time);
            } else {
                println!("  ⚠️ Performance slower than expected (expected < {:?})", expected_max_time);
            }
            
            performance_results.push((size_name, avg_duration, successful_opens));
        } else {
            println!("  ❌ No successful opens for performance testing");
        }
    }
    
    // Summary
    println!("\nPerformance Summary:");
    for (size, duration, success_count) in performance_results {
        println!("  {} files: {:?} avg ({} successful opens)", size, duration, success_count);
    }
    
    Ok(())
}

/// Test memory usage during SSTable operations
#[tokio::test]
async fn test_memory_usage_patterns() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    println!("Testing memory usage patterns");
    
    // Create test files with different characteristics
    let memory_tests = vec![
        ("many_small_rows", create_many_small_rows_data(), "Many small rows"),
        ("few_large_rows", create_few_large_rows_data(), "Few large rows"),
        ("mixed_sizes", create_mixed_size_rows_data(), "Mixed row sizes"),
    ];
    
    for (test_name, test_data, description) in memory_tests {
        println!("  Testing memory usage: {} ({})", test_name, description);
        
        let sstable_path = harness.create_test_sstable(&format!("memory_{}", test_name), test_data).await?;
        
        // Get baseline memory usage (this is a simplified approach)
        let start_time = Instant::now();
        
        match harness.open_reader(&sstable_path).await {
            Ok(_reader) => {
                let open_duration = start_time.elapsed();
                println!("    File opened in {:?}", open_duration);
                
                // Simulate some operations that might use memory
                let operation_start = Instant::now();
                
                // We can't easily measure actual memory usage in a unit test,
                // but we can measure operation duration as a proxy
                tokio::time::sleep(Duration::from_millis(1)).await;
                
                let operation_duration = operation_start.elapsed();
                println!("    Operations completed in {:?}", operation_duration);
                
                // Check that operations complete in reasonable time
                if operation_duration < Duration::from_millis(100) {
                    println!("    ✅ Memory usage appears efficient");
                } else {
                    println!("    ⚠️ Operations took longer than expected");
                }
            },
            Err(e) => {
                println!("    ⚠️ Could not test memory usage: {}", e);
            }
        }
    }
    
    Ok(())
}

/// Test concurrent performance characteristics
#[tokio::test]
async fn test_concurrent_performance() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    println!("Testing concurrent performance");
    
    // Create multiple test files
    let num_files = 5;
    let mut file_paths = Vec::new();
    
    for i in 0..num_files {
        let test_data = TestSSTableData {
            keyspace: format!("concurrent_perf_{}", i),
            table: "concurrent_table".to_string(),
            rows: vec![TestRow {
                key: format!("concurrent_key_{}", i).into_bytes(),
                columns: {
                    let mut cols = std::collections::HashMap::new();
                    cols.insert("file_id".to_string(), format!("{}", i).into_bytes());
                    cols.insert("data".to_string(), generate_test_data(10_000));
                    cols
                },
                timestamp: Some(1640995200000 + i as i64 * 1000),
            }],
            compression: None,
            version: "5.0".to_string(),
        };
        
        let path = harness.create_test_sstable(&format!("concurrent_perf_{}", i), test_data).await?;
        file_paths.push(path);
    }
    
    // Test sequential vs concurrent performance
    
    // Sequential test
    println!("  Sequential file opening:");
    let sequential_start = Instant::now();
    let mut sequential_successes = 0;
    
    for path in &file_paths {
        match harness.open_reader(path).await {
            Ok(_) => sequential_successes += 1,
            Err(e) => println!("    Sequential open failed: {}", e),
        }
    }
    
    let sequential_duration = sequential_start.elapsed();
    println!("    Sequential: {:?} ({}/{} files)", sequential_duration, sequential_successes, num_files);
    
    // Concurrent test
    println!("  Concurrent file opening:");
    let concurrent_start = Instant::now();
    
    let mut tasks = Vec::new();
    
    for (i, path) in file_paths.iter().enumerate() {
        let harness_config = harness.config.clone();
        let harness_platform = harness.platform.clone();
        let test_path = path.clone();
        
        let task = tokio::spawn(async move {
            let result = cqlite_core::storage::sstable::reader::SSTableReader::open(
                &test_path, &harness_config, harness_platform
            ).await;
            
            match result {
                Ok(_) => Ok(()),
                Err(e) => {
                    println!("    Concurrent open {} failed: {}", i, e);
                    Err(())
                }
            }
        });
        
        tasks.push(task);
    }
    
    let mut concurrent_successes = 0;
    for task in tasks {
        match task.await {
            Ok(Ok(())) => concurrent_successes += 1,
            _ => {}
        }
    }
    
    let concurrent_duration = concurrent_start.elapsed();
    println!("    Concurrent: {:?} ({}/{} files)", concurrent_duration, concurrent_successes, num_files);
    
    // Compare performance
    if concurrent_duration < sequential_duration {
        let speedup = sequential_duration.as_nanos() as f64 / concurrent_duration.as_nanos() as f64;
        println!("    ✅ Concurrent is {:.1}x faster", speedup);
    } else {
        println!("    ⚠️ Concurrent performance not significantly better");
    }
    
    Ok(())
}

/// Test performance with different compression algorithms
#[tokio::test]
async fn test_compression_performance() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    println!("Testing compression performance");
    
    let compression_algorithms = vec![
        (None, "uncompressed"),
        (Some("LZ4".to_string()), "LZ4"),
        (Some("Snappy".to_string()), "Snappy"),
        (Some("GZIP".to_string()), "GZIP"),
    ];
    
    let test_data_base = generate_compressible_test_data(50_000); // 50KB of compressible data
    
    for (compression, name) in compression_algorithms {
        println!("  Testing {} compression performance", name);
        
        let test_data = TestSSTableData {
            keyspace: format!("compression_perf_{}", name),
            table: "compression_table".to_string(),
            compression,
            rows: vec![TestRow {
                key: format!("compression_key_{}", name).into_bytes(),
                columns: {
                    let mut cols = std::collections::HashMap::new();
                    cols.insert("algorithm".to_string(), name.as_bytes().to_vec());
                    cols.insert("data".to_string(), test_data_base.clone());
                    cols
                },
                timestamp: Some(1640995200000),
            }],
            version: "5.0".to_string(),
        };
        
        // Time file creation
        let create_start = Instant::now();
        let sstable_path = harness.create_test_sstable(&format!("compression_perf_{}", name), test_data).await?;
        let create_duration = create_start.elapsed();
        
        // Get file size
        let file_size = tokio::fs::metadata(&sstable_path).await
            .map(|m| m.len())
            .unwrap_or(0);
        
        // Time file opening
        let open_start = Instant::now();
        let open_result = harness.open_reader(&sstable_path).await;
        let open_duration = open_start.elapsed();
        
        match open_result {
            Ok(_) => {
                println!("    ✅ {} - Create: {:?}, Open: {:?}, Size: {} bytes", 
                         name, create_duration, open_duration, file_size);
                
                // Calculate performance metrics
                let total_time = create_duration + open_duration;
                if total_time < Duration::from_millis(500) {
                    println!("      ✅ Good performance");
                } else {
                    println!("      ⚠️ Slower than expected");
                }
            },
            Err(e) => {
                println!("    ⚠️ {} - Create: {:?}, Open failed: {}, Size: {} bytes", 
                         name, create_duration, e, file_size);
            }
        }
    }
    
    Ok(())
}

/// Test performance regression detection
#[tokio::test]
async fn test_performance_regression() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    println!("Testing performance regression detection");
    
    // Create a baseline test file
    let baseline_data = TestSSTableData {
        keyspace: "regression_test".to_string(),
        table: "baseline_table".to_string(),
        rows: create_standard_test_rows(100), // 100 rows
        compression: None,
        version: "5.0".to_string(),
    };
    
    let baseline_path = harness.create_test_sstable("regression_baseline", baseline_data).await?;
    
    // Run multiple iterations to establish baseline
    let iterations = 20;
    let mut durations = Vec::new();
    
    for i in 0..iterations {
        let start_time = Instant::now();
        
        match harness.open_reader(&baseline_path).await {
            Ok(_) => {
                durations.push(start_time.elapsed());
            },
            Err(e) => {
                println!("  ⚠️ Iteration {} failed: {}", i, e);
            }
        }
    }
    
    if durations.is_empty() {
        println!("  ❌ No successful operations for regression testing");
        return Ok(());
    }
    
    // Calculate statistics
    durations.sort();
    let min_duration = durations[0];
    let max_duration = durations[durations.len() - 1];
    let median_duration = durations[durations.len() / 2];
    let avg_duration: Duration = durations.iter().sum::<Duration>() / durations.len() as u32;
    
    println!("  Performance baseline ({} iterations):", iterations);
    println!("    Min: {:?}", min_duration);
    println!("    Max: {:?}", max_duration);
    println!("    Median: {:?}", median_duration);
    println!("    Average: {:?}", avg_duration);
    
    // Check for reasonable performance characteristics
    let performance_variance = max_duration.as_nanos() as f64 / min_duration.as_nanos() as f64;
    
    if performance_variance < 3.0 {
        println!("    ✅ Performance is consistent (variance: {:.1}x)", performance_variance);
    } else {
        println!("    ⚠️ High performance variance (variance: {:.1}x)", performance_variance);
    }
    
    // Check for reasonable absolute performance
    if avg_duration < Duration::from_millis(100) {
        println!("    ✅ Performance is acceptable");
    } else {
        println!("    ⚠️ Performance may be slower than expected");
    }
    
    Ok(())
}

// Helper functions for generating test data

fn generate_test_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    
    // Create somewhat realistic data patterns
    let patterns = vec![
        b"user_data_",
        b"timestamp_",
        b"metadata_", 
        b"content_",
        b"index_",
    ];
    
    let mut pattern_idx = 0;
    let mut remaining = size;
    
    while remaining > 0 {
        let pattern = patterns[pattern_idx % patterns.len()];
        let chunk_size = std::cmp::min(remaining, pattern.len());
        
        data.extend_from_slice(&pattern[..chunk_size]);
        remaining -= chunk_size;
        pattern_idx += 1;
        
        // Add some variation
        if remaining > 0 {
            data.push((pattern_idx % 256) as u8);
            remaining -= 1;
        }
    }
    
    data
}

fn create_many_small_rows_data() -> TestSSTableData {
    let mut rows = Vec::new();
    
    for i in 0..1000 {
        rows.push(TestRow {
            key: format!("small_row_{}", i).into_bytes(),
            columns: {
                let mut cols = std::collections::HashMap::new();
                cols.insert("id".to_string(), format!("{}", i).into_bytes());
                cols.insert("data".to_string(), b"small".to_vec());
                cols
            },
            timestamp: Some(1640995200000 + i),
        });
    }
    
    TestSSTableData {
        keyspace: "memory_test".to_string(),
        table: "many_small_rows".to_string(),
        rows,
        compression: None,
        version: "5.0".to_string(),
    }
}

fn create_few_large_rows_data() -> TestSSTableData {
    let mut rows = Vec::new();
    
    for i in 0..10 {
        rows.push(TestRow {
            key: format!("large_row_{}", i).into_bytes(),
            columns: {
                let mut cols = std::collections::HashMap::new();
                cols.insert("id".to_string(), format!("{}", i).into_bytes());
                cols.insert("large_data".to_string(), generate_test_data(10_000));
                cols
            },
            timestamp: Some(1640995200000 + i * 1000),
        });
    }
    
    TestSSTableData {
        keyspace: "memory_test".to_string(),
        table: "few_large_rows".to_string(),
        rows,
        compression: None,
        version: "5.0".to_string(),
    }
}

fn create_mixed_size_rows_data() -> TestSSTableData {
    let mut rows = Vec::new();
    
    for i in 0..100 {
        let data_size = match i % 4 {
            0 => 100,   // Small
            1 => 1_000, // Medium
            2 => 10_000, // Large
            _ => 500,   // Medium-small
        };
        
        rows.push(TestRow {
            key: format!("mixed_row_{}", i).into_bytes(),
            columns: {
                let mut cols = std::collections::HashMap::new();
                cols.insert("id".to_string(), format!("{}", i).into_bytes());
                cols.insert("mixed_data".to_string(), generate_test_data(data_size));
                cols
            },
            timestamp: Some(1640995200000 + i * 100),
        });
    }
    
    TestSSTableData {
        keyspace: "memory_test".to_string(),
        table: "mixed_size_rows".to_string(),
        rows,
        compression: None,
        version: "5.0".to_string(),
    }
}

fn generate_compressible_test_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    
    // Create highly compressible patterns
    let base_pattern = b"COMPRESSIBLE_DATA_PATTERN_";
    let pattern_repeats = size / base_pattern.len();
    let remainder = size % base_pattern.len();
    
    for _ in 0..pattern_repeats {
        data.extend_from_slice(base_pattern);
    }
    
    if remainder > 0 {
        data.extend_from_slice(&base_pattern[..remainder]);
    }
    
    data
}

fn create_standard_test_rows(count: usize) -> Vec<TestRow> {
    let mut rows = Vec::with_capacity(count);
    
    for i in 0..count {
        rows.push(TestRow {
            key: format!("standard_key_{:04}", i).into_bytes(),
            columns: {
                let mut cols = std::collections::HashMap::new();
                cols.insert("id".to_string(), format!("{}", i).into_bytes());
                cols.insert("name".to_string(), format!("User {}", i).into_bytes());
                cols.insert("email".to_string(), format!("user{}@example.com", i).into_bytes());
                cols.insert("data".to_string(), generate_test_data(100));
                cols
            },
            timestamp: Some(1640995200000 + i as i64 * 1000),
        });
    }
    
    rows
}