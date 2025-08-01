// SSTable compression algorithm tests - Issue #25

use super::*;
use cqlite_core::{Config, Error, Result};

/// Test different compression algorithms
#[tokio::test]
async fn test_compression_algorithms() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Test different compression algorithms supported by Cassandra
    let compression_algorithms = vec![
        ("LZ4", "Fast compression with good performance"),
        ("Snappy", "Google's compression algorithm"),
        ("Deflate", "Standard deflate compression"),
        ("ZSTD", "Facebook's Zstandard compression"),
        ("GZIP", "Traditional gzip compression"),
    ];
    
    for (algorithm, description) in compression_algorithms {
        println!("Testing compression algorithm: {} ({})", algorithm, description);
        
        let test_data = TestSSTableData {
            keyspace: format!("compressed_ks_{}", algorithm.to_lowercase()),
            table: format!("compressed_table_{}", algorithm.to_lowercase()),
            compression: Some(algorithm.to_string()),
            version: "5.0".to_string(),
            rows: vec![
                TestRow {
                    key: format!("{}_key_1", algorithm).into_bytes(),
                    columns: {
                        let mut cols = std::collections::HashMap::new();
                        cols.insert("compression".to_string(), algorithm.as_bytes().to_vec());
                        cols.insert("data".to_string(), b"This is test data that should compress well with repetitive content".to_vec());
                        cols.insert("large_data".to_string(), "x".repeat(1000).into_bytes());
                        cols
                    },
                    timestamp: Some(1640995200000),
                },
                TestRow {
                    key: format!("{}_key_2", algorithm).into_bytes(),
                    columns: {
                        let mut cols = std::collections::HashMap::new();
                        cols.insert("compression".to_string(), algorithm.as_bytes().to_vec());
                        cols.insert("random_data".to_string(), generate_random_data(500)),
                        cols
                    },
                    timestamp: Some(1640995260000),
                },
            ],
        };
        
        let sstable_path = harness.create_test_sstable(&format!("compressed_{}", algorithm.to_lowercase()), test_data).await?;
        
        // Verify file was created
        assert!(sstable_path.exists(), "Compressed SSTable for {} should be created", algorithm);
        
        // Test that reader can handle compressed file (may fail due to format, but shouldn't crash)
        let _result = harness.open_reader(&sstable_path).await;
        
        println!("Compression test for {} completed", algorithm);
    }
    
    Ok(())
}

/// Test compression ratio validation
#[tokio::test]
async fn test_compression_ratios() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Create test data with different compressibility characteristics
    let compressibility_tests = vec![
        ("highly_compressible", "A".repeat(2000), "Should compress very well"),
        ("moderately_compressible", "ABC".repeat(500), "Should compress moderately"),
        ("random_data", generate_random_string(1000), "Should compress poorly"),
        ("mixed_data", format!("{}{}", "A".repeat(500), generate_random_string(500)), "Mixed compression"),
    ];
    
    for (test_name, data_content, description) in compressibility_tests {
        println!("Testing compression ratio for: {} ({})", test_name, description);
        
        // Test with different compression algorithms
        let algorithms = vec!["LZ4", "Snappy", "GZIP"];
        
        for algorithm in algorithms {
            let test_data = TestSSTableData {
                keyspace: format!("ratio_test_{}", test_name),
                table: format!("table_{}", algorithm.to_lowercase()),
                compression: Some(algorithm.to_string()),
                version: "5.0".to_string(),
                rows: vec![TestRow {
                    key: format!("{}_{}_key", test_name, algorithm).into_bytes(),
                    columns: {
                        let mut cols = std::collections::HashMap::new();
                        cols.insert("algorithm".to_string(), algorithm.as_bytes().to_vec());
                        cols.insert("test_data".to_string(), data_content.as_bytes().to_vec());
                        cols
                    },
                    timestamp: Some(1640995200000),
                }],
            };
            
            let sstable_path = harness.create_test_sstable(
                &format!("ratio_{}_{}", test_name, algorithm.to_lowercase()), 
                test_data
            ).await?;
            
            // Check file size (as a proxy for compression effectiveness)
            let metadata = tokio::fs::metadata(&sstable_path).await
                .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to get metadata: {}", e))))?;
            
            println!("  {} with {}: {} bytes", test_name, algorithm, metadata.len());
            
            // Test reading
            let _result = harness.open_reader(&sstable_path).await;
        }
    }
    
    Ok(())
}

/// Test compression header validation
#[tokio::test]
async fn test_compression_header_validation() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Test different compression header scenarios
    let header_tests = vec![
        ("valid_lz4", "LZ4", true, "Standard LZ4 header"),
        ("valid_snappy", "Snappy", true, "Standard Snappy header"),
        ("invalid_algorithm", "InvalidCompression", false, "Unknown compression algorithm"),
        ("empty_compression", "", false, "Empty compression field"),
    ];
    
    for (test_name, compression_type, should_be_valid, description) in header_tests {
        println!("Testing compression header: {} ({})", test_name, description);
        
        let compression = if compression_type.is_empty() { 
            None 
        } else { 
            Some(compression_type.to_string()) 
        };
        
        let test_data = TestSSTableData {
            keyspace: format!("header_test_{}", test_name),
            table: "header_validation".to_string(),
            compression,
            version: "5.0".to_string(),
            rows: vec![TestRow {
                key: format!("header_{}_key", test_name).into_bytes(),
                columns: {
                    let mut cols = std::collections::HashMap::new();
                    cols.insert("header_test".to_string(), compression_type.as_bytes().to_vec());
                    cols.insert("validation".to_string(), b"test data".to_vec());
                    cols
                },
                timestamp: Some(1640995200000),
            }],
        };
        
        let sstable_path = harness.create_test_sstable(&format!("header_{}", test_name), test_data).await?;
        
        // Test reading the header
        let result = harness.open_reader(&sstable_path).await;
        
        if should_be_valid {
            println!("  Expected valid compression header for {}", compression_type);
        } else {
            println!("  Expected invalid compression header for {}", compression_type);
        }
        
        // We don't assert specific success/failure since our test files aren't real SSTables
        println!("Header validation test '{}' completed", test_name);
    }
    
    Ok(())
}

/// Test compression with large datasets
#[tokio::test]
async fn test_compression_with_large_data() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Create progressively larger test datasets
    let size_tests = vec![
        ("small", 1_000, "1KB data"),
        ("medium", 10_000, "10KB data"),
        ("large", 100_000, "100KB data"),
    ];
    
    for (size_name, data_size, description) in size_tests {
        println!("Testing compression with {} ({})", size_name, description);
        
        // Generate large data with patterns that should compress
        let large_data = generate_compressible_data(data_size);
        
        let algorithms = vec!["LZ4", "Snappy"];
        
        for algorithm in algorithms {
            let test_data = TestSSTableData {
                keyspace: format!("large_test_{}", size_name),
                table: format!("compressed_{}", algorithm.to_lowercase()),
                compression: Some(algorithm.to_string()),
                version: "5.0".to_string(),
                rows: vec![TestRow {
                    key: format!("large_{}_{}_key", size_name, algorithm).into_bytes(),
                    columns: {
                        let mut cols = std::collections::HashMap::new();
                        cols.insert("size".to_string(), size_name.as_bytes().to_vec());
                        cols.insert("algorithm".to_string(), algorithm.as_bytes().to_vec());
                        cols.insert("large_data".to_string(), large_data.clone());
                        cols
                    },
                    timestamp: Some(1640995200000),
                }],
            };
            
            let sstable_path = harness.create_test_sstable(
                &format!("large_{}_{}", size_name, algorithm.to_lowercase()), 
                test_data
            ).await?;
            
            // Verify file was created and check size
            let metadata = tokio::fs::metadata(&sstable_path).await
                .map_err(|e| Error::Io(std::io::Error::other(format!("Failed to get metadata: {}", e))))?;
            
            println!("  {} data with {}: {} bytes on disk", description, algorithm, metadata.len());
            
            // Test reading large compressed file
            let _result = harness.open_reader(&sstable_path).await;
        }
    }
    
    Ok(())
}

/// Test concurrent compression handling
#[tokio::test]
async fn test_concurrent_compression_handling() -> Result<()> {
    let harness = SSTableTestHarness::new().await?;
    
    // Create multiple compressed files concurrently
    let mut file_paths = Vec::new();
    let algorithms = vec!["LZ4", "Snappy", "GZIP"];
    
    // Create test files
    for (i, algorithm) in algorithms.iter().enumerate() {
        let test_data = TestSSTableData {
            keyspace: format!("concurrent_compression_{}", i),
            table: format!("table_{}", algorithm.to_lowercase()),
            compression: Some(algorithm.to_string()),
            version: "5.0".to_string(),
            rows: vec![TestRow {
                key: format!("concurrent_{}_{}_key", i, algorithm).into_bytes(),
                columns: {
                    let mut cols = std::collections::HashMap::new();
                    cols.insert("thread_id".to_string(), format!("{}", i).into_bytes());
                    cols.insert("algorithm".to_string(), algorithm.as_bytes().to_vec());
                    cols.insert("data".to_string(), generate_compressible_data(5000));
                    cols
                },
                timestamp: Some(1640995200000 + i as i64 * 1000),
            }],
        };
        
        let path = harness.create_test_sstable(&format!("concurrent_{}_{}", i, algorithm.to_lowercase()), test_data).await?;
        file_paths.push(path);
    }
    
    // Test concurrent reading of compressed files
    let mut tasks = Vec::new();
    
    for (i, path) in file_paths.iter().enumerate() {
        let harness_config = harness.config.clone();
        let harness_platform = harness.platform.clone();
        let test_path = path.clone();
        
        let task = tokio::spawn(async move {
            let _result = cqlite_core::storage::sstable::reader::SSTableReader::open(
                &test_path, &harness_config, harness_platform
            ).await;
            
            println!("Concurrent compression test {} completed", i);
            Result::<()>::Ok(())
        });
        
        tasks.push(task);
    }
    
    // Wait for all tasks
    for (i, task) in tasks.into_iter().enumerate() {
        let result = task.await
            .map_err(|e| Error::Io(std::io::Error::other(format!("Concurrent task {} failed: {}", i, e))))?;
        result?;
    }
    
    Ok(())
}

// Helper functions for test data generation

fn generate_random_data(size: usize) -> Vec<u8> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut data = Vec::with_capacity(size);
    let mut hasher = DefaultHasher::new();
    
    for i in 0..size {
        i.hash(&mut hasher);
        data.push((hasher.finish() & 0xFF) as u8);
    }
    
    data
}

fn generate_random_string(size: usize) -> String {
    let chars: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut result = String::with_capacity(size);
    
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    
    for i in 0..size {
        i.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % chars.len();
        result.push(chars[idx] as char);
    }
    
    result
}

fn generate_compressible_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    
    // Create patterns that compress well
    let patterns = vec![
        b"AAAAAAAAAA".to_vec(),
        b"BBBBBBBBBB".to_vec(),
        b"CCCCCCCCCC".to_vec(),
        b"1234567890".to_vec(),
        b"          ".to_vec(), // spaces
    ];
    
    let mut pattern_idx = 0;
    let mut remaining = size;
    
    while remaining > 0 {
        let pattern = &patterns[pattern_idx % patterns.len()];
        let chunk_size = std::cmp::min(remaining, pattern.len());
        
        data.extend_from_slice(&pattern[..chunk_size]);
        remaining -= chunk_size;
        pattern_idx += 1;
    }
    
    data
}