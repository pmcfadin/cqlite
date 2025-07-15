//! Integration Tests for Performance Features
//!
//! End-to-end testing of all performance-critical components
//! to ensure they work together correctly.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::time::sleep;
use cqlite_core::{Config, StorageEngine, Value, RowKey, types::TableId};
use cqlite_core::platform::Platform;

// Test performance targets
const WRITE_PERFORMANCE_TARGET: f64 = 10_000.0; // ops/sec
const READ_PERFORMANCE_TARGET: f64 = 50_000.0; // ops/sec
const MEMORY_USAGE_TARGET: u64 = 128 * 1024 * 1024; // 128MB

#[tokio::test]
async fn test_end_to_end_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::performance_optimized();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let engine = StorageEngine::open(temp_dir.path(), &config, platform).await.unwrap();
    let table_id = TableId::new("perf_test_table");
    
    // Write performance test
    let write_start = Instant::now();
    let write_count = 10_000;
    
    for i in 0..write_count {
        let key = RowKey::from(format!("perf_key_{:08}", i));
        let value = Value::Text(format!("perf_value_{}", i));
        engine.put(&table_id, key, value).await.unwrap();
    }
    
    let write_duration = write_start.elapsed();
    let write_ops_per_sec = write_count as f64 / write_duration.as_secs_f64();
    
    println!("Write performance: {:.2} ops/sec", write_ops_per_sec);
    
    // Force flush to ensure data is written
    engine.flush().await.unwrap();
    
    // Read performance test
    let read_start = Instant::now();
    let read_count = 10_000;
    
    for i in 0..read_count {
        let key = RowKey::from(format!("perf_key_{:08}", i));
        let result = engine.get(&table_id, &key).await.unwrap();
        assert!(result.is_some());
    }
    
    let read_duration = read_start.elapsed();
    let read_ops_per_sec = read_count as f64 / read_duration.as_secs_f64();
    
    println!("Read performance: {:.2} ops/sec", read_ops_per_sec);
    
    // Memory usage test
    let stats = engine.stats().await.unwrap();
    let memory_usage = stats.memtable.size_bytes;
    
    println!("Memory usage: {} bytes ({:.2} MB)", memory_usage, memory_usage as f64 / 1024.0 / 1024.0);
    
    // Assert performance targets
    assert!(write_ops_per_sec >= WRITE_PERFORMANCE_TARGET, 
        "Write performance below target: {:.2} < {:.2}", write_ops_per_sec, WRITE_PERFORMANCE_TARGET);
    assert!(read_ops_per_sec >= READ_PERFORMANCE_TARGET, 
        "Read performance below target: {:.2} < {:.2}", read_ops_per_sec, READ_PERFORMANCE_TARGET);
    assert!(memory_usage <= MEMORY_USAGE_TARGET, 
        "Memory usage above target: {} > {}", memory_usage, MEMORY_USAGE_TARGET);
}

#[tokio::test]
async fn test_concurrent_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::performance_optimized();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let engine = Arc::new(StorageEngine::open(temp_dir.path(), &config, platform).await.unwrap());
    let table_id = TableId::new("concurrent_test_table");
    
    let start_time = Instant::now();
    let mut handles = Vec::new();
    
    // Spawn 10 concurrent tasks
    for thread_id in 0..10 {
        let engine = engine.clone();
        let table_id = table_id.clone();
        
        let handle = tokio::spawn(async move {
            for i in 0..1000 {
                let key = RowKey::from(format!("concurrent_key_{}_{:04}", thread_id, i));
                let value = Value::Text(format!("concurrent_value_{}_{}", thread_id, i));
                engine.put(&table_id, key, value).await.unwrap();
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }
    
    let duration = start_time.elapsed();
    let total_ops = 10 * 1000;
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    
    println!("Concurrent performance: {:.2} ops/sec", ops_per_sec);
    
    // Verify all data was written correctly
    for thread_id in 0..10 {
        for i in 0..1000 {
            let key = RowKey::from(format!("concurrent_key_{}_{:04}", thread_id, i));
            let result = engine.get(&table_id, &key).await.unwrap();
            assert!(result.is_some());
        }
    }
    
    // Assert concurrent performance is reasonable
    assert!(ops_per_sec >= 5_000.0, "Concurrent performance too low: {:.2}", ops_per_sec);
}

#[tokio::test]
async fn test_memory_optimization() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::memory_optimized();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let engine = StorageEngine::open(temp_dir.path(), &config, platform).await.unwrap();
    let table_id = TableId::new("memory_test_table");
    
    // Insert data and monitor memory usage
    let initial_stats = engine.stats().await.unwrap();
    let initial_memory = initial_stats.memtable.size_bytes;
    
    // Insert 50K records
    for i in 0..50_000 {
        let key = RowKey::from(format!("memory_key_{:08}", i));
        let value = Value::Text(format!("memory_value_{}", i));
        engine.put(&table_id, key, value).await.unwrap();
        
        // Force flush every 10K records to test memory management
        if i % 10_000 == 0 {
            engine.flush().await.unwrap();
        }
    }
    
    let final_stats = engine.stats().await.unwrap();
    let final_memory = final_stats.memtable.size_bytes;
    let memory_growth = final_memory - initial_memory;
    
    println!("Memory growth: {} bytes ({:.2} MB)", memory_growth, memory_growth as f64 / 1024.0 / 1024.0);
    
    // Memory growth should be reasonable
    assert!(memory_growth <= 64 * 1024 * 1024, "Memory growth too high: {} bytes", memory_growth);
}

#[tokio::test]
async fn test_compression_performance() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::performance_optimized();
    config.storage.enable_compression = true;
    config.storage.compression_algorithm = cqlite_core::config::CompressionAlgorithm::Lz4;
    
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let engine = StorageEngine::open(temp_dir.path(), &config, platform).await.unwrap();
    let table_id = TableId::new("compression_test_table");
    
    // Create highly compressible data
    let compressible_data = "This is a highly compressible string that repeats many times. ".repeat(100);
    
    let start_time = Instant::now();
    
    // Insert compressible data
    for i in 0..1000 {
        let key = RowKey::from(format!("compress_key_{:04}", i));
        let value = Value::Text(format!("{}{}", compressible_data, i));
        engine.put(&table_id, key, value).await.unwrap();
    }
    
    // Force flush to trigger compression
    engine.flush().await.unwrap();
    
    let duration = start_time.elapsed();
    let ops_per_sec = 1000.0 / duration.as_secs_f64();
    
    println!("Compression performance: {:.2} ops/sec", ops_per_sec);
    
    // Get compression statistics
    let stats = engine.stats().await.unwrap();
    let compressed_size = stats.sstables.total_size;
    let uncompressed_estimate = 1000 * compressible_data.len() as u64;
    let compression_ratio = compressed_size as f64 / uncompressed_estimate as f64;
    
    println!("Compression ratio: {:.2}%", compression_ratio * 100.0);
    
    // Verify compression is working
    assert!(compression_ratio < 0.5, "Compression ratio too high: {:.2}", compression_ratio);
    
    // Verify read performance with compression
    let read_start = Instant::now();
    for i in 0..1000 {
        let key = RowKey::from(format!("compress_key_{:04}", i));
        let result = engine.get(&table_id, &key).await.unwrap();
        assert!(result.is_some());
    }
    let read_duration = read_start.elapsed();
    let read_ops_per_sec = 1000.0 / read_duration.as_secs_f64();
    
    println!("Compressed read performance: {:.2} ops/sec", read_ops_per_sec);
}

#[tokio::test]
async fn test_sstable_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::performance_optimized();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let engine = StorageEngine::open(temp_dir.path(), &config, platform).await.unwrap();
    let table_id = TableId::new("sstable_test_table");
    
    // Fill memtable to trigger SSTable creation
    let memtable_threshold = config.storage.memtable_size_threshold as usize;
    let estimated_entry_size = 100; // bytes
    let entries_needed = memtable_threshold / estimated_entry_size;
    
    let start_time = Instant::now();
    
    // Insert enough data to trigger multiple SSTable flushes
    for i in 0..entries_needed * 3 {
        let key = RowKey::from(format!("sstable_key_{:08}", i));
        let value = Value::Text(format!("sstable_value_{}_with_extra_data", i));
        engine.put(&table_id, key, value).await.unwrap();
    }
    
    let insert_duration = start_time.elapsed();
    let insert_ops_per_sec = (entries_needed * 3) as f64 / insert_duration.as_secs_f64();
    
    println!("SSTable insert performance: {:.2} ops/sec", insert_ops_per_sec);
    
    // Force final flush
    engine.flush().await.unwrap();
    
    // Test read performance across multiple SSTables
    let read_start = Instant::now();
    for i in 0..1000 {
        let key = RowKey::from(format!("sstable_key_{:08}", i * 10));
        let result = engine.get(&table_id, &key).await.unwrap();
        assert!(result.is_some());
    }
    let read_duration = read_start.elapsed();
    let read_ops_per_sec = 1000.0 / read_duration.as_secs_f64();
    
    println!("SSTable read performance: {:.2} ops/sec", read_ops_per_sec);
    
    // Get SSTable statistics
    let stats = engine.stats().await.unwrap();
    println!("SSTable count: {}", stats.sstables.sstable_count);
    println!("Total SSTable size: {} bytes", stats.sstables.total_size);
    
    // Verify multiple SSTables were created
    assert!(stats.sstables.sstable_count > 1, "Expected multiple SSTables");
}

#[tokio::test]
async fn test_query_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::performance_optimized();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let engine = StorageEngine::open(temp_dir.path(), &config, platform).await.unwrap();
    let table_id = TableId::new("query_test_table");
    
    // Insert ordered data for range queries
    for i in 0..10_000 {
        let key = RowKey::from(format!("query_key_{:08}", i));
        let value = Value::Text(format!("query_value_{}", i));
        engine.put(&table_id, key, value).await.unwrap();
    }
    
    engine.flush().await.unwrap();
    
    // Test range query performance
    let start_key = RowKey::from("query_key_00001000");
    let end_key = RowKey::from("query_key_00002000");
    
    let query_start = Instant::now();
    let results = engine.scan(&table_id, Some(&start_key), Some(&end_key), Some(1000)).await.unwrap();
    let query_duration = query_start.elapsed();
    
    println!("Range query performance: {:.2} ms", query_duration.as_millis());
    println!("Range query results: {} rows", results.len());
    
    // Verify results are correct
    assert!(results.len() <= 1000, "Too many results returned");
    assert!(!results.is_empty(), "No results returned");
    
    // Test point query performance
    let point_start = Instant::now();
    for i in 0..1000 {
        let key = RowKey::from(format!("query_key_{:08}", i * 10));
        let result = engine.get(&table_id, &key).await.unwrap();
        assert!(result.is_some());
    }
    let point_duration = point_start.elapsed();
    let point_ops_per_sec = 1000.0 / point_duration.as_secs_f64();
    
    println!("Point query performance: {:.2} ops/sec", point_ops_per_sec);
}

#[tokio::test]
async fn test_mixed_workload_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::performance_optimized();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let engine = Arc::new(StorageEngine::open(temp_dir.path(), &config, platform).await.unwrap());
    let table_id = TableId::new("mixed_workload_table");
    
    // Pre-populate with some data
    for i in 0..1000 {
        let key = RowKey::from(format!("mixed_key_{:08}", i));
        let value = Value::Text(format!("mixed_value_{}", i));
        engine.put(&table_id, key, value).await.unwrap();
    }
    
    engine.flush().await.unwrap();
    
    // Mixed workload: 70% reads, 30% writes
    let start_time = Instant::now();
    let total_ops = 10_000;
    let mut read_count = 0;
    let mut write_count = 0;
    
    for i in 0..total_ops {
        if i % 10 < 7 {
            // Read operation
            let key = RowKey::from(format!("mixed_key_{:08}", i % 1000));
            let _result = engine.get(&table_id, &key).await.unwrap();
            read_count += 1;
        } else {
            // Write operation
            let key = RowKey::from(format!("mixed_key_{:08}", i));
            let value = Value::Text(format!("mixed_value_{}", i));
            engine.put(&table_id, key, value).await.unwrap();
            write_count += 1;
        }
    }
    
    let duration = start_time.elapsed();
    let mixed_ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    
    println!("Mixed workload performance: {:.2} ops/sec", mixed_ops_per_sec);
    println!("Read operations: {}", read_count);
    println!("Write operations: {}", write_count);
    
    // Assert mixed workload performance
    assert!(mixed_ops_per_sec >= 5_000.0, "Mixed workload performance too low: {:.2}", mixed_ops_per_sec);
}

#[tokio::test]
async fn test_sustained_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::performance_optimized();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let engine = Arc::new(StorageEngine::open(temp_dir.path(), &config, platform).await.unwrap());
    let table_id = TableId::new("sustained_test_table");
    
    let test_duration = Duration::from_secs(30);
    let start_time = Instant::now();
    let mut operation_count = 0;
    
    // Run sustained operations for 30 seconds
    while start_time.elapsed() < test_duration {
        let key = RowKey::from(format!("sustained_key_{:08}", operation_count));
        let value = Value::Text(format!("sustained_value_{}", operation_count));
        engine.put(&table_id, key, value).await.unwrap();
        
        operation_count += 1;
        
        // Periodic flush to prevent memory buildup
        if operation_count % 1000 == 0 {
            engine.flush().await.unwrap();
        }
        
        // Small delay to prevent CPU overload
        if operation_count % 100 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }
    
    let actual_duration = start_time.elapsed();
    let sustained_ops_per_sec = operation_count as f64 / actual_duration.as_secs_f64();
    
    println!("Sustained performance: {:.2} ops/sec over {} seconds", 
             sustained_ops_per_sec, actual_duration.as_secs());
    
    // Memory usage should remain stable
    let stats = engine.stats().await.unwrap();
    let memory_mb = stats.memtable.size_bytes / 1024 / 1024;
    
    println!("Final memory usage: {} MB", memory_mb);
    
    // Assert sustained performance
    assert!(sustained_ops_per_sec >= 1_000.0, "Sustained performance too low: {:.2}", sustained_ops_per_sec);
    assert!(memory_mb <= 128, "Memory usage too high: {} MB", memory_mb);
}

#[tokio::test]
async fn test_recovery_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::performance_optimized();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    
    // Phase 1: Create and populate database
    {
        let engine = StorageEngine::open(temp_dir.path(), &config, platform.clone()).await.unwrap();
        let table_id = TableId::new("recovery_test_table");
        
        // Insert data
        for i in 0..10_000 {
            let key = RowKey::from(format!("recovery_key_{:08}", i));
            let value = Value::Text(format!("recovery_value_{}", i));
            engine.put(&table_id, key, value).await.unwrap();
        }
        
        // Ensure data is persisted
        engine.flush().await.unwrap();
    } // Engine is dropped here, simulating shutdown
    
    // Phase 2: Measure recovery time
    let recovery_start = Instant::now();
    let engine = StorageEngine::open(temp_dir.path(), &config, platform).await.unwrap();
    let recovery_time = recovery_start.elapsed();
    
    println!("Recovery time: {:.2} ms", recovery_time.as_millis());
    
    // Phase 3: Verify data integrity and read performance
    let table_id = TableId::new("recovery_test_table");
    let verification_start = Instant::now();
    
    for i in 0..1000 {
        let key = RowKey::from(format!("recovery_key_{:08}", i * 10));
        let result = engine.get(&table_id, &key).await.unwrap();
        assert!(result.is_some(), "Data not found after recovery");
    }
    
    let verification_time = verification_start.elapsed();
    let verification_ops_per_sec = 1000.0 / verification_time.as_secs_f64();
    
    println!("Post-recovery read performance: {:.2} ops/sec", verification_ops_per_sec);
    
    // Assert recovery performance
    assert!(recovery_time.as_millis() <= 5000, "Recovery time too slow: {} ms", recovery_time.as_millis());
    assert!(verification_ops_per_sec >= 10_000.0, "Post-recovery performance too low: {:.2}", verification_ops_per_sec);
}

#[tokio::test]
async fn test_bloom_filter_performance() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::performance_optimized();
    config.storage.enable_bloom_filters = true;
    config.storage.bloom_filter_fp_rate = 0.01;
    
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let engine = StorageEngine::open(temp_dir.path(), &config, platform).await.unwrap();
    let table_id = TableId::new("bloom_test_table");
    
    // Insert data to create bloom filter
    for i in 0..10_000 {
        let key = RowKey::from(format!("bloom_key_{:08}", i));
        let value = Value::Text(format!("bloom_value_{}", i));
        engine.put(&table_id, key, value).await.unwrap();
    }
    
    engine.flush().await.unwrap();
    
    // Test bloom filter effectiveness
    let start_time = Instant::now();
    let mut found_count = 0;
    let mut not_found_count = 0;
    
    // Test with existing keys (should all be found)
    for i in 0..5_000 {
        let key = RowKey::from(format!("bloom_key_{:08}", i));
        let result = engine.get(&table_id, &key).await.unwrap();
        if result.is_some() {
            found_count += 1;
        } else {
            not_found_count += 1;
        }
    }
    
    // Test with non-existing keys (should mostly not be found)
    for i in 10_000..15_000 {
        let key = RowKey::from(format!("bloom_key_{:08}", i));
        let result = engine.get(&table_id, &key).await.unwrap();
        if result.is_some() {
            found_count += 1;
        } else {
            not_found_count += 1;
        }
    }
    
    let duration = start_time.elapsed();
    let lookup_ops_per_sec = 10_000.0 / duration.as_secs_f64();
    
    println!("Bloom filter lookup performance: {:.2} ops/sec", lookup_ops_per_sec);
    println!("Found: {}, Not found: {}", found_count, not_found_count);
    
    // Verify bloom filter effectiveness
    assert_eq!(found_count, 5_000, "All existing keys should be found");
    // Allow some false positives but they should be minimal
    let false_positive_rate = (found_count - 5_000) as f64 / 5_000.0;
    assert!(false_positive_rate <= 0.02, "False positive rate too high: {:.4}", false_positive_rate);
}

#[tokio::test]
async fn test_data_type_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::performance_optimized();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let engine = StorageEngine::open(temp_dir.path(), &config, platform).await.unwrap();
    let table_id = TableId::new("datatype_test_table");
    
    // Test different data types
    let test_data = vec![
        ("integer", Value::Integer(42)),
        ("bigint", Value::BigInt(9223372036854775807)),
        ("float", Value::Float(3.14159)),
        ("boolean", Value::Boolean(true)),
        ("text", Value::Text("Hello, World!".to_string())),
        ("blob", Value::Blob(vec![1, 2, 3, 4, 5])),
        ("timestamp", Value::Timestamp(1640995200000000)),
    ];
    
    let start_time = Instant::now();
    
    // Insert all data types
    for (type_name, value) in test_data.clone() {
        for i in 0..1000 {
            let key = RowKey::from(format!("{}_{:04}", type_name, i));
            engine.put(&table_id, key, value.clone()).await.unwrap();
        }
    }
    
    let insert_duration = start_time.elapsed();
    let insert_ops_per_sec = 7000.0 / insert_duration.as_secs_f64();
    
    println!("Data type insert performance: {:.2} ops/sec", insert_ops_per_sec);
    
    // Test read performance for each data type
    let read_start = Instant::now();
    
    for (type_name, expected_value) in test_data {
        for i in 0..100 {
            let key = RowKey::from(format!("{}_{:04}", type_name, i));
            let result = engine.get(&table_id, &key).await.unwrap();
            assert_eq!(result, Some(expected_value.clone()));
        }
    }
    
    let read_duration = read_start.elapsed();
    let read_ops_per_sec = 700.0 / read_duration.as_secs_f64();
    
    println!("Data type read performance: {:.2} ops/sec", read_ops_per_sec);
}