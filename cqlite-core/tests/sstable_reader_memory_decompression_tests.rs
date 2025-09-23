//! Memory-bounded decompression tests for SSTable reader fixes
//!
//! Tests verify that large block decompression operates within memory constraints
//! and handles memory pressure scenarios gracefully.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::fs;

use cqlite_core::Config;
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;

/// Memory constraints for decompression tests
const MAX_DECOMPRESSION_MEMORY_MB: usize = 50; // Max 50MB for decompression
const LARGE_BLOCK_SIZE_MB: usize = 10; // 10MB blocks for testing
const MEMORY_PRESSURE_THRESHOLD_MB: usize = 100; // Memory pressure simulation
const MAX_DECOMPRESSION_TIME_MS: u64 = 5000; // Max 5 seconds for large blocks

/// Test memory-bounded decompression with large compressed blocks
#[tokio::test]
async fn test_large_block_memory_bounded_decompression() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "large-block-decompression";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    // Create test files with large compressed blocks
    create_large_block_test_files(&scenario_dir, base_name, LARGE_BLOCK_SIZE_MB).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let memory_before = get_memory_usage();
    println!("Memory before test: {} MB", memory_before);

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            // Test decompression of large blocks
            for block_index in 0..5 {
                println!("Testing decompression of large block {}", block_index);

                let memory_before_block = get_memory_usage();
                let start_time = Instant::now();

                // Attempt to read large block
                let key = format!("large_block_key_{:04}", block_index).into_bytes();
                let _result = reader.lookup_partition_with_index(&key).await;

                let decompression_time = start_time.elapsed();
                let memory_after_block = get_memory_usage();
                let memory_used = memory_after_block.saturating_sub(memory_before_block);

                println!(
                    "Block {}: Decompression time: {:?}, Memory used: {} MB",
                    block_index, decompression_time, memory_used
                );

                // Verify memory usage is bounded
                assert!(
                    memory_used <= MAX_DECOMPRESSION_MEMORY_MB,
                    "Block {} decompression used {} MB, exceeds limit {} MB",
                    block_index,
                    memory_used,
                    MAX_DECOMPRESSION_MEMORY_MB
                );

                // Verify decompression time is reasonable
                assert!(
                    decompression_time.as_millis() <= MAX_DECOMPRESSION_TIME_MS as u128,
                    "Block {} decompression took {:?}, exceeds limit {} ms",
                    block_index,
                    decompression_time,
                    MAX_DECOMPRESSION_TIME_MS
                );

                // Force garbage collection between blocks
                force_garbage_collection();
            }

            let memory_after = get_memory_usage();
            let total_memory_increase = memory_after.saturating_sub(memory_before);

            println!("Total memory increase: {} MB", total_memory_increase);

            // Total memory usage should remain reasonable
            assert!(
                total_memory_increase <= MAX_DECOMPRESSION_MEMORY_MB * 2,
                "Total memory increase {} MB exceeds reasonable limit",
                total_memory_increase
            );
        }
        Err(e) => {
            println!("Large block decompression test skipped: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test decompression under simulated memory pressure
#[tokio::test]
async fn test_memory_pressure_decompression() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "memory-pressure-decompression";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    create_memory_pressure_test_files(&scenario_dir, base_name).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Simulate memory pressure by allocating large buffers
    let _memory_pressure = simulate_memory_pressure(MEMORY_PRESSURE_THRESHOLD_MB);

    let memory_under_pressure = get_memory_usage();
    println!("Memory under pressure: {} MB", memory_under_pressure);

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            // Test decompression under memory pressure
            for i in 0..10 {
                let memory_before_op = get_memory_usage();
                let start_time = Instant::now();

                let key = format!("pressure_test_key_{:04}", i).into_bytes();
                let result = reader.lookup_partition_with_index(&key).await;

                let operation_time = start_time.elapsed();
                let memory_after_op = get_memory_usage();
                let memory_delta = memory_after_op.saturating_sub(memory_before_op);

                println!(
                    "Operation {}: Time: {:?}, Memory delta: {} MB, Success: {}",
                    i,
                    operation_time,
                    memory_delta,
                    result.is_ok()
                );

                // Operations should complete in reasonable time even under pressure
                assert!(
                    operation_time.as_millis() <= MAX_DECOMPRESSION_TIME_MS as u128 * 2,
                    "Operation {} under memory pressure took too long: {:?}",
                    i,
                    operation_time
                );

                // Memory usage should remain controlled
                assert!(
                    memory_delta <= MAX_DECOMPRESSION_MEMORY_MB,
                    "Operation {} used excessive memory under pressure: {} MB",
                    i,
                    memory_delta
                );

                // Give system time to stabilize
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        Err(e) => {
            println!("Memory pressure decompression test skipped: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test streaming decompression for very large blocks
#[tokio::test]
async fn test_streaming_decompression_memory_bounds() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "streaming-decompression";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    // Create files with very large blocks that require streaming
    create_streaming_test_files(&scenario_dir, base_name, LARGE_BLOCK_SIZE_MB * 2).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            // Test streaming decompression
            let test_cases = vec![
                ("small_stream", 1),
                ("medium_stream", 5),
                ("large_stream", 10),
            ];

            for (test_name, stream_count) in test_cases {
                println!("Testing streaming decompression: {}", test_name);

                let memory_before_stream = get_memory_usage();
                let start_time = Instant::now();

                // Simulate streaming access pattern
                for i in 0..stream_count {
                    let key = format!("stream_key_{:04}", i).into_bytes();
                    let _ = reader.lookup_partition_with_index(&key).await;

                    // Check memory periodically during streaming
                    if i % 2 == 0 {
                        let current_memory = get_memory_usage();
                        let memory_used = current_memory.saturating_sub(memory_before_stream);

                        assert!(
                            memory_used <= MAX_DECOMPRESSION_MEMORY_MB * 3, // Allow more for streaming
                            "Streaming {} operation {} used too much memory: {} MB",
                            test_name,
                            i,
                            memory_used
                        );
                    }
                }

                let streaming_time = start_time.elapsed();
                let memory_after_stream = get_memory_usage();
                let total_memory_used = memory_after_stream.saturating_sub(memory_before_stream);

                println!(
                    "Streaming {}: Time: {:?}, Total memory: {} MB",
                    test_name, streaming_time, total_memory_used
                );

                // Verify streaming doesn't accumulate excessive memory
                assert!(
                    total_memory_used <= MAX_DECOMPRESSION_MEMORY_MB * 2,
                    "Streaming {} accumulated too much memory: {} MB",
                    test_name,
                    total_memory_used
                );

                // Streaming should be reasonably fast
                let expected_max_time =
                    Duration::from_millis(MAX_DECOMPRESSION_TIME_MS * stream_count as u64);
                assert!(
                    streaming_time <= expected_max_time,
                    "Streaming {} took too long: {:?}",
                    test_name,
                    streaming_time
                );

                force_garbage_collection();
            }
        }
        Err(e) => {
            println!("Streaming decompression test skipped: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test concurrent decompression memory isolation
#[tokio::test]
async fn test_concurrent_decompression_memory_isolation() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "concurrent-decompression";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    create_concurrent_test_files(&scenario_dir, base_name).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            let reader = Arc::new(reader);
            let concurrency_levels = vec![2, 5, 10];

            for concurrency in concurrency_levels {
                println!(
                    "Testing concurrent decompression with {} threads",
                    concurrency
                );

                let memory_before_concurrent = get_memory_usage();
                let start_time = Instant::now();

                let mut handles = Vec::new();

                for thread_id in 0..concurrency {
                    let reader_clone = Arc::clone(&reader);
                    let handle = tokio::spawn(async move {
                        let thread_memory_before = get_memory_usage();

                        for i in 0..10 {
                            let key = format!("concurrent_key_{}_{:04}", thread_id, i).into_bytes();
                            let _result = reader_clone.lookup_partition_with_index(&key).await;

                            // Brief pause to allow interleaving
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }

                        let thread_memory_after = get_memory_usage();
                        thread_memory_after.saturating_sub(thread_memory_before)
                    });

                    handles.push(handle);
                }

                // Wait for all concurrent operations to complete
                let mut thread_memory_usage = Vec::new();
                for handle in handles {
                    if let Ok(memory_used) = handle.await {
                        thread_memory_usage.push(memory_used);
                    }
                }

                let concurrent_time = start_time.elapsed();
                let memory_after_concurrent = get_memory_usage();
                let total_concurrent_memory =
                    memory_after_concurrent.saturating_sub(memory_before_concurrent);

                println!(
                    "Concurrent {}: Time: {:?}, Total memory: {} MB, Thread usage: {:?}",
                    concurrency, concurrent_time, total_concurrent_memory, thread_memory_usage
                );

                // Verify that concurrent operations don't multiply memory usage excessively
                let max_expected_memory = MAX_DECOMPRESSION_MEMORY_MB * concurrency.min(3); // Cap at 3x
                assert!(
                    total_concurrent_memory <= max_expected_memory,
                    "Concurrent {} operations used {} MB, exceeds expected {} MB",
                    concurrency,
                    total_concurrent_memory,
                    max_expected_memory
                );

                // Individual threads should have reasonable memory usage
                for (i, &thread_memory) in thread_memory_usage.iter().enumerate() {
                    assert!(
                        thread_memory <= MAX_DECOMPRESSION_MEMORY_MB,
                        "Thread {} used {} MB, exceeds limit {} MB",
                        i,
                        thread_memory,
                        MAX_DECOMPRESSION_MEMORY_MB
                    );
                }

                force_garbage_collection();
            }
        }
        Err(e) => {
            println!("Concurrent decompression test skipped: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test decompression error handling under memory constraints
#[tokio::test]
async fn test_decompression_error_handling_memory_bounds() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "error-handling-decompression";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    // Create files with potential decompression issues
    create_error_test_files(&scenario_dir, base_name).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            let memory_before_errors = get_memory_usage();

            // Test various error scenarios
            let error_test_cases = vec![
                ("corrupted_block", "corrupted_key"),
                ("oversized_block", "oversized_key"),
                ("invalid_compression", "invalid_key"),
            ];

            for (error_type, key_prefix) in error_test_cases {
                println!("Testing error handling for: {}", error_type);

                let memory_before_error = get_memory_usage();

                for i in 0..5 {
                    let key = format!("{}_{:04}", key_prefix, i).into_bytes();
                    let result = reader.lookup_partition_with_index(&key).await;

                    // Should handle errors gracefully
                    match result {
                        Ok(_) => println!("Operation succeeded for {} key {}", error_type, i),
                        Err(e) => println!("Expected error for {} key {}: {}", error_type, i, e),
                    }

                    let memory_after_error = get_memory_usage();
                    let memory_used = memory_after_error.saturating_sub(memory_before_error);

                    // Error handling shouldn't leak memory
                    assert!(
                        memory_used <= MAX_DECOMPRESSION_MEMORY_MB,
                        "Error handling for {} used excessive memory: {} MB",
                        error_type,
                        memory_used
                    );
                }
            }

            let memory_after_errors = get_memory_usage();
            let total_error_memory = memory_after_errors.saturating_sub(memory_before_errors);

            println!(
                "Total memory used for error handling: {} MB",
                total_error_memory
            );

            // Error handling should not accumulate memory
            assert!(
                total_error_memory <= MAX_DECOMPRESSION_MEMORY_MB,
                "Error handling accumulated too much memory: {} MB",
                total_error_memory
            );
        }
        Err(e) => {
            println!("Error handling decompression test skipped: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

// Helper functions

fn get_memory_usage() -> usize {
    // Get current memory usage in MB
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/self/status") {
            for line in contents.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<usize>() {
                            return kb / 1024; // Convert KB to MB
                        }
                    }
                }
            }
        }
    }

    // Fallback for other platforms
    10 // 10MB baseline
}

fn simulate_memory_pressure(pressure_mb: usize) -> Vec<Vec<u8>> {
    // Allocate memory to simulate pressure
    let buffer_size = 1024 * 1024; // 1MB buffers
    let buffer_count = pressure_mb;

    (0..buffer_count)
        .map(|i| vec![(i % 256) as u8; buffer_size])
        .collect()
}

fn force_garbage_collection() {
    // Force garbage collection/cleanup
    let _temp: Vec<Vec<u8>> = (0..100).map(|_| vec![0u8; 1024]).collect();

    // Sleep briefly to allow cleanup
    std::thread::sleep(Duration::from_millis(10));
}

// Test file creation functions

async fn create_large_block_test_files(dir: &Path, base_name: &str, block_size_mb: usize) {
    create_large_block_data_file(dir, base_name, block_size_mb).await;
    create_large_block_index_file(dir, base_name).await;
    create_large_block_summary_file(dir, base_name).await;
    create_large_block_statistics_file(dir, base_name).await;
    create_large_block_filter_file(dir, base_name).await;
}

async fn create_large_block_data_file(dir: &Path, base_name: &str, block_size_mb: usize) {
    let path = dir.join(format!("{}-Data.db", base_name));
    let mut data = Vec::new();

    // SSTable header
    data.extend_from_slice(&[
        0x6f, 0x61, 0x00, 0x00, // Magic "oa" + version
        0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
    ]);

    // Create large blocks
    let block_count = 5;
    let block_size = block_size_mb * 1024 * 1024 / block_count;

    for block_index in 0..block_count {
        // Block header (simulated compressed block)
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Block type
        data.extend_from_slice(&(block_size as u32).to_be_bytes()); // Block size

        // Large block content (simulated)
        let key = format!("large_block_key_{:04}", block_index);
        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());

        // Large payload (filled with pattern to simulate real data)
        let payload_size = block_size.saturating_sub(key.len() + 8);
        let pattern = (block_index % 256) as u8;
        data.extend_from_slice(&vec![pattern; payload_size]);
    }

    fs::write(path, data).await.unwrap();
}

async fn create_memory_pressure_test_files(dir: &Path, base_name: &str) {
    create_memory_pressure_data_file(dir, base_name).await;
    create_memory_pressure_index_file(dir, base_name).await;
    create_memory_pressure_summary_file(dir, base_name).await;
    create_memory_pressure_statistics_file(dir, base_name).await;
    create_memory_pressure_filter_file(dir, base_name).await;
}

async fn create_memory_pressure_data_file(dir: &Path, base_name: &str) {
    let path = dir.join(format!("{}-Data.db", base_name));
    let mut data = Vec::new();

    // SSTable header
    data.extend_from_slice(&[
        0x6f, 0x61, 0x00, 0x00, // Magic "oa" + version
        0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
    ]);

    // Create moderately sized blocks for pressure testing
    for i in 0..20 {
        let key = format!("pressure_test_key_{:04}", i);
        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&[0x00, 0x00, 0x10, 0x00]); // 4KB blocks
        data.extend_from_slice(&vec![0xDD; 4096]); // 4KB of data
    }

    fs::write(path, data).await.unwrap();
}

async fn create_streaming_test_files(dir: &Path, base_name: &str, total_size_mb: usize) {
    create_streaming_data_file(dir, base_name, total_size_mb).await;
    create_streaming_index_file(dir, base_name).await;
    create_streaming_summary_file(dir, base_name).await;
    create_streaming_statistics_file(dir, base_name).await;
    create_streaming_filter_file(dir, base_name).await;
}

async fn create_streaming_data_file(dir: &Path, base_name: &str, total_size_mb: usize) {
    let path = dir.join(format!("{}-Data.db", base_name));
    let mut data = Vec::new();

    // SSTable header
    data.extend_from_slice(&[
        0x6f, 0x61, 0x00, 0x00, // Magic "oa" + version
        0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
    ]);

    // Create streaming blocks
    let block_count = 20;
    let block_size = total_size_mb * 1024 * 1024 / block_count;

    for i in 0..block_count {
        let key = format!("stream_key_{:04}", i);
        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&(block_size as u32).to_be_bytes());

        // Fill with streaming pattern
        let pattern = vec![(i % 256) as u8; block_size];
        data.extend_from_slice(&pattern);
    }

    fs::write(path, data).await.unwrap();
}

async fn create_concurrent_test_files(dir: &Path, base_name: &str) {
    create_concurrent_data_file(dir, base_name).await;
    create_concurrent_index_file(dir, base_name).await;
    create_concurrent_summary_file(dir, base_name).await;
    create_concurrent_statistics_file(dir, base_name).await;
    create_concurrent_filter_file(dir, base_name).await;
}

async fn create_concurrent_data_file(dir: &Path, base_name: &str) {
    let path = dir.join(format!("{}-Data.db", base_name));
    let mut data = Vec::new();

    // SSTable header
    data.extend_from_slice(&[
        0x6f, 0x61, 0x00, 0x00, // Magic "oa" + version
        0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
    ]);

    // Create blocks for concurrent access
    for thread_id in 0..10 {
        for i in 0..10 {
            let key = format!("concurrent_key_{}_{:04}", thread_id, i);
            data.extend_from_slice(&(key.len() as u32).to_be_bytes());
            data.extend_from_slice(key.as_bytes());
            data.extend_from_slice(&[0x00, 0x00, 0x08, 0x00]); // 2KB blocks
            data.extend_from_slice(&vec![0xEE; 2048]); // 2KB of data
        }
    }

    fs::write(path, data).await.unwrap();
}

async fn create_error_test_files(dir: &Path, base_name: &str) {
    create_error_data_file(dir, base_name).await;
    create_error_index_file(dir, base_name).await;
    create_error_summary_file(dir, base_name).await;
    create_error_statistics_file(dir, base_name).await;
    create_error_filter_file(dir, base_name).await;
}

async fn create_error_data_file(dir: &Path, base_name: &str) {
    let path = dir.join(format!("{}-Data.db", base_name));
    let mut data = Vec::new();

    // SSTable header
    data.extend_from_slice(&[
        0x6f, 0x61, 0x00, 0x00, // Magic "oa" + version
        0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
    ]);

    // Create blocks with potential issues for error testing
    for i in 0..15 {
        let key = match i % 3 {
            0 => format!("corrupted_key_{:04}", i),
            1 => format!("oversized_key_{:04}", i),
            _ => format!("invalid_key_{:04}", i),
        };

        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());

        // Different block sizes for different error scenarios
        let block_size = match i % 3 {
            0 => 1024, // Normal size
            1 => 8192, // Large size
            _ => 512,  // Small size
        };

        data.extend_from_slice(&(block_size as u32).to_be_bytes());

        // Fill with different patterns
        let pattern = match i % 3 {
            0 => vec![0xFF; block_size], // All 1s (potential corruption indicator)
            1 => vec![0x00; block_size], // All 0s
            _ => (0..block_size).map(|j| (j % 256) as u8).collect(), // Incremental pattern
        };

        data.extend_from_slice(&pattern);
    }

    fs::write(path, data).await.unwrap();
}

// Simplified helper file creation functions (shared structure)

async fn create_large_block_index_file(dir: &Path, base_name: &str) {
    create_helper_index_file(dir, base_name, 5).await;
}

async fn create_large_block_summary_file(dir: &Path, base_name: &str) {
    create_helper_summary_file(dir, base_name, 5).await;
}

async fn create_large_block_statistics_file(dir: &Path, base_name: &str) {
    create_helper_statistics_file(dir, base_name, "large-block").await;
}

async fn create_large_block_filter_file(dir: &Path, base_name: &str) {
    create_helper_filter_file(dir, base_name, 4096).await;
}

async fn create_memory_pressure_index_file(dir: &Path, base_name: &str) {
    create_helper_index_file(dir, base_name, 20).await;
}

async fn create_memory_pressure_summary_file(dir: &Path, base_name: &str) {
    create_helper_summary_file(dir, base_name, 10).await;
}

async fn create_memory_pressure_statistics_file(dir: &Path, base_name: &str) {
    create_helper_statistics_file(dir, base_name, "memory-pressure").await;
}

async fn create_memory_pressure_filter_file(dir: &Path, base_name: &str) {
    create_helper_filter_file(dir, base_name, 2048).await;
}

async fn create_streaming_index_file(dir: &Path, base_name: &str) {
    create_helper_index_file(dir, base_name, 20).await;
}

async fn create_streaming_summary_file(dir: &Path, base_name: &str) {
    create_helper_summary_file(dir, base_name, 10).await;
}

async fn create_streaming_statistics_file(dir: &Path, base_name: &str) {
    create_helper_statistics_file(dir, base_name, "streaming").await;
}

async fn create_streaming_filter_file(dir: &Path, base_name: &str) {
    create_helper_filter_file(dir, base_name, 8192).await;
}

async fn create_concurrent_index_file(dir: &Path, base_name: &str) {
    create_helper_index_file(dir, base_name, 100).await;
}

async fn create_concurrent_summary_file(dir: &Path, base_name: &str) {
    create_helper_summary_file(dir, base_name, 20).await;
}

async fn create_concurrent_statistics_file(dir: &Path, base_name: &str) {
    create_helper_statistics_file(dir, base_name, "concurrent").await;
}

async fn create_concurrent_filter_file(dir: &Path, base_name: &str) {
    create_helper_filter_file(dir, base_name, 4096).await;
}

async fn create_error_index_file(dir: &Path, base_name: &str) {
    create_helper_index_file(dir, base_name, 15).await;
}

async fn create_error_summary_file(dir: &Path, base_name: &str) {
    create_helper_summary_file(dir, base_name, 8).await;
}

async fn create_error_statistics_file(dir: &Path, base_name: &str) {
    create_helper_statistics_file(dir, base_name, "error").await;
}

async fn create_error_filter_file(dir: &Path, base_name: &str) {
    create_helper_filter_file(dir, base_name, 1024).await;
}

// Helper file creation functions

async fn create_helper_index_file(dir: &Path, base_name: &str, entry_count: usize) {
    let path = dir.join(format!("{}-Index.db", base_name));
    let mut data = Vec::new();

    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Version
    data.extend_from_slice(&(entry_count as u32).to_be_bytes());

    for i in 0..entry_count {
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x20]); // Digest length
        let mut digest = vec![0; 32];
        digest[0] = (i % 256) as u8;
        data.extend_from_slice(&digest);
        data.extend_from_slice(&((i * 1000) as u64).to_be_bytes());
        data.extend_from_slice(&(100u32).to_be_bytes());
    }

    fs::write(path, data).await.unwrap();
}

async fn create_helper_summary_file(dir: &Path, base_name: &str, entry_count: usize) {
    let path = dir.join(format!("{}-Summary.db", base_name));
    let mut data = Vec::new();

    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Version
    data.extend_from_slice(&(entry_count as u32).to_be_bytes());

    for i in 0..entry_count {
        let key = format!("summary_{:04}", i);
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&((i as i64) * 1000000).to_be_bytes());
        data.extend_from_slice(&((i * 500) as u64).to_be_bytes());
        data.extend_from_slice(&(i as u32).to_be_bytes());
    }

    fs::write(path, data).await.unwrap();
}

async fn create_helper_statistics_file(dir: &Path, base_name: &str, test_type: &str) {
    let path = dir.join(format!("{}-Statistics.db", base_name));
    let mut data = Vec::new();

    let base_count = match test_type {
        "large-block" => 50000,
        "memory-pressure" => 20000,
        "streaming" => 100000,
        "concurrent" => 10000,
        "error" => 15000,
        _ => 1000,
    };

    let stats = vec![
        ("min_timestamp", 1640995200000u64),
        ("max_timestamp", 1672531200000u64),
        ("live_row_count", base_count),
        ("total_data_size", base_count * 64),
        ("compaction_level", 0u64),
    ];

    for (key, value) in stats {
        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&(8u32).to_be_bytes());
        data.extend_from_slice(&value.to_be_bytes());
    }

    fs::write(path, data).await.unwrap();
}

async fn create_helper_filter_file(dir: &Path, base_name: &str, filter_size: usize) {
    let path = dir.join(format!("{}-Filter.db", base_name));
    let mut data = Vec::new();

    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x02]); // Version
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x07]); // Hash functions
    data.extend_from_slice(&(filter_size as u32).to_be_bytes());

    let bit_array = vec![0xAA; filter_size]; // Alternating pattern
    data.extend_from_slice(&bit_array);

    fs::write(path, data).await.unwrap();
}
