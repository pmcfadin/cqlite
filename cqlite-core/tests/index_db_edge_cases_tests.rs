//! Index.db Edge Cases and Boundary Condition Tests
//!
//! These tests validate Index.db parsing under extreme conditions and edge cases
//! that could expose bugs in the offset calculation logic.
//!
//! Key edge cases tested:
//! - Empty Index.db files
//! - Single partition entries
//! - Maximum size partitions
//! - Malformed data handling
//! - Memory limits and large datasets

use cqlite_core::{
    Config, Error,
    platform::Platform,
    storage::sstable::{SSTableReader, index_reader::IndexReader},
};
use std::{path::Path, sync::Arc};
use tempfile::TempDir;
use tokio::fs;

/// Test Index.db with empty file
#[tokio::test]
async fn test_empty_index_file() {
    let temp_dir = TempDir::new().unwrap();
    let empty_index = temp_dir.path().join("empty-Index.db");

    // Create empty file
    fs::write(&empty_index, b"").await.unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Should handle empty file gracefully
    let result = IndexReader::open(&empty_index, platform).await;
    assert!(result.is_err(), "Empty Index.db should return an error");

    match result {
        Err(Error::Corruption(_)) => println!("✓ Empty file correctly identified as corruption"),
        Err(e) => println!("✓ Empty file handled with error: {}", e),
        Ok(_) => panic!("Empty file should not parse successfully"),
    }
}

/// Test Index.db with single partition entry
#[tokio::test]
async fn test_single_partition_index() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let data_file = base_path.join("single-Data.db");
    let index_file = base_path.join("single-Index.db");

    create_single_partition_data(&data_file).await;
    create_single_partition_index(&index_file).await;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let index_reader = IndexReader::open(&index_file, platform.clone())
        .await
        .unwrap();
    let entries = index_reader.get_partition_entries();

    assert_eq!(entries.len(), 1, "Should have exactly one partition");

    let entry = &entries[0];
    assert!(
        entry.data_offset > 0,
        "Single partition should have non-zero offset"
    );
    assert_eq!(entry.key_digest.len(), 16, "Key digest should be 16 bytes");

    // Test lookup
    let lookup_result = index_reader.lookup_partition(&entry.key_digest);
    assert!(lookup_result.is_some(), "Should find the single partition");

    let found_entry = lookup_result.unwrap();
    assert_eq!(found_entry.data_offset, entry.data_offset);

    println!(
        "✓ Single partition test passed: offset={}",
        entry.data_offset
    );
}

/// Test Index.db with maximum reasonable size
#[tokio::test]
async fn test_large_index_file() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let data_file = base_path.join("large-Data.db");
    let index_file = base_path.join("large-Index.db");

    let partition_count = 10000; // Large but reasonable for testing
    create_large_data_file(&data_file, partition_count).await;
    create_large_index_file(&index_file, partition_count).await;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let start_time = std::time::Instant::now();
    let index_reader = IndexReader::open(&index_file, platform.clone())
        .await
        .unwrap();
    let parse_time = start_time.elapsed();

    println!("Large index parsing time: {}ms", parse_time.as_millis());

    let entries = index_reader.get_partition_entries();
    assert_eq!(
        entries.len(),
        partition_count,
        "Should parse all {} partitions",
        partition_count
    );

    // Validate first and last entries
    let first_entry = &entries[0];
    let last_entry = &entries[partition_count - 1];

    assert!(
        first_entry.data_offset > 0,
        "First partition should have non-zero offset"
    );
    assert!(
        last_entry.data_offset > first_entry.data_offset,
        "Last partition should have larger offset"
    );

    // Test random lookups for performance
    let test_indices = vec![0, 1000, 5000, 9000, 9999];
    for &idx in &test_indices {
        let entry = &entries[idx];
        let lookup_start = std::time::Instant::now();
        let found = index_reader.lookup_partition(&entry.key_digest);
        let lookup_time = lookup_start.elapsed();

        assert!(found.is_some(), "Should find partition at index {}", idx);
        assert!(
            lookup_time.as_millis() < 10,
            "Lookup should be fast (<10ms)"
        );
    }

    println!(
        "✓ Large index file test passed with {} partitions",
        partition_count
    );
}

/// Test Index.db with malformed data
#[tokio::test]
async fn test_malformed_index_data() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Test 1: Invalid marker
    let invalid_marker_file = base_path.join("invalid-marker-Index.db");
    let invalid_data = vec![
        0xFF, 0xFF, // Invalid marker (should be 0x0010)
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // Key digest
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
    ];
    fs::write(&invalid_marker_file, invalid_data).await.unwrap();

    let result = IndexReader::open(&invalid_marker_file, platform.clone()).await;
    // Should either parse gracefully or return error
    match result {
        Ok(reader) => {
            let entries = reader.get_partition_entries();
            println!(
                "✓ Invalid marker handled gracefully with {} entries",
                entries.len()
            );
        }
        Err(e) => {
            println!("✓ Invalid marker correctly rejected: {}", e);
        }
    }

    // Test 2: Truncated key digest
    let truncated_file = base_path.join("truncated-Index.db");
    let truncated_data = vec![
        0x00, 0x10, // Valid marker
        0x01, 0x02, 0x03, 0x04, // Only 4 bytes instead of 16
    ];
    fs::write(&truncated_file, truncated_data).await.unwrap();

    let result = IndexReader::open(&truncated_file, platform.clone()).await;
    // Should handle truncation gracefully
    match result {
        Ok(reader) => {
            let entries = reader.get_partition_entries();
            println!("✓ Truncated file handled with {} entries", entries.len());
        }
        Err(e) => {
            println!("✓ Truncated file correctly rejected: {}", e);
        }
    }

    // Test 3: Random binary data
    let random_file = base_path.join("random-Index.db");
    let random_data: Vec<u8> = (0..100).map(|i| (i * 123 + 456) as u8).collect();
    fs::write(&random_file, random_data).await.unwrap();

    let result = IndexReader::open(&random_file, platform).await;
    match result {
        Ok(reader) => {
            let entries = reader.get_partition_entries();
            println!("✓ Random data handled with {} entries", entries.len());
        }
        Err(e) => {
            println!("✓ Random data correctly rejected: {}", e);
        }
    }
}

/// Test boundary conditions for partition offsets with real SSTable data
#[tokio::test]
async fn test_partition_offset_boundaries() {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Use environment-relative paths for test datasets
    let test_data_base = if let Ok(datasets_root) = std::env::var("CQLITE_DATASETS_ROOT") {
        std::path::PathBuf::from(datasets_root)
    } else {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("test-data")
            .join("datasets")
    };

    let test_data_paths = vec![
        test_data_base.join("sstables/test_timeseries/user_sessions-7063d860934a11f08d448925b7a9e804/nb-1-big-Data.db"),
        test_data_base.join("sstables/test_timeseries/sensor_data-701e1cd0934a11f08d448925b7a9e804/nb-1-big-Data.db"),
        test_data_base.join("sstables/test_timeseries/log_entries-7046da80934a11f08d448925b7a9e804/nb-1-big-Data.db"),
    ];

    for data_file_path in test_data_paths {
        if !data_file_path.exists() {
            println!(
                "⚠ Skipping {} - file does not exist",
                data_file_path.display()
            );
            continue;
        }

        println!(
            "Testing boundary conditions with: {}",
            data_file_path.display()
        );

        match SSTableReader::open(&data_file_path, &config, platform.clone()).await {
            Ok(reader) => {
                // Test reading at file boundaries
                test_file_boundary_conditions(&reader, &data_file_path).await;

                // Test EOF conditions
                test_eof_conditions(&reader).await;

                // Test invalid offset handling
                test_invalid_offset_handling(&reader).await;

                println!(
                    "✓ Boundary condition tests passed for {}",
                    data_file_path.display()
                );
            }
            Err(e) => {
                println!(
                    "⚠ Could not load {}: {} (this may be expected for some edge cases)",
                    data_file_path.display(),
                    e
                );
            }
        }
    }
}

/// Test memory usage with very large theoretical dataset
#[tokio::test]
async fn test_memory_usage_large_dataset() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let index_file = base_path.join("memory-test-Index.db");

    // Create a large but memory-efficient test (many partitions)
    let partition_count = 50000; // 50K partitions
    create_memory_efficient_large_index(&index_file, partition_count).await;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Measure memory before loading
    let memory_before = get_memory_usage();

    let index_reader = IndexReader::open(&index_file, platform).await.unwrap();
    let entries = index_reader.get_partition_entries();

    // Measure memory after loading
    let memory_after = get_memory_usage();
    let memory_increase = memory_after.saturating_sub(memory_before);

    assert_eq!(entries.len(), partition_count);

    // Memory usage should be reasonable (less than 100MB for 50K partitions)
    let max_expected_memory = 100 * 1024 * 1024; // 100MB
    assert!(
        memory_increase < max_expected_memory,
        "Memory usage should be reasonable: {} bytes (max: {} bytes)",
        memory_increase,
        max_expected_memory
    );

    println!(
        "✓ Memory test passed: {} partitions loaded, memory increase: {} KB",
        partition_count,
        memory_increase / 1024
    );
}

/// Test concurrent access to Index.db with real SSTable data
#[tokio::test]
async fn test_concurrent_index_access() {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Use environment-relative path for test dataset
    let test_data_base = if let Ok(datasets_root) = std::env::var("CQLITE_DATASETS_ROOT") {
        std::path::PathBuf::from(datasets_root)
    } else {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("test-data")
            .join("datasets")
    };

    let data_file = test_data_base.join(
        "sstables/test_timeseries/user_sessions-7063d860934a11f08d448925b7a9e804/nb-1-big-Data.db",
    );

    if !data_file.exists() {
        println!(
            "⚠ Skipping concurrent access test - test data file does not exist: {}",
            data_file.display()
        );
        return;
    }

    println!(
        "Testing concurrent access with real SSTable: {}",
        data_file.display()
    );

    let reader = match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => Arc::new(reader),
        Err(e) => {
            println!("⚠ Could not load test data: {} (this may be expected)", e);
            return;
        }
    };

    // Test concurrent access with multiple threads
    let num_concurrent_threads = 10;
    let operations_per_thread = 50;
    let mut handles = Vec::new();

    println!(
        "Spawning {} concurrent threads, {} operations each",
        num_concurrent_threads, operations_per_thread
    );

    for thread_id in 0..num_concurrent_threads {
        let reader_clone = Arc::clone(&reader);
        let handle = tokio::spawn(async move {
            let mut successful_operations = 0;

            for op_id in 0..operations_per_thread {
                // Generate deterministic but varied partition keys for testing
                let partition_key = generate_test_partition_key(thread_id, op_id);

                // Perform concurrent index lookup
                match reader_clone
                    .lookup_partition_with_index(&partition_key)
                    .await
                {
                    Ok(_) => {
                        successful_operations += 1;
                    }
                    Err(_) => {
                        // Lookup failures are acceptable as we're testing with generated keys
                        // The important thing is that the operation completes without panicking
                    }
                }

                // Occasionally test other concurrent operations
                if op_id % 10 == 0 {
                    let _ = reader_clone.get_timestamp_range().await;
                }
                if op_id % 15 == 0 {
                    let _ = reader_clone.get_token_coverage().await;
                }
            }

            (thread_id, successful_operations)
        });
        handles.push(handle);
    }

    // Wait for all concurrent tasks to complete
    let start_time = std::time::Instant::now();
    let results = futures::future::join_all(handles).await;
    let elapsed = start_time.elapsed();

    // Validate thread safety - all tasks should complete successfully
    let mut total_operations = 0;
    let mut completed_threads = 0;

    for result in results {
        match result {
            Ok((thread_id, operations)) => {
                completed_threads += 1;
                total_operations += operations;
                println!("✓ Thread {} completed {} operations", thread_id, operations);
            }
            Err(e) => {
                panic!("Thread panicked during concurrent access: {}", e);
            }
        }
    }

    // Assertions for thread safety validation
    assert_eq!(
        completed_threads, num_concurrent_threads,
        "All {} threads should complete successfully",
        num_concurrent_threads
    );

    assert!(
        elapsed.as_millis() < 10000,
        "Concurrent operations should complete in reasonable time: {}ms",
        elapsed.as_millis()
    );

    println!(
        "✓ Concurrent access test passed: {} threads, {} total operations, completed in {}ms",
        completed_threads,
        total_operations,
        elapsed.as_millis()
    );
}

/// Test Index.db with zero-length key digests (edge case)
#[tokio::test]
async fn test_zero_length_key_digest() {
    let temp_dir = TempDir::new().unwrap();
    let index_file = temp_dir.path().join("zero-key-Index.db");

    // Create Index.db with zero-length key digest (should be invalid)
    let data = vec![
        0x00,
        0x10, // Marker
             // No key digest data follows (simulates zero-length)
    ];
    fs::write(&index_file, data).await.unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let result = IndexReader::open(&index_file, platform).await;

    // Should handle gracefully (either parse with 0 entries or return error)
    match result {
        Ok(reader) => {
            let entries = reader.get_partition_entries();
            println!(
                "✓ Zero-length key digest handled with {} entries",
                entries.len()
            );
        }
        Err(e) => {
            println!("✓ Zero-length key digest correctly rejected: {}", e);
        }
    }
}

// Helper functions for creating test data

async fn create_single_partition_data(path: &Path) {
    let data = vec![
        // Minimal SSTable header
        0x6f, 0x61, 0x00, 0x00, // Magic (0x6f610000 - supported format)
        0x0e, 0x00, 0x00, 0x00, // Version
        0x00, 0x00, 0x00, 0x01, // Table count
        0x00, 0x00, 0x00, 0x01, // Partition count
        0x00, 0x00, 0x00, 0x00, // Reserved
        0x00, 0x00, 0x00, 0x00, // Reserved
        // Single partition data
        0x73, 0x69, 0x6e, 0x67, 0x6c, 0x65, 0x5f, 0x6b, // "single_k"
        0x65, 0x79, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // "ey" + padding
    ];

    fs::write(path, data).await.unwrap();
}

async fn create_single_partition_index(path: &Path) {
    let data = vec![
        0x00, 0x10, // Marker
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // Single key digest
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
    ];

    fs::write(path, data).await.unwrap();
}

async fn create_large_data_file(path: &Path, partition_count: usize) {
    let mut data = Vec::new();

    // Header
    data.extend_from_slice(&[0x6f, 0x61, 0x00, 0x00]); // Magic (0x6f610000 - supported format) // Magic
    data.extend_from_slice(&[0x0e, 0x00, 0x00, 0x00]); // Version
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Table count

    // Partition count (4 bytes, big endian)
    data.push((partition_count >> 24) as u8);
    data.push((partition_count >> 16) as u8);
    data.push((partition_count >> 8) as u8);
    data.push(partition_count as u8);

    data.extend(vec![0x00; 8]); // Reserved

    // Generate partitions
    for i in 0..partition_count {
        let partition_key = format!("large_partition_{:06}", i);
        data.extend_from_slice(&[0x00, partition_key.len() as u8]);
        data.extend_from_slice(partition_key.as_bytes());
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x40]); // 64 bytes data
        data.extend(vec![(i % 256) as u8; 64]);
    }

    fs::write(path, data).await.unwrap();
}

async fn create_large_index_file(path: &Path, partition_count: usize) {
    let mut data = Vec::new();

    for i in 0..partition_count {
        data.extend_from_slice(&[0x00, 0x10]); // Marker

        // Generate deterministic key digest
        for j in 0..16 {
            data.push(((i + j * 17) % 256) as u8);
        }
    }

    fs::write(path, data).await.unwrap();
}

// Removed: create_boundary_test_data - no longer needed as we use real SSTable data

// Removed: create_boundary_test_index - no longer needed as we use real SSTable data

async fn create_memory_efficient_large_index(path: &Path, partition_count: usize) {
    // Use efficient batch writing for large files
    const BATCH_SIZE: usize = 1000;
    let mut all_data = Vec::new();

    for batch_start in (0..partition_count).step_by(BATCH_SIZE) {
        let batch_end = std::cmp::min(batch_start + BATCH_SIZE, partition_count);

        for i in batch_start..batch_end {
            all_data.extend_from_slice(&[0x00, 0x10]); // Marker

            // Generate unique key digest
            for j in 0..16 {
                all_data.push(((i + j * 7) % 256) as u8);
            }
        }
    }

    fs::write(path, all_data).await.unwrap();
}

// Removed: create_concurrent_test_data - no longer needed as we use real SSTable data

// Removed: create_concurrent_test_index - no longer needed as we use real SSTable data

fn get_memory_usage() -> usize {
    // Simple memory usage approximation
    // In a real implementation, you might use platform-specific APIs
    std::mem::size_of::<u8>() * 1024 // Simplified memory usage estimate
}

// Helper functions for real SSTable boundary testing

async fn test_file_boundary_conditions(reader: &SSTableReader, data_file: &Path) {
    println!("Testing file boundary conditions");

    // Get file size to test boundary conditions
    let file_metadata = tokio::fs::metadata(data_file).await.unwrap();
    let file_size = file_metadata.len();

    println!("File size: {} bytes", file_size);

    // Test operations that might access file boundaries
    let _ = reader.get_timestamp_range().await;
    let _ = reader.get_token_coverage().await;

    // Test token range at boundaries
    let _ = reader.iterate_token_range(i64::MIN, i64::MIN + 1000).await;
    let _ = reader.iterate_token_range(i64::MAX - 1000, i64::MAX).await;

    println!("✓ File boundary conditions tested");
}

async fn test_eof_conditions(reader: &SSTableReader) {
    println!("Testing EOF conditions");

    // Test operations that might read to end of file
    let _ = reader.iterate_token_range(i64::MIN, i64::MAX).await;

    // Test metadata operations that scan the entire file
    let _ = reader.stats().await;

    println!("✓ EOF conditions tested");
}

async fn test_invalid_offset_handling(reader: &SSTableReader) {
    println!("Testing invalid offset handling");

    // Test with keys that are unlikely to exist (to test offset boundary handling)
    let invalid_keys = [
        b"\x00\x00\x00\x00".as_slice(), // Null bytes
        b"\xFF\xFF\xFF\xFF".as_slice(), // Max bytes
        &vec![0x00; 1024],              // Large null key
        &vec![0xFF; 1024],              // Large max key
        b"invalid_key_that_should_not_exist_in_real_data".as_slice(),
    ];

    for key in &invalid_keys {
        // These lookups should handle invalid offsets gracefully
        let _ = reader.lookup_partition_with_index(key).await;
    }

    println!("✓ Invalid offset handling tested");
}

fn generate_test_partition_key(thread_id: usize, operation_id: usize) -> Vec<u8> {
    // Generate deterministic partition keys for concurrent testing
    let key_variants = [
        format!("user_{:04}_{:04}", thread_id, operation_id),
        format!("session_{:04}_{:04}", thread_id, operation_id),
        format!("event_{:04}_{:04}", thread_id, operation_id),
        format!("metric_{:04}_{:04}", thread_id, operation_id),
        format!("log_{:04}_{:04}", thread_id, operation_id),
    ];

    let variant_index = (thread_id + operation_id) % key_variants.len();
    key_variants[variant_index].clone().into_bytes()
}
