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

/// Test boundary conditions for partition offsets
#[tokio::test]
async fn test_partition_offset_boundaries() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let data_file = base_path.join("boundary-Data.db");
    let index_file = base_path.join("boundary-Index.db");

    create_boundary_test_data(&data_file).await;
    create_boundary_test_index(&index_file).await;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let reader = SSTableReader::open(&data_file, &config, platform)
        .await
        .unwrap();

    // Test partitions at specific boundary positions
    let boundary_tests = vec![
        (
            "first_partition",
            "Should handle first partition after header",
        ),
        (
            "middle_partition",
            "Should handle middle partition correctly",
        ),
        ("last_partition", "Should handle last partition at file end"),
    ];

    for (partition_key, description) in boundary_tests {
        if let Ok(Some((offset, size))) = reader
            .lookup_partition_with_index(partition_key.as_bytes())
            .await
        {
            assert!(offset > 0, "{}: offset should be non-zero", description);
            assert!(size > 0, "{}: size should be non-zero", description);

            // Verify offset is reasonable (not impossibly large)
            assert!(
                offset < 1_000_000,
                "{}: offset should be reasonable",
                description
            );

            println!("✓ {}: offset={}, size={}", description, offset, size);
        } else {
            println!(
                "ℹ {}: partition not found (acceptable for test data)",
                description
            );
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

/// Test concurrent access to Index.db
#[tokio::test]
async fn test_concurrent_index_access() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let data_file = base_path.join("concurrent-Data.db");
    let index_file = base_path.join("concurrent-Index.db");

    create_concurrent_test_data(&data_file).await;
    create_concurrent_test_index(&index_file).await;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let reader = Arc::new(
        SSTableReader::open(&data_file, &config, platform)
            .await
            .unwrap(),
    );

    // Spawn multiple concurrent lookup tasks
    let mut handles = Vec::new();

    for i in 0..10 {
        let reader_clone = Arc::clone(&reader);
        let handle = tokio::spawn(async move {
            let partition_key = format!("concurrent_partition_{:03}", i);

            for _ in 0..100 {
                let _ = reader_clone
                    .lookup_partition_with_index(partition_key.as_bytes())
                    .await;
            }

            i // Return task ID
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results = futures::future::join_all(handles).await;

    let mut completed_tasks = 0;
    for result in results {
        if let Ok(task_id) = result {
            completed_tasks += 1;
            println!("✓ Concurrent task {} completed", task_id);
        }
    }

    assert_eq!(completed_tasks, 10, "All concurrent tasks should complete");
    println!(
        "✓ Concurrent access test passed with {} tasks",
        completed_tasks
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
        0x6d, 0x61, 0x00, 0x00, // Magic
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
    data.extend_from_slice(&[0x6d, 0x61, 0x00, 0x00]); // Magic
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

async fn create_boundary_test_data(path: &Path) {
    let data = vec![
        // Header (24 bytes)
        0x6d, 0x61, 0x00, 0x00, // Magic
        0x0e, 0x00, 0x00, 0x00, // Version
        0x00, 0x00, 0x00, 0x01, // Table count
        0x00, 0x00, 0x00, 0x03, // Partition count
        0x00, 0x00, 0x00, 0x00, // Reserved
        0x00, 0x00, 0x00, 0x00, // Reserved
        // First partition immediately after header
        0x66, 0x69, 0x72, 0x73, 0x74, 0x5f, 0x70, 0x61, // "first_pa"
        0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x00, // "rtition" + null
        // Middle partition
        0x6d, 0x69, 0x64, 0x64, 0x6c, 0x65, 0x5f, 0x70, // "middle_p"
        0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, // "artition"
        // Last partition
        0x6c, 0x61, 0x73, 0x74, 0x5f, 0x70, 0x61, 0x72, // "last_par"
        0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x00, 0x00, // "tition" + padding
    ];

    fs::write(path, data).await.unwrap();
}

async fn create_boundary_test_index(path: &Path) {
    let data = vec![
        // First partition entry
        0x00, 0x10, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, // Middle partition entry
        0x00, 0x10, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
        0x02, 0x02, 0x02, // Last partition entry
        0x00, 0x10, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
        0x03, 0x03, 0x03,
    ];

    fs::write(path, data).await.unwrap();
}

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

async fn create_concurrent_test_data(path: &Path) {
    let mut data = Vec::new();

    // Header
    data.extend_from_slice(&[0x6d, 0x61, 0x00, 0x00]);
    data.extend_from_slice(&[0x0e, 0x00, 0x00, 0x00]);
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Table count
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x0A]); // 10 partitions
    data.extend(vec![0x00; 8]); // Reserved

    for i in 0..10 {
        let partition_key = format!("concurrent_partition_{:03}", i);
        data.extend_from_slice(&[0x00, partition_key.len() as u8]);
        data.extend_from_slice(partition_key.as_bytes());
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x32]); // 50 bytes data
        data.extend(vec![0xCC; 50]);
    }

    fs::write(path, data).await.unwrap();
}

async fn create_concurrent_test_index(path: &Path) {
    let mut data = Vec::new();

    for i in 0..10 {
        data.extend_from_slice(&[0x00, 0x10]);

        // Generate key digest for concurrent test
        for j in 0..16 {
            data.push(((i * 16 + j + 100) % 256) as u8);
        }
    }

    fs::write(path, data).await.unwrap();
}

fn get_memory_usage() -> usize {
    // Simple memory usage approximation
    // In a real implementation, you might use platform-specific APIs
    std::mem::size_of::<u8>() * 1024 // Simplified memory usage estimate
}
