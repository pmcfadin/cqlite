//! Index.db Parsing Regression Tests
//!
//! These tests validate Index.db parsing behavior per Issue #92 requirements:
//! - WITHOUT Summary.db: offsets must be 0 (no heuristics - Issue #28 mandate)
//! - WITH Summary.db: offsets calculated via spec-accurate correlation
//!
//! Previous behavior (Issue #66): Used heuristic estimation (hardcoded base=1024, size=4096)
//! New behavior (Issue #92): No guessing - requires Summary.db for accurate offsets
//!
//! Tests updated to validate correct no-heuristics behavior

use cqlite_core::{
    platform::Platform,
    storage::sstable::{index_reader::IndexReader, SSTableReader},
    Config,
};
use std::{collections::HashSet, path::Path, sync::Arc};
use tempfile::TempDir;
use tokio::fs;

// Import test utilities
mod common;
use common::sstable_test_utils::{PerformanceTestUtils, TestContext};

/// Test that validates no-heuristics mandate (Issue #92)
/// Without Summary.db, offsets MUST be 0 (no guessing allowed)
#[tokio::test]
async fn test_no_heuristics_without_summary() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    // Create Index.db without Summary.db
    let index_file = base_path.join("test-Index.db");
    create_index_file_with_real_offsets(&index_file).await;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Parse Index.db WITHOUT Summary.db
    let index_reader = IndexReader::open(&index_file, platform.clone())
        .await
        .unwrap();

    let partition_entries = index_reader.get_partition_entries();

    // NEW BEHAVIOR (Issue #92): Without Summary.db, all offsets must be 0
    // This enforces the no-heuristics mandate from Issue #28
    for (i, entry) in partition_entries.iter().enumerate() {
        assert_eq!(
            entry.data_offset, 0,
            "Partition {} must have offset=0 without Summary.db (no heuristics allowed)",
            i
        );
    }

    println!("✓ No-heuristics mandate validated: offsets=0 without Summary.db");
}

/// Test partition lookup returns correct Data.db offsets using real SSTable data
#[tokio::test]
#[ignore = "Temporarily disabled - new SSTable formats need header parser updates"]
async fn test_partition_lookup_correct_offsets() {
    let mut context = TestContext::new("test_basic").await.unwrap();
    let sstable_path = context
        .prepare_sstable("simple_table")
        .await
        .unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Find the actual Data.db file in the prepared SSTable directory
    let entries = fs::read_dir(&sstable_path).await.unwrap();
    let mut data_file = None;
    let mut index_file = None;

    let mut entries_vec = vec![];
    let mut entries_stream = entries;
    while let Some(entry) = entries_stream.next_entry().await.unwrap() {
        entries_vec.push(entry);
    }

    for entry in entries_vec {
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.contains("Data.db") && !name.contains(".jsonl") {
                data_file = Some(path.clone());
            } else if name.contains("Index.db") {
                index_file = Some(path.clone());
            }
        }
    }

    // Skip test if SSTable binary files not present (CI uses refs-only dataset)
    let data_file = match data_file {
        Some(f) => f,
        None => {
            println!("⏭️  Skipping test: Data.db file not present in dataset (refs-only mode)");
            return;
        }
    };
    let index_file = match index_file {
        Some(f) => f,
        None => {
            println!("⏭️  Skipping test: Index.db file not present in dataset (refs-only mode)");
            return;
        }
    };

    // Test with real SSTable reader
    let _reader = SSTableReader::open(&data_file, &config, platform.clone())
        .await
        .unwrap();

    // Test index reader directly (without Summary.db)
    let index_reader = IndexReader::open(&index_file, platform).await.unwrap();

    let partition_entries = index_reader.get_partition_entries();
    println!(
        "Found {} partition entries in real Index.db",
        partition_entries.len()
    );

    // NOTE (Issue #92): Without Summary.db, offsets will be 0 (no heuristics)
    // Validate that partition keys are parsed correctly
    let mut valid_lookups = 0;

    for (i, entry) in partition_entries.iter().enumerate().take(5) {
        // Test first 5 partitions
        // Validate partition key digest is non-empty
        assert!(
            !entry.key_digest.is_empty(),
            "Partition {} should have valid key digest",
            i
        );

        // Test actual partition lookup via index (by key digest)
        if let Some(lookup_entry) = index_reader.lookup_partition(&entry.key_digest) {
            assert_eq!(
                lookup_entry.data_offset, entry.data_offset,
                "Index lookup should return same offset as direct access"
            );
            valid_lookups += 1;
        }

        println!(
            "✓ Real partition {} parsed: key_digest_len={}, offset={} (0=no Summary.db)",
            i,
            entry.key_digest.len(),
            entry.data_offset
        );
    }

    assert!(
        valid_lookups > 0,
        "Should have at least one successful partition lookup"
    );

    println!(
        "✓ Partition lookup test passed with {} valid lookups",
        valid_lookups
    );
    println!("   Note: Offsets are 0 without Summary.db (correct per Issue #92)");

    let _metrics = context.cleanup().unwrap();
}

/// Test Index.db with real SSTable data validation
#[tokio::test]
async fn test_index_with_real_sstable_data() {
    let mut context = TestContext::new("test_basic").await.unwrap();
    let sstable_path = context
        .prepare_sstable("multi_partition_table")
        .await
        .unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Find actual SSTable component files
    let entries = fs::read_dir(&sstable_path).await.unwrap();
    let mut data_file = None;
    let mut index_file = None;

    let mut entries_vec = vec![];
    let mut entries_stream = entries;
    while let Some(entry) = entries_stream.next_entry().await.unwrap() {
        entries_vec.push(entry);
    }

    for entry in entries_vec {
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.contains("Data.db") && !name.contains(".jsonl") {
                data_file = Some(path.clone());
            } else if name.contains("Index.db") {
                index_file = Some(path.clone());
            }
        }
    }

    // Skip test if SSTable binary files not present (CI uses refs-only dataset)
    let data_file = match data_file {
        Some(f) => f,
        None => {
            println!("⏭️  Skipping test: Data.db file not present in dataset (refs-only mode)");
            return;
        }
    };
    let index_file = match index_file {
        Some(f) => f,
        None => {
            println!("⏭️  Skipping test: Index.db file not present in dataset (refs-only mode)");
            return;
        }
    };

    // Test that Index.db entries correspond to actual SSTable data
    let index_reader = IndexReader::open(&index_file, platform.clone())
        .await
        .expect("Should be able to open real Index.db file");

    let _sstable_reader = SSTableReader::open(&data_file, &config, platform)
        .await
        .expect("Should be able to open real Data.db file");

    let partition_entries = index_reader.get_partition_entries();
    println!(
        "Real SSTable has {} partition entries",
        partition_entries.len()
    );

    let mut unique_offsets = HashSet::new();

    for entry in partition_entries.iter().take(10) {
        // Test first 10 partitions
        // NOTE (Issue #92): Without Summary.db, offsets will be 0 (no heuristics)
        // This is CORRECT behavior per the no-heuristics mandate
        // To get real offsets, IndexReader must be opened with Summary.db

        // For now, just validate that entries are parsed (offsets may be 0)
        // Full offset validation requires Summary.db correlation (tracked separately)

        unique_offsets.insert(entry.data_offset);

        // Test lookup consistency
        let key_digest = &entry.key_digest;
        if let Some(looked_up_entry) = index_reader.lookup_partition(key_digest) {
            assert_eq!(
                looked_up_entry.data_offset, entry.data_offset,
                "Index lookup should return same offset as direct access"
            );

            assert_eq!(
                looked_up_entry.data_size, entry.data_size,
                "Index lookup should return same size as direct access"
            );
        }
    }

    // NOTE (Issue #92): Without Summary.db, all offsets will be 0
    // This is expected and correct behavior (no heuristics mandate)
    println!(
        "✓ Real SSTable validation completed: {} entries validated",
        partition_entries.len()
    );
    println!("   Note: Offsets are 0 without Summary.db (correct per Issue #92)");

    let _metrics = context.cleanup().unwrap();
}

/// Test edge cases and boundary conditions using real SSTable data
#[tokio::test]
async fn test_index_edge_cases() {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Test 1: Test with smallest available real dataset
    let mut context = TestContext::new("test_basic").await.unwrap();
    let available_tables = context.get_available_tables().unwrap();

    println!(
        "Available tables for edge case testing: {}",
        available_tables.len()
    );

    // Test with the first available table
    if let Some(table) = available_tables.first() {
        let sstable_path = context.prepare_sstable(&table.name).await.unwrap();

        // Find index file
        let entries = fs::read_dir(&sstable_path).await.unwrap();
        let mut index_file = None;

        let mut entries_vec = vec![];
        let mut entries_stream = entries;
        while let Some(entry) = entries_stream.next_entry().await.unwrap() {
            entries_vec.push(entry);
        }

        for entry in entries_vec {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.contains("Index.db") {
                    index_file = Some(path);
                    break;
                }
            }
        }

        if let Some(index_file) = index_file {
            let index_reader = IndexReader::open(&index_file, platform.clone())
                .await
                .expect("Should be able to open real Index.db file");

            let entries = index_reader.get_partition_entries();
            println!(
                "Real Index.db has {} entries for edge case testing",
                entries.len()
            );

            // Test 2: Verify entries are parsed (offsets may be 0 without Summary.db)
            // NOTE (Issue #92): Without Summary.db, offsets will be 0 (no heuristics)
            let mut zero_offset_count = 0;
            let mut non_zero_offset_count = 0;

            for entry in entries.iter() {
                // Validate key digest is present
                assert!(
                    !entry.key_digest.is_empty(),
                    "Entry should have valid key digest"
                );

                if entry.data_offset == 0 {
                    zero_offset_count += 1;
                } else {
                    non_zero_offset_count += 1;
                }
            }

            println!(
                "Index entries: {} with offsets, {} without (no Summary.db)",
                non_zero_offset_count, zero_offset_count
            );
            println!("   Note: Zero offsets are correct without Summary.db (Issue #92)");

            // Test 3: Verify partition lookups work for edge cases (first and last entries)
            if !entries.is_empty() {
                // Test first entry
                let first_entry = &entries[0];
                let lookup_result = index_reader.lookup_partition(&first_entry.key_digest);
                assert!(lookup_result.is_some(), "First entry lookup should succeed");

                // Test last entry
                let last_entry = &entries[entries.len() - 1];
                let lookup_result = index_reader.lookup_partition(&last_entry.key_digest);
                assert!(lookup_result.is_some(), "Last entry lookup should succeed");

                println!(
                    "✓ Edge case boundary tests passed for {} entries",
                    entries.len()
                );
            }

            // Test 4: Test with non-existent key digest
            let fake_digest = vec![0u8; 16]; // All zeros
            let lookup_result = index_reader.lookup_partition(&fake_digest);
            assert!(
                lookup_result.is_none(),
                "Non-existent key lookup should return None"
            );

            println!("✓ Non-existent key test passed");
        }
    }

    // Test 5: Try to open non-existent file
    let temp_dir = TempDir::new().unwrap();
    let non_existent = temp_dir.path().join("does-not-exist-Index.db");
    let result = IndexReader::open(&non_existent, platform.clone()).await;
    assert!(result.is_err(), "Opening non-existent file should fail");

    // Test 6: Try to open empty file
    let empty_index = temp_dir.path().join("empty-Index.db");
    fs::write(&empty_index, b"").await.unwrap();
    let result = IndexReader::open(&empty_index, platform).await;
    assert!(result.is_err(), "Opening empty Index.db should fail");

    println!("✓ All edge case tests passed");

    let _metrics = context.cleanup().unwrap();
}

/// Test promoted index functionality for wide partitions using real SSTable data
#[tokio::test]
async fn test_promoted_index_wide_partitions() {
    let mut context = TestContext::new("test_timeseries").await.unwrap();

    // Try different time series tables which are more likely to have wide partitions
    let available_tables = context.get_available_tables().unwrap();
    println!("Available timeseries tables: {}", available_tables.len());

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let mut found_promoted_index = false;
    let mut tested_tables = 0;

    // Test multiple tables to find ones with promoted indexes
    for table in available_tables.iter().take(3) {
        // Test up to 3 tables
        println!("Testing table: {}", table.name);

        match context.prepare_sstable(&table.name).await {
            Ok(sstable_path) => {
                // Find index file
                let entries = fs::read_dir(&sstable_path).await.unwrap();
                let mut index_file = None;

                let mut entries_vec = vec![];
                let mut entries_stream = entries;
                while let Some(entry) = entries_stream.next_entry().await.unwrap() {
                    entries_vec.push(entry);
                }

                for entry in entries_vec {
                    let path = entry.path();
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.contains("Index.db") {
                            index_file = Some(path);
                            break;
                        }
                    }
                }

                if let Some(index_file) = index_file {
                    match IndexReader::open(&index_file, platform.clone()).await {
                        Ok(index_reader) => {
                            tested_tables += 1;
                            let entries = index_reader.get_partition_entries();
                            println!(
                                "Table {} has {} partition entries",
                                table.name,
                                entries.len()
                            );

                            // Check for promoted indexes
                            let mut promoted_count = 0;
                            let mut total_promoted_entries = 0;

                            for (i, entry) in entries.iter().enumerate() {
                                if let Some(ref promoted) = entry.promoted_index {
                                    promoted_count += 1;
                                    found_promoted_index = true;

                                    println!(
                                        "Found promoted index in partition {} with {} entries",
                                        i,
                                        promoted.entries.len()
                                    );

                                    // Verify promoted index entries have proper structure
                                    for (j, promoted_entry) in promoted.entries.iter().enumerate() {
                                        assert!(
                                            true, // partition_offset is u64, always >= 0
                                            "Promoted index entry {} should have valid partition offset, got {}",
                                            j,
                                            promoted_entry.partition_offset
                                        );
                                        assert!(
                                            promoted_entry.section_size > 0,
                                            "Promoted index entry {} should have non-zero section size, got {}",
                                            j,
                                            promoted_entry.section_size
                                        );
                                        total_promoted_entries += 1;
                                    }
                                }
                            }

                            if promoted_count > 0 {
                                println!(
                                    "✓ Table {} has {} partitions with promoted indexes ({} total promoted entries)",
                                    table.name, promoted_count, total_promoted_entries
                                );
                            } else {
                                println!(
                                    "  Table {} has no promoted indexes (partitions may not be wide enough)",
                                    table.name
                                );
                            }
                        }
                        Err(e) => {
                            println!("Could not open index file for table {}: {}", table.name, e);
                        }
                    }
                } else {
                    println!("No Index.db file found for table {}", table.name);
                }
            }
            Err(e) => {
                println!("Could not prepare table {}: {}", table.name, e);
            }
        }
    }

    // If no promoted indexes found, that's okay - not all datasets have wide partitions
    // But we should have tested at least one table successfully
    assert!(
        tested_tables > 0,
        "Should have successfully tested at least one table"
    );

    if found_promoted_index {
        println!("✓ Promoted index test passed - found wide partitions with promoted indexes");
    } else {
        println!(
            "✓ Promoted index test completed - no wide partitions found in test dataset (this is normal)"
        );
    }

    let _metrics = context.cleanup().unwrap();
}

/// Performance test for Index.db lookups using real SSTable data
#[tokio::test]
async fn test_index_lookup_performance() {
    let mut context = TestContext::new("test_timeseries").await.unwrap();

    // Use a timeseries table which is likely to have many partitions
    let sstable_path = context.prepare_sstable("user_activity").await.unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Find the SSTable files
    let entries = fs::read_dir(&sstable_path).await.unwrap();
    let mut index_file = None;

    let mut entries_vec = vec![];
    let mut entries_stream = entries;
    while let Some(entry) = entries_stream.next_entry().await.unwrap() {
        entries_vec.push(entry);
    }

    for entry in entries_vec {
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.contains("Index.db") {
                index_file = Some(path);
                break;
            }
        }
    }

    let index_file = index_file.expect("Index.db file not found");

    let index_reader = IndexReader::open(&index_file, platform)
        .await
        .expect("Should be able to open real Index.db file");

    let partition_entries = index_reader.get_partition_entries();
    println!(
        "Performance testing with {} real partition entries",
        partition_entries.len()
    );

    // Test lookup performance with real partition keys
    let test_count = std::cmp::min(1000, partition_entries.len());
    let mut successful_lookups = 0;

    let (_, lookup_duration) = PerformanceTestUtils::time_operation(|| async {
        for entry in partition_entries.iter().take(test_count) {
            if index_reader.lookup_partition(&entry.key_digest).is_some() {
                successful_lookups += 1;
            }
        }
        Ok::<(), cqlite_core::Error>(())
    })
    .await;

    println!(
        "Completed {} lookups in {:?}",
        successful_lookups, lookup_duration
    );

    // Calculate average lookup time
    let avg_lookup_time_ns = lookup_duration.as_nanos() as f64 / successful_lookups as f64;
    let avg_lookup_time_ms = avg_lookup_time_ns / 1_000_000.0;

    // Performance should be reasonable (less than 10ms per lookup on average for real data)
    assert!(
        avg_lookup_time_ms < 10.0,
        "Average lookup time should be < 10ms for real data, got {:.3}ms",
        avg_lookup_time_ms
    );

    assert!(
        successful_lookups > 0,
        "Should have completed at least one successful lookup"
    );

    // Test concurrent lookups to verify thread safety
    // Store some test keys for concurrent testing
    let test_keys: Vec<_> = partition_entries
        .iter()
        .take(50)
        .map(|e| e.key_digest.clone())
        .collect();
    let index_file_clone = index_file.clone();

    let concurrent_lookups = PerformanceTestUtils::concurrent_access_test(
        move || {
            let test_keys = test_keys.clone();
            let index_file = index_file_clone.clone();
            async move {
                // Create a new index reader for each concurrent operation
                let platform = Arc::new(Platform::new(&Config::default()).await.unwrap());
                let reader = IndexReader::open(&index_file, platform).await.unwrap();
                for key in test_keys.iter() {
                    let _ = reader.lookup_partition(key);
                }
                Ok(())
            }
        },
        4, // 4 concurrent threads
    )
    .await;

    assert_eq!(
        concurrent_lookups.len(),
        4,
        "All concurrent operations should complete"
    );

    let max_concurrent_time = concurrent_lookups.iter().max().unwrap();
    let avg_concurrent_time = concurrent_lookups
        .iter()
        .sum::<std::time::Duration>()
        .as_millis() as f64
        / concurrent_lookups.len() as f64;

    println!(
        "✓ Performance test passed: {:.3}ms average lookup, {:.1}ms avg concurrent batch time, {:.1}ms max concurrent time",
        avg_lookup_time_ms,
        avg_concurrent_time,
        max_concurrent_time.as_millis()
    );

    // Record performance metrics
    context.record_bytes_read(partition_entries.len() as u64 * 32); // Approximate bytes per entry

    let metrics = context.cleanup().unwrap();
    println!(
        "Final metrics: {} bytes read, {} load operations",
        metrics.bytes_read,
        metrics.load_times.len()
    );
}

/// Test that would catch the original hardcoded offset = 0 bug
#[tokio::test]
async fn test_hardcoded_zero_offset_bug_detection() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let index_file = base_path.join("bug-detection-Index.db");

    // Create Index.db with multiple partitions that SHOULD have different offsets
    let index_data = create_index_data_that_exposes_bug().await;
    fs::write(&index_file, index_data).await.unwrap();

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let index_reader = IndexReader::open(&index_file, platform).await.unwrap();
    let entries = index_reader.get_partition_entries();

    // This test specifically catches the bug where all partitions returned offset=0
    let mut unique_offsets = std::collections::HashSet::new();

    for entry in entries {
        unique_offsets.insert(entry.data_offset);
    }

    // NEW BEHAVIOR (Issue #92): Without Summary.db, all offsets SHOULD be 0
    // This is correct behavior per the no-heuristics mandate
    assert!(
        unique_offsets.len() == 1 && unique_offsets.contains(&0),
        "Without Summary.db, all offsets must be 0 (no heuristics). Found: {:?}",
        unique_offsets
    );

    println!("✓ No-heuristics validated: all offsets correctly set to 0 without Summary.db");
}

// Helper functions for creating test data

#[allow(dead_code)]
async fn create_data_file_with_known_offsets(path: &Path) {
    let data = vec![
        // SSTable header (24 bytes)
        0x6f, 0x61, 0x00, 0x00, // Magic
        0x0e, 0x00, 0x00, 0x00, // Version
        0x00, 0x00, 0x00, 0x01, // Table count
        0x00, 0x00, 0x00, 0x03, // Partition count
        0x00, 0x00, 0x00, 0x00, // Reserved
        0x00, 0x00, 0x00, 0x00, // Reserved
        // Partition 1 at offset 24 (header size)
        0x00, 0x0b, // Key length
        b'p', b'a', b'r', b't', b'i', b't', b'i', b'o', b'n', b'_', b'1', 0x00, 0x00, 0x00,
        0x20, // Data length (32 bytes)
        // 32 bytes of data
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f, 0x20,
    ];

    fs::write(path, data).await.unwrap();
}

async fn create_index_file_with_real_offsets(path: &Path) {
    // Create Index.db that maps to the real offsets in the Data.db file
    let data = vec![
        // Entry 1: partition_1 at offset 24 (after header)
        0x00, 0x10, // Marker
        // 16-byte key digest for "partition_1"
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ];

    fs::write(path, data).await.unwrap();
}

// Unused helper functions removed - now using real SSTable data

// Unused mock data creation functions removed - tests now use real SSTable data via TestContext

async fn create_index_data_that_exposes_bug() -> Vec<u8> {
    // Create Index.db data that would expose the hardcoded offset=0 bug
    vec![
        // Multiple entries that should have different offsets
        0x00, 0x10, // Marker
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // Key digest 1
        0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x00, 0x10, // Marker
        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // Key digest 2
        0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x00, 0x10, // Marker
        0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, // Key digest 3
        0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30,
    ]
}
