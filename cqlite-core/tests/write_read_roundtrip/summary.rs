//! Summary.db Write-Read Roundtrip Tests
//!
//! Tests that verify Summary.db files written by SummaryWriter can be
//! correctly parsed by SummaryReader.
//!
//! ## What These Tests Verify
//!
//! - Summary entries round-trip correctly
//! - First/last keys are preserved
//! - Little-endian offset table is correctly written and read
//! - Sampling parameters are preserved
//!
//! ## Dependencies
//!
//! - Writer: `cqlite_core::storage::sstable::writer::SummaryWriter`
//! - Reader: `cqlite_core::storage::sstable::summary_reader::SummaryReader`

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::summary_reader::SummaryReader;
use cqlite_core::storage::sstable::writer::SummaryWriter;
use cqlite_core::storage::write_engine::mutation::DecoratedKey;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Test basic Summary.db roundtrip with single entry
#[tokio::test]
async fn test_summary_roundtrip_single_entry() {
    let temp_dir = TempDir::new().unwrap();
    let summary_path = temp_dir.path().join("nb-1-big-Summary.db");

    // Create summary writer with default sampling interval
    let mut writer = SummaryWriter::new(128);

    // Add a single entry
    let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x01]);
    writer.add_entry(&key, 0).expect("add_entry should succeed");

    // Finalize to bytes
    let summary_bytes = writer
        .finish()
        .expect("SummaryWriter finish should succeed");

    // Write to file
    let mut file = File::create(&summary_path)
        .await
        .expect("Should create Summary.db");
    file.write_all(&summary_bytes)
        .await
        .expect("Should write Summary.db");
    file.flush().await.expect("Should flush");
    drop(file);

    // Read back using SummaryReader
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let reader = SummaryReader::open(&summary_path, platform)
        .await
        .expect("SummaryReader should open Summary.db");

    // Verify entry count
    let entries = reader.get_entries();
    assert!(!entries.is_empty(), "Should have at least 1 summary entry");

    // Verify first and last keys
    let first_key = reader.get_first_key();
    let last_key = reader.get_last_key();
    assert!(!first_key.is_empty(), "First key should not be empty");
    assert!(!last_key.is_empty(), "Last key should not be empty");
}

/// Test Summary.db roundtrip with multiple entries
#[tokio::test]
async fn test_summary_roundtrip_multiple_entries() {
    let temp_dir = TempDir::new().unwrap();
    let summary_path = temp_dir.path().join("nb-1-big-Summary.db");

    // Create summary writer
    let mut writer = SummaryWriter::new(128);

    // Add multiple entries (simulating Index.db sampling)
    let entries_data = vec![
        (100i64, vec![0x00, 0x00, 0x00, 0x01], 0u64),
        (200i64, vec![0x00, 0x00, 0x00, 0x02], 1024u64),
        (300i64, vec![0x00, 0x00, 0x00, 0x03], 2048u64),
        (400i64, vec![0x00, 0x00, 0x00, 0x04], 3072u64),
        (500i64, vec![0x00, 0x00, 0x00, 0x05], 4096u64),
    ];

    for (token, key_bytes, position) in &entries_data {
        let key = DecoratedKey::new(*token, key_bytes.clone());
        writer
            .add_entry(&key, *position)
            .expect("add_entry should succeed");
    }

    // Finalize to bytes
    let summary_bytes = writer
        .finish()
        .expect("SummaryWriter finish should succeed");

    // Write to file
    let mut file = File::create(&summary_path)
        .await
        .expect("Should create Summary.db");
    file.write_all(&summary_bytes)
        .await
        .expect("Should write Summary.db");
    file.flush().await.expect("Should flush");
    drop(file);

    // Read back using SummaryReader
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let reader = SummaryReader::open(&summary_path, platform)
        .await
        .expect("SummaryReader should open Summary.db");

    // Verify entry count matches
    let entries = reader.get_entries();
    assert_eq!(
        entries.len(),
        entries_data.len(),
        "Should have {} summary entries",
        entries_data.len()
    );

    // Verify positions are preserved
    for (i, (_, _, expected_position)) in entries_data.iter().enumerate() {
        assert_eq!(
            entries[i].position, *expected_position,
            "Entry {} position should be {}",
            i, expected_position
        );
    }
}

/// Test Summary.db roundtrip via WriteEngine
#[tokio::test]
async fn test_summary_roundtrip_via_write_engine() {
    use super::{create_simple_mutation, create_simple_schema};
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};

    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write multiple partitions
    for i in 0..20 {
        let mutation = create_simple_mutation(i, &format!("user{}", i), i * 10, 1000000 + i as i64);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    // Flush to create SSTable
    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Verify Summary.db exists
    assert!(info.summary_path.exists(), "Summary.db should exist");

    // Read using SummaryReader
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let reader = SummaryReader::open(&info.summary_path, platform)
        .await
        .expect("SummaryReader should open Summary.db created by WriteEngine");

    // Verify we have entries
    let entries = reader.get_entries();
    assert!(
        !entries.is_empty(),
        "Summary.db should have entries from WriteEngine"
    );

    // Verify first/last keys exist
    let first_key = reader.get_first_key();
    let last_key = reader.get_last_key();
    assert!(!first_key.is_empty(), "First key should exist");
    assert!(!last_key.is_empty(), "Last key should exist");

    // First key should differ from last key (multiple partitions)
    if entries.len() > 1 {
        assert_ne!(
            first_key, last_key,
            "First and last keys should differ for multiple partitions"
        );
    }
}

/// Test Summary.db header parameters
#[tokio::test]
async fn test_summary_header_parameters() {
    let temp_dir = TempDir::new().unwrap();
    let summary_path = temp_dir.path().join("nb-1-big-Summary.db");

    // Create summary writer with specific interval
    let min_index_interval = 64u32;
    let mut writer = SummaryWriter::new(min_index_interval);

    // Add entries
    for i in 0..10 {
        let key = DecoratedKey::new(i as i64 * 100, vec![0x00, 0x00, 0x00, i as u8]);
        writer
            .add_entry(&key, i as u64 * 256)
            .expect("add_entry should succeed");
    }

    // Finalize to bytes
    let summary_bytes = writer
        .finish()
        .expect("SummaryWriter finish should succeed");

    // Write to file
    let mut file = File::create(&summary_path)
        .await
        .expect("Should create Summary.db");
    file.write_all(&summary_bytes)
        .await
        .expect("Should write Summary.db");
    file.flush().await.expect("Should flush");
    drop(file);

    // Read back using SummaryReader
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let reader = SummaryReader::open(&summary_path, platform)
        .await
        .expect("SummaryReader should open Summary.db");

    // Verify header parameters
    let header = reader.get_header();
    assert_eq!(
        header.min_index_interval, min_index_interval,
        "Min index interval should be preserved"
    );
    assert!(
        header.entries_count > 0,
        "Entries count in header should be > 0"
    );
}

/// Test Summary.db with large positions (testing little-endian encoding)
#[tokio::test]
async fn test_summary_large_positions() {
    let temp_dir = TempDir::new().unwrap();
    let summary_path = temp_dir.path().join("nb-1-big-Summary.db");

    // Create summary writer
    let mut writer = SummaryWriter::new(128);

    // Add entries with large positions (testing 8-byte encoding)
    let entries_data = vec![
        (100i64, vec![0x01], 0u64),
        (200i64, vec![0x02], 1_000_000_000u64),   // 1 GB
        (300i64, vec![0x03], 10_000_000_000u64),  // 10 GB
        (400i64, vec![0x04], 100_000_000_000u64), // 100 GB
    ];

    for (token, key_bytes, position) in &entries_data {
        let key = DecoratedKey::new(*token, key_bytes.clone());
        writer
            .add_entry(&key, *position)
            .expect("add_entry should succeed");
    }

    // Finalize to bytes
    let summary_bytes = writer
        .finish()
        .expect("SummaryWriter finish should succeed");

    // Write to file
    let mut file = File::create(&summary_path)
        .await
        .expect("Should create Summary.db");
    file.write_all(&summary_bytes)
        .await
        .expect("Should write Summary.db");
    file.flush().await.expect("Should flush");
    drop(file);

    // Read back using SummaryReader
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let reader = SummaryReader::open(&summary_path, platform)
        .await
        .expect("SummaryReader should open Summary.db with large positions");

    // Verify large positions are preserved
    let entries = reader.get_entries();
    assert_eq!(entries.len(), 4, "Should have 4 entries");

    assert_eq!(entries[0].position, 0, "First position should be 0");
    assert_eq!(
        entries[1].position, 1_000_000_000,
        "Second position should be 1GB"
    );
    assert_eq!(
        entries[2].position, 10_000_000_000,
        "Third position should be 10GB"
    );
    assert_eq!(
        entries[3].position, 100_000_000_000,
        "Fourth position should be 100GB"
    );
}

/// Test Summary.db entry lookup functionality
#[tokio::test]
async fn test_summary_entry_lookup() {
    let temp_dir = TempDir::new().unwrap();
    let summary_path = temp_dir.path().join("nb-1-big-Summary.db");

    // Create summary writer
    let mut writer = SummaryWriter::new(128);

    // Add entries at known positions
    let entries_data = vec![
        (100i64, vec![0x00, 0x00, 0x00, 0x01], 0u64),
        (200i64, vec![0x00, 0x00, 0x00, 0x02], 1000u64),
        (300i64, vec![0x00, 0x00, 0x00, 0x03], 2000u64),
        (400i64, vec![0x00, 0x00, 0x00, 0x04], 3000u64),
    ];

    for (token, key_bytes, position) in &entries_data {
        let key = DecoratedKey::new(*token, key_bytes.clone());
        writer
            .add_entry(&key, *position)
            .expect("add_entry should succeed");
    }

    // Finalize to bytes
    let summary_bytes = writer
        .finish()
        .expect("SummaryWriter finish should succeed");

    // Write to file
    let mut file = File::create(&summary_path)
        .await
        .expect("Should create Summary.db");
    file.write_all(&summary_bytes)
        .await
        .expect("Should write Summary.db");
    file.flush().await.expect("Should flush");
    drop(file);

    // Read back using SummaryReader
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let reader = SummaryReader::open(&summary_path, platform)
        .await
        .expect("SummaryReader should open Summary.db");

    // Test find_entry_for_position for various target positions
    // Should find the entry at or before the target position

    // Position 500 should find entry at position 0 (before first boundary)
    let entry = reader.find_entry_for_position(500);
    assert!(entry.is_some(), "Should find entry for position 500");
    assert_eq!(
        entry.unwrap().position,
        0,
        "Entry for position 500 should be at position 0"
    );

    // Position 1500 should find entry at position 1000
    let entry = reader.find_entry_for_position(1500);
    assert!(entry.is_some(), "Should find entry for position 1500");
    assert_eq!(
        entry.unwrap().position,
        1000,
        "Entry for position 1500 should be at position 1000"
    );
}

/// Test Summary.db offset tracking with Index.db integration (Issue #407)
///
/// Verifies that Summary.db offsets point to the correct byte positions in Index.db.
/// This test addresses the critical requirement that Summary.db entries must point to
/// the exact locations where Index.db entries start.
#[tokio::test]
async fn test_summary_offset_tracking_with_index() {
    use cqlite_core::storage::sstable::index_reader::IndexReader;
    use cqlite_core::storage::sstable::writer::IndexWriter;

    let temp_dir = TempDir::new().unwrap();
    let index_path = temp_dir.path().join("nb-1-big-Index.db");
    let summary_path = temp_dir.path().join("nb-1-big-Summary.db");

    // Create Index.db and Summary.db simultaneously (simulating SSTableWriter flow)
    let mut index_writer = IndexWriter::new();
    let mut summary_writer = SummaryWriter::new(128);

    // Write 384 partitions (3 summary samples at 0, 128, 256)
    let mut summary_sample_offsets = Vec::new();
    for i in 0..384 {
        let token = (i * 1000) as i64;
        let key_bytes = vec![0x00, 0x00, 0x00, (i % 256) as u8];
        let data_offset = (i * 100) as u64;

        let key = DecoratedKey::new(token, key_bytes);

        // Write to Index.db and capture entry info
        let entry_info = index_writer
            .add_partition(&key, data_offset)
            .expect("add_partition should succeed");

        // Sample every 128th entry for Summary.db
        if i % 128 == 0 {
            summary_writer
                .add_entry(&key, entry_info.index_offset)
                .expect("add_entry should succeed");
            summary_sample_offsets.push((i, entry_info.index_offset));
        }
    }

    // Finalize both files
    let index_bytes = index_writer
        .finish()
        .expect("IndexWriter finish should succeed");
    let summary_bytes = summary_writer
        .finish()
        .expect("SummaryWriter finish should succeed");

    // Write files
    let mut index_file = File::create(&index_path)
        .await
        .expect("Should create Index.db");
    index_file
        .write_all(&index_bytes)
        .await
        .expect("Should write Index.db");
    index_file.flush().await.expect("Should flush Index.db");
    drop(index_file);

    let mut summary_file = File::create(&summary_path)
        .await
        .expect("Should create Summary.db");
    summary_file
        .write_all(&summary_bytes)
        .await
        .expect("Should write Summary.db");
    summary_file.flush().await.expect("Should flush Summary.db");
    drop(summary_file);

    // Read back both files
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );

    let index_reader = IndexReader::open(&index_path, platform.clone())
        .await
        .expect("IndexReader should open Index.db");
    let summary_reader = SummaryReader::open(&summary_path, platform)
        .await
        .expect("SummaryReader should open Summary.db");

    // Get entries from both readers
    let index_entries = index_reader.get_partition_entries();
    let summary_entries = summary_reader.get_entries();

    // Verify we have the expected number of entries
    assert_eq!(
        index_entries.len(),
        384,
        "Index.db should have 384 partition entries"
    );
    assert_eq!(
        summary_entries.len(),
        3,
        "Summary.db should have 3 sampled entries (0, 128, 256)"
    );

    // Critical verification: Summary.db offsets must match actual Index.db entry positions
    // Read raw Index.db to verify marker positions
    let index_bytes_vec = tokio::fs::read(&index_path)
        .await
        .expect("Should read Index.db");

    // Each Summary.db entry should point to the start of the corresponding Index.db entry
    for (sample_idx, summary_entry) in summary_entries.iter().enumerate() {
        let expected_partition_idx = sample_idx * 128;

        // Verify the offset points to the start of an Index.db entry. In the real
        // Cassandra BIG/NB format each entry begins with the partition key LENGTH
        // (u16 BE), not a 0x0010 marker. These keys are 4 bytes, so the entry starts
        // with the length prefix 0x0004 (Issue #552).
        let offset = summary_entry.position as usize;
        assert!(
            offset + 2 <= index_bytes_vec.len(),
            "Summary offset {} should be within Index.db bounds",
            offset
        );

        // Check that the offset points to the Index.db entry's key-length prefix.
        let key_len_bytes = &index_bytes_vec[offset..offset + 2];
        assert_eq!(
            key_len_bytes,
            &[0x00, 0x04],
            "Summary entry {} should point to Index.db entry key-length prefix at offset {}",
            sample_idx,
            offset
        );

        // Verify this matches our tracked offsets
        let (_tracked_idx, tracked_offset) = summary_sample_offsets[sample_idx];
        assert_eq!(
            summary_entry.position, tracked_offset,
            "Summary entry {} offset should match tracked offset from IndexWriter",
            sample_idx
        );

        println!(
            "✓ Summary entry {} points to Index.db partition {} at offset {} (key-length prefix 0x0004)",
            sample_idx, expected_partition_idx, offset
        );
    }

    // Additional verification: compute expected offsets based on entry sizes.
    // Each BIG-format Index.db entry: 2 (key_len) + 4 (raw key) + VInt(data_offset) + VInt(0).
    // For data_offset values 0, 100, 200, ..., most will be 1-byte VInts (< 128).
    // Entry size = 2 + 4 + 1 + 1 = 8 bytes for data_offset < 128.
    // Entry size = 2 + 4 + 2 + 1 = 9 bytes for data_offset >= 128.

    let mut cumulative_offset = 0u64;
    for i in 0..384 {
        let data_offset = (i * 100) as u64;

        // Calculate expected entry size based on VInt encoding
        let vint_size = if data_offset < 128 {
            1
        } else if data_offset < 16384 {
            2
        } else if data_offset < 2097152 {
            3
        } else {
            4 // Sufficient for this test
        };
        // key_len(2) + raw key(4) + data_offset_vint + promoted_len_vint(0)
        let entry_size = 2 + 4 + vint_size + 1;

        // If this is a sampled partition, verify the offset matches
        if i % 128 == 0 {
            let sample_idx = i / 128;
            assert_eq!(
                summary_entries[sample_idx].position, cumulative_offset,
                "Summary entry {} should point to cumulative offset {}",
                sample_idx, cumulative_offset
            );
        }

        cumulative_offset += entry_size;
    }

    println!(
        "✓ Summary.db offset tracking verified for all {} sampled entries",
        summary_entries.len()
    );
}
