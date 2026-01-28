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
    let summary_bytes = writer.finish().expect("SummaryWriter finish should succeed");

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
    let platform = Arc::new(Platform::new(&config).await.expect("Platform creation should succeed"));
    let reader = SummaryReader::open(&summary_path, platform)
        .await
        .expect("SummaryReader should open Summary.db");

    // Verify entry count
    let entries = reader.get_entries();
    assert!(!entries.is_empty(), "Should have at least 1 summary entry");

    // Verify first and last keys
    let first_key = reader.get_first_key();
    let last_key = reader.get_last_key();
    assert!(
        !first_key.is_empty(),
        "First key should not be empty"
    );
    assert!(
        !last_key.is_empty(),
        "Last key should not be empty"
    );
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
        writer.add_entry(&key, *position).expect("add_entry should succeed");
    }

    // Finalize to bytes
    let summary_bytes = writer.finish().expect("SummaryWriter finish should succeed");

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
    let platform = Arc::new(Platform::new(&config).await.expect("Platform creation should succeed"));
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
    let platform = Arc::new(Platform::new(&config).await.expect("Platform creation should succeed"));
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
        writer.add_entry(&key, i as u64 * 256).expect("add_entry should succeed");
    }

    // Finalize to bytes
    let summary_bytes = writer.finish().expect("SummaryWriter finish should succeed");

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
    let platform = Arc::new(Platform::new(&config).await.expect("Platform creation should succeed"));
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
        (200i64, vec![0x02], 1_000_000_000u64),        // 1 GB
        (300i64, vec![0x03], 10_000_000_000u64),       // 10 GB
        (400i64, vec![0x04], 100_000_000_000u64),      // 100 GB
    ];

    for (token, key_bytes, position) in &entries_data {
        let key = DecoratedKey::new(*token, key_bytes.clone());
        writer.add_entry(&key, *position).expect("add_entry should succeed");
    }

    // Finalize to bytes
    let summary_bytes = writer.finish().expect("SummaryWriter finish should succeed");

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
    let platform = Arc::new(Platform::new(&config).await.expect("Platform creation should succeed"));
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
        writer.add_entry(&key, *position).expect("add_entry should succeed");
    }

    // Finalize to bytes
    let summary_bytes = writer.finish().expect("SummaryWriter finish should succeed");

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
    let platform = Arc::new(Platform::new(&config).await.expect("Platform creation should succeed"));
    let reader = SummaryReader::open(&summary_path, platform)
        .await
        .expect("SummaryReader should open Summary.db");

    // Test find_entry_for_position for various target positions
    // Should find the entry at or before the target position

    // Position 500 should find entry at position 0 (before first boundary)
    let entry = reader.find_entry_for_position(500);
    assert!(entry.is_some(), "Should find entry for position 500");
    assert_eq!(
        entry.unwrap().position, 0,
        "Entry for position 500 should be at position 0"
    );

    // Position 1500 should find entry at position 1000
    let entry = reader.find_entry_for_position(1500);
    assert!(entry.is_some(), "Should find entry for position 1500");
    assert_eq!(
        entry.unwrap().position, 1000,
        "Entry for position 1500 should be at position 1000"
    );
}
