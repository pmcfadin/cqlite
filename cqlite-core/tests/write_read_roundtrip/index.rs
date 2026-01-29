//! Index.db Write-Read Roundtrip Tests
//!
//! Tests that verify Index.db files written by IndexWriter can be
//! correctly parsed by IndexReader.
//!
//! ## What These Tests Verify
//!
//! - Partition entries are correctly written and read back
//! - Data.db offsets are preserved accurately
//! - Token ordering is maintained
//! - Partition key digests match
//!
//! ## Dependencies
//!
//! - Writer: `cqlite_core::storage::sstable::writer::IndexWriter`
//! - Reader: `cqlite_core::storage::sstable::index_reader::IndexReader`

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::storage::sstable::writer::IndexWriter;
use cqlite_core::storage::write_engine::mutation::DecoratedKey;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Test basic Index.db roundtrip with single partition
#[tokio::test]
async fn test_index_roundtrip_single_partition() {
    let temp_dir = TempDir::new().unwrap();
    let index_path = temp_dir.path().join("nb-1-big-Index.db");

    // Create index writer and add a single partition
    let mut writer = IndexWriter::new();
    let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x01]); // token=12345, pk bytes
    writer
        .add_partition(&key, 0)
        .expect("add_partition should succeed"); // offset 0 in Data.db

    // Finalize to bytes
    let index_bytes = writer.finish().expect("IndexWriter finish should succeed");

    // Write to file
    let mut file = File::create(&index_path)
        .await
        .expect("Should create Index.db");
    file.write_all(&index_bytes)
        .await
        .expect("Should write Index.db");
    file.flush().await.expect("Should flush");
    drop(file);

    // Read back using IndexReader
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let reader = IndexReader::open(&index_path, platform)
        .await
        .expect("IndexReader should open Index.db");

    // Verify partition entry count
    let entries = reader.get_partition_entries();
    assert_eq!(entries.len(), 1, "Should have 1 partition entry");

    // Verify offset
    assert_eq!(entries[0].data_offset, 0, "Partition offset should be 0");
}

/// Test Index.db roundtrip with multiple partitions
#[tokio::test]
async fn test_index_roundtrip_multiple_partitions() {
    let temp_dir = TempDir::new().unwrap();
    let index_path = temp_dir.path().join("nb-1-big-Index.db");

    // Create index writer with multiple partitions
    let mut writer = IndexWriter::new();

    // Add partitions in token order (required by Index.db format)
    let partitions = vec![
        (100i64, vec![0x00, 0x00, 0x00, 0x01], 0u64),
        (200i64, vec![0x00, 0x00, 0x00, 0x02], 256u64),
        (300i64, vec![0x00, 0x00, 0x00, 0x03], 512u64),
        (400i64, vec![0x00, 0x00, 0x00, 0x04], 768u64),
        (500i64, vec![0x00, 0x00, 0x00, 0x05], 1024u64),
    ];

    for (token, key_bytes, offset) in &partitions {
        let key = DecoratedKey::new(*token, key_bytes.clone());
        writer
            .add_partition(&key, *offset)
            .expect("add_partition should succeed");
    }

    // Finalize to bytes
    let index_bytes = writer.finish().expect("IndexWriter finish should succeed");

    // Write to file
    let mut file = File::create(&index_path)
        .await
        .expect("Should create Index.db");
    file.write_all(&index_bytes)
        .await
        .expect("Should write Index.db");
    file.flush().await.expect("Should flush");
    drop(file);

    // Read back using IndexReader
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let reader = IndexReader::open(&index_path, platform)
        .await
        .expect("IndexReader should open Index.db");

    // Verify partition count
    let entries = reader.get_partition_entries();
    assert_eq!(entries.len(), 5, "Should have 5 partition entries");

    // Verify offsets are in correct order
    for (i, (_token, _key_bytes, expected_offset)) in partitions.iter().enumerate() {
        assert_eq!(
            entries[i].data_offset, *expected_offset,
            "Partition {} offset should be {}",
            i, expected_offset
        );
    }
}

/// Test Index.db roundtrip via WriteEngine
#[tokio::test]
async fn test_index_roundtrip_via_write_engine() {
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
    for i in 0..10 {
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

    // Verify Index.db exists
    assert!(info.index_path.exists(), "Index.db should exist");

    // Read using IndexReader
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let reader = IndexReader::open(&info.index_path, platform)
        .await
        .expect("IndexReader should open Index.db created by WriteEngine");

    // Verify we have the expected number of partition entries
    let entries = reader.get_partition_entries();
    assert_eq!(
        entries.len(),
        10,
        "Should have 10 partition entries from WriteEngine"
    );

    // Verify offsets are monotonically increasing (partitions are sequential in Data.db)
    for i in 1..entries.len() {
        assert!(
            entries[i].data_offset > entries[i - 1].data_offset,
            "Partition {} offset ({}) should be greater than partition {} offset ({})",
            i,
            entries[i].data_offset,
            i - 1,
            entries[i - 1].data_offset
        );
    }
}

/// Test Index.db with large offsets (>32-bit)
#[tokio::test]
async fn test_index_roundtrip_large_offsets() {
    let temp_dir = TempDir::new().unwrap();
    let index_path = temp_dir.path().join("nb-1-big-Index.db");

    // Create index writer with large offsets (simulating large Data.db)
    let mut writer = IndexWriter::new();

    // Large offsets that require VInt encoding
    let partitions = vec![
        (100i64, vec![0x01], 0u64),
        (200i64, vec![0x02], 1_000_000_000u64), // 1 GB offset
        (300i64, vec![0x03], 5_000_000_000u64), // 5 GB offset
    ];

    for (token, key_bytes, offset) in &partitions {
        let key = DecoratedKey::new(*token, key_bytes.clone());
        writer
            .add_partition(&key, *offset)
            .expect("add_partition should succeed");
    }

    // Finalize to bytes
    let index_bytes = writer.finish().expect("IndexWriter finish should succeed");

    // Write to file
    let mut file = File::create(&index_path)
        .await
        .expect("Should create Index.db");
    file.write_all(&index_bytes)
        .await
        .expect("Should write Index.db");
    file.flush().await.expect("Should flush");
    drop(file);

    // Read back using IndexReader
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let reader = IndexReader::open(&index_path, platform)
        .await
        .expect("IndexReader should open Index.db with large offsets");

    // Verify partition count
    let entries = reader.get_partition_entries();
    assert_eq!(entries.len(), 3, "Should have 3 partition entries");

    // Verify large offsets are preserved
    assert_eq!(entries[0].data_offset, 0, "First offset should be 0");
    assert_eq!(
        entries[1].data_offset, 1_000_000_000,
        "Second offset should be 1GB"
    );
    assert_eq!(
        entries[2].data_offset, 5_000_000_000,
        "Third offset should be 5GB"
    );
}

/// Test Index.db partition key digest calculation
#[tokio::test]
async fn test_index_partition_key_digest() {
    let temp_dir = TempDir::new().unwrap();
    let index_path = temp_dir.path().join("nb-1-big-Index.db");

    // Create index writer
    let mut writer = IndexWriter::new();

    // Known partition key
    let pk_bytes = vec![0x00, 0x00, 0x00, 0x2A]; // int 42 in big-endian
    let key = DecoratedKey::new(12345, pk_bytes.clone());
    writer
        .add_partition(&key, 0)
        .expect("add_partition should succeed");

    // Calculate expected MD5 digest
    let expected_digest = md5::compute(&pk_bytes);

    // Finalize to bytes
    let index_bytes = writer.finish().expect("IndexWriter finish should succeed");

    // Write to file
    let mut file = File::create(&index_path)
        .await
        .expect("Should create Index.db");
    file.write_all(&index_bytes)
        .await
        .expect("Should write Index.db");
    file.flush().await.expect("Should flush");
    drop(file);

    // Read back using IndexReader
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let reader = IndexReader::open(&index_path, platform)
        .await
        .expect("IndexReader should open Index.db");

    // Verify partition entry
    let entries = reader.get_partition_entries();
    assert_eq!(entries.len(), 1, "Should have 1 partition entry");

    // Verify key digest matches MD5 of partition key bytes
    assert_eq!(
        entries[0].key_digest.as_ref(),
        expected_digest.as_slice(),
        "Partition key digest should be MD5 of key bytes"
    );
}
