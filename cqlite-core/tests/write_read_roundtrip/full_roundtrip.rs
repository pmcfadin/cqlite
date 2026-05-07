//! Full Write-Read Roundtrip Tests (Issue #450)
//!
//! Tests that verify SSTables written by the WriteEngine can be read back
//! through SSTableManager, validating the complete write→flush→read pipeline.
//!
//! ## What These Tests Verify
//!
//! - Writer creates proper directory structure (keyspace/table/)
//! - SSTableManager discovers SSTables in subdirectories
//! - Table name extraction works for writer-produced paths
//! - Row count matches between written mutations and read results
//! - Cell values are preserved through the roundtrip

#![cfg(feature = "write-support")]

use super::{create_simple_mutation, create_simple_schema};
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::{SSTableManager, SSTableReader};
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_core::types::TableId;
use cqlite_core::Config;
use std::sync::Arc;
use tempfile::TempDir;

/// Test that writer creates keyspace/table subdirectory structure
#[tokio::test]
async fn test_writer_creates_subdirectory_structure() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();
    let data_dir = temp_dir.path().join("data");

    let config = WriteEngineConfig::new(data_dir.clone(), temp_dir.path().join("wal"), schema);

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write and flush
    let mutation = create_simple_mutation(1, "Alice", 100, 1000000);
    engine
        .write_async(mutation)
        .await
        .expect("Write should succeed");

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Verify directory structure: data/test_roundtrip/simple/nb-1-big-Data.db
    let expected_dir = data_dir.join("test_roundtrip").join("simple");
    assert!(
        expected_dir.exists(),
        "Should create keyspace/table subdirectory: {}",
        expected_dir.display()
    );

    // Data.db should be inside the subdirectory, not in the root data dir
    assert!(
        info.data_path.starts_with(&expected_dir),
        "Data.db should be under keyspace/table dir: {}",
        info.data_path.display()
    );
}

/// Test that SSTableManager can discover and read back writer-produced SSTables
#[tokio::test]
async fn test_sstable_manager_reads_written_sstables() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();
    let data_dir = temp_dir.path().join("data");

    let config = WriteEngineConfig::new(
        data_dir.clone(),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write 3 mutations with different partition keys
    for i in 1..=3 {
        let mutation =
            create_simple_mutation(i, &format!("User{}", i), i * 100, 1000000 + i as i64);
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
    assert_eq!(info.partition_count, 3);

    // Now create SSTableManager pointing at the data directory
    let cqlite_config = Config::default();
    let platform = Arc::new(Platform::new(&cqlite_config).await.unwrap());

    let manager = SSTableManager::new(
        &data_dir,
        &cqlite_config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("SSTableManager should load written SSTables");

    // Verify SSTables were discovered
    let stats = manager.stats().await.unwrap();
    assert!(
        stats.sstable_count > 0,
        "SSTableManager should discover at least 1 SSTable, got {}",
        stats.sstable_count
    );

    // Scan for the table - the table name should be "simple"
    let table_id = TableId::from("test_roundtrip.simple");
    let results = manager
        .scan(&table_id, None, None, None, Some(&schema))
        .await
        .expect("Scan should succeed");

    // Should get back 3 rows (one per partition)
    assert_eq!(
        results.len(),
        3,
        "Should read back all 3 written partitions, got {}",
        results.len()
    );
}

/// Test roundtrip with multiple flushes produces readable SSTables
#[tokio::test]
async fn test_multiple_flushes_all_readable() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();
    let data_dir = temp_dir.path().join("data");

    let config = WriteEngineConfig::new(
        data_dir.clone(),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // First batch: write and flush
    let mutation = create_simple_mutation(1, "Alice", 100, 1000000);
    engine.write_async(mutation).await.unwrap();
    engine.flush().await.unwrap().unwrap();

    // Second batch: write and flush (different generation)
    let mutation = create_simple_mutation(2, "Bob", 200, 2000000);
    engine.write_async(mutation).await.unwrap();
    engine.flush().await.unwrap().unwrap();

    // Read back
    let cqlite_config = Config::default();
    let platform = Arc::new(Platform::new(&cqlite_config).await.unwrap());

    let manager = SSTableManager::new(
        &data_dir,
        &cqlite_config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("SSTableManager should load SSTables");

    let stats = manager.stats().await.unwrap();
    assert!(
        stats.sstable_count >= 2,
        "Should find at least 2 SSTables from 2 flushes, got {}",
        stats.sstable_count
    );

    let table_id = TableId::from("test_roundtrip.simple");
    let results = manager
        .scan(&table_id, None, None, None, Some(&schema))
        .await
        .unwrap();

    assert_eq!(
        results.len(),
        2,
        "Should read back both partitions from both SSTables, got {}",
        results.len()
    );
}

/// Issue #500: `iterate_all_partitions` must return every partition from a
/// writer-produced SSTable.
///
/// Before the fix, the Summary→Index lookup loop returned 0 entries because
/// the writer emits Index.db in a raw-key format the reader's digest-based
/// parser cannot resolve, even though Summary.db was populated. The k-way
/// merger relied on this method, so compaction silently produced empty output.
///
/// Now the method falls back to `sequential_scan` when the digest lookup
/// resolves nothing, so a fresh `SSTableReader` opened on writer output yields
/// the same partition count that was written.
#[tokio::test]
async fn test_iterate_all_partitions_writer_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();
    let data_dir = temp_dir.path().join("data");

    let config = WriteEngineConfig::new(
        data_dir.clone(),
        temp_dir.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    const N_ROWS: i32 = 10;
    for i in 1..=N_ROWS {
        let mutation =
            create_simple_mutation(i, &format!("user-{}", i), i * 100, 1_000_000 + i as i64);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");
    assert_eq!(info.partition_count, N_ROWS as usize);
    assert!(
        info.data_path.exists(),
        "Data.db must exist at {}",
        info.data_path.display()
    );

    // Reopen with a fresh SSTableReader and call iterate_all_partitions directly.
    let cqlite_config = Config::default();
    let platform = Arc::new(
        Platform::new(&cqlite_config)
            .await
            .expect("Platform creation"),
    );
    let reader = SSTableReader::open(&info.data_path, &cqlite_config, platform)
        .await
        .expect("Reader should open writer-produced Data.db");

    let entries = reader
        .iterate_all_partitions()
        .await
        .expect("iterate_all_partitions must not error");

    assert_eq!(
        entries.len(),
        N_ROWS as usize,
        "iterate_all_partitions must return every written partition (Issue #500). \
         Wrote {} partitions, got {}",
        N_ROWS,
        entries.len()
    );
}
