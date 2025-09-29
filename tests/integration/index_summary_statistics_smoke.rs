//! Index, Summary, and Statistics database smoke tests
//!
//! Validates core functionality of Index.db, Summary.db, and Statistics.db components
//! using real Cassandra 5 datasets and canonical dataset helpers.
//! Uses *-Data.db prefix derivation for companion file discovery.

#![allow(unused_imports)]
#![allow(dead_code)]

use cqlite_core::testing::dataset_helpers::{list_tables, resolve_table_to_sstable_path};
use cqlite_core::{
    platform::Platform,
    storage::sstable::{index_reader::IndexReader, SSTableReader},
    Config, Result,
};
use std::{path::Path, sync::Arc};

/// Index.db: digest extraction produces correct results for M1 validation
#[tokio::test]
async fn test_index_digest_extraction_for_m1() -> Result<()> {
    // Use first 2 available tables for deterministic testing
    let available_tables = list_tables(None)
        .map_err(|e| cqlite_core::Error::corruption(format!("Dataset error: {e}")))?;

    for table_info in available_tables.iter().take(2) {
        let sstable_dir = resolve_table_to_sstable_path(&table_info.keyspace, &table_info.table)
            .map_err(|e| cqlite_core::Error::corruption(format!("Dataset error: {e}")))?;

        // Find Data.db and derive Index.db companion
        let data_file = find_data_file(&sstable_dir)?;
        let index_file = derive_companion_file(&data_file, "Index.db")?;

        // Skip if Index.db doesn't exist (some tables may not have it)
        if !index_file.exists() {
            continue;
        }

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);

        // Use IndexReader for Index.db files, not SSTableReader
        let index_reader = IndexReader::open(&index_file, platform.clone()).await?;

        // Validate that we can extract partition digests (M1 requirement)
        let partition_entries = index_reader.get_partition_entries();

        // Basic validation that digest extraction works
        // Note: Some Index.db files may be empty, which is valid for M1 scope
        if !partition_entries.is_empty() {
            println!("Found {} partition entries", partition_entries.len());
        }

        // Validate digest format if present
        for entry in partition_entries.iter().take(5) {
            // Check first 5 for performance
            assert_eq!(entry.key_digest.len(), 16, "Key digest should be 16 bytes");
        }

        let stats = index_reader.get_statistics();
        println!(
            "Index stats for {}.{}: {} partitions",
            table_info.keyspace, table_info.table, stats.total_partitions
        );
    }
    Ok(())
}

/// Summary.db: basic TOC validation passes (M1 scope)
#[tokio::test]
async fn test_summary_basic_toc_validation() -> Result<()> {
    let available_tables = list_tables(None)
        .map_err(|e| cqlite_core::Error::corruption(format!("Dataset error: {e}")))?;

    for table_info in available_tables.iter().take(2) {
        let sstable_dir = resolve_table_to_sstable_path(&table_info.keyspace, &table_info.table)
            .map_err(|e| cqlite_core::Error::corruption(format!("Dataset error: {e}")))?;

        let data_file = find_data_file(&sstable_dir)?;
        let summary_file = derive_companion_file(&data_file, "Summary.db")?;

        // Skip if Summary.db doesn't exist
        if !summary_file.exists() {
            continue;
        }

        // For M1 scope: just validate that Summary.db exists and is readable
        // Extended Summary.db parsing is gated for post-M1
        let metadata = std::fs::metadata(&summary_file).map_err(|e| {
            cqlite_core::Error::corruption(format!("Cannot read Summary.db metadata: {e}"))
        })?;

        assert!(metadata.len() > 0, "Summary.db should not be empty");
        println!(
            "Summary.db for {}.{}: {} bytes",
            table_info.keyspace,
            table_info.table,
            metadata.len()
        );
    }
    Ok(())
}

/// Statistics.db: basic file validation (M1 scope)
#[tokio::test]
async fn test_statistics_basic_validation() -> Result<()> {
    let available_tables = list_tables(None)
        .map_err(|e| cqlite_core::Error::corruption(format!("Dataset error: {e}")))?;

    for table_info in available_tables.iter().take(2) {
        let sstable_dir = resolve_table_to_sstable_path(&table_info.keyspace, &table_info.table)
            .map_err(|e| cqlite_core::Error::corruption(format!("Dataset error: {e}")))?;

        let data_file = find_data_file(&sstable_dir)?;
        let stats_file = derive_companion_file(&data_file, "Statistics.db")?;

        // Skip if Statistics.db doesn't exist
        if !stats_file.exists() {
            continue;
        }

        // For M1 scope: just validate that Statistics.db exists and is readable
        // Extended Statistics.db parsing is gated for post-M1
        let metadata = std::fs::metadata(&stats_file).map_err(|e| {
            cqlite_core::Error::corruption(format!("Cannot read Statistics.db metadata: {e}"))
        })?;

        assert!(metadata.len() > 0, "Statistics.db should not be empty");
        println!(
            "Statistics.db for {}.{}: {} bytes",
            table_info.keyspace,
            table_info.table,
            metadata.len()
        );
    }
    Ok(())
}

/// Find *-Data.db file in table directory
fn find_data_file(sstable_dir: &Path) -> Result<std::path::PathBuf> {
    let entries = std::fs::read_dir(sstable_dir).map_err(|e| {
        cqlite_core::Error::corruption(format!("Failed to read SSTable directory: {e}"))
    })?;

    for entry in entries {
        let entry = entry
            .map_err(|e| cqlite_core::Error::corruption(format!("Directory entry error: {e}")))?;
        let path = entry.path();

        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with("-Data.db") {
                return Ok(path);
            }
        }
    }

    Err(cqlite_core::Error::corruption(
        "No *-Data.db file found".to_string(),
    ))
}

/// Derive companion file from Data.db prefix
/// nb-1-big-Data.db → nb-1-big-Index.db, nb-1-big-Summary.db, nb-1-big-Statistics.db
fn derive_companion_file(data_file: &Path, companion_type: &str) -> Result<std::path::PathBuf> {
    let data_name = data_file
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| cqlite_core::Error::corruption("Invalid Data.db filename".to_string()))?;

    if !data_name.ends_with("-Data.db") {
        return Err(cqlite_core::Error::corruption(
            "File is not a *-Data.db file".to_string(),
        ));
    }

    // Extract prefix: "nb-1-big-Data.db" → "nb-1-big"
    let prefix = &data_name[..data_name.len() - "-Data.db".len()];
    let companion_name = format!("{prefix}-{companion_type}");

    let companion_path = data_file
        .parent()
        .ok_or_else(|| {
            cqlite_core::Error::corruption("Data.db has no parent directory".to_string())
        })?
        .join(companion_name);

    Ok(companion_path)
}
