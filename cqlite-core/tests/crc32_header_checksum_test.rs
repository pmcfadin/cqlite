//! Tests for CRC32 header checksum detection (Issue #153)
//!
//! Validates that headers with CRC32 checksums are correctly detected,
//! validated, and parsed.

use cqlite_core::{storage::sstable::reader::SSTableReader, Config, Platform};
use std::path::PathBuf;
use std::sync::Arc;

fn get_test_data_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("test-data/datasets"))
}

#[tokio::test]
async fn test_collection_clustering_table_with_crc32_checksum() {
    let test_root = get_test_data_root();
    let data_db_path = test_root.join(
        "sstables/test_collections/collection_clustering_table-6bf78680a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
    );

    if !data_db_path.exists() {
        println!("⚠️  Skipping: collection_clustering_table Data.db not found");
        return;
    }

    // Initialize Platform and Config
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("Platform init failed"));

    // This should succeed with the CRC32 checksum detection
    let result = SSTableReader::open(&data_db_path, &config, platform.clone()).await;

    match result {
        Ok(reader) => {
            println!("✅ Successfully opened collection_clustering_table with CRC32 checksum");
            println!("   SSTable generation: {}", reader.header().generation);
        }
        Err(e) => {
            panic!(
                "Failed to open collection_clustering_table: {}. \
                 This table has a CRC32 checksum prefix (0x71160000) that should be detected and validated.",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_empty_collections_table_with_crc32_checksum() {
    let test_root = get_test_data_root();
    let data_db_path = test_root.join(
        "sstables/test_collections/empty_collections_table-6be780f0a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
    );

    if !data_db_path.exists() {
        println!("⚠️  Skipping: empty_collections_table Data.db not found");
        return;
    }

    // Initialize Platform and Config
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("Platform init failed"));

    // This should succeed with the CRC32 checksum detection
    let result = SSTableReader::open(&data_db_path, &config, platform.clone()).await;

    match result {
        Ok(reader) => {
            println!("✅ Successfully opened empty_collections_table with CRC32 checksum");
            println!("   SSTable generation: {}", reader.header().generation);
        }
        Err(e) => {
            panic!(
                "Failed to open empty_collections_table: {}. \
                 This table has a CRC32 checksum prefix (0xf1185c00) that should be detected and validated.",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_all_collection_tables_open_successfully() {
    let test_root = get_test_data_root();
    let collections_dir = test_root.join("sstables/test_collections");

    if !collections_dir.exists() {
        println!("⚠️  Skipping: test_collections directory not found");
        return;
    }

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("Platform init failed"));

    let mut tested = 0;
    let mut succeeded = 0;
    let mut failed = Vec::new();

    // Iterate through all collection table directories
    let mut entries = tokio::fs::read_dir(&collections_dir)
        .await
        .expect("Failed to read collections directory");

    while let Some(entry) = entries.next_entry().await.expect("Failed to read entry") {
        let dir_path = entry.path();
        if !dir_path.is_dir() {
            continue;
        }

        // Look for Data.db file
        let table_name = dir_path.file_name().unwrap().to_string_lossy().to_string();
        let data_db_candidates = vec![
            dir_path.join("nb-1-big-Data.db"),
            dir_path.join("oa-1-big-Data.db"),
        ];

        for data_db_path in data_db_candidates {
            if !data_db_path.exists() {
                continue;
            }

            tested += 1;
            println!("Testing: {}", table_name);

            match SSTableReader::open(&data_db_path, &config, platform.clone()).await {
                Ok(_reader) => {
                    println!("  ✅ Opened successfully");
                    succeeded += 1;
                }
                Err(e) => {
                    println!("  ❌ Failed: {}", e);
                    failed.push((table_name.clone(), e.to_string()));
                }
            }
            break;
        }
    }

    if tested == 0 {
        println!("⚠️  No collection tables found to test");
        return;
    }

    println!(
        "\nSummary: {}/{} tables opened successfully",
        succeeded, tested
    );

    if !failed.is_empty() {
        println!("\nFailed tables:");
        for (name, error) in &failed {
            println!("  - {}: {}", name, error);
        }
        panic!(
            "{}/{} collection tables failed to open. See details above.",
            failed.len(),
            tested
        );
    }

    assert!(
        succeeded > 0,
        "At least one collection table should open successfully"
    );
}
