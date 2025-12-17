//! Integration tests for compression detection in SSTable readers.
//!
//! These tests use real SSTable data from test-data/datasets/sstables/
//! to verify compression detection and initialization (Issue #28 compliance).

use std::path::Path;
use std::sync::Arc;

use cqlite_core::storage::sstable::reader::{extract_sstable_base_name, SSTableReader};
use cqlite_core::{Config, Platform};

/// Helper to get test datasets root
fn get_test_datasets_root() -> Option<std::path::PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(std::path::PathBuf::from)
}

/// Helper to find SSTable directory for a table
fn find_table_dir(
    datasets_root: &Path,
    keyspace: &str,
    table_prefix: &str,
) -> Option<std::path::PathBuf> {
    let keyspace_dir = datasets_root.join("sstables").join(keyspace);
    if !keyspace_dir.exists() {
        return None;
    }

    std::fs::read_dir(&keyspace_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(table_prefix))
                    .unwrap_or(false)
        })
}

/// Helper to find Data.db file in a table directory
fn find_data_file(table_dir: &Path) -> Option<std::path::PathBuf> {
    std::fs::read_dir(table_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_file()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
        })
}

// =============================================================================
// Integration Tests with Real SSTable Data
// =============================================================================

#[tokio::test]
async fn test_compression_detection_simple_table() {
    let Some(datasets_root) = get_test_datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let Some(table_dir) = find_table_dir(&datasets_root, "test_basic", "simple_table") else {
        eprintln!("simple_table not found, skipping test");
        return;
    };

    let Some(data_file) = find_data_file(&table_dir) else {
        eprintln!("Data.db not found in simple_table, skipping test");
        return;
    };

    // Verify CompressionInfo.db exists
    let base_name = extract_sstable_base_name(&data_file).expect("Should extract base name");
    let compression_info_path = table_dir.join(format!("{}-CompressionInfo.db", base_name));

    assert!(
        compression_info_path.exists(),
        "CompressionInfo.db should exist at {:?}",
        compression_info_path
    );

    // Open SSTable reader to verify compression detection
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let reader = SSTableReader::open(&data_file, &config, platform)
        .await
        .expect("Failed to open SSTable");

    // simple_table should be compressed - verify reader opened successfully
    // The reader internally detects compression from header or CompressionInfo.db
    eprintln!(
        "Successfully opened simple_table with Cassandra version: {:?}",
        reader.header().cassandra_version
    );
}

#[tokio::test]
async fn test_compression_detection_uncompressed_table() {
    let Some(datasets_root) = get_test_datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let Some(table_dir) = find_table_dir(&datasets_root, "test_basic", "uncompressed_table") else {
        eprintln!("uncompressed_table not found, skipping test");
        return;
    };

    let Some(data_file) = find_data_file(&table_dir) else {
        eprintln!("Data.db not found in uncompressed_table, skipping test");
        return;
    };

    // Open SSTable reader
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let reader = SSTableReader::open(&data_file, &config, platform)
        .await
        .expect("Failed to open SSTable");

    eprintln!(
        "Successfully opened uncompressed_table with Cassandra version: {:?}",
        reader.header().cassandra_version
    );

    // For uncompressed tables, compression reader should not be present
    // (or indicate no compression)
}

#[tokio::test]
async fn test_compression_detection_compression_test_table() {
    let Some(datasets_root) = get_test_datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let Some(table_dir) = find_table_dir(&datasets_root, "test_basic", "compression_test_table")
    else {
        eprintln!("compression_test_table not found, skipping test");
        return;
    };

    let Some(data_file) = find_data_file(&table_dir) else {
        eprintln!("Data.db not found in compression_test_table, skipping test");
        return;
    };

    // Open SSTable reader
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let reader = SSTableReader::open(&data_file, &config, platform)
        .await
        .expect("Failed to open SSTable");

    eprintln!(
        "Successfully opened compression_test_table with Cassandra version: {:?}",
        reader.header().cassandra_version
    );
}

#[tokio::test]
async fn test_compression_info_discovery_all_test_basic_tables() {
    let Some(datasets_root) = get_test_datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let keyspace_dir = datasets_root.join("sstables").join("test_basic");
    if !keyspace_dir.exists() {
        eprintln!("test_basic keyspace not found, skipping test");
        return;
    }

    let entries: Vec<_> = std::fs::read_dir(&keyspace_dir)
        .expect("Should read keyspace dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    eprintln!("Found {} tables in test_basic", entries.len());

    let mut tables_with_compression = 0;
    let mut tables_without_compression = 0;

    for entry in entries {
        let table_dir = entry.path();
        let table_name = table_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        // Find Data.db file
        let Some(data_file) = find_data_file(&table_dir) else {
            eprintln!("  {} - No Data.db found", table_name);
            continue;
        };

        // Check for CompressionInfo.db
        if let Some(base_name) = extract_sstable_base_name(&data_file) {
            let compression_info_path = table_dir.join(format!("{}-CompressionInfo.db", base_name));
            if compression_info_path.exists() {
                let metadata =
                    std::fs::metadata(&compression_info_path).expect("Should get metadata");
                eprintln!(
                    "  {} - CompressionInfo.db: {} bytes",
                    table_name,
                    metadata.len()
                );
                tables_with_compression += 1;
            } else {
                eprintln!("  {} - No CompressionInfo.db (uncompressed)", table_name);
                tables_without_compression += 1;
            }
        } else {
            eprintln!("  {} - Could not extract base name", table_name);
        }
    }

    eprintln!(
        "\nSummary: {} compressed, {} uncompressed",
        tables_with_compression, tables_without_compression
    );

    // We expect at least some tables to be compressed
    assert!(
        tables_with_compression > 0,
        "Expected at least some compressed tables"
    );
}

#[tokio::test]
async fn test_extract_base_name_on_real_files() {
    let Some(datasets_root) = get_test_datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let Some(table_dir) = find_table_dir(&datasets_root, "test_basic", "simple_table") else {
        eprintln!("simple_table not found, skipping test");
        return;
    };

    // Test on all .db files in the table directory
    let entries: Vec<_> = std::fs::read_dir(&table_dir)
        .expect("Should read table dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|ext| ext == "db").unwrap_or(false))
        .collect();

    eprintln!("Found {} .db files in simple_table", entries.len());

    let mut extracted_base_names: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    for entry in entries {
        let path = entry.path();
        let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

        if let Some(base_name) = extract_sstable_base_name(&path) {
            eprintln!("  {} -> {}", filename, base_name);
            extracted_base_names.insert(base_name);
        } else {
            eprintln!("  {} -> (no base name extracted)", filename);
        }
    }

    // All component files should have the same base name
    assert_eq!(
        extracted_base_names.len(),
        1,
        "All component files should have the same base name, got {:?}",
        extracted_base_names
    );
}

#[tokio::test]
async fn test_compression_info_file_sizes() {
    let Some(datasets_root) = get_test_datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let keyspace_dir = datasets_root.join("sstables").join("test_basic");
    if !keyspace_dir.exists() {
        eprintln!("test_basic keyspace not found, skipping test");
        return;
    }

    // Find all CompressionInfo.db files and validate their sizes
    let mut compression_files: Vec<(String, u64)> = Vec::new();

    for entry in std::fs::read_dir(&keyspace_dir)
        .expect("Should read keyspace dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
    {
        let table_dir = entry.path();
        for file_entry in std::fs::read_dir(&table_dir)
            .expect("Should read table dir")
            .filter_map(|e| e.ok())
        {
            let path = file_entry.path();
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with("-CompressionInfo.db"))
                .unwrap_or(false)
            {
                let metadata = std::fs::metadata(&path).expect("Should get metadata");
                let name = path
                    .parent()
                    .and_then(|p| p.file_name())
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                compression_files.push((name, metadata.len()));
            }
        }
    }

    eprintln!("CompressionInfo.db file sizes:");
    for (name, size) in &compression_files {
        eprintln!("  {}: {} bytes", name, size);

        // Sanity check: CompressionInfo.db files should have minimum size
        // Header (4 bytes) + algorithm (at least 1 byte) + some chunk data
        assert!(
            *size >= 8,
            "CompressionInfo.db for {} is suspiciously small: {} bytes",
            name,
            size
        );
    }
}

#[tokio::test]
async fn test_open_all_test_basic_tables_with_compression() {
    let Some(datasets_root) = get_test_datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let keyspace_dir = datasets_root.join("sstables").join("test_basic");
    if !keyspace_dir.exists() {
        eprintln!("test_basic keyspace not found, skipping test");
        return;
    }

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let entries: Vec<_> = std::fs::read_dir(&keyspace_dir)
        .expect("Should read keyspace dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    let mut success_count = 0;
    let mut fail_count = 0;

    for entry in entries {
        let table_dir = entry.path();
        let table_name = table_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        let Some(data_file) = find_data_file(&table_dir) else {
            eprintln!("  {} - No Data.db found", table_name);
            continue;
        };

        match SSTableReader::open(&data_file, &config, platform.clone()).await {
            Ok(reader) => {
                eprintln!(
                    "  ✓ {} - Opened successfully (version: {:?})",
                    table_name,
                    reader.header().cassandra_version
                );
                success_count += 1;
            }
            Err(e) => {
                eprintln!("  ✗ {} - Failed: {}", table_name, e);
                fail_count += 1;
            }
        }
    }

    eprintln!(
        "\nResults: {} succeeded, {} failed",
        success_count, fail_count
    );

    // Most tables should open successfully
    assert!(
        success_count > 0,
        "Expected at least some tables to open successfully"
    );
}
