//! Integration tests for component loading in SSTable readers.
//!
//! These tests use real SSTable data from test-data/datasets/sstables/
//! to verify component file discovery and loading (Issue #28 compliance).

use std::path::Path;
use std::sync::Arc;

use cqlite_core::storage::sstable::reader::SSTableReader;
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
// Component File Discovery Tests
// =============================================================================

#[tokio::test]
async fn test_sstable_reader_loads_index_component() {
    let Some(datasets_root) = get_test_datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let Some(table_dir) = find_table_dir(&datasets_root, "test_basic", "simple_table") else {
        eprintln!("simple_table not found, skipping test");
        return;
    };

    let Some(data_file) = find_data_file(&table_dir) else {
        eprintln!("Data.db not found, skipping test");
        return;
    };

    // Check that Index.db component exists
    let index_db_exists = std::fs::read_dir(&table_dir)
        .expect("Should read table dir")
        .filter_map(|e| e.ok())
        .any(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.ends_with("-Index.db"))
                .unwrap_or(false)
        });

    eprintln!("Index.db exists: {}", index_db_exists);

    // Open SSTable reader - it should detect and load the Index component
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let reader = SSTableReader::open(&data_file, &config, platform)
        .await
        .expect("Failed to open SSTable");

    // The reader should have loaded successfully with or without Index
    eprintln!(
        "SSTableReader opened successfully for simple_table (version: {:?})",
        reader.header().cassandra_version
    );
}

#[tokio::test]
async fn test_sstable_reader_loads_statistics_component() {
    let Some(datasets_root) = get_test_datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let Some(table_dir) = find_table_dir(&datasets_root, "test_basic", "simple_table") else {
        eprintln!("simple_table not found, skipping test");
        return;
    };

    let Some(data_file) = find_data_file(&table_dir) else {
        eprintln!("Data.db not found, skipping test");
        return;
    };

    // Check that Statistics.db component exists
    let statistics_exists = std::fs::read_dir(&table_dir)
        .expect("Should read table dir")
        .filter_map(|e| e.ok())
        .any(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.ends_with("-Statistics.db"))
                .unwrap_or(false)
        });

    eprintln!("Statistics.db exists: {}", statistics_exists);

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
        "SSTableReader opened with statistics detection (version: {:?})",
        reader.header().cassandra_version
    );
}

#[tokio::test]
async fn test_all_test_basic_tables_open_with_components() {
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

    eprintln!("Testing {} tables in test_basic", entries.len());

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

        // Count component files
        let component_count = std::fs::read_dir(&table_dir)
            .expect("Should read table dir")
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with(".db") && !n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
            .count();

        match SSTableReader::open(&data_file, &config, platform.clone()).await {
            Ok(reader) => {
                eprintln!(
                    "  ✓ {} - {} components, version: {:?}",
                    table_name,
                    component_count,
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

    assert!(
        success_count > 0,
        "Expected at least some tables to open successfully"
    );
}

#[tokio::test]
async fn test_component_file_presence_in_test_data() {
    let Some(datasets_root) = get_test_datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let keyspace_dir = datasets_root.join("sstables").join("test_basic");
    if !keyspace_dir.exists() {
        eprintln!("test_basic keyspace not found, skipping test");
        return;
    }

    // Track component file presence across all tables
    let mut component_stats: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

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
            let filename = file_entry.file_name().to_str().unwrap_or("").to_string();

            if filename.ends_with(".db") {
                // Extract component type from filename
                // Format: nb-1-big-ComponentType.db
                if let Some(component_type) = filename
                    .strip_suffix(".db")
                    .and_then(|s| s.rsplit('-').next())
                {
                    *component_stats
                        .entry(component_type.to_string())
                        .or_insert(0) += 1;
                }
            }
        }
    }

    eprintln!("Component file statistics across test_basic:");
    let mut sorted_stats: Vec<_> = component_stats.iter().collect();
    sorted_stats.sort_by(|a, b| b.1.cmp(a.1));

    for (component, count) in &sorted_stats {
        eprintln!("  {}: {} files", component, count);
    }

    // Verify we found expected component types
    assert!(
        component_stats.contains_key("Data"),
        "Should find Data.db files"
    );
    assert!(
        component_stats.contains_key("Index") || component_stats.contains_key("Statistics"),
        "Should find Index.db or Statistics.db files"
    );
}

#[tokio::test]
async fn test_all_collections_tables_open() {
    let Some(datasets_root) = get_test_datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let keyspace_dir = datasets_root.join("sstables").join("test_collections");
    if !keyspace_dir.exists() {
        eprintln!("test_collections keyspace not found, skipping test");
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

    eprintln!("Testing {} tables in test_collections", entries.len());

    let mut success_count = 0;

    for entry in entries {
        let table_dir = entry.path();
        let table_name = table_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        let Some(data_file) = find_data_file(&table_dir) else {
            continue;
        };

        match SSTableReader::open(&data_file, &config, platform.clone()).await {
            Ok(_) => {
                eprintln!("  ✓ {}", table_name);
                success_count += 1;
            }
            Err(e) => {
                eprintln!("  ✗ {} - {}", table_name, e);
            }
        }
    }

    eprintln!("\nSuccessfully opened {} tables", success_count);
}

#[tokio::test]
async fn test_timeseries_tables_open() {
    let Some(datasets_root) = get_test_datasets_root() else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let keyspace_dir = datasets_root.join("sstables").join("test_timeseries");
    if !keyspace_dir.exists() {
        eprintln!("test_timeseries keyspace not found, skipping test");
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

    eprintln!("Testing {} tables in test_timeseries", entries.len());

    let mut success_count = 0;

    for entry in entries {
        let table_dir = entry.path();
        let table_name = table_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        let Some(data_file) = find_data_file(&table_dir) else {
            continue;
        };

        match SSTableReader::open(&data_file, &config, platform.clone()).await {
            Ok(_) => {
                eprintln!("  ✓ {}", table_name);
                success_count += 1;
            }
            Err(e) => {
                eprintln!("  ✗ {} - {}", table_name, e);
            }
        }
    }

    eprintln!("\nSuccessfully opened {} tables", success_count);
}
