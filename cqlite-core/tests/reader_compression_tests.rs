//! Integration tests for compression detection in SSTable readers.
//!
//! These tests use real SSTable data from test-data/datasets/sstables/
//! to verify compression detection and initialization (Issue #28 compliance).

use std::path::Path;
use std::sync::Arc;

use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use cqlite_core::storage::sstable::reader::{extract_sstable_base_name, SSTableReader};
use cqlite_core::testing::dataset_helpers::require_fixtures_strict;
use cqlite_core::{Config, Platform};

/// Tables that the pinned `test_basic` keyspace fixture is expected to ship
/// (issue #1230). Under strict mode (`require_fixtures_strict`) every one of
/// these MUST be present and openable, so a dropped table or a partial dataset
/// extraction reds CI instead of silently passing on whatever survived.
const EXPECTED_TEST_BASIC_TABLES: &[&str] = &[
    "composite_key_table",
    "compression_test_table",
    "counters",
    "multi_partition_table",
    "simple_table",
    "static_columns_table",
    "ttl_test_table",
    "uncompressed_table",
];

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
    // Fail-closed gate (issue #1230): if the dataset root is unset/absent, a
    // strict CI lane must FAIL rather than silently pass on missing data; local
    // dev without the fixtures still skips.
    let Some(datasets_root) = get_test_datasets_root() else {
        assert!(
            !require_fixtures_strict(),
            "CQLITE_REQUIRE_FIXTURES=1 but CQLITE_DATASETS_ROOT is unset — \
             fetch with bash test-data/scripts/fetch-datasets.sh"
        );
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let keyspace_dir = datasets_root.join("sstables").join("test_basic");
    if !keyspace_dir.exists() {
        assert!(
            !require_fixtures_strict(),
            "CQLITE_REQUIRE_FIXTURES=1 but test_basic keyspace is absent under {} — \
             fetch with bash test-data/scripts/fetch-datasets.sh",
            keyspace_dir.display()
        );
        eprintln!("test_basic keyspace not found, skipping test");
        return;
    }

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let strict = require_fixtures_strict();
    let mut opened: Vec<&str> = Vec::new();

    for &table in EXPECTED_TEST_BASIC_TABLES {
        // Resolve the <table>-<uuid> directory, then its Data.db.
        let data_file = find_table_dir(&datasets_root, "test_basic", &format!("{table}-"))
            .as_deref()
            .and_then(find_data_file);

        let Some(data_file) = data_file else {
            // A missing expected table is a hard failure under strict mode so a
            // dropped table or a partial extraction reds CI (was: silent
            // `continue` that still let `success_count > 0` pass).
            assert!(
                !strict,
                "CQLITE_REQUIRE_FIXTURES=1 but expected table test_basic.{table} \
                 has no Data.db under {} — dropped table or partial dataset?",
                keyspace_dir.display()
            );
            eprintln!("  {table} - No Data.db found (skipped, non-strict)");
            continue;
        };

        let reader = SSTableReader::open(&data_file, &config, platform.clone())
            .await
            .unwrap_or_else(|e| panic!("test_basic.{table} failed to open: {e}"));

        // Real content assertion (not just "opened without error"): the on-disk
        // Data.db is non-empty and the reader parsed a header.
        let version = reader.header().cassandra_version;
        let data_len = std::fs::metadata(&data_file)
            .map(|m| m.len())
            .unwrap_or_else(|e| panic!("test_basic.{table} Data.db metadata: {e}"));
        assert!(
            data_len > 0,
            "test_basic.{table} Data.db is present but empty (0 bytes)"
        );
        eprintln!("  ✓ {table} - Opened (version: {version:?}, {data_len} bytes)");
        opened.push(table);
    }

    eprintln!(
        "\nResults: {}/{} expected test_basic tables opened",
        opened.len(),
        EXPECTED_TEST_BASIC_TABLES.len()
    );

    if strict {
        // Under strict mode every expected table must be present and openable.
        assert_eq!(
            opened.len(),
            EXPECTED_TEST_BASIC_TABLES.len(),
            "strict mode: only {:?} of the expected {:?} tables opened",
            opened,
            EXPECTED_TEST_BASIC_TABLES
        );
    } else {
        // Non-strict local dev: at least the core table must be readable so the
        // test still proves the read path works when any fixtures are present.
        assert!(
            !opened.is_empty(),
            "Expected at least one test_basic table to open successfully"
        );
    }
}

/// Helper: find any nb-1-big-CompressionInfo.db under the datasets root.
/// Prefers the sensor_data table; falls back to any match for robustness.
fn find_compression_info_file(datasets_root: &Path) -> Option<std::path::PathBuf> {
    // Preferred path
    let preferred = datasets_root.join(
        "sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-CompressionInfo.db",
    );
    if preferred.exists() {
        return Some(preferred);
    }

    // Fallback: walk sstables directory for any CompressionInfo.db
    let sstables_root = datasets_root.join("sstables");
    if !sstables_root.exists() {
        return None;
    }
    for keyspace_entry in std::fs::read_dir(&sstables_root)
        .ok()?
        .filter_map(|e| e.ok())
    {
        let keyspace_dir = keyspace_entry.path();
        if !keyspace_dir.is_dir() {
            continue;
        }
        for table_entry in std::fs::read_dir(&keyspace_dir)
            .ok()?
            .filter_map(|e| e.ok())
        {
            let table_dir = table_entry.path();
            if !table_dir.is_dir() {
                continue;
            }
            for file_entry in std::fs::read_dir(&table_dir).ok()?.filter_map(|e| e.ok()) {
                let path = file_entry.path();
                if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with("-CompressionInfo.db"))
                    .unwrap_or(false)
                {
                    return Some(path);
                }
            }
        }
    }
    None
}

/// Real-fixture integration test: parse a CompressionInfo.db file from the test
/// dataset and validate the key fields (Issue #638 fix).
///
/// Asserts:
/// - algorithm == "LZ4Compressor"
/// - chunk_length == 16384
/// - chunk_offsets is non-empty
/// - chunk_offsets are strictly increasing
#[test]
fn test_real_fixture_compression_info_parse() {
    let Some(datasets_root) = std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(std::path::PathBuf::from)
    else {
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
        return;
    };

    let Some(ci_path) = find_compression_info_file(&datasets_root) else {
        eprintln!("No CompressionInfo.db found under datasets root, skipping test");
        return;
    };

    eprintln!("Parsing: {:?}", ci_path);

    let data =
        std::fs::read(&ci_path).unwrap_or_else(|e| panic!("Failed to read {:?}: {}", ci_path, e));

    let info = CompressionInfo::parse(&data)
        .unwrap_or_else(|e| panic!("CompressionInfo::parse failed on {:?}: {}", ci_path, e));

    assert_eq!(
        info.algorithm, "LZ4Compressor",
        "Expected LZ4Compressor, got {:?}",
        info.algorithm
    );

    assert_eq!(
        info.chunk_length, 16384,
        "Expected chunk_length == 16384, got {}",
        info.chunk_length
    );

    assert!(
        !info.chunk_offsets.is_empty(),
        "chunk_offsets must be non-empty"
    );

    // Verify chunk_offsets are strictly increasing
    for window in info.chunk_offsets.windows(2) {
        assert!(
            window[1] > window[0],
            "chunk_offsets must be strictly increasing: {} >= {}",
            window[0],
            window[1]
        );
    }

    eprintln!(
        "CompressionInfo parsed OK: algorithm={}, chunk_length={}, offsets={}",
        info.algorithm,
        info.chunk_length,
        info.chunk_offsets.len()
    );
}
