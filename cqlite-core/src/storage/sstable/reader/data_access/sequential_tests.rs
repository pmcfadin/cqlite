//! Unit + corpus tests for the materializing scan path (`SSTableReader::scan`,
//! `sequential_scan`, `scan_for_key`).
//!
//! Split out of `sequential.rs` per the campsite rule (#1116/#1135) — that source
//! file was 1319 lines against the ~800-line target, 496 of them this inline
//! `mod tests`. This is a VERBATIM move: no assertion changed.
//!
//! Included from [`super`] via `#[path = "sequential_tests.rs"]`, so `use super::*`
//! reaches the scan implementation under test.

use super::*;

// =========================================================================
// Issue #1411: point-lookup parse-error classification (unit level)
//
// Proves `scan_for_key`'s parse-error handling matches the scan path
// (`stitch_and_parse_all_chunks`, which propagates EVERY `parse_block` error):
// only "this reader has no schema for the table" soft-misses to Ok(None) so a
// multi-reader `get()` can try the next reader; real corruption / malformed
// blocks (and a deep schema error when a schema IS present) stay fatal.
// =========================================================================

#[test]
fn soft_miss_only_when_schema_absent_and_schema_error() {
    // The one legitimate soft-miss: no schema for this reader → the parser
    // reports Error::Schema before touching bytes → caller tries next reader.
    assert!(is_parse_soft_miss(
        false,
        &Error::schema("V5CompressedLegacy format requires schema for ks.tbl")
    ));
}

#[test]
fn schema_error_with_schema_present_is_fatal() {
    // A schema IS present but a deep type/UDT resolution failed → real error,
    // NOT a missing key. Must propagate (matches the scan path).
    assert!(!is_parse_soft_miss(
        true,
        &Error::schema("Not a UserType: frozen<...>")
    ));
}

#[test]
fn corruption_classes_are_always_fatal() {
    // Real data corruption / malformed block classes MUST propagate in BOTH
    // schema-present and schema-absent modes — never masked as "not found".
    // These are exactly the classes the scan path surfaces via `?`.
    for schema_present in [true, false] {
        assert!(!is_parse_soft_miss(
            schema_present,
            &Error::corruption("chunk 0 CRC mismatch at offset 0x0")
        ));
        assert!(!is_parse_soft_miss(
            schema_present,
            &Error::invalid_format("malformed row header in chunk 0 at offset 0x0")
        ));
        assert!(!is_parse_soft_miss(
            schema_present,
            &Error::Parse("bad VInt".to_string())
        ));
    }
}

// =========================================================================
// Integration tests with real SSTable data
// =========================================================================

#[tokio::test]
async fn test_get_nonexistent_key() {
    use std::path::PathBuf;
    use std::sync::Arc;

    // Test with real SSTable data if available
    let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(root) => PathBuf::from(root),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
            return;
        }
    };

    let simple_table_dir = datasets_root.join("sstables/test_basic");
    if !simple_table_dir.exists() {
        eprintln!("test_basic not found, skipping test");
        return;
    }

    // Find simple_table
    let table_dir = std::fs::read_dir(&simple_table_dir)
        .ok()
        .and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("simple_table"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

    let Some(table_path) = table_dir else {
        eprintln!("simple_table not found, skipping");
        return;
    };

    // Find Data.db file
    let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
        entries
            .filter_map(|e| e.ok())
            .find(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
            .map(|e| e.path())
    });

    let Some(data_path) = data_file else {
        eprintln!("Data.db not found, skipping");
        return;
    };

    let config = crate::Config::default();
    let platform = Arc::new(
        crate::Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .expect("Failed to open SSTable");

    // Try to get a key that doesn't exist
    let table_id = TableId::new("test_basic.simple_table".to_string());
    let nonexistent_key = RowKey::new(vec![0xFF, 0xFF, 0xFF, 0xFF]); // Very unlikely to exist

    let result = reader.get(&table_id, &nonexistent_key).await;
    assert!(
        result.is_ok(),
        "get() should succeed even for nonexistent key"
    );
    assert!(
        result.unwrap().is_none(),
        "Nonexistent key should return None"
    );
}

#[tokio::test]
async fn test_scan_with_limit() {
    use std::path::PathBuf;
    use std::sync::Arc;

    let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(root) => PathBuf::from(root),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
            return;
        }
    };

    let simple_table_dir = datasets_root.join("sstables/test_basic");
    if !simple_table_dir.exists() {
        eprintln!("test_basic not found, skipping test");
        return;
    }

    // Find simple_table
    let table_dir = std::fs::read_dir(&simple_table_dir)
        .ok()
        .and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("simple_table"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

    let Some(table_path) = table_dir else {
        eprintln!("simple_table not found, skipping");
        return;
    };

    let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
        entries
            .filter_map(|e| e.ok())
            .find(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
            .map(|e| e.path())
    });

    let Some(data_path) = data_file else {
        eprintln!("Data.db not found, skipping");
        return;
    };

    let config = crate::Config::default();
    let platform = Arc::new(
        crate::Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .expect("Failed to open SSTable");

    let table_id = TableId::new("test_basic.simple_table".to_string());

    // Test scan with limit
    let result = reader.scan(&table_id, None, None, Some(5), None).await;
    assert!(result.is_ok(), "scan() should succeed");

    let entries = result.unwrap();
    assert!(
        entries.len() <= 5,
        "Scan with limit 5 should return at most 5 entries, got {}",
        entries.len()
    );

    eprintln!("Scan with limit 5 returned {} entries", entries.len());
}

#[tokio::test]
async fn test_scan_full_table() {
    use std::path::PathBuf;
    use std::sync::Arc;

    let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(root) => PathBuf::from(root),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
            return;
        }
    };

    let simple_table_dir = datasets_root.join("sstables/test_basic");
    if !simple_table_dir.exists() {
        eprintln!("test_basic not found, skipping test");
        return;
    }

    // Find simple_table
    let table_dir = std::fs::read_dir(&simple_table_dir)
        .ok()
        .and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("simple_table"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

    let Some(table_path) = table_dir else {
        eprintln!("simple_table not found, skipping");
        return;
    };

    let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
        entries
            .filter_map(|e| e.ok())
            .find(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
            .map(|e| e.path())
    });

    let Some(data_path) = data_file else {
        eprintln!("Data.db not found, skipping");
        return;
    };

    let config = crate::Config::default();
    let platform = Arc::new(
        crate::Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .expect("Failed to open SSTable");

    let table_id = TableId::new("test_basic.simple_table".to_string());

    // Full table scan (no limit)
    let result = reader.scan(&table_id, None, None, None, None).await;
    assert!(result.is_ok(), "Full scan should succeed");

    let entries = result.unwrap();
    eprintln!("Full scan returned {} entries", entries.len());
}

#[tokio::test]
async fn test_get_all_entries() {
    use std::path::PathBuf;
    use std::sync::Arc;

    let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(root) => PathBuf::from(root),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
            return;
        }
    };

    let simple_table_dir = datasets_root.join("sstables/test_basic");
    if !simple_table_dir.exists() {
        eprintln!("test_basic not found, skipping test");
        return;
    }

    // Find simple_table
    let table_dir = std::fs::read_dir(&simple_table_dir)
        .ok()
        .and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("simple_table"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

    let Some(table_path) = table_dir else {
        eprintln!("simple_table not found, skipping");
        return;
    };

    let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
        entries
            .filter_map(|e| e.ok())
            .find(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
            .map(|e| e.path())
    });

    let Some(data_path) = data_file else {
        eprintln!("Data.db not found, skipping");
        return;
    };

    let config = crate::Config::default();
    let platform = Arc::new(
        crate::Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .expect("Failed to open SSTable");

    // Get all entries (for compaction use case)
    let result = reader.get_all_entries().await;
    assert!(result.is_ok(), "get_all_entries() should succeed");

    let entries = result.unwrap();
    eprintln!("get_all_entries() returned {} entries", entries.len());
}

/// Regression test for Issue #480: static cell duplication on read.
///
/// static_columns_table has 100 partitions, each containing one static_block
/// and one clustering row. CQLite should return exactly 100 result rows — one
/// per partition — not 200 (which would occur if static rows were emitted as
/// separate result entries).
///
/// Two bugs were fixed:
/// 1. Snappy varint collision: bytes `0xC0 0x51` at the start of the Snappy
///    stream were misidentified as the V5_0StaticColumns magic number, causing
///    the file pointer to advance past part of the compressed data before
///    decompression, resulting in "corrupt input" errors.
/// 2. Static row duplication: static rows were pushed into `results` just like
///    clustering rows. They should be accumulated per-partition and merged into
///    each subsequent clustering row instead.
#[tokio::test]
async fn test_static_columns_table_row_count_issue480() {
    use std::path::PathBuf;
    use std::sync::Arc;

    let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(root) => PathBuf::from(root),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set, skipping Issue #480 regression test");
            return;
        }
    };

    let table_base = datasets_root.join("sstables/test_basic");
    if !table_base.exists() {
        eprintln!("test_basic dir not found, skipping Issue #480 regression test");
        return;
    }

    // Locate the static_columns_table directory
    let table_dir = std::fs::read_dir(&table_base).ok().and_then(|entries| {
        entries
            .filter_map(|e| e.ok())
            .find(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.starts_with("static_columns_table"))
                    .unwrap_or(false)
            })
            .map(|e| e.path())
    });

    let Some(table_path) = table_dir else {
        eprintln!("static_columns_table not found, skipping Issue #480 regression test");
        return;
    };

    // Find the Data.db file (must be real binary, not macOS ._resource_fork)
    let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
        entries
            .filter_map(|e| e.ok())
            .find(|e| {
                let name = e.file_name();
                let s = name.to_str().unwrap_or("");
                s.ends_with("-Data.db") && !s.starts_with("._")
            })
            .map(|e| e.path())
    });

    let Some(data_path) = data_file else {
        eprintln!("Data.db not found in static_columns_table dir, skipping");
        return;
    };

    let config = crate::Config::default();
    let platform = Arc::new(
        crate::Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );

    let reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .expect("Failed to open static_columns_table SSTable");

    let table_id = crate::types::TableId::new("test_basic.static_columns_table".to_string());
    let result = reader.scan(&table_id, None, None, None, None).await;
    assert!(
        result.is_ok(),
        "Scan of static_columns_table should succeed: {:?}",
        result.err()
    );

    let entries = result.unwrap();
    eprintln!(
        "Issue #480 regression: static_columns_table scan returned {} rows",
        entries.len()
    );

    // Expected: one row per partition (static merged into the clustering row).
    // Derive the count from the sstabledump JSONL golden (one object/partition)
    // instead of hardcoding it, so the guard survives corpus reshapes (issue
    // #1935; cf. PR #2209). #480 regression: 0 rows (decompress fail) / 2x
    // (static rows split) / one-per-partition (full fix).
    let mut golden_path = data_path.clone().into_os_string();
    golden_path.push(".jsonl");
    let golden = std::fs::read_to_string(&golden_path)
        .expect("static_columns_table sstabledump JSONL golden must exist");
    let expected_rows = golden.lines().filter(|l| !l.trim().is_empty()).count();
    // No-vacuous-pass guard: the golden must actually contain partitions.
    assert!(
        expected_rows > 0,
        "golden must have >0 partitions (no vacuous pass)"
    );
    assert_eq!(
        entries.len(),
        expected_rows,
        "static_columns_table should return one row per partition ({expected_rows} \
         per the sstabledump JSONL golden), got {}. Regression for Issue #480: \
         static cell duplication on read.",
        entries.len()
    );
}
