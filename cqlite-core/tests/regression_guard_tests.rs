//! Regression Guard Tests (Issue #268)
//!
//! These tests prevent known bugs from returning. Each test is tied to a specific
//! historical issue and guards against its regression.
//!
//! **Philosophy**: These bugs were hard to find. Regression would be silent. Tests are insurance.
//!
//! ## Test Data Requirements
//!
//! Tests require the `CQLITE_DATASETS_ROOT` environment variable or will use fallback path:
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --package cqlite-core regression_guard
//! ```
//!
//! ## Coverage
//!
//! | Issue | Bug Description | Guard Test |
//! |-------|----------------|------------|
//! | #191 | Partition key columns missing from results | test_partition_key_bytes_not_empty_guards_191 |
//! | #129/#140 | SELECT * non-deterministic column order | test_column_values_typed_correctly_guards_129_140 |
//! | #238/#239 | UDTs in collections returned as hex blobs | test_udt_in_collection_has_fields_guards_238_239 |
//! | #240 | DATE type falling back to blob | test_date_values_are_date_type_guards_240 |
//! | #258 | Timestamp overflow for large values | test_timestamps_in_valid_range_guards_258 |
//! | #220 | UDT field order mismatch | test_udt_has_named_fields_guards_220 |

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::testing::dataset_helpers::{resolve_table_to_sstable_path, should_ignore_file};
use cqlite_core::ScanRow;
use cqlite_core::{Config, Platform, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ============================================================================
// Test Helpers
// ============================================================================

/// Find Data.db file in SSTable directory
fn find_data_db(sstable_dir: &Path) -> Option<PathBuf> {
    if let Ok(entries) = std::fs::read_dir(sstable_dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if should_ignore_file(name) {
                    continue;
                }
                if name.ends_with("-Data.db") {
                    return Some(entry.path());
                }
            }
        }
    }
    None
}

/// Check if test data is available
fn test_data_available() -> bool {
    std::env::var("CQLITE_DATASETS_ROOT").is_ok()
        || resolve_table_to_sstable_path("test_basic", "simple_table").is_ok()
}

/// Helper to recursively find UDT values in nested structures
fn find_udt_values(value: &Value, results: &mut Vec<Value>) {
    match value {
        Value::Udt(_) => results.push(value.clone()),
        Value::List(items) => {
            for item in items {
                find_udt_values(item, results);
            }
        }
        Value::Set(items) => {
            for item in items {
                find_udt_values(item, results);
            }
        }
        Value::Map(pairs) => {
            for (k, v) in pairs {
                find_udt_values(k, results);
                find_udt_values(v, results);
            }
        }
        Value::Frozen(inner) => find_udt_values(inner, results),
        _ => {}
    }
}

/// Issue #1334: entries decode to the `ScanRow` carrier. Search a live row's
/// cell values for UDTs; a marker (tombstone/null) contributes none.
fn find_udt_values_in_row(row: &ScanRow, results: &mut Vec<Value>) {
    if let ScanRow::Row(cells) = row {
        for (_, v) in cells {
            find_udt_values(v, results);
        }
    }
}

/// Helper to recursively find Timestamp values
fn find_timestamp_values(value: &Value, results: &mut Vec<i64>) {
    match value {
        Value::Timestamp(ts) => results.push(*ts),
        Value::List(items) => {
            for item in items {
                find_timestamp_values(item, results);
            }
        }
        Value::Set(items) => {
            for item in items {
                find_timestamp_values(item, results);
            }
        }
        Value::Map(pairs) => {
            for (k, v) in pairs {
                find_timestamp_values(k, results);
                find_timestamp_values(v, results);
            }
        }
        Value::Frozen(inner) => find_timestamp_values(inner, results),
        _ => {}
    }
}

/// Issue #1334: entries decode to the `ScanRow` carrier. Search a live row's
/// cell values for `Timestamp`s; a marker (tombstone/null) contributes none.
fn find_timestamp_values_in_row(row: &ScanRow, results: &mut Vec<i64>) {
    if let ScanRow::Row(cells) = row {
        for (_, v) in cells {
            find_timestamp_values(v, results);
        }
    }
}

/// Helper to recursively find Date values
fn find_date_values(value: &Value, results: &mut Vec<i32>) {
    match value {
        Value::Date(d) => results.push(*d),
        Value::List(items) => {
            for item in items {
                find_date_values(item, results);
            }
        }
        Value::Set(items) => {
            for item in items {
                find_date_values(item, results);
            }
        }
        Value::Map(pairs) => {
            for (k, v) in pairs {
                find_date_values(k, results);
                find_date_values(v, results);
            }
        }
        Value::Frozen(inner) => find_date_values(inner, results),
        _ => {}
    }
}

/// Issue #1334: entries decode to the `ScanRow` carrier. Search a live row's
/// cell values for `Date`s; a marker (tombstone/null) contributes none.
fn find_date_values_in_row(row: &ScanRow, results: &mut Vec<i32>) {
    if let ScanRow::Row(cells) = row {
        for (_, v) in cells {
            find_date_values(v, results);
        }
    }
}

// ============================================================================
// Regression Guard Tests
// ============================================================================

/// Guards Issue #191: Partition key columns were missing from SELECT results
/// because Cassandra stores them in RowKey, not cell data.
///
/// The fix added partition key synthesis in select_executor.rs to decode
/// partition key values from RowKey bytes.
///
/// This test verifies that partition keys have non-empty bytes (the foundation
/// for synthesis - if bytes are empty, synthesis cannot work).
#[tokio::test]
async fn test_partition_key_bytes_not_empty_guards_191() {
    if !test_data_available() {
        eprintln!("Skipping test: test data not available");
        return;
    }

    let sstable_dir = match resolve_table_to_sstable_path("test_basic", "simple_table") {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    let data_db = match find_data_db(&sstable_dir) {
        Some(p) => p,
        None => {
            eprintln!("Skipping test: no Data.db found");
            return;
        }
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform creation"));

    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("SSTable open");

    let entries = reader.get_all_entries().await.expect("get entries");

    // Issue #191 guard: All partition keys must have non-empty bytes
    let mut partition_count = 0;
    let mut empty_key_count = 0;

    for (_table_id, row_key, _value) in &entries {
        partition_count += 1;
        if row_key.0.is_empty() {
            empty_key_count += 1;
        }
    }

    assert!(
        partition_count > 0,
        "Issue #191 guard: Expected partitions in simple_table"
    );
    assert_eq!(
        empty_key_count, 0,
        "Issue #191 guard: Found {} partitions with empty RowKey bytes (would break key synthesis)",
        empty_key_count
    );

    println!(
        "Issue #191 guard PASSED: {}/{} partitions have non-empty keys",
        partition_count, partition_count
    );
}

/// Guards Issues #129/#140: SELECT * returned columns in random HashMap order,
/// causing non-deterministic JSON output.
///
/// The fix ensured column order comes from metadata.columns, not HashMap iteration.
///
/// This test verifies that parsed values contain properly typed data (not all blobs),
/// which is a prerequisite for correct column mapping.
#[tokio::test]
async fn test_column_values_typed_correctly_guards_129_140() {
    if !test_data_available() {
        eprintln!("Skipping test: test data not available");
        return;
    }

    let sstable_dir = match resolve_table_to_sstable_path("test_basic", "simple_table") {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    let data_db = match find_data_db(&sstable_dir) {
        Some(p) => p,
        None => {
            eprintln!("Skipping test: no Data.db found");
            return;
        }
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform creation"));

    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("SSTable open");

    let entries = reader.get_all_entries().await.expect("get entries");

    // Issue #129/#140 guard: Values should be properly typed, not all blobs
    let mut total_values = 0;
    let mut typed_values = 0; // Non-Blob, non-Null values
    let mut blob_values = 0;

    for (_table_id, _row_key, value) in &entries {
        // Issue #1334: rows decode to `ScanRow::Row` keyed by `Arc<str>`.
        if let ScanRow::Row(columns) = value {
            for (_col_key, col_val) in columns {
                total_values += 1;
                match col_val {
                    Value::Blob(_) => blob_values += 1,
                    Value::Null => {} // Nulls are expected
                    _ => typed_values += 1,
                }
            }
        }
    }

    // At least 50% of non-null values should be typed (not blobs)
    // simple_table has UUID, TEXT, INT, BIGINT, FLOAT, BOOLEAN, TIMESTAMP, DATE, etc.
    let non_null_values = typed_values + blob_values;
    let typed_ratio = if non_null_values > 0 {
        typed_values as f64 / non_null_values as f64
    } else {
        0.0
    };

    assert!(
        typed_ratio >= 0.5,
        "Issue #129/#140 guard: Only {:.1}% of values are typed (expected >= 50%). \
         {} typed, {} blob, {} total",
        typed_ratio * 100.0,
        typed_values,
        blob_values,
        total_values
    );

    println!(
        "Issue #129/#140 guard PASSED: {:.1}% typed values ({}/{})",
        typed_ratio * 100.0,
        typed_values,
        non_null_values
    );
}

/// Guards Issues #238/#239: UDTs nested in collections were returned as
/// "0x..." hex blobs instead of parsed field structures.
///
/// The fix extended parse_value_with_comparator() with full UDT handling and
/// propagated UDT registry through the parsing chain.
///
/// This test verifies that UDTs in collections have proper field structures.
#[tokio::test]
async fn test_udt_in_collection_has_fields_guards_238_239() {
    if !test_data_available() {
        eprintln!("Skipping test: test data not available");
        return;
    }

    let sstable_dir =
        match resolve_table_to_sstable_path("test_collections", "collections_with_udts") {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Skipping test: {}", e);
                return;
            }
        };

    let data_db = match find_data_db(&sstable_dir) {
        Some(p) => p,
        None => {
            eprintln!("Skipping test: no Data.db found");
            return;
        }
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform creation"));

    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("SSTable open");

    let entries = reader.get_all_entries().await.expect("get entries");

    // Issue #238/#239 guard: UDTs in collections should have fields, not be blobs
    let mut udt_count = 0;
    let mut udt_with_fields = 0;

    for (_table_id, _row_key, value) in &entries {
        let mut udts = Vec::new();
        find_udt_values_in_row(value, &mut udts);

        for udt in udts {
            udt_count += 1;
            if let Value::Udt(udt_value) = udt {
                if !udt_value.fields.is_empty() {
                    udt_with_fields += 1;
                }
            }
        }
    }

    // All UDTs found should have fields
    assert!(
        udt_count > 0,
        "Issue #238/#239 guard: Expected UDT values in collections_with_udts table"
    );
    assert_eq!(
        udt_with_fields,
        udt_count,
        "Issue #238/#239 guard: {}/{} UDTs have empty fields (would appear as blobs)",
        udt_count - udt_with_fields,
        udt_count
    );

    println!(
        "Issue #238/#239 guard PASSED: {}/{} UDTs have proper field structures",
        udt_with_fields, udt_count
    );
}

/// Guards Issue #240: DATE type fell back to blob when comparator wasn't registered,
/// producing wrong values.
///
/// The fix added dedicated ComparatorType::Date variant and proper DATE encoding
/// offset handling in all parsing paths.
///
/// This test verifies that DATE columns are parsed as Value::Date, not Value::Blob.
#[tokio::test]
async fn test_date_values_are_date_type_guards_240() {
    if !test_data_available() {
        eprintln!("Skipping test: test data not available");
        return;
    }

    let sstable_dir = match resolve_table_to_sstable_path("test_basic", "simple_table") {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    let data_db = match find_data_db(&sstable_dir) {
        Some(p) => p,
        None => {
            eprintln!("Skipping test: no Data.db found");
            return;
        }
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform creation"));

    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("SSTable open");

    let entries = reader.get_all_entries().await.expect("get entries");

    // Issue #240 guard: Look for Date values in the parsed data
    let mut date_count = 0;

    for (_table_id, _row_key, value) in &entries {
        let mut dates = Vec::new();
        find_date_values_in_row(value, &mut dates);
        date_count += dates.len();

        // Also verify dates are in reasonable range (not garbage from wrong parsing)
        for date in &dates {
            // Dates should be within reasonable range: 1970-2100 (0 to ~47000 days)
            assert!(
                *date >= -25000 && *date <= 50000,
                "Issue #240 guard: Date value {} is out of reasonable range \
                 (possible wrong offset or blob misparse)",
                date
            );
        }
    }

    // simple_table has a birth_date DATE column
    assert!(
        date_count > 0,
        "Issue #240 guard: Expected DATE values in simple_table (has birth_date column)"
    );

    println!(
        "Issue #240 guard PASSED: {} DATE values found and validated",
        date_count
    );
}

/// Guards Issue #258: Timestamps were multiplied by 1000 (ms→µs), causing
/// overflow for dates after ~2262.
///
/// The fix removed the erroneous multiplication - Value::Timestamp stores
/// milliseconds per definition.
///
/// This test verifies that timestamps are in valid range (not overflowed).
#[tokio::test]
async fn test_timestamps_in_valid_range_guards_258() {
    if !test_data_available() {
        eprintln!("Skipping test: test data not available");
        return;
    }

    let sstable_dir = match resolve_table_to_sstable_path("test_timeseries", "sensor_data") {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    let data_db = match find_data_db(&sstable_dir) {
        Some(p) => p,
        None => {
            eprintln!("Skipping test: no Data.db found");
            return;
        }
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform creation"));

    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("SSTable open");

    let entries = reader.get_all_entries().await.expect("get entries");

    // Issue #258 guard: Timestamps should be in valid range (not overflowed)
    // Valid range: 1970-01-01 to 3000-01-01 (in milliseconds)
    let min_valid_ts: i64 = 0; // 1970-01-01
    let max_valid_ts: i64 = 32503680000000; // ~3000-01-01 in ms

    let mut timestamp_count = 0;
    let mut overflow_count = 0;

    for (_table_id, _row_key, value) in &entries {
        let mut timestamps = Vec::new();
        find_timestamp_values_in_row(value, &mut timestamps);

        for ts in timestamps {
            timestamp_count += 1;
            if ts < min_valid_ts || ts > max_valid_ts {
                overflow_count += 1;
                eprintln!(
                    "Issue #258 guard: Timestamp {} is out of valid range (overflow?)",
                    ts
                );
            }
        }
    }

    assert!(
        timestamp_count > 0,
        "Issue #258 guard: Expected TIMESTAMP values in sensor_data table"
    );
    assert_eq!(
        overflow_count, 0,
        "Issue #258 guard: {}/{} timestamps appear overflowed (out of 1970-3000 range)",
        overflow_count, timestamp_count
    );

    println!(
        "Issue #258 guard PASSED: {}/{} timestamps in valid range",
        timestamp_count, timestamp_count
    );
}

/// Guards Issue #220: UDT fields could be deserialized out of order, causing
/// field values to be misaligned with names.
///
/// The fix implemented complete UDT infrastructure with ordered field parsing
/// matching schema definition order.
///
/// This test verifies that UDT fields have proper names (not empty strings).
#[tokio::test]
async fn test_udt_has_named_fields_guards_220() {
    if !test_data_available() {
        eprintln!("Skipping test: test data not available");
        return;
    }

    let sstable_dir =
        match resolve_table_to_sstable_path("test_collections", "collections_with_udts") {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Skipping test: {}", e);
                return;
            }
        };

    let data_db = match find_data_db(&sstable_dir) {
        Some(p) => p,
        None => {
            eprintln!("Skipping test: no Data.db found");
            return;
        }
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform creation"));

    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("SSTable open");

    let entries = reader.get_all_entries().await.expect("get entries");

    // Issue #220 guard: UDT fields should have non-empty names
    let mut udt_count = 0;
    let mut fields_with_names = 0;
    let mut total_fields = 0;

    for (_table_id, _row_key, value) in &entries {
        let mut udts = Vec::new();
        find_udt_values_in_row(value, &mut udts);

        for udt in udts {
            if let Value::Udt(udt_value) = udt {
                udt_count += 1;
                for field in &udt_value.fields {
                    total_fields += 1;
                    if !field.name.is_empty() {
                        fields_with_names += 1;
                    }
                }
            }
        }
    }

    assert!(
        udt_count > 0,
        "Issue #220 guard: Expected UDT values in collections_with_udts table"
    );
    assert!(
        total_fields > 0,
        "Issue #220 guard: Expected UDT fields in collections_with_udts table"
    );
    assert_eq!(
        fields_with_names,
        total_fields,
        "Issue #220 guard: {}/{} UDT fields have empty names (would cause misalignment)",
        total_fields - fields_with_names,
        total_fields
    );

    println!(
        "Issue #220 guard PASSED: {} UDTs with {}/{} named fields",
        udt_count, fields_with_names, total_fields
    );
}
