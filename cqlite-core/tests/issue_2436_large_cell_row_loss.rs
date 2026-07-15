//! Issue #2436 (P0, oracle-driven): a single-cell `Value::Text`/`Value::Blob`
//! whose serialized size crosses a boundary between 950,000 and 1,000,000 bytes
//! caused the ENTIRE row to be SILENTLY dropped on read — the write succeeded,
//! the reader returned `Ok` with ZERO rows, and no code path signalled anything.
//!
//! Bisected boundary (from the issue):
//! ```text
//! size=950_000   -> Ok(1)   (row present)
//! size=1_000_000 -> Ok(0)   <-- row silently vanished, no error
//! size=2_000_000 -> Ok(0)
//! ```
//!
//! This pin writes ONE partition with a single row whose `name` column is a
//! `Value::Text` of `size` bytes (`"x".repeat(size)`) via
//! `SSTableWriter::write_partition` + `finish()` (uncompressed BIG), then reads
//! it back and asserts the row round-trips BYTE-IDENTICAL (length + content),
//! not merely that a row count is 1 (rejecting count-only illusions per the
//! regression-test-verification doctrine).
//!
//! It exercises THREE read paths that share the partition-body parser:
//!   * `iterate_all_partitions` (the full `Index.db` random-read path, the exact
//!     path the issue bisected),
//!   * `get_all_entries` (the sequential-scan path), and
//!   * `get` (the point-read `lookup_partition_with_index` path),
//! proving the fix lands in the shared parser, not one call site.

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::{ScanRow, Value};
use cqlite_core::{Config, Platform};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Write a single partition (id = 1) whose `name` column is `value`.
async fn write_single_cell_fixture(temp: &TempDir, value: String) -> std::path::PathBuf {
    let schema = schema();
    let mut writer = SSTableWriter::new(temp.path().to_path_buf(), 1, &schema).unwrap();
    let m = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(value),
        }],
        1_000_000,
        None,
    );
    let key = m.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![m]).unwrap();
    let info = writer.finish().await.unwrap();
    info.data_path
}

async fn open_reader(data_path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    SSTableReader::open(data_path, &config, platform)
        .await
        .unwrap()
}

/// Extract the single row's `name` text value from a `ScanRow`, if present.
fn name_text(row: &ScanRow) -> Option<String> {
    if let ScanRow::Row(cells) = row {
        for (col, val) in cells {
            if col.as_ref() == "name" {
                if let Value::Text(s) = val {
                    return Some(s.clone());
                }
            }
        }
    }
    None
}

/// Core assertion: a single-cell text value of `size` bytes must round-trip
/// through `iterate_all_partitions` byte-identical (length + content), for the
/// sizes the issue proved silently dropped the row.
async fn assert_roundtrips_full_index(size: usize) {
    let expected = "x".repeat(size);
    let temp = TempDir::new().unwrap();
    let data_path = write_single_cell_fixture(&temp, expected.clone()).await;
    let reader = open_reader(&data_path).await;

    let rows = reader.iterate_all_partitions().await.unwrap();
    assert_eq!(
        rows.len(),
        1,
        "iterate_all_partitions must return the ONE written partition for a \
         {size}-byte single-cell text value (issue #2436: it silently returned \
         0 rows past ~1MB)"
    );
    let got = name_text(&rows[0].1)
        .unwrap_or_else(|| panic!("row for size={size} is missing its `name` text cell"));
    assert_eq!(
        got.len(),
        size,
        "round-tripped `name` length mismatch for size={size}"
    );
    assert_eq!(
        got, expected,
        "round-tripped `name` content mismatch for size={size}"
    );
}

/// Same fixture, read back through the SEQUENTIAL-SCAN path (`get_all_entries`)
/// AND the POINT-READ path (`get`), proving the shared partition-body parser fix
/// covers every read path over an uncompressed BIG SSTable, not just the one the
/// issue enumerated.
async fn assert_roundtrips_scan_and_point(size: usize) {
    let expected = "x".repeat(size);
    let temp = TempDir::new().unwrap();
    let data_path = write_single_cell_fixture(&temp, expected.clone()).await;
    let reader = open_reader(&data_path).await;

    // --- sequential scan ---
    let entries = reader.get_all_entries().await.unwrap();
    assert_eq!(
        entries.len(),
        1,
        "get_all_entries (sequential scan) must return the ONE written partition \
         for size={size} (issue #2436)"
    );
    let (table_id, key, scan_row) = &entries[0];
    let got = name_text(scan_row)
        .unwrap_or_else(|| panic!("sequential-scan row size={size} missing `name` cell"));
    assert_eq!(got.len(), size, "sequential-scan `name` length mismatch size={size}");
    assert_eq!(got, expected, "sequential-scan `name` content mismatch size={size}");

    // --- point read (lookup_partition_with_index -> parse) ---
    let point = reader
        .get(table_id, key)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("point read returned no row for size={size}"));
    let got = name_text(&point)
        .unwrap_or_else(|| panic!("point-read row size={size} missing `name` cell"));
    assert_eq!(got.len(), size, "point-read `name` length mismatch size={size}");
    assert_eq!(got, expected, "point-read `name` content mismatch size={size}");
}

#[tokio::test]
async fn full_index_950k_roundtrips() {
    assert_roundtrips_full_index(950_000).await;
}

#[tokio::test]
async fn full_index_1m_roundtrips() {
    assert_roundtrips_full_index(1_000_000).await;
}

#[tokio::test]
async fn full_index_2m_roundtrips() {
    assert_roundtrips_full_index(2_000_000).await;
}

#[tokio::test]
async fn scan_and_point_1m_roundtrips() {
    assert_roundtrips_scan_and_point(1_000_000).await;
}

#[tokio::test]
async fn scan_and_point_2m_roundtrips() {
    assert_roundtrips_scan_and_point(2_000_000).await;
}
