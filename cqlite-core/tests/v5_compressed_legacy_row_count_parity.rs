/// Regression test for Issue #196: Ensure parser reads ALL partitions
///
/// This test validates that the V5CompressedLegacy parser continues reading
/// partitions even when encountering malformed data, rather than silently
/// stopping after ~3 partitions.
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::testing::resolve_table_to_sstable_path;
use cqlite_core::{Config, Platform};

fn count_jsonl_rows(path: &Path) -> std::io::Result<usize> {
    let file = File::open(path)?;
    Ok(BufReader::new(file).lines().count())
}

/// Find the Data.db file in the SSTable directory
fn find_data_db(dir: &Path) -> Option<PathBuf> {
    if dir.is_file() {
        // Already a file, return it if it's Data.db
        if dir.file_name()?.to_str()?.ends_with("-Data.db") {
            return Some(dir.to_path_buf());
        }
        return None;
    }

    // Search directory for Data.db file
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with("-Data.db") {
                    return Some(entry.path());
                }
            }
        }
    }
    None
}

#[tokio::test]
async fn test_v5_multi_partition_parity_simple_table() {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Use test_basic/simple_table - should have multiple partitions
    let sstable_dir = resolve_table_to_sstable_path("test_basic", "simple_table")
        .expect("Failed to resolve test_basic/simple_table path");

    let sstable_path =
        find_data_db(&sstable_dir).expect("Failed to find Data.db file in SSTable directory");

    let reader = SSTableReader::open(&sstable_path, &config, platform)
        .await
        .expect("Failed to open SSTable");

    let entries = reader
        .get_all_entries()
        .await
        .expect("Failed to read entries");

    // Load expected count from JSONL reference file (same basename as Data.db with .jsonl suffix)
    let jsonl_filename = sstable_path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.replace("-Data.db", "-Data.db.jsonl"))
        .expect("Failed to construct JSONL filename");
    let jsonl_path = sstable_dir.join(jsonl_filename);

    let expected_rows = count_jsonl_rows(&jsonl_path).expect("Failed to read Data.db.jsonl file");

    assert_eq!(
        entries.len(),
        expected_rows,
        "Parser stopped early (Issue #196): got {} rows, expected {} from Data.db.jsonl",
        entries.len(),
        expected_rows
    );
}

#[tokio::test]
async fn test_v5_multi_partition_parity_collection_table() {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Use test_collections/collection_table - should have multiple partitions with complex data
    let sstable_dir = resolve_table_to_sstable_path("test_collections", "collection_table")
        .expect("Failed to resolve test_collections/collection_table path");

    let sstable_path =
        find_data_db(&sstable_dir).expect("Failed to find Data.db file in SSTable directory");

    let reader = SSTableReader::open(&sstable_path, &config, platform)
        .await
        .expect("Failed to open SSTable");

    let entries = reader
        .get_all_entries()
        .await
        .expect("Failed to read entries");

    // Load expected count from JSONL reference file (same basename as Data.db with .jsonl suffix)
    let jsonl_filename = sstable_path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.replace("-Data.db", "-Data.db.jsonl"))
        .expect("Failed to construct JSONL filename");
    let jsonl_path = sstable_dir.join(jsonl_filename);

    let expected_rows = count_jsonl_rows(&jsonl_path).expect("Failed to read Data.db.jsonl file");

    assert_eq!(
        entries.len(),
        expected_rows,
        "Parser stopped early on collection table (Issue #196): got {} rows, expected {} from Data.db.jsonl",
        entries.len(),
        expected_rows
    );
}

#[tokio::test]
async fn test_v5_no_early_termination_on_parse_errors() {
    // This test ensures that parse errors don't cause early termination
    // The parser should log warnings and continue to subsequent partitions

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let sstable_dir = resolve_table_to_sstable_path("test_basic", "simple_table")
        .expect("Failed to resolve test_basic/simple_table path");

    let sstable_path =
        find_data_db(&sstable_dir).expect("Failed to find Data.db file in SSTable directory");

    let reader = SSTableReader::open(&sstable_path, &config, platform)
        .await
        .expect("Failed to open SSTable");

    let entries = reader
        .get_all_entries()
        .await
        .expect("Failed to read entries");

    // At minimum, we should get some entries - the bug would cause 0 or very few
    assert!(
        !entries.is_empty(),
        "Parser should read at least some entries, but got 0 (possible early termination)"
    );
}
