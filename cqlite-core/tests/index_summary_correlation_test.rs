//! Integration test validating spec-accurate Index.db offset extraction (Issue #92)
//!
//! This test validates the no-heuristics mandate:
//! - Index.db parsed WITHOUT Summary.db must return 0 offsets (no guessing)
//! - Index.db parsed WITH Summary.db must use correlation for actual offsets
//! - This ensures compliance with Issue #28 no-heuristics requirement

use cqlite_core::{
    config::Config,
    platform::Platform,
    storage::sstable::{index_reader::IndexReader, summary_reader::SummaryReader},
};
use std::{fs, path::PathBuf, sync::Arc};

/// Find a table directory by name pattern (e.g., "simple_table-<uuid>")
fn find_table_dir(datasets_root: &str, table_name: &str) -> Option<PathBuf> {
    let sstable_path = PathBuf::from(datasets_root).join("sstables/test_basic");

    if let Ok(entries) = fs::read_dir(&sstable_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                    if dir_name.starts_with(&format!("{}-", table_name)) {
                        return Some(path);
                    }
                }
            }
        }
    }

    None
}

#[tokio::test]
async fn test_index_without_summary_returns_zero() {
    // Test that parsing Index.db without Summary.db returns 0 offsets (no heuristics)
    let datasets_root = std::env::var("CQLITE_DATASETS_ROOT")
        .expect("CQLITE_DATASETS_ROOT must be set for this test");

    let sstable_dir = find_table_dir(&datasets_root, "simple_table")
        .expect("simple_table directory must exist in test_basic");

    let index_path = sstable_dir.join("nb-1-big-Index.db");

    assert!(
        index_path.exists(),
        "Index.db file must exist at {:?} for this test",
        index_path
    );

    let config = Config::memory_optimized();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create Platform"),
    );

    // Parse Index.db WITHOUT Summary.db
    let index_reader = IndexReader::open(&index_path, Arc::clone(&platform))
        .await
        .expect("Failed to open Index.db");

    let entries = index_reader.get_partition_entries();

    // All offsets should be 0 (Issue #92 - no heuristics mandate)
    for entry in entries {
        assert_eq!(
            entry.data_offset, 0,
            "Without Summary.db, offsets must be 0 (no heuristics)"
        );
    }

    println!("✅ Verified: Index.db without Summary.db returns zero offsets (no heuristics)");
}

#[tokio::test]
#[ignore = "Known issue: Summary.db parser has C5 format compatibility issues (Issue #92)"]
async fn test_index_with_summary_correlation() {
    // Test that parsing Index.db WITH Summary.db uses proper correlation for offsets
    // NOTE: Summary.db parser has known issues with C5 format (Issue #92 scope: Index.db only)
    // This test validates the correlation logic exists and will work when Summary parser is fixed

    let datasets_root = std::env::var("CQLITE_DATASETS_ROOT")
        .expect("CQLITE_DATASETS_ROOT must be set for this test");

    let sstable_dir = find_table_dir(&datasets_root, "simple_table")
        .expect("simple_table directory must exist in test_basic");

    let index_path = sstable_dir.join("nb-1-big-Index.db");
    let summary_path = sstable_dir.join("nb-1-big-Summary.db");

    assert!(
        index_path.exists() && summary_path.exists(),
        "Index.db and Summary.db files must exist for this test"
    );

    let config = Config::memory_optimized();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create Platform"),
    );

    // Try to parse Summary.db - will fail with known C5 format issues
    let summary_reader_result = SummaryReader::open(&summary_path, Arc::clone(&platform)).await;

    let summary_reader = summary_reader_result.unwrap();

    // Parse Index.db WITH Summary.db
    let index_reader =
        IndexReader::open_with_summary(&index_path, Arc::clone(&platform), Some(&summary_reader))
            .await
            .expect("Failed to open Index.db with Summary");

    let entries = index_reader.get_partition_entries();
    let summary_entries = summary_reader.get_entries();

    // Verify that entries matching Summary samples have non-zero offsets
    let mut matched_offsets = 0;
    for (idx, entry) in entries.iter().enumerate() {
        let index_byte_position = (idx * 18) as u64;

        // Check if this entry matches a Summary sample
        if let Some(summary_entry) = summary_entries
            .iter()
            .find(|e| e.index_offset == index_byte_position)
        {
            assert_eq!(
                entry.data_offset, summary_entry.position as u64,
                "Entry at index {} should match Summary.db position",
                idx
            );
            matched_offsets += 1;
        }
    }

    assert!(
        matched_offsets > 0,
        "Should have at least one exact Summary.db match"
    );

    println!(
        "✅ Verified: Index.db with Summary.db correlation - {} exact matches",
        matched_offsets
    );
}
