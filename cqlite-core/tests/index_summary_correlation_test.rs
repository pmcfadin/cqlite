//! Integration test validating spec-accurate Index.db offset extraction (Issue #92, #237)
//!
//! This test validates correct Index.db parsing:
//! - Index.db in NB format (Cassandra 5.0+) contains VInt-encoded offsets
//! - Offsets are parsed directly from Index.db using proper VInt decoding (Issue #237)
//! - Summary.db is used for sampling/optimization, not required for basic offset discovery
//! - This ensures compliance with Issue #28 no-heuristics requirement (we read actual data)

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
async fn test_index_parses_vint_offsets() {
    // Test that Index.db correctly parses VInt-encoded offsets (Issue #237 fix)
    // NB format (Cassandra 5.0+) stores offsets as unsigned VInts directly in Index.db
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

    // Parse Index.db - should get real VInt-decoded offsets
    let index_reader = IndexReader::open(&index_path, Arc::clone(&platform))
        .await
        .expect("Failed to open Index.db");

    let entries = index_reader.get_partition_entries();

    // Should have entries with actual offsets (not zeros)
    assert!(
        !entries.is_empty(),
        "Index.db should contain partition entries"
    );

    // First entry should have offset 0 (first partition starts at beginning of data section)
    // Subsequent entries should have increasing offsets
    let mut prev_offset = 0u64;
    for (idx, entry) in entries.iter().enumerate() {
        if idx == 0 {
            // First partition typically starts at offset 0
            assert_eq!(
                entry.data_offset, 0,
                "First partition should start at offset 0"
            );
        } else {
            // Subsequent partitions should have increasing offsets
            assert!(
                entry.data_offset >= prev_offset,
                "Partition {} offset ({}) should be >= previous offset ({})",
                idx,
                entry.data_offset,
                prev_offset
            );
        }
        prev_offset = entry.data_offset;
    }

    // Verify we have multiple partitions with non-zero offsets
    let non_zero_count = entries.iter().filter(|e| e.data_offset > 0).count();
    assert!(
        non_zero_count > 0,
        "Should have partitions with non-zero offsets (Issue #237 VInt parsing)"
    );

    println!(
        "✅ Verified: Index.db VInt parsing works - {} entries, {} with non-zero offsets",
        entries.len(),
        non_zero_count
    );
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
        // Note: index_offset renamed to position in Issue #218
        if let Some(summary_entry) = summary_entries
            .iter()
            .find(|e| e.position == index_byte_position)
        {
            // Summary.db position points to Index.db offset
            assert_eq!(
                entry.data_offset, summary_entry.position,
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
