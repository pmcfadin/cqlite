//! Integration test validating spec-accurate Index.db offset extraction (Issue #92)
//!
//! This test validates the no-heuristics mandate:
//! - Index.db parsed WITHOUT Summary.db must return 0 offsets (no guessing)
//! - This ensures compliance with Issue #28 no-heuristics requirement

use cqlite_core::{
    config::Config, platform::Platform, storage::sstable::index_reader::IndexReader,
};
use std::{path::PathBuf, sync::Arc};

#[tokio::test]
async fn test_index_without_summary_returns_zero() {
    // Test that parsing Index.db without Summary.db returns 0 offsets (no heuristics)
    let datasets_root =
        std::env::var("CQLITE_DATASETS_ROOT").unwrap_or_else(|_| "test-data/datasets".to_string());

    let sstable_dir = format!(
        "{}/sstables/test_basic/simple_table-6de93b70934a11f08d448925b7a9e804",
        datasets_root
    );

    let index_path = PathBuf::from(format!("{}/nb-1-big-Index.db", sstable_dir));
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
