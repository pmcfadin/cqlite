//! Integration test: Write Statistics.db and verify it can be parsed

#![cfg(feature = "write-support")]

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};

#[test]
fn test_statistics_roundtrip() {
    // Initialize logging to see debug output
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init();
    // Create test file in /tmp for debugging
    let stats_path = std::path::PathBuf::from("/tmp/test-roundtrip-Statistics.db");

    // Build metadata
    let mut meta = StatisticsMetadata::new();
    meta.update_timestamp(1000000);
    meta.update_timestamp(2000000);
    meta.update_local_deletion_time(1234567);
    meta.update_ttl(3600);
    meta.increment_partition_count();
    meta.increment_partition_count();
    meta.partition_count = 10;
    meta.row_count = 100;

    // Write Statistics.db
    let writer = StatisticsWriter::new(stats_path.clone());
    writer.write(&meta, None).expect("Write should succeed");

    // Verify file exists
    assert!(stats_path.exists());

    // Read back and parse
    let file_data = std::fs::read(&stats_path).expect("Should read file");
    let result = parse_statistics_with_fallback(&file_data, None);

    assert!(
        result.is_ok(),
        "Should parse written Statistics.db: {:?}",
        result
    );

    let (_remaining, stats) = result.unwrap();

    // Verify timestamp stats were preserved
    assert_eq!(stats.timestamp_stats.min_timestamp, 1000000);

    println!("✓ Statistics.db roundtrip test passed");
}
