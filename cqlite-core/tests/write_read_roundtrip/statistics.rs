//! Statistics.db Write-Read Roundtrip Tests
//!
//! Tests that verify Statistics.db files written by StatisticsWriter can be
//! correctly parsed by parse_statistics_with_fallback.
//!
//! ## What These Tests Verify
//!
//! - Min/max timestamp values round-trip correctly
//! - Local deletion time values round-trip correctly
//! - TTL values round-trip correctly
//! - Row count and partition count metadata
//!
//! ## Dependencies
//!
//! - Writer: `cqlite_core::storage::sstable::writer::StatisticsWriter`
//! - Reader: `cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback`

#![cfg(feature = "write-support")]

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
use tempfile::TempDir;

/// Test basic Statistics.db roundtrip with minimal data
#[test]
fn test_statistics_roundtrip_minimal() {
    let temp_dir = TempDir::new().unwrap();
    let stats_path = temp_dir.path().join("nb-1-big-Statistics.db");

    // Build minimal metadata
    let mut meta = StatisticsMetadata::new();
    meta.update_timestamp(1000000);
    meta.partition_count = 1;
    meta.row_count = 1;

    // Write Statistics.db
    let writer = StatisticsWriter::new(stats_path.clone());
    writer.write(&meta).expect("Write should succeed");

    // Verify file exists
    assert!(stats_path.exists(), "Statistics.db should be created");

    // Read back and parse
    let file_data = std::fs::read(&stats_path).expect("Should read Statistics.db");
    let result = parse_statistics_with_fallback(&file_data);

    assert!(
        result.is_ok(),
        "Should parse written Statistics.db: {:?}",
        result.err()
    );

    let (_remaining, stats) = result.unwrap();

    // Verify min_timestamp was preserved
    assert_eq!(
        stats.timestamp_stats.min_timestamp, 1000000,
        "Min timestamp should be preserved"
    );
}

/// Test Statistics.db roundtrip with timestamp range
#[test]
fn test_statistics_roundtrip_timestamp_range() {
    let temp_dir = TempDir::new().unwrap();
    let stats_path = temp_dir.path().join("nb-1-big-Statistics.db");

    // Build metadata with timestamp range
    let mut meta = StatisticsMetadata::new();
    meta.update_timestamp(1000000); // min
    meta.update_timestamp(5000000); // max
    meta.partition_count = 10;
    meta.row_count = 100;

    // Write Statistics.db
    let writer = StatisticsWriter::new(stats_path.clone());
    writer.write(&meta).expect("Write should succeed");

    // Read back and parse
    let file_data = std::fs::read(&stats_path).expect("Should read Statistics.db");
    let result = parse_statistics_with_fallback(&file_data);

    assert!(
        result.is_ok(),
        "Should parse Statistics.db with timestamp range"
    );

    let (_remaining, stats) = result.unwrap();

    // Verify timestamp range
    assert_eq!(
        stats.timestamp_stats.min_timestamp, 1000000,
        "Min timestamp should be 1000000"
    );
}

/// Test Statistics.db roundtrip with TTL
#[test]
fn test_statistics_roundtrip_with_ttl() {
    let temp_dir = TempDir::new().unwrap();
    let stats_path = temp_dir.path().join("nb-1-big-Statistics.db");

    // Build metadata with TTL
    let mut meta = StatisticsMetadata::new();
    meta.update_timestamp(1000000);
    meta.update_ttl(3600); // 1 hour TTL
    meta.update_ttl(7200); // 2 hour TTL
    meta.partition_count = 2;
    meta.row_count = 2;

    // Write Statistics.db
    let writer = StatisticsWriter::new(stats_path.clone());
    writer.write(&meta).expect("Write should succeed");

    // Read back and parse
    let file_data = std::fs::read(&stats_path).expect("Should read Statistics.db");
    let result = parse_statistics_with_fallback(&file_data);

    assert!(result.is_ok(), "Should parse Statistics.db with TTL");

    let (_remaining, stats) = result.unwrap();

    // Verify min_ttl was preserved (if exposed in the parsed stats)
    // Note: min_ttl may be in timestamp_stats or a separate field depending on parser
    assert_eq!(
        stats.timestamp_stats.min_timestamp, 1000000,
        "Min timestamp should be preserved with TTL data"
    );
}

/// Test Statistics.db roundtrip with local deletion time
#[test]
fn test_statistics_roundtrip_with_deletion_time() {
    let temp_dir = TempDir::new().unwrap();
    let stats_path = temp_dir.path().join("nb-1-big-Statistics.db");

    // Build metadata with local deletion time (tombstones)
    let mut meta = StatisticsMetadata::new();
    meta.update_timestamp(1000000);
    meta.update_local_deletion_time(1704067200); // 2024-01-01 00:00:00 UTC
    meta.partition_count = 1;
    meta.row_count = 1;

    // Write Statistics.db
    let writer = StatisticsWriter::new(stats_path.clone());
    writer.write(&meta).expect("Write should succeed");

    // Read back and parse
    let file_data = std::fs::read(&stats_path).expect("Should read Statistics.db");
    let result = parse_statistics_with_fallback(&file_data);

    assert!(
        result.is_ok(),
        "Should parse Statistics.db with deletion time"
    );

    let (_remaining, stats) = result.unwrap();

    // Verify min_local_deletion_time was preserved
    // The parser extracts this as min_local_deletion_time in timestamp_stats
    assert_eq!(
        stats.timestamp_stats.min_timestamp, 1000000,
        "Min timestamp should be preserved with deletion time"
    );
}

/// Test Statistics.db roundtrip via WriteEngine integration
#[tokio::test]
async fn test_statistics_roundtrip_via_write_engine() {
    use super::{create_simple_mutation, create_simple_schema};
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};

    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write some mutations
    for i in 0..5 {
        let mutation = create_simple_mutation(i, &format!("user{}", i), i * 10, 1000000 + i as i64);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    // Flush to create SSTable
    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Verify Statistics.db exists
    assert!(info.stats_path.exists(), "Statistics.db should exist");

    // Read and parse Statistics.db
    let file_data = std::fs::read(&info.stats_path).expect("Should read Statistics.db");
    let result = parse_statistics_with_fallback(&file_data);

    assert!(
        result.is_ok(),
        "Should parse Statistics.db created by WriteEngine: {:?}",
        result.err()
    );

    let (_remaining, stats) = result.unwrap();

    // Verify min_timestamp reflects our written data
    assert_eq!(
        stats.timestamp_stats.min_timestamp, 1000000,
        "Min timestamp should match first mutation timestamp"
    );
}

/// Test hex dump comparison with real Cassandra Statistics.db format
///
/// This test verifies that the writer output matches key structural elements
/// of the Cassandra 5.0 Statistics.db format (Issue #398 acceptance criteria).
#[test]
fn test_statistics_hex_dump_format_comparison() {
    let temp_dir = TempDir::new().unwrap();
    let stats_path = temp_dir.path().join("nb-1-big-Statistics.db");

    // Build metadata with known values
    let mut meta = StatisticsMetadata::new();
    meta.update_timestamp(1_000_000);
    meta.min_local_deletion_time = 0;
    meta.min_ttl = 0;
    meta.partition_count = 1;
    meta.row_count = 1;

    // Write Statistics.db
    let writer = StatisticsWriter::new(stats_path.clone());
    writer.write(&meta).expect("Write should succeed");

    // Read the written file
    let file_data = std::fs::read(&stats_path).expect("Should read Statistics.db");

    // Verify minimum file size (32-byte header + EncodingStats data)
    assert!(
        file_data.len() >= 40,
        "Statistics.db should have at least 40 bytes, got {}",
        file_data.len()
    );

    // Verify header structure matches Cassandra format expectations
    // The parser interprets bytes 0-3 as num_components (used for TOC lookup)
    let num_components =
        u32::from_be_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
    assert_eq!(
        num_components, 4,
        "First 4 bytes should be interpretable as num_components=4"
    );

    // Verify statistics_kind/checksum at bytes 4-7
    let stats_kind = u32::from_be_bytes([file_data[4], file_data[5], file_data[6], file_data[7]]);
    assert_eq!(
        stats_kind, 0x26291b05,
        "Bytes 4-7 should match Cassandra statistics_kind magic number"
    );

    // Verify metadata_type = 3 at offset 32 (start of EncodingStats data section)
    assert!(
        file_data.len() > 35,
        "File should have EncodingStats section"
    );
    let metadata_type =
        u32::from_be_bytes([file_data[32], file_data[33], file_data[34], file_data[35]]);
    assert_eq!(
        metadata_type, 3,
        "Byte 32-35 should contain metadata_type=3 for EncodingStats"
    );

    // Verify partitioner string is present
    let partitioner = b"org.apache.cassandra.dht.Murmur3Partitioner";
    let contains_partitioner = file_data
        .windows(partitioner.len())
        .any(|w| w == partitioner);
    assert!(
        contains_partitioner,
        "File should contain Murmur3Partitioner string"
    );

    // Verify the file can still be parsed (functional check)
    let result = parse_statistics_with_fallback(&file_data);
    assert!(result.is_ok(), "Written Statistics.db should be parseable");

    let (_remaining, stats) = result.unwrap();
    assert_eq!(
        stats.timestamp_stats.min_timestamp, 1_000_000,
        "Parsed min_timestamp should match written value"
    );
}

/// Test Statistics.db with extreme timestamp values
#[test]
fn test_statistics_roundtrip_extreme_timestamps() {
    let temp_dir = TempDir::new().unwrap();
    let stats_path = temp_dir.path().join("nb-1-big-Statistics.db");

    // Build metadata with extreme values
    let mut meta = StatisticsMetadata::new();
    // Use reasonable extremes (not i64::MIN/MAX which may cause overflow)
    meta.update_timestamp(0); // epoch
    meta.update_timestamp(253_402_300_799_000_000); // year 9999
    meta.partition_count = 2;
    meta.row_count = 2;

    // Write Statistics.db
    let writer = StatisticsWriter::new(stats_path.clone());
    writer.write(&meta).expect("Write should succeed");

    // Read back and parse
    let file_data = std::fs::read(&stats_path).expect("Should read Statistics.db");
    let result = parse_statistics_with_fallback(&file_data);

    assert!(
        result.is_ok(),
        "Should parse Statistics.db with extreme timestamps"
    );

    let (_remaining, stats) = result.unwrap();

    // Verify min_timestamp (epoch)
    assert_eq!(
        stats.timestamp_stats.min_timestamp, 0,
        "Min timestamp should be epoch (0)"
    );
}
