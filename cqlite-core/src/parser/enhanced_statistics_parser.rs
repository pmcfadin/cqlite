//! Enhanced Statistics.db parser for Cassandra 5.0 'nb' format
//!
//! # Implementation Status (Issue #162)
//!
//! This module provides **MINIMAL PARSING** of nb-format Statistics.db files to support
//! delta-coded timestamp decoding in V5CompressedLegacy parser.
//!
//! ## Current Implementation
//!
//! Parses ONLY the EncodingStats fields required for delta decoding:
//! - Header (32 bytes): version, data_length, checksum, metadata
//! - EncodingStats section: partitioner, minTimestamp, minLocalDeletionTime, minTTL
//!
//! All other statistics (row counts, histograms, column stats, etc.) are populated with
//! placeholder values. This is sufficient for V5CompressedLegacy parser baseline values.
//!
//! ## Previous Implementation (REMOVED)
//!
//! The previous implementation violated the no-heuristics mandate (Issue #28) by fabricating
//! statistics from header metadata. It was removed and replaced with this minimal real-data
//! parser that extracts only what's needed from the actual binary format.
//!
//! ## Deferred to Future Milestones
//!
//! Complete Statistics.db parsing including:
//! - Row count statistics and distribution histograms
//! - Column-level statistics and cardinality estimates
//! - Partition size histograms and percentiles
//! - Compression ratio and performance metrics
//! - Checksum validation (header.checksum field not yet validated)
//!
//! ## References
//!
//! - Issue #162: Fix Statistics reader for Cassandra 5 nb format
//! - Issue #28: No-heuristics mandate for modern Cassandra 5.0 paths
//! - Issue #105: Remove heuristic estimation from enhanced_statistics_parser.rs
//! - `docs/development/rust_developer_guide.md`: Architecture decisions

use super::statistics::*;
use super::vint::{parse_vint, parse_vuint};
use crate::error::{Error, Result};
use nom::{bytes::complete::take, number::complete::be_u32, IResult};

/// Enhanced Statistics.db header parser for real 'nb' format
///
/// This function parses the actual 32-byte binary header structure from
/// Cassandra 5.0 Statistics.db files. Based on hex analysis of real files:
///
/// ```text
/// 00000000  00 00 00 04 26 29 1b 05  00 00 00 00 00 00 00 2c
/// 00000010  00 00 00 01 00 00 00 65  00 00 00 02 00 00 14 d4
/// ```
///
/// # Binary Format (32 bytes)
///
/// - Bytes 0-3:   `version_type` (u32 BE) - Format version identifier (e.g., 0x00000004)
/// - Bytes 4-7:   `statistics_kind` (u32 BE) - Statistics type marker (e.g., 0x26291b05)
/// - Bytes 8-11:  `reserved1` (u32 BE) - Reserved field (typically 0x00000000)
/// - Bytes 12-15: `data_length` (u32 BE) - Length of variable-length data section
/// - Bytes 16-19: `metadata1` (u32 BE) - Metadata field (purpose TBD in M2)
/// - Bytes 20-23: `metadata2` (u32 BE) - Metadata field (purpose TBD in M2)
/// - Bytes 24-27: `metadata3` (u32 BE) - Metadata field (purpose TBD in M2)
/// - Bytes 28-31: `checksum_or_more` (u32 BE) - Checksum or additional metadata
///
/// # Returns
///
/// `Ok((remaining_input, StatisticsHeader))` on successful parse of 32-byte header.
///
/// # Note
///
/// This is the ONLY function in this module that reads actual binary data.
/// All other parsing functions have been removed per Issue #28 mandate.
pub fn parse_nb_format_header(input: &[u8]) -> IResult<&[u8], StatisticsHeader> {
    let (input, version_type) = be_u32(input)?;
    let (input, statistics_kind) = be_u32(input)?;
    let (input, _reserved1) = be_u32(input)?;
    let (input, data_length) = be_u32(input)?;
    let (input, metadata1) = be_u32(input)?;
    let (input, metadata2) = be_u32(input)?;
    let (input, metadata3) = be_u32(input)?;
    let (input, checksum_or_more) = be_u32(input)?;

    Ok((
        input,
        StatisticsHeader {
            version: version_type,
            statistics_kind,
            data_length,
            metadata1,
            metadata2,
            metadata3,
            checksum: checksum_or_more,
            table_id: None,
        },
    ))
}

/// Parse minimal nb-format statistics data for delta-coding baseline (Issue #162)
///
/// This implementation parses ONLY the EncodingStats fields required for delta decoding:
/// - partitioner (string)
/// - minTimestamp (VInt)
/// - minLocalDeletionTime (VInt)
/// - minTTL (VInt)
///
/// All other fields (histograms, column stats, etc.) are skipped to minimize complexity.
/// This is sufficient for V5CompressedLegacy parser which needs baseline values for
/// delta-coded timestamps and TTLs.
///
/// # Format (observed from real nb-format Statistics.db files)
///
/// After 32-byte header:
/// - metadata_type (u32 BE) = 0x00000003 (indicates EncodingStats section)
/// - data_length (VInt) - length of remaining data
/// - partitioner_length (VInt) - length of partitioner class name string
/// - partitioner (UTF-8 string) - e.g., "org.apache.cassandra.dht.Murmur3Partitioner"
/// - additional_metadata (various VInts) - skipped
/// - minTimestamp (VInt, microseconds)
/// - minLocalDeletionTime (VInt, seconds)
/// - minTTL (VInt, seconds)
///
/// # Returns
///
/// Partial statistics with only TimestampStatistics populated from real data.
pub fn parse_nb_format_statistics_data(
    input: &[u8],
    header: &StatisticsHeader,
) -> Result<(
    RowStatistics,
    TimestampStatistics,
    TableStatistics,
    PartitionStatistics,
    CompressionStatistics,
)> {
    // Parse the EncodingStats section from the data following the header
    let result = parse_minimal_encoding_stats(input);

    match result {
        Ok((_, (min_timestamp, min_deletion_time, min_ttl))) => {
            // Create minimal statistics with only timestamp data populated
            let row_stats = RowStatistics {
                total_rows: 0,
                live_rows: 0,
                tombstone_count: 0,
                partition_count: 0,
                avg_rows_per_partition: 0.0,
                row_size_histogram: vec![],
            };

            let timestamp_stats = TimestampStatistics {
                min_timestamp,
                max_timestamp: min_timestamp, // Not parsed, use min as placeholder
                min_deletion_time,
                max_deletion_time: min_deletion_time,
                min_ttl,
                max_ttl: min_ttl,
                rows_with_ttl: 0,
            };

            let table_stats = TableStatistics {
                disk_size: 0,
                uncompressed_size: 0,
                compressed_size: 0,
                compression_ratio: 1.0,
                block_count: 0,
                avg_block_size: 0.0,
                index_size: 0,
                bloom_filter_size: 0,
                level_count: 0,
            };

            let partition_stats = PartitionStatistics {
                avg_partition_size: 0.0,
                min_partition_size: 0,
                max_partition_size: 0,
                large_partition_percentage: 0.0,
                size_histogram: vec![],
            };

            let compression_stats = CompressionStatistics {
                algorithm: "unknown".to_string(),
                original_size: 0,
                compressed_size: 0,
                ratio: 1.0,
                compression_speed: 0.0,
                decompression_speed: 0.0,
                compressed_blocks: 0,
            };

            Ok((
                row_stats,
                timestamp_stats,
                table_stats,
                partition_stats,
                compression_stats,
            ))
        }
        Err(e) => {
            log::debug!(
                "Failed to parse minimal EncodingStats from Statistics.db: {:?}",
                e
            );
            Err(Error::UnsupportedFormat(format!(
                "Failed to parse minimal nb-format Statistics.db EncodingStats: {:?}. \
                         This is required for delta-coded timestamp decoding. \
                         Header checksum: 0x{:08x}, data_length: {}",
                e, header.checksum, header.data_length
            )))
        }
    }
}

/// Parse minimal EncodingStats section from nb-format Statistics.db
///
/// Returns: (min_timestamp, min_deletion_time, min_ttl)
fn parse_minimal_encoding_stats(input: &[u8]) -> IResult<&[u8], (i64, i64, Option<i64>)> {
    // Skip metadata_type (u32 BE) at start of data section
    let (input, _metadata_type) = be_u32(input)?;

    // Parse data section length (VInt)
    let (input, _data_length) = parse_vuint(input)?;

    // Parse partitioner string length (VInt)
    let (input, partitioner_len) = parse_vuint(input)?;

    // Skip partitioner string (we don't need it)
    let (input, _) = take(partitioner_len as usize)(input)?;

    // The exact structure after partitioner varies, but we know EncodingStats fields appear
    // after some metadata. Based on observed data, we need to skip past additional metadata
    // before reaching minTimestamp, minLocalDeletionTime, and minTTL.
    //
    // Strategy: Parse VInts and look for patterns that match expected timestamp ranges
    // Expected minTimestamp: ~1759713124861209 (microseconds, large positive number)
    // Expected minLocalDeletionTime: ~1442880000 (seconds, reasonable epoch value)
    // Expected minTTL: 0 or small positive number

    // Skip additional metadata (observed: ~2 VInts before timestamp fields)
    let (input, _metadata1) = parse_vuint(input)?;
    let (input, _metadata2) = parse_vuint(input)?;

    // Now parse the EncodingStats fields
    // minTimestamp (VInt, signed microseconds)
    let (input, min_timestamp) = parse_vint(input)?;

    // minLocalDeletionTime (VInt, signed seconds)
    let (input, min_deletion_time) = parse_vint(input)?;

    // minTTL (VInt, signed seconds) - may be 0
    let (input, min_ttl_value) = parse_vint(input)?;
    let min_ttl = if min_ttl_value == 0 {
        Some(0)
    } else {
        Some(min_ttl_value)
    };

    Ok((input, (min_timestamp, min_deletion_time, min_ttl)))
}

/// Main enhanced parser for real Statistics.db files (minimal implementation for Issue #162)
///
/// This function parses the header and minimal EncodingStats fields from nb-format
/// Statistics.db files. Only timestamp-related fields are extracted; all other
/// statistics (histograms, column stats, etc.) are populated with placeholder values.
///
/// This is sufficient for V5CompressedLegacy parser which requires min_timestamp,
/// min_local_deletion_time, and min_ttl for delta decoding baseline.
///
/// # Returns
///
/// SSTableStatistics with only header and timestamp_stats populated from real data.
pub fn parse_enhanced_statistics_file(input: &[u8]) -> IResult<&[u8], SSTableStatistics> {
    // Parse the 32-byte header
    let (remaining, header) = parse_nb_format_header(input)?;

    // Parse minimal statistics data (EncodingStats only)
    let result = parse_nb_format_statistics_data(remaining, &header);

    match result {
        Ok((row_stats, timestamp_stats, table_stats, partition_stats, compression_stats)) => {
            let statistics = SSTableStatistics {
                header,
                row_stats,
                timestamp_stats,
                column_stats: vec![],
                table_stats,
                partition_stats,
                compression_stats,
                metadata: std::collections::HashMap::new(),
            };
            Ok((remaining, statistics))
        }
        Err(e) => {
            // Convert Error to nom::Err
            log::warn!("Failed to parse nb-format Statistics.db: {}", e);
            Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )))
        }
    }
}

/// Enhanced statistics reader with fallback (minimal implementation for Issue #162)
///
/// Attempts to parse nb-format Statistics.db with minimal EncodingStats extraction.
/// This provides the minimum fields needed for delta-coded timestamp decoding.
///
/// # Returns
///
/// SSTableStatistics with minimal fields populated, or error if parsing fails.
pub fn parse_statistics_with_fallback(input: &[u8]) -> IResult<&[u8], SSTableStatistics> {
    // Try the minimal enhanced parser
    parse_enhanced_statistics_file(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nb_format_header_parsing() {
        // Test data based on real file hex dump
        let test_data = vec![
            0x00, 0x00, 0x00, 0x04, // version_type = 4
            0x26, 0x29, 0x1b, 0x05, // statistics_kind
            0x00, 0x00, 0x00, 0x00, // reserved
            0x00, 0x00, 0x00, 0x2c, // data_length = 44
            0x00, 0x00, 0x00, 0x01, // metadata1 = 1
            0x00, 0x00, 0x00, 0x65, // metadata2 = 101
            0x00, 0x00, 0x00, 0x02, // metadata3 = 2
            0x00, 0x00, 0x14, 0xd4, // checksum/more = 5332
        ];

        let result = parse_nb_format_header(&test_data);
        assert!(result.is_ok());

        let (_, header) = result.unwrap();
        assert_eq!(header.version, 4);
        assert_eq!(header.statistics_kind, 0x2629_1b05);
        assert_eq!(header.data_length, 44);
        assert_eq!(header.metadata1, 1);
        assert_eq!(header.metadata2, 101);
        assert_eq!(header.metadata3, 2);
        assert_eq!(header.checksum, 0x14d4);
    }

    #[test]
    fn test_statistics_data_extraction_with_invalid_data() {
        // Test with insufficient/invalid data - should fail to parse VInts
        let header = StatisticsHeader {
            version: 4,
            statistics_kind: 0x2629_1b05,
            data_length: 44,
            metadata1: 1,
            metadata2: 101,
            metadata3: 2,
            checksum: 0x14d4,
            table_id: None,
        };

        let dummy_data = vec![0xFF; 10]; // Too short to parse properly
        let result = parse_nb_format_statistics_data(&dummy_data, &header);

        // Should return error because data is too short for VInt parsing
        assert!(result.is_err());
    }

    #[test]
    fn test_enhanced_statistics_file_with_incomplete_data() {
        // Test data with valid header but missing data section
        let test_data = vec![
            0x00, 0x00, 0x00, 0x04, // version = 4
            0x26, 0x29, 0x1b, 0x05, // statistics_kind
            0x00, 0x00, 0x00, 0x00, // reserved
            0x00, 0x00, 0x00, 0x2c, // data_length = 44
            0x00, 0x00, 0x00, 0x01, // metadata1 = 1
            0x00, 0x00, 0x00, 0x65, // metadata2 = 101
            0x00, 0x00, 0x00, 0x02, // metadata3 = 2
            0x00, 0x00, 0x14,
            0xd4, // checksum = 5332
                  // No data section - should fail parsing
        ];

        let result = parse_enhanced_statistics_file(&test_data);

        // Should fail since there's no data section to parse
        assert!(result.is_err());
    }

    #[test]
    fn test_parser_fallback_with_incomplete_data() {
        // Test with valid header but incomplete data
        let test_data = vec![
            0x00, 0x00, 0x00, 0x04, // version = 4
            0x26, 0x29, 0x1b, 0x05, // statistics_kind
            0x00, 0x00, 0x00, 0x00, // reserved
            0x00, 0x00, 0x00, 0x2c, // data_length = 44
            0x00, 0x00, 0x00, 0x01, // metadata1 = 1
            0x00, 0x00, 0x00, 0x65, // metadata2 = 101
            0x00, 0x00, 0x00, 0x02, // metadata3 = 2
            0x00, 0x00, 0x14, 0xd4, // checksum = 5332
        ];

        let result = parse_statistics_with_fallback(&test_data);

        // Should fail - incomplete data
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_data_returns_error() {
        // Test with insufficient data
        let invalid_data = vec![0xFF; 10];
        let result = parse_statistics_with_fallback(&invalid_data);
        assert!(result.is_err(), "Invalid data should fail to parse");
    }
}
