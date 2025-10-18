//! Statistics.db parser for Cassandra 5+ SSTable format
//!
//! This module provides comprehensive parsing of Statistics.db files which contain
//! detailed metadata about SSTable contents including row counts, min/max timestamps,
//! column statistics, and other metadata for efficient query planning.

use super::vint::{parse_vint, parse_vint_length};
use crate::error::{Error, Result};
use nom::{
    bytes::complete::take,
    multi::count,
    number::complete::{be_f64, be_i64, be_u32, be_u64, be_u8},
    IResult,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Statistics.db file header with version and metadata
/// Updated to support both legacy and enhanced formats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsHeader {
    /// Format version/type identifier
    pub version: u32,
    /// Statistics type/kind identifier (for enhanced format) or table_id (legacy)
    pub statistics_kind: u32,
    /// Data length or offset
    pub data_length: u32,
    /// Additional metadata field
    pub metadata1: u32,
    /// Additional metadata field
    pub metadata2: u32,
    /// Additional metadata field
    pub metadata3: u32,
    /// CRC32 checksum of the statistics data
    pub checksum: u32,
    /// Table UUID for validation (optional for enhanced format)
    pub table_id: Option<[u8; 16]>,
}

/// Comprehensive SSTable statistics extracted from Statistics.db
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableStatistics {
    /// Header information
    pub header: StatisticsHeader,
    /// Row count statistics
    pub row_stats: RowStatistics,
    /// Timestamp range information
    pub timestamp_stats: TimestampStatistics,
    /// Column-level statistics
    pub column_stats: Vec<ColumnStatistics>,
    /// Table-level aggregated statistics
    pub table_stats: TableStatistics,
    /// Partition size distribution
    pub partition_stats: PartitionStatistics,
    /// Compression statistics
    pub compression_stats: CompressionStatistics,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
    /// SerializationHeader columns (Issue #163)
    ///
    /// Column definitions parsed from SerializationHeader embedded in nb-format
    /// Statistics.db files. Used for schema extraction in V5CompressedLegacy format.
    /// Empty if SerializationHeader not found in Statistics.db.
    #[serde(default)]
    pub serialization_header_columns: Vec<super::header::ColumnInfo>,
}

/// Row count and distribution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowStatistics {
    /// Total number of rows in the SSTable
    pub total_rows: u64,
    /// Number of live (non-tombstone) rows
    pub live_rows: u64,
    /// Number of tombstone markers
    pub tombstone_count: u64,
    /// Estimated number of partitions
    pub partition_count: u64,
    /// Average rows per partition
    pub avg_rows_per_partition: f64,
    /// Row size distribution histogram
    pub row_size_histogram: Vec<RowSizeBucket>,
}

/// Timestamp range and TTL statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampStatistics {
    /// Minimum timestamp in the SSTable (microseconds since epoch)
    pub min_timestamp: i64,
    /// Maximum timestamp in the SSTable (microseconds since epoch)
    pub max_timestamp: i64,
    /// Minimum deletion time (for tombstones)
    pub min_deletion_time: i64,
    /// Maximum deletion time (for tombstones)
    pub max_deletion_time: i64,
    /// Minimum TTL value
    pub min_ttl: Option<i64>,
    /// Maximum TTL value
    pub max_ttl: Option<i64>,
    /// Number of rows with TTL
    pub rows_with_ttl: u64,
}

/// Per-column statistics for query optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Column name
    pub name: String,
    /// Column type (CQL type)
    pub column_type: String,
    /// Number of non-null values
    pub value_count: u64,
    /// Number of null values
    pub null_count: u64,
    /// Minimum value (serialized as bytes)
    pub min_value: Option<Vec<u8>>,
    /// Maximum value (serialized as bytes)
    pub max_value: Option<Vec<u8>>,
    /// Average serialized size in bytes
    pub avg_size: f64,
    /// Estimated cardinality (distinct values)
    pub cardinality: u64,
    /// Value frequency histogram for common values
    pub value_histogram: Vec<ValueFrequency>,
    /// Whether this column has an index
    pub has_index: bool,
}

/// Table-level aggregated statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    /// Total disk space used by the SSTable
    pub disk_size: u64,
    /// Uncompressed size
    pub uncompressed_size: u64,
    /// Compressed size
    pub compressed_size: u64,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Number of blocks in the SSTable
    pub block_count: u64,
    /// Average block size
    pub avg_block_size: f64,
    /// Index size in bytes
    pub index_size: u64,
    /// Bloom filter size in bytes
    pub bloom_filter_size: u64,
    /// Number of levels in LSM tree
    pub level_count: u32,
}

/// Partition size distribution for efficient range queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionStatistics {
    /// Average partition size in bytes
    pub avg_partition_size: f64,
    /// Minimum partition size
    pub min_partition_size: u64,
    /// Maximum partition size
    pub max_partition_size: u64,
    /// Partition size distribution
    pub size_histogram: Vec<PartitionSizeBucket>,
    /// Percentage of large partitions (>1MB)
    pub large_partition_percentage: f64,
}

/// Compression algorithm performance statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStatistics {
    /// Compression algorithm used
    pub algorithm: String,
    /// Original size before compression
    pub original_size: u64,
    /// Compressed size
    pub compressed_size: u64,
    /// Compression ratio (compressed/original)
    pub ratio: f64,
    /// Compression speed in MB/s
    pub compression_speed: f64,
    /// Decompression speed in MB/s
    pub decompression_speed: f64,
    /// Number of compressed blocks
    pub compressed_blocks: u64,
}

/// Row size distribution bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowSizeBucket {
    /// Size range start (inclusive)
    pub size_start: u64,
    /// Size range end (exclusive)
    pub size_end: u64,
    /// Number of rows in this bucket
    pub count: u64,
    /// Percentage of total rows
    pub percentage: f64,
}

/// Value frequency information for column statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueFrequency {
    /// Serialized value (truncated for large values)
    pub value: Vec<u8>,
    /// Number of occurrences
    pub frequency: u64,
    /// Percentage of total non-null values
    pub percentage: f64,
}

/// Partition size distribution bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSizeBucket {
    /// Size range start (inclusive)
    pub size_start: u64,
    /// Size range end (exclusive)
    pub size_end: u64,
    /// Number of partitions in this bucket
    pub count: u64,
    /// Cumulative percentage
    pub cumulative_percentage: f64,
}

/// Parse the complete Statistics.db file
pub fn parse_statistics_file(input: &[u8]) -> IResult<&[u8], SSTableStatistics> {
    let (input, header) = parse_statistics_header(input)?;
    let (input, row_stats) = parse_row_statistics(input)?;
    let (input, timestamp_stats) = parse_timestamp_statistics(input)?;
    let (input, column_stats) = parse_column_statistics(input, header.data_length)?;
    let (input, table_stats) = parse_table_statistics(input)?;
    let (input, partition_stats) = parse_partition_statistics(input)?;
    let (input, compression_stats) = parse_compression_statistics(input)?;
    let (input, metadata) = parse_metadata_section(input)?;

    Ok((
        input,
        SSTableStatistics {
            header,
            row_stats,
            timestamp_stats,
            column_stats,
            table_stats,
            partition_stats,
            compression_stats,
            metadata,
            serialization_header_columns: vec![], // Not available in legacy format
        },
    ))
}

/// Parse the Statistics.db file header with authoritative format detection
///
/// Statistics.db format is definitively identified by the version field:
/// - **Version 4**: 'nb' (new big) format - Cassandra 5.0+ enhanced statistics
///     - Structure: version(4) + statistics_kind(4) + reserved(4) + data_length(4) +
///       metadata1(4) + metadata2(4) + metadata3(4) + checksum(4) = 32 bytes
///     - Authoritative marker: version == 4
///     - Used by: Cassandra 5.0+ with 'nb' SSTable format
///
/// - **Versions 1-3**: Legacy format - pre-Cassandra 5.0 statistics
///     - Structure: version(4) + table_id(16) + section_count(4) + file_size(8) + checksum(4) = 36 bytes
///     - Authoritative marker: version in range 1..=3
///     - Used by: Cassandra 3.x and 4.x
///
/// Any other version number is unsupported and results in a parse error.
pub fn parse_statistics_header(input: &[u8]) -> IResult<&[u8], StatisticsHeader> {
    let (remaining, version) = be_u32(input)?;

    match version {
        // nb-format: Cassandra 5.0+ enhanced statistics (version 4)
        // This is the authoritative format identifier - no heuristics needed
        4 => parse_nb_format_header(remaining, version),

        // Legacy format: Cassandra 3.x/4.x statistics (versions 1-3)
        // Definitively identified by version range
        1..=3 => parse_legacy_format_header(remaining, version),

        // Unknown/unsupported version - fail explicitly
        // This ensures we never silently misparse corrupt or future formats
        _ => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        ))),
    }
}

/// Parse nb-format (version 4) Statistics.db header
///
/// Format structure (Cassandra 5.0+):
/// ```text
/// [0..4]   version: u32          = 4 (nb-format identifier)
/// [4..8]   statistics_kind: u32  (statistics type/kind identifier)
/// [8..12]  reserved: u32         (reserved field, typically 0)
/// [12..16] data_length: u32      (length of statistics data section)
/// [16..20] metadata1: u32        (metadata field 1)
/// [20..24] metadata2: u32        (metadata field 2)
/// [24..28] metadata3: u32        (metadata field 3)
/// [28..32] checksum: u32         (CRC32 checksum)
/// ```
fn parse_nb_format_header(input: &[u8], version: u32) -> IResult<&[u8], StatisticsHeader> {
    let (input, statistics_kind) = be_u32(input)?;
    let (input, _reserved) = be_u32(input)?;
    let (input, data_length) = be_u32(input)?;
    let (input, metadata1) = be_u32(input)?;
    let (input, metadata2) = be_u32(input)?;
    let (input, metadata3) = be_u32(input)?;
    let (input, checksum) = be_u32(input)?;

    Ok((
        input,
        StatisticsHeader {
            version,
            statistics_kind,
            data_length,
            metadata1,
            metadata2,
            metadata3,
            checksum,
            table_id: None, // nb-format does not include table_id in header
        },
    ))
}

/// Parse legacy format (versions 1-3) Statistics.db header
///
/// Format structure (Cassandra 3.x/4.x):
/// ```text
/// [0..4]   version: u32          = 1, 2, or 3 (legacy format identifier)
/// [4..20]  table_id: [u8; 16]    (UUID of the table)
/// [20..24] section_count: u32    (number of statistics sections)
/// [24..32] file_size: u64        (total file size)
/// [32..36] checksum: u32         (CRC32 checksum)
/// ```
fn parse_legacy_format_header(input: &[u8], version: u32) -> IResult<&[u8], StatisticsHeader> {
    let (input, table_id_raw) = take(16u8)(input)?;
    let mut table_id_array = [0u8; 16];
    table_id_array.copy_from_slice(table_id_raw);

    let (input, section_count) = be_u32(input)?;
    let (input, file_size) = be_u64(input)?;
    let (input, checksum) = be_u32(input)?;

    Ok((
        input,
        StatisticsHeader {
            version,
            statistics_kind: 0, // Not used in legacy format
            data_length: section_count,
            metadata1: (file_size >> 32) as u32,
            metadata2: file_size as u32,
            metadata3: 0,
            checksum,
            table_id: Some(table_id_array),
        },
    ))
}

/// Parse row count and distribution statistics
pub fn parse_row_statistics(input: &[u8]) -> IResult<&[u8], RowStatistics> {
    let (input, total_rows) = parse_vint_as_u64(input)?;
    let (input, live_rows) = parse_vint_as_u64(input)?;
    let (input, tombstone_count) = parse_vint_as_u64(input)?;
    let (input, partition_count) = parse_vint_as_u64(input)?;
    let (input, avg_rows_per_partition) = be_f64(input)?;
    let (input, histogram_count) = be_u32(input)?;
    let (input, row_size_histogram) =
        count(parse_row_size_bucket, histogram_count as usize)(input)?;

    Ok((
        input,
        RowStatistics {
            total_rows,
            live_rows,
            tombstone_count,
            partition_count,
            avg_rows_per_partition,
            row_size_histogram,
        },
    ))
}

/// Parse timestamp range statistics
pub fn parse_timestamp_statistics(input: &[u8]) -> IResult<&[u8], TimestampStatistics> {
    let (input, min_timestamp) = be_i64(input)?;
    let (input, max_timestamp) = be_i64(input)?;
    let (input, min_deletion_time) = be_i64(input)?;
    let (input, max_deletion_time) = be_i64(input)?;
    let (input, has_ttl) = be_u8(input)?;
    let (input, min_ttl, max_ttl, rows_with_ttl) = if has_ttl != 0 {
        let (input, min_ttl) = be_i64(input)?;
        let (input, max_ttl) = be_i64(input)?;
        let (input, rows_with_ttl) = parse_vint_as_u64(input)?;
        (input, Some(min_ttl), Some(max_ttl), rows_with_ttl)
    } else {
        (input, None, None, 0)
    };

    Ok((
        input,
        TimestampStatistics {
            min_timestamp,
            max_timestamp,
            min_deletion_time,
            max_deletion_time,
            min_ttl,
            max_ttl,
            rows_with_ttl,
        },
    ))
}

/// Parse column-level statistics
pub fn parse_column_statistics(
    input: &[u8],
    column_count: u32,
) -> IResult<&[u8], Vec<ColumnStatistics>> {
    count(parse_single_column_statistics, column_count as usize)(input)
}

/// Parse statistics for a single column
pub fn parse_single_column_statistics(input: &[u8]) -> IResult<&[u8], ColumnStatistics> {
    let (input, name_len) = parse_vint_length(input)?;
    let (input, name_bytes) = take(name_len)(input)?;
    let name = String::from_utf8_lossy(name_bytes).to_string();

    let (input, type_len) = parse_vint_length(input)?;
    let (input, type_bytes) = take(type_len)(input)?;
    let column_type = String::from_utf8_lossy(type_bytes).to_string();

    let (input, value_count) = parse_vint_as_u64(input)?;
    let (input, null_count) = parse_vint_as_u64(input)?;

    let (input, has_min_max) = be_u8(input)?;
    let (input, min_value, max_value) = if has_min_max != 0 {
        let (input, min_len) = parse_vint_length(input)?;
        let (input, min_bytes) = take(min_len)(input)?;
        let (input, max_len) = parse_vint_length(input)?;
        let (input, max_bytes) = take(max_len)(input)?;
        (input, Some(min_bytes.to_vec()), Some(max_bytes.to_vec()))
    } else {
        (input, None, None)
    };

    let (input, avg_size) = be_f64(input)?;
    let (input, cardinality) = parse_vint_as_u64(input)?;

    let (input, histogram_count) = be_u32(input)?;
    let (input, value_histogram) = count(parse_value_frequency, histogram_count as usize)(input)?;

    let (input, has_index) = be_u8(input)?;

    Ok((
        input,
        ColumnStatistics {
            name,
            column_type,
            value_count,
            null_count,
            min_value,
            max_value,
            avg_size,
            cardinality,
            value_histogram,
            has_index: has_index != 0,
        },
    ))
}

/// Parse table-level statistics
pub fn parse_table_statistics(input: &[u8]) -> IResult<&[u8], TableStatistics> {
    let (input, disk_size) = be_u64(input)?;
    let (input, uncompressed_size) = be_u64(input)?;
    let (input, compression_ratio) = be_f64(input)?;
    let (input, block_count) = parse_vint_as_u64(input)?;
    let (input, avg_block_size) = be_f64(input)?;
    let (input, index_size) = be_u64(input)?;
    let (input, bloom_filter_size) = be_u64(input)?;
    let (input, level_count) = be_u32(input)?;

    Ok((
        input,
        TableStatistics {
            disk_size,
            uncompressed_size,
            compressed_size: disk_size, // For now, assume disk_size is compressed_size
            compression_ratio,
            block_count,
            avg_block_size,
            index_size,
            bloom_filter_size,
            level_count,
        },
    ))
}

/// Parse partition size distribution statistics
pub fn parse_partition_statistics(input: &[u8]) -> IResult<&[u8], PartitionStatistics> {
    let (input, avg_partition_size) = be_f64(input)?;
    let (input, min_partition_size) = be_u64(input)?;
    let (input, max_partition_size) = be_u64(input)?;
    let (input, large_partition_percentage) = be_f64(input)?;

    let (input, histogram_count) = be_u32(input)?;
    let (input, size_histogram) =
        count(parse_partition_size_bucket, histogram_count as usize)(input)?;

    Ok((
        input,
        PartitionStatistics {
            avg_partition_size,
            min_partition_size,
            max_partition_size,
            size_histogram,
            large_partition_percentage,
        },
    ))
}

/// Parse compression performance statistics
pub fn parse_compression_statistics(input: &[u8]) -> IResult<&[u8], CompressionStatistics> {
    let (input, algorithm_len) = parse_vint_length(input)?;
    let (input, algorithm_bytes) = take(algorithm_len)(input)?;
    let algorithm = String::from_utf8_lossy(algorithm_bytes).to_string();

    let (input, original_size) = be_u64(input)?;
    let (input, compressed_size) = be_u64(input)?;
    let (input, ratio) = be_f64(input)?;
    let (input, compression_speed) = be_f64(input)?;
    let (input, decompression_speed) = be_f64(input)?;
    let (input, compressed_blocks) = parse_vint_as_u64(input)?;

    Ok((
        input,
        CompressionStatistics {
            algorithm,
            original_size,
            compressed_size,
            ratio,
            compression_speed,
            decompression_speed,
            compressed_blocks,
        },
    ))
}

/// Parse additional metadata section
pub fn parse_metadata_section(input: &[u8]) -> IResult<&[u8], HashMap<String, String>> {
    let (input, metadata_count) = be_u32(input)?;
    let mut metadata = HashMap::new();

    let mut remaining = input;
    for _ in 0..metadata_count {
        let (next, key_len) = parse_vint_length(remaining)?;
        let (next, key_bytes) = take(key_len)(next)?;
        let key = String::from_utf8_lossy(key_bytes).to_string();

        let (next, value_len) = parse_vint_length(next)?;
        let (next, value_bytes) = take(value_len)(next)?;
        let value = String::from_utf8_lossy(value_bytes).to_string();

        metadata.insert(key, value);
        remaining = next;
    }

    Ok((remaining, metadata))
}

/// Parse a row size histogram bucket
pub fn parse_row_size_bucket(input: &[u8]) -> IResult<&[u8], RowSizeBucket> {
    let (input, size_start) = parse_vint_as_u64(input)?;
    let (input, size_end) = parse_vint_as_u64(input)?;
    let (input, count) = parse_vint_as_u64(input)?;
    let (input, percentage) = be_f64(input)?;

    Ok((
        input,
        RowSizeBucket {
            size_start,
            size_end,
            count,
            percentage,
        },
    ))
}

/// Parse a partition size histogram bucket
pub fn parse_partition_size_bucket(input: &[u8]) -> IResult<&[u8], PartitionSizeBucket> {
    let (input, size_start) = parse_vint_as_u64(input)?;
    let (input, size_end) = parse_vint_as_u64(input)?;
    let (input, count) = parse_vint_as_u64(input)?;
    let (input, cumulative_percentage) = be_f64(input)?;

    Ok((
        input,
        PartitionSizeBucket {
            size_start,
            size_end,
            count,
            cumulative_percentage,
        },
    ))
}

/// Parse a value frequency entry
pub fn parse_value_frequency(input: &[u8]) -> IResult<&[u8], ValueFrequency> {
    let (input, value_len) = parse_vint_length(input)?;
    let (input, value_bytes) = take(value_len)(input)?;
    let (input, frequency) = parse_vint_as_u64(input)?;
    let (input, percentage) = be_f64(input)?;

    Ok((
        input,
        ValueFrequency {
            value: value_bytes.to_vec(),
            frequency,
            percentage,
        },
    ))
}

/// Helper function to parse VInt as u64
fn parse_vint_as_u64(input: &[u8]) -> IResult<&[u8], u64> {
    let (input, value) = parse_vint(input)?;
    Ok((input, value as u64))
}

/// Statistics analyzer for enhanced reporting
pub struct StatisticsAnalyzer;

impl StatisticsAnalyzer {
    /// Analyze statistics and generate human-readable summary
    pub fn analyze(stats: &SSTableStatistics) -> StatisticsSummary {
        let data_efficiency = Self::calculate_data_efficiency(stats);
        let query_performance_hints = Self::generate_query_hints(stats);
        let storage_recommendations = Self::generate_storage_recommendations(stats);
        let health_score = Self::calculate_health_score(stats);

        StatisticsSummary {
            total_rows: stats.row_stats.total_rows,
            live_data_percentage: (stats.row_stats.live_rows as f64
                / stats.row_stats.total_rows as f64)
                * 100.0,
            compression_efficiency: stats.compression_stats.ratio * 100.0,
            timestamp_range_days: Self::calculate_timestamp_range_days(stats),
            largest_partition_mb: stats.partition_stats.max_partition_size as f64 / 1_048_576.0,
            data_efficiency,
            query_performance_hints,
            storage_recommendations,
            health_score,
        }
    }

    fn calculate_data_efficiency(stats: &SSTableStatistics) -> f64 {
        let live_ratio = stats.row_stats.live_rows as f64 / stats.row_stats.total_rows as f64;
        let compression_ratio = stats.compression_stats.ratio;
        let partition_efficiency = 1.0 - (stats.partition_stats.large_partition_percentage / 100.0);

        (live_ratio + compression_ratio + partition_efficiency) / 3.0 * 100.0
    }

    fn generate_query_hints(stats: &SSTableStatistics) -> Vec<String> {
        let mut hints = Vec::new();

        if stats.partition_stats.large_partition_percentage > 10.0 {
            hints.push("Consider reviewing partition key design - high percentage of large partitions detected".to_string());
        }

        if stats.row_stats.tombstone_count > stats.row_stats.live_rows / 4 {
            hints.push("High tombstone ratio - consider running compaction".to_string());
        }

        if stats.table_stats.compression_ratio < 0.5 {
            hints.push("Low compression ratio - data may not be well-suited for current compression algorithm".to_string());
        }

        hints
    }

    fn generate_storage_recommendations(stats: &SSTableStatistics) -> Vec<String> {
        let mut recommendations = Vec::new();

        if stats.table_stats.disk_size > 1_073_741_824 {
            recommendations
                .push("Large SSTable detected - consider more frequent compaction".to_string());
        }

        if stats.row_stats.avg_rows_per_partition < 10.0 {
            recommendations.push(
                "Low average rows per partition - partition key may be too granular".to_string(),
            );
        }

        recommendations
    }

    fn calculate_health_score(stats: &SSTableStatistics) -> f64 {
        let mut score = 100.0;

        // Deduct for high tombstone ratio
        let tombstone_ratio =
            stats.row_stats.tombstone_count as f64 / stats.row_stats.total_rows as f64;
        score -= tombstone_ratio * 30.0;

        // Deduct for poor compression
        if stats.compression_stats.ratio < 0.5 {
            score -= 20.0;
        }

        // Deduct for large partitions
        score -= stats.partition_stats.large_partition_percentage;

        score.max(0.0)
    }

    fn calculate_timestamp_range_days(stats: &SSTableStatistics) -> f64 {
        let range_micros =
            stats.timestamp_stats.max_timestamp - stats.timestamp_stats.min_timestamp;
        range_micros as f64 / (1_000_000.0 * 60.0 * 60.0 * 24.0)
    }
}

/// Human-readable statistics summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsSummary {
    pub total_rows: u64,
    pub live_data_percentage: f64,
    pub compression_efficiency: f64,
    pub timestamp_range_days: f64,
    pub largest_partition_mb: f64,
    pub data_efficiency: f64,
    pub query_performance_hints: Vec<String>,
    pub storage_recommendations: Vec<String>,
    pub health_score: f64,
}

/// Serialize Statistics structure to bytes (for testing and validation)
pub fn serialize_statistics(_stats: &SSTableStatistics) -> Result<Vec<u8>> {
    // This would implement the reverse of parsing for complete round-trip testing
    // For now, return an error indicating this is not implemented
    Err(Error::corruption(
        "Statistics serialization not yet implemented",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistics_header_parsing() {
        let test_data = vec![
            0x00, 0x00, 0x00, 0x01, // version = 1
            // table_id (16 bytes)
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
            0x0F, 0x10, 0x00, 0x00, 0x00, 0x05, // section_count = 5
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // file_size = 4096
            0x12, 0x34, 0x56, 0x78, // checksum
        ];

        let result = parse_statistics_header(&test_data);
        assert!(result.is_ok());

        let (_, header) = result.unwrap();
        assert_eq!(header.version, 1);
        // assert_eq!(header.section_count, 5); // Field not available
        // assert_eq!(header.file_size, 4096); // Field not available
        assert_eq!(header.checksum, 0x12345678);
    }

    #[test]
    fn test_nb_format_authoritative_detection() {
        // nb-format (version 4) - should parse as nb-format
        let nb_data = vec![
            0x00, 0x00, 0x00, 0x04, // version = 4 (authoritative nb-format marker)
            0x26, 0x29, 0x1b, 0x05, // statistics_kind
            0x00, 0x00, 0x00, 0x00, // reserved
            0x00, 0x00, 0x00, 0x2c, // data_length = 44
            0x00, 0x00, 0x00, 0x01, // metadata1 = 1
            0x00, 0x00, 0x00, 0x65, // metadata2 = 101
            0x00, 0x00, 0x00, 0x02, // metadata3 = 2
            0x00, 0x00, 0x14, 0xd4, // checksum = 5332
        ];

        let result = parse_statistics_header(&nb_data);
        assert!(result.is_ok());

        let (_, header) = result.unwrap();
        assert_eq!(header.version, 4);
        assert_eq!(header.statistics_kind, 0x26291b05);
        assert_eq!(header.data_length, 44);
        assert!(header.table_id.is_none()); // nb-format has no table_id
    }

    #[test]
    fn test_legacy_format_authoritative_detection() {
        // Legacy format (version 2) - should parse as legacy
        let legacy_data = vec![
            0x00, 0x00, 0x00, 0x02, // version = 2 (authoritative legacy marker)
            // table_id (16 bytes)
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00, 0x00, 0x00, 0x00, 0x0A, // section_count = 10
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, // file_size = 65536
            0xAB, 0xCD, 0xEF, 0x12, // checksum
        ];

        let result = parse_statistics_header(&legacy_data);
        assert!(result.is_ok());

        let (_, header) = result.unwrap();
        assert_eq!(header.version, 2);
        assert_eq!(header.statistics_kind, 0); // legacy format doesn't use this
        assert!(header.table_id.is_some()); // legacy format has table_id
    }

    #[test]
    fn test_unsupported_version_rejection() {
        // Version 0 - should be rejected
        let invalid_v0 = vec![
            0x00, 0x00, 0x00, 0x00, // version = 0 (invalid)
            0x00, 0x00, 0x00, 0x00, // ...rest doesn't matter
        ];
        assert!(parse_statistics_header(&invalid_v0).is_err());

        // Version 5 - should be rejected (future/unknown version)
        let invalid_v5 = vec![
            0x00, 0x00, 0x00, 0x05, // version = 5 (unsupported)
            0x00, 0x00, 0x00, 0x00, // ...rest doesn't matter
        ];
        assert!(parse_statistics_header(&invalid_v5).is_err());

        // Version 255 - should be rejected
        let invalid_v255 = vec![
            0x00, 0x00, 0x00, 0xFF, // version = 255 (unsupported)
            0x00, 0x00, 0x00, 0x00, // ...rest doesn't matter
        ];
        assert!(parse_statistics_header(&invalid_v255).is_err());
    }

    #[test]
    fn test_no_heuristics_version_4_with_short_input() {
        // Previous implementation used heuristic: version == 4 && input.len() >= 28
        // New implementation uses ONLY version number - no length check heuristic
        // This test ensures we don't fall back to legacy parsing with short input

        let short_nb_data = vec![
            0x00, 0x00, 0x00, 0x04, // version = 4 (authoritative nb-format)
            0x26, 0x29, 0x1b, 0x05, // statistics_kind
            0x00, 0x00, 0x00, 0x00, // reserved
            0x00, 0x00, 0x00,
            0x2c, // data_length = 44
                  // Missing remaining fields - should fail parsing, not switch formats
        ];

        let result = parse_statistics_header(&short_nb_data);
        // Should fail because version 4 DEFINITIVELY means nb-format
        // and nb-format requires 32 bytes. This is NOT a heuristic,
        // it's the authoritative format specification.
        assert!(result.is_err());
    }

    #[test]
    fn test_statistics_analyzer() {
        let stats = create_test_statistics();
        let summary = StatisticsAnalyzer::analyze(&stats);

        assert!(summary.total_rows > 0);
        assert!(summary.health_score >= 0.0 && summary.health_score <= 100.0);
        assert!(summary.live_data_percentage >= 0.0 && summary.live_data_percentage <= 100.0);
    }

    fn create_test_statistics() -> SSTableStatistics {
        SSTableStatistics {
            header: StatisticsHeader {
                version: 1,
                statistics_kind: 3,
                data_length: 1024,
                metadata1: 0,
                metadata2: 0,
                metadata3: 0,
                checksum: 0x12345678,
                table_id: Some([1; 16]),
            },
            row_stats: RowStatistics {
                total_rows: 1000,
                live_rows: 900,
                tombstone_count: 100,
                partition_count: 50,
                avg_rows_per_partition: 20.0,
                row_size_histogram: vec![],
            },
            timestamp_stats: TimestampStatistics {
                min_timestamp: 1000000,
                max_timestamp: 2000000,
                min_deletion_time: 0,
                max_deletion_time: 0,
                min_ttl: None,
                max_ttl: None,
                rows_with_ttl: 0,
            },
            column_stats: vec![],
            table_stats: TableStatistics {
                disk_size: 1024 * 1024,
                uncompressed_size: 2048 * 1024,
                compressed_size: 1024 * 1024,
                compression_ratio: 0.5,
                block_count: 100,
                avg_block_size: 1024.0,
                index_size: 1024,
                bloom_filter_size: 512,
                level_count: 1,
            },
            partition_stats: PartitionStatistics {
                avg_partition_size: 20480.0,
                min_partition_size: 1024,
                max_partition_size: 1048576,
                size_histogram: vec![],
                large_partition_percentage: 5.0,
            },
            compression_stats: CompressionStatistics {
                algorithm: "LZ4".to_string(),
                original_size: 2048 * 1024,
                compressed_size: 1024 * 1024,
                ratio: 0.5,
                compression_speed: 100.0,
                decompression_speed: 200.0,
                compressed_blocks: 100,
            },
            metadata: HashMap::new(),
            serialization_header_columns: vec![],
        }
    }
}
