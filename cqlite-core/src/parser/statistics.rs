//! Statistics.db parser for Cassandra 5+ SSTable format
//!
//! This module provides comprehensive parsing of Statistics.db files which contain
//! detailed metadata about SSTable contents including row counts, min/max timestamps,
//! column statistics, and other metadata for efficient query planning.

// Issue #1623/#1637: the legacy Statistics.db parse/serialize subtree is superseded
// on the prod path by enhanced_statistics_parser and was removed as dead code (J3).
// What remains here is the shared statistics type definitions reused by the enhanced
// parser and reader, the authoritative header/timestamp decoders (pinned by tests),
// and the `StatisticsAnalyzer` report path (wired into `StatisticsReader`).
use super::vint::parse_vint;
use nom::{
    bytes::complete::take,
    number::complete::{be_i64, be_u32, be_u64, be_u8},
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
    /// Table-level aggregated statistics, or `None` when NOT authoritatively
    /// parsed from `Statistics.db` (issue #1653). The enhanced (nb) parser does
    /// not decode these table-level metrics, so it is honestly `None` rather than
    /// a fabricated all-zero `TableStatistics`; the legacy parser populates
    /// `Some(..)`.
    pub table_stats: Option<TableStatistics>,
    /// Partition size distribution, or `None` when NOT authoritatively parsed from
    /// `Statistics.db` (issue #1653 — the enhanced parser previously fabricated an
    /// all-zero `PartitionStatistics`).
    pub partition_stats: Option<PartitionStatistics>,
    /// Compression statistics, or `None` when NOT authoritatively parsed from
    /// `Statistics.db` (issue #1653 — the enhanced parser previously fabricated a
    /// `CompressionStatistics` with `algorithm: "unknown"` and all-zero metrics).
    pub compression_stats: Option<CompressionStatistics>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
    /// SerializationHeader columns (Issue #163)
    ///
    /// Column definitions parsed from SerializationHeader embedded in nb-format
    /// Statistics.db files. Used for schema extraction in V5CompressedLegacy format.
    /// Empty if SerializationHeader not found in Statistics.db.
    #[serde(default)]
    pub serialization_header_columns: Vec<super::header::ColumnInfo>,
    /// Partition key definitions extracted from SerializationHeader (Issue #195)
    #[serde(default)]
    pub serialization_header_partition_keys: Vec<super::header::ColumnInfo>,
    /// Clustering key definitions extracted from SerializationHeader (Issue #195)
    #[serde(default)]
    pub serialization_header_clustering_keys: Vec<super::header::ColumnInfo>,
    /// Estimated tombstone-drop-times histogram as `(point, count)` pairs,
    /// decoded best-effort from the STATS component (Issue #1073). Empty when
    /// the SSTable carries no tombstones or the histogram could not be decoded.
    #[serde(default)]
    pub tombstone_drop_times: Vec<(i64, u64)>,
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
    /// Maximum timestamp in the SSTable (microseconds since epoch), or `None`
    /// when it is NOT authoritatively available from `Statistics.db` (issue
    /// #1653). The enhanced (nb) parser leaves this `None` until the best-effort
    /// STATS post-pass recovers the real `maxTimestamp` (#1729); a `None` here is
    /// an honest "unavailable", NOT a fabricated placeholder (it previously
    /// aliased `min_timestamp`/`i64::MIN`, both of which lied about the max). A
    /// consumer that needs a real maximum MUST treat `None` as unavailable and
    /// fail closed (see `write_engine::merge::fully_expired`).
    pub max_timestamp: Option<i64>,
    /// Minimum deletion time (for tombstones)
    pub min_deletion_time: i64,
    /// Maximum deletion time (for tombstones). Genuinely decoded from STATS by the
    /// best-effort post-pass (#1073/#1011); before it runs the enhanced parser
    /// uses the authoritative `NO_DELETION_TIME` (`i64::MAX`) "no deletions
    /// recorded" value, so a post-pass failure fails CLOSED (never classified
    /// fully-expired) rather than the old `= min_deletion_time` lie (issue #1653).
    pub max_deletion_time: i64,
    /// Minimum TTL value
    pub min_ttl: Option<i64>,
    /// Maximum TTL value, or `None` when not authoritatively available (issue
    /// #1653 — the enhanced parser previously aliased this to `min_ttl`, a lie).
    pub max_ttl: Option<i64>,
    /// Number of rows with TTL, or `None` when not authoritatively available from
    /// `Statistics.db` (issue #1653). The enhanced (nb) parser does not decode a
    /// per-SSTable rows-with-TTL count, so it is honestly `None` rather than a
    /// fabricated `0` (which claimed "no rows have a TTL").
    pub rows_with_ttl: Option<u64>,
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
            // Legacy format genuinely parses `max_timestamp` and `rows_with_ttl`
            // from fixed-width fields (issue #1653); the enhanced parser leaves
            // them `None` when unavailable. But Cassandra seeds its timestamp
            // `MinMaxLongTracker` max with `Long.MIN_VALUE`, so an SSTable that
            // recorded no live write timestamp serializes that `i64::MIN`
            // sentinel verbatim. Decode it to `None` via the SAME helper the
            // enhanced parser uses, so a legacy parse can never leak the "no max
            // recorded" sentinel as a real maximum (roborev #1653 legacy leak).
            max_timestamp: crate::parser::repair_metadata::decode_max_timestamp(max_timestamp),
            min_deletion_time,
            max_deletion_time,
            min_ttl,
            max_ttl,
            rows_with_ttl: Some(rows_with_ttl),
        },
    ))
}

/// Helper function to parse VInt as u64
fn parse_vint_as_u64(input: &[u8]) -> IResult<&[u8], u64> {
    let (input, value) = parse_vint(input)?;
    Ok((input, value as u64))
}

/// Whether `avg_rows_per_partition` holds a REAL derived value rather than the
/// documented #1325 unavailable sentinel `0.0`.
///
/// The average is `total_rows / partition_count`; the parser leaves it `0.0`
/// whenever EITHER `total_rows` is not authoritatively reachable from STATS OR
/// `partition_count == 0`. So the value is only real when BOTH counts are
/// positive. Single-sourced here and reused by every consumer (recommendations
/// in this module and the report renderer in `statistics_reader`) to keep the
/// availability condition from drifting. No heuristic (#28); see #1352.
pub(crate) fn avg_rows_available(stats: &SSTableStatistics) -> bool {
    stats.row_stats.total_rows > 0 && stats.row_stats.partition_count > 0
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
            // `None` when not authoritatively available; see `live_data_percentage`.
            live_data_percentage: Self::live_data_percentage(stats),
            // `None` when compression stats were not authoritatively parsed
            // (issue #1653) rather than a fabricated `0% × 100`.
            compression_efficiency: stats.compression_stats.as_ref().map(|c| c.ratio * 100.0),
            timestamp_range_days: Self::calculate_timestamp_range_days(stats),
            // `None` when partition stats were not authoritatively parsed
            // (issue #1653) rather than a fabricated `0 MB`.
            largest_partition_mb: stats
                .partition_stats
                .as_ref()
                .map(|p| p.max_partition_size as f64 / 1_048_576.0),
            data_efficiency,
            query_performance_hints,
            storage_recommendations,
            health_score,
        }
    }

    /// Live-data percentage, or `None` when it is not authoritatively available.
    ///
    /// `live_rows == 0` is the documented #1325 sentinel meaning "not
    /// authoritatively derivable from STATS" (STATS has no per-SSTable live-row
    /// count), so we return `None` rather than a misleading concrete `0.00%`.
    /// A genuinely fully-tombstoned SSTable also reads as unavailable here —
    /// honest, since we cannot distinguish it from "unknown" until the
    /// `RowStatistics` representation is redesigned (#1352). No heuristic (#28).
    fn live_data_percentage(stats: &SSTableStatistics) -> Option<f64> {
        if stats.row_stats.live_rows == 0 || stats.row_stats.total_rows == 0 {
            return None;
        }
        Some((stats.row_stats.live_rows as f64 / stats.row_stats.total_rows as f64) * 100.0)
    }

    /// Overall data efficiency, or `None` when it is not authoritatively
    /// derivable.
    ///
    /// Data efficiency blends the live-row ratio with compression and partition
    /// efficiency, so it can only be computed when the live ratio is available.
    /// `live_rows == 0` is the documented #1325 sentinel meaning "not
    /// authoritatively available from STATS" (STATS has no per-SSTable live-row
    /// count), and `total_rows == 0` would make the ratio `NaN`; in either case
    /// we return `None` rather than a misleading concrete number. This reuses
    /// the exact availability check of `live_data_percentage`. No heuristic
    /// (#28); representation redesign is tracked by #1352.
    fn calculate_data_efficiency(stats: &SSTableStatistics) -> Option<f64> {
        if stats.row_stats.live_rows == 0 || stats.row_stats.total_rows == 0 {
            return None;
        }
        // Data efficiency blends the live ratio with compression and partition
        // efficiency. When either compression or partition statistics were NOT
        // authoritatively parsed (issue #1653 — the enhanced nb parser leaves them
        // `None`) the blend cannot be computed honestly, so return `None` rather
        // than substituting a fabricated default (no-heuristics mandate #28).
        let (Some(compression), Some(partition)) = (
            stats.compression_stats.as_ref(),
            stats.partition_stats.as_ref(),
        ) else {
            return None;
        };
        let live_ratio = stats.row_stats.live_rows as f64 / stats.row_stats.total_rows as f64;
        let compression_ratio = compression.ratio;
        let partition_efficiency = 1.0 - (partition.large_partition_percentage / 100.0);

        Some((live_ratio + compression_ratio + partition_efficiency) / 3.0 * 100.0)
    }

    fn generate_query_hints(stats: &SSTableStatistics) -> Vec<String> {
        let mut hints = Vec::new();

        // Partition/compression hints only fire when those statistics were
        // authoritatively parsed (issue #1653): a `None` (enhanced nb parser)
        // means "unavailable", so we emit no hint rather than one derived from a
        // fabricated default. No heuristic (#28).
        if let Some(partition) = stats.partition_stats.as_ref() {
            if partition.large_partition_percentage > 10.0 {
                hints.push("Consider reviewing partition key design - high percentage of large partitions detected".to_string());
            }
        }

        // The "high tombstone ratio" hint compares tombstones against the live
        // row count. When `live_rows == 0` that is the documented #1325 sentinel
        // for "not authoritatively available from STATS" (not a measured zero),
        // so `live_rows / 4 == 0` would make ANY tombstone trip the hint. Skip
        // the hint entirely when the live count is unavailable rather than emit
        // a misleading recommendation. No heuristic (#28); see #1352.
        if stats.row_stats.live_rows > 0
            && stats.row_stats.tombstone_count > stats.row_stats.live_rows / 4
        {
            hints.push("High tombstone ratio - consider running compaction".to_string());
        }

        if let Some(table) = stats.table_stats.as_ref() {
            if table.compression_ratio < 0.5 {
                hints.push("Low compression ratio - data may not be well-suited for current compression algorithm".to_string());
            }
        }

        hints
    }

    fn generate_storage_recommendations(stats: &SSTableStatistics) -> Vec<String> {
        let mut recommendations = Vec::new();

        // Only recommend on an authoritatively-parsed disk size (issue #1653): a
        // `None` `table_stats` (enhanced nb parser) is "unavailable", not a
        // measured tiny SSTable.
        if let Some(table) = stats.table_stats.as_ref() {
            if table.disk_size > 1_073_741_824 {
                recommendations
                    .push("Large SSTable detected - consider more frequent compaction".to_string());
            }
        }

        // `avg_rows_per_partition == 0.0` is the documented #1325 unavailable
        // sentinel: the parser leaves it 0.0 whenever `total_rows` is not
        // authoritatively reachable from STATS OR `partition_count == 0` (the
        // average is `total_rows / partition_count`). Only emit the granularity
        // recommendation when the average is a REAL derived value — i.e. BOTH
        // `total_rows > 0` AND `partition_count > 0` (so the average was actually
        // computed). Otherwise the sentinel `0.0 < 10.0` would always trip this
        // hint (e.g. on nb SSTables whose gated walk can't reach `totalRows`, or
        // when `partition_count == 0`), a misleading recommendation from a
        // non-value. No heuristic (#28); see #1352.
        if avg_rows_available(stats) && stats.row_stats.avg_rows_per_partition < 10.0 {
            recommendations.push(
                "Low average rows per partition - partition key may be too granular".to_string(),
            );
        }

        recommendations
    }

    fn calculate_health_score(stats: &SSTableStatistics) -> f64 {
        let mut score = 100.0;

        // Deduct for high tombstone ratio. This derives from `total_rows` (an
        // authoritative STATS count), NOT the #1325 `live_rows` unavailable
        // sentinel, so it stays a concrete score. Guard `total_rows == 0` so the
        // ratio does not become `NaN` (which would poison the whole score).
        if stats.row_stats.total_rows > 0 {
            let tombstone_ratio =
                stats.row_stats.tombstone_count as f64 / stats.row_stats.total_rows as f64;
            score -= tombstone_ratio * 30.0;
        }

        // Deduct for poor compression / large partitions ONLY when those
        // statistics were authoritatively parsed (issue #1653). A `None`
        // (enhanced nb parser) is "unavailable", so we do not deduct from a
        // fabricated default — the score reflects only what is known. No
        // heuristic (#28).
        if let Some(compression) = stats.compression_stats.as_ref() {
            if compression.ratio < 0.5 {
                score -= 20.0;
            }
        }

        if let Some(partition) = stats.partition_stats.as_ref() {
            score -= partition.large_partition_percentage;
        }

        score.max(0.0)
    }

    fn calculate_timestamp_range_days(stats: &SSTableStatistics) -> f64 {
        // Fail-closed (#1729/#1653): `max_timestamp` is `None` when the
        // authoritative maxTimestamp is unavailable. When the max is unavailable —
        // or is somehow below the min — we cannot compute a real range, so report
        // 0.0 rather than an underflowing/garbage span.
        let min = stats.timestamp_stats.min_timestamp;
        let max = match stats.timestamp_stats.max_timestamp {
            Some(max) if max >= min => max,
            _ => return 0.0,
        };
        // `max >= min` here, so the difference is non-negative and cannot
        // overflow i64 for realistic timestamps; use a checked subtraction to
        // stay defensive against pathological inputs.
        let range_micros = max.saturating_sub(min);
        range_micros as f64 / (1_000_000.0 * 60.0 * 60.0 * 24.0)
    }
}

/// Human-readable statistics summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsSummary {
    pub total_rows: u64,
    /// Percentage of live (non-tombstoned) data, or `None` when not
    /// authoritatively available (the #1325 `live_rows == 0` sentinel; see
    /// `StatisticsAnalyzer::live_data_percentage` and #1352).
    pub live_data_percentage: Option<f64>,
    /// Compression efficiency (ratio × 100), or `None` when compression
    /// statistics are not authoritatively available (issue #1653 — the enhanced
    /// nb parser does not decode them; see `SSTableStatistics::compression_stats`).
    pub compression_efficiency: Option<f64>,
    pub timestamp_range_days: f64,
    /// Largest partition size in MB, or `None` when partition statistics are not
    /// authoritatively available (issue #1653; see
    /// `SSTableStatistics::partition_stats`).
    pub largest_partition_mb: Option<f64>,
    /// Blended data-efficiency score, or `None` when the live-row ratio is not
    /// authoritatively available (the #1325 `live_rows == 0` sentinel, or
    /// `total_rows == 0`); see `StatisticsAnalyzer::calculate_data_efficiency`
    /// and #1352.
    pub data_efficiency: Option<f64>,
    pub query_performance_hints: Vec<String>,
    pub storage_recommendations: Vec<String>,
    pub health_score: f64,
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
        let live_pct = summary
            .live_data_percentage
            .expect("fixture has authoritative live_rows > 0");
        assert!((0.0..=100.0).contains(&live_pct));
    }

    /// #1325 roborev finding: when `live_rows` is the documented "not
    /// authoritatively available from STATS" sentinel (0) but `total_rows` is a
    /// real authoritative count, the analyzer MUST report live-data% as
    /// unavailable (`None`) rather than a misleading concrete `0.00%`. Other
    /// authoritative fields (`total_rows`) stay intact. See #1352.
    #[test]
    fn test_live_data_percentage_unavailable_when_live_rows_sentinel() {
        let mut stats = create_test_statistics();
        stats.row_stats.total_rows = 1000; // real authoritative count
        stats.row_stats.live_rows = 0; // documented "unavailable" sentinel

        let summary = StatisticsAnalyzer::analyze(&stats);

        assert_eq!(
            summary.live_data_percentage, None,
            "live_rows==0 sentinel with total_rows>0 must report None, not 0.00%"
        );
        // Data efficiency is derived from the live ratio, so it must be
        // unavailable under the same sentinel (not an artificially low number).
        assert_eq!(
            summary.data_efficiency, None,
            "data_efficiency must be None when live_rows is the unavailable sentinel"
        );
        // Authoritative counts are unaffected.
        assert_eq!(summary.total_rows, 1000);
        // Health score derives from total_rows (authoritative), not live_rows,
        // so it stays a concrete, finite value.
        assert!(
            summary.health_score.is_finite(),
            "health_score must stay finite when live_rows is the sentinel"
        );
    }

    /// #1325 sweep: the "high tombstone ratio" query hint compares tombstones
    /// against `live_rows`. When `live_rows == 0` (unavailable sentinel), the
    /// old `live_rows / 4 == 0` comparison would trip the hint for ANY
    /// tombstone. It must be suppressed instead. No heuristic (#28); see #1352.
    #[test]
    fn test_query_hint_suppressed_when_live_rows_sentinel() {
        let mut stats = create_test_statistics();
        stats.row_stats.total_rows = 1000;
        stats.row_stats.live_rows = 0; // unavailable sentinel
        stats.row_stats.tombstone_count = 500; // would trip vs. live_rows/4 == 0

        let summary = StatisticsAnalyzer::analyze(&stats);

        assert!(
            !summary
                .query_performance_hints
                .iter()
                .any(|h| h.contains("tombstone")),
            "tombstone hint must be suppressed when live_rows is the unavailable sentinel"
        );
    }

    /// #1325 sweep: when `live_rows` is a real count, the tombstone hint and
    /// data-efficiency stay concrete (non-vacuous positive path).
    #[test]
    fn test_derived_metrics_available_when_live_rows_present() {
        let mut stats = create_test_statistics();
        stats.row_stats.total_rows = 1000;
        stats.row_stats.live_rows = 100; // real count; live_rows/4 == 25
        stats.row_stats.tombstone_count = 500; // 500 > 25 -> hint fires

        let summary = StatisticsAnalyzer::analyze(&stats);

        assert!(
            summary.live_data_percentage.is_some(),
            "live_data_percentage must be Some when live_rows > 0"
        );
        let eff = summary
            .data_efficiency
            .expect("data_efficiency must be Some when live_rows > 0");
        assert!(eff.is_finite());
        assert!(
            summary
                .query_performance_hints
                .iter()
                .any(|h| h.contains("tombstone")),
            "tombstone hint must fire when tombstones exceed live_rows/4"
        );
    }

    /// #1325 sweep: health score must not become `NaN` when `total_rows == 0`
    /// (guard the div-by-zero in the tombstone-ratio deduction).
    #[test]
    fn test_health_score_finite_when_total_rows_zero() {
        let mut stats = create_test_statistics();
        stats.row_stats.total_rows = 0;
        stats.row_stats.live_rows = 0;
        stats.row_stats.tombstone_count = 0;

        let summary = StatisticsAnalyzer::analyze(&stats);

        assert!(
            summary.health_score.is_finite(),
            "health_score must be finite (not NaN) when total_rows == 0"
        );
        assert!((0.0..=100.0).contains(&summary.health_score));
    }

    /// #1325 roborev finding: the "partition key may be too granular" storage
    /// recommendation compares `avg_rows_per_partition < 10.0`. When
    /// `total_rows == 0` the parser leaves `avg_rows_per_partition == 0.0` as the
    /// documented unavailable sentinel, so `0.0 < 10.0` would ALWAYS trip the
    /// recommendation on nb SSTables whose gated walk cannot reach `totalRows`.
    /// It must be suppressed. No heuristic (#28); see #1352.
    #[test]
    fn test_granularity_recommendation_suppressed_when_avg_rows_sentinel() {
        let mut stats = create_test_statistics();
        // Unavailable sentinel: no authoritative total_rows -> avg left 0.0.
        stats.row_stats.total_rows = 0;
        stats.row_stats.avg_rows_per_partition = 0.0;
        // Keep disk_size small so the "large SSTable" recommendation does not fire.
        if let Some(t) = stats.table_stats.as_mut() {
            t.disk_size = 1024;
        }

        let summary = StatisticsAnalyzer::analyze(&stats);

        assert!(
            !summary
                .storage_recommendations
                .iter()
                .any(|r| r.contains("too granular")),
            "granularity recommendation must be suppressed when avg_rows_per_partition \
             is the unavailable sentinel (total_rows == 0)"
        );
    }

    /// #1325 sweep (non-vacuous positive path): with a REAL low average
    /// (total_rows > 0), the granularity recommendation still fires.
    #[test]
    fn test_granularity_recommendation_fires_when_avg_rows_real_and_low() {
        let mut stats = create_test_statistics();
        stats.row_stats.total_rows = 8; // real authoritative count
        stats.row_stats.partition_count = 4;
        stats.row_stats.avg_rows_per_partition = 2.0; // real, genuinely low (< 10)
        if let Some(t) = stats.table_stats.as_mut() {
            t.disk_size = 1024;
        }

        let summary = StatisticsAnalyzer::analyze(&stats);

        assert!(
            summary
                .storage_recommendations
                .iter()
                .any(|r| r.contains("too granular")),
            "granularity recommendation must fire for a REAL low avg_rows_per_partition"
        );
    }

    /// #1325 sweep (non-vacuous): with a real HIGH average the recommendation
    /// does NOT fire — proving the guard did not just always-suppress.
    #[test]
    fn test_granularity_recommendation_absent_when_avg_rows_real_and_high() {
        let mut stats = create_test_statistics();
        stats.row_stats.total_rows = 1000; // real; default avg is 20.0 (>= 10)
        if let Some(t) = stats.table_stats.as_mut() {
            t.disk_size = 1024;
        }

        let summary = StatisticsAnalyzer::analyze(&stats);

        assert!(
            !summary
                .storage_recommendations
                .iter()
                .any(|r| r.contains("too granular")),
            "granularity recommendation must NOT fire for a REAL high avg_rows_per_partition"
        );
    }

    /// #1374 roborev finding: `avg_rows_per_partition` is `total_rows /
    /// partition_count`, so the `0.0` unavailable sentinel also occurs when
    /// `total_rows > 0` but `partition_count == 0`. The availability guard must
    /// require BOTH counts positive; a stats object with real `total_rows` but
    /// zero partitions must still SUPPRESS the "too granular" recommendation.
    #[test]
    fn test_granularity_recommendation_suppressed_when_partition_count_zero() {
        let mut stats = create_test_statistics();
        stats.row_stats.total_rows = 8; // real authoritative count
        stats.row_stats.partition_count = 0; // but no partitions -> avg is sentinel
        stats.row_stats.avg_rows_per_partition = 0.0;
        if let Some(t) = stats.table_stats.as_mut() {
            t.disk_size = 1024;
        }

        // The shared availability helper must report unavailable.
        assert!(
            !avg_rows_available(&stats),
            "avg_rows must be unavailable when partition_count == 0 despite total_rows > 0"
        );

        let summary = StatisticsAnalyzer::analyze(&stats);

        assert!(
            !summary
                .storage_recommendations
                .iter()
                .any(|r| r.contains("too granular")),
            "granularity recommendation must be suppressed when partition_count == 0 \
             (avg_rows_per_partition is the unavailable sentinel)"
        );
    }

    #[test]
    fn test_parse_timestamp_statistics_no_ttl() {
        let mut data = Vec::new();
        data.extend_from_slice(&1000000i64.to_be_bytes()); // min_timestamp
        data.extend_from_slice(&2000000i64.to_be_bytes()); // max_timestamp
        data.extend_from_slice(&0i64.to_be_bytes()); // min_deletion_time
        data.extend_from_slice(&0i64.to_be_bytes()); // max_deletion_time
        data.push(0); // has_ttl = false

        let result = parse_timestamp_statistics(&data);
        assert!(result.is_ok());

        let (remaining, ts_stats) = result.unwrap();
        assert!(remaining.is_empty());
        assert_eq!(ts_stats.min_timestamp, 1000000);
        // Legacy parse genuinely reads these → `Some(..)` (issue #1653).
        assert_eq!(ts_stats.max_timestamp, Some(2000000));
        assert_eq!(ts_stats.min_deletion_time, 0);
        assert_eq!(ts_stats.max_deletion_time, 0);
        assert!(ts_stats.min_ttl.is_none());
        assert!(ts_stats.max_ttl.is_none());
        assert_eq!(ts_stats.rows_with_ttl, Some(0));
    }

    #[test]
    fn test_parse_timestamp_statistics_with_ttl() {
        use super::super::vint::encode_vint;

        let mut data = Vec::new();
        data.extend_from_slice(&1000000i64.to_be_bytes());
        data.extend_from_slice(&2000000i64.to_be_bytes());
        data.extend_from_slice(&0i64.to_be_bytes());
        data.extend_from_slice(&0i64.to_be_bytes());
        data.push(1); // has_ttl = true
        data.extend_from_slice(&3600i64.to_be_bytes()); // min_ttl
        data.extend_from_slice(&86400i64.to_be_bytes()); // max_ttl
        data.extend_from_slice(&encode_vint(250)); // rows_with_ttl

        let result = parse_timestamp_statistics(&data);
        assert!(result.is_ok());

        let (_, ts_stats) = result.unwrap();
        assert_eq!(ts_stats.min_ttl, Some(3600));
        assert_eq!(ts_stats.max_ttl, Some(86400));
        assert_eq!(ts_stats.rows_with_ttl, Some(250));
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
                max_timestamp: Some(2000000),
                min_deletion_time: 0,
                max_deletion_time: 0,
                min_ttl: None,
                max_ttl: None,
                rows_with_ttl: Some(0),
            },
            column_stats: vec![],
            table_stats: Some(TableStatistics {
                disk_size: 1024 * 1024,
                uncompressed_size: 2048 * 1024,
                compressed_size: 1024 * 1024,
                compression_ratio: 0.5,
                block_count: 100,
                avg_block_size: 1024.0,
                index_size: 1024,
                bloom_filter_size: 512,
                level_count: 1,
            }),
            partition_stats: Some(PartitionStatistics {
                avg_partition_size: 20480.0,
                min_partition_size: 1024,
                max_partition_size: 1048576,
                size_histogram: vec![],
                large_partition_percentage: 5.0,
            }),
            compression_stats: Some(CompressionStatistics {
                algorithm: "LZ4".to_string(),
                original_size: 2048 * 1024,
                compressed_size: 1024 * 1024,
                ratio: 0.5,
                compression_speed: 100.0,
                decompression_speed: 200.0,
                compressed_blocks: 100,
            }),
            metadata: HashMap::new(),
            serialization_header_columns: vec![],
            serialization_header_partition_keys: vec![],
            serialization_header_clustering_keys: vec![],
            tombstone_drop_times: vec![],
        }
    }
}
