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
//!
//! # Module layout (Issue #1125 split)
//!
//! - [`header`] — 32-byte file header + Table-of-Contents (component offsets).
//! - [`encoding_stats`] — EncodingStats (min timestamp/deletion-time/TTL) decode.
//! - [`serialization_header`] — table-schema (partition/clustering/static/regular columns).
//! - [`marshal_type`] — Cassandra marshal-type → CQL conversion + `ColumnInfo` builders.
//! - this `mod.rs` — the public entry points that orchestrate the above.

mod encoding_stats;
mod header;
mod marshal_type;
mod serialization_header;

pub use header::parse_nb_format_header;

use super::statistics::*;
use crate::error::{Error, Result};
use crate::storage::sstable::version_gate::VersionGates;
use encoding_stats::parse_minimal_encoding_stats;
use header::parse_statistics_toc_for_header_offset;

/// Type alias for EncodingStats parse result to reduce complexity
type EncodingStatsResult = (
    i64,
    i64,
    Option<i64>,
    Vec<super::header::ColumnInfo>,
    Vec<super::header::ColumnInfo>,
    Vec<super::header::ColumnInfo>,
);

/// Type alias for SerializationHeader parse result to reduce complexity
type SerializationHeaderResult = (Vec<String>, Vec<String>, Vec<super::header::ColumnInfo>);

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
#[allow(clippy::type_complexity)]
pub fn parse_nb_format_statistics_data(
    input: &[u8],
    header: &StatisticsHeader,
    full_input: &[u8],
    // VG3 plumbing: gates are threaded here so version-sensitive decisions in
    // parse_encoding_stats_vuints (e.g. has_uint_deletion_time) can be flipped
    // without re-deriving gates from the filename.
    // Pass `None` from callers that do not have gates (standalone tools, tests).
    gates: Option<&VersionGates>,
) -> Result<(
    RowStatistics,
    TimestampStatistics,
    Vec<super::header::ColumnInfo>,
    Vec<super::header::ColumnInfo>,
    Vec<super::header::ColumnInfo>,
)> {
    // Get HEADER offset from TOC (Issue #216)
    let header_offset = parse_statistics_toc_for_header_offset(full_input);

    // Parse the EncodingStats section from the data following the header
    let result = parse_minimal_encoding_stats(input, full_input, header_offset, gates);

    match result {
        Ok((
            _,
            (
                min_timestamp,
                min_deletion_time,
                min_ttl,
                partition_columns,
                clustering_columns,
                regular_columns,
            ),
        )) => {
            // Populate the authoritative row/partition counts from the STATS
            // component (issue #1325). We reuse the single source of truth added
            // in #944 — `repair_metadata::read_table_counts` — rather than
            // re-deriving the walk here:
            //   * `partition_count` = Σ `estimatedPartitionSize` histogram bucket
            //     counts (self-describing; always decodable).
            //   * `total_rows`      = STATS `totalRows` via the version-gated walk,
            //     `None` when not authoritatively traversable (e.g. a clustered
            //     covered-Slice whose bound values are not modeled).
            // No-heuristics mandate (#28): when a value is NOT authoritatively
            // reachable we leave the documented 0 rather than fabricating a count.
            //   * `total_rows`: `None` from `read_table_counts` → 0 (meaning
            //     "not authoritatively reachable", never a guessed count).
            //   * `live_rows`: STATS carries no per-SSTable live-row count that is
            //     authoritatively distinguishable from `total_rows` here, so it is
            //     left as 0 (documented "not authoritatively derivable") rather
            //     than guessed. (Consumers that need an upper bound use
            //     `total_rows`; see cqlite-flight `read_table_counts` doctrine.)
            //   * `avg_rows_per_partition`: computed only when BOTH counts are
            //     authoritative and `partition_count > 0`; otherwise left 0.0.
            // `full_input` is the raw Statistics.db buffer (with TOC), exactly the
            // input `read_table_counts` expects. This is strictly additive — an
            // Err is logged and the fields stay at 0 (a Statistics.db that parses
            // today must keep parsing).
            //
            // No-heuristics gate discipline (#28, #1325 roborev finding): the
            // version-gated `read_table_counts` walk MUST run with gates that match
            // the file's actual on-disk layout — Statistics.db does NOT self-describe
            // its SSTable version (the version comes from the filename → the gates).
            // The `nb` and `oa`/`da` STATS min/max blocks have DIFFERENT layouts, so
            // synthesizing `nb` gates for a file whose format we have not
            // authoritatively established would mis-walk an `oa`/`da` buffer and could
            // read a BOGUS nonzero `totalRows`. This function is the SINGLE entry point
            // for `parse_statistics_with_fallback`, which is a public API reachable with
            // `None` gates for ANY format (standalone tools, tests) — there is no
            // format-detection dispatch that guarantees only `nb` bytes arrive here.
            //
            // Therefore we pass the caller's gates through UNCHANGED and NEVER
            // synthesize a version:
            //   * `Some(gates)` — the caller (e.g. `StatisticsReader::open`) derived
            //     these from the filename, so they are authoritative for the file's
            //     real version (nb / oa / da). The gated walk reaches `totalRows`.
            //   * `None` — the format is UNKNOWN. We cannot authoritatively establish
            //     it, so `read_table_counts` stops before the gated min/max block and
            //     reports `total_rows = None`. We surface that as `0` meaning "not
            //     authoritatively available", NEVER a guessed count. This is strictly
            //     safer than the old `nb` synthesis, which could fabricate a nonzero
            //     count for an oa/da file.
            let (total_rows, partition_count) =
                match crate::parser::repair_metadata::read_table_counts(full_input, gates) {
                    // `total_rows.unwrap_or(0)`: a `None` here means STATS does NOT
                    // authoritatively expose `totalRows` (e.g. an unmodeled
                    // improvedMinMax covered-Slice bound blocks the walk). The 0 is
                    // therefore "not authoritatively available from STATS", NOT a
                    // measured zero — never a guessed count (#1325, no-heuristics #28).
                    Ok(counts) => (counts.total_rows.unwrap_or(0), counts.partition_count),
                    Err(e) => {
                        tracing::debug!(
                            "Best-effort authoritative row/partition-count decode failed; \
                             leaving RowStatistics counts at 0: {:?}",
                            e
                        );
                        (0, 0)
                    }
                };
            let avg_rows_per_partition = if partition_count > 0 && total_rows > 0 {
                total_rows as f64 / partition_count as f64
            } else {
                0.0
            };

            let row_stats = RowStatistics {
                total_rows,
                // `live_rows = 0` here means "NOT authoritatively available from
                // STATS for nb", NOT a measured zero. STATS carries no per-SSTable
                // live-row count that is authoritatively distinguishable from
                // `total_rows`, so per #1325's acceptance criteria this unreachable
                // field stays "0-with-documented-meaning" rather than being guessed
                // or aliased to `total_rows` (no-heuristics mandate #28). Changing
                // this field to `Option` is a broader API change tracked separately.
                live_rows: 0,
                tombstone_count: 0,
                partition_count,
                avg_rows_per_partition,
                row_size_histogram: vec![],
            };

            let timestamp_stats = TimestampStatistics {
                min_timestamp,
                // `None` = maxTimestamp NOT yet authoritatively available (issue
                // #1653). The real `maxTimestamp` is recovered from STATS field 4
                // by the best-effort post-pass in `parse_enhanced_statistics_file`
                // (issue #1729), which sets `Some(..)`. The inner EncodingStats
                // parser does not reach field 4, so it stays `None` here — an
                // honest "unavailable", NOT the old `i64::MIN` sentinel or the even
                // older `min_timestamp` alias (both lied about the max). A consumer
                // that needs a real max must treat `None` as unavailable.
                max_timestamp: None,
                min_deletion_time,
                // Genuinely parsed by the STATS post-pass (#1073/#1011), which
                // overwrites this below. Until then use the authoritative
                // `NO_DELETION_TIME` (`i64::MAX`) "no deletions recorded" value so a
                // post-pass failure fails CLOSED (never classified fully-expired),
                // NOT the old `= min_deletion_time` placeholder which reported the
                // MIN as the max (issue #1653).
                max_deletion_time: i64::MAX,
                min_ttl,
                // `None` = maxTTL not authoritatively available (issue #1653). The
                // enhanced parser does not decode a per-SSTable max TTL, so it is
                // honestly `None`, NOT the old `= min_ttl` alias (which lied).
                max_ttl: None,
                // `None` = rows-with-TTL count not authoritatively available (issue
                // #1653), NOT a fabricated `0` (which claimed "no rows have a TTL").
                rows_with_ttl: None,
            };

            // Table/partition/compression statistics are NOT decoded by the
            // enhanced (nb) parser. They are left `None` on `SSTableStatistics`
            // (built by the caller) rather than fabricated with all-zero /
            // "unknown" placeholders that were type-indistinguishable from real
            // values (issue #1653; no-heuristics/no-fabrication mandate #28).
            Ok((
                row_stats,
                timestamp_stats,
                partition_columns,
                clustering_columns,
                regular_columns,
            ))
        }
        Err(e) => {
            tracing::debug!(
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

/// Main enhanced parser for real Statistics.db files (minimal implementation for Issue #162)
///
/// This function parses the header and minimal EncodingStats fields from nb-format
/// Statistics.db files. Only timestamp-related fields are extracted; all other
/// statistics (histograms, column stats, etc.) are populated with placeholder values.
///
/// This is sufficient for V5CompressedLegacy parser which requires min_timestamp,
/// min_local_deletion_time, and min_ttl for delta decoding baseline.
///
/// # Arguments
///
/// * `gates` - Optional [`VersionGates`] for version-sensitive decoding decisions
///   (VG1 plumbing).  Pass `None` from standalone tools/tests to use nb-compatible
///   defaults; pass `Some(&gates)` from `SSTableReader` to enable VG3 gating.
///
/// # Returns
///
/// SSTableStatistics with only header and timestamp_stats populated from real data.
pub fn parse_enhanced_statistics_file<'a>(
    input: &'a [u8],
    gates: Option<&VersionGates>,
) -> nom::IResult<&'a [u8], SSTableStatistics> {
    // Parse the 32-byte header
    let (remaining, header) = parse_nb_format_header(input)?;

    // Parse minimal statistics data (EncodingStats + SerializationHeader columns)
    // Pass full input for TOC-based HEADER offset lookup (Issue #216)
    let result = parse_nb_format_statistics_data(remaining, &header, input, gates);

    match result {
        Ok((row_stats, mut timestamp_stats, partition_columns, clustering_columns, columns)) => {
            tracing::debug!(
                "Successfully parsed Statistics.db serialization header: {} partition keys, {} clustering keys, {} regular columns",
                partition_columns.len(),
                clustering_columns.len(),
                columns.len()
            );

            // Best-effort STATS-extras decode over the FULL buffer (with TOC):
            // recover the authoritative maxLocalDeletionTime (the inner parser
            // leaves it as a placeholder equal to min_deletion_time), the
            // authoritative maxTimestamp (issue #1729 — the inner parser leaves
            // it as the `i64::MIN` "unavailable" sentinel), and the estimated
            // tombstone-drop-times histogram (Issue #1073). This is strictly
            // additive — a Statistics.db that parses today must keep parsing, so
            // an Err here is logged and the placeholders are kept. When
            // maxTimestamp is unavailable (extras.max_timestamp == None, i.e.
            // Cassandra's Long.MIN_VALUE sentinel) we deliberately leave
            // `max_timestamp == None` in place (issue #1653) rather than
            // substituting the min — consumers requiring a real max must not
            // proceed.
            let tombstone_drop_times =
                match crate::parser::repair_metadata::parse_stats_extras(input, gates) {
                    Ok(extras) => {
                        // Only set max_deletion_time. Do NOT touch min_deletion_time
                        // (the EncodingStats min baseline consumed elsewhere).
                        timestamp_stats.max_deletion_time = extras.max_local_deletion_time;
                        // `extras.max_timestamp` is already `Option`: `Some` only
                        // when the authoritative max was decoded (#1729). Propagate
                        // it directly — `None` stays `None` (issue #1653).
                        if let Some(max_ts) = extras.max_timestamp {
                            timestamp_stats.max_timestamp = Some(max_ts);
                        }
                        // Authoritative maxTTL (issue #1537): the inner nb walk only
                        // decodes the EncodingStats minTTL baseline and honestly
                        // leaves `max_ttl = None`. Recover the true maximum from the
                        // STATS-extras walk so the compaction TTL-expiry LDT floor
                        // (`compute_expiry_ttl_ldt_floor`) can lower the output's
                        // min-LDT delta baseline below a creation-time (`ldt - ttl`)
                        // tombstone. `None` (no expiring cells) stays `None`.
                        if extras.max_ttl.is_some() {
                            timestamp_stats.max_ttl = extras.max_ttl;
                        }
                        extras.tombstone_drop_times
                    }
                    Err(e) => {
                        tracing::debug!(
                            "Best-effort STATS-extras decode failed; keeping placeholders: {:?}",
                            e
                        );
                        vec![]
                    }
                };

            let statistics = SSTableStatistics {
                header,
                row_stats,
                timestamp_stats,
                column_stats: vec![],
                // NOT decoded by the enhanced (nb) parser → honestly `None`
                // rather than fabricated all-zero / "unknown" placeholders
                // (issue #1653; no-heuristics mandate #28).
                table_stats: None,
                partition_stats: None,
                compression_stats: None,
                metadata: std::collections::HashMap::new(),
                serialization_header_columns: columns,
                serialization_header_partition_keys: partition_columns,
                serialization_header_clustering_keys: clustering_columns,
                tombstone_drop_times,
            };

            Ok((remaining, statistics))
        }
        Err(e) => {
            // Convert Error to nom::Err
            tracing::warn!("Failed to parse nb-format Statistics.db: {}", e);
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
/// # Arguments
///
/// * `gates` - Optional [`VersionGates`] threaded from `SSTableReader` for VG3
///   version-sensitive decoding.  Pass `None` from standalone tools/tests; the
///   nb-compatible behaviour is used when `gates` is `None`.
///
/// # Returns
///
/// SSTableStatistics with minimal fields populated, or error if parsing fails.
pub fn parse_statistics_with_fallback<'a>(
    input: &'a [u8],
    gates: Option<&VersionGates>,
) -> nom::IResult<&'a [u8], SSTableStatistics> {
    // Try the minimal enhanced parser
    parse_enhanced_statistics_file(input, gates)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let result = parse_nb_format_statistics_data(&dummy_data, &header, &dummy_data, None);

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

        let result = parse_enhanced_statistics_file(&test_data, None);

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

        let result = parse_statistics_with_fallback(&test_data, None);

        // Should fail - incomplete data
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_data_returns_error() {
        // Test with insufficient data
        let invalid_data = vec![0xFF; 10];
        let result = parse_statistics_with_fallback(&invalid_data, None);
        assert!(result.is_err(), "Invalid data should fail to parse");
    }

    /// Locate a real nb `Statistics.db` fixture under `CQLITE_DATASETS_ROOT`.
    /// Returns `None` (test skips) when the dataset binaries are absent.
    #[cfg(test)]
    fn real_nb_statistics_db() -> Option<Vec<u8>> {
        let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
        let path = std::path::PathBuf::from(root).join(
            "sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/\
             nb-1-big-Statistics.db",
        );
        if !path.exists() {
            eprintln!("dataset Statistics.db absent, skipping: {path:?}");
            return None;
        }
        std::fs::read(&path).ok()
    }

    /// Issue #1653 (a): when the parser does NOT authoritatively reach a field,
    /// the fabricated placeholder must be gone — the value is honestly `None`, NOT
    /// the old `max_timestamp == min_timestamp` / `i64::MIN` sentinel.
    ///
    /// The INNER parser (`parse_nb_format_statistics_data`) decodes only the
    /// EncodingStats block and never reaches STATS field 4 (max timestamp) or a
    /// per-SSTable max-TTL / rows-with-TTL count, so those stay `None`. This is
    /// exactly the "parser doesn't populate it" case the issue's TDD test targets
    /// (which FAILED on main, where `max_timestamp` aliased the min).
    #[test]
    fn inner_parser_leaves_unavailable_timestamp_fields_none() {
        let Some(bytes) = real_nb_statistics_db() else {
            return;
        };
        let (remaining, header) =
            parse_nb_format_header(&bytes).expect("nb header must parse on a real fixture");
        let (_row, ts_stats, _pk, _ck, _cols) =
            parse_nb_format_statistics_data(remaining, &header, &bytes, None)
                .expect("inner EncodingStats parse must succeed on a real fixture");

        // A real time-series SSTable records write timestamps, so its min is a
        // real (non-sentinel) value — proving the fixture is meaningful.
        assert_ne!(
            ts_stats.min_timestamp,
            i64::MIN,
            "fixture must carry a real min_timestamp"
        );
        // The headline (#1653): the inner parser does not reach the authoritative
        // max, so it is honestly `None` — NOT a fabricated `Some(min)`/sentinel.
        assert_eq!(
            ts_stats.max_timestamp, None,
            "inner parser must leave max_timestamp None (not fabricated as min); issue #1653"
        );
        assert_eq!(
            ts_stats.max_ttl, None,
            "inner parser must leave max_ttl None (not fabricated as min_ttl); issue #1653"
        );
        assert_eq!(
            ts_stats.rows_with_ttl, None,
            "inner parser must leave rows_with_ttl None (not fabricated as 0); issue #1653"
        );
    }

    /// Issue #1653 (a, cont.): the full enhanced parse must NOT fabricate the
    /// wholly-unparsed table/partition/compression statistics — they are honestly
    /// `None`, never an all-zero / `algorithm: "unknown"` block.
    #[test]
    fn enhanced_parse_leaves_unparsed_stat_blocks_none() {
        let Some(bytes) = real_nb_statistics_db() else {
            return;
        };
        let (_remaining, stats) = parse_enhanced_statistics_file(&bytes, None)
            .expect("enhanced parse must succeed on a real fixture");

        assert!(
            stats.table_stats.is_none(),
            "enhanced nb parser must not fabricate table_stats; issue #1653"
        );
        assert!(
            stats.partition_stats.is_none(),
            "enhanced nb parser must not fabricate partition_stats; issue #1653"
        );
        assert!(
            stats.compression_stats.is_none(),
            "enhanced nb parser must not fabricate compression_stats; issue #1653"
        );
    }

    /// Issue #1653 (b): a genuinely-present statistic still yields
    /// `Some(real_value)`. The best-effort STATS post-pass in
    /// `parse_enhanced_statistics_file` recovers the authoritative `maxTimestamp`
    /// (#1729) for a real time-series SSTable, so `max_timestamp` is `Some` and is
    /// a real max `>= min` — proving the change did not break genuine decoding.
    #[test]
    fn enhanced_parse_recovers_real_max_timestamp_as_some() {
        let Some(bytes) = real_nb_statistics_db() else {
            return;
        };
        let (_remaining, stats) = parse_enhanced_statistics_file(&bytes, None)
            .expect("enhanced parse must succeed on a real fixture");

        let min = stats.timestamp_stats.min_timestamp;
        let max = stats
            .timestamp_stats
            .max_timestamp
            .expect("a real time-series nb SSTable must expose an authoritative maxTimestamp");
        assert!(
            max >= min,
            "recovered max_timestamp ({max}) must be a real max >= min ({min})"
        );
    }
}
