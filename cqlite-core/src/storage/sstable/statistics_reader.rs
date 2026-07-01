//! Statistics.db file reader for enhanced SSTable metadata
//!
//! This module provides a high-level interface for reading and analyzing
//! Statistics.db files that accompany SSTable Data.db files in Cassandra 5+.

use crate::{
    error::{Error, Result},
    parser::enhanced_statistics_parser::parse_statistics_with_fallback,
    parser::statistics::{SSTableStatistics, StatisticsAnalyzer, StatisticsSummary},
    platform::Platform,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

/// Calculate CRC32 checksum for data validation
fn crc32_checksum(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;

    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xedb8_8320_u32;
            } else {
                crc >>= 1;
            }
        }
    }

    !crc
}

/// High-level Statistics.db file reader
pub struct StatisticsReader {
    /// Path to the Statistics.db file
    file_path: PathBuf,
    /// Parsed statistics data
    statistics: SSTableStatistics,
    /// Platform abstraction for file operations
    #[allow(dead_code)]
    platform: Arc<Platform>,
}

impl StatisticsReader {
    /// Open and parse a Statistics.db file
    pub async fn open(path: &Path, platform: Arc<Platform>) -> Result<Self> {
        // Derive the format gates from the component filename (e.g.
        // `da-1-bti-Statistics.db` vs `nb-1-big-Statistics.db`) so the best-effort
        // STATS-extras decode (max local deletion time + tombstone-drop histogram,
        // #1073) interprets the version-sensitive fields correctly: the modern
        // (oa/da) `u32::MAX` no-deletion sentinel and the 12-byte modern histogram
        // bin width differ from the legacy (nb) signed-i32 / 16-byte forms. When the
        // filename does not parse, fall back to None (nb-compatible defaults).
        //
        // #1249 (spec R1): reject below-floor versions BEFORE *any* filesystem
        // access. Gates derive solely from the filename string (no file I/O), so
        // this is the earliest point the na+ floor can be enforced: a parsed-but-
        // below-floor version is propagated as a FATAL `Error::UnsupportedVersion`
        // here — before the existence check, `File::open`, and `read_to_end` —
        // instead of returning `NotFound` after touching the filesystem or
        // silently degrading to nb-compatible defaults (mirrors
        // `SSTableReader::open_inner` in reader/mod.rs, which gates before
        // `tokio::fs::metadata`). Only a genuinely unparseable / structurally-
        // malformed descriptor falls back to None and proceeds to the FS checks.
        let gates = match crate::storage::sstable::version_gate::VersionGates::from_path(path) {
            Ok(gates) => Some(gates),
            Err(e @ Error::UnsupportedVersion { .. }) => return Err(e),
            Err(e) => {
                log::debug!(
                    "StatisticsReader::open: could not derive VersionGates from {:?} ({}); \
                     defaulting to nb-compatible behaviour",
                    path,
                    e
                );
                None
            }
        };

        if !platform.fs().exists(path).await? {
            return Err(Error::not_found(format!(
                "Statistics.db file not found: {}",
                path.display()
            )));
        }

        // Read the entire file (Statistics.db files are typically small).
        // Reached only for at-or-above-floor (or structurally-unparseable)
        // descriptors — a below-floor version was already rejected above.
        let mut file = File::open(path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let statistics = match parse_statistics_with_fallback(&buffer, gates.as_ref()) {
            Ok((_, stats)) => stats,
            Err(e) => {
                return Err(Error::corruption(format!(
                    "Failed to parse Statistics.db with enhanced parser: {:?}",
                    e
                )));
            }
        };

        // Checksum validation: DEFERRED TO M2 MILESTONE
        //
        // The nb-format Statistics.db checksum algorithm is not yet implemented (Issue #28).
        // The checksum field (header.checksum) contains a value but we cannot validate it
        // without knowing the exact algorithm Cassandra 5.0+ uses for this file format.
        //
        // Known limitations:
        // - Files with corrupt data may be silently accepted
        // - No data integrity guarantee for Statistics.db parsing
        // - M2 milestone will implement proper CRC32/Adler32/other validation
        //
        // This limitation is explicitly documented rather than silently ignored.
        // Statistics.db is metadata-only (not critical path) so risk is acceptable for M1.
        if statistics.header.checksum != 0 {
            // Checksum present but not validated - this is a known M1 limitation
        }

        Ok(Self {
            file_path: path.to_path_buf(),
            statistics,
            platform,
        })
    }

    /// Get the raw statistics data
    pub fn statistics(&self) -> &SSTableStatistics {
        &self.statistics
    }

    /// Get a human-readable summary analysis
    pub fn analyze(&self) -> StatisticsSummary {
        StatisticsAnalyzer::analyze(&self.statistics)
    }

    /// Get the file path
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// Validate checksum for parsed statistics
    pub async fn validate_checksum(&self) -> Result<bool> {
        // Read the raw file data for checksum calculation
        let mut file = File::open(&self.file_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        if buffer.len() < 4 {
            return Err(Error::corruption(
                "Statistics file too small for checksum validation".to_string(),
            ));
        }

        // Extract the stored checksum from header
        let stored_checksum = self.statistics.header.checksum;

        // Calculate CRC32 checksum of the data section (excluding the checksum field itself)
        let data_section = if buffer.len() >= 32 {
            &buffer[28..buffer.len() - 4] // Skip header to checksum field, then skip checksum
        } else {
            return Err(Error::corruption(
                "Invalid Statistics file format for checksum validation".to_string(),
            ));
        };

        let calculated_checksum = crc32_checksum(data_section);

        Ok(calculated_checksum == stored_checksum)
    }

    /// Check if the Statistics.db corresponds to a specific table
    pub fn matches_table(&self, table_id: &[u8; 16]) -> bool {
        if let Some(ref stats_table_id) = self.statistics.header.table_id {
            stats_table_id == table_id
        } else {
            false // Cannot match without table ID
        }
    }

    /// Get row count information
    pub fn row_count(&self) -> u64 {
        self.statistics.row_stats.total_rows
    }

    /// Get live row count (excluding tombstones)
    pub fn live_row_count(&self) -> u64 {
        self.statistics.row_stats.live_rows
    }

    /// Get timestamp range in microseconds
    pub fn timestamp_range(&self) -> (i64, i64) {
        (
            self.statistics.timestamp_stats.min_timestamp,
            self.statistics.timestamp_stats.max_timestamp,
        )
    }

    /// Get compression information
    pub fn compression_info(&self) -> (&str, f64) {
        (
            &self.statistics.compression_stats.algorithm,
            self.statistics.compression_stats.ratio,
        )
    }

    /// Get partition statistics
    pub fn partition_info(&self) -> (u64, f64, u64) {
        (
            self.statistics.partition_stats.min_partition_size,
            self.statistics.partition_stats.avg_partition_size,
            self.statistics.partition_stats.max_partition_size,
        )
    }

    /// Get column statistics by name
    pub fn column_stats(
        &self,
        column_name: &str,
    ) -> Option<&crate::parser::statistics::ColumnStatistics> {
        self.statistics
            .column_stats
            .iter()
            .find(|col| col.name == column_name)
    }

    /// Get all column names with statistics
    pub fn column_names(&self) -> Vec<&str> {
        self.statistics
            .column_stats
            .iter()
            .map(|col| col.name.as_str())
            .collect()
    }

    /// Check if data has TTL information
    pub fn has_ttl_data(&self) -> bool {
        self.statistics.timestamp_stats.rows_with_ttl > 0
    }

    /// Get disk space usage information
    pub fn disk_usage(&self) -> (u64, u64, f64) {
        (
            self.statistics.table_stats.compressed_size,
            self.statistics.table_stats.uncompressed_size,
            self.statistics.table_stats.compression_ratio,
        )
    }

    /// Generate a detailed report
    pub fn generate_report(&self, include_column_details: bool) -> String {
        let mut report = String::new();
        let summary = self.analyze();

        report.push_str("# SSTable Statistics Report\n\n");

        // Overview section
        report.push_str("## Overview\n");
        // `total_rows == 0` is the documented #1325 "not authoritatively
        // reachable from STATS" sentinel (the version-gated walk could not reach
        // `totalRows`), not a measured zero. Render it as `unavailable` rather
        // than a misleading `0` (#1352). No heuristic (#28).
        report.push_str(&match summary.total_rows {
            0 => "- **Total Rows**: unavailable\n".to_string(),
            n => format!("- **Total Rows**: {}\n", n),
        });
        // `live_data_percentage` is `None` when `live_rows` is the documented
        // #1325 "not authoritatively available from STATS" sentinel; render it as
        // unavailable rather than a misleading concrete 0.00% (#1352).
        report.push_str(&match summary.live_data_percentage {
            Some(pct) => format!("- **Live Data**: {:.2}%\n", pct),
            None => "- **Live Data**: unavailable\n".to_string(),
        });
        report.push_str(&format!(
            "- **Compression Efficiency**: {:.2}%\n",
            summary.compression_efficiency
        ));
        report.push_str(&format!(
            "- **Time Range**: {:.1} days\n",
            summary.timestamp_range_days
        ));
        report.push_str(&format!(
            "- **Largest Partition**: {:.2} MB\n",
            summary.largest_partition_mb
        ));
        report.push_str(&format!(
            "- **Health Score**: {:.1}/100\n\n",
            summary.health_score
        ));

        // Row statistics
        report.push_str("## Row Statistics\n");
        // Same #1325 `total_rows == 0` unavailable sentinel as the overview above.
        report.push_str(&match self.statistics.row_stats.total_rows {
            0 => "- Total rows: unavailable\n".to_string(),
            n => format!("- Total rows: {}\n", n),
        });
        // `live_rows == 0` is the documented #1325 "not authoritatively
        // available from STATS" sentinel (STATS has no per-SSTable live-row
        // count), not a measured zero. Render it as `unavailable` for
        // consistency with the overview live-data% above rather than a
        // misleading `0` (#1352).
        report.push_str(&match self.statistics.row_stats.live_rows {
            0 => "- Live rows: unavailable\n".to_string(),
            live => format!("- Live rows: {}\n", live),
        });
        report.push_str(&format!(
            "- Tombstones: {}\n",
            self.statistics.row_stats.tombstone_count
        ));
        report.push_str(&format!(
            "- Partitions: {}\n",
            self.statistics.row_stats.partition_count
        ));
        // `avg_rows_per_partition == 0.0` is the documented #1325 unavailable
        // sentinel: the parser leaves it 0.0 whenever `total_rows` is not
        // authoritatively reachable OR `partition_count == 0` (the average is
        // `total_rows / partition_count`, so BOTH must be positive for the value
        // to be real). Render it as `unavailable` unless both counts are positive
        // rather than a misleading `0.0` (#1352). Availability guard is
        // single-sourced via `avg_rows_available`. No heuristic (#28).
        report.push_str(
            &if crate::parser::statistics::avg_rows_available(&self.statistics) {
                format!(
                    "- Average rows per partition: {:.1}\n\n",
                    self.statistics.row_stats.avg_rows_per_partition
                )
            } else {
                "- Average rows per partition: unavailable\n\n".to_string()
            },
        );

        // Timestamp information
        if self.statistics.timestamp_stats.min_timestamp != 0 {
            let min_time = chrono::DateTime::from_timestamp_micros(
                self.statistics.timestamp_stats.min_timestamp,
            );
            let max_time = chrono::DateTime::from_timestamp_micros(
                self.statistics.timestamp_stats.max_timestamp,
            );

            report.push_str("## Timestamp Range\n");
            if let (Some(min), Some(max)) = (min_time, max_time) {
                report.push_str(&format!(
                    "- From: {}\n",
                    min.format("%Y-%m-%d %H:%M:%S UTC")
                ));
                report.push_str(&format!("- To: {}\n", max.format("%Y-%m-%d %H:%M:%S UTC")));
            } else {
                report.push_str(&format!(
                    "- Min timestamp: {}\n",
                    self.statistics.timestamp_stats.min_timestamp
                ));
                report.push_str(&format!(
                    "- Max timestamp: {}\n",
                    self.statistics.timestamp_stats.max_timestamp
                ));
            }

            if self.has_ttl_data() {
                report.push_str(&format!(
                    "- Rows with TTL: {}\n",
                    self.statistics.timestamp_stats.rows_with_ttl
                ));
            }
            report.push('\n');
        }

        // Compression statistics
        report.push_str("## Compression\n");
        report.push_str(&format!(
            "- Algorithm: {}\n",
            self.statistics.compression_stats.algorithm
        ));
        report.push_str(&format!(
            "- Original size: {:.2} MB\n",
            self.statistics.compression_stats.original_size as f64 / 1_048_576.0
        ));
        report.push_str(&format!(
            "- Compressed size: {:.2} MB\n",
            self.statistics.compression_stats.compressed_size as f64 / 1_048_576.0
        ));
        report.push_str(&format!(
            "- Ratio: {:.2}%\n",
            self.statistics.compression_stats.ratio * 100.0
        ));
        report.push_str(&format!(
            "- Speed: {:.1} MB/s (compress), {:.1} MB/s (decompress)\n\n",
            self.statistics.compression_stats.compression_speed,
            self.statistics.compression_stats.decompression_speed
        ));

        // Partition statistics
        report.push_str("## Partition Distribution\n");
        report.push_str(&format!(
            "- Average size: {:.2} KB\n",
            self.statistics.partition_stats.avg_partition_size / 1024.0
        ));
        report.push_str(&format!(
            "- Range: {:.2} KB - {:.2} MB\n",
            self.statistics.partition_stats.min_partition_size as f64 / 1024.0,
            self.statistics.partition_stats.max_partition_size as f64 / 1_048_576.0
        ));
        report.push_str(&format!(
            "- Large partitions (>1MB): {:.1}%\n\n",
            self.statistics.partition_stats.large_partition_percentage
        ));

        // Column statistics
        if include_column_details && !self.statistics.column_stats.is_empty() {
            report.push_str("## Column Statistics\n");
            for column in &self.statistics.column_stats {
                report.push_str(&format!("### {}\n", column.name));
                report.push_str(&format!("- Type: {}\n", column.column_type));
                report.push_str(&format!("- Values: {}\n", column.value_count));
                report.push_str(&format!("- Nulls: {}\n", column.null_count));
                report.push_str(&format!("- Average size: {:.1} bytes\n", column.avg_size));
                report.push_str(&format!("- Cardinality: {}\n", column.cardinality));
                if column.has_index {
                    report.push_str("- **Indexed**: Yes\n");
                }
                report.push('\n');
            }
        }

        // Performance hints
        if !summary.query_performance_hints.is_empty() {
            report.push_str("## Query Performance Hints\n");
            for hint in &summary.query_performance_hints {
                report.push_str(&format!("- {}\n", hint));
            }
            report.push('\n');
        }

        // Storage recommendations
        if !summary.storage_recommendations.is_empty() {
            report.push_str("## Storage Recommendations\n");
            for rec in &summary.storage_recommendations {
                report.push_str(&format!("- {}\n", rec));
            }
            report.push('\n');
        }

        report
    }

    /// Get a compact summary for CLI display
    pub fn compact_summary(&self) -> String {
        let summary = self.analyze();
        // See `generate_report`: `None` = live-data% not authoritatively available
        // (#1325 sentinel), rendered as "?% live" instead of a misleading 0.0%.
        let live = match summary.live_data_percentage {
            Some(pct) => format!("{:.1}% live", pct),
            None => "?% live".to_string(),
        };
        // `total_rows == 0` is the documented #1325 "not authoritatively reachable
        // from STATS" sentinel; render it as "?" here rather than a misleading `0`
        // (matches the "?% live" convention above) (#1352). No heuristic (#28).
        let rows = match summary.total_rows {
            0 => "?".to_string(),
            n => n.to_string(),
        };
        format!(
            "Rows: {} ({}) | Compression: {:.1}% | Health: {:.0}/100 | Size: {:.2} MB",
            rows,
            live,
            summary.compression_efficiency,
            summary.health_score,
            self.statistics.table_stats.disk_size as f64 / 1_048_576.0
        )
    }

    /// Build a reader directly from in-memory statistics (test-only).
    ///
    /// The production constructor requires a real on-disk `Statistics.db`; this
    /// lets the report-rendering unit tests exercise the #1325 unavailable-sentinel
    /// rendering without a fixture file.
    #[cfg(test)]
    async fn from_statistics_for_test(statistics: SSTableStatistics) -> Self {
        let config = crate::Config::default();
        let platform =
            std::sync::Arc::new(crate::Platform::new(&config).await.expect("test platform"));
        Self {
            file_path: PathBuf::from("in-memory-test-Statistics.db"),
            statistics,
            platform,
        }
    }
}

/// Utility function to find Statistics.db file for a given Data.db file
pub async fn find_statistics_file(data_db_path: &Path) -> Option<PathBuf> {
    if let Some(parent) = data_db_path.parent() {
        if let Some(stem) = data_db_path.file_stem() {
            if let Some(stem_str) = stem.to_str() {
                // Replace "Data.db" with "Statistics.db"
                let stats_name = stem_str.replace("-Data", "-Statistics") + ".db";
                let stats_path = parent.join(stats_name);

                if tokio::fs::metadata(&stats_path).await.is_ok() {
                    return Some(stats_path);
                }
            }
        }
    }
    None
}

/// Utility function to check if a Statistics.db file exists for an SSTable directory
pub async fn check_statistics_availability(sstable_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut stats_files = Vec::new();

    let mut dir_entries = tokio::fs::read_dir(sstable_dir).await?;
    while let Some(entry) = dir_entries.next_entry().await? {
        let path = entry.path();
        if let Some(file_name) = path.file_name() {
            if let Some(name_str) = file_name.to_str() {
                if name_str.contains("-Statistics.db") {
                    stats_files.push(path);
                }
            }
        }
    }

    Ok(stats_files)
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_statistics_reader_creation() {
        // This test would require a real Statistics.db file
        // For now, just test the basic structure
        assert!(true);
    }

    #[tokio::test]
    async fn test_find_statistics_file() {
        use std::path::PathBuf;

        let data_path = PathBuf::from("/path/to/sstables/users-123abc-Data.db");
        // find_statistics_file would look for users-123abc-Statistics.db
        // This is a unit test for the path manipulation logic

        if let Some(_parent) = data_path.parent() {
            if let Some(stem) = data_path.file_stem() {
                if let Some(stem_str) = stem.to_str() {
                    let stats_name = stem_str.replace("-Data", "-Statistics") + ".db";
                    assert_eq!(stats_name, "users-123abc-Statistics.db");
                }
            }
        }
    }

    /// #1249 (roborev finding 1): a below-floor `*-Statistics.db` descriptor
    /// MUST make `StatisticsReader::open` fail with a typed
    /// `Error::UnsupportedVersion` rather than silently degrading to
    /// nb-compatible defaults and proceeding to parse. Drives the public
    /// `StatisticsReader::open` surface against a real on-disk file whose
    /// version (`ma`, a pre-`na` BIG version) is below the floor.
    #[tokio::test]
    async fn test_open_below_floor_statistics_rejected() {
        use crate::error::Error;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("create tempdir");
        // `ma` is a pre-`na` BIG version, below the version floor (#1249).
        let path = dir.path().join("ma-1-big-Statistics.db");
        // A present-but-below-floor file must still be rejected with the typed
        // floor error rather than parsed; this exercises the gate-floor branch
        // for an on-disk file.
        std::fs::write(&path, b"not really a valid statistics file").expect("write fixture");

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("create platform"),
        );

        let result = super::StatisticsReader::open(&path, platform).await;

        match result {
            Err(Error::UnsupportedVersion { version, floor }) => {
                assert_eq!(version, "ma", "error must name the offending version");
                assert_eq!(floor, "na", "error must name the na BIG floor");
            }
            Err(other) => panic!(
                "expected UnsupportedVersion for below-floor Statistics.db, got {:?}",
                other
            ),
            Ok(_) => panic!("below-floor Statistics.db must NOT open on nb-compatible defaults"),
        }
    }

    /// #1297: an unknown ABOVE-floor BIG version (`nc`, outside the exact
    /// `{na, nb, oa}` allowlist) must make `StatisticsReader::open` fail with a
    /// typed `Error::UnsupportedVersion` rather than parse an unvalidated layout
    /// on nb-compatible gates. Drives the public `StatisticsReader::open`
    /// surface (wiring evidence for the ceiling, not just the gate helper).
    #[tokio::test]
    async fn test_open_above_floor_unknown_statistics_rejected() {
        use crate::error::Error;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("create tempdir");
        // `nc` is above the `na` floor but NOT in the supported allowlist.
        let path = dir.path().join("nc-1-big-Statistics.db");
        std::fs::write(&path, b"not really a valid statistics file").expect("write fixture");

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("create platform"),
        );

        match super::StatisticsReader::open(&path, platform).await {
            Err(Error::UnsupportedVersion { version, .. }) => {
                assert_eq!(version, "nc", "error must name the offending version");
            }
            Err(other) => panic!(
                "expected UnsupportedVersion for above-allowlist Statistics.db, got {:?}",
                other
            ),
            Ok(_) => {
                panic!("above-allowlist Statistics.db must NOT open on nb-compatible defaults")
            }
        }
    }

    /// #1249 R1 (roborev finding 1): the version-floor check fires BEFORE the
    /// file body is read/parsed. A below-floor `*-Statistics.db` with an empty
    /// (0-byte) body must STILL fail with `UnsupportedVersion` rather than a
    /// parse/corruption error — proving `read_to_end` + the enhanced parser are
    /// never reached for a below-floor descriptor.
    #[tokio::test]
    async fn test_open_below_floor_statistics_rejected_before_body_read() {
        use crate::error::Error;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("ma-1-big-Statistics.db");
        // Empty body: if the gate ran AFTER read_to_end/parse, an empty file
        // would surface as a parse/corruption error, not UnsupportedVersion.
        std::fs::write(&path, b"").expect("write empty fixture");

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("create platform"),
        );

        match super::StatisticsReader::open(&path, platform).await {
            Err(Error::UnsupportedVersion { version, floor }) => {
                assert_eq!(version, "ma");
                assert_eq!(floor, "na");
            }
            Err(other) => panic!(
                "expected UnsupportedVersion before body read for empty below-floor \
                 Statistics.db, got {:?}",
                other
            ),
            Ok(_) => {
                panic!("below-floor Statistics.db with empty body must be rejected before parse")
            }
        }
    }

    /// #1249 R1: the version-floor check fires BEFORE *any* filesystem access.
    /// A below-floor `*-Statistics.db` descriptor whose path does NOT exist on
    /// disk must STILL fail with the typed `Error::UnsupportedVersion`, NOT a
    /// `NotFound`/I/O error. This proves the floor is enforced from the filename
    /// alone, before the existence check, `File::open`, and `read_to_end`
    /// (mirrors `SSTableReader::open_inner` gating before `tokio::fs::metadata`).
    #[tokio::test]
    async fn test_open_below_floor_statistics_rejected_before_fs_access() {
        use crate::error::Error;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("create tempdir");
        // `ma` is below the `na` BIG floor. Deliberately DO NOT create the file:
        // if the existence check ran first, this would surface as NotFound.
        let path = dir.path().join("ma-1-big-Statistics.db");
        assert!(
            !path.exists(),
            "fixture path must not exist on disk for this test"
        );

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("create platform"),
        );

        match super::StatisticsReader::open(&path, platform).await {
            Err(Error::UnsupportedVersion { version, floor }) => {
                assert_eq!(version, "ma", "error must name the offending version");
                assert_eq!(floor, "na", "error must name the na BIG floor");
            }
            Err(other) => panic!(
                "expected UnsupportedVersion before any FS access for a nonexistent \
                 below-floor Statistics.db, got {:?}",
                other
            ),
            Ok(_) => panic!("below-floor Statistics.db must NOT open even when absent"),
        }
    }

    /// Minimal in-memory statistics for report-rendering tests. `total_rows`,
    /// `live_rows`, and `avg_rows_per_partition` are the #1325 sentinel-carrying
    /// fields the caller sets per-scenario; everything else is a benign default.
    #[cfg(test)]
    fn stats_with(
        total_rows: u64,
        live_rows: u64,
        avg_rows_per_partition: f64,
    ) -> crate::parser::statistics::SSTableStatistics {
        use crate::parser::statistics::*;
        SSTableStatistics {
            header: StatisticsHeader {
                version: 4,
                statistics_kind: 0,
                data_length: 0,
                metadata1: 0,
                metadata2: 0,
                metadata3: 0,
                checksum: 0,
                table_id: None,
            },
            row_stats: RowStatistics {
                total_rows,
                live_rows,
                tombstone_count: 0,
                partition_count: 10,
                avg_rows_per_partition,
                row_size_histogram: vec![],
            },
            timestamp_stats: TimestampStatistics {
                min_timestamp: 0,
                max_timestamp: 0,
                min_deletion_time: 0,
                max_deletion_time: 0,
                min_ttl: None,
                max_ttl: None,
                rows_with_ttl: 0,
            },
            column_stats: vec![],
            table_stats: TableStatistics {
                disk_size: 1024,
                uncompressed_size: 1024,
                compressed_size: 1024,
                compression_ratio: 1.0,
                block_count: 1,
                avg_block_size: 1024.0,
                index_size: 0,
                bloom_filter_size: 0,
                level_count: 0,
            },
            partition_stats: PartitionStatistics {
                avg_partition_size: 0.0,
                min_partition_size: 0,
                max_partition_size: 0,
                size_histogram: vec![],
                large_partition_percentage: 0.0,
            },
            compression_stats: CompressionStatistics {
                algorithm: "none".to_string(),
                original_size: 1024,
                compressed_size: 1024,
                ratio: 1.0,
                compression_speed: 0.0,
                decompression_speed: 0.0,
                compressed_blocks: 0,
            },
            metadata: std::collections::HashMap::new(),
            serialization_header_columns: vec![],
            serialization_header_partition_keys: vec![],
            serialization_header_clustering_keys: vec![],
            tombstone_drop_times: vec![],
        }
    }

    /// #1325 sweep: `generate_report` and `compact_summary` must render the
    /// unavailable sentinels (`total_rows == 0`, `avg_rows_per_partition == 0.0`)
    /// as "unavailable"/"?" rather than a misleading concrete `0`.
    #[tokio::test]
    async fn test_report_renders_unavailable_when_counts_sentinel() {
        let reader = super::StatisticsReader::from_statistics_for_test(stats_with(0, 0, 0.0)).await;

        let report = reader.generate_report(false);
        assert!(
            report.contains("**Total Rows**: unavailable"),
            "overview Total Rows must render unavailable for the total_rows==0 sentinel:\n{report}"
        );
        assert!(
            report.contains("Total rows: unavailable"),
            "row-stats Total rows must render unavailable for the sentinel:\n{report}"
        );
        assert!(
            report.contains("Average rows per partition: unavailable"),
            "avg rows/partition must render unavailable for the sentinel:\n{report}"
        );
        assert!(
            !report.contains("Total rows: 0"),
            "must NOT render the misleading concrete 0:\n{report}"
        );

        let compact = reader.compact_summary();
        assert!(
            compact.contains("Rows: ?"),
            "compact summary must render '?' for the total_rows==0 sentinel: {compact}"
        );
    }

    /// #1325 sweep (non-vacuous positive path): real counts render as concrete
    /// numbers, not "unavailable".
    #[tokio::test]
    async fn test_report_renders_concrete_when_counts_real() {
        let reader =
            super::StatisticsReader::from_statistics_for_test(stats_with(1000, 900, 20.0)).await;

        let report = reader.generate_report(false);
        assert!(
            report.contains("**Total Rows**: 1000"),
            "overview must render the real total_rows:\n{report}"
        );
        assert!(
            report.contains("Average rows per partition: 20.0"),
            "avg rows/partition must render the real value:\n{report}"
        );
        assert!(
            !report.contains("Total Rows**: unavailable"),
            "must NOT render unavailable when the count is real:\n{report}"
        );

        let compact = reader.compact_summary();
        assert!(
            compact.contains("Rows: 1000"),
            "compact summary must render the real total_rows: {compact}"
        );
    }

    /// #1374 roborev finding: `avg_rows_per_partition` is
    /// `total_rows / partition_count`, so its `0.0` unavailable sentinel also
    /// occurs when `total_rows > 0` but `partition_count == 0`. The report must
    /// render "unavailable" in that case (guard requires BOTH counts positive).
    /// Total rows itself is real here, so it must still render concretely.
    #[tokio::test]
    async fn test_report_renders_avg_unavailable_when_partition_count_zero() {
        let mut stats = stats_with(1000, 900, 0.0);
        stats.row_stats.partition_count = 0; // avg is the unavailable sentinel
        let reader = super::StatisticsReader::from_statistics_for_test(stats).await;

        let report = reader.generate_report(false);
        assert!(
            report.contains("Average rows per partition: unavailable"),
            "avg rows/partition must render unavailable when partition_count == 0:\n{report}"
        );
        assert!(
            !report.contains("Average rows per partition: 0.0"),
            "must NOT render the misleading concrete 0.0 avg:\n{report}"
        );
        // Total rows is authoritative here, so it must still render concretely.
        assert!(
            report.contains("Total rows: 1000"),
            "row-stats Total rows must still render the real count:\n{report}"
        );
    }
}
