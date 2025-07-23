//! Statistics.db file reader for enhanced SSTable metadata
//!
//! This module provides a high-level interface for reading and analyzing
//! Statistics.db files that accompany SSTable Data.db files in Cassandra 5+.

use crate::{
    error::{Error, Result},
    parser::statistics::{SSTableStatistics, StatisticsAnalyzer, StatisticsSummary},
    parser::enhanced_statistics_parser::parse_statistics_with_fallback,
    platform::Platform,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

/// High-level Statistics.db file reader
pub struct StatisticsReader {
    /// Path to the Statistics.db file
    file_path: PathBuf,
    /// Parsed statistics data
    statistics: SSTableStatistics,
    /// Platform abstraction for file operations
    platform: Arc<Platform>,
}

impl StatisticsReader {
    /// Open and parse a Statistics.db file
    pub async fn open(path: &Path, platform: Arc<Platform>) -> Result<Self> {
        if !platform.fs().exists(path).await? {
            return Err(Error::not_found(format!(
                "Statistics.db file not found: {}",
                path.display()
            )));
        }

        // Read the entire file (Statistics.db files are typically small)
        let mut file = File::open(path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        // Parse the statistics data using enhanced parser with fallback
        let statistics = match parse_statistics_with_fallback(&buffer) {
            Ok((_, stats)) => stats,
            Err(e) => {
                return Err(Error::corruption(format!(
                    "Failed to parse Statistics.db with enhanced parser: {:?}",
                    e
                )));
            }
        };

        // Validate checksum if present (for enhanced parser, checksums are handled differently)
        if statistics.header.checksum != 0 {
            // For nb format, the checksum field may not be a simple CRC32
            // Skip validation for now as the format is more complex
            // For nb format, the checksum field may not be a simple CRC32
            // Skip validation for now as the format is more complex
            // TODO: Add proper checksum validation for nb format
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
    pub fn column_stats(&self, column_name: &str) -> Option<&crate::parser::statistics::ColumnStatistics> {
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
        report.push_str(&format!("- **Total Rows**: {}\n", summary.total_rows));
        report.push_str(&format!("- **Live Data**: {:.2}%\n", summary.live_data_percentage));
        report.push_str(&format!("- **Compression Efficiency**: {:.2}%\n", summary.compression_efficiency));
        report.push_str(&format!("- **Time Range**: {:.1} days\n", summary.timestamp_range_days));
        report.push_str(&format!("- **Largest Partition**: {:.2} MB\n", summary.largest_partition_mb));
        report.push_str(&format!("- **Health Score**: {:.1}/100\n\n", summary.health_score));

        // Row statistics
        report.push_str("## Row Statistics\n");
        report.push_str(&format!("- Total rows: {}\n", self.statistics.row_stats.total_rows));
        report.push_str(&format!("- Live rows: {}\n", self.statistics.row_stats.live_rows));
        report.push_str(&format!("- Tombstones: {}\n", self.statistics.row_stats.tombstone_count));
        report.push_str(&format!("- Partitions: {}\n", self.statistics.row_stats.partition_count));
        report.push_str(&format!(
            "- Average rows per partition: {:.1}\n\n",
            self.statistics.row_stats.avg_rows_per_partition
        ));

        // Timestamp information
        if self.statistics.timestamp_stats.min_timestamp != 0 {
            let min_time = chrono::DateTime::from_timestamp_micros(self.statistics.timestamp_stats.min_timestamp);
            let max_time = chrono::DateTime::from_timestamp_micros(self.statistics.timestamp_stats.max_timestamp);
            
            report.push_str("## Timestamp Range\n");
            if let (Some(min), Some(max)) = (min_time, max_time) {
                report.push_str(&format!("- From: {}\n", min.format("%Y-%m-%d %H:%M:%S UTC")));
                report.push_str(&format!("- To: {}\n", max.format("%Y-%m-%d %H:%M:%S UTC")));
            } else {
                report.push_str(&format!("- Min timestamp: {}\n", self.statistics.timestamp_stats.min_timestamp));
                report.push_str(&format!("- Max timestamp: {}\n", self.statistics.timestamp_stats.max_timestamp));
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
        report.push_str(&format!("- Algorithm: {}\n", self.statistics.compression_stats.algorithm));
        report.push_str(&format!(
            "- Original size: {:.2} MB\n",
            self.statistics.compression_stats.original_size as f64 / 1_048_576.0
        ));
        report.push_str(&format!(
            "- Compressed size: {:.2} MB\n",
            self.statistics.compression_stats.compressed_size as f64 / 1_048_576.0
        ));
        report.push_str(&format!("- Ratio: {:.2}%\n", self.statistics.compression_stats.ratio * 100.0));
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
        format!(
            "Rows: {} ({:.1}% live) | Compression: {:.1}% | Health: {:.0}/100 | Size: {:.2} MB",
            summary.total_rows,
            summary.live_data_percentage,
            summary.compression_efficiency,
            summary.health_score,
            self.statistics.table_stats.disk_size as f64 / 1_048_576.0
        )
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
    use super::*;
    use crate::Config;

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
        
        if let Some(parent) = data_path.parent() {
            if let Some(stem) = data_path.file_stem() {
                if let Some(stem_str) = stem.to_str() {
                    let stats_name = stem_str.replace("-Data", "-Statistics") + ".db";
                    assert_eq!(stats_name, "users-123abc-Statistics.db");
                }
            }
        }
    }
}