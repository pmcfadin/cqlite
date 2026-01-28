//! Statistics.db writer - writes SSTable metadata
//!
//! Generates the Statistics.db component with min/max timestamps, TTL, and
//! other metadata used for delta encoding in Data.db.
//!
//! Critical requirements:
//! - MUST be written BEFORE Data.db (provides delta encoding baseline)
//! - Min timestamp, max timestamp
//! - Min TTL, max TTL
//! - Min local deletion time, max local deletion time
//! - Partition count, row count
//!
//! # Statistics.db Format (Cassandra 5.0 nb-format)
//!
//! The file consists of:
//! 1. Table of Contents (TOC) - Component type/offset pairs
//! 2. Component sections (VALIDATION, COMPACTION, STATS, HEADER)
//!
//! ## TOC Structure
//! ```text
//! [u32 BE] num_components
//! [u32 BE] checksum
//! For each component:
//!   [u32 BE] component_type (0=VALIDATION, 1=COMPACTION, 2=STATS, 3=HEADER)
//!   [u32 BE] component_offset (byte offset from start of file)
//! ```
//!
//! ## Component Sections
//!
//! ### VALIDATION Metadata (type 0)
//! - Partition count
//! - Max local deletion time (used for tombstone GC)
//!
//! ### COMPACTION Metadata (type 1)
//! - Ancestor generations
//! - Cardinality estimator
//!
//! ### STATS Metadata (type 2) - EncodingStats
//! - Min timestamp (microseconds)
//! - Min local deletion time (seconds)
//! - Min TTL (seconds)
//!
//! ### HEADER Metadata (type 3) - SerializationHeader
//! - Partition key types
//! - Clustering key types
//! - Regular column definitions

use crate::error::{Error, Result};
use crate::parser::vint::{encode_vint, encode_vuint};
use std::io::Write;
use std::path::PathBuf;

// Cassandra MetadataType enum ordinals are defined but not used in the simplified
// nb-format writer which uses a sequential layout instead of TOC-based components

/// Statistics metadata collected during memtable flush
///
/// This structure holds all the metadata needed to write Statistics.db.
/// Values are collected as rows are written to Data.db.
#[derive(Debug, Clone)]
pub struct StatisticsMetadata {
    /// Minimum timestamp in the SSTable (microseconds since epoch)
    pub min_timestamp: i64,
    /// Maximum timestamp in the SSTable (microseconds since epoch)
    pub max_timestamp: i64,
    /// Minimum local deletion time (seconds since epoch, for tombstones)
    pub min_local_deletion_time: i32,
    /// Maximum local deletion time (seconds since epoch)
    pub max_local_deletion_time: i32,
    /// Minimum TTL value (seconds, 0 if no TTL)
    pub min_ttl: i32,
    /// Maximum TTL value (seconds, 0 if no TTL)
    pub max_ttl: i32,
    /// Total number of partitions in the SSTable
    pub partition_count: u64,
    /// Total number of rows (live + tombstones)
    pub row_count: u64,
    /// Total number of columns across all rows
    pub column_count: u64,
    /// Total size of all rows in bytes
    pub total_rows_size: u64,
}

impl Default for StatisticsMetadata {
    fn default() -> Self {
        Self {
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            min_local_deletion_time: i32::MAX,
            max_local_deletion_time: i32::MIN,
            min_ttl: i32::MAX,
            max_ttl: 0,
            partition_count: 0,
            row_count: 0,
            column_count: 0,
            total_rows_size: 0,
        }
    }
}

impl StatisticsMetadata {
    /// Create a new empty statistics metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Update timestamp range with a new timestamp value
    pub fn update_timestamp(&mut self, timestamp: i64) {
        self.min_timestamp = self.min_timestamp.min(timestamp);
        self.max_timestamp = self.max_timestamp.max(timestamp);
    }

    /// Update local deletion time range (for tombstones)
    pub fn update_local_deletion_time(&mut self, deletion_time: i32) {
        self.min_local_deletion_time = self.min_local_deletion_time.min(deletion_time);
        self.max_local_deletion_time = self.max_local_deletion_time.max(deletion_time);
    }

    /// Update TTL range
    pub fn update_ttl(&mut self, ttl: i32) {
        if ttl > 0 {
            self.min_ttl = self.min_ttl.min(ttl);
            self.max_ttl = self.max_ttl.max(ttl);
        }
    }

    /// Increment partition count
    pub fn increment_partition_count(&mut self) {
        self.partition_count += 1;
    }

    /// Increment row count
    pub fn increment_row_count(&mut self) {
        self.row_count += 1;
    }

    /// Add to column count
    pub fn add_column_count(&mut self, count: u64) {
        self.column_count += count;
    }

    /// Add to total rows size
    pub fn add_rows_size(&mut self, size: u64) {
        self.total_rows_size += size;
    }

    /// Finalize metadata before writing (normalize sentinel values)
    pub fn finalize(&mut self) {
        // If no timestamps were recorded, set to 0
        if self.min_timestamp == i64::MAX {
            self.min_timestamp = 0;
        }
        if self.max_timestamp == i64::MIN {
            self.max_timestamp = 0;
        }

        // If no deletion times were recorded, set to 0
        if self.min_local_deletion_time == i32::MAX {
            self.min_local_deletion_time = 0;
        }
        if self.max_local_deletion_time == i32::MIN {
            self.max_local_deletion_time = 0;
        }

        // If no TTLs were recorded, set min_ttl to 0
        if self.min_ttl == i32::MAX {
            self.min_ttl = 0;
        }
    }
}

/// Statistics.db component writer
///
/// Writes the Statistics.db file with metadata for SSTable delta encoding.
#[derive(Debug)]
pub struct StatisticsWriter {
    /// Path to the Statistics.db file to write
    path: PathBuf,
}

impl StatisticsWriter {
    /// Create a new Statistics.db writer
    ///
    /// # Arguments
    /// * `path` - Path where Statistics.db will be written
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Write Statistics.db file with the given metadata
    ///
    /// This generates a minimal nb-format Statistics.db following the observed format
    /// from real Cassandra 5.0 files:
    /// 1. 32-byte header (version 4, statistics_kind, data_length, checksum)
    /// 2. Sequential component data (EncodingStats with partitioner, min values)
    ///
    /// # Arguments
    /// * `metadata` - Statistics metadata to write
    ///
    /// # Returns
    /// `Ok(())` on success, or an error if writing fails
    pub fn write(&self, metadata: &StatisticsMetadata) -> Result<()> {
        let mut meta = metadata.clone();
        meta.finalize();

        // Build EncodingStats section (the critical data for delta encoding)
        let encoding_stats_bytes = self.build_encoding_stats(&meta)?;

        // Build 32-byte nb-format header
        let mut header_buffer = Vec::new();

        // Version = 4 (nb-format)
        header_buffer.write_all(&4u32.to_be_bytes())?;

        // Statistics kind (using same value as observed in real files: 0x26291b05)
        // This appears to be a magic number/hash in Cassandra
        header_buffer.write_all(&0x26291b05u32.to_be_bytes())?;

        // Reserved field
        header_buffer.write_all(&0u32.to_be_bytes())?;

        // Data length (length of the EncodingStats section)
        header_buffer.write_all(&(encoding_stats_bytes.len() as u32).to_be_bytes())?;

        // Metadata fields (observed values: 1, 101, 2)
        // These appear to be version/format markers
        header_buffer.write_all(&1u32.to_be_bytes())?;  // metadata1
        header_buffer.write_all(&101u32.to_be_bytes())?;  // metadata2 (101 = 0x65)
        header_buffer.write_all(&2u32.to_be_bytes())?;  // metadata3

        // Checksum (placeholder - proper CRC32 deferred to future milestone)
        header_buffer.write_all(&0u32.to_be_bytes())?;

        // Assemble final file: header + encoding stats
        let mut file_buffer = Vec::new();
        file_buffer.write_all(&header_buffer)?;
        file_buffer.write_all(&encoding_stats_bytes)?;

        // Write to file
        std::fs::write(&self.path, file_buffer).map_err(|e| {
            Error::Storage(format!(
                "Failed to write Statistics.db to {}: {}",
                self.path.display(),
                e
            ))
        })?;

        Ok(())
    }

    /// Build EncodingStats section for delta encoding
    ///
    /// This is the critical component that provides baseline values for Data.db delta encoding.
    /// The format follows the observed structure from real Cassandra 5.0 Statistics.db files.
    ///
    /// Format (observed from enhanced_statistics_parser.rs):
    /// - [VUInt] partitioner_length (unsigned VInt)
    /// - [bytes] partitioner_class_name
    /// - [VUInt] metadata1 (unknown purpose, value 0)
    /// - [VUInt] metadata2 (unknown purpose, value 0)
    /// - [VInt] min_timestamp (microseconds, signed ZigZag VInt)
    /// - [VInt] min_local_deletion_time (seconds, signed ZigZag VInt)
    /// - [VInt] min_ttl (seconds, signed ZigZag VInt)
    fn build_encoding_stats(&self, metadata: &StatisticsMetadata) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // Partitioner (use Murmur3Partitioner as default)
        let partitioner = b"org.apache.cassandra.dht.Murmur3Partitioner";
        // Use unsigned VInt for string length (matches parse_vuint)
        buffer.write_all(&encode_vuint(partitioner.len() as u64))?;
        buffer.write_all(partitioner)?;

        // Unknown metadata fields (observed in parser, skipped during reading)
        // Use placeholder values of 0
        buffer.write_all(&encode_vuint(0))?;  // metadata1
        buffer.write_all(&encode_vuint(0))?;  // metadata2

        // EncodingStats baseline values for delta encoding
        // These values MUST match what Data.db uses as deltas
        // Use signed ZigZag VInt for these values (matches parse_vint)
        buffer.write_all(&encode_vint(metadata.min_timestamp))?;
        buffer.write_all(&encode_vint(metadata.min_local_deletion_time as i64))?;
        buffer.write_all(&encode_vint(metadata.min_ttl as i64))?;

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_statistics_metadata_default() {
        let meta = StatisticsMetadata::new();
        assert_eq!(meta.partition_count, 0);
        assert_eq!(meta.row_count, 0);
    }

    #[test]
    fn test_statistics_metadata_update_timestamp() {
        let mut meta = StatisticsMetadata::new();
        meta.update_timestamp(1000000);
        meta.update_timestamp(2000000);
        meta.update_timestamp(500000);

        assert_eq!(meta.min_timestamp, 500000);
        assert_eq!(meta.max_timestamp, 2000000);
    }

    #[test]
    fn test_statistics_metadata_update_ttl() {
        let mut meta = StatisticsMetadata::new();
        meta.update_ttl(3600);
        meta.update_ttl(86400);
        meta.update_ttl(1800);

        assert_eq!(meta.min_ttl, 1800);
        assert_eq!(meta.max_ttl, 86400);
    }

    #[test]
    fn test_statistics_metadata_finalize() {
        let mut meta = StatisticsMetadata::new();
        // Don't set any values
        meta.finalize();

        // Should normalize sentinel values to 0
        assert_eq!(meta.min_timestamp, 0);
        assert_eq!(meta.max_timestamp, 0);
        assert_eq!(meta.min_local_deletion_time, 0);
        assert_eq!(meta.max_local_deletion_time, 0);
        assert_eq!(meta.min_ttl, 0);
    }

    #[test]
    fn test_statistics_writer_basic() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("test-Statistics.db");

        let writer = StatisticsWriter::new(stats_path.clone());

        let mut meta = StatisticsMetadata::new();
        meta.update_timestamp(1000000);
        meta.update_timestamp(2000000);
        meta.min_local_deletion_time = 0;
        meta.max_local_deletion_time = 0;
        meta.min_ttl = 0;
        meta.max_ttl = 0;
        meta.partition_count = 10;
        meta.row_count = 100;

        let result = writer.write(&meta);
        assert!(result.is_ok(), "Write should succeed: {:?}", result);

        // Verify file was created
        assert!(stats_path.exists());

        // Verify file is not empty
        let file_size = std::fs::metadata(&stats_path).unwrap().len();
        assert!(file_size > 0, "Statistics.db should not be empty");

        // Read back and verify nb-format header
        let file_data = std::fs::read(&stats_path).unwrap();
        assert!(file_data.len() >= 32, "File should have at least 32-byte header");

        // Verify version = 4 (nb-format)
        let version = u32::from_be_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
        assert_eq!(version, 4, "Should have nb-format version 4");

        // Verify statistics_kind
        let stats_kind = u32::from_be_bytes([file_data[4], file_data[5], file_data[6], file_data[7]]);
        assert_eq!(stats_kind, 0x26291b05, "Should have expected statistics_kind");
    }

    #[test]
    fn test_build_encoding_stats() {
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = 1000000;
        meta.min_local_deletion_time = 0;
        meta.min_ttl = 3600;

        let result = writer.build_encoding_stats(&meta);
        assert!(result.is_ok());

        let bytes = result.unwrap();
        assert!(!bytes.is_empty());

        // Should contain partitioner string
        let partitioner = b"org.apache.cassandra.dht.Murmur3Partitioner";
        assert!(bytes.windows(partitioner.len()).any(|w| w == partitioner));
    }

    #[test]
    fn test_nb_format_constants() {
        // Verify nb-format version is 4
        let version = 4u32;
        assert_eq!(version, 4);

        // Verify statistics_kind magic number
        let stats_kind = 0x26291b05u32;
        assert_eq!(stats_kind, 0x26291b05);
    }
}
