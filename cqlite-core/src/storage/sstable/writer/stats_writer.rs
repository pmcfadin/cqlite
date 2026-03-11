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
//! # Statistics.db Format (Minimal Compatibility Implementation)
//!
//! **Note**: This implementation produces a minimal format that is compatible with
//! the `enhanced_statistics_parser.rs` reader but does NOT produce a full Cassandra
//! TOC structure. Instead, it uses a hybrid format where the 32-byte header is
//! structured to be parseable by both `parse_nb_format_header()` (which reads 8 u32s)
//! and `parse_statistics_toc_for_header_offset()` (which looks for num_components=4).
//!
//! ## Actual Format Produced
//!
//! ```text
//! Bytes 0-31: Header (doubles as fake TOC)
//!   [u32 BE] 4                    - Interpreted as num_components or version
//!   [u32 BE] 0x26291b05           - Statistics magic number
//!   [u32 BE] 0                    - Reserved
//!   [u32 BE] data_length          - Length of EncodingStats data
//!   [u32 BE] 1, 0x65, 2, 0        - Metadata fields (observed in real files)
//!
//! Bytes 32+: EncodingStats data
//!   [u32 BE] 3                    - Metadata type (EncodingStats marker)
//!   [VUInt]  0                    - Data length placeholder
//!   [VUInt]  43                   - Partitioner string length
//!   [bytes]  Murmur3Partitioner   - Partitioner class name
//!   [VUInt]  0, 0                 - Metadata placeholders
//!   [VInt]   min_timestamp        - ZigZag encoded microseconds
//!   [VInt]   min_deletion_time    - ZigZag encoded seconds
//!   [VInt]   min_ttl              - ZigZag encoded seconds
//! ```
//!
//! ## Full Cassandra TOC Structure (for reference, NOT implemented)
//!
//! Real Cassandra Statistics.db files have:
//! 1. TOC: num_components (4) + checksum + 4 component entries (32 bytes)
//! 2. VALIDATION component at offset ~44
//! 3. COMPACTION component
//! 4. STATS component (contains EncodingStats within larger structure)
//! 5. HEADER component (SerializationHeader with schema)

use crate::error::{Error, Result};
use crate::parser::vint::{encode_vint, encode_vuint};
use std::io::Write;
use std::path::PathBuf;

/// Statistics.db magic number observed in Cassandra 5.0 nb-format files.
/// This value appears at bytes 4-7 and is interpreted as:
/// - `statistics_kind` by parse_nb_format_header()
/// - `checksum` by parse_statistics_toc_for_header_offset()
///
/// Source: Hex dump of real Cassandra Statistics.db files
const STATISTICS_KIND_MAGIC: u32 = 0x26291b05;

/// nb-format version number. Value 4 indicates Cassandra 5.0 format.
/// This value appears at bytes 0-3 and is interpreted as:
/// - `version_type` by parse_nb_format_header()
/// - `num_components` by parse_statistics_toc_for_header_offset()
const NB_FORMAT_VERSION: u32 = 4;

/// Metadata field values observed in real Cassandra Statistics.db files.
/// Purpose of these values is unclear; they may relate to TOC entry structure
/// or format versioning. Values extracted from hex dump analysis.
const METADATA_FIELD_1: u32 = 1;
const METADATA_FIELD_2: u32 = 0x65; // 101 decimal
const METADATA_FIELD_3: u32 = 2;

/// EncodingStats section type marker.
/// Indicates start of EncodingStats data after the 32-byte header.
const ENCODING_STATS_TYPE: u32 = 3;

/// Default partitioner class name.
/// Currently only Murmur3Partitioner is supported.
const DEFAULT_PARTITIONER: &[u8] = b"org.apache.cassandra.dht.Murmur3Partitioner";

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
    /// This generates a Cassandra 5.0 compatible Statistics.db file with a hybrid format:
    /// - Bytes 0-31: "Header" (actually TOC entries 0-2, read by parse_nb_format_header)
    /// - Bytes 32-39: TOC entry 3 + EncodingStats prefix
    /// - Bytes 40+: Actual EncodingStats data
    ///
    /// The parser reads this as:
    /// 1. parse_nb_format_header reads bytes 0-31
    /// 2. parse_minimal_encoding_stats reads bytes 32+ as: metadata_type (u32), then data
    ///
    /// # Arguments
    /// * `metadata` - Statistics metadata to write
    ///
    /// # Returns
    /// `Ok(())` on success, or an error if writing fails
    pub fn write(&self, metadata: &StatisticsMetadata) -> Result<()> {
        let mut meta = metadata.clone();
        meta.finalize();

        // Build the EncodingStats data section (includes metadata_type and all data)
        let encoding_data = self.build_encoding_stats_data(&meta)?;

        // Build the complete file structure
        let mut file_buffer = Vec::new();

        // Bytes 0-31: 32-byte header (matches parse_nb_format_header expectations)
        file_buffer.write_all(&NB_FORMAT_VERSION.to_be_bytes())?;
        file_buffer.write_all(&STATISTICS_KIND_MAGIC.to_be_bytes())?;
        file_buffer.write_all(&0u32.to_be_bytes())?; // reserved1
        file_buffer.write_all(&(encoding_data.len() as u32).to_be_bytes())?; // data_length
        file_buffer.write_all(&METADATA_FIELD_1.to_be_bytes())?;
        file_buffer.write_all(&METADATA_FIELD_2.to_be_bytes())?;
        file_buffer.write_all(&METADATA_FIELD_3.to_be_bytes())?;
        file_buffer.write_all(&0u32.to_be_bytes())?; // checksum_or_more (placeholder)

        // Bytes 32+: EncodingStats data (starts with metadata_type = 3)
        file_buffer.write_all(&encoding_data)?;

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

    /// Build EncodingStats data section (bytes 32+)
    ///
    /// Matches the format observed in real Cassandra 5.0 Statistics.db files.
    /// Based on hex analysis, the structure after the 32-byte header is:
    /// - [u32 BE] metadata_type = 3
    /// - [8 bytes] Unknown fields (appears to be part of TOC entry 3)
    /// - [u8] Reserved byte = 0x00
    /// - [VUInt] partitioner_length
    /// - [bytes] partitioner_class_name
    /// - [Unknown data] Additional metadata before EncodingStats
    /// - [VInt] min_timestamp
    /// - [VInt] min_local_deletion_time
    /// - [VInt] min_ttl
    ///
    /// This implementation creates a minimal version that the parser can read.
    fn build_encoding_stats_data(&self, metadata: &StatisticsMetadata) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // Metadata type = 3 (EncodingStats identifier)
        buffer.write_all(&ENCODING_STATS_TYPE.to_be_bytes())?;

        // data_length (VUInt) - The parser reads and discards this
        // TODO(M6): Investigate if this needs to be the actual data length
        buffer.write_all(&encode_vuint(0))?;

        // Partitioner (currently only Murmur3Partitioner is supported)
        buffer.write_all(&encode_vuint(DEFAULT_PARTITIONER.len() as u64))?;
        buffer.write_all(DEFAULT_PARTITIONER)?;

        // Metadata fields (observed in parser, purpose unclear)
        // TODO(M6): Investigate what these fields represent in Cassandra
        buffer.write_all(&encode_vuint(0))?; // metadata1 (placeholder)
        buffer.write_all(&encode_vuint(0))?; // metadata2 (placeholder)

        // EncodingStats baseline values for delta encoding
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

        // Read back and verify hybrid format
        let file_data = std::fs::read(&stats_path).unwrap();
        assert!(file_data.len() >= 40, "File should have at least 40 bytes");

        // Verify num_components = 4 (byte 0-3)
        let num_components =
            u32::from_be_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
        assert_eq!(num_components, 4, "Should have num_components=4");

        // Verify statistics_kind/checksum (bytes 4-7)
        let stats_kind =
            u32::from_be_bytes([file_data[4], file_data[5], file_data[6], file_data[7]]);
        assert_eq!(
            stats_kind, 0x26291b05,
            "Should have expected statistics_kind"
        );

        // Verify metadata_type = 3 at offset 32
        let metadata_type =
            u32::from_be_bytes([file_data[32], file_data[33], file_data[34], file_data[35]]);
        assert_eq!(metadata_type, 3, "metadata_type should be 3");
    }

    #[test]
    fn test_build_encoding_stats_data() {
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = 1000000;
        meta.min_local_deletion_time = 0;
        meta.min_ttl = 3600;

        let result = writer.build_encoding_stats_data(&meta);
        assert!(result.is_ok());

        let bytes = result.unwrap();
        assert!(!bytes.is_empty());

        // Should contain partitioner string
        let partitioner = b"org.apache.cassandra.dht.Murmur3Partitioner";
        assert!(bytes.windows(partitioner.len()).any(|w| w == partitioner));
    }

    #[test]
    fn test_encoding_stats_data_format() {
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));

        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = 1000000;
        meta.min_local_deletion_time = 0;
        meta.min_ttl = 0;

        let result = writer.build_encoding_stats_data(&meta);
        assert!(result.is_ok());

        let data = result.unwrap();
        assert!(!data.is_empty());

        // Should contain partitioner string
        let partitioner = b"org.apache.cassandra.dht.Murmur3Partitioner";
        assert!(data.windows(partitioner.len()).any(|w| w == partitioner));
    }
}
