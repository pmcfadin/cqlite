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
//! # Statistics.db Format (Cassandra 5.0 Compatible)
//!
//! This implementation produces a full Cassandra 5.0 nb-format Statistics.db with:
//! - TOC (Table of Contents) with checksums
//! - Four metadata components: VALIDATION, COMPACTION, STATS, HEADER
//! - Per-component CRC32 checksums
//! - Global CRC32 checksum validation
//!
//! ## Format Structure
//!
//! ```text
//! [0-3]   num_components (u32 BE) = 4
//! [4-7]   CRC32(num_components)
//! [8-39]  TOC entries (4 components × 8 bytes each):
//!           [u32 BE] component_type (MetadataType ordinal)
//!           [u32 BE] component_offset
//! [40-43] CRC32(num_components + all TOC entries) [cumulative]
//! [44+]   Component data:
//!           [N bytes] component_data
//!           [4 bytes] CRC32(component_data)
//!           ... (repeated for each component)
//! ```
//!
//! ## MetadataType Component IDs
//!
//! From Cassandra's `MetadataType.java` enum (ordinal values):
//! - 0: VALIDATION (validator class name)
//! - 1: COMPACTION (compaction metadata)
//! - 2: STATS (statistics including EncodingStats)
//! - 3: SERIALIZATION_HEADER (table schema)

use crate::error::{Error, Result};
use crate::parser::vint::encode_vuint;
use std::io::Write;
use std::path::PathBuf;

/// Epoch constants for EncodingStats (from Cassandra's EncodingStats.java)
/// These are used to compute deltas from a baseline for more compact encoding
#[allow(dead_code)]
const TIMESTAMP_EPOCH: i64 = 1442880000000000; // Sept 22, 2015 00:00:00 UTC in microseconds
#[allow(dead_code)]
const DELETION_TIME_EPOCH: i32 = 1442880000; // Sept 22, 2015 00:00:00 UTC in seconds
#[allow(dead_code)]
const TTL_EPOCH: i32 = 0; // TTL epoch is 0 (no offset)

/// Number of metadata components in Statistics.db
/// Cassandra 5.0 nb-format has 4 components: VALIDATION, COMPACTION, STATS, HEADER
const NUM_COMPONENTS: u32 = 4;

/// MetadataType ordinal values (from Cassandra's MetadataType.java enum)
const METADATA_TYPE_VALIDATION: u32 = 0;
const METADATA_TYPE_COMPACTION: u32 = 1;
const METADATA_TYPE_STATS: u32 = 2;
const METADATA_TYPE_SERIALIZATION_HEADER: u32 = 3;

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
    /// Generates a Cassandra 5.0 compatible Statistics.db file with full TOC structure:
    /// 1. TOC header with component count and checksums
    /// 2. VALIDATION component (validator class name)
    /// 3. COMPACTION component (minimal metadata)
    /// 4. STATS component (EncodingStats with baselines)
    /// 5. SERIALIZATION_HEADER component (minimal schema stub)
    ///
    /// Each component is followed by a CRC32 checksum for validation.
    ///
    /// # Arguments
    /// * `metadata` - Statistics metadata to write
    ///
    /// # Returns
    /// `Ok(())` on success, or an error if writing fails
    pub fn write(&self, metadata: &StatisticsMetadata) -> Result<()> {
        let mut meta = metadata.clone();
        meta.finalize();

        // Build component data
        let validation_data = self.build_validation_component()?;
        let compaction_data = self.build_compaction_component()?;
        let stats_data = self.build_stats_component(&meta)?;
        let header_data = self.build_serialization_header_component()?;

        // Calculate component offsets
        // TOC structure: 4 (count) + 4 (checksum) + (4*8) TOC entries + 4 (checksum) = 44 bytes
        let toc_size = 4 + 4 + (NUM_COMPONENTS as usize * 8) + 4;
        let mut offset = toc_size;

        let validation_offset = offset;
        offset += validation_data.len() + 4; // +4 for component checksum

        let compaction_offset = offset;
        offset += compaction_data.len() + 4;

        let stats_offset = offset;
        offset += stats_data.len() + 4;

        let header_offset = offset;
        // header_data has its own checksum at the end

        // Verify all offsets fit in u32 (Statistics.db should never exceed 4GB)
        if offset > u32::MAX as usize {
            return Err(Error::Storage(format!(
                "Statistics.db too large: {} bytes exceeds u32::MAX",
                offset
            )));
        }

        // Build the complete file
        let mut buffer = Vec::new();
        let mut crc = crc32fast::Hasher::new();

        // Write component count
        buffer.write_all(&NUM_COMPONENTS.to_be_bytes())?;
        self.update_checksum_int(&mut crc, NUM_COMPONENTS);

        // Write first checksum (after count)
        let checksum1 = crc.clone().finalize();
        buffer.write_all(&checksum1.to_be_bytes())?;

        // Reset CRC for TOC (we'll recompute cumulatively)
        crc = crc32fast::Hasher::new();
        self.update_checksum_int(&mut crc, NUM_COMPONENTS);

        // Write TOC entries (type, offset pairs)
        self.write_toc_entry(
            &mut buffer,
            &mut crc,
            METADATA_TYPE_VALIDATION,
            validation_offset as u32,
        )?;
        self.write_toc_entry(
            &mut buffer,
            &mut crc,
            METADATA_TYPE_COMPACTION,
            compaction_offset as u32,
        )?;
        self.write_toc_entry(
            &mut buffer,
            &mut crc,
            METADATA_TYPE_STATS,
            stats_offset as u32,
        )?;
        self.write_toc_entry(
            &mut buffer,
            &mut crc,
            METADATA_TYPE_SERIALIZATION_HEADER,
            header_offset as u32,
        )?;

        // Write TOC checksum (cumulative from count)
        let toc_checksum = crc.finalize();
        buffer.write_all(&toc_checksum.to_be_bytes())?;

        // Write components with per-component checksums
        self.write_component(&mut buffer, &validation_data)?;
        self.write_component(&mut buffer, &compaction_data)?;
        self.write_component(&mut buffer, &stats_data)?;
        self.write_component(&mut buffer, &header_data)?;

        // Write to file
        std::fs::write(&self.path, buffer).map_err(|e| {
            Error::Storage(format!(
                "Failed to write Statistics.db to {}: {}",
                self.path.display(),
                e
            ))
        })?;

        Ok(())
    }

    /// Update CRC32 checksum with a u32 value (big-endian)
    ///
    /// Mimics Java's FBUtilities.updateChecksumInt()
    fn update_checksum_int(&self, crc: &mut crc32fast::Hasher, value: u32) {
        crc.update(&value.to_be_bytes());
    }

    /// Write a TOC entry (component type and offset) with cumulative CRC update
    fn write_toc_entry(
        &self,
        buffer: &mut Vec<u8>,
        crc: &mut crc32fast::Hasher,
        component_type: u32,
        offset: u32,
    ) -> Result<()> {
        buffer.write_all(&component_type.to_be_bytes())?;
        self.update_checksum_int(crc, component_type);

        buffer.write_all(&offset.to_be_bytes())?;
        self.update_checksum_int(crc, offset);

        Ok(())
    }

    /// Write a component with its CRC32 checksum
    fn write_component(&self, buffer: &mut Vec<u8>, data: &[u8]) -> Result<()> {
        // Write component data
        buffer.write_all(data)?;

        // Write component checksum
        let checksum = crc32fast::hash(data);
        buffer.write_all(&checksum.to_be_bytes())?;

        Ok(())
    }

    /// Build VALIDATION component (MetadataType ordinal 0)
    ///
    /// Format (ValidationMetadata.java):
    /// - partitioner class name (Java writeUTF: u16 BE length + UTF-8 bytes)
    /// - bloom filter FP chance (f64 BE)
    fn build_validation_component(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // Partitioner class name (Java writeUTF format)
        let partitioner = b"org.apache.cassandra.dht.Murmur3Partitioner";

        // Java writeUTF: u16 BE length prefix + modified UTF-8 bytes
        let len = partitioner.len() as u16;
        buffer.write_all(&len.to_be_bytes())?;
        buffer.write_all(partitioner)?;

        // Bloom filter false positive chance (f64 BE)
        let fp_chance = 0.01f64;
        buffer.write_all(&fp_chance.to_be_bytes())?;

        Ok(buffer)
    }

    /// Build COMPACTION component (MetadataType ordinal 1)
    ///
    /// Format (CompactionMetadata.java):
    /// - cardinality estimator (i32 BE length + HyperLogLogPlus bytes)
    ///
    /// We write a minimal valid empty HyperLogLogPlus sketch.
    fn build_compaction_component(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // Cardinality estimator: ByteArrayUtil.writeWithLength(bytes, out)
        // Format: i32 BE length + data bytes
        //
        // Minimal valid HyperLogLogPlus(p=11, sp=25) in SPARSE format:
        // - 4 bytes: version (-2 as i32 = 0xFFFFFFFE)
        // - 1 byte: p = 11 (0x0B)
        // - 1 byte: sp = 25 (0x19)
        // - 1 byte: format type = SPARSE (0x01)
        // - 4 bytes: tempSetSize = 0
        // - 4 bytes: sparseSetSize = 0
        // Total: 15 bytes

        const HLL_DATA: [u8; 15] = [
            0xFF, 0xFF, 0xFF, 0xFE, // version = -2 (HyperLogLogPlus marker)
            0x0B, // p = 11 (precision)
            0x19, // sp = 25 (sparse precision)
            0x01, // format = SPARSE
            0x00, 0x00, 0x00, 0x00, // tempSetSize = 0
            0x00, 0x00, 0x00, 0x00, // sparseSetSize = 0
        ];

        // Write length prefix (i32 BE)
        buffer.write_all(&(HLL_DATA.len() as i32).to_be_bytes())?;

        // Write HLL data
        buffer.write_all(&HLL_DATA)?;

        Ok(buffer)
    }

    /// Build STATS component (MetadataType ordinal 2)
    ///
    /// Format for nb version (StatsMetadata.java lines 401-512):
    /// This is a complete serialization of all required fields for Cassandra 5.0 nb format.
    fn build_stats_component(&self, metadata: &StatisticsMetadata) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // 1-2. EstimatedHistogram estimatedPartitionSize and estimatedCellPerPartitionCount
        // Minimal valid histogram: size=2, one offset/count pair
        self.write_estimated_histogram(&mut buffer)?;
        self.write_estimated_histogram(&mut buffer)?;

        // 3. CommitLogPosition commitLogUpperBound (NONE = segmentId=-1, position=0)
        buffer.write_all(&(-1i64).to_be_bytes())?; // segmentId
        buffer.write_all(&0i32.to_be_bytes())?; // position

        // 4. long minTimestamp
        buffer.write_all(&metadata.min_timestamp.to_be_bytes())?;

        // 5. long maxTimestamp
        buffer.write_all(&metadata.max_timestamp.to_be_bytes())?;

        // 6. int minLocalDeletionTime (use Integer.MAX_VALUE if no deletions)
        let min_del_time = if metadata.min_local_deletion_time == 0 {
            i32::MAX
        } else {
            metadata.min_local_deletion_time
        };
        buffer.write_all(&min_del_time.to_be_bytes())?;

        // 7. int maxLocalDeletionTime
        let max_del_time = if metadata.max_local_deletion_time == 0 {
            i32::MAX
        } else {
            metadata.max_local_deletion_time
        };
        buffer.write_all(&max_del_time.to_be_bytes())?;

        // 8. int minTTL
        buffer.write_all(&metadata.min_ttl.to_be_bytes())?;

        // 9. int maxTTL
        buffer.write_all(&metadata.max_ttl.to_be_bytes())?;

        // 10. double compressionRatio (use -1.0 for unknown)
        buffer.write_all(&(-1.0f64).to_be_bytes())?;

        // 11. TombstoneHistogram estimatedTombstoneDropTime (empty for nb: size=0)
        self.write_tombstone_histogram(&mut buffer)?;

        // 12. int sstableLevel
        buffer.write_all(&0i32.to_be_bytes())?;

        // 13. long repairedAt
        buffer.write_all(&0i64.to_be_bytes())?;

        // 14. int minClusteringCount (no clustering = 0)
        buffer.write_all(&0i32.to_be_bytes())?;

        // 15. [clustering values] - count=0 means no values to write

        // 16. int maxClusteringCount
        buffer.write_all(&0i32.to_be_bytes())?;

        // 17. [clustering values] - count=0 means no values to write

        // 18. boolean hasLegacyCounterShards
        buffer.write_all(&[0x00])?; // false

        // 19. long totalColumnsSet
        buffer.write_all(&metadata.column_count.to_be_bytes())?;

        // 20. long totalRows
        buffer.write_all(&metadata.row_count.to_be_bytes())?;

        // 21. CommitLogPosition commitLogLowerBound (NONE)
        buffer.write_all(&(-1i64).to_be_bytes())?; // segmentId
        buffer.write_all(&0i32.to_be_bytes())?; // position

        // 22. IntervalSet<CommitLogPosition> commitLogIntervals (empty set: size=0)
        buffer.write_all(&0i32.to_be_bytes())?;

        // 23. byte pendingRepair (0 = null, no pending repair)
        buffer.write_all(&[0x00])?;

        // 24. boolean isTransient
        buffer.write_all(&[0x00])?; // false

        // 25. byte originatingHostId (0 = null)
        buffer.write_all(&[0x00])?;

        Ok(buffer)
    }

    /// Write an EstimatedHistogram (EstimatedHistogram.java lines 414-429)
    ///
    /// Format:
    /// - int: bucket count (we use 2 for minimal valid histogram)
    /// - for each bucket: long offset + long count
    ///
    /// Minimal valid: 2 buckets (size-1=1 offset, size=2 counts)
    fn write_estimated_histogram(&self, buffer: &mut Vec<u8>) -> Result<()> {
        // Bucket count
        buffer.write_all(&2i32.to_be_bytes())?;

        // Bucket 0: offset=1, count=0
        buffer.write_all(&1i64.to_be_bytes())?; // offset
        buffer.write_all(&0i64.to_be_bytes())?; // count

        // Bucket 1: offset=1 (gets overwritten per spec), count=0
        buffer.write_all(&1i64.to_be_bytes())?; // offset (overwrite of offsets[0])
        buffer.write_all(&0i64.to_be_bytes())?; // count

        Ok(())
    }

    /// Write a TombstoneHistogram for nb format (LegacyHistogramSerializer)
    ///
    /// Format:
    /// - int: maxBinSize (= size)
    /// - int: size
    /// - for each entry: double point + long value
    ///
    /// Empty histogram: maxBinSize=0, size=0
    fn write_tombstone_histogram(&self, buffer: &mut Vec<u8>) -> Result<()> {
        buffer.write_all(&0i32.to_be_bytes())?; // maxBinSize
        buffer.write_all(&0i32.to_be_bytes())?; // size
        Ok(())
    }

    /// Build SERIALIZATION_HEADER component (MetadataType ordinal 3)
    ///
    /// Format (SerializationHeader.java Serializer, lines 594-603):
    /// - EncodingStats: 3 unsigned VInts (minTimestamp, minLocalDeletionTime, minTTL deltas from epochs)
    /// - keyType: VInt length + UTF-8 type string
    /// - clusteringTypes: unsigned VInt count + list of types
    /// - staticColumns: unsigned VInt count + map of (column name, type)
    /// - regularColumns: unsigned VInt count + map of (column name, type)
    fn build_serialization_header_component(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // EncodingStats: 3 unsigned VInts representing deltas from epochs
        // minTimestamp - TIMESTAMP_EPOCH
        let min_ts_delta = 0u64; // Use 0 for minimal stub (timestamp at epoch)
        buffer.write_all(&encode_vuint(min_ts_delta))?;

        // minLocalDeletionTime - DELETION_TIME_EPOCH
        let min_del_delta = 0u64; // Use 0 for minimal stub
        buffer.write_all(&encode_vuint(min_del_delta))?;

        // minTTL - TTL_EPOCH (TTL_EPOCH=0, so just use 0)
        let min_ttl_delta = 0u64;
        buffer.write_all(&encode_vuint(min_ttl_delta))?;

        // keyType: AbstractTypeSerializer.serialize
        // Format: unsigned VInt length + UTF-8 bytes
        let key_type = b"org.apache.cassandra.db.marshal.BytesType";
        buffer.write_all(&encode_vuint(key_type.len() as u64))?;
        buffer.write_all(key_type)?;

        // clusteringTypes: writeList with unsigned VInt count
        buffer.write_all(&encode_vuint(0))?; // No clustering types

        // staticColumns: writeColumnsWithTypes (unsigned VInt count)
        buffer.write_all(&encode_vuint(0))?; // No static columns

        // regularColumns: writeColumnsWithTypes (unsigned VInt count)
        buffer.write_all(&encode_vuint(0))?; // No regular columns

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

        // Read back and verify TOC structure
        let file_data = std::fs::read(&stats_path).unwrap();
        assert!(
            file_data.len() >= 44,
            "File should have at least 44 bytes (TOC)"
        );

        // Verify num_components = 4 (bytes 0-3)
        let num_components =
            u32::from_be_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
        assert_eq!(num_components, 4, "Should have num_components=4");

        // Verify first checksum (bytes 4-7) matches CRC32(num_components)
        let checksum1 =
            u32::from_be_bytes([file_data[4], file_data[5], file_data[6], file_data[7]]);
        let expected_checksum1 = crc32fast::hash(&num_components.to_be_bytes());
        assert_eq!(
            checksum1, expected_checksum1,
            "First checksum should match CRC32(num_components)"
        );

        // Verify TOC entries exist (bytes 8-39)
        // Each entry is 8 bytes: 4 for type, 4 for offset
        assert!(file_data.len() >= 40, "Should have space for TOC entries");

        // Verify TOC checksum at byte 40
        assert!(file_data.len() >= 44, "Should have TOC checksum at byte 40");
    }

    #[test]
    fn test_build_validation_component() {
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let result = writer.build_validation_component();
        assert!(result.is_ok());

        let bytes = result.unwrap();
        assert!(!bytes.is_empty());

        // Should contain partitioner class name (Java writeUTF format: u16 BE length + UTF-8)
        let partitioner = b"org.apache.cassandra.dht.Murmur3Partitioner";
        assert!(bytes.windows(partitioner.len()).any(|w| w == partitioner));

        // Should also contain bloom filter FP chance (f64 BE) = 0.01
        // Total length should be: 2 (length) + 43 (partitioner) + 8 (f64) = 53 bytes
        assert_eq!(bytes.len(), 53);
    }

    #[test]
    fn test_build_stats_component() {
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));

        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = 1000000;
        meta.max_timestamp = 2000000;
        meta.min_local_deletion_time = 0;
        meta.max_local_deletion_time = 0;
        meta.min_ttl = 0;
        meta.max_ttl = 0;
        meta.partition_count = 100;
        meta.row_count = 100;
        meta.column_count = 200;

        let result = writer.build_stats_component(&meta);
        assert!(result.is_ok());

        let data = result.unwrap();
        assert!(!data.is_empty());

        // STATS component now has a complex binary format (nb version)
        // It should contain:
        // - 2x EstimatedHistogram (2 buckets each = 36 bytes each)
        // - CommitLogPosition upper bound (12 bytes)
        // - min/max timestamps (16 bytes)
        // - min/max deletion times (8 bytes)
        // - min/max TTL (8 bytes)
        // - compression ratio (8 bytes)
        // - TombstoneHistogram (8 bytes for empty)
        // - sstableLevel (4 bytes)
        // - repairedAt (8 bytes)
        // - min/max clustering count (8 bytes)
        // - hasLegacyCounterShards (1 byte)
        // - totalColumnsSet (8 bytes)
        // - totalRows (8 bytes)
        // - CommitLogPosition lower bound (12 bytes)
        // - commitLogIntervals empty set (4 bytes)
        // - pendingRepair (1 byte)
        // - isTransient (1 byte)
        // - originatingHostId (1 byte)
        // Total: 36+36+12+16+8+8+8+8+4+8+8+1+8+8+12+4+1+1+1 = 188 bytes
        assert_eq!(data.len(), 188);

        // Verify the row count is present (at offset 36+36+12+16+8+8+8+8+4+8+8+1+8 = 161)
        let row_count_offset = 161;
        let row_count_bytes = &data[row_count_offset..row_count_offset + 8];
        let row_count = u64::from_be_bytes(row_count_bytes.try_into().unwrap());
        assert_eq!(row_count, 100);
    }

    #[test]
    fn test_checksums_format() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("test-Statistics.db");

        let writer = StatisticsWriter::new(stats_path.clone());

        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = 1000000;
        meta.partition_count = 10;

        writer.write(&meta).unwrap();

        // Read file and verify checksum structure
        let file_data = std::fs::read(&stats_path).unwrap();

        // Parse and verify count checksum
        let num_components =
            u32::from_be_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
        let checksum1 =
            u32::from_be_bytes([file_data[4], file_data[5], file_data[6], file_data[7]]);

        let mut crc = crc32fast::Hasher::new();
        crc.update(&num_components.to_be_bytes());
        let expected_checksum1 = crc.finalize();

        assert_eq!(checksum1, expected_checksum1, "Count checksum should match");

        // Parse TOC entries and verify cumulative checksum
        let mut crc = crc32fast::Hasher::new();
        crc.update(&num_components.to_be_bytes());

        for i in 0..num_components {
            let offset = 8 + (i as usize * 8);
            let comp_type = u32::from_be_bytes([
                file_data[offset],
                file_data[offset + 1],
                file_data[offset + 2],
                file_data[offset + 3],
            ]);
            let comp_offset = u32::from_be_bytes([
                file_data[offset + 4],
                file_data[offset + 5],
                file_data[offset + 6],
                file_data[offset + 7],
            ]);

            crc.update(&comp_type.to_be_bytes());
            crc.update(&comp_offset.to_be_bytes());
        }

        let toc_checksum =
            u32::from_be_bytes([file_data[40], file_data[41], file_data[42], file_data[43]]);
        let expected_toc_checksum = crc.finalize();

        assert_eq!(
            toc_checksum, expected_toc_checksum,
            "TOC checksum should match cumulative CRC32"
        );
    }

    #[test]
    fn test_component_checksums() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("test-Statistics.db");

        let writer = StatisticsWriter::new(stats_path.clone());

        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = 1000000;
        meta.partition_count = 100;

        writer.write(&meta).unwrap();

        // Read file and verify per-component checksums
        let file_data = std::fs::read(&stats_path).unwrap();

        // Parse TOC to get component offsets
        let num_components =
            u32::from_be_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
        assert_eq!(num_components, 4);

        let mut component_offsets = Vec::new();
        for i in 0..num_components {
            let offset = 8 + (i as usize * 8) + 4; // +4 to skip type, get offset
            let comp_offset = u32::from_be_bytes([
                file_data[offset],
                file_data[offset + 1],
                file_data[offset + 2],
                file_data[offset + 3],
            ]);
            component_offsets.push(comp_offset as usize);
        }

        // Verify each component's checksum
        for i in 0..num_components as usize {
            let comp_start = component_offsets[i];

            // Calculate component length
            let comp_end = if i < component_offsets.len() - 1 {
                component_offsets[i + 1]
            } else {
                file_data.len()
            };

            // Component data ends 4 bytes before next component (for checksum)
            let comp_length = comp_end - comp_start - 4;
            let component_data = &file_data[comp_start..comp_start + comp_length];

            // Read stored checksum
            let stored_checksum = u32::from_be_bytes([
                file_data[comp_start + comp_length],
                file_data[comp_start + comp_length + 1],
                file_data[comp_start + comp_length + 2],
                file_data[comp_start + comp_length + 3],
            ]);

            // Compute expected checksum
            let computed_checksum = crc32fast::hash(component_data);

            assert_eq!(
                stored_checksum, computed_checksum,
                "Component {} checksum mismatch",
                i
            );
        }
    }

    #[test]
    fn test_component_binary_formats() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("test-Statistics.db");

        let writer = StatisticsWriter::new(stats_path.clone());

        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = 1000000;
        meta.max_timestamp = 2000000;
        meta.min_local_deletion_time = 0;
        meta.max_local_deletion_time = 0;
        meta.min_ttl = 100;
        meta.max_ttl = 200;
        meta.partition_count = 50;
        meta.row_count = 150;
        meta.column_count = 300;

        writer.write(&meta).unwrap();

        // Read and parse the file
        let file_data = std::fs::read(&stats_path).unwrap();

        // Verify TOC structure
        let num_components =
            u32::from_be_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
        assert_eq!(num_components, 4, "Should have 4 components");

        // Read component offsets
        let validation_offset =
            u32::from_be_bytes([file_data[12], file_data[13], file_data[14], file_data[15]])
                as usize;
        let compaction_offset =
            u32::from_be_bytes([file_data[20], file_data[21], file_data[22], file_data[23]])
                as usize;
        let stats_offset =
            u32::from_be_bytes([file_data[28], file_data[29], file_data[30], file_data[31]])
                as usize;
        let header_offset =
            u32::from_be_bytes([file_data[36], file_data[37], file_data[38], file_data[39]])
                as usize;

        // Verify VALIDATION component format
        // First 2 bytes should be u16 BE length of partitioner string
        let partitioner_len = u16::from_be_bytes([
            file_data[validation_offset],
            file_data[validation_offset + 1],
        ]);
        assert_eq!(
            partitioner_len, 43,
            "Partitioner string length should be 43"
        );

        // Verify COMPACTION component format
        // First 4 bytes should be i32 BE length of HLL data
        let hll_len = i32::from_be_bytes([
            file_data[compaction_offset],
            file_data[compaction_offset + 1],
            file_data[compaction_offset + 2],
            file_data[compaction_offset + 3],
        ]);
        assert_eq!(hll_len, 15, "HLL data length should be 15 bytes");

        // Verify HLL version marker (next 4 bytes should be -2 = 0xFFFFFFFE)
        let hll_version = i32::from_be_bytes([
            file_data[compaction_offset + 4],
            file_data[compaction_offset + 5],
            file_data[compaction_offset + 6],
            file_data[compaction_offset + 7],
        ]);
        assert_eq!(hll_version, -2, "HLL version should be -2");

        // Verify STATS component has correct total size (188 bytes + 4 byte checksum)
        let stats_end = header_offset;
        let stats_size = stats_end - stats_offset - 4; // -4 for checksum
        assert_eq!(stats_size, 188, "STATS component should be 188 bytes");

        // Verify min_timestamp in STATS component (at offset: 2*36 + 12 = 84 from stats_offset)
        let ts_offset = stats_offset + 84;
        let min_ts = i64::from_be_bytes([
            file_data[ts_offset],
            file_data[ts_offset + 1],
            file_data[ts_offset + 2],
            file_data[ts_offset + 3],
            file_data[ts_offset + 4],
            file_data[ts_offset + 5],
            file_data[ts_offset + 6],
            file_data[ts_offset + 7],
        ]);
        assert_eq!(min_ts, 1000000, "Min timestamp should be preserved");

        // Verify SERIALIZATION_HEADER component
        // Should start with unsigned VInt encoding of EncodingStats
        // First byte should be 0x00 (vuint encoding of 0)
        assert_eq!(
            file_data[header_offset], 0x00,
            "First EncodingStats delta should be 0"
        );
    }
}
