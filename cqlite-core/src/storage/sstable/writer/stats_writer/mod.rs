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
//!
//! # Module layout
//!
//! This writer is split by responsibility (epic #1116):
//! - [`metadata`]: `StatisticsMetadata` aggregation + `TombstoneHistogram`
//! - [`marshal`]: CQL type name → Cassandra marshal-type conversion
//! - [`components`]: VALIDATION / COMPACTION / STATS component builders
//! - [`serialization_header`]: the SERIALIZATION_HEADER builder
//! - this module: `StatisticsWriter` orchestration (TOC + `write`)

mod components;
mod marshal;
pub mod metadata;
mod serialization_header;

pub use metadata::{StatisticsMetadata, TombstoneHistogram};
// Re-exported so callers can keep using the pre-split path
// `...writer::stats_writer::cql_type_to_marshal_type` (e.g. data_writer.rs).
pub(crate) use marshal::cql_type_to_marshal_type;

use crate::error::{Error, Result};
use crate::schema::TableSchema;
use std::io::Write;
use std::path::PathBuf;

/// Epoch constants for EncodingStats (from Cassandra's EncodingStats.java)
/// These are used to compute deltas from a baseline for more compact encoding
const TIMESTAMP_EPOCH: i64 = 1442880000000000; // Sept 22, 2015 00:00:00 UTC in microseconds
const DELETION_TIME_EPOCH: i32 = 1442880000; // Sept 22, 2015 00:00:00 UTC in seconds
const TTL_EPOCH: i32 = 0; // TTL epoch is 0 (no offset)

/// Number of metadata components in Statistics.db
/// Cassandra 5.0 nb-format has 4 components: VALIDATION, COMPACTION, STATS, HEADER
const NUM_COMPONENTS: u32 = 4;

/// MetadataType ordinal values (from Cassandra's MetadataType.java enum)
const METADATA_TYPE_VALIDATION: u32 = 0;
const METADATA_TYPE_COMPACTION: u32 = 1;
const METADATA_TYPE_STATS: u32 = 2;
const METADATA_TYPE_SERIALIZATION_HEADER: u32 = 3;

/// Statistics.db component writer
///
/// Writes the Statistics.db file with metadata for SSTable delta encoding.
#[derive(Debug)]
pub struct StatisticsWriter {
    /// Path to the Statistics.db file to write
    path: PathBuf,
    /// Emit the Cassandra-canonical `da` (BtiFormat) `StatsMetadata` layout
    /// instead of the legacy `nb` layout.
    ///
    /// The `da` STATS component differs from `nb` in the fields gated by the
    /// BtiFormat version flags (all true for `da` except `hasLegacyMinMax`):
    /// `hasUIntDeletionTime`, `hasImprovedMinMax` (clusteringTypes + a covered
    /// `Slice` instead of legacy min/max value lists), `hasIsTransient`,
    /// `hasOriginatingHostId`, `hasPartitionLevelDeletionsPresenceMarker`,
    /// `hasKeyRange` (first/last key) and `hasTokenSpaceCoverage`. Cassandra's
    /// `sstabledump`/`sstablemetadata` deserialize a `da`-descriptor
    /// Statistics.db with this layout and reject the `nb` layout (a
    /// `Slice.<init>` assertion fires while reading `coveredClustering`).
    /// Authority: cassandra-5.0.0 `StatsMetadata.StatsMetadataSerializer` +
    /// `BtiFormat.BtiVersion` version flags.
    bti: bool,
}

impl StatisticsWriter {
    /// Create a new Statistics.db writer for the legacy `nb`/`oa` BIG layout.
    ///
    /// # Arguments
    /// * `path` - Path where Statistics.db will be written
    pub fn new(path: PathBuf) -> Self {
        Self { path, bti: false }
    }

    /// Create a new Statistics.db writer that emits the Cassandra-canonical `da`
    /// (BtiFormat) `StatsMetadata` layout.
    pub fn new_bti(path: PathBuf) -> Self {
        Self { path, bti: true }
    }

    /// Write Statistics.db file with the given metadata
    ///
    /// Generates a Cassandra 5.0 compatible Statistics.db file with full TOC structure:
    /// 1. TOC header with component count and checksums
    /// 2. VALIDATION component (validator class name)
    /// 3. COMPACTION component (minimal metadata)
    /// 4. STATS component (EncodingStats with baselines)
    /// 5. SERIALIZATION_HEADER component (schema-derived or minimal stub)
    ///
    /// Each component is followed by a CRC32 checksum for validation.
    ///
    /// # Arguments
    /// * `metadata` - Statistics metadata to write
    /// * `schema` - Optional table schema for populating serialization header
    ///
    /// # Returns
    /// `Ok(())` on success, or an error if writing fails
    pub fn write(&self, metadata: &StatisticsMetadata, schema: Option<&TableSchema>) -> Result<()> {
        let mut meta = metadata.clone();
        meta.finalize();

        // Build component data
        let validation_data = self.build_validation_component()?;
        let compaction_data = self.build_compaction_component()?;
        // The STATS body is version-gated: BTI (`da`) emits the BtiFormat
        // `StatsMetadata` layout (covered-clustering Slice, uint deletion times,
        // key range, token-space coverage); BIG (`nb`/`oa`) emits the legacy
        // layout. `schema` is needed for the `da` clustering-type list.
        let stats_data = if self.bti {
            self.build_stats_component_da(&meta, schema)?
        } else {
            self.build_stats_component(&meta)?
        };
        // Use pre-finalize metadata for the SerializationHeader EncodingStats.
        // The baselines in the header MUST match those used by the DataWriter for
        // delta encoding. The DataWriter uses the raw (pre-finalize) metadata values.
        let header_data = self.build_serialization_header_component(schema, metadata)?;

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
}

#[cfg(test)]
mod tests {
    use super::metadata::TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE;
    use super::*;
    use tempfile::TempDir;

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

        let result = writer.write(&meta, None);
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
    fn test_checksums_format() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("test-Statistics.db");

        let writer = StatisticsWriter::new(stats_path.clone());

        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = 1000000;
        meta.partition_count = 10;

        writer.write(&meta, None).unwrap();

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

        writer.write(&meta, None).unwrap();

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
        // Use realistic values at or above epoch baselines to avoid wrapping
        // in EncodingStats delta encoding (TIMESTAMP_EPOCH = 1442880000000000,
        // DELETION_TIME_EPOCH = 1442880000, TTL_EPOCH = 0).
        meta.min_timestamp = TIMESTAMP_EPOCH;
        meta.max_timestamp = TIMESTAMP_EPOCH + 1000000;
        meta.min_local_deletion_time = DELETION_TIME_EPOCH;
        meta.max_local_deletion_time = DELETION_TIME_EPOCH + 100;
        meta.min_ttl = 0;
        meta.max_ttl = 200;
        meta.partition_count = 50;
        meta.row_count = 150;
        meta.column_count = 300;

        writer.write(&meta, None).unwrap();

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
        assert_eq!(min_ts, TIMESTAMP_EPOCH, "Min timestamp should be preserved");

        // Verify SERIALIZATION_HEADER component
        // Should start with 3 unsigned VInts for EncodingStats deltas.
        // All metadata values == their epoch baselines, so all deltas are 0.
        // encode_vuint(0) = [0x00].
        assert_eq!(
            file_data[header_offset], 0x00,
            "EncodingStats minTimestamp delta should be 0"
        );
        assert_eq!(
            file_data[header_offset + 1],
            0x00,
            "EncodingStats minLocalDeletionTime delta should be 0"
        );
        assert_eq!(
            file_data[header_offset + 2],
            0x00,
            "EncodingStats minTTL delta should be 0"
        );
    }

    /// Verify that Statistics.db written for a table WITH tombstones produces a
    /// non-empty `estimatedTombstoneDropTime` histogram in the STATS component.
    ///
    /// This is the primary acceptance-criterion test for issue #730.
    #[test]
    fn test_statistics_db_tombstone_histogram_nonempty() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("tombstone-Statistics.db");
        let writer = StatisticsWriter::new(stats_path.clone());

        // Simulate two tombstone local-deletion-times
        let ldt1 = 1_700_000_000i32;
        let ldt2 = 1_700_100_000i32;

        let mut meta = StatisticsMetadata::new();
        meta.update_timestamp(1_600_000_000_000_000); // some live write
        meta.update_local_deletion_time(ldt1);
        meta.update_local_deletion_time(ldt2);
        meta.partition_count = 1;
        meta.row_count = 2;
        meta.column_count = 4;

        writer.write(&meta, None).expect("write should succeed");

        // ---- Parse the resulting Statistics.db and find the histogram ----
        let file_data = std::fs::read(&stats_path).expect("file should exist");

        // The STATS component offset is stored in the TOC at bytes 28–31
        // (3rd entry, 4-byte type + 4-byte offset = 8 bytes each, starting at byte 8;
        //  entries: [8..16] VALIDATION, [16..24] COMPACTION, [24..32] STATS, [32..40] HEADER)
        let stats_offset =
            u32::from_be_bytes([file_data[28], file_data[29], file_data[30], file_data[31]])
                as usize;

        // Within the STATS component, the histogram field starts after:
        //   2 × EstimatedHistogram  = 2 × 36 = 72 bytes
        //   CommitLogPosition upper = 12 bytes
        //   minTimestamp (i64)      =  8 bytes
        //   maxTimestamp (i64)      =  8 bytes
        //   minLocalDeletionTime    =  4 bytes
        //   maxLocalDeletionTime    =  4 bytes
        //   minTTL                  =  4 bytes
        //   maxTTL                  =  4 bytes
        //   compressionRatio (f64)  =  8 bytes
        // = 72 + 12 + 8 + 8 + 4 + 4 + 4 + 4 + 8 = 124 bytes
        let histogram_offset = stats_offset + 124;

        // Read maxBinSize and size
        let max_bin_size = i32::from_be_bytes(
            file_data[histogram_offset..histogram_offset + 4]
                .try_into()
                .expect("histogram maxBinSize bytes"),
        );
        let histo_size = i32::from_be_bytes(
            file_data[histogram_offset + 4..histogram_offset + 8]
                .try_into()
                .expect("histogram size bytes"),
        );

        assert_eq!(
            max_bin_size,
            TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE,
            "estimatedTombstoneDropTime maxBinSize should be {} for a non-empty histogram (issue #730)",
            TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE
        );
        assert_eq!(
            histo_size, 2,
            "estimatedTombstoneDropTime should have 2 bins for 2 distinct deletion times (issue #730)"
        );

        // Verify the first bin's point matches ldt1
        let point0 = f64::from_be_bytes(
            file_data[histogram_offset + 8..histogram_offset + 16]
                .try_into()
                .expect("bin0 point bytes"),
        );
        assert_eq!(
            point0, ldt1 as f64,
            "first histogram bin point should match ldt1"
        );

        // Verify the second bin's point matches ldt2
        let point1 = f64::from_be_bytes(
            file_data[histogram_offset + 24..histogram_offset + 32]
                .try_into()
                .expect("bin1 point bytes"),
        );
        assert_eq!(
            point1, ldt2 as f64,
            "second histogram bin point should match ldt2"
        );

        // Verify both bins have count = 1
        let value0 = i64::from_be_bytes(
            file_data[histogram_offset + 16..histogram_offset + 24]
                .try_into()
                .expect("bin0 value bytes"),
        );
        let value1 = i64::from_be_bytes(
            file_data[histogram_offset + 32..histogram_offset + 40]
                .try_into()
                .expect("bin1 value bytes"),
        );
        assert_eq!(value0, 1, "first bin count should be 1");
        assert_eq!(value1, 1, "second bin count should be 1");
    }
}
