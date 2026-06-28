//! VALIDATION / COMPACTION / STATS component builders for Statistics.db,
//! plus the EstimatedHistogram and TombstoneHistogram serialisers.
//!
//! The SERIALIZATION_HEADER component lives in `serialization_header.rs`.

use super::marshal::cql_type_to_marshal_type;
use super::metadata::{StatisticsMetadata, TombstoneHistogram, TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE};
use super::StatisticsWriter;
use crate::error::Result;
use crate::parser::vint::encode_vuint;
use crate::schema::TableSchema;
use std::io::Write;

impl StatisticsWriter {
    /// Build VALIDATION component (MetadataType ordinal 0)
    ///
    /// Format (ValidationMetadata.java):
    /// - partitioner class name (Java writeUTF: u16 BE length + UTF-8 bytes)
    /// - bloom filter FP chance (f64 BE)
    pub(super) fn build_validation_component(&self) -> Result<Vec<u8>> {
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
    pub(super) fn build_compaction_component(&self) -> Result<Vec<u8>> {
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
    pub(super) fn build_stats_component(&self, metadata: &StatisticsMetadata) -> Result<Vec<u8>> {
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

        // 11. TombstoneHistogram estimatedTombstoneDropTime
        // Populated from tombstone local-deletion-times accumulated during the write path.
        // Legacy serializer (nb format): maxBinSize (i32), size (i32),
        // then size × (f64 point, i64 value).
        self.write_tombstone_histogram(&mut buffer, &metadata.tombstone_histogram)?;

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

    /// Build the STATS component for the Cassandra-canonical `da` (BtiFormat)
    /// layout.
    ///
    /// Field order and gating follow cassandra-5.0.0
    /// `StatsMetadata.StatsMetadataSerializer.serialize` evaluated for
    /// `BtiFormat.BtiVersion` (all version flags `true` except `hasLegacyMinMax`,
    /// which is `false`):
    ///
    /// 1.  estimatedPartitionSize (EstimatedHistogram)
    /// 2.  estimatedCellPerPartitionCount (EstimatedHistogram)
    /// 3.  commitLogUpperBound (CommitLogPosition)
    /// 4.  minTimestamp, maxTimestamp (long)
    /// 5.  min/maxLocalDeletionTime as **unsigned int** (`hasUIntDeletionTime`);
    ///     `NO_DELETION_TIME` (Long.MAX) maps to `0xFFFFFFFF`.
    /// 6.  minTTL, maxTTL (int)
    /// 7.  compressionRatio (double)
    /// 8.  estimatedTombstoneDropTime (TombstoneHistogram)
    /// 9.  sstableLevel (int), repairedAt (long)
    /// 10. improvedMinMax (`!hasLegacyMinMax && hasImprovedMinMax`):
    ///     clusteringTypes list + coveredClustering `Slice`. We emit
    ///     `Slice.ALL` (BOTTOM..TOP) — a valid, conservative covering slice that
    ///     Cassandra accepts and that matches the `Covered clusterings: [, ]`
    ///     shown by `sstablemetadata` on the real `da` fixtures.
    /// 11. hasLegacyCounterShards (bool)
    /// 12. totalColumnsSet, totalRows (long)
    /// 13. commitLogLowerBound (CommitLogPosition), commitLogIntervals (IntervalSet)
    /// 14. pendingRepair (byte 0 = null)
    /// 15. isTransient (bool)
    /// 16. originatingHostId (byte 0 = null)
    /// 17. hasPartitionLevelDeletions (bool)
    /// 18. firstKey, lastKey (vint-length ByteBuffer) — `hasKeyRange`
    /// 19. tokenSpaceCoverage (double) — `hasTokenSpaceCoverage`
    pub(super) fn build_stats_component_da(
        &self,
        metadata: &StatisticsMetadata,
        schema: Option<&TableSchema>,
    ) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // 1-2. EstimatedHistogram estimatedPartitionSize / estimatedCellPerPartitionCount
        self.write_estimated_histogram(&mut buffer)?;
        self.write_estimated_histogram(&mut buffer)?;

        // 3. CommitLogPosition commitLogUpperBound (NONE)
        buffer.write_all(&(-1i64).to_be_bytes())?; // segmentId
        buffer.write_all(&0i32.to_be_bytes())?; // position

        // 4. minTimestamp, maxTimestamp
        buffer.write_all(&metadata.min_timestamp.to_be_bytes())?;
        buffer.write_all(&metadata.max_timestamp.to_be_bytes())?;

        // 5. min/maxLocalDeletionTime — unsigned int (hasUIntDeletionTime).
        // No tombstones (sentinel) → NO_DELETION_TIME_UNSIGNED_INTEGER = 0xFFFFFFFF;
        // otherwise the low 32 bits of the (seconds-since-epoch) deletion time
        // (Cassandra `CassandraUInt.fromLong` == `(int) value`).
        let min_ldt_uint: u32 = if metadata.min_local_deletion_time == 0 {
            0xFFFF_FFFF
        } else {
            metadata.min_local_deletion_time as u32
        };
        let max_ldt_uint: u32 = if metadata.max_local_deletion_time == 0 {
            0xFFFF_FFFF
        } else {
            metadata.max_local_deletion_time as u32
        };
        buffer.write_all(&min_ldt_uint.to_be_bytes())?;
        buffer.write_all(&max_ldt_uint.to_be_bytes())?;

        // 6. minTTL, maxTTL
        buffer.write_all(&metadata.min_ttl.to_be_bytes())?;
        buffer.write_all(&metadata.max_ttl.to_be_bytes())?;

        // 7. compressionRatio (-1.0 = unknown)
        buffer.write_all(&(-1.0f64).to_be_bytes())?;

        // 8. TombstoneHistogram estimatedTombstoneDropTime.
        //
        // The `da` (BtiFormat) version resolves `TombstoneHistogram.getSerializer`
        // to the modern `HistogramSerializer` (long point + int value per bin),
        // NOT the legacy serializer (double + long) used by older versions. Using
        // the legacy entry encoding here mis-sizes the body and derails the
        // subsequent `coveredClustering` Slice deserialization in Cassandra.
        self.write_tombstone_histogram_modern(&mut buffer, &metadata.tombstone_histogram)?;

        // 9. sstableLevel, repairedAt
        buffer.write_all(&0i32.to_be_bytes())?;
        buffer.write_all(&0i64.to_be_bytes())?;

        // 10. improvedMinMax: clusteringTypes list + coveredClustering Slice.
        //
        // AbstractTypeSerializer.serializeList: unsigned-VInt count, then each
        // type as an unsigned-VInt-length-prefixed UTF-8 marshal-class string —
        // the exact encoding used for the SERIALIZATION_HEADER clusteringTypes.
        let clustering_types: Vec<String> = schema
            .map(|s| {
                s.clustering_keys
                    .iter()
                    .map(|ck| cql_type_to_marshal_type(&ck.data_type))
                    .collect()
            })
            .unwrap_or_default();
        buffer.write_all(&encode_vuint(clustering_types.len() as u64))?;
        for ty in &clustering_types {
            buffer.write_all(&encode_vuint(ty.len() as u64))?;
            buffer.write_all(ty.as_bytes())?;
        }
        // coveredClustering = Slice.ALL = (BOTTOM, TOP). A ClusteringBound
        // serialises as `[byte kind ordinal][short size][values...]`; BOTTOM and
        // TOP are empty bounds (size 0). Kind ordinals (ClusteringPrefix.Kind):
        // INCL_START_BOUND = 1 (BOTTOM), INCL_END_BOUND = 6 (TOP).
        const KIND_INCL_START_BOUND: u8 = 1;
        const KIND_INCL_END_BOUND: u8 = 6;
        buffer.write_all(&[KIND_INCL_START_BOUND])?;
        buffer.write_all(&0u16.to_be_bytes())?; // start size = 0
        buffer.write_all(&[KIND_INCL_END_BOUND])?;
        buffer.write_all(&0u16.to_be_bytes())?; // end size = 0

        // 11. hasLegacyCounterShards
        buffer.write_all(&[0x00])?;

        // 12. totalColumnsSet, totalRows
        buffer.write_all(&metadata.column_count.to_be_bytes())?;
        buffer.write_all(&metadata.row_count.to_be_bytes())?;

        // 13. commitLogLowerBound (NONE) + commitLogIntervals (empty IntervalSet)
        buffer.write_all(&(-1i64).to_be_bytes())?; // segmentId
        buffer.write_all(&0i32.to_be_bytes())?; // position
        buffer.write_all(&0i32.to_be_bytes())?; // interval set size = 0

        // 14. pendingRepair (null)
        buffer.write_all(&[0x00])?;

        // 15. isTransient
        buffer.write_all(&[0x00])?;

        // 16. originatingHostId (null)
        buffer.write_all(&[0x00])?;

        // 17. hasPartitionLevelDeletions
        //
        // cassandra-5.0.0 StatsMetadata.StatsMetadataSerializer.serialize line 495:
        //   out.writeBoolean(component.hasPartitionLevelDeletions)
        // DataOutput.writeBoolean emits a single byte: 0x01 for true, 0x00 false.
        // Set when any partition written carries a partition-level tombstone.
        let has_partition_deletions = if metadata.has_partition_level_deletions {
            0x01u8
        } else {
            0x00u8
        };
        buffer.write_all(&[has_partition_deletions])?;

        // 18. firstKey, lastKey (ByteBufferUtil.writeWithVIntLength:
        // unsigned-VInt length then the raw key bytes). Empty when no partitions.
        let first = metadata.first_key.as_deref().unwrap_or(&[]);
        let last = metadata.last_key.as_deref().unwrap_or(&[]);
        buffer.write_all(&encode_vuint(first.len() as u64))?;
        buffer.write_all(first)?;
        buffer.write_all(&encode_vuint(last.len() as u64))?;
        buffer.write_all(last)?;

        // 19. tokenSpaceCoverage (double). Cassandra writes NaN when not computed
        // (sstablemetadata renders this as "Local token space coverage: NaN").
        buffer.write_all(&f64::NAN.to_be_bytes())?;

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

    /// Serialise a `TombstoneHistogram` using Cassandra's legacy format (nb/mc).
    ///
    /// Binary layout (matches `TombstoneHistogram.LegacyHistogramSerializer`):
    /// ```text
    /// i32 BE  maxBinSize  — capacity: 100 when non-empty, 0 when empty
    /// i32 BE  size        — actual number of populated bins
    /// for each of the `size` bins (ascending point order):
    ///   f64 BE  point  — local-deletion-time bucket centre (as double)
    ///   i64 BE  value  — tombstone count for this bucket
    /// ```
    ///
    /// An empty histogram writes 8 bytes (`maxBinSize=0, size=0`), matching
    /// what Cassandra writes for SSTables with no tombstones.
    pub(super) fn write_tombstone_histogram(
        &self,
        buffer: &mut Vec<u8>,
        histogram: &TombstoneHistogram,
    ) -> Result<()> {
        if histogram.is_empty() {
            // Empty: maxBinSize=0, size=0 — no entry pairs follow
            buffer.write_all(&0i32.to_be_bytes())?; // maxBinSize
            buffer.write_all(&0i32.to_be_bytes())?; // size
        } else {
            // Non-empty: maxBinSize=100 (the capacity constant), then the bins
            buffer.write_all(&TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE.to_be_bytes())?; // maxBinSize
            buffer.write_all(&histogram.size().to_be_bytes())?; // size
            for (point, value) in histogram.entries() {
                buffer.write_all(&point.to_be_bytes())?; // f64 BE point
                buffer.write_all(&value.to_be_bytes())?; // i64 BE value
            }
        }
        Ok(())
    }

    /// Serialise a `TombstoneHistogram` using Cassandra's modern (`oa`/`da`)
    /// `HistogramSerializer` format.
    ///
    /// Identical framing to the legacy serializer (`i32 maxBinSize`, `i32 size`)
    /// but each bin entry is `i64 BE point` + `i32 BE value` (12 bytes), where
    /// the legacy serializer used `f64 point` + `i64 value` (16 bytes). The
    /// version split: `StatsMetadata.StatsMetadataSerializer` resolves
    /// `TombstoneHistogram.getSerializer(version)` to the modern
    /// `HistogramSerializer` for `oa`+ (incl. `BtiFormat.BtiVersion` `da`) and
    /// the `LegacyHistogramSerializer` for older versions.
    ///
    /// Authority: repo `docs/sstables-definitive-guide/statistics-db-writer-spec.md`
    /// (TombstoneHistogram legacy vs modern note) and
    /// cassandra-5.0.0 `TombstoneHistogram.java` (`HistogramSerializer` ->
    /// `long` point + `int` value).
    ///
    /// ```text
    /// i32 BE  maxBinSize  — capacity: 100 when non-empty, 0 when empty
    /// i32 BE  size        — actual number of populated bins
    /// for each of the `size` bins (ascending point order):
    ///   i64 BE  point  — local-deletion-time bucket centre (as long)
    ///   i32 BE  value  — tombstone count for this bucket
    /// ```
    fn write_tombstone_histogram_modern(
        &self,
        buffer: &mut Vec<u8>,
        histogram: &TombstoneHistogram,
    ) -> Result<()> {
        if histogram.is_empty() {
            buffer.write_all(&0i32.to_be_bytes())?; // maxBinSize
            buffer.write_all(&0i32.to_be_bytes())?; // size
        } else {
            buffer.write_all(&TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE.to_be_bytes())?; // maxBinSize
            buffer.write_all(&histogram.size().to_be_bytes())?; // size
            for (point, value) in histogram.entries() {
                // Modern: long point (truncate the double bucket centre) + int value.
                buffer.write_all(&(point as i64).to_be_bytes())?; // i64 BE point
                buffer.write_all(&(value as i32).to_be_bytes())?; // i32 BE value
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sstable::writer::stats_writer::metadata::TombstoneHistogram;
    use std::path::PathBuf;

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

    /// The `da` STATS body must serialise `hasPartitionLevelDeletions` as a
    /// single `writeBoolean` byte: `0x01` when the SSTable contains a
    /// partition-level deletion, `0x00` otherwise. Authority: cassandra-5.0.0
    /// `StatsMetadata.StatsMetadataSerializer.serialize` line 495
    /// (`out.writeBoolean(component.hasPartitionLevelDeletions)`), gated by
    /// `version.hasPartitionLevelDeletionsPresenceMarker()` which is `true` for
    /// `BtiFormat.BtiVersion`.
    #[test]
    fn test_da_stats_has_partition_level_deletions_marker_byte() {
        let writer = StatisticsWriter::new_bti(PathBuf::from("da-1-bti-Statistics.db"));

        // Identical metadata except for the partition-level-deletion flag, so
        // the only differing byte is the hasPartitionLevelDeletions marker.
        let mut base = StatisticsMetadata::new();
        base.min_timestamp = 1_000_000;
        base.max_timestamp = 2_000_000;
        base.min_local_deletion_time = 0;
        base.max_local_deletion_time = 0;
        base.partition_count = 1;
        base.row_count = 1;
        base.column_count = 1;
        base.finalize();

        let mut with_del = base.clone();
        with_del.mark_partition_level_deletion();
        assert!(with_del.has_partition_level_deletions);
        assert!(!base.has_partition_level_deletions);

        let no_del_bytes = writer.build_stats_component_da(&base, None).unwrap();
        let del_bytes = writer.build_stats_component_da(&with_del, None).unwrap();

        // Same length: only the one marker byte changes value.
        assert_eq!(
            no_del_bytes.len(),
            del_bytes.len(),
            "the flag must not change the serialized length"
        );

        // Exactly one byte differs, and it flips 0x00 <-> 0x01.
        let diffs: Vec<usize> = no_del_bytes
            .iter()
            .zip(del_bytes.iter())
            .enumerate()
            .filter(|(_, (a, b))| a != b)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            diffs.len(),
            1,
            "only the hasPartitionLevelDeletions marker byte should differ"
        );
        let marker = diffs[0];
        assert_eq!(
            no_del_bytes[marker], 0x00,
            "no partition deletion => marker byte 0x00"
        );
        assert_eq!(
            del_bytes[marker], 0x01,
            "partition deletion present => marker byte 0x01"
        );
    }

    // -----------------------------------------------------------------------
    // TombstoneHistogram serialisation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_tombstone_histogram_empty() {
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let h = TombstoneHistogram::new();
        let mut buf = Vec::new();
        writer.write_tombstone_histogram(&mut buf, &h).unwrap();

        // Empty: 4 bytes maxBinSize=0 + 4 bytes size=0 = 8 bytes total
        assert_eq!(buf.len(), 8);
        let max_bin_size = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        let size = i32::from_be_bytes(buf[4..8].try_into().unwrap());
        assert_eq!(max_bin_size, 0, "empty histogram maxBinSize should be 0");
        assert_eq!(size, 0, "empty histogram size should be 0");
    }

    #[test]
    fn test_write_tombstone_histogram_nonempty() {
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let mut h = TombstoneHistogram::new();
        h.update(1_700_000_000);
        h.update(1_700_000_100);
        let mut buf = Vec::new();
        writer.write_tombstone_histogram(&mut buf, &h).unwrap();

        // Non-empty with 2 bins:
        //   4 bytes maxBinSize=100
        //   4 bytes size=2
        //   2 × (8 f64 + 8 i64) = 32 bytes
        // Total = 40 bytes
        assert_eq!(buf.len(), 40, "2-bin histogram should be 40 bytes");

        let max_bin_size = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        let size = i32::from_be_bytes(buf[4..8].try_into().unwrap());
        assert_eq!(
            max_bin_size, 100,
            "non-empty histogram maxBinSize should be 100"
        );
        assert_eq!(size, 2, "histogram size should be 2");

        // Verify first bin point (should be 1_700_000_000.0)
        let point0 = f64::from_be_bytes(buf[8..16].try_into().unwrap());
        assert_eq!(point0, 1_700_000_000.0f64);
        let value0 = i64::from_be_bytes(buf[16..24].try_into().unwrap());
        assert_eq!(value0, 1);

        // Verify second bin point (should be 1_700_000_100.0)
        let point1 = f64::from_be_bytes(buf[24..32].try_into().unwrap());
        assert_eq!(point1, 1_700_000_100.0f64);
        let value1 = i64::from_be_bytes(buf[32..40].try_into().unwrap());
        assert_eq!(value1, 1);
    }
}
