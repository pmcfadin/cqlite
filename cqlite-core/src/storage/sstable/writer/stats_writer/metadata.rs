//! Statistics metadata collected during memtable flush.
//!
//! Holds the per-SSTable aggregates (timestamp/TTL/deletion-time ranges, counts,
//! key range, tombstone drop-time histogram) consumed by the Statistics.db
//! component builders.

use super::estimated_histogram::EstimatedHistogram;
use std::collections::BTreeMap;

/// Maximum number of bins in the tombstone drop-time histogram.
///
/// Mirrors Cassandra's `StreamingTombstoneHistogramBuilder.MAX_BIN_SIZE = 100`.
pub(super) const TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE: i32 = 100;

/// Streaming tombstone drop-time histogram.
///
/// Mirrors Cassandra's `StreamingTombstoneHistogramBuilder` with a fixed maximum
/// of 100 bins. Tombstone local-deletion-times are accumulated; when the bin count
/// exceeds the maximum the two nearest bins are merged, keeping the bin count at or
/// below `MAX_BIN_SIZE`.
///
/// Serialisation uses the **legacy** (nb/mc) format:
/// ```text
/// i32 BE  maxBinSize  (100 when non-empty, 0 when empty)
/// i32 BE  size        (actual number of bins)
/// for each bin:
///   f64 BE  point  (local-deletion-time as a double)
///   i64 BE  value  (count of tombstones in this bin)
/// ```
#[derive(Debug, Clone, Default)]
pub struct TombstoneHistogram {
    /// Bins keyed by `local_deletion_time as i64` (for stable ordering).
    /// Each value is the count of tombstones that map to this bin.
    bins: BTreeMap<i64, i64>,
}

impl TombstoneHistogram {
    /// Create an empty histogram.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a tombstone with the given local-deletion-time (Unix seconds).
    ///
    /// If adding the new point would exceed `MAX_BIN_SIZE` the two closest
    /// existing bins are merged first, exactly as Cassandra's builder does.
    pub fn update(&mut self, local_deletion_time: i32) {
        let key = local_deletion_time as i64;
        *self.bins.entry(key).or_insert(0) += 1;

        // Merge bins while over capacity
        while self.bins.len() as i32 > TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE {
            self.merge_closest_bins();
        }
    }

    /// Return `true` if no tombstone deletion times have been recorded.
    pub fn is_empty(&self) -> bool {
        self.bins.is_empty()
    }

    /// Return the number of populated bins.
    pub fn size(&self) -> i32 {
        self.bins.len() as i32
    }

    /// Merge the two bins whose keys are closest together.
    ///
    /// The merged bin's key is the weighted average of the two points;
    /// its value is the sum of both counts.  This matches Cassandra's
    /// `mergeNearestBins` approach.
    fn merge_closest_bins(&mut self) {
        if self.bins.len() < 2 {
            return;
        }

        // Find the pair of adjacent keys with the smallest gap.
        let keys: Vec<i64> = self.bins.keys().copied().collect();
        let mut min_gap = i64::MAX;
        let mut merge_idx = 0usize;
        for i in 0..keys.len() - 1 {
            let gap = keys[i + 1] - keys[i];
            if gap < min_gap {
                min_gap = gap;
                merge_idx = i;
            }
        }

        let k1 = keys[merge_idx];
        let k2 = keys[merge_idx + 1];
        let v1 = self.bins.remove(&k1).unwrap_or(0);
        let v2 = self.bins.remove(&k2).unwrap_or(0);
        let total = v1 + v2;
        // Weighted average key (same formula as Cassandra)
        let merged_key = if total == 0 {
            (k1 + k2) / 2
        } else {
            (k1 * v1 + k2 * v2) / total
        };
        *self.bins.entry(merged_key).or_insert(0) += total;
    }

    /// Iterate over bins in ascending key order as `(point_as_f64, count)` pairs.
    pub(super) fn entries(&self) -> impl Iterator<Item = (f64, i64)> + '_ {
        self.bins.iter().map(|(&k, &v)| (k as f64, v))
    }
}

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
    /// Histogram of tombstone local-deletion-times for `estimatedTombstoneDropTime`.
    ///
    /// Populated by `update_local_deletion_time`; serialised into Statistics.db
    /// so Cassandra can compute `estimatedDroppableTombstoneRatio` and schedule
    /// tombstone compaction.  Uses the same streaming builder algorithm as
    /// Cassandra's `StreamingTombstoneHistogramBuilder` (max 100 bins).
    pub tombstone_histogram: TombstoneHistogram,
    /// First (lowest-token) decorated partition-key bytes written to this
    /// SSTable, or `None` if no partition has been written. Serialised as the
    /// `da`-format `StatsMetadata.firstKey` (BtiFormat `hasKeyRange`); tracked in
    /// token order by `SSTableWriter::write_partition`.
    pub first_key: Option<Vec<u8>>,
    /// Last (highest-token) decorated partition-key bytes written to this
    /// SSTable. Serialised as the `da`-format `StatsMetadata.lastKey`.
    pub last_key: Option<Vec<u8>>,
    /// Whether any partition written to this SSTable carries a partition-level
    /// deletion (partition tombstone).
    ///
    /// Serialised as the `da`-format `StatsMetadata.hasPartitionLevelDeletions`
    /// boolean (`hasPartitionLevelDeletionsPresenceMarker`); written as a single
    /// `writeBoolean` byte (`0x01` true, `0x00` false) — matching
    /// cassandra-5.0.0 `StatsMetadata.StatsMetadataSerializer.serialize`
    /// (line 495). Set by `SSTableWriter::write_partition` whenever a mutation
    /// contributes a `partition_tombstone`. Ignored by the legacy BIG (nb/oa)
    /// STATS body, which never serialises this field.
    pub has_partition_level_deletions: bool,

    /// `repairedAt` repair timestamp (`0` = unrepaired). Serialised verbatim
    /// into the STATS component `repairedAt` field. Preserved through compaction
    /// from the (compatible) input SSTables (issue #1021); a fresh memtable flush
    /// leaves it `0`.
    pub repaired_at: i64,

    /// `pendingRepair` incremental-repair session UUID (`None` = no pending
    /// repair). Serialised as the STATS `pendingRepair` nullable field (presence
    /// byte then 16-byte UUID). Preserved through compaction from compatible
    /// inputs (issue #1021); a fresh flush leaves it `None`.
    pub pending_repair: Option<[u8; 16]>,

    /// `isTransient` flag (transiently-replicated data). Serialised as the STATS
    /// `isTransient` boolean. Preserved through compaction from compatible inputs
    /// (issue #1021); a fresh flush leaves it `false`.
    pub is_transient: bool,

    /// `estimatedPartitionSize` EstimatedHistogram — one observation per
    /// partition of its serialized Data.db size in bytes. Serialised as the FIRST
    /// STATS field; the authoritative partition-count decode (`read_table_counts`,
    /// issue #944) sums its bucket counts, so `Σ counts == partition_count`
    /// (issue #1327). Populated by [`Self::record_partition`].
    pub estimated_partition_size: EstimatedHistogram,

    /// `estimatedCellPerPartitionCount` EstimatedHistogram — one observation per
    /// partition of its cell count. Serialised as the SECOND STATS field.
    /// Populated by [`Self::record_partition`].
    pub estimated_cell_count: EstimatedHistogram,
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
            tombstone_histogram: TombstoneHistogram::new(),
            first_key: None,
            last_key: None,
            has_partition_level_deletions: false,
            repaired_at: 0,
            pending_repair: None,
            is_transient: false,
            estimated_partition_size: EstimatedHistogram::new(),
            estimated_cell_count: EstimatedHistogram::new(),
        }
    }
}

impl StatisticsMetadata {
    /// Create a new empty statistics metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Update timestamp range with a new timestamp value.
    ///
    /// LIVE deletion markers must never enter the min/max aggregates (issue
    /// #851, Cassandra `d5bc7fb5`). Cassandra encodes `DeletionTime.LIVE` with
    /// `markedForDeleteAt = Long.MIN_VALUE`, and a `NO_DELETION` / absent
    /// liveness timestamp surfaces as `Long.MAX_VALUE`. Folding either sentinel
    /// into `minTimestamp` / `maxTimestamp` would poison the stats, so they are
    /// skipped here at the single aggregation chokepoint.
    pub fn update_timestamp(&mut self, timestamp: i64) {
        if Self::is_live_timestamp(timestamp) {
            return;
        }
        self.min_timestamp = self.min_timestamp.min(timestamp);
        self.max_timestamp = self.max_timestamp.max(timestamp);
    }

    /// Update local deletion time range (for tombstones) and the drop-time histogram.
    ///
    /// Call this for every tombstone local-deletion-time encountered while writing:
    /// cell tombstones, row deletions, range tombstones, and partition tombstones.
    ///
    /// A LIVE marker (`DeletionTime.LIVE.localDeletionTime == Integer.MAX_VALUE`,
    /// the same value as `Cell.NO_DELETION_TIME`) is not a tombstone: it must not
    /// pull `minLocalDeletionTime` down to the sentinel nor inflate the tombstone
    /// drop-time histogram (issue #851, Cassandra `d5bc7fb5`).
    pub fn update_local_deletion_time(&mut self, deletion_time: i32) {
        if Self::is_live_local_deletion_time(deletion_time) {
            return;
        }
        self.min_local_deletion_time = self.min_local_deletion_time.min(deletion_time);
        self.max_local_deletion_time = self.max_local_deletion_time.max(deletion_time);
        self.tombstone_histogram.update(deletion_time);
    }

    /// True when `timestamp` is a LIVE / `NO_DELETION` marker rather than a real
    /// deletion timestamp. CQLite writes `DeletionTime.LIVE` as `Long.MIN_VALUE`
    /// (see `data_writer.rs`); Cassandra's `NO_DELETION` / `NO_TIMESTAMP` sentinel
    /// is `Long.MAX_VALUE`. Both mean "no deletion" and must be excluded from
    /// timestamp aggregation.
    fn is_live_timestamp(timestamp: i64) -> bool {
        timestamp == i64::MIN || timestamp == i64::MAX
    }

    /// True when `deletion_time` is a LIVE marker (`Integer.MAX_VALUE`) rather
    /// than a real tombstone local-deletion-time.
    fn is_live_local_deletion_time(deletion_time: i32) -> bool {
        deletion_time == i32::MAX
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

    /// Record one partition's serialized size and cell count into the
    /// `estimatedPartitionSize` / `estimatedCellPerPartitionCount` histograms.
    ///
    /// Called once per partition written to Data.db with the exact serialized
    /// partition byte length (`serialized_size`) and the number of cells emitted
    /// (`cell_count`). Because it adds exactly one observation per partition, the
    /// `estimatedPartitionSize` histogram's Σ bucket counts equals the SSTable's
    /// partition count — the value the read-side authoritative decode
    /// (`read_table_counts`, issue #944) reports. This mirrors Cassandra's
    /// `MetadataCollector.addPartitionSizeInBytes` /
    /// `addCellPerPartitionCount` (issue #1327).
    pub fn record_partition(&mut self, serialized_size: u64, cell_count: u64) {
        self.estimated_partition_size.add(serialized_size);
        self.estimated_cell_count.add(cell_count);
    }

    /// Record a partition key in the SSTable key range.
    ///
    /// Partitions are written in ascending token order (enforced by
    /// `SSTableWriter::write_partition`), so the FIRST key seen is the lowest and
    /// the LAST is the highest. Used to populate the `da`-format
    /// `StatsMetadata.firstKey`/`lastKey` (`hasKeyRange`). The clone is bounded by
    /// the partition-key size (typically tens of bytes), not the partition data.
    pub fn update_key_range(&mut self, key: &[u8]) {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());
    }

    /// Record that a partition-level deletion (partition tombstone) was written.
    ///
    /// Once set, this stays `true` for the lifetime of the SSTable and drives the
    /// `da`-format `StatsMetadata.hasPartitionLevelDeletions` marker. Called by
    /// `SSTableWriter::write_partition` for any partition carrying a
    /// `partition_tombstone`.
    pub fn mark_partition_level_deletion(&mut self) {
        self.has_partition_level_deletions = true;
    }

    /// Set the persisted repair state (`repairedAt`, `pendingRepair`,
    /// `isTransient`) carried into the output STATS component.
    ///
    /// Used by the compaction merge path to preserve the repair metadata of
    /// compatible inputs through to the merged output (issue #1021). A fresh
    /// memtable flush never calls this, so the default unrepaired state
    /// (`repaired_at = 0`, `pending_repair = None`, `is_transient = false`) is
    /// retained.
    pub fn set_repair_state(
        &mut self,
        repaired_at: i64,
        pending_repair: Option<[u8; 16]>,
        is_transient: bool,
    ) {
        self.repaired_at = repaired_at;
        self.pending_repair = pending_repair;
        self.is_transient = is_transient;
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

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Issue #851 / Cassandra `d5bc7fb5`: a LIVE complex-deletion / liveness
    /// marker must not poison the timestamp aggregates. CQLite encodes
    /// `DeletionTime.LIVE` as `Long.MIN_VALUE`; Cassandra's `NO_DELETION` /
    /// `NO_TIMESTAMP` is `Long.MAX_VALUE`. Both must be ignored.
    #[test]
    fn test_update_timestamp_ignores_live_markers() {
        let mut meta = StatisticsMetadata::new();
        meta.update_timestamp(1_000_000);

        // LIVE marker (CQLite sentinel) must not pull min down to i64::MIN.
        meta.update_timestamp(i64::MIN);
        // NO_DELETION / NO_TIMESTAMP (Cassandra sentinel) must not push max up.
        meta.update_timestamp(i64::MAX);

        assert_eq!(
            meta.min_timestamp, 1_000_000,
            "LIVE marker must not poison min_timestamp"
        );
        assert_eq!(
            meta.max_timestamp, 1_000_000,
            "NO_DELETION marker must not poison max_timestamp"
        );
    }

    /// A LIVE complex-deletion marker (`localDeletionTime == Integer.MAX_VALUE`)
    /// is not a tombstone: it must not lower `min_local_deletion_time` nor inflate
    /// the tombstone drop-time histogram (issue #851, Cassandra `d5bc7fb5`).
    #[test]
    fn test_update_local_deletion_time_ignores_live_marker() {
        let mut meta = StatisticsMetadata::new();
        meta.update_local_deletion_time(1_500_000_000);

        // LIVE marker: must be skipped entirely.
        meta.update_local_deletion_time(i32::MAX);

        assert_eq!(
            meta.min_local_deletion_time, 1_500_000_000,
            "LIVE marker must not poison min_local_deletion_time"
        );
        assert_eq!(meta.max_local_deletion_time, 1_500_000_000);
        // Only the one real tombstone bin; the LIVE marker did not enter the histogram.
        assert_eq!(
            meta.tombstone_histogram.size(),
            1,
            "LIVE marker must not be counted as a tombstone in the histogram"
        );
    }

    /// With only LIVE markers, stats remain at sentinels and `finalize()`
    /// normalizes them to 0 (no tombstones recorded).
    #[test]
    fn test_only_live_markers_finalize_to_zero() {
        let mut meta = StatisticsMetadata::new();
        meta.update_timestamp(i64::MIN);
        meta.update_timestamp(i64::MAX);
        meta.update_local_deletion_time(i32::MAX);

        assert!(meta.tombstone_histogram.is_empty());

        meta.finalize();
        assert_eq!(meta.min_timestamp, 0);
        assert_eq!(meta.max_timestamp, 0);
        assert_eq!(meta.min_local_deletion_time, 0);
        assert_eq!(meta.max_local_deletion_time, 0);
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

    // -----------------------------------------------------------------------
    // TombstoneHistogram unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_tombstone_histogram_empty() {
        let h = TombstoneHistogram::new();
        assert!(h.is_empty());
        assert_eq!(h.size(), 0);
    }

    #[test]
    fn test_tombstone_histogram_single_entry() {
        let mut h = TombstoneHistogram::new();
        h.update(1_700_000_000);
        assert!(!h.is_empty());
        assert_eq!(h.size(), 1);
        let entries: Vec<_> = h.entries().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 1_700_000_000.0f64);
        assert_eq!(entries[0].1, 1);
    }

    #[test]
    fn test_tombstone_histogram_multiple_entries() {
        let mut h = TombstoneHistogram::new();
        // Three distinct deletion times
        h.update(1_000);
        h.update(2_000);
        h.update(1_000); // duplicate — should increment count
        assert_eq!(h.size(), 2);
        let entries: Vec<_> = h.entries().collect();
        // Entries are sorted by point value ascending
        assert_eq!(entries[0].0, 1_000.0f64);
        assert_eq!(entries[0].1, 2); // count = 2
        assert_eq!(entries[1].0, 2_000.0f64);
        assert_eq!(entries[1].1, 1);
    }

    #[test]
    fn test_tombstone_histogram_bin_merge_at_capacity() {
        let mut h = TombstoneHistogram::new();
        // Insert 101 distinct deletion times — should trigger a merge so bins <= 100
        for i in 0..=100i32 {
            h.update(1_700_000_000 + i);
        }
        assert!(
            h.size() <= TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE,
            "bins should not exceed MAX_BIN_SIZE after merge: got {}",
            h.size()
        );
        assert!(!h.is_empty());
    }

    /// Verify that `StatisticsMetadata::update_local_deletion_time` feeds the histogram.
    #[test]
    fn test_metadata_update_local_deletion_time_populates_histogram() {
        let mut meta = StatisticsMetadata::new();
        assert!(meta.tombstone_histogram.is_empty());

        meta.update_local_deletion_time(1_700_000_000);
        meta.update_local_deletion_time(1_700_100_000);
        meta.update_local_deletion_time(1_700_000_000); // duplicate

        assert!(!meta.tombstone_histogram.is_empty());
        assert_eq!(
            meta.tombstone_histogram.size(),
            2,
            "two distinct ldts → 2 bins"
        );

        // The bin for 1_700_000_000 should have count 2
        let entries: Vec<_> = meta.tombstone_histogram.entries().collect();
        let (pt0, v0) = entries[0];
        assert_eq!(pt0, 1_700_000_000.0f64);
        assert_eq!(v0, 2);
    }
}
