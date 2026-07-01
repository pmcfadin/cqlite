//! Cassandra-canonical `EstimatedHistogram` accumulator + serialiser.
//!
//! Populates the STATS-component `estimatedPartitionSize` and
//! `estimatedCellPerPartitionCount` histograms so a write-produced
//! `Statistics.db` carries the same authoritative per-table distribution
//! Cassandra writes (issue #1327). The read-side authoritative partition-count
//! decode (`read_table_counts`, issue #944) sums the `estimatedPartitionSize`
//! bucket counts; before this each histogram was empty, so it decoded `0`.
//!
//! # Two distinct default shapes (issue #1327 finding 1)
//!
//! Cassandra seeds the two STATS histograms with DIFFERENT bucket counts, not
//! one shared shape (this was previously wrong in CQLite — the cell-count
//! histogram reused the partition-size shape). Authority:
//! cassandra-5.0.0 `io/sstable/metadata/MetadataCollector.java`:
//!
//! ```text
//! static EstimatedHistogram defaultCellPerPartitionCountHistogram()
//! {
//!     // EH of 118 can track a max value of 4139110981, i.e., > 4B cells
//!     return new EstimatedHistogram(118);
//! }
//! static EstimatedHistogram defaultPartitionSizeHistogram()
//! {
//!     // EH of 155 can track a max value of 3520571548412 i.e. 3.5TB
//!     return new EstimatedHistogram(155);
//! }
//! ```
//!
//! `new EstimatedHistogram(size)` builds `size` bucket OFFSETS via
//! `newOffsets(size, considerZeroes=false)` and a `bucketOffsets.length + 1`
//! bucket-count array (the trailing overflow bucket). So:
//!
//! | Histogram                        | offsets | serialised buckets |
//! |----------------------------------|---------|--------------------|
//! | `estimatedPartitionSize`         | 155     | 156                |
//! | `estimatedCellPerPartitionCount` | 118     | 119                |
//!
//! # Offset series (matches `EstimatedHistogram.newOffsets`)
//!
//! `newOffsets(size, false)` (cassandra-5.0.0 `utils/EstimatedHistogram.java`)
//! produces a STRICTLY-monotonic geometric series with NO leading duplicate in
//! the offsets array itself:
//!
//! ```text
//! long last = 1;
//! result[0] = last;                       // = 1
//! for (i = 1; i < size; i++) {
//!     long next = Math.round(last * 1.2);
//!     if (next == last) next++;
//!     result[i] = next; last = next;
//! }
//! ```
//!
//! yielding `[1, 2, 3, 4, 5, 6, 7, 8, ...]`. The two documented max offsets are
//! reproduced exactly (offsets[154] == 3_520_571_548_412 and
//! offsets[117] == 4_139_110_981), which pins the series + bucket counts against
//! the Cassandra source comments.
//!
//! # Serialised leading duplicate is a SERIALISER artefact, not an offset
//!
//! The `[1, 1, 2, 3, ...]` leading duplicate observed at the head of the STATS
//! component in the committed annotated fixture
//! (`docs/sstables-definitive-guide/statistics-db-annotated-dump.txt`, STATS at
//! 0x0b53) is produced by `EstimatedHistogramSerializer.serialize`, which emits
//! `offsets[i == 0 ? 0 : i - 1]` for each of the `buckets.length` buckets — so
//! `offsets[0]` is written TWICE (for bucket index 0 and 1). The offsets array
//! itself is `[1, 2, 3, ...]`. CQLite therefore stores the pure series and
//! reproduces the duplicate at serialisation, matching Cassandra byte-for-byte
//! for the RIGHT reason.
//!
//! # Wire format (matches `EstimatedHistogram.EstimatedHistogramSerializer`)
//!
//! ```text
//! i32 BE  bucketCount            = bucketOffsets.len() + 1
//! for each bucket index i in 0..bucketCount:
//!   i64 BE  offset  — offsets[i == 0 ? 0 : i - 1], or Long.MAX_VALUE for the
//!                     overflow bucket (i == bucketCount - 1)
//!   i64 BE  count   — number of observations that fell in this bucket
//! ```

/// Number of bucket OFFSETS in the `estimatedPartitionSize` histogram.
/// cassandra-5.0.0 `MetadataCollector.defaultPartitionSizeHistogram` →
/// `new EstimatedHistogram(155)`. Serialised bucket count is this + 1 (the
/// overflow bucket) = 156, matching the committed
/// `statistics-db-annotated-dump.txt`.
const PARTITION_SIZE_OFFSET_COUNT: usize = 155;

/// Number of bucket OFFSETS in the `estimatedCellPerPartitionCount` histogram.
/// cassandra-5.0.0 `MetadataCollector.defaultCellPerPartitionCountHistogram` →
/// `new EstimatedHistogram(118)`. Serialised bucket count is this + 1 = 119.
const CELL_COUNT_OFFSET_COUNT: usize = 118;

/// Sentinel offset Cassandra writes for the trailing overflow bucket
/// (`Long.MAX_VALUE`). Any observation larger than the last real offset falls
/// here.
const OVERFLOW_OFFSET: i64 = i64::MAX;

/// A Cassandra-canonical `EstimatedHistogram` used for `estimatedPartitionSize`
/// and `estimatedCellPerPartitionCount`.
///
/// Bucket offsets are the fixed geometric series `[1, 2, 3, ...]` (length
/// depends on the histogram); `counts` has one extra slot for the overflow
/// bucket. Every observation increments exactly one bucket, so `Σ counts` equals
/// the number of observations (one per partition), which is what the reader
/// decodes as `partition_count`.
#[derive(Debug, Clone)]
pub struct EstimatedHistogram {
    /// Bucket boundaries (inclusive upper bounds), strictly increasing:
    /// `[1, 2, 3, 4, ...]`. This is the PURE Cassandra `newOffsets` series with
    /// no leading duplicate; the serialised leading duplicate is applied in
    /// [`Self::write_to`].
    offsets: Vec<i64>,
    /// Per-bucket observation counts; length is `offsets.len() + 1` (the last
    /// element is the overflow bucket). `u64` cannot overflow for any real
    /// SSTable (bounded by partition/cell counts) and is summed as `u64` by the
    /// reader.
    counts: Vec<u64>,
}

impl Default for EstimatedHistogram {
    fn default() -> Self {
        Self::partition_size()
    }
}

impl EstimatedHistogram {
    /// The `estimatedPartitionSize` histogram (155 offsets → 156 serialised
    /// buckets). cassandra-5.0.0 `MetadataCollector.defaultPartitionSizeHistogram`.
    pub fn partition_size() -> Self {
        Self::with_offset_count(PARTITION_SIZE_OFFSET_COUNT)
    }

    /// The `estimatedCellPerPartitionCount` histogram (118 offsets → 119
    /// serialised buckets). cassandra-5.0.0
    /// `MetadataCollector.defaultCellPerPartitionCountHistogram`.
    pub fn cell_per_partition_count() -> Self {
        Self::with_offset_count(CELL_COUNT_OFFSET_COUNT)
    }

    /// Build an empty histogram with `offset_count` canonical bucket offsets and
    /// `offset_count + 1` zeroed bucket counts (the trailing overflow bucket).
    fn with_offset_count(offset_count: usize) -> Self {
        let offsets = new_offsets(offset_count);
        let counts = vec![0u64; offsets.len() + 1];
        Self { offsets, counts }
    }

    /// Record one observation of `value` (a serialized partition size in bytes,
    /// or a partition's cell count), incrementing the bucket whose offset is the
    /// smallest `>= value`. Values above the last real offset land in the
    /// overflow bucket.
    ///
    /// Mirrors `EstimatedHistogram.add(long)`:
    /// `index = Arrays.binarySearch(offsets, value)`; on a miss (`< 0`) the
    /// insertion point `-(index+1)` selects the first offset strictly greater
    /// than `value`.
    pub fn add(&mut self, value: u64) {
        // Saturate to i64 for comparison against the (i64) offsets. Real
        // partition sizes / cell counts never approach i64::MAX; saturation is a
        // fail-safe, not an expected path.
        let v = i64::try_from(value).unwrap_or(i64::MAX);
        let idx = match self.offsets.binary_search(&v) {
            // Exact match: that bucket's inclusive upper bound equals `value`.
            Ok(i) => i,
            // Miss: insertion point is the first offset strictly greater than
            // `value`; when `value` exceeds every offset this is the overflow
            // bucket (index == offsets.len()).
            Err(i) => i,
        };
        // `idx` is always in-bounds for `counts` (length offsets.len()+1).
        self.counts[idx] = self.counts[idx].saturating_add(1);
    }

    /// Serialise the histogram into `buffer` in Cassandra's
    /// `EstimatedHistogramSerializer` wire format (see module docs).
    ///
    /// The offset written for bucket `i` is `offsets[i == 0 ? 0 : i - 1]`,
    /// exactly as `EstimatedHistogramSerializer.serialize` does — which is why
    /// `offsets[0]` (value `1`) appears twice in the serialised stream. The final
    /// (overflow) bucket `i == bucketCount - 1` writes `offsets[bucketCount - 2]`,
    /// i.e. the LAST real offset — Cassandra does NOT write `Long.MAX_VALUE` for
    /// the overflow bucket in this format. `OVERFLOW_OFFSET` is only a defensive
    /// fallback that never fires (`off_idx` is always in-bounds here).
    pub fn write_to(&self, buffer: &mut Vec<u8>) {
        let bucket_count = self.counts.len() as i32;
        buffer.extend_from_slice(&bucket_count.to_be_bytes());
        for (i, &count) in self.counts.iter().enumerate() {
            // Cassandra serialiser: writeLong(offsets[i == 0 ? 0 : i - 1]).
            let off_idx = i.saturating_sub(1);
            let offset = self
                .offsets
                .get(off_idx)
                .copied()
                .unwrap_or(OVERFLOW_OFFSET);
            buffer.extend_from_slice(&offset.to_be_bytes());
            // Counts are logically non-negative; the reader decodes each as i64.
            buffer.extend_from_slice(&(count as i64).to_be_bytes());
        }
    }

    /// Total observation count (`Σ bucket counts`) — equals the number of
    /// partitions/cells recorded. Exposed for tests and invariants.
    #[cfg(test)]
    pub fn total(&self) -> u64 {
        self.counts.iter().copied().fold(0u64, u64::saturating_add)
    }
}

/// Build the canonical `EstimatedHistogram` bucket-offset series of length
/// `size`.
///
/// Matches Cassandra's `EstimatedHistogram.newOffsets(size, considerZeroes=false)`
/// (cassandra-5.0.0 `utils/EstimatedHistogram.java`): `offsets[0] = 1`, then each
/// subsequent offset is `round(prev * 1.2)`, bumped by one if the rounding did
/// not advance. Result: the strictly-monotonic series `[1, 2, 3, 4, 5, 6, 7, ...]`
/// (no leading duplicate — that duplicate is a serialiser artefact, see
/// [`EstimatedHistogram::write_to`]).
fn new_offsets(size: usize) -> Vec<i64> {
    let mut offsets = Vec::with_capacity(size);
    if size == 0 {
        return offsets;
    }
    // result[0] = 1
    let mut last: i64 = 1;
    offsets.push(last);
    for _ in 1..size {
        // round(last * 1.2), matching Java Math.round (half-up); if the round did
        // not advance, bump by one (Cassandra: `if (next == last) next++`).
        let scaled = (last as f64) * 1.2;
        let mut next = scaled.round() as i64;
        if next == last {
            next += 1;
        }
        offsets.push(next);
        last = next;
    }
    offsets
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_size_offsets_match_cassandra_155() {
        let off = new_offsets(PARTITION_SIZE_OFFSET_COUNT);
        assert_eq!(off.len(), 155, "155 offsets => 156 serialised buckets");
        // Pure Cassandra series (no leading duplicate in the offsets array).
        assert_eq!(&off[..8], &[1, 2, 3, 4, 5, 6, 7, 8]);
        // Documented max value from cassandra-5.0.0
        // MetadataCollector.defaultPartitionSizeHistogram: EH(155) tracks up to
        // 3520571548412 (3.5TB) — pins the exact series + count.
        assert_eq!(off[154], 3_520_571_548_412);
        // Strictly increasing throughout.
        for w in off.windows(2) {
            assert!(w[1] > w[0], "offsets must be strictly increasing");
        }
    }

    #[test]
    fn cell_count_offsets_match_cassandra_118() {
        let off = new_offsets(CELL_COUNT_OFFSET_COUNT);
        assert_eq!(off.len(), 118, "118 offsets => 119 serialised buckets");
        assert_eq!(&off[..8], &[1, 2, 3, 4, 5, 6, 7, 8]);
        // Documented max value from cassandra-5.0.0
        // MetadataCollector.defaultCellPerPartitionCountHistogram: EH(118) tracks
        // up to 4139110981 (>4B cells) — pins the exact series + count.
        assert_eq!(off[117], 4_139_110_981);
        for w in off.windows(2) {
            assert!(w[1] > w[0], "offsets must be strictly increasing");
        }
    }

    #[test]
    fn add_sums_to_observation_count() {
        let mut h = EstimatedHistogram::partition_size();
        for _ in 0..42 {
            h.add(1234);
        }
        assert_eq!(h.total(), 42, "one increment per observation");
    }

    #[test]
    fn add_routes_to_smallest_offset_at_or_above_value() {
        let mut h = EstimatedHistogram::partition_size();
        // With the pure offsets `[1, 2, 3, ...]`:
        //   value 0 -> insertion point 0 -> bucket 0 (offset 1).
        //   value 1 -> exact match on offset[0] == 1 -> bucket 0.
        h.add(0);
        h.add(1);
        assert_eq!(
            h.counts[0], 2,
            "values 0 and 1 both fall in the offset-1 bucket"
        );
        assert_eq!(h.offsets[0], 1);
        // value 2 lands in the offset-2 bucket (index 1).
        h.add(2);
        assert_eq!(h.counts[1], 1);
        assert_eq!(h.offsets[1], 2);
        // A huge value lands in the overflow bucket (last slot).
        h.add(u64::MAX);
        assert_eq!(*h.counts.last().unwrap(), 1);
        assert_eq!(h.total(), 4);
    }

    /// Issue #1327 finding 1: the `estimatedPartitionSize` histogram serialises
    /// with 156 buckets and the canonical leading-duplicate offset prefix
    /// `[1, 1, 2, 3, 4, 5, 6, 7]`, matching the annotated Statistics.db fixture
    /// (`docs/sstables-definitive-guide/statistics-db-annotated-dump.txt`, STATS
    /// component at 0x0b53).
    #[test]
    fn partition_size_serialized_shape_matches_fixture() {
        let h = EstimatedHistogram::partition_size();
        let mut buf = Vec::new();
        h.write_to(&mut buf);

        let bucket_count = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        assert_eq!(bucket_count, 156, "155 offsets + 1 overflow bucket");
        assert_eq!(buf.len(), 4 + 156 * 16);

        // The serialiser's `offsets[i == 0 ? 0 : i - 1]` produces a LEADING
        // DUPLICATE 1: [1, 1, 2, 3, 4, 5, 6, 7].
        let expected_prefix = [1i64, 1, 2, 3, 4, 5, 6, 7];
        for (i, &want) in expected_prefix.iter().enumerate() {
            let opos = 4 + i * 16;
            let got = i64::from_be_bytes(buf[opos..opos + 8].try_into().unwrap());
            assert_eq!(got, want, "serialised partition-size offset[{i}]");
        }

        // BYTE-FOR-BYTE pin against the committed annotated Statistics.db fixture
        // (`docs/sstables-definitive-guide/statistics-db-annotated-dump.txt`, STATS
        // component at 0x0b53, first 128 bytes = bucketCount + first 7 full
        // (offset,count) pairs + one more offset). This nails the interleaved
        // `i32 bucketCount` then `(i64 offset, i64 count)` layout, big-endian
        // ordering, and the empty (all-zero) counts of a freshly-seeded histogram
        // to the REAL Cassandra 5.0 bytes — not just the decoded i64 offset values.
        #[rustfmt::skip]
        let fixture_first_128: [u8; 128] = [
            0x00, 0x00, 0x00, 0x9c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(
            &buf[..128],
            &fixture_first_128,
            "serialised partition-size histogram must match the annotated \
             Statistics.db fixture byte-for-byte (STATS at 0x0b53)"
        );
    }

    /// Issue #1327 finding 1: the `estimatedCellPerPartitionCount` histogram
    /// serialises with its OWN authoritative shape — 119 buckets (NOT 156) — and
    /// the same leading-duplicate offset prefix. This asserts the cell-count
    /// histogram INDEPENDENTLY of the partition-size histogram, so one proof does
    /// not stand in for the other.
    #[test]
    fn cell_count_serialized_shape_is_119_buckets() {
        let h = EstimatedHistogram::cell_per_partition_count();
        let mut buf = Vec::new();
        h.write_to(&mut buf);

        let bucket_count = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        assert_eq!(
            bucket_count, 119,
            "cell-count histogram is EH(118) => 119 serialised buckets, \
             distinct from the 156-bucket partition-size histogram"
        );
        assert_eq!(buf.len(), 4 + 119 * 16);

        // Same serialiser leading-duplicate prefix as any EstimatedHistogram.
        let expected_prefix = [1i64, 1, 2, 3, 4, 5, 6, 7];
        for (i, &want) in expected_prefix.iter().enumerate() {
            let opos = 4 + i * 16;
            let got = i64::from_be_bytes(buf[opos..opos + 8].try_into().unwrap());
            assert_eq!(got, want, "serialised cell-count offset[{i}]");
        }
    }

    /// The two default histograms must NOT share a shape (regression for the
    /// finding-1 bug where cell-count reused the 156-bucket partition-size shape).
    #[test]
    fn partition_size_and_cell_count_have_distinct_bucket_counts() {
        let mut ps = Vec::new();
        EstimatedHistogram::partition_size().write_to(&mut ps);
        let mut cc = Vec::new();
        EstimatedHistogram::cell_per_partition_count().write_to(&mut cc);

        let ps_count = i32::from_be_bytes(ps[0..4].try_into().unwrap());
        let cc_count = i32::from_be_bytes(cc[0..4].try_into().unwrap());
        assert_eq!(ps_count, 156);
        assert_eq!(cc_count, 119);
        assert_ne!(
            ps_count, cc_count,
            "partition-size and cell-count histograms have distinct Cassandra shapes"
        );
    }

    #[test]
    fn serialized_body_sums_to_observation_count() {
        let mut h = EstimatedHistogram::cell_per_partition_count();
        h.add(10);
        h.add(10);
        let mut buf = Vec::new();
        h.write_to(&mut buf);

        let bucket_count = i32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
        let mut sum = 0i64;
        for i in 0..bucket_count {
            let cpos = 4 + i * 16 + 8;
            sum += i64::from_be_bytes(buf[cpos..cpos + 8].try_into().unwrap());
        }
        assert_eq!(sum, 2);
    }
}
