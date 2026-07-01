//! Cassandra-canonical `EstimatedHistogram` accumulator + serialiser.
//!
//! Populates the STATS-component `estimatedPartitionSize` and
//! `estimatedCellPerPartitionCount` histograms so a write-produced
//! `Statistics.db` carries the same authoritative per-table distribution
//! Cassandra writes (issue #1327). The read-side authoritative partition-count
//! decode (`read_table_counts`, issue #944) sums the `estimatedPartitionSize`
//! bucket counts; before this each histogram was empty, so it decoded `0`.
//!
//! # Format (matches `EstimatedHistogram.EstimatedHistogramSerializer`)
//!
//! ```text
//! i32 BE  bucketCount            = bucketOffsets.len() + 1  (the trailing
//!                                   +1 is the overflow bucket)
//! for each of the bucketCount buckets:
//!   i64 BE  offset  — bucketOffsets[i], or Long.MAX_VALUE for the overflow
//!                     bucket (index == bucketOffsets.len())
//!   i64 BE  count   — number of observations that fell in this bucket
//! ```
//!
//! # Bucket offsets (matches `EstimatedHistogram.newOffsets`)
//!
//! Cassandra's `MetadataCollector` seeds both histograms with
//! `EstimatedHistogram.newEstimatedHistogram()` → a 155-offset series (156
//! serialised buckets, matching the real-fixture annotated dump). The series
//! starts at 1 and grows geometrically by a factor of `1.2`, rounding to the
//! nearest integer and forcing strict monotonicity:
//!
//! ```text
//! offsets[0] = 1
//! offsets[j] = max(round(offsets[j-1] * 1.2), offsets[j-1] + 1)
//! ```
//!
//! Authority: cassandra-5.0.0 `utils/EstimatedHistogram.java`
//! (`newOffsets(size, considerZeroes=false)`, `EstimatedHistogramSerializer`)
//! and `io/sstable/metadata/MetadataCollector.java`
//! (`defaultPartitionSizeHistogram` / `defaultCellPerPartitionCountHistogram`).

/// Number of bucket OFFSETS in the canonical partition-size / cell-count
/// histograms. Cassandra's `MetadataCollector` default; serialised bucket count
/// is this + 1 (the overflow bucket). Matches the 156-bucket count observed in
/// the committed `statistics-db-annotated-dump.txt`.
const DEFAULT_OFFSET_COUNT: usize = 155;

/// Sentinel offset Cassandra writes for the trailing overflow bucket
/// (`Long.MAX_VALUE`). Any observation larger than the last real offset falls
/// here.
const OVERFLOW_OFFSET: i64 = i64::MAX;

/// A Cassandra-canonical `EstimatedHistogram` used for `estimatedPartitionSize`
/// and `estimatedCellPerPartitionCount`.
///
/// Bucket offsets are the fixed geometric series; `counts` has one extra slot
/// for the overflow bucket. Every observation increments exactly one bucket, so
/// `Σ counts` equals the number of observations (one per partition), which is
/// what the reader decodes as `partition_count`.
#[derive(Debug, Clone)]
pub struct EstimatedHistogram {
    /// Strictly increasing bucket boundaries (inclusive upper bounds).
    offsets: Vec<i64>,
    /// Per-bucket observation counts; length is `offsets.len() + 1` (the last
    /// element is the overflow bucket). `u64` cannot overflow for any real
    /// SSTable (bounded by partition/cell counts) and is summed as `u64` by the
    /// reader.
    counts: Vec<u64>,
}

impl Default for EstimatedHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl EstimatedHistogram {
    /// Create an empty histogram with the canonical default bucket offsets.
    pub fn new() -> Self {
        let offsets = new_offsets(DEFAULT_OFFSET_COUNT);
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
    pub fn write_to(&self, buffer: &mut Vec<u8>) {
        let bucket_count = self.counts.len() as i32;
        buffer.extend_from_slice(&bucket_count.to_be_bytes());
        for (i, &count) in self.counts.iter().enumerate() {
            let offset = self.offsets.get(i).copied().unwrap_or(OVERFLOW_OFFSET);
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

/// Build the canonical geometric bucket-offset series of length `size`.
///
/// `EstimatedHistogram.newOffsets(size, considerZeroes=false)`: `offsets[0] = 1`
/// then each subsequent offset is `round(prev * 1.2)`, forced to be strictly
/// greater than its predecessor.
fn new_offsets(size: usize) -> Vec<i64> {
    let mut offsets = Vec::with_capacity(size);
    if size == 0 {
        return offsets;
    }
    let mut last: i64 = 1;
    offsets.push(last);
    for _ in 1..size {
        // round(last * 1.2), matching Java Math.round (half-up on the .5 tie for
        // the small, always-positive values in this series).
        let scaled = (last as f64) * 1.2;
        let mut next = scaled.round() as i64;
        if next <= last {
            next = last + 1;
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
    fn default_offsets_match_cassandra_series() {
        let off = new_offsets(DEFAULT_OFFSET_COUNT);
        assert_eq!(off.len(), 155, "155 offsets => 156 serialised buckets");
        assert_eq!(off[0], 1);
        // Strictly increasing, geometric-ish.
        for w in off.windows(2) {
            assert!(w[1] > w[0], "offsets must be strictly increasing");
        }
        // Spot-check the first few against the hand-computed 1.2x series.
        // 1, round(1.2)=1 -> +1 = 2, round(2.4)=2 -> +1 = 3, round(3.6)=4, ...
        assert_eq!(&off[..5], &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn add_sums_to_observation_count() {
        let mut h = EstimatedHistogram::new();
        for _ in 0..42 {
            h.add(1234);
        }
        assert_eq!(h.total(), 42, "one increment per observation");
    }

    #[test]
    fn add_routes_to_smallest_offset_at_or_above_value() {
        let mut h = EstimatedHistogram::new();
        // value 1 lands in bucket 0 (offset 1, exact match).
        h.add(1);
        // value 0 also lands in bucket 0 (first offset >= 0 is offset[0]=1).
        h.add(0);
        assert_eq!(h.counts[0], 2);
        // A huge value lands in the overflow bucket (last slot).
        h.add(u64::MAX);
        assert_eq!(*h.counts.last().unwrap(), 1);
        assert_eq!(h.total(), 3);
    }

    #[test]
    fn serialized_shape_matches_serializer() {
        let mut h = EstimatedHistogram::new();
        h.add(10);
        h.add(10);
        let mut buf = Vec::new();
        h.write_to(&mut buf);

        let bucket_count = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        assert_eq!(bucket_count, 156, "155 offsets + 1 overflow bucket");
        // Body length: 4 (count) + bucket_count * 16 (offset+count pairs).
        assert_eq!(buf.len(), 4 + 156 * 16);

        // Overflow bucket offset is Long.MAX_VALUE.
        let last_off_pos = 4 + (156 - 1) * 16;
        let last_off = i64::from_be_bytes(buf[last_off_pos..last_off_pos + 8].try_into().unwrap());
        assert_eq!(last_off, i64::MAX);

        // Σ bucket counts across the serialised body == observation count.
        let mut sum = 0i64;
        for i in 0..156usize {
            let cpos = 4 + i * 16 + 8;
            sum += i64::from_be_bytes(buf[cpos..cpos + 8].try_into().unwrap());
        }
        assert_eq!(sum, 2);
    }
}
