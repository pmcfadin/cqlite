//! STATS-component field decoders for `cqlite diagnose` (issue #4204), split out of
//! `repair_metadata.rs` (file-size ratchet #1116). A CHILD module: it shares that file's private
//! cursor / bounds helpers.

use super::{
    decode_max_local_deletion_time, skip_estimated_histogram, stats_component_bounds, Cursor,
    VersionGates,
};
use crate::error::Error;
use crate::Result;

/// `estimatedCellPerPartitionCount.mean()`/`.count()` inputs, decoded straight
/// from the STATS component (issue #4204, `cqlite diagnose`'s
/// `getEstimatedDroppableTombstoneRatio` port — `design.md` D3).
///
/// Mirrors cassandra-5.0.8 `EstimatedHistogram.rawMean()`/`.count()`
/// (`src/java/org/apache/cassandra/utils/EstimatedHistogram.java`) exactly:
/// `mean = ceil(Σ bucket[i]*offset[i] / Σ bucket[i])` for `i` in
/// `[0, lastBucket)` (the LAST bucket is the overflow bucket and is excluded
/// from the mean sum, though its count is still added by `count()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CellPerPartitionStats {
    /// `estimatedCellPerPartitionCount.mean()` — `0` when there were no
    /// observations outside the overflow bucket (Cassandra's `0/0` -> `NaN` ->
    /// `(long) NaN == 0` edge case, reproduced exactly rather than divide-by-zero).
    pub mean: i64,
    /// `estimatedCellPerPartitionCount.count()` — Σ of EVERY bucket, including
    /// the overflow bucket.
    pub count: u64,
    /// `true` when the overflow (last) bucket holds any observations. Real
    /// Cassandra's `mean()` throws `IllegalStateException` in this state; this
    /// decoder instead reports it so the caller can render the ratio as
    /// `unmeasured` rather than panic (issue #4204, #4159 class: never guess past
    /// a state Cassandra itself refuses to compute a mean for).
    pub overflowed: bool,
}

/// Decode [`CellPerPartitionStats`] from a raw `Statistics.db` buffer (issue
/// #4204). `estimatedCellPerPartitionCount` is the SECOND `EstimatedHistogram` in
/// the STATS body (right after `estimatedPartitionSize`) and is reachable by the
/// same fully self-describing forward walk [`read_table_counts`] uses to reach
/// the first — no version gates needed.
///
/// Returns `Ok(None)` when the `Statistics.db` carries no STATS component
/// (nothing to decode). Returns `Err(Corruption)` for a truncated/malformed STATS
/// body, matching every other decoder in this module (fail closed).
pub(crate) fn read_cell_per_partition_stats(input: &[u8]) -> Result<Option<CellPerPartitionStats>> {
    let Some(bounds) = stats_component_bounds(input)? else {
        return Ok(None);
    };
    let mut c = Cursor::new(&input[bounds.start..bounds.end]);

    // 1. estimatedPartitionSize histogram — skip (not needed here).
    skip_estimated_histogram(&mut c)?;

    // 2. estimatedCellPerPartitionCount histogram — READ every (offset, count)
    //    pair (offset shift: see the deserialize note on the mean loop below).
    let bucket_count = c.read_i32()?;
    if bucket_count < 0 {
        return Err(Error::Corruption(format!(
            "negative estimatedCellPerPartitionCount bucket count {bucket_count}"
        )));
    }
    let bucket_count = bucket_count as usize;
    let mut offsets: Vec<i64> = Vec::with_capacity(bucket_count.min(256));
    let mut counts: Vec<u64> = Vec::with_capacity(bucket_count.min(256));
    for _ in 0..bucket_count {
        let offset = c.read_i64()?;
        let bucket = c.read_i64()?;
        if bucket < 0 {
            return Err(Error::Corruption(format!(
                "negative estimatedCellPerPartitionCount bucket value {bucket}"
            )));
        }
        offsets.push(offset);
        counts.push(bucket as u64);
    }

    if bucket_count == 0 {
        return Ok(Some(CellPerPartitionStats {
            mean: 0,
            count: 0,
            overflowed: false,
        }));
    }

    let last_bucket = bucket_count - 1;
    let overflowed = counts[last_bucket] > 0;

    // count() sums EVERY bucket including the overflow one.
    let total_count: u64 = counts.iter().fold(0u64, |acc, &v| acc.saturating_add(v));

    // rawMean()/mean() exclude the overflow bucket. `elements == 0` reproduces
    // Cassandra's `0.0/0.0 == NaN -> (long) NaN == 0` edge case rather than a
    // divide-by-zero.
    let mut elements: u64 = 0;
    let mut sum: i128 = 0;
    for i in 0..last_bucket {
        // cassandra-5.0.8 `EstimatedHistogram.serializer.deserialize` stores pair `i` at
        // `offsets[i == 0 ? 0 : i - 1]`, so pair 0's offset is overwritten by pair 1's: after
        // deserialization bucket `i`'s offset is the ON-DISK offset of pair `i + 1`.
        elements = elements.saturating_add(counts[i]);
        sum += counts[i] as i128 * offsets[i + 1] as i128;
    }
    let mean: i64 = if elements == 0 {
        0
    } else {
        let raw_mean = sum as f64 / elements as f64;
        raw_mean.ceil() as i64
    };

    Ok(Some(CellPerPartitionStats {
        mean,
        count: total_count,
        overflowed,
    }))
}

/// Decode the STATS component's `compressionRatio` (`f64`, field 7 of the
/// self-describing forward walk — see [`parse_repair_metadata`]'s numbered
/// steps) directly (issue #4204, `cqlite diagnose` R2.1).
///
/// `SSTableStatistics::compression_stats` (the richer algorithm-name/speed
/// structure the enhanced `nb` parser leaves `None`, issue #1653) is a
/// DIFFERENT field from this one: `compressionRatio` is a single `f64` written
/// unconditionally by `StatsMetadata.StatsMetadataSerializer.serialize` (no
/// version gate), reachable by the SAME fully self-describing walk
/// [`read_table_counts`]/[`read_cell_per_partition_stats`] already use — no
/// gates needed. This is what `sstablemetadata`'s own "Compression ratio: …"
/// line prints, whether or not the SSTable is actually compressed (an
/// uncompressed SSTable's ratio is `1.0` or the Cassandra-computed default; a
/// negative sentinel means "not yet computed", see `CompressionMetadata` docs —
/// this decoder returns whatever `f64` is on disk, not a heuristic).
///
/// Returns `Ok(None)` when `Statistics.db` carries no STATS component.
pub(crate) fn read_compression_ratio(input: &[u8]) -> Result<Option<f64>> {
    let Some(bounds) = stats_component_bounds(input)? else {
        return Ok(None);
    };
    let mut c = Cursor::new(&input[bounds.start..bounds.end]);

    // 1-2. estimatedPartitionSize + estimatedCellPerPartitionCount.
    skip_estimated_histogram(&mut c)?;
    skip_estimated_histogram(&mut c)?;
    // 3. commitLogUpperBound: i64 segmentId + i32 position.
    c.skip(8 + 4)?;
    // 4. minTimestamp, maxTimestamp.
    c.skip(8 + 8)?;
    // 5. min/maxLocalDeletionTime.
    c.skip(4 + 4)?;
    // 6. minTTL, maxTTL.
    c.skip(4 + 4)?;
    // 7. compressionRatio (f64) — READ.
    let ratio = c.read_f64()?;
    Ok(Some(ratio))
}

/// The STATS component's own `minTimestamp` and `minLocalDeletionTime` (issue #4204): what
/// `sstablemetadata` prints as `Minimum timestamp` / `SSTable min local deletion time`. NOT the
/// `EncodingStats` baselines of the serialization header, which are a different, per-partition
/// encoding aid (e.g. `EncodingStats.minLocalDeletionTime` is the 2015 epoch for a table with no
/// tombstones, while STATS reports "no tombstones"). The deletion time is normalized exactly like
/// `maxLocalDeletionTime` (`i64::MAX` = none). `Ok(None)` when there is no STATS component.
pub(crate) fn read_stats_minimums(
    input: &[u8],
    gates: Option<&VersionGates>,
) -> Result<Option<(i64, i64)>> {
    let Some(bounds) = stats_component_bounds(input)? else {
        return Ok(None);
    };
    let modern = gates
        .map(super::uses_modern_tombstone_histogram)
        .unwrap_or(false);
    let mut c = Cursor::new(&input[bounds.start..bounds.end]);
    skip_estimated_histogram(&mut c)?;
    skip_estimated_histogram(&mut c)?;
    c.skip(8 + 4)?; // commitLogUpperBound
    let min_timestamp = c.read_i64()?;
    c.skip(8)?; // maxTimestamp
    let min_ldt = decode_max_local_deletion_time(c.read_u32()?, modern);
    Ok(Some((min_timestamp, min_ldt)))
}
