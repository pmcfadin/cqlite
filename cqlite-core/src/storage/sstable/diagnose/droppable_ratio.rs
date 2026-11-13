//! `getEstimatedDroppableTombstoneRatio`, ported exactly (issue #4204, `design.md` D3).
//!
//! Source of truth (read at the pinned `cassandra-5.0.8` tag, never a CQLite
//! `file:line`, #3041): `StatsMetadata.getEstimatedDroppableTombstoneRatio` +
//! `TombstoneHistogram.sum` (`StreamingTombstoneHistogramBuilder.DataHolder.sum`).
//!
//! ```java
//! // StatsMetadata.java, cassandra-5.0.8
//! public double getEstimatedDroppableTombstoneRatio(long gcBefore) {
//!     long estimatedColumnCount = estimatedCellPerPartitionCount.mean() * estimatedCellPerPartitionCount.count();
//!     if (estimatedColumnCount > 0) {
//!         double droppable = estimatedTombstoneDropTime.sum(gcBefore);
//!         return droppable / estimatedColumnCount;
//!     }
//!     return 0.0f;
//! }
//! ```
//!
//! `estimatedColumnCount`'s two histogram statistics come from
//! [`crate::parser::repair_metadata::read_cell_per_partition_stats`] (a NEW
//! read-side decode this change adds — task 0.1 confirmed the read side did not
//! previously decode `estimatedCellPerPartitionCount`'s raw buckets back out,
//! only `estimatedPartitionSize`'s bucket SUM via `read_table_counts`).
//! `droppable` reuses the ALREADY-decoded
//! [`crate::parser::statistics::SSTableStatistics::tombstone_drop_times`]
//! (issue #1073).

use crate::parser::repair_metadata::CellPerPartitionStats;

/// A `--now`-independent gc_grace cutoff formula, factored out of
/// `write_engine::merge::compute_gc_before` so `diagnose`'s cheap tier (which the
/// CLI spec requires to run WITHOUT `--schema`, R6) can compute the SAME
/// `gcBefore` Cassandra/`compact`/`scrub` use without requiring a full
/// [`crate::schema::TableSchema`] object.
///
/// `gc_grace_seconds` is `None` when no schema (or no `gc_grace_seconds` table
/// option) is available — in that case Cassandra's own `TableParams` default of
/// 10 days is used (`DEFAULT_GC_GRACE_SECONDS`, matching
/// `compute_gc_before`'s own `None` branch exactly: this is a factoring choice,
/// not a divergent formula). Returns `None` only when a caller-declared value is
/// invalid (negative), which disables purging (a strict no-op) — garbage
/// metadata must never cause a tombstone to read as droppable.
pub(crate) fn compute_gc_before_secs(gc_grace_seconds: Option<i64>, now_secs: i64) -> Option<i64> {
    /// Cassandra's `TableParams.DEFAULT_GC_GRACE_SECONDS` (10 days).
    const DEFAULT_GC_GRACE_SECONDS: i64 = 864_000;
    match gc_grace_seconds {
        None => Some(now_secs - DEFAULT_GC_GRACE_SECONDS),
        Some(gc_grace_seconds) if gc_grace_seconds >= 0 => Some(now_secs - gc_grace_seconds),
        Some(_) => None,
    }
}

/// Outcome of attempting to compute `getEstimatedDroppableTombstoneRatio`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum DroppableRatio {
    /// The formula's exact Cassandra value.
    Value(f64),
    /// `estimatedCellPerPartitionCount`'s overflow bucket held observations —
    /// real Cassandra's `mean()` would throw `IllegalStateException` in this
    /// state. Reported honestly as unmeasured rather than guessed (#4159 class).
    Overflowed,
}

/// Port of `StatsMetadata.getEstimatedDroppableTombstoneRatio(gcBefore)`, exactly.
///
/// `tombstone_drop_times` is `(point, count)` pairs; the estimate is undefined
/// (Cassandra treats it as "no droppable data" — `estimatedColumnCount == 0`
/// short-circuits to `0.0` — before ever calling `.sum()`) whenever there are no
/// observations to weight, so this mirrors that ordering exactly: `mean()`/
/// `count()` are computed first, and `.sum()` (the tombstone histogram walk) is
/// skipped entirely when `estimatedColumnCount <= 0`.
pub(crate) fn estimated_droppable_tombstone_ratio(
    cell_per_partition: &CellPerPartitionStats,
    tombstone_drop_times: &[(i64, u64)],
    gc_before_secs: i64,
) -> DroppableRatio {
    if cell_per_partition.overflowed {
        return DroppableRatio::Overflowed;
    }
    // i128 to stay clear of i64 overflow for a pathological mean*count product
    // (roborev pre-empt: integer overflow class) before deciding the `> 0` gate.
    let estimated_column_count = cell_per_partition.mean as i128 * cell_per_partition.count as i128;
    if estimated_column_count <= 0 {
        return DroppableRatio::Value(0.0);
    }
    let droppable = sum_tombstone_drop_times(tombstone_drop_times, gc_before_secs);
    DroppableRatio::Value(droppable / estimated_column_count as f64)
}

/// Port of `StreamingTombstoneHistogramBuilder.DataHolder.sum(int b)`, exactly
/// (cassandra-5.0.8): "estimated number of points in the interval `[-inf, b]`",
/// with LINEAR INTERPOLATION at the bin straddling `b` — never a plain
/// less-than-or-equal bucket sum. `bins` is sorted defensively (ascending by
/// point) before the walk: the on-disk histogram is written in point order, but
/// this formula's early-return-on-first-`point > b` logic requires it, and a
/// decode that silently assumed order without asserting it would be exactly the
/// #28 no-heuristics trap this module otherwise avoids.
pub(crate) fn sum_tombstone_drop_times(bins: &[(i64, u64)], b: i64) -> f64 {
    let mut sorted: Vec<(i64, u64)> = bins.to_vec();
    sorted.sort_by_key(|&(point, _)| point);

    let mut sum: f64 = 0.0;
    for (i, &(point, value)) in sorted.iter().enumerate() {
        let value = value as f64;
        if point > b {
            if i == 0 {
                // no prev point
                return 0.0;
            }
            let (prev_point, prev_value) = sorted[i - 1];
            let prev_value = prev_value as f64;
            let denom = (point - prev_point) as f64;
            // `denom` is > 0 here (points are strictly ascending post-sort and
            // `point > prev_point` — a truly zero-width interval cannot occur for
            // two distinct bins), so this cannot divide by zero.
            let weight = (b - prev_point) as f64 / denom;
            let mb = prev_value + (value - prev_value) * weight;
            sum -= prev_value;
            sum += (prev_value + mb) * weight / 2.0;
            sum += prev_value / 2.0;
            return sum;
        }
        sum += value;
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_before_defaults_to_ten_days_without_schema() {
        let now = 2_000_000_000i64;
        assert_eq!(
            compute_gc_before_secs(None, now),
            Some(now - 864_000),
            "no gc_grace_seconds -> Cassandra's 10-day TableParams default"
        );
    }

    #[test]
    fn gc_before_uses_declared_value() {
        assert_eq!(compute_gc_before_secs(Some(0), 1_000), Some(1_000));
        assert_eq!(compute_gc_before_secs(Some(500), 1_000), Some(500));
    }

    #[test]
    fn gc_before_negative_declared_value_disables_purge() {
        assert_eq!(compute_gc_before_secs(Some(-1), 1_000), None);
    }

    #[test]
    fn sum_before_first_point_is_zero() {
        let bins = vec![(100i64, 5u64)];
        assert_eq!(sum_tombstone_drop_times(&bins, 50), 0.0);
    }

    #[test]
    fn sum_at_or_after_last_point_is_full_sum() {
        let bins = vec![(100i64, 5u64), (200i64, 3u64)];
        assert_eq!(sum_tombstone_drop_times(&bins, 500), 8.0);
    }

    #[test]
    fn ratio_zero_when_no_observations() {
        let stats = CellPerPartitionStats {
            mean: 0,
            count: 0,
            overflowed: false,
        };
        let ratio = estimated_droppable_tombstone_ratio(&stats, &[], 1000);
        assert_eq!(ratio, DroppableRatio::Value(0.0));
    }

    #[test]
    fn ratio_overflowed_is_reported_not_guessed() {
        let stats = CellPerPartitionStats {
            mean: 10,
            count: 10,
            overflowed: true,
        };
        let ratio = estimated_droppable_tombstone_ratio(&stats, &[(1, 1)], 1000);
        assert_eq!(ratio, DroppableRatio::Overflowed);
    }

    /// Oracle-independent case matching the committed
    /// `test_tomb/tombstone_histogram` fixture's own numbers (see
    /// `issue_4204_diagnose_cheap_tier.rs`): mean=8, count=3 (from the printed
    /// "Column Count" histogram 6/8/10 each count 1, matching `totalColumnsSet:
    /// 24` = 8*3), one drop-time bin at 1782342000 with count 5.
    #[test]
    fn ratio_matches_tombstone_histogram_fixture_shape() {
        let stats = CellPerPartitionStats {
            mean: 8,
            count: 3,
            overflowed: false,
        };
        let bins = vec![(1_782_342_000i64, 5u64)];
        // gc_before before the drop time -> 0.0 (matches the oracle file's
        // captured "Estimated droppable tombstones: 0.0").
        assert_eq!(
            estimated_droppable_tombstone_ratio(&stats, &bins, 1_000_000_000),
            DroppableRatio::Value(0.0)
        );
        // gc_before after the drop time -> 5/24.
        match estimated_droppable_tombstone_ratio(&stats, &bins, 1_782_342_100) {
            DroppableRatio::Value(v) => assert!((v - 5.0 / 24.0).abs() < 1e-12),
            other => panic!("expected Value, got {other:?}"),
        }
    }
}
