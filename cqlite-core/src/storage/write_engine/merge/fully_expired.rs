//! Fully-expired SSTable drop classification (issue #1388).
//!
//! Parity with Cassandra `CompactionController.getFullyExpiredSSTables`: before a
//! compaction rewrites anything, classify the subset of input SSTables that are
//! entirely past `gcBefore` AND provably shadow nothing outside the compaction
//! set, so they can be DROPPED WHOLE (excluded from the K-way merge and reclaimed
//! after the output publishes) instead of read, merged, and re-serialized.
//!
//! The classification is **metadata-only** — a single integer comparison against
//! the authoritative `Statistics.db` `TimestampStatistics.max_deletion_time`
//! (Cassandra `StatsMetadata.maxLocalDeletionTime`) plus the existing #935
//! overlap bound (`compute_max_purgeable_timestamp`). It NEVER reads/decodes/scans
//! a candidate's `Data.db` (no-heuristics mandate, issue #28): that is both the
//! whole point of the optimization (dropping must not pay a scan) and the
//! no-heuristics-compliant path. Trust in `Statistics.db` is already load-bearing
//! across the write engine (`compute_baseline_min`, `compute_max_purgeable_timestamp`).

#![cfg(feature = "write-support")]

use std::path::{Path, PathBuf};

use super::{compute_max_purgeable_timestamp, stats_path_for};
use crate::parser::statistics::TimestampStatistics;

/// Classify a candidate SSTable as *fully expired* for a compaction with cutoff
/// `gc_before_secs` from AUTHORITATIVE `Statistics.db` metadata ONLY.
///
/// A candidate is fully expired iff EVERY cell/tombstone in it has
/// `localDeletionTime < gcBefore`. The single authoritative field that proves this
/// is `TimestampStatistics.max_deletion_time` (Cassandra
/// `StatsMetadata.maxLocalDeletionTime`): if the MAXIMUM local-deletion-time in the
/// SSTable is below `gcBefore`, all of them are. This is exactly Cassandra's
/// `getFullyExpiredSSTables` predicate
/// (`sstable.getSSTableMetadata().maxLocalDeletionTime < gcBefore`).
///
/// The LIVE / `NO_DELETION_TIME` sentinel (an SSTable holding any live, non-TTL
/// data) surfaces from the parser as `i64::MAX`, which is never `< gcBefore`, so
/// such an SSTable is correctly NOT classified fully expired — the sentinel falls
/// out of the comparison naturally with no special-casing.
pub(super) fn is_fully_expired(stats: &TimestampStatistics, gc_before_secs: i64) -> bool {
    stats.max_deletion_time < gc_before_secs
}

/// Read a candidate SSTable's `TimestampStatistics` from its sibling
/// `Statistics.db`. Returns `None` when the file is absent or fails to parse — the
/// conservative signal that its expiry/timestamp facts are UNKNOWN, so the
/// candidate must NOT be dropped (matches `compute_max_purgeable_timestamp`
/// degradation). Authoritative metadata only (no cell scan).
fn read_timestamp_stats(data_path: &Path) -> Option<TimestampStatistics> {
    let stats_path = stats_path_for(data_path);
    if !stats_path.exists() {
        return None;
    }
    let stats_bytes = match std::fs::read(&stats_path) {
        Ok(b) => b,
        Err(e) => {
            log::warn!(
                "Could not read Statistics.db {:?} for fully-expired check: {}",
                stats_path,
                e
            );
            return None;
        }
    };
    match crate::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
        &stats_bytes,
        None,
    ) {
        Ok((_, sstable_stats)) => Some(sstable_stats.timestamp_stats),
        Err(e) => {
            log::warn!(
                "Could not parse Statistics.db {:?} for fully-expired check: {:?}",
                stats_path,
                e
            );
            None
        }
    }
}

/// Compute the subset of `input_paths` that may be DROPPED WHOLE for this
/// compaction (parity with Cassandra `CompactionController.getFullyExpiredSSTables`).
///
/// A candidate is included in the drop-set iff BOTH hold, from authoritative
/// `Statistics.db` metadata only (no cell scan):
///
/// 1. **Fully expired** — `max_deletion_time < gc_before_secs` (see
///    [`is_fully_expired`]).
/// 2. **Overlap-safe** — its `max_timestamp` is STRICTLY LESS THAN the minimum
///    write timestamp across every OUTSIDE overlapping SSTable, so nothing it holds
///    (tombstone or data) can shadow older data living outside the compaction set
///    (dropping it can never resurrect data). The outside bound is the identical
///    coarse global-`min_timestamp` bound [`compute_max_purgeable_timestamp`]
///    computes (issue #935 gate; OQ-2 → (A), key-range precision deferred).
///
/// Conservatism (never resurrect data, never drop live data):
/// - `gc_before_secs == None` (invalid/absent gc_grace disables purging) ⇒ empty
///   drop-set.
/// - An empty `outside_paths` (a FULL/major compaction: nothing outside to shadow)
///   ⇒ the overlap bound is `+inf` ⇒ every fully-expired candidate is droppable.
/// - A non-empty `outside_paths` whose bound is UNKNOWN
///   (`compute_max_purgeable_timestamp` returned `None` because an outside
///   `Statistics.db` was unreadable) ⇒ empty drop-set (cannot prove safety).
/// - A candidate whose own `Statistics.db` is absent/unreadable ⇒ not droppable.
///
/// The returned paths are a subset of `input_paths`, in input order.
pub fn fully_expired_sstables(
    input_paths: &[PathBuf],
    outside_paths: &[PathBuf],
    gc_before_secs: Option<i64>,
) -> Vec<PathBuf> {
    // No cutoff ⇒ purging is disabled ⇒ never drop (conservative, matches
    // compute_gc_before degradation).
    let Some(gc_before) = gc_before_secs else {
        return Vec::new();
    };

    // Overlap bound. An empty outside set (full/major compaction) has nothing to
    // shadow ⇒ +inf bound (i64::MAX): every fully-expired candidate is droppable.
    // A non-empty outside set with an UNKNOWN bound (an outside Statistics.db could
    // not be read/parsed) ⇒ we cannot prove safety ⇒ drop nothing.
    let outside_bound: i64 = if outside_paths.is_empty() {
        i64::MAX
    } else {
        match compute_max_purgeable_timestamp(outside_paths) {
            Some(bound) => bound,
            None => return Vec::new(),
        }
    };

    input_paths
        .iter()
        .filter(|data_path| {
            let Some(stats) = read_timestamp_stats(data_path) else {
                // Unknown candidate metadata ⇒ not droppable.
                return false;
            };
            // Fully expired AND provably shadows nothing outside the set.
            is_fully_expired(&stats, gc_before) && stats.max_timestamp < outside_bound
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
    use tempfile::TempDir;

    /// Construct a `TimestampStatistics` with the two fields the drop decision
    /// consults set explicitly; the rest are irrelevant.
    fn ts_stats(max_deletion_time: i64, max_timestamp: i64) -> TimestampStatistics {
        TimestampStatistics {
            min_timestamp: 0,
            max_timestamp,
            min_deletion_time: 0,
            max_deletion_time,
            min_ttl: None,
            max_ttl: None,
            rows_with_ttl: 0,
        }
    }

    /// R1: an all-expired SSTable (`max_deletion_time < gcBefore`) is classified
    /// fully expired.
    #[test]
    fn is_fully_expired_true_when_max_ldt_below_gc_before() {
        assert!(is_fully_expired(&ts_stats(900, 0), 1_000));
    }

    /// R1: `max_deletion_time` at or above `gcBefore` is NOT fully expired (the
    /// strict `<` predicate retains the boundary, matching Cassandra).
    #[test]
    fn is_fully_expired_false_at_or_above_gc_before() {
        assert!(!is_fully_expired(&ts_stats(1_000, 0), 1_000));
        assert!(!is_fully_expired(&ts_stats(1_500, 0), 1_000));
    }

    /// R1: the LIVE / NO_DELETION_TIME sentinel (`i64::MAX`) is never expired.
    #[test]
    fn is_fully_expired_false_for_live_sentinel() {
        assert!(!is_fully_expired(&ts_stats(i64::MAX, 0), 1_000));
    }

    /// Write a synthetic Statistics.db beside a zero-byte Data.db, returning the
    /// Data.db path. `max_ldt == i32::MAX` = "no tombstones / live" (parses back as
    /// the `i64::MAX` NO_DELETION_TIME sentinel).
    fn write_expiry_stats(dir: &Path, gen: u32, max_ldt: i32, max_ts: i64, min_ts: i64) -> PathBuf {
        let data_path = dir.join(format!("nb-{gen}-big-Data.db"));
        std::fs::write(&data_path, b"synthetic-data-never-read").expect("touch Data.db");
        let stats_path = dir.join(format!("nb-{gen}-big-Statistics.db"));
        let mut meta = StatisticsMetadata::new();
        meta.max_local_deletion_time = max_ldt;
        meta.min_local_deletion_time = max_ldt.min(0);
        meta.max_timestamp = max_ts;
        meta.min_timestamp = min_ts;
        StatisticsWriter::new(stats_path)
            .write(&meta, None)
            .expect("write Statistics.db");
        data_path
    }

    /// R1 scenario "classified fully expired without a cell scan": the drop-set is
    /// computed purely from Statistics.db metadata. Proven by DELETING the
    /// candidate's Data.db after writing its Statistics.db — classification still
    /// succeeds because it never touches Data.db.
    #[test]
    fn fully_expired_detection_reads_no_data_db() {
        let tmp = TempDir::new().expect("temp dir");
        let cand = write_expiry_stats(tmp.path(), 1, 500, 10_000, 5_000);
        std::fs::remove_file(&cand).expect("remove Data.db to prove no read");

        let dropped = fully_expired_sstables(&[cand.clone()], &[], Some(1_000));
        assert_eq!(
            dropped,
            vec![cand],
            "fully-expired classification must come from Statistics.db alone, no Data.db read"
        );
    }

    /// R1 scenario: a live SSTable is never dropped.
    #[test]
    fn live_sstable_never_dropped() {
        let tmp = TempDir::new().expect("temp dir");
        let live = write_expiry_stats(tmp.path(), 1, i32::MAX, 10_000, 5_000);
        let dropped = fully_expired_sstables(&[live], &[], Some(1_000));
        assert!(dropped.is_empty(), "a live SSTable must never be dropped");
    }

    /// R1 scenario: `gcBefore == None` drops nothing, even for an expired SSTable.
    #[test]
    fn fully_expired_none_gc_before_drops_nothing() {
        let tmp = TempDir::new().expect("temp dir");
        let cand = write_expiry_stats(tmp.path(), 1, 500, 10_000, 5_000);
        let dropped = fully_expired_sstables(&[cand], &[], None);
        assert!(
            dropped.is_empty(),
            "None gcBefore must disable dropping (conservative)"
        );
    }

    /// R1 scenario: an unreadable/absent candidate Statistics.db drops nothing.
    #[test]
    fn fully_expired_unreadable_candidate_stats_drops_nothing() {
        let tmp = TempDir::new().expect("temp dir");
        let cand = tmp.path().join("nb-1-big-Data.db");
        std::fs::write(&cand, b"").expect("touch Data.db");
        let dropped = fully_expired_sstables(&[cand], &[], Some(1_000));
        assert!(
            dropped.is_empty(),
            "an unreadable candidate Statistics.db must not be dropped"
        );
    }

    /// R2 scenario: a fully-expired candidate that could shadow older data outside
    /// the set is RETAINED.
    #[test]
    fn overlap_gate_retains_shadowing_candidate() {
        let tmp = TempDir::new().expect("temp dir");
        let cand = write_expiry_stats(tmp.path(), 1, 500, 10_000, 8_000);
        // Outside min write ts (5_000) <= candidate max_timestamp (10_000) ⇒ retain.
        let outside = write_expiry_stats(tmp.path(), 2, i32::MAX, 20_000, 5_000);
        let dropped = fully_expired_sstables(&[cand], &[outside], Some(1_000));
        assert!(
            dropped.is_empty(),
            "a candidate that could shadow older outside data must be retained"
        );
    }

    /// R2 scenario: a fully-expired candidate older than everything outside the
    /// set is DROPPED.
    #[test]
    fn overlap_gate_drops_candidate_older_than_outside() {
        let tmp = TempDir::new().expect("temp dir");
        let cand = write_expiry_stats(tmp.path(), 1, 500, 4_000, 1_000);
        // Outside min write ts (5_000) > candidate max_timestamp (4_000) ⇒ drop.
        let outside = write_expiry_stats(tmp.path(), 2, i32::MAX, 20_000, 5_000);
        let dropped = fully_expired_sstables(&[cand.clone()], &[outside], Some(1_000));
        assert_eq!(
            dropped,
            vec![cand],
            "a candidate older than every outside SSTable must be dropped"
        );
    }

    /// R2 scenario: a major/full compaction (empty outside set) drops every
    /// fully-expired candidate (+inf overlap bound), keeping live.
    #[test]
    fn major_compaction_empty_outside_drops_all_expired() {
        let tmp = TempDir::new().expect("temp dir");
        let expired_a = write_expiry_stats(tmp.path(), 1, 500, 10_000, 5_000);
        let expired_b = write_expiry_stats(tmp.path(), 2, 800, i64::MAX, 5_000);
        let live = write_expiry_stats(tmp.path(), 3, i32::MAX, 10_000, 5_000);
        let dropped = fully_expired_sstables(
            &[expired_a.clone(), live, expired_b.clone()],
            &[],
            Some(1_000),
        );
        assert_eq!(
            dropped,
            vec![expired_a, expired_b],
            "a major compaction drops every fully-expired input (+inf bound), keeping live"
        );
    }

    /// R2 scenario: an UNKNOWN outside bound in a PARTIAL compaction retains ALL
    /// fully-expired candidates.
    #[test]
    fn unknown_outside_bound_retains_all() {
        let tmp = TempDir::new().expect("temp dir");
        let cand = write_expiry_stats(tmp.path(), 1, 500, 4_000, 1_000);
        // Outside Data.db with NO sibling Statistics.db ⇒ unknown bound.
        let outside = tmp.path().join("nb-2-big-Data.db");
        std::fs::write(&outside, b"").expect("touch outside Data.db");
        let dropped = fully_expired_sstables(&[cand], &[outside], Some(1_000));
        assert!(
            dropped.is_empty(),
            "an unknown outside bound in a partial compaction must retain all candidates"
        );
    }
}
