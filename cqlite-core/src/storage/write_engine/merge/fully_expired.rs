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
//!
//! The whole module is gated `#[cfg(feature = "write-support")]` at its `mod`
//! declaration in the parent, so no inner `#![cfg]` is needed here.

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

/// The authoritative newest write timestamp of an SSTable, or `None` when it is
/// UNAVAILABLE (#1729 fail-closed `i64::MIN` sentinel).
///
/// Mirrors [`crate::storage::sstable::statistics_reader::StatisticsReader::max_timestamp`]:
/// a drop/GC gate must never treat an unknown newest write as `i64::MIN` (which
/// would compare below any overlap bound and wrongly permit a drop). Returning
/// `None` here forces the overlap gate to FAIL CLOSED (retain the candidate).
fn authoritative_max_timestamp(stats: &TimestampStatistics) -> Option<i64> {
    // `max_timestamp` is already `Option` (issue #1653): `Some` only when the
    // authoritative maximum was decoded, `None` when unavailable. Propagate it
    // directly — a `None` forces the overlap gate to FAIL CLOSED.
    stats.max_timestamp
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
///    [`is_fully_expired`]). Since #1728 the writer stamps live cells with the
///    `NO_DELETION_TIME` sentinel, so a MIXED SSTable (an old tombstone below
///    `gcBefore` PLUS any live non-TTL cell) finalizes `max_deletion_time` as the
///    `i32::MAX` live sentinel (parsed back as `i64::MAX`), which is never
///    `< gc_before_secs`. Such a mixed SSTable is therefore correctly NOT
///    fully-expired — closing the former data-loss gap (roborev F1) with the now
///    authoritative `max_local_deletion_time`.
/// 2. **Overlap-safe** — its authoritative `max_timestamp` is STRICTLY LESS THAN
///    the minimum write timestamp across every overlapping SSTable that is NOT
///    itself fully-expired: both the OUTSIDE overlapping SSTables (`outside_paths`)
///    AND the NON-EXPIRED inputs of THIS compaction. If the candidate's newest
///    write predates all of them, nothing it holds (tombstone or data) can shadow
///    live data that would otherwise survive, so dropping it can never resurrect
///    data. Mirrors Cassandra `CompactionController.getFullyExpiredSSTables`, which
///    folds the min write timestamp of the non-fully-expired *compacting* SSTables
///    into the same overlap bound as the non-compacting overlapping ones. The
///    non-compacting bound reuses [`compute_max_purgeable_timestamp`] (the coarse
///    global-`min_timestamp` #935 gate; OQ-2 → (A), key-range precision deferred).
///    Since #1729 the candidate's `max_timestamp` may be the `i64::MIN` UNAVAILABLE
///    sentinel; a candidate whose newest write is unknown CANNOT be proven to
///    predate the overlap bound, so it FAILS CLOSED (is retained, never dropped) —
///    closing the former resurrection gap (roborev F2).
///
/// A non-expired INPUT is included in the bound because dropping a fully-expired
/// SSTable whose tombstone shadows an OLDER live cell in a *co-compacting* input
/// would resurrect that cell: the tombstone (dropped) no longer purges it, and the
/// co-input's live cell is merged into the output. (A fully-expired co-input is NOT
/// folded in: its own data is past `gcBefore` and would be purged anyway, so a
/// tombstone shadowing it can never resurrect surviving data.)
///
/// Conservatism (never resurrect data, never drop live data):
/// - `gc_before_secs == None` (invalid/absent gc_grace disables purging) ⇒ empty
///   drop-set.
/// - An empty `outside_paths` AND no non-expired inputs (e.g. a FULL/major
///   compaction of only-expired SSTables) ⇒ the bound is `+inf` ⇒ every candidate
///   is droppable.
/// - A non-empty `outside_paths` whose bound is UNKNOWN
///   (`compute_max_purgeable_timestamp` returned `None` because an outside
///   `Statistics.db` was unreadable) ⇒ empty drop-set (cannot prove safety).
/// - A candidate whose own `Statistics.db` is absent/unreadable ⇒ not droppable
///   (it is treated as non-expired, and its `min_timestamp` is unknown, so it also
///   cannot lower the bound — the whole drop-set is disabled to stay safe).
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

    // Read each input's timestamp stats ONCE. A candidate whose own Statistics.db
    // is unreadable is treated as non-expired (not droppable) AND, because its
    // min_timestamp is then unknown, it cannot be safely folded into the overlap
    // bound — so an unreadable INPUT disables the whole drop-set (conservative).
    let mut input_stats: Vec<(&PathBuf, Option<TimestampStatistics>)> =
        Vec::with_capacity(input_paths.len());
    for p in input_paths {
        input_stats.push((p, read_timestamp_stats(p)));
    }

    // Outside overlap bound (non-compacting overlapping SSTables). An empty outside
    // set contributes +inf; a non-empty set with an UNKNOWN bound (an outside
    // Statistics.db unreadable) ⇒ cannot prove safety ⇒ drop nothing.
    let outside_bound: Option<i64> = if outside_paths.is_empty() {
        Some(i64::MAX)
    } else {
        match compute_max_purgeable_timestamp(outside_paths) {
            Some(bound) => Some(bound),
            None => return Vec::new(),
        }
    };

    classify_drop_set(&input_stats, outside_bound, gc_before)
}

/// Pure drop-set classifier over already-read stats (no I/O), shared by
/// [`fully_expired_sstables`] and its regression tests.
///
/// `outside_bound` is the overlap bound from the OUTSIDE (non-compacting) SSTables:
/// `Some(i64::MAX)` for an empty outside set (+inf), `Some(b)` for a known bound,
/// and callers must have already returned an empty drop-set for an UNKNOWN outside
/// bound before calling this. It is here typed `Option` only so a caller can pass a
/// resolved bound; a `None` here fails closed (empty drop-set).
///
/// Returns the subset of paths (in order) that may be dropped whole. See
/// [`fully_expired_sstables`] for the full contract.
fn classify_drop_set(
    input_stats: &[(&PathBuf, Option<TimestampStatistics>)],
    outside_bound: Option<i64>,
    gc_before: i64,
) -> Vec<PathBuf> {
    let Some(mut overlap_bound) = outside_bound else {
        return Vec::new();
    };

    // Fold in the min write timestamp of every NON-EXPIRED input of THIS
    // compaction (Cassandra parity). A fully-expired input does not constrain the
    // bound (its own data is past gcBefore and purged anyway). An input whose
    // stats could not be read is NON-expired with an UNKNOWN min_timestamp, which
    // we cannot fold safely ⇒ disable the drop-set.
    for (_, stats) in input_stats {
        match stats {
            Some(s) if is_fully_expired(s, gc_before) => {}
            Some(s) => overlap_bound = overlap_bound.min(s.min_timestamp),
            None => return Vec::new(),
        }
    }

    input_stats
        .iter()
        .filter_map(|(path, stats)| {
            let stats = stats.as_ref()?;
            // Fully expired AND provably shadows nothing that would otherwise
            // survive (its newest write predates every non-expired overlap).
            // #1729: an UNAVAILABLE max_timestamp (i64::MIN sentinel) cannot be
            // proven to predate the bound ⇒ fail closed (retain, never drop).
            let candidate_max_ts = authoritative_max_timestamp(stats)?;
            if is_fully_expired(stats, gc_before) && candidate_max_ts < overlap_bound {
                Some((*path).clone())
            } else {
                None
            }
        })
        .collect()
}

/// Split `input_paths` into `(merge_inputs, dropped_whole)` given a `drop_set`
/// (a subset of `input_paths`, e.g. from [`fully_expired_sstables`]).
///
/// `merge_inputs` is `input_paths` minus the drop-set — the SSTables the K-way
/// merger will actually read. `dropped_whole` is the FULL drop-set: the SSTables
/// proven fully expired + overlap-safe, to be reported and reclaimed after
/// publish.
///
/// Degenerate all-dropped guard: the merger requires at least one input, so when
/// the drop-set is EVERY input (a major compaction of an only-expired input set),
/// ONE dropped SSTable is ALSO retained in `merge_inputs` (its rows purge to empty
/// through the normal merge path) so the merger has a source. That retained
/// SSTable stays in `dropped_whole` too, so the drop is reported honestly and
/// reclaimed on both surfaces (roborev #1388 Medium): the reclaim loops dedupe
/// against `merge_inputs` (which the core WriteEngine path deletes anyway) so the
/// retained SSTable is deleted exactly once. Only the retained SSTable pays a read
/// cost; the rest are still excluded from the merge.
///
/// Shared by both compaction surfaces so the exclusion + all-dropped guard live in
/// one place (issue #1388).
pub(crate) fn split_merge_and_dropped(
    input_paths: &[PathBuf],
    drop_set: Vec<PathBuf>,
) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let dropped_lookup: std::collections::HashSet<&PathBuf> = drop_set.iter().collect();
    let mut merge_inputs: Vec<PathBuf> = input_paths
        .iter()
        .filter(|p| !dropped_lookup.contains(*p))
        .cloned()
        .collect();
    if !merge_inputs.is_empty() {
        return (merge_inputs, drop_set);
    }
    // Everything was dropped: retain one for the merger. It STAYS in the drop-set
    // so the drop is reported/reclaimed honestly; reclaim sites dedupe against
    // `merge_inputs` to delete it exactly once (roborev #1388 Medium).
    if let Some(retained) = drop_set.first() {
        merge_inputs.push(retained.clone());
    }
    (merge_inputs, drop_set)
}

/// Reclaim (best-effort component-delete) each SSTable in `dropped_whole` that the
/// caller is NOT already deleting via another path, calling `delete` on the
/// survivors. The degenerate all-dropped guard retains one dropped SSTable in BOTH
/// `dropped_whole` and `merge_inputs` (see [`split_merge_and_dropped`]), so it is
/// reclaimed exactly once:
///
/// - Core WriteEngine surface: it deletes ALL merge inputs separately, so it
///   passes `already_deleted = merge_inputs` here to skip the retained one (avoid a
///   double-delete + spurious orphan warning).
/// - CLI one-shot surface: it deletes NO merge inputs (the operator owns the input
///   dir; output lands in a separate `--output` dir), so it passes an EMPTY
///   `already_deleted` and the retained SSTable IS reclaimed here — closing the
///   former "all-expired input left on disk" gap (roborev #1388 Medium).
///
/// `delete` is the surface's own best-effort delete (logs, never errors), so this
/// helper only decides the set to reclaim.
pub(crate) fn reclaim_dropped_whole<F: FnMut(&Path)>(
    dropped_whole: &[PathBuf],
    already_deleted: &[PathBuf],
    mut delete: F,
) {
    let skip: std::collections::HashSet<&PathBuf> = already_deleted.iter().collect();
    for dropped in dropped_whole {
        if skip.contains(dropped) {
            continue;
        }
        delete(dropped);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
    use tempfile::TempDir;

    /// Construct a `TimestampStatistics` with the two fields the drop decision
    /// consults set explicitly; the rest are irrelevant.
    fn ts_stats(max_deletion_time: i64, max_timestamp: Option<i64>) -> TimestampStatistics {
        TimestampStatistics {
            min_timestamp: 0,
            max_timestamp,
            min_deletion_time: 0,
            max_deletion_time,
            min_ttl: None,
            max_ttl: None,
            rows_with_ttl: None,
        }
    }

    /// R1: an all-expired SSTable (`max_deletion_time < gcBefore`) is classified
    /// fully expired.
    #[test]
    fn is_fully_expired_true_when_max_ldt_below_gc_before() {
        assert!(is_fully_expired(&ts_stats(900, Some(0)), 1_000));
    }

    /// R1: `max_deletion_time` at or above `gcBefore` is NOT fully expired (the
    /// strict `<` predicate retains the boundary, matching Cassandra).
    #[test]
    fn is_fully_expired_false_at_or_above_gc_before() {
        assert!(!is_fully_expired(&ts_stats(1_000, Some(0)), 1_000));
        assert!(!is_fully_expired(&ts_stats(1_500, Some(0)), 1_000));
    }

    /// R1: the LIVE / NO_DELETION_TIME sentinel (`i64::MAX`) is never expired.
    #[test]
    fn is_fully_expired_false_for_live_sentinel() {
        assert!(!is_fully_expired(&ts_stats(i64::MAX, Some(0)), 1_000));
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
    /// fully-expired candidate whose newest write predates the co-compacting live
    /// input's oldest write, keeping the live input. Expired data is OLDER than the
    /// live data (the realistic case), so both expired inputs are droppable.
    #[test]
    fn major_compaction_empty_outside_drops_expired_older_than_live() {
        let tmp = TempDir::new().expect("temp dir");
        // Expired inputs at LOW write timestamps (older than the live input).
        let expired_a = write_expiry_stats(tmp.path(), 1, 500, 3_000, 1_000);
        let expired_b = write_expiry_stats(tmp.path(), 2, 800, 4_000, 1_000);
        // Live co-input at a HIGHER min write ts (5_000): the expired tombstones
        // (max_ts 3_000/4_000) predate it, so dropping them resurrects nothing.
        let live = write_expiry_stats(tmp.path(), 3, i32::MAX, 10_000, 5_000);
        let dropped = fully_expired_sstables(
            &[expired_a.clone(), live, expired_b.clone()],
            &[],
            Some(1_000),
        );
        assert_eq!(
            dropped,
            vec![expired_a, expired_b],
            "a major compaction drops fully-expired inputs older than the live co-input, keeping live"
        );
    }

    /// R2 scenario (Cassandra parity / #1384 regression guard): a fully-expired
    /// input whose tombstone could shadow OLDER live data in a CO-COMPACTING input
    /// is NOT dropped — even in a full compaction (empty outside set). Folding the
    /// non-expired input's min write timestamp into the overlap bound prevents the
    /// resurrection that dropping the tombstone whole would otherwise cause.
    #[test]
    fn co_input_shadowing_expired_sstable_not_dropped() {
        let tmp = TempDir::new().expect("temp dir");
        // Fully-expired input holding a tombstone at a HIGH write ts (10_000): it
        // would shadow the live co-input's older data.
        let expired = write_expiry_stats(tmp.path(), 1, 500, 10_000, 9_000);
        // Live co-input with OLDER data (min write ts 5_000 <= tombstone max_ts).
        let live = write_expiry_stats(tmp.path(), 2, i32::MAX, 8_000, 5_000);
        let dropped = fully_expired_sstables(&[expired, live], &[], Some(1_000));
        assert!(
            dropped.is_empty(),
            "an expired input whose tombstone shadows a co-input's older live data must not be dropped"
        );
    }

    /// Roborev F2 (former resurrection) regression: the overlap gate compares the
    /// candidate's TRUE NEWEST write (`max_timestamp`, #1729-authoritative), not its
    /// `min_timestamp`. A candidate whose `min_timestamp < overlap_bound <
    /// max_timestamp` (an old tombstone min but a NEWER tombstone max) is RETAINED —
    /// its newest write does NOT predate the outside data, so dropping it could
    /// resurrect data. A gate that mistakenly used `min_timestamp` would drop it.
    #[test]
    fn overlap_gate_uses_max_timestamp_not_min_retains_when_max_above_bound() {
        let tmp = TempDir::new().expect("temp dir");
        // Candidate: fully expired (LDT 500 < 1_000), min write ts 1_000 (OLD),
        // max write ts 8_000 (NEW — a later tombstone/write in the same SSTable).
        let cand = write_expiry_stats(tmp.path(), 1, 500, 8_000, 1_000);
        // Outside min write ts = 5_000 ⇒ overlap_bound = 5_000. The candidate's
        // min (1_000) is BELOW the bound, but its max (8_000) is ABOVE it.
        let outside = write_expiry_stats(tmp.path(), 2, i32::MAX, 20_000, 5_000);
        let dropped = fully_expired_sstables(&[cand], &[outside], Some(1_000));
        assert!(
            dropped.is_empty(),
            "min<bound<max must RETAIN: the gate compares the true newest write \
             (max_timestamp), so a candidate whose newest write postdates the bound \
             is not droppable (F2 resurrection guard)"
        );
    }

    /// Roborev F2 fail-closed: since #1729 a candidate's `max_timestamp` may be the
    /// `i64::MIN` UNAVAILABLE sentinel. An unknown newest write cannot be proven to
    /// predate the overlap bound, so the candidate is RETAINED (never dropped) even
    /// though it is fully expired and its (unavailable) sentinel would compare below
    /// any real bound. Exercised through the pure classifier so the in-memory
    /// sentinel is honest (the on-disk StatisticsWriter finalize normalizes
    /// `i64::MIN → 0`, so it cannot round-trip the sentinel).
    #[test]
    fn overlap_gate_fails_closed_on_unavailable_max_timestamp() {
        let cand: PathBuf = PathBuf::from("nb-1-big-Data.db");
        // Fully expired (LDT 500 < 1_000) but max_timestamp is unavailable
        // (`None`, issue #1653/#1729). A naive "unavailable compares below bound"
        // would wrongly permit a drop; the authoritative accessor forces a retain.
        let stats = ts_stats(500, None);
        let input_stats = [(&cand, Some(stats))];
        // Concrete, above-sentinel outside bound (5_000).
        let dropped = classify_drop_set(&input_stats, Some(5_000), 1_000);
        assert!(
            dropped.is_empty(),
            "an UNAVAILABLE (None) max_timestamp must fail closed: retain, never drop (#1729 F2/#1653)"
        );

        // Control: the SAME candidate with a real, below-bound max_timestamp IS
        // droppable, proving it is the unavailable case (not some other field)
        // that retains.
        let live_max = ts_stats(500, Some(4_000));
        let input_stats_live = [(&cand, Some(live_max))];
        assert_eq!(
            classify_drop_set(&input_stats_live, Some(5_000), 1_000),
            vec![cand.clone()],
            "a concrete below-bound max_timestamp on the same candidate is droppable"
        );
    }

    /// The [`authoritative_max_timestamp`] accessor mirrors #1729/#1653: `None`
    /// when the max is unavailable, `Some(v)` otherwise (incl. concrete negatives).
    #[test]
    fn authoritative_max_timestamp_maps_sentinel_to_none() {
        assert_eq!(authoritative_max_timestamp(&ts_stats(0, None)), None);
        assert_eq!(
            authoritative_max_timestamp(&ts_stats(0, Some(8_000))),
            Some(8_000)
        );
        assert_eq!(
            authoritative_max_timestamp(&ts_stats(0, Some(i64::MIN + 1))),
            Some(i64::MIN + 1)
        );
    }

    /// Roborev #1388 (Medium): the all-dropped guard retains ONE input for the
    /// merger but keeps the FULL drop-set in `dropped_whole` (so nothing is
    /// underreported or left unreclaimed). The retained input appears in BOTH
    /// returned lists; reclaim sites dedup it.
    #[test]
    fn split_all_dropped_retains_one_but_reports_full_drop_set() {
        let a = PathBuf::from("nb-1-big-Data.db");
        let b = PathBuf::from("nb-2-big-Data.db");
        let inputs = [a.clone(), b.clone()];
        let drop_set = vec![a.clone(), b.clone()];

        let (merge_inputs, dropped_whole) = split_merge_and_dropped(&inputs, drop_set);
        assert_eq!(
            merge_inputs.len(),
            1,
            "exactly one input is retained for the merger's at-least-one requirement"
        );
        assert_eq!(
            dropped_whole,
            vec![a, b],
            "the FULL drop-set is still reported (no underreporting)"
        );
        assert!(
            dropped_whole.contains(&merge_inputs[0]),
            "the retained merger input is also in the reported drop-set"
        );
    }

    /// A partial drop-set (a live input survives) leaves `merge_inputs` non-empty
    /// and `dropped_whole` unchanged — the guard does not fire.
    #[test]
    fn split_partial_drop_leaves_live_input_and_full_drop_set() {
        let expired = PathBuf::from("nb-1-big-Data.db");
        let live = PathBuf::from("nb-2-big-Data.db");
        let inputs = [expired.clone(), live.clone()];
        let (merge_inputs, dropped_whole) = split_merge_and_dropped(&inputs, vec![expired.clone()]);
        assert_eq!(merge_inputs, vec![live], "only the live input is merged");
        assert_eq!(dropped_whole, vec![expired], "the expired input is dropped");
    }

    /// [`reclaim_dropped_whole`] deletes only the drops NOT already deleted by the
    /// caller: the core surface (which deletes all merge inputs) skips the retained
    /// one; the CLI surface (empty `already_deleted`) reclaims every drop.
    #[test]
    fn reclaim_dedups_against_already_deleted() {
        let a = PathBuf::from("nb-1-big-Data.db");
        let b = PathBuf::from("nb-2-big-Data.db");
        let dropped_whole = [a.clone(), b.clone()];

        // Core surface: `b` is the retained merge input (already deleted) ⇒ only `a`.
        let mut core_deleted = Vec::new();
        reclaim_dropped_whole(&dropped_whole, &[b.clone()], |p| {
            core_deleted.push(p.to_path_buf())
        });
        assert_eq!(
            core_deleted,
            vec![a.clone()],
            "core surface skips the retained merge input to avoid a double-delete"
        );

        // CLI surface: nothing pre-deleted ⇒ both reclaimed here.
        let mut cli_deleted = Vec::new();
        reclaim_dropped_whole(&dropped_whole, &[], |p| cli_deleted.push(p.to_path_buf()));
        assert_eq!(
            cli_deleted,
            vec![a, b],
            "CLI surface reclaims every drop (it deletes no merge inputs otherwise)"
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
