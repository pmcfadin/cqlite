//! Unit tests for [`super`] (stats_writer/metadata.rs).
//!
//! Loaded via `#[path]` from `metadata.rs` so the production module stays
//! under the campsite-rule size target (issue #4246, epic #1135).

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

/// Issue #1728: a live non-TTL `Write` cell folds `NO_DELETION_TIME`
/// (`i32::MAX`) into `max_local_deletion_time`, matching Cassandra's
/// MetadataCollector. In a MIXED SSTable (an older tombstone at a smaller
/// real LDT plus a live cell) `max_local_deletion_time` must be the live
/// sentinel, NOT the tombstone LDT — while `min_local_deletion_time` and the
/// tombstone drop-time histogram continue to describe the real tombstone
/// only (issue #851 chokepoint invariants preserved).
#[test]
fn test_note_live_local_deletion_time_mixed_sstable() {
    let mut meta = StatisticsMetadata::new();
    // An older tombstone at a real, smaller LDT.
    meta.update_local_deletion_time(1_500_000_000);
    // A live non-TTL cell in the same (mixed) SSTable.
    meta.note_live_local_deletion_time();

    assert_eq!(
        meta.max_local_deletion_time,
        i32::MAX,
        "a live cell must lift max_local_deletion_time to the NO_DELETION_TIME sentinel"
    );
    assert_eq!(
        meta.min_local_deletion_time, 1_500_000_000,
        "the live sentinel must not poison min_local_deletion_time"
    );
    assert_eq!(
        meta.tombstone_histogram.size(),
        1,
        "the live cell must not be counted as a tombstone in the drop-time histogram"
    );

    // The sentinel survives finalize (only i32::MIN normalizes to 0), so the
    // STATS component serialises the live NO_DELETION_TIME sentinel.
    meta.finalize();
    assert_eq!(meta.max_local_deletion_time, i32::MAX);
}

/// Issue #1728: a pure-live SSTable (only live `Write` cells, no tombstone)
/// ends with `max_local_deletion_time == i32::MAX`, which serialises to the
/// same `NO_DELETION_TIME` sentinel as the untouched `== 0` "no deletions"
/// case — so pure-live output byte-parity is unchanged.
#[test]
fn test_note_live_local_deletion_time_pure_live() {
    let mut meta = StatisticsMetadata::new();
    meta.note_live_local_deletion_time();

    assert_eq!(meta.max_local_deletion_time, i32::MAX);
    assert_eq!(
        meta.min_local_deletion_time,
        i32::MAX,
        "no tombstone recorded: min stays at its unset sentinel"
    );
    assert!(
        meta.tombstone_histogram.is_empty(),
        "a live cell must not create a tombstone histogram bin"
    );

    meta.finalize();
    // min unset -> normalized to 0; max holds the live sentinel.
    assert_eq!(meta.min_local_deletion_time, 0);
    assert_eq!(meta.max_local_deletion_time, i32::MAX);
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

// -----------------------------------------------------------------------
// Issue #1668 stage 5a: prove the per-mutation stats fold is associative /
// order-independent, so `write_partition`'s baseline computation does NOT
// require the whole partition's `Vec<Mutation>` at once once baselines are
// pre-seeded (issue #729) — it can be fed incrementally, one row at a
// time, across any grouping or ordering of the SAME underlying values.
// -----------------------------------------------------------------------

/// `update_timestamp`/`update_local_deletion_time`/`update_ttl` are pure
/// min/max folds (see their doc comments): feeding the SAME set of values
/// in a DIFFERENT order (or split across many separate "calls" instead of
/// one batch) must produce an IDENTICAL final `min_timestamp` /
/// `max_timestamp` / `min_local_deletion_time` / `max_local_deletion_time`
/// / `min_ttl` / `max_ttl`. This is the concrete proof (not just a code
/// read) that a caller can stream rows into these folds one at a time
/// instead of pre-collecting a whole `Vec<Mutation>` first.
#[test]
fn stats_fold_is_order_independent_issue_1668_stage_5a() {
    // A representative fixture: several distinct timestamps, local
    // deletion times, and TTLs, INCLUDING the LIVE sentinels that must be
    // filtered identically regardless of order (issue #851).
    let timestamps = [
        500_000i64,
        2_000_000,
        1_000_000,
        i64::MIN,
        i64::MAX,
        750_000,
    ];
    let ldts = [1_700_000_000i32, 1_650_000_000, 1_690_000_000, i32::MAX];
    let ttls = [3600i32, 60, 86400, 0, -1];

    // Batch order A: as declared above (mirrors `write_partition`'s
    // single forward `for mutation in &mutations` pass).
    let mut batch_a = StatisticsMetadata::new();
    for &ts in &timestamps {
        batch_a.update_timestamp(ts);
    }
    for &ldt in &ldts {
        batch_a.update_local_deletion_time(ldt);
    }
    for &ttl in &ttls {
        batch_a.update_ttl(ttl);
    }

    // Batch order B: REVERSED and INTERLEAVED — simulates rows arriving
    // one cluster group at a time, in a different relative order, across
    // (hypothetically) multiple incremental calls.
    let mut batch_b = StatisticsMetadata::new();
    for &ttl in ttls.iter().rev() {
        batch_b.update_ttl(ttl);
    }
    for &ts in timestamps.iter().rev() {
        batch_b.update_timestamp(ts);
    }
    for &ldt in ldts.iter().rev() {
        batch_b.update_local_deletion_time(ldt);
    }

    // Batch order C: fully interleaved, one value of EACH kind per
    // "cluster group" — the closest analogue to a genuine per-row stream.
    let mut batch_c = StatisticsMetadata::new();
    let n = timestamps.len().max(ldts.len()).max(ttls.len());
    for i in 0..n {
        if let Some(&ts) = timestamps.get(i) {
            batch_c.update_timestamp(ts);
        }
        if let Some(&ldt) = ldts.get(i) {
            batch_c.update_local_deletion_time(ldt);
        }
        if let Some(&ttl) = ttls.get(i) {
            batch_c.update_ttl(ttl);
        }
    }

    for (name, batch) in [("B (reversed)", &batch_b), ("C (interleaved)", &batch_c)] {
        assert_eq!(
            batch.min_timestamp, batch_a.min_timestamp,
            "min_timestamp diverged for order {name}"
        );
        assert_eq!(
            batch.max_timestamp, batch_a.max_timestamp,
            "max_timestamp diverged for order {name}"
        );
        assert_eq!(
            batch.min_local_deletion_time, batch_a.min_local_deletion_time,
            "min_local_deletion_time diverged for order {name}"
        );
        assert_eq!(
            batch.max_local_deletion_time, batch_a.max_local_deletion_time,
            "max_local_deletion_time diverged for order {name}"
        );
        assert_eq!(
            batch.min_ttl, batch_a.min_ttl,
            "min_ttl diverged for order {name}"
        );
        assert_eq!(
            batch.max_ttl, batch_a.max_ttl,
            "max_ttl diverged for order {name}"
        );
        assert_eq!(
            batch.tombstone_histogram.size(),
            batch_a.tombstone_histogram.size(),
            "tombstone_histogram bin count diverged for order {name}"
        );
    }

    // Sanity: the LIVE sentinels were actually exercised and correctly
    // excluded (issue #851) — the real values, not the sentinels, won.
    assert_eq!(batch_a.min_timestamp, 500_000);
    assert_eq!(batch_a.max_timestamp, 2_000_000);
    assert_eq!(batch_a.min_local_deletion_time, 1_650_000_000);
    assert_eq!(batch_a.max_local_deletion_time, 1_700_000_000);
    assert_eq!(batch_a.min_ttl, 60);
    assert_eq!(batch_a.max_ttl, 86_400);
}
