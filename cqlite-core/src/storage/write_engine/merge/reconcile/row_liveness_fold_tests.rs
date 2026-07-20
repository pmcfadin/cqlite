//! Issue #2374/#2789: the cross-generation row-marker liveness FOLD in
//! [`ReconcileState`] (Step 1 —
//! `self.row_liveness = self.row_liveness.merge(entry.row_liveness)`) must carry
//! the timestamp-LWW winner onto the emitted [`MergeEntry`].
//!
//! ## Gap these tests close (roborev Low)
//!
//! `RowLiveness::merge` (the pure fold) and the single-entry `apply_*_shadowing`
//! carry-forward each have direct tests, but the MULTI-entry fold through the
//! REAL `ReconcileState` add/consume path — reconciling two `MergeEntry`s that
//! carry DIFFERENT markers for the same key into ONE emitted entry — was only
//! exercised end-to-end by the Flight parity lane, which SKIPs when fixtures are
//! absent. A regression that dropped or mis-ordered the fold would then pass
//! unnoticed in a clean checkout. These tests drive the production reconcile
//! (`KWayMerger::reconcile_cluster_with_overlap`, the same kernel
//! `merge_partition_rows` calls) so the fold is asserted directly.
//!
//! ## Oracle
//!
//! [`RowLiveness::merge`] is Cassandra last-write-wins on the marker WRITE
//! timestamp: the marker with the higher `marker_timestamp` wins OUTRIGHT (its
//! expiry / live-forever state taken as-is). So folding a live-forever marker
//! @ts=200 with a TTL'd marker @ts=300 must yield the ts=300 marker regardless of
//! fold order, and the emitted row's `marker_live_at` must reflect 300's expiry
//! (HIDDEN once `now` passes it), never 200's live-forever visibility.

#![cfg(feature = "write-support")]

use super::super::{CellData, KWayMerger, MergeEntry, RowData};
use crate::storage::sstable::reader::compaction_row::RowLiveness;
use crate::storage::write_engine::mutation::DecoratedKey;
use crate::types::Value;
use std::collections::HashMap;

/// A decorated key from a single token byte.
fn dk(byte: u8) -> DecoratedKey {
    DecoratedKey::from_key_bytes(vec![byte]).expect("token")
}

/// A live entry for the shared (unclustered) key carrying one data cell so the
/// reconciled row survives as `RowData::Live` (and thus carries `row_liveness`
/// through Step 4), plus a primary-key liveness marker.
fn live_entry_with_marker(run_index: usize, cell_val: i32, marker: RowLiveness) -> MergeEntry {
    MergeEntry::new(
        run_index,
        dk(1),
        None,
        marker.marker_timestamp.unwrap_or(0),
        RowData::Live {
            cells: vec![CellData::new(
                "v".to_string(),
                Value::Integer(cell_val),
                marker.marker_timestamp.unwrap_or(0),
            )],
        },
    )
    .with_row_liveness(marker)
}

/// A live-forever marker at write timestamp `ts`.
fn live_forever(ts: i64) -> RowLiveness {
    RowLiveness {
        has_marker: true,
        expires_at_seconds: None,
        marker_timestamp: Some(ts),
    }
}

/// A TTL'd marker at write timestamp `ts` expiring at epoch second `expiry`.
fn ttl_marker(ts: i64, expiry: i64) -> RowLiveness {
    RowLiveness {
        has_marker: true,
        expires_at_seconds: Some(expiry),
        marker_timestamp: Some(ts),
    }
}

/// Reconcile the two entries through the production kernel and return the
/// emitted entry's folded `row_liveness`.
fn fold(entries: Vec<MergeEntry>) -> RowLiveness {
    let out = KWayMerger::reconcile_cluster_with_overlap(
        None,
        entries,
        &HashMap::new(),
        None,     // no gc purge
        i64::MAX, // full compaction (overlap gate open)
    )
    .expect("a live row with surviving data must be emitted");
    out.row_liveness
}

/// Older live-forever @ts=200 folded with a NEWER expired-TTL @ts=300 → the
/// ts=300 marker wins outright: emitted `marker_timestamp == 300`,
/// `expires_at_seconds == 300's`, and the row is HIDDEN once `now` passes 300's
/// expiry (a most-permissive union would have kept it live-forever/VISIBLE).
/// Fold order must not matter, so both feed orders are asserted.
///
/// This would FAIL if the Step 1 fold were dropped: the accumulator's
/// `row_liveness` would stay `RowLiveness::default()` (`has_marker == false`,
/// `marker_timestamp == None`), so both assertions on the winning marker's
/// timestamp and its expired visibility would break.
#[test]
fn reconcile_folds_two_markers_timestamp_lww_wins() {
    let expiry = 1_000; // epoch second the ts=300 TTL marker expired at.
    let older = live_forever(200);
    let newer = ttl_marker(300, expiry);

    for (a, b, order) in [
        (older, newer, "older-then-newer"),
        (newer, older, "newer-then-older"),
    ] {
        // run_index 0 = newest file; give the two entries distinct data values.
        let folded = fold(vec![
            live_entry_with_marker(0, 7, a),
            live_entry_with_marker(1, 9, b),
        ]);

        // The NEWER (ts=300) marker wins outright, taking its expiry as-is.
        assert_eq!(
            folded.marker_timestamp,
            Some(300),
            "timestamp-LWW: the ts=300 marker must win the fold ({order})"
        );
        assert_eq!(
            folded.expires_at_seconds,
            Some(expiry),
            "the winning ts=300 marker carries its own TTL expiry ({order})"
        );
        // Live/expired state is the winner's: HIDDEN after 300's expiry, and
        // the older live-forever marker did NOT leak its permanence.
        assert!(
            !folded.marker_live_at(expiry + 1),
            "row must be HIDDEN once now passes the winning marker's expiry ({order})"
        );
        assert!(
            folded.marker_live_at(expiry - 1),
            "row is still visible strictly before the winning marker's expiry ({order})"
        );
    }
}

/// Reverse polarity: older expired-TTL @ts=200 folded with a NEWER live-forever
/// @ts=300 → the ts=300 live-forever marker wins, so the row stays VISIBLE even
/// long after the older marker's expiry. Both feed orders asserted.
#[test]
fn reconcile_folds_newer_live_forever_wins() {
    let older = ttl_marker(200, 500);
    let newer = live_forever(300);

    for (a, b, order) in [
        (older, newer, "older-then-newer"),
        (newer, older, "newer-then-older"),
    ] {
        let folded = fold(vec![
            live_entry_with_marker(0, 1, a),
            live_entry_with_marker(1, 2, b),
        ]);

        assert_eq!(
            folded.marker_timestamp,
            Some(300),
            "timestamp-LWW: the ts=300 live-forever marker must win the fold ({order})"
        );
        assert_eq!(
            folded.expires_at_seconds, None,
            "the winning ts=300 marker is live-forever ({order})"
        );
        assert!(
            folded.marker_live_at(1_000_000),
            "a newer live-forever marker keeps the row visible indefinitely ({order})"
        );
    }
}
