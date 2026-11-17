//! Primary-key liveness MARKER steps of the reconcile kernel (issue #4581).
//!
//! A row's marker is Cassandra's `LivenessInfo`. Two things can remove it during a compaction, and
//! the ORDER is what keeps a purged deletion from leaving a ghost row:
//!
//! 1. [`shadow_marker_by_row_deletion`](ReconcileState::shadow_marker_by_row_deletion): the merge
//!    (`Row.Merger.merge`) drops a marker covered by the active deletion
//!    (`cassandra-5.0.8` `db/rows/Row.java:762-763`, `activeDeletion.deletes(rowInfo)`;
//!    `DeletionTime.deletes(long)`: `timestamp <= markedForDeleteAt`, `db/DeletionTime.java:173-176`).
//! 2. [`purge_marker`](ReconcileState::purge_marker): the purge (`BTreeRow.purge`,
//!    `db/rows/BTreeRow.java:459-465`) then drops an expired marker whose gc grace has elapsed
//!    (`DeletionPurger.shouldPurge(LivenessInfo, nowInSec)`, `db/DeletionPurger.java:31-34`).
//!
//! Cassandra runs the merge BEFORE the purge (`CompactionIterator` purges the merged output), so
//! step 1 must precede the row-deletion gc purge in `purge_gc_grace`: a row deletion purged first
//! would stop shadowing its marker and resurrect it as a ghost row.
//!
//! No other row deletion rule applies to the marker: `Row.Deletion.isShadowedBy(LivenessInfo)`
//! (`Row.java:430-433`) only fires for a SHADOWABLE (materialized-view, deprecated) deletion, which
//! CQL never writes. A REGULAR row deletion therefore survives a strictly newer marker, and the
//! row keeps both (confirmed on Cassandra-compacted output, `issue_4581_*`).

use super::ReconcileState;
use crate::storage::sstable::reader::compaction_row::RowLiveness;

impl ReconcileState {
    /// Drop the marker when the winning row deletion covers it (`marker_ts <= markedForDeleteAt`).
    /// A strictly newer marker is kept and coexists with the deletion.
    pub(in crate::storage::write_engine::merge) fn shadow_marker_by_row_deletion(&mut self) {
        let (Some(deleted_at), Some(marker_ts)) =
            (self.row_del, self.row_liveness.marker_timestamp)
        else {
            return;
        };
        if self.row_liveness.has_marker && marker_ts <= deleted_at {
            self.row_liveness = RowLiveness::default();
        }
    }

    /// Purge an EXPIRED marker (`!isLive(now)`: `localExpirationTime <= now`) once
    /// `localExpirationTime < gcBefore` and its own timestamp is `< max_purgeable_timestamp` — the
    /// same gate as every tombstone (`PurgeFunction`'s purger, `db/partitions/PurgeFunction.java:40-43`;
    /// overlap evaluator `CompactionController.getPurgeEvaluator`, `CompactionController.java:245-284`).
    /// An expired marker still inside gc_grace is kept verbatim (ts/TTL/LDT), not turned into a
    /// tombstone.
    ///
    /// `onlyPurgeRepairedTombstones` (`!(only && ldt >= oldestUnrepairedTombstone)`) is a table
    /// compaction option CQLite does not model for ANY tombstone (it defaults to `false`, under
    /// which the clause is vacuous, and mixed repair states are rejected up front,
    /// `repair_state.rs`); the marker goes through the identical gate as the row deletion.
    ///
    /// `gc_before_secs`/`now_secs` of `None` disable purging/expiry (a strict no-op, #845/#1382).
    /// Records `marker_purged` so a clustered key-only row (clustering pseudo-cells only) is
    /// dropped with it.
    pub(in crate::storage::write_engine::merge) fn purge_marker(
        &mut self,
        now_secs: Option<i64>,
        gc_before_secs: Option<i64>,
        max_purgeable_timestamp: i64,
    ) {
        let (Some(now), Some(gc_before)) = (now_secs, gc_before_secs) else {
            return;
        };
        let liveness = self.row_liveness;
        let (true, Some(expires_at), Some(marker_ts)) = (
            liveness.has_marker,
            liveness.expires_at_seconds,
            liveness.marker_timestamp,
        ) else {
            return; // no marker, or live forever: `LivenessInfo.isLive` is always true
        };
        // `expires_at_seconds` is already the unsigned on-disk `localExpirationTime`.
        let expired = expires_at <= now;
        if expired && expires_at < gc_before && marker_ts < max_purgeable_timestamp {
            self.row_liveness = RowLiveness::default();
            self.marker_purged = true;
        }
    }
}
