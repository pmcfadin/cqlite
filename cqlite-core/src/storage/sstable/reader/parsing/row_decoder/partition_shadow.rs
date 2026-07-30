//! Read-side tombstone/TTL shadowing for a single partition (issue #1741).
//!
//! Single-generation / full-scan reads historically bypassed reconciliation and
//! returned partition-deleted, range-tombstoned, or TTL-expired data as live. This
//! helper restores Cassandra `SELECT` semantics on the READ path without routing
//! through the write-support `ReconcileState`: it captures the partition-level
//! deletion, tracks the currently-open range tombstone as the row stream is walked
//! in clustering order, and decides per row whether a user-facing scan must hide it.
//!
//! It is **un-gated** — NOT behind the `write-support` feature — because read
//! correctness must not depend on a write feature (AC2). It is shared by every
//! user-facing emit path so there is ONE read-side shadowing implementation.
//!
//! Timestamp comparisons follow Cassandra `DeletionTime.deletes(ts) = ts <=
//! markedForDeleteAt`: a deletion at `d` shadows the whole row iff every piece of
//! the row's data is older than (or equal to) `d`. A row that still carries a cell
//! strictly newer than the deletion survives (the deletion shadows only the
//! already-absent older cells), matching the row-tombstone coexistence rule (#932).

use super::*;

/// Authoritative per-clustering-column reversal flags for `schema`, in schema
/// clustering-key order: `true` for a column declared `CLUSTERING ORDER BY
/// (... DESC)`, `false` for ASC (issue #1741). Threaded into [`PartitionShadow`]
/// so range-tombstone coverage compares clustering prefixes in physical storage
/// order. Same `is_reversed` derivation the BTI reader uses
/// (`data_access/bti.rs`). No-heuristics: schema clustering order only.
pub(super) fn clustering_reversed_flags(schema: &crate::schema::TableSchema) -> Vec<bool> {
    schema
        .clustering_keys
        .iter()
        .map(|c| matches!(c.order, crate::schema::ClusteringOrder::Desc))
        .collect()
}

/// Per-partition read-side shadowing state (issue #1741).
///
/// The type is `pub(crate)` so the multi-generation read path
/// (`storage::sstable::generation_merge`) can reuse its per-cell decision
/// [`PartitionShadow::cell_shadowed_or_expired`] POST-merge (issue #1849), keeping
/// ONE read-visibility implementation. Its stateful methods (`open`,
/// `feed_range_marker`, `row_hidden`, …) stay `pub(super)` — only the stateless
/// per-cell decision is shared outward.
pub(crate) struct PartitionShadow {
    /// Partition-level `markedForDeleteAt` (µs), or `None` when the partition is live.
    partition_deletion: Option<i64>,
    /// The currently-open range tombstone: `(start_bound_values, start_inclusive,
    /// deleted_at_µs)`. Cassandra writes non-overlapping range tombstones in
    /// clustering order — a start marker opens a range, an end marker closes it, and
    /// a boundary marker closes one and opens the next — so at most ONE range is open
    /// at a time. Every row physically between the open start marker and its
    /// (not-yet-seen) end marker falls inside the range, so a row is covered while
    /// the range is open iff it is at/after the start bound.
    open_range: Option<(Vec<Value>, bool, i64)>,
    /// Current wall-clock, epoch seconds, for read-time TTL expiry.
    now_secs: i64,
    /// Per-clustering-column reversal flags in schema order: `true` for a column
    /// declared `CLUSTERING ORDER BY (... DESC)`. Range-tombstone coverage must
    /// compare a row's clustering prefix against the range bounds in PHYSICAL
    /// storage order, which for a DESC column is the reverse of value order — so
    /// this carries the authoritative schema clustering order into the coverage
    /// FSM. A missing/short entry is treated as ASC (`false`), matching the
    /// `is_reversed` idiom used by the sibling reader code (`data_access/bti.rs`).
    /// No-heuristics: sourced only from `TableSchema::clustering_keys`.
    clustering_reversed: Vec<bool>,
}

impl PartitionShadow {
    /// Open shadowing for a partition given its decoded partition-level deletion
    /// (`Some((markedForDeleteAt_µs, localDeletionTime_s))` when the partition
    /// carries a tombstone, `None` when live), the current epoch-seconds clock, and
    /// the per-clustering-column reversal flags (`true` = DESC) in schema order.
    pub(super) fn open(
        now_secs: i64,
        partition_deletion: Option<(i64, i32)>,
        clustering_reversed: Vec<bool>,
    ) -> Self {
        Self {
            partition_deletion: partition_deletion.map(|(mfda, _ldt)| mfda),
            open_range: None,
            now_secs,
            clustering_reversed,
        }
    }

    /// Feed one decoded range-tombstone bound marker, running the same start/end/
    /// boundary FSM the delta-scan path uses. `bound_kind` is the raw Cassandra
    /// `ClusteringPrefix.Kind` ordinal; `deleted_at_secondary` is present only for
    /// boundary markers (kind 2/5) and carries the new range's deletion time.
    pub(super) fn feed_range_marker(
        &mut self,
        bound_values: Vec<Value>,
        bound_kind: u8,
        deleted_at_primary: i64,
        deleted_at_secondary: Option<i64>,
    ) -> Result<()> {
        match bound_kind {
            // Simple start bound: open a range (INCL_START=1, EXCL_START=7).
            1 | 7 => {
                self.open_range = Some((bound_values, bound_kind == 1, deleted_at_primary));
            }
            // Simple end bound: close the open range (EXCL_END=0, INCL_END=6).
            0 | 6 => {
                self.open_range = None;
            }
            // EXCL_END_INCL_START_BOUNDARY: close prev, open new range (inclusive start).
            2 => {
                let d = deleted_at_secondary.unwrap_or(deleted_at_primary);
                self.open_range = Some((bound_values, true, d));
            }
            // INCL_END_EXCL_START_BOUNDARY: close prev, open new range (exclusive start).
            5 => {
                let d = deleted_at_secondary.unwrap_or(deleted_at_primary);
                self.open_range = Some((bound_values, false, d));
            }
            unknown => {
                return Err(Error::corruption(format!(
                    "read-shadow: unknown range tombstone bound kind {unknown} — cannot represent \
                     faithfully (no-heuristics mandate, issue #28)"
                )));
            }
        }
        Ok(())
    }

    /// `true` when the row's clustering values must be extracted for a coverage
    /// check — i.e. a range tombstone is currently open. The tombstone-free common
    /// case returns `false`, so the caller skips the per-row clustering clone.
    pub(super) fn needs_clustering(&self) -> bool {
        self.open_range.is_some()
    }

    /// `true` when a user-facing `SELECT` must HIDE this row: it is shadowed by the
    /// partition tombstone or the open range tombstone (all of the row's data is
    /// older than the deletion), or it is TTL-expired. `clustering` is the row's
    /// clustering-key values (needed only for range coverage; pass an empty slice
    /// when [`Self::needs_clustering`] is `false`).
    pub(super) fn row_hidden(&self, header: &RowHeader, clustering: &[Value]) -> bool {
        // Partition-level deletion shadows every row whose data predates it.
        if let Some(d) = self.partition_deletion {
            if header.shadowed_by_deletion_at(d) {
                return true;
            }
        }
        // Open range tombstone shadows covered rows whose data predates it.
        if let Some((start, inclusive, d)) = &self.open_range {
            if self.at_or_after_start(clustering, start, *inclusive)
                && header.shadowed_by_deletion_at(*d)
            {
                return true;
            }
        }
        // Read-time TTL expiry.
        header.row_liveness_expired(self.now_secs)
    }

    /// Whether `row_ck` lies at/after the range's start bound in PHYSICAL storage
    /// order. While a range is open its end has not yet been seen, so a covered row
    /// need only satisfy the start bound. Comparison honours the per-column
    /// clustering order: for a DESC column the physical order is the reverse of
    /// value order, so we reverse that component's comparison — the same rule
    /// [`crate::storage::write_engine::mutation::ClusteringKey::compare`] applies
    /// (`ordering.reverse()` when the column is `ClusteringOrder::Desc`). Using the
    /// threaded `clustering_reversed` flags keeps this schema-authoritative with no
    /// per-row allocation (we compare the borrowed `Value` slices in place rather
    /// than materialising `ClusteringKey`/`TableSchema` per row). A prefix start
    /// bound (fewer components than the clustering arity) treats its missing
    /// components as −∞, per Cassandra clustering-bound semantics.
    fn at_or_after_start(&self, row_ck: &[Value], start: &[Value], inclusive: bool) -> bool {
        // Unbounded (open) start covers everything.
        if start.is_empty() {
            return true;
        }
        for (i, (a, b)) in row_ck.iter().zip(start.iter()).enumerate() {
            // Clustering-key values share a type per component, so `partial_cmp`
            // yields a total order here; fall back to Equal defensively.
            let mut ord = a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal);
            // DESC column: physical order is the reverse of value order.
            if self.clustering_reversed.get(i).copied().unwrap_or(false) {
                ord = ord.reverse();
            }
            match ord {
                std::cmp::Ordering::Less => return false,
                std::cmp::Ordering::Greater => return true,
                std::cmp::Ordering::Equal => {}
            }
        }
        // All compared components equal.
        match row_ck.len().cmp(&start.len()) {
            // Row extends a shorter (prefix) start bound: its missing components are
            // −∞, so the row is strictly after the start ⇒ covered.
            std::cmp::Ordering::Greater => true,
            // Row is a prefix of a longer start bound (fewer clustering components
            // than the bound) — not a well-formed data row; treat as before start.
            std::cmp::Ordering::Less => false,
            // Exact boundary point: covered only when the start bound is inclusive.
            std::cmp::Ordering::Equal => inclusive,
        }
    }

    /// The effective covering deletion timestamp (µs) for a row with clustering prefix
    /// `clustering`: the partition tombstone `markedForDeleteAt`, folded with the open
    /// range tombstone's `deleted_at` when the row falls inside the range. `None` when
    /// nothing covers the row. Shared by per-cell shadow filtering (issue #1741,
    /// Finding 1) and the primary-key-liveness decision below.
    pub(super) fn covering_deleted_at(&self, clustering: &[Value]) -> Option<i64> {
        let mut cover = self.partition_deletion;
        if let Some((start, inclusive, d)) = &self.open_range {
            if self.at_or_after_start(clustering, start, *inclusive) {
                cover = Some(cover.map_or(*d, |c| c.max(*d)));
            }
        }
        cover
    }

    /// Per-cell shadow context for the row-data cell loop (issue #1741, Finding 1):
    /// `(covering_deleted_at, now_secs)`. A data cell is dropped when its effective
    /// write ts <= `covering_deleted_at`, or it is TTL-expired at `now_secs`.
    pub(super) fn cell_context(&self, clustering: &[Value]) -> (Option<i64>, i64) {
        (self.covering_deleted_at(clustering), self.now_secs)
    }

    /// Whether a data cell is dropped by read-side shadow/TTL filtering (issue #1741,
    /// Finding 1): its effective write ts is shadowed by the covering deletion
    /// (`eff_ts <= cover`, matching `DeletionTime.deletes`), OR it is TTL-expired at
    /// `now` (`eff_exp <= now`). A cell with no authoritative write ts is NEVER
    /// shadowed (no-heuristics: we hide only what authoritative metadata proves
    /// stale). Associated function so both the row-data cell loop and its unit pins
    /// share one decision.
    pub(crate) fn cell_shadowed_or_expired(
        cover: Option<i64>,
        now: i64,
        eff_ts: Option<i64>,
        eff_exp: Option<i64>,
    ) -> bool {
        let shadowed = matches!((cover, eff_ts), (Some(d), Some(t)) if t <= d);
        let expired = eff_exp.is_some_and(|e| e <= now);
        shadowed || expired
    }

    /// Whether a decoded SIMPLE CELL must be DROPPED from a user-facing row
    /// because it is a CELL TOMBSTONE — i.e. a deleted cell (issue #3094).
    ///
    /// A deleted cell is ABSENT from the row Cassandra reconciles: `Cell.isLive`
    /// is false for a tombstone, so the `Row`'s `ColumnData` carries nothing for
    /// that column and CQL renders it NULL. Dropping it here is what makes
    /// `SELECT` return NULL instead of surfacing the raw
    /// `Value::Tombstone(CellTombstone)` that `cell_value.rs` decodes. Before
    /// this drop that tombstone reached the row carrier, and the Arrow encoder
    /// correctly fail-closed (#1485) on it, erroring the WHOLE Flight `do_get`
    /// stream (`column 'w': expected Text value, got Tombstone(..)`).
    ///
    /// `user_facing` is the SAME discriminator the #1741 per-cell shadow/TTL
    /// filter uses: `true` exactly when a [`PartitionShadow`] was threaded into
    /// the decode (a query read). The PHYSICAL consumers — compaction merge
    /// input, sstabledump parity, delta scan — thread `None` and MUST keep
    /// receiving the tombstone with its authoritative deletion timestamp (#505),
    /// so their streams stay byte-unchanged.
    ///
    /// A tombstone is never folded into the row LIVENESS aggregate
    /// (`has_live_forever_data_cell` / `max_data_cell_expires_at`): it is not live
    /// data. Nor does it supply a TIMESTAMP to the row's shadow maximum — only the
    /// PRESENCE fact `has_deleted_data_cell`; see [`Self::has_shadow_evidence`].
    ///
    /// This mirrors the multi-generation read path, whose merged-cell filter
    /// (`generation_merge::ReadVisibility::filter_live`) already skipped
    /// `Value::Tombstone` cells, and the collection path, which already skipped
    /// tombstoned elements — so all three read paths now agree.
    #[inline]
    pub(crate) fn cell_tombstone_dropped(user_facing: bool, is_cell_tombstone: bool) -> bool {
        user_facing && is_cell_tombstone
    }

    /// Whether a decoded simple-cell `value` is specifically a CELL TOMBSTONE —
    /// the ONLY tombstone shape [`Self::cell_tombstone_dropped`] may drop.
    ///
    /// Matching the `Value::Tombstone` discriminant alone would silently accept a
    /// row/range/partition tombstone value if one ever surfaced in the simple-cell
    /// loop (none can today: `cell_value.rs` builds exactly
    /// `TombstoneType::CellTombstone` there) and drop it as if it were a deleted
    /// cell. Naming the variant keeps the decision authoritative rather than
    /// inferred from a broader shape (no-heuristics, #28); an unexpected shape
    /// instead flows on unchanged and fails closed downstream.
    #[inline]
    pub(crate) fn is_cell_tombstone(value: &Value) -> bool {
        matches!(
            value,
            Value::Tombstone(t) if matches!(t.tombstone_type, TombstoneType::CellTombstone)
        )
    }

    /// Whether the row carries enough authoritative evidence for
    /// [`RowHeader::shadowed_by_deletion_at`] to hide it — i.e. whether its
    /// `i64::MIN` "no authoritative timestamp" FAIL-SAFE is defeated (issue #3094).
    /// `max_write_ts` is the row's LIVE-write maximum (`max(liveness marker, live data
    /// cells)`, `i64::MIN` when neither exists); `has_deleted_data_cell` is the mere
    /// PRESENCE of a decoded cell TOMBSTONE.
    ///
    /// ## The invariant
    ///
    /// Tombstone evidence may ONLY defeat the fail-safe; it must NEVER raise the row
    /// maximum. That maximum is compared `max_ts <= markedForDeleteAt`, so raising it
    /// can only move a row hidden → VISIBLE — the exact resurrection direction #3094
    /// closes. A tombstone making a row visible is never correct: Cassandra purges a
    /// surviving tombstone (`Filter.applyToRow` → `row.purge(…, PURGE_ALL, …)`) and
    /// drops the emptied row (guide Ch.11), so it can only REMOVE data from a row.
    ///
    /// The counterexample that forced presence over an earlier "fallback max"
    /// (`live.or(deleted_only)`): a LIVE LIVENESS MARKER at `T`, NO data cell, a cell
    /// tombstone at `T + 10s`, under a partition deletion at `T + 5s`. With no
    /// live-cell evidence the fallback fired, so `T + 10s` became the aggregate and
    /// `max_write_timestamp` MAXed it with the liveness `T` → `T + 10s > T + 5s`: an
    /// all-null phantom row out of a deleted partition where Cassandra returns 0 rows.
    ///
    /// ## Why presence suffices
    ///
    /// A row whose ONLY cells are tombstones (`UPDATE t SET w = null WHERE …`, no
    /// INSERT liveness marker) has `max_write_ts == i64::MIN`, and the fail-safe exists
    /// solely to avoid hiding an EMPTY/TRUNCATED parse. A decoded tombstone proves a
    /// genuinely reduced row, so any covering deletion hides it (`i64::MIN <= cover`
    /// always) — the direction Cassandra answers, since a row with no live data is
    /// dropped whether or not the deletion predates the tombstone. Residual (#3121):
    /// with NO covering deletion such a row is still emitted, all-null.
    #[inline]
    pub(crate) fn has_shadow_evidence(max_write_ts: i64, has_deleted_data_cell: bool) -> bool {
        max_write_ts != i64::MIN || has_deleted_data_cell
    }

    /// Fold `value` into a running `Option` MAX accumulator: `None` contributes
    /// nothing, so an absent authoritative timestamp never becomes a `0` (which
    /// would read as "written at the epoch" — a heuristic, #28).
    #[inline]
    pub(crate) fn fold_max(acc: Option<i64>, value: Option<i64>) -> Option<i64> {
        match (acc, value) {
            (Some(a), Some(v)) => Some(a.max(v)),
            (Some(a), None) => Some(a),
            (None, v) => v,
        }
    }
}

/// Whether a partition-level deletion at `cover` (µs, `markedForDeleteAt`) shadows
/// the WHOLE of a merged (post-`KWayMerger`) row whose maximum DATA-cell write
/// timestamp is `max_data_cell_timestamp` (issue #1849).
///
/// This is the multi-generation read path's reuse of the single-gen row-level
/// decision [`RowHeader::shadowed_by_deletion_at`]: it builds the same `RowHeader`
/// the single-gen emit path folds (a row with NO surfaced primary-key liveness
/// marker — the `KWayMerger` output does not carry one — and the given max data-cell
/// timestamp) and applies the identical `ts <= markedForDeleteAt` rule. A row with
/// no data-cell timestamp (`None`) is NEVER shadowed (fail-safe / no-heuristics: we
/// hide only what authoritative metadata proves predates the deletion).
///
/// Read-time TTL expiry is applied PER CELL on the merged path (via
/// [`PartitionShadow::cell_shadowed_or_expired`]); whole-row TTL hiding needs the
/// primary-key liveness marker, which the merger output lacks, so it is deliberately
/// NOT decided here (issue #1849 scope note).
///
/// Gated on `write-support`: the sole caller is the cross-generation read path
/// (`generation_merge`), whose `mod` declaration in `sstable/mod.rs` is itself
/// `#[cfg(feature = "write-support")]`. Without this matching gate the function is
/// orphaned (zero callers) under the `minimal-build` feature set and `-D dead_code`
/// fails the build. `cell_shadowed_or_expired` needs no such gate — the single-gen
/// read path (`row_data`/`complex_column`) calls it unconditionally.
#[cfg(feature = "write-support")]
pub(crate) fn merged_row_shadowed_by_partition(
    cover: Option<i64>,
    max_data_cell_timestamp: Option<i64>,
) -> bool {
    let Some(deleted_at) = cover else {
        return false;
    };
    let header = RowHeader {
        timestamp: None,
        ttl: None,
        liveness_expires_at_seconds: None,
        local_deletion_time: None,
        marked_for_delete_at: None,
        header_size: 0,
        row_size_vint_len: 0,
        missing_columns_bitmap: None,
        max_data_cell_timestamp,
        max_data_cell_expires_at: None,
        has_live_forever_data_cell: false,
        // Hardcoded `false` as a DELIBERATE, TRACKED limitation (#3129 AC4), NOT because
        // no tombstone can reach here: `apply_partition_shadowing`'s `is_data` is BY NAME,
        // so a cell tombstone strictly newer than `markedForDeleteAt` survives the merge
        // and `filter_live` drops it inside that same fold without reporting its presence.
        has_deleted_data_cell: false,
    };
    header.shadowed_by_deletion_at(deleted_at)
}

#[cfg(test)]
mod tests {
    //! Issue #1741: deterministic pins for the read-side shadowing primitives.
    //! These exercise the exact partition-deletion, range-tombstone-FSM, and TTL
    //! decision logic the emit paths call, independent of on-disk fixtures.
    use super::*;

    /// Build a `RowHeader` describing a live row whose max data timestamp is
    /// `write_ts_micros` and (optionally) whose row-liveness TTL expires at
    /// `liveness_expiry_secs`. `has_live_forever` marks a no-TTL data cell.
    fn row(
        write_ts_micros: i64,
        liveness_expiry_secs: Option<i64>,
        max_cell_expires_at_secs: Option<i64>,
        has_live_forever: bool,
    ) -> RowHeader {
        RowHeader {
            timestamp: Some(write_ts_micros),
            ttl: None,
            liveness_expires_at_seconds: liveness_expiry_secs,
            local_deletion_time: None,
            marked_for_delete_at: None,
            header_size: 0,
            row_size_vint_len: 0,
            missing_columns_bitmap: None,
            max_data_cell_timestamp: Some(write_ts_micros),
            max_data_cell_expires_at: max_cell_expires_at_secs,
            has_live_forever_data_cell: has_live_forever,
            has_deleted_data_cell: false,
        }
    }

    #[test]
    fn partition_deletion_shadows_only_older_or_equal_rows() {
        // Partition tombstone at markedForDeleteAt = 2000µs.
        let shadow = PartitionShadow::open(0, Some((2000, 12345)), vec![]);
        // Row older than the deletion → hidden.
        assert!(shadow.row_hidden(&row(1000, None, None, false), &[]));
        // Row exactly at the deletion → hidden (deletes: ts <= markedForDeleteAt).
        assert!(shadow.row_hidden(&row(2000, None, None, false), &[]));
        // Row strictly newer than the deletion → survives.
        assert!(!shadow.row_hidden(&row(3000, None, None, false), &[]));
    }

    #[test]
    fn row_with_no_authoritative_timestamp_is_not_shadowed() {
        // A row with no liveness and no decodable data cell (max_ts = i64::MIN, e.g.
        // an empty/undecodable row) must NOT be hidden — we only shadow when
        // authoritative metadata proves the row predates the deletion (no guessing).
        let mut h = row(0, None, None, false);
        h.timestamp = None;
        h.max_data_cell_timestamp = None;
        let shadow = PartitionShadow::open(0, Some((8_000_000_000_000_000_000, 12345)), vec![]);
        assert!(!shadow.row_hidden(&h, &[]));
    }

    #[test]
    fn live_partition_hides_nothing() {
        let shadow = PartitionShadow::open(0, None, vec![]);
        assert!(!shadow.row_hidden(&row(1000, None, None, false), &[]));
    }

    #[test]
    fn range_tombstone_fsm_shadows_covered_older_rows() {
        // ASC single clustering column (no reversal). INCL_START at ck=[10],
        // INCL_END at ck=[20], deleted_at = 5000µs.
        let mut shadow = PartitionShadow::open(0, None, vec![false]);
        shadow
            .feed_range_marker(vec![Value::Integer(10)], 1, 5000, None)
            .unwrap();
        assert!(shadow.needs_clustering());
        // Covered older row (ck=15, ts < deleted_at) → hidden.
        assert!(shadow.row_hidden(&row(1000, None, None, false), &[Value::Integer(15)]));
        // Covered but strictly-newer row (ts > deleted_at) → survives.
        assert!(!shadow.row_hidden(&row(9000, None, None, false), &[Value::Integer(15)]));
        // Row before the open start bound (ck=5) → not covered → survives.
        assert!(!shadow.row_hidden(&row(1000, None, None, false), &[Value::Integer(5)]));
        // Close the range; subsequent rows are no longer covered.
        shadow
            .feed_range_marker(vec![Value::Integer(20)], 6, 5000, None)
            .unwrap();
        assert!(!shadow.needs_clustering());
        assert!(!shadow.row_hidden(&row(1000, None, None, false), &[Value::Integer(15)]));
    }

    #[test]
    fn range_tombstone_coverage_honors_desc_clustering_order() {
        // Table with `CLUSTERING ORDER BY (ck DESC)`: physical storage order is the
        // REVERSE of value order, so an INCL_START marker at ck=[10] opens a range
        // that physically covers rows with ck < 10 (which come AFTER [10] on disk),
        // NOT rows with ck > 10. deleted_at = 5000µs; rows below are older (ts=1000).
        //
        // Revert-verify: with the pre-fix raw `partial_cmp` (ignoring DESC), the
        // coverage sides invert — ck=15 is wrongly hidden and ck=5 wrongly kept.
        let mut shadow = PartitionShadow::open(0, None, vec![true]);
        shadow
            .feed_range_marker(vec![Value::Integer(10)], 1, 5000, None)
            .unwrap();
        assert!(shadow.needs_clustering());
        // ck=5 is physically AFTER the DESC start [10] → covered → hidden.
        assert!(shadow.row_hidden(&row(1000, None, None, false), &[Value::Integer(5)]));
        // ck=15 is physically BEFORE the DESC start [10] → not covered → survives.
        assert!(!shadow.row_hidden(&row(1000, None, None, false), &[Value::Integer(15)]));
        // Exact inclusive boundary ck=10 → covered → hidden.
        assert!(shadow.row_hidden(&row(1000, None, None, false), &[Value::Integer(10)]));
        // A covered DESC row that is strictly newer than the deletion still survives.
        assert!(!shadow.row_hidden(&row(9000, None, None, false), &[Value::Integer(5)]));
    }

    #[test]
    fn range_tombstone_boundary_reopens_new_range() {
        // EXCL_END_INCL_START_BOUNDARY (kind 2) closes the prev range and opens a new
        // one (inclusive start) using the secondary deletion time.
        let mut shadow = PartitionShadow::open(0, None, vec![false]);
        shadow
            .feed_range_marker(vec![Value::Integer(10)], 2, 5000, Some(6000))
            .unwrap();
        // New range starts inclusive at ck=10 with deleted_at=6000.
        assert!(shadow.row_hidden(&row(5500, None, None, false), &[Value::Integer(12)]));
        assert!(!shadow.row_hidden(&row(7000, None, None, false), &[Value::Integer(12)]));
    }

    #[test]
    fn unknown_bound_kind_is_rejected() {
        let mut shadow = PartitionShadow::open(0, None, vec![false]);
        assert!(shadow
            .feed_range_marker(vec![Value::Integer(1)], 9, 1, None)
            .is_err());
    }

    #[test]
    fn ttl_expired_row_is_hidden_and_live_ttl_is_kept() {
        // now = 1_000_000 epoch-seconds.
        let now = 1_000_000i64;
        let shadow = PartitionShadow::open(now, None, vec![]);
        // Row-liveness TTL already expired, no live-forever cell → hidden.
        assert!(shadow.row_hidden(&row(10, Some(500_000), None, false), &[]));
        // Row-liveness TTL still in the future → visible.
        assert!(!shadow.row_hidden(&row(10, Some(2_000_000), None, false), &[]));
        // Expired liveness but a live-forever data cell (later UPDATE) → visible.
        assert!(!shadow.row_hidden(&row(10, Some(500_000), None, true), &[]));
        // Expired liveness, only expiring cells whose max expiry is still future → visible.
        assert!(!shadow.row_hidden(&row(10, Some(500_000), Some(2_000_000), false), &[]));
        // Row with NO TTL anywhere is never TTL-hidden.
        assert!(!shadow.row_hidden(&row(10, None, None, false), &[]));
    }

    /// Issue #1741 (Finding 1, test 2 / per-cell drop decision): a data cell is
    /// dropped when its effective write ts is shadowed by the covering deletion OR it
    /// is TTL-expired at `now`; a cell without an authoritative write ts is never
    /// shadowed. The per-cell TTL branch is pinned here (not end-to-end) because the
    /// writer stamps an expiring cell's `localDeletionTime` as `now + ttl`, so a
    /// PAST-expired per-cell TTL is not synthesizable via a fresh writer flush; the
    /// row-data cell loop calls exactly this function.
    ///
    /// Revert-verify: dropping the `expired` term makes the TTL assertions FALSE;
    /// dropping the `shadowed` term makes the shadow assertions FALSE.
    #[test]
    fn per_cell_shadow_and_ttl_drop_decision() {
        use super::PartitionShadow as PS;
        let now = 1_000_000i64;
        // Shadow: cell older/equal to the covering deletion (2000µs) → dropped.
        assert!(PS::cell_shadowed_or_expired(
            Some(2000),
            now,
            Some(1000),
            None
        ));
        assert!(PS::cell_shadowed_or_expired(
            Some(2000),
            now,
            Some(2000),
            None
        ));
        // Shadow: cell strictly newer than the deletion → kept.
        assert!(!PS::cell_shadowed_or_expired(
            Some(2000),
            now,
            Some(3000),
            None
        ));
        // No covering deletion → never shadowed.
        assert!(!PS::cell_shadowed_or_expired(None, now, Some(1), None));
        // No authoritative write ts → never shadowed (no-heuristics).
        assert!(!PS::cell_shadowed_or_expired(Some(2000), now, None, None));
        // TTL: expiry at/BEFORE now → expired → dropped (regardless of covering).
        assert!(PS::cell_shadowed_or_expired(
            None,
            now,
            Some(9999),
            Some(now)
        ));
        assert!(PS::cell_shadowed_or_expired(
            None,
            now,
            Some(9999),
            Some(now - 1)
        ));
        // TTL: expiry in the future → kept.
        assert!(!PS::cell_shadowed_or_expired(
            None,
            now,
            Some(9999),
            Some(now + 1)
        ));
    }

    /// Issue #1741 (Finding 1, test 3): a row whose non-key data cells are ALL
    /// shadowed/expired and which has no live pk-liveness marker must be hidden. The
    /// row-data cell loop drops the stale cells from the emitted map BUT still folds
    /// their write ts / expiry into the row aggregate, so `row_hidden` recognises the
    /// reduced row as fully shadowed/expired. A single decoded-then-dropped cell
    /// leaves `max_data_cell_timestamp = Some(<= covering)` (NOT the `i64::MIN`
    /// sentinel a truncated no-cell parse leaves), which is exactly the signal that
    /// distinguishes a genuinely reduced row (hidden) from a truncated parse (kept).
    ///
    /// Revert-verify: if a shadowed/expired cell were EXCLUDED from the aggregate
    /// (leaving `None`), `max_write_timestamp` would be the `i64::MIN` sentinel and
    /// the first two assertions (hidden) would become FALSE.
    #[test]
    fn reduced_to_primary_key_row_is_hidden() {
        // (a) All non-key cells shadowed by a partition tombstone (2000µs), no live
        //     marker: the dropped cells' ts folds to max_data_cell_timestamp <= 2000,
        //     so row_hidden shadows the whole row.
        let shadow = PartitionShadow::open(0, Some((2000, 12345)), vec![]);
        let mut h = row(0, None, None, false);
        h.timestamp = None; // UPDATE-only row (no pk-liveness marker)
        h.max_data_cell_timestamp = Some(1500); // newest shadowed cell, <= 2000
        assert!(shadow.row_hidden(&h, &[]));

        // (b) All non-key cells TTL-expired (no covering deletion), no live marker:
        //     the dropped cells fold an expiry in the past and are NOT live-forever.
        let now = 1_000_000i64;
        let shadow_ttl = PartitionShadow::open(now, None, vec![]);
        let mut h2 = row(0, None, Some(now - 1), false);
        h2.timestamp = None;
        assert!(shadow_ttl.row_hidden(&h2, &[]));

        // (c) A truncated / marker-only parse (NO decoded cells → sentinel) is NOT
        //     hidden — it is a partial read, not a genuinely reduced row.
        let mut h3 = row(0, None, None, false);
        h3.timestamp = None;
        h3.max_data_cell_timestamp = None;
        assert!(!shadow.row_hidden(&h3, &[]));

        // (d) A surviving non-key cell newer than the deletion keeps the row visible.
        let mut h4 = row(0, None, None, false);
        h4.timestamp = None;
        h4.max_data_cell_timestamp = Some(3000); // > 2000
        assert!(!shadow.row_hidden(&h4, &[]));
    }

    /// Issue #1849: the multi-generation read path's row-level partition-shadow
    /// reuse. A merged row is hidden by a partition tombstone iff its max DATA-cell
    /// write ts is `<= markedForDeleteAt`; a row with no data-cell ts (`None`) or no
    /// cover is NEVER hidden. Revert-verify: dropping the `<=` (using `<`) makes the
    /// exact-boundary assertion FALSE; returning `true` on `None` cover would hide
    /// live rows.
    #[cfg(feature = "write-support")]
    #[test]
    fn merged_row_partition_shadow_reuse() {
        use super::merged_row_shadowed_by_partition as f;
        // No cover → never hidden.
        assert!(!f(None, Some(1_000)));
        // Cover but no data-cell ts (pk-only / undecodable) → never hidden.
        assert!(!f(Some(2_000), None));
        // Data older than the deletion → hidden.
        assert!(f(Some(2_000), Some(1_000)));
        // Data exactly at the deletion → hidden (deletes: ts <= markedForDeleteAt).
        assert!(f(Some(2_000), Some(2_000)));
        // Data strictly newer than the deletion → survives.
        assert!(!f(Some(2_000), Some(3_000)));
    }

    /// Issue #3094: the cell-tombstone drop decision. A DELETED CELL is dropped
    /// from a user-facing row (so the column reads NULL) and kept verbatim on the
    /// physical read paths (so compaction / sstabledump / delta-scan streams stay
    /// byte-unchanged with their authoritative deletion timestamps, #505).
    ///
    /// Revert-verify: hardcoding `false` (the pre-fix behaviour) makes the first
    /// assertion FALSE and a deleted cell reaches the Arrow encoder as a raw
    /// `Value::Tombstone`, failing the whole `do_get` stream; hardcoding `true`
    /// makes the third assertion FALSE and silently strips cell tombstones out of
    /// the compaction merge input, resurrecting deleted cells on rewrite.
    #[test]
    fn cell_tombstone_dropped_only_on_the_user_facing_path() {
        use super::PartitionShadow as PS;
        // User-facing read + a cell tombstone → dropped (column reads NULL).
        assert!(PS::cell_tombstone_dropped(true, true));
        // User-facing read + a live cell → kept (the tombstone rule must not
        // touch live data; its shadow/TTL fate is `cell_shadowed_or_expired`).
        assert!(!PS::cell_tombstone_dropped(true, false));
        // PHYSICAL read + a cell tombstone → kept verbatim (#505).
        assert!(!PS::cell_tombstone_dropped(false, true));
        // Physical read + a live cell → kept.
        assert!(!PS::cell_tombstone_dropped(false, false));
    }

    /// Issue #3094 (nit 3): only a `TombstoneType::CellTombstone` is a DELETED CELL.
    /// The simple-cell loop cannot surface any other tombstone shape today, so this
    /// pins that the predicate names the variant instead of accepting the broader
    /// `Value::Tombstone` discriminant — a row/range/partition tombstone value must
    /// NOT be silently dropped as if it were a deleted cell (no-heuristics, #28).
    #[test]
    fn only_a_cell_tombstone_counts_as_a_deleted_cell() {
        fn tombstone(kind: TombstoneType) -> Value {
            Value::Tombstone(Box::new(TombstoneInfo {
                deletion_time: 1_000,
                tombstone_type: kind,
                local_deletion_time: 1,
                ttl: None,
                range_start: None,
                range_end: None,
            }))
        }
        assert!(PartitionShadow::is_cell_tombstone(&tombstone(
            TombstoneType::CellTombstone
        )));
        for other in [
            TombstoneType::RowTombstone,
            TombstoneType::RangeTombstone,
            TombstoneType::PartitionTombstone,
            TombstoneType::TtlExpiration,
        ] {
            assert!(
                !PartitionShadow::is_cell_tombstone(&tombstone(other)),
                "{other:?} is not a deleted CELL and must not be dropped as one"
            );
        }
        // A live value is obviously not a deleted cell.
        assert!(!PartitionShadow::is_cell_tombstone(&Value::Integer(7)));
    }

    /// Issue #3094 (blocker): tombstone evidence DEFEATS the `i64::MIN` fail-safe and
    /// NOTHING ELSE — a pure presence predicate cannot carry a timestamp into the row
    /// maximum, which is what makes the fix one-directional (visible → hidden only).
    /// Revert-verify: dropping the `|| has_deleted_data_cell` term makes the first
    /// assertion FALSE (a deletion-only row resurrects inside a deleted partition);
    /// returning `true` unconditionally makes the last FALSE (hiding truncated parses,
    /// against the #28 fail-safe).
    #[test]
    fn tombstone_presence_defeats_the_failsafe_without_a_timestamp() {
        let f = PartitionShadow::has_shadow_evidence;
        // Deletion-only row: no live write at all, but a decoded tombstone cell.
        assert!(f(i64::MIN, true));
        // Live write evidence needs no tombstone.
        assert!(f(1_000, false));
        assert!(f(1_000, true));
        // Nothing decoded at all → fail-safe holds (never hide a truncated parse).
        assert!(!f(i64::MIN, false));
    }

    /// Issue #3094 (blocker, header-level consequence): both resurrection shapes —
    /// (a) no liveness marker, only a tombstone cell; (b) a live liveness marker at
    /// 1_000µs, no live data cell, and a tombstone cell at 6_000µs under a deletion
    /// at 5_000µs, which must stay HIDDEN because the tombstone contributes no
    /// timestamp. Revert-verify: folding that 6_000µs into `max_data_cell_timestamp`
    /// (the pre-fix "fallback max") makes (b) FALSE — an all-null phantom row out of
    /// a deleted partition. End-to-end pin:
    /// `cqlite-core/tests/issue_3094_partition_deleted_row_not_resurrected.rs`.
    #[test]
    fn deletion_only_and_liveness_only_rows_are_shadowed_by_a_covering_deletion() {
        // (a) No liveness marker (UPDATE-only); the row's ONLY cell is a tombstone.
        let mut h = row(0, None, None, false);
        h.timestamp = None;
        h.max_data_cell_timestamp = None;
        h.has_deleted_data_cell = true;
        assert!(PartitionShadow::open(0, Some((5_000, 12345)), vec![]).row_hidden(&h, &[]));

        // (b) Live liveness marker @1_000, no live data cell, tombstone cell (whose
        //     6_000µs write ts must NOT reach the aggregate) under a deletion @5_000.
        let mut h2 = row(1_000, None, None, false);
        h2.max_data_cell_timestamp = None;
        h2.has_deleted_data_cell = true;
        assert!(PartitionShadow::open(0, Some((5_000, 12345)), vec![]).row_hidden(&h2, &[]));
        // Same row under an OLDER deletion @500: the liveness marker survives it, so
        // the row stays visible on LIVE evidence.
        assert!(!PartitionShadow::open(0, Some((500, 12345)), vec![]).row_hidden(&h2, &[]));

        // (c) A live data cell newer than the deletion still keeps the row visible,
        //     tombstone presence notwithstanding.
        let mut h3 = row(1_000, None, None, false);
        h3.max_data_cell_timestamp = Some(9_000);
        h3.has_deleted_data_cell = true;
        assert!(!PartitionShadow::open(0, Some((5_000, 12345)), vec![]).row_hidden(&h3, &[]));
    }

    /// Issue #1741 (Finding 2, header-level consequence): a row whose aggregated
    /// max data-cell timestamp (which now folds in a non-frozen collection's newest
    /// `max_element_writetime`) exceeds the covering deletion survives, even when
    /// the row-liveness `timestamp` predates the deletion. The end-to-end fold is
    /// pinned by `collection_element_newer_than_partition_tombstone_survives`; this
    /// pins the decision the fold feeds.
    #[test]
    fn newer_collection_element_ts_keeps_row_visible() {
        let shadow = PartitionShadow::open(0, Some((2000, 12345)), vec![]);
        // Row marker older than the deletion (1000 <= 2000) but a folded element ts
        // (3000) newer than it → NOT shadowed → visible.
        let mut h = row(1000, None, None, false);
        h.max_data_cell_timestamp = Some(3000);
        assert!(!shadow.row_hidden(&h, &[]));
        // Without the fold (max cell ts == row ts == 1000 <= 2000) → shadowed.
        let mut h_prefold = row(1000, None, None, false);
        h_prefold.max_data_cell_timestamp = Some(1000);
        assert!(shadow.row_hidden(&h_prefold, &[]));
    }
}
