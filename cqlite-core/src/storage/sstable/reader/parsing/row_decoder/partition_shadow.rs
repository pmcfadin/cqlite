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
#[path = "partition_shadow_tests.rs"]
mod tests;
