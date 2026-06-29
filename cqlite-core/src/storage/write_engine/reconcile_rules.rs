//! Shared reconcile rule layer (issue #947).
//!
//! The Cassandra `Cells#reconcile` cell tie-break and the complex-deletion
//! strict-supersede + shadow-before-purge boundaries used to be implemented
//! TWICE and hand-synced:
//!
//! * the COMPACTION/merge path (`merge/mod.rs` + `merge/reconcile.rs`),
//!   reconciling [`CellData`](super::merge::CellData);
//! * the FLUSH/write path
//!   (`sstable::writer::data_writer::rows::DataWriter::merge_row_group`),
//!   reconciling `MergedOp`.
//!
//! Both paths now adapt their concrete cell type into the minimal common
//! [`ReconcileCell`] view and call the pure decision functions below, so the
//! load-bearing comparisons live in ONE place and consistency is structural —
//! not a comment promising two copies were kept in sync.
//!
//! These are pure scalar predicates (no allocation, no I/O). The per-container
//! iteration (a `HashMap` of `CellData` winners vs. a `Vec` of `MergedOp`)
//! differs between the two call sites and stays at each site; only the
//! decision rules are shared.
//!
//! ## Parity anchors (do not change without a Cassandra reference)
//!
//! * [`cell_wins`] — `Cells#reconcile` (commit `a62c749`; issues #848/#498).
//! * [`complex_deletion_supersedes`] — strict-supersede (commit `bd244649`).
//! * [`element_survives_complex_deletion`] — shadow-before-purge boundary
//!   (commit `f66fa14f`; the `<=` element-vs-marker rule, #498).

/// Minimal common view of a reconcilable cell.
///
/// The only two accessors BOTH write paths share for the `Cells#reconcile`
/// tie-break: the cell's write timestamp and whether it is a deletion
/// (tombstone). Each call site implements this over its concrete type
/// ([`CellData`](super::merge::CellData) on the merge path, `MergedOp` on the
/// writer path) and how it RECOGNIZES a tombstone — a `Value::Tombstone`
/// payload vs. a `CellOperation::Delete` op — stays with the type, since that
/// recognition is genuinely type-specific.
///
/// The on-disk `localDeletionTime` is intentionally NOT part of this view: it
/// is consulted only by the merge path's gc-grace purge stage (which the flush
/// path has no analogue for), so sharing it here would force a meaningless
/// accessor on the writer type. The tie-break itself never consults LDT — a
/// same-timestamp tombstone wins BEFORE any LDT compare.
pub(crate) trait ReconcileCell {
    /// Cell write timestamp in microseconds (`markedForDeleteAt` for a deletion).
    fn timestamp(&self) -> i64;
    /// Whether this cell is a deletion (tombstone) rather than a live/expiring
    /// value.
    fn is_tombstone(&self) -> bool;
}

/// Cassandra `Cells#reconcile` replace decision (parity commit `a62c749`):
/// returns `true` when `candidate` should replace the current `existing` winner.
///
/// Decided IN THIS ORDER, short-circuiting:
/// 1. **timestamp** — a strictly higher write timestamp always wins.
/// 2. **deletion vs live/expiring at EQUAL timestamp** — a cell DELETION
///    (tombstone) beats a LIVE or EXPIRING (TTL) cell, decided BEFORE any
///    `localDeletionTime` compare, so an expiring cell can never resurrect data
///    over a same-timestamp tombstone (issues #848 / #498). An expiring cell is
///    treated as LIVE (it carries a real value + a TTL, not a tombstone), so
///    this one rule subsumes both tombstone-beats-live and
///    tombstone-beats-expiring.
/// 3. **equal timestamp + equal liveness** — returns `false`. Cassandra's
///    reconcile is order-independent and equivalent here, so the caller keeps
///    its first-seen winner. (The writer path overlays an order-dependent
///    last-write-wins tie-break for this single case at its call site; that
///    overlay is writer-only and NOT part of this shared rule.)
pub(crate) fn cell_wins<C: ReconcileCell + ?Sized>(candidate: &C, existing: &C) -> bool {
    if candidate.timestamp() != existing.timestamp() {
        return candidate.timestamp() > existing.timestamp();
    }
    // Equal timestamp: a deletion wins over a live/expiring cell, BEFORE any
    // localDeletionTime compare (parity `a62c749`).
    candidate.is_tombstone() && !existing.is_tombstone()
}

/// Strict-supersede predicate for complex (collection / UDT) deletion markers
/// (parity commit `bd244649`): a candidate marker supersedes the active one for
/// the SAME column ONLY when its `markedForDeleteAt` is STRICTLY GREATER. Equal
/// (or lesser) timestamps do NOT supersede.
///
/// Both call sites reduce their carried markers to one active deletion per
/// column NAME using this predicate; the per-column iteration differs but the
/// strictly-greater comparison is shared here.
#[inline]
pub(crate) fn complex_deletion_supersedes(candidate_mfda: i64, active_mfda: i64) -> bool {
    candidate_mfda > active_mfda
}

/// Shadow-before-purge boundary (parity commit `f66fa14f`; the element-vs-marker
/// `<=` rule, #498): a complex ELEMENT survives the active complex deletion on
/// its column only when the element's own timestamp is STRICTLY GREATER than the
/// marker's `markedForDeleteAt`. An element with `ts <= mfda` is shadowed
/// (covered) and dropped before the marker can be purged.
///
/// Returns `true` when the element SURVIVES.
#[inline]
pub(crate) fn element_survives_complex_deletion(element_ts: i64, mfda: i64) -> bool {
    element_ts > mfda
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tiny test double for the [`ReconcileCell`] view.
    struct Cell {
        ts: i64,
        tomb: bool,
    }
    impl ReconcileCell for Cell {
        fn timestamp(&self) -> i64 {
            self.ts
        }
        fn is_tombstone(&self) -> bool {
            self.tomb
        }
    }
    fn live(ts: i64) -> Cell {
        Cell { ts, tomb: false }
    }
    fn tomb(ts: i64) -> Cell {
        Cell { ts, tomb: true }
    }

    // ── cell_wins: timestamp axis ───────────────────────────────────────────

    #[test]
    fn strictly_greater_timestamp_wins() {
        assert!(cell_wins(&live(200), &live(100)));
        assert!(!cell_wins(&live(100), &live(200)));
        // A live cell at a strictly-greater ts beats an older tombstone.
        assert!(cell_wins(&live(200), &tomb(100)));
        // An older live cell does not beat a newer tombstone.
        assert!(!cell_wins(&live(100), &tomb(200)));
    }

    // ── cell_wins: EQUAL-ts deletion-vs-live/expiring (the #848/#498 axis) ───

    #[test]
    fn equal_ts_tombstone_beats_live() {
        // A same-ts tombstone candidate replaces a live existing winner.
        assert!(cell_wins(&tomb(100), &live(100)));
        // A same-ts live candidate does NOT replace a tombstone winner.
        assert!(!cell_wins(&live(100), &tomb(100)));
    }

    #[test]
    fn equal_ts_tombstone_beats_expiring() {
        // An expiring (TTL) cell is modeled as LIVE here (it is not a tombstone):
        // the deletion still wins, BEFORE any localDeletionTime/expiry compare.
        let expiring = live(100); // carries a value + TTL → not a tombstone
        assert!(cell_wins(&tomb(100), &expiring));
        assert!(!cell_wins(&expiring, &tomb(100)));
    }

    #[test]
    fn equal_ts_equal_liveness_keeps_first_seen() {
        // Equal ts + equal liveness => no replacement (caller keeps first-seen).
        assert!(!cell_wins(&live(100), &live(100)));
        assert!(!cell_wins(&tomb(100), &tomb(100)));
    }

    // ── complex_deletion_supersedes: STRICT-greater, EQUAL does not ──────────

    #[test]
    fn complex_deletion_strict_supersede_boundary() {
        assert!(complex_deletion_supersedes(300, 200)); // strictly greater supersedes
        assert!(!complex_deletion_supersedes(200, 200)); // EQUAL does NOT supersede
        assert!(!complex_deletion_supersedes(100, 200)); // lesser does not supersede
    }

    // ── element_survives_complex_deletion: the `<=` shadow boundary ──────────

    #[test]
    fn element_shadow_before_purge_boundary() {
        // ts STRICTLY GREATER than mfda survives.
        assert!(element_survives_complex_deletion(201, 200));
        // ts EQUAL to mfda is shadowed (the `<=` boundary, #498).
        assert!(!element_survives_complex_deletion(200, 200));
        // ts LESS than mfda is shadowed.
        assert!(!element_survives_complex_deletion(199, 200));
    }
}
