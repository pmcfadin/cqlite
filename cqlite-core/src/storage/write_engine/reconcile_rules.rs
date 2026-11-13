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

/// Common view of a reconcilable cell: exactly the fields Cassandra's
/// `Cells.resolveRegular` (`db/rows/Cells.java:79-129`, cassandra-5.0.8) consults.
///
/// Each call site implements this over its concrete type
/// ([`CellData`](super::merge::CellData) on the merge path, `MergedOp` on the writer path).
/// How a site RECOGNIZES a tombstone — a `Value::Tombstone` payload vs. a
/// `CellOperation::Delete` op — stays with the type.
pub(crate) trait ReconcileCell {
    /// Cell write timestamp in microseconds (`markedForDeleteAt` for a deletion).
    fn timestamp(&self) -> i64;
    /// Whether this cell is a deletion (tombstone) rather than a live/expiring value.
    fn is_tombstone(&self) -> bool;
    /// `Some(ttl)` for an EXPIRING cell (`Cell.isExpiring()`, `ttl != NO_TTL`); `None` for a
    /// plain live cell or a tombstone.
    fn expiring_ttl(&self) -> Option<u32> {
        None
    }
    /// The on-disk `localDeletionTime` (unsigned GC-clock seconds, carried in an `i32`): the
    /// EXPIRY instant of an expiring cell, the deletion instant of a tombstone. Unused for a
    /// plain live cell.
    fn local_deletion_time(&self) -> i32 {
        0
    }
    /// The serialized value bytes `Cells.compareValues` (`ValueAccessor.compare`: unsigned
    /// lexicographic) orders by. Only consulted on a full tie, so it may allocate.
    fn value_bytes(&self) -> Vec<u8> {
        Vec::new()
    }
}

/// Cassandra `Cells.resolveRegular` (cassandra-5.0.8 `db/rows/Cells.java:79-129`): returns
/// `true` when `candidate` should replace the current `existing` winner. It is a TOTAL order, so
/// the winner is the same whichever cell arrives first (`Cells.reconcile` javadoc, `:59`:
/// "commutative over its cells arguments"). Decided IN THIS ORDER, short-circuiting:
///
/// 1. **timestamp** — strictly higher wins.
/// 2. **expiring-or-tombstone beats live** at equal timestamp (`:93-98`; CASSANDRA-14592 — an
///    expiring cell is treated like a tombstone because it becomes one).
/// 3. **tombstone beats expiring** (`:100-108`).
/// 4. both tombstones, or both expiring: the larger `localDeletionTime` wins (`:111-118`).
/// 5. both expiring with equal `localDeletionTime`: the LOWER TTL wins (`:120-125`).
/// 6. otherwise the larger serialized value (`compareValues(left, right) >= 0`, `:128`); a full
///    tie keeps `existing` (the two cells are then indistinguishable).
pub(crate) fn cell_wins<C: ReconcileCell + ?Sized>(candidate: &C, existing: &C) -> bool {
    if candidate.timestamp() != existing.timestamp() {
        return candidate.timestamp() > existing.timestamp();
    }
    let cand_flagged = candidate.is_tombstone() || candidate.expiring_ttl().is_some();
    let exist_flagged = existing.is_tombstone() || existing.expiring_ttl().is_some();
    if cand_flagged != exist_flagged {
        return cand_flagged;
    }
    if cand_flagged {
        if candidate.is_tombstone() != existing.is_tombstone() {
            return candidate.is_tombstone();
        }
        // localDeletionTime is an UNSIGNED 32-bit GC-clock value carried in an i32.
        let (cl, el) = (
            candidate.local_deletion_time() as u32,
            existing.local_deletion_time() as u32,
        );
        if cl != el {
            return cl > el;
        }
        if let (Some(ct), Some(et)) = (candidate.expiring_ttl(), existing.expiring_ttl()) {
            if ct != et {
                return ct < et;
            }
        }
    }
    candidate.value_bytes() > existing.value_bytes()
}

/// Cassandra `DeletionTime.supersedes` (cassandra-5.0.8 `db/DeletionTime.java:158`): a deletion
/// `(markedForDeleteAt, localDeletionTime)` supersedes another when its `markedForDeleteAt` is
/// higher, or EQUAL with a higher `localDeletionTime` (unsigned GC-clock seconds). Used for ROW,
/// RANGE and PARTITION deletions, so equal-timestamp deletions resolve by LDT in either order.
pub(crate) fn deletion_supersedes(candidate: (i64, i32), current: (i64, i32)) -> bool {
    candidate.0 > current.0
        || (candidate.0 == current.0 && (candidate.1 as u32) > (current.1 as u32))
}

/// `supersedes` or equal (`>=`): the newest deletion of a group, last of equals.
pub(crate) fn deletion_ge(candidate: (i64, i32), current: (i64, i32)) -> bool {
    candidate == current || deletion_supersedes(candidate, current)
}

/// [`deletion_ge`] against a possibly-absent current deletion.
pub(crate) fn ge_opt(current: Option<(i64, i32)>, candidate: (i64, i32)) -> bool {
    current.is_none_or(|c| deletion_ge(candidate, c))
}

/// [`deletion_supersedes`] for a possibly-absent current deletion `(markedForDeleteAt)` paired with
/// its `localDeletionTime` (the merge keeps the two in separate fields).
pub(crate) fn supersedes_opt(
    current_mfda: Option<i64>,
    current_ldt: i32,
    candidate: (i64, i32),
) -> bool {
    current_mfda.is_none_or(|d| deletion_supersedes(candidate, (d, current_ldt)))
}

/// [`deletion_supersedes`] for two range tombstones.
pub(crate) fn rt_supersedes(
    candidate: &crate::storage::write_engine::mutation::RangeTombstone,
    current: &crate::storage::write_engine::mutation::RangeTombstone,
) -> bool {
    deletion_supersedes(
        (candidate.deletion_time, candidate.local_deletion_time),
        (current.deletion_time, current.local_deletion_time),
    )
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
