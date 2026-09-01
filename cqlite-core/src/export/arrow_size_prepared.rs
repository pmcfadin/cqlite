//! Per-ROW charging over a RESOLVED column set (issues #2825 / #3552).
//!
//! # The responsibility boundary
//!
//! `arrow_size.rs` owns the charging MODEL — the structural constants, the public
//! entry points, and [`Estimator`](super::Estimator), which charges ONE Arrow
//! slot and the child slots it implies. This file owns the layer above it: given a
//! whole ROW's worth of already-resolved cells, walk the projected columns and
//! charge each one's slot.
//!
//! That is one responsibility with two callers, which is exactly why it is its own
//! file:
//!
//! * [`charge_row`] is the loop BOTH accountings share —
//!   [`estimate_arrow_row_bytes`](super::estimate_arrow_row_bytes), which resolves
//!   each cell with a `values.get(name)` probe, and the fused push-time accounting
//!   below, which resolves them once for the Arrow build pass. Sharing this loop is
//!   what stops the two from drifting into two charging models; they differ ONLY in
//!   how a cell is resolved, and their equivalence is pinned per row over the shared
//!   shape corpus by `arrow_row_accumulator`'s
//!   `fused_width_equals_the_standalone_estimate_over_the_shape_corpus`.
//! * [`PreparedColumns`] is the fused side's cache: the per-column slot SHAPES,
//!   resolved once per column set instead of once per row, plus the two cell
//!   resolutions that map `super::super::arrow_row_accumulator`'s storage layouts
//!   (a dense row-major staging slot, and a sparse column-major store) onto the
//!   `(shape, cell)` pairs [`charge_row`] consumes.
//!
//! So the seam is per-SLOT charging (parent) versus per-ROW iteration over a
//! resolved column set (here) — not a line count. Nothing here knows how a slot is
//! charged, and nothing in the parent knows how a caller stores its cells.
//!
//! Split out of `arrow_size.rs` when the fused accounting pushed that file past the
//! campsite threshold (epic #1116; the file was 657 lines before issue #3552 and
//! 813 after). Declared as a CHILD module of `arrow_size`, so it can see that
//! module's private [`Estimator`](super::Estimator) and [`Shape`](super::Shape) —
//! the same wiring `arrow_size_shape.rs` and `arrow_size_render.rs` use.
//!
//! Visibility is unchanged by the move: [`PreparedColumns`] and its methods stay
//! `pub(in crate::export)` and are re-exported from `arrow_size` under that same
//! restriction, so the accumulator's import path does not change and no surface
//! widened to make the split possible.

use super::{column_shape, column_slack_bytes, Estimator, Shape};
use crate::query::ColumnInfo;
use crate::types::Value;

/// The charging core BOTH accountings share (issue #3552).
///
/// Takes one `(slot shape, resolved cell)` pair per projected column, in column
/// order, and charges the model this file documents. `None` is an ABSENT column
/// and is charged exactly like a present-but-null one: its validity byte, its
/// shape's structural overhead and its per-column residual are all still owed,
/// because the converter materializes a slot for every projected column of every
/// row whether the row carries the cell or not.
///
/// Sharing this is what stops the standalone estimator and the push-time fused
/// accounting from drifting into two charging models — they differ ONLY in how a
/// column's cell is resolved, which is the property the fused accounting's
/// equivalence test pins.
///
/// `pub(super)` and no wider: `arrow_size`'s own `estimate_arrow_row_bytes` is the
/// second caller (a PARENT cannot see a child's private items, unlike the reverse),
/// and nothing outside `arrow_size` has any business charging a row directly.
pub(super) fn charge_row<'a, I>(cells: I) -> usize
where
    I: IntoIterator<Item = (Shape<'a>, Option<&'a Value>)>,
{
    let mut est = Estimator::new();
    for (shape, cell) in cells {
        // Both node budgets are per COLUMN (review C2).
        est.begin_column();
        // The per-column residual, charged exactly once per row and only for a
        // column whose builder materializes a childless array node (review C1).
        est.add(column_slack_bytes(&shape));
        // A LEAF column is charged in place, so the narrow path never allocates
        // the worklist at all (`Vec::new` does not allocate until its first
        // push). Only collection/struct cells reach `drain`.
        est.charge_child(shape, cell);
        est.drain();
        if est.total == usize::MAX {
            return usize::MAX;
        }
    }
    est.total
}

/// One cell out of a ROW-MAJOR staging slot, addressed by canonical index.
///
/// A free function (not a closure at the call site) so the `.zip(..)` argument
/// stays short enough to read — and so the indirection is named once.
fn staged_cell(cells: &[Option<Value>], c: usize) -> Option<&Value> {
    cells.get(c).and_then(Option::as_ref)
}

/// One cell out of a SPARSE column-major cell store, addressed by canonical
/// index and row: `cells[c]` holds `(row index, value)` for each PRESENT cell,
/// in ascending row order.
///
/// `None` means the row carries no cell for that column — a legitimate ABSENT
/// cell, charged exactly as `estimate_arrow_row_bytes` charges a failed
/// `values.get(name)`, never zero. The binary search is `O(log present)` and is
/// only on the re-derivation path (`recomputed_payload`), which is the O(rows)
/// recomputation the incremental accumulator exists to avoid in the first place —
/// the PRODUCTION charge reads the dense staging slot in `row_bytes`.
fn sparse_cell(cells: &[Vec<(usize, Value)>], c: usize, row: usize) -> Option<&Value> {
    let store = cells.get(c)?;
    let at = store.binary_search_by_key(&row, |(r, _)| *r).ok()?;
    store.get(at).map(|(_, value)| value)
}

/// Per-column Arrow slot shapes resolved ONCE for a column set, so the fused
/// push-time accounting does not re-run `column_shape` per row (issue #3552).
///
/// Visible ONLY inside `crate::export`: this is the seam between the estimator and
/// `super::arrow_row_accumulator`, not a public contract and not a crate-wide one
/// (issue #3552 review N5). `estimate_arrow_row_bytes` remains the public surface.
pub(in crate::export) struct PreparedColumns<'a> {
    shapes: Vec<Shape<'a>>,
}

impl<'a> PreparedColumns<'a> {
    /// Resolve every column's Arrow slot shape once.
    pub(in crate::export) fn new(columns: &'a [ColumnInfo]) -> Self {
        Self {
            shapes: columns.iter().map(column_shape).collect(),
        }
    }

    /// Number of prepared columns.
    pub(in crate::export) fn len(&self) -> usize {
        self.shapes.len()
    }

    /// Width of one row whose cells are ALREADY resolved, read through a
    /// `canonical` indirection: column `i`'s cell is `cells[canonical[i]]`.
    ///
    /// The indirection is what lets a value be STORED ONCE and charged for every
    /// output column that names it. Two output columns with the same name
    /// (`SELECT a, a`) share one canonical slot, so this charges them both —
    /// exactly as `estimate_arrow_row_bytes` charges both, resolving the name
    /// twice — without the value existing twice (issue #3552 review B3).
    /// `canonical[i] == i` for every column with a unique name.
    ///
    /// Fails closed to `usize::MAX` on an arity mismatch or an out-of-range
    /// canonical index. A short `canonical` would otherwise silently drop the
    /// trailing columns' charges (`zip` truncates) and UNDER-count, the one
    /// direction the conservatism contract forbids.
    pub(in crate::export) fn row_bytes(
        &self,
        cells: &[Option<Value>],
        canonical: &[usize],
    ) -> usize {
        if canonical.len() != self.shapes.len() {
            return usize::MAX;
        }
        if canonical.iter().any(|&c| c >= cells.len()) {
            return usize::MAX;
        }
        charge_row(
            self.shapes
                .iter()
                .copied()
                .zip(canonical.iter().map(|&c| staged_cell(cells, c))),
        )
    }

    /// Width of row `row` of a SPARSE column-major cell store, read through the
    /// same `canonical` indirection: column `i`'s cell is the entry for `row` in
    /// `cells[canonical[i]]`, or absent.
    ///
    /// The layout the accumulator commits into, so a buffered row's width can be
    /// re-derived from the stored cells rather than from a remembered number.
    ///
    /// Fails closed to `usize::MAX` on an arity mismatch or an out-of-range
    /// canonical index, for the same reason as [`Self::row_bytes`]. It does NOT
    /// fail closed on a row missing from a column's store: in a sparse store that
    /// is an ABSENT cell, charged as `None` — the same charge
    /// `estimate_arrow_row_bytes` makes. `row < rows` is the CALLER's invariant
    /// (`recomputed_payload` iterates `0..rows`); a cell wrongly missing from the
    /// store would make this total come out SMALLER than the running accumulator,
    /// which is exactly what `flush_credited`'s debug assertion compares.
    pub(in crate::export) fn row_bytes_columnar(
        &self,
        cells: &[Vec<(usize, Value)>],
        canonical: &[usize],
        row: usize,
    ) -> usize {
        if canonical.len() != self.shapes.len() {
            return usize::MAX;
        }
        if canonical.iter().any(|&c| c >= cells.len()) {
            return usize::MAX;
        }
        charge_row(
            self.shapes
                .iter()
                .copied()
                .zip(canonical.iter().map(|&c| sparse_cell(cells, c, row))),
        )
    }
}
