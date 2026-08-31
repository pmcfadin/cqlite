//! Push-time columnar row accumulator: ONE cell resolution per row, feeding both
//! the Arrow build pass and the payload-byte accounting (issue #3552).
//!
//! # What was duplicated
//!
//! The `cqlite-flight` `do_get` row routes resolved every projected cell of every
//! row TWICE:
//!
//! 1. at PUSH time, [`estimate_arrow_row_bytes`](super::estimate_arrow_row_bytes)
//!    walked the projected columns and probed `row.values.get(col.name)` — one
//!    sip-hash string lookup into the row's own value map per column per row
//!    (`N·M` probes, including absent cells: the exact pattern issue #1495
//!    removed from the build pass);
//! 2. at FLUSH time, `arrow_columnar::transpose_columns` walked the buffered rows
//!    again to build the column-major cell slices the builders consume.
//!
//! Issue #3248 priced that first walk at 592 cyc/row (`Estimator::charge_slot`
//! 163 + `charge_child` 93 on top).
//!
//! # The fold
//!
//! [`ArrowRowAccumulator`] moves the build pass's FIRST STAGE — the transpose —
//! to push time and charges the estimator from the cells it has just resolved, so
//! a cell is resolved exactly once:
//!
//! * [`stage`](ArrowRowAccumulator::stage) consumes one row, routes each of its
//!   OWN entries into that column's staging slot through a small
//!   `name → column indices` map built once per column set (no probe against the
//!   large per-row map), and returns the row's payload width charged from those
//!   slots;
//! * [`commit`](ArrowRowAccumulator::commit) moves the staged cells into the
//!   column-major store;
//! * [`to_record_batch`](ArrowRowAccumulator::to_record_batch) hands each column's
//!   already-transposed cells straight to its builder — `transpose_columns` is not
//!   run at all on this path.
//!
//! The store is reused across batches ([`clear`](ArrowRowAccumulator::clear)
//! keeps every allocation), so the per-batch `n_cols × vec![None; n_rows]`
//! allocation is gone too.
//!
//! # Why stage/commit, and not push (the byte-budget contract — AC2)
//!
//! The width has two consumers that MUST run before any batch materializes:
//! `BatchByteCap::cut_before(width)` decides the batch boundary before the row
//! joins the buffer (test-then-push — building a batch to discover it is oversized
//! "is a report, not a cap"), and issue #2821's reserve-before-materialize turns
//! the accumulated payload into an egress credit reservation before the build runs.
//! So the width is still produced BEFORE the row joins the batch, and the caller's
//! sequence — `stage` → `cut_before` → (flush) → `commit` → `accumulate` — is
//! byte-for-byte the sequence it replaces (`estimate` → `cut_before` → (flush) →
//! `push` → `accumulate`). Thresholds, timing and batch boundaries are unchanged;
//! only the duplicated resolution is gone.
//!
//! A staged row deliberately SURVIVES a flush: `clear` empties the committed
//! store and leaves the staging slot alone, exactly as the old code pushed the
//! crossing row into the freshly emptied buffer.
//!
//! # Conservatism is preserved, including for ABSENT columns
//!
//! `Σ width(columns, row) >= arrow_payload_bytes(batch)` still holds. The one
//! place a fold like this can silently under-charge is an ABSENT cell:
//! `transpose_columns` never enumerates the projected columns per row, so it never
//! *sees* an absent cell, while the estimator charges one (its validity byte, its
//! shape's structural overhead and `column_slack_bytes`). This accumulator
//! therefore keeps the estimator's enumeration, not the transpose's: the staging
//! slot is a vector of ARITY `columns.len()` reset to `None` per row, and the
//! charge walks all of it — so an absent column arrives at the charging core as
//! `None`, which is exactly what a failed `row.values.get(name)` produces in
//! `estimate_arrow_row_bytes`. Nothing about the absent-column charge is
//! re-derived or approximated here.
//!
//! Both accountings share ONE charging core (`arrow_size::charge_row`) and differ
//! only in resolution, and their equivalence over the shared shape corpus —
//! including absent columns, saturating fan-out and duplicate column names — is
//! pinned by `arrow_row_accumulator_tests`.

use std::collections::HashMap;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::query::{ColumnInfo, QueryRow};
use crate::types::Value;

use super::arrow_convert::{build_arrow_schema, convert_column_to_array, ArrowConvertError};
use super::arrow_size::PreparedColumns;

/// Reusable column-major row accumulator for one projected column set.
///
/// Borrows the column set (it is stable for the whole scan) and owns the cells it
/// has taken out of the rows. See the module documentation for the stage/commit
/// contract and why it is shaped that way.
pub struct ArrowRowAccumulator<'a> {
    /// The projected output columns, in output order.
    columns: &'a [ColumnInfo],
    /// Per-column Arrow slot shapes, resolved once (never per row).
    prepared: PreparedColumns<'a>,
    /// `column name → ALL of its output indices`. Duplicate names (`SELECT a, a`)
    /// map to several indices, so a present cell reaches every matching column —
    /// the behaviour `transpose_columns` documents and preserves.
    name_to_indices: HashMap<&'a str, Vec<usize>>,
    /// Committed cells, column-major: `cells[c][i]` is row `i`'s cell for column
    /// `c`. Cleared (never reallocated) per batch.
    cells: Vec<Vec<Option<Value>>>,
    /// The row under test: one slot per column, all `None` between rows.
    staged: Vec<Option<Value>>,
    /// Whether [`Self::staged`] currently holds a staged, uncommitted row.
    has_staged: bool,
    /// Committed row count. Tracked explicitly so a ZERO-column projection still
    /// reports its rows.
    rows: usize,
}

impl<'a> ArrowRowAccumulator<'a> {
    /// Prepare an accumulator for `columns`, sized for `capacity` rows per batch.
    ///
    /// Column shapes and the `name → indices` map are resolved HERE, once, and
    /// reused for every row and every batch.
    pub fn with_capacity(columns: &'a [ColumnInfo], capacity: usize) -> Self {
        let mut name_to_indices: HashMap<&'a str, Vec<usize>> =
            HashMap::with_capacity(columns.len());
        for (idx, col) in columns.iter().enumerate() {
            name_to_indices
                .entry(col.name.as_str())
                .or_default()
                .push(idx);
        }
        Self {
            columns,
            prepared: PreparedColumns::new(columns),
            name_to_indices,
            cells: (0..columns.len())
                .map(|_| Vec::with_capacity(capacity))
                .collect(),
            staged: (0..columns.len()).map(|_| None).collect(),
            has_staged: false,
            rows: 0,
        }
    }

    /// Prepare an accumulator for `columns` with no pre-sizing.
    pub fn new(columns: &'a [ColumnInfo]) -> Self {
        Self::with_capacity(columns, 0)
    }

    /// The projected column set this accumulator transposes for.
    pub fn columns(&self) -> &'a [ColumnInfo] {
        self.columns
    }

    /// Committed rows in the current batch (a staged-but-uncommitted row is NOT
    /// counted — it has not joined the batch).
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Whether the current batch holds no committed row.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Resolve `row`'s projected cells ONCE into the staging slot and return the
    /// row's Arrow payload width — the same number
    /// [`estimate_arrow_row_bytes`](super::estimate_arrow_row_bytes) reports for
    /// the same `(columns, row)`, charged through the same core from the cells the
    /// build pass will consume.
    ///
    /// The row does NOT join the batch here: the caller may flush the committed
    /// rows first (test-then-push) and must then call [`Self::commit`].
    ///
    /// Staging twice without committing would be a caller-sequence bug. Rather
    /// than DROP the earlier row (silent data loss), the pending row is committed
    /// first; a debug build asserts instead, so the misuse is loud where it can be.
    pub fn stage(&mut self, row: QueryRow) -> usize {
        debug_assert!(
            !self.has_staged,
            "ArrowRowAccumulator::stage called with a row still staged — the \
             contract is stage -> (flush) -> commit per row (issue #3552)"
        );
        if self.has_staged {
            self.commit();
        }
        // Reset the staging slot: an ABSENT column must arrive at the charging
        // core as `None`, exactly as a failed `row.values.get(name)` does.
        for slot in &mut self.staged {
            *slot = None;
        }
        // Resolve by iterating the ROW's own entries — the large per-row value map
        // is never probed by column name (issue #1495 / parser epic J1). An entry
        // whose name is not projected is dropped, as the transpose dropped it.
        let staged = &mut self.staged;
        let name_to_indices = &self.name_to_indices;
        for (name, value) in row.values {
            if let Some(indices) = name_to_indices.get(name.as_ref()) {
                // One value, possibly several output columns: the LAST index takes
                // it by move, the others clone. Duplicate output columns for one
                // name (`SELECT a, a`) are the only case that clones, and the
                // clones are equal values, so the batch is byte-identical to the
                // transpose's replicated reference.
                if let Some((&last, rest)) = indices.split_last() {
                    for &idx in rest {
                        if let Some(slot) = staged.get_mut(idx) {
                            *slot = Some(value.clone());
                        }
                    }
                    if let Some(slot) = staged.get_mut(last) {
                        *slot = Some(value);
                    }
                }
            }
        }
        self.has_staged = true;
        self.prepared.row_bytes(&self.staged)
    }

    /// Move the staged row's cells into the batch. A no-op when nothing is staged.
    pub fn commit(&mut self) {
        if !self.has_staged {
            return;
        }
        for (column, slot) in self.cells.iter_mut().zip(self.staged.iter_mut()) {
            column.push(slot.take());
        }
        self.rows += 1;
        self.has_staged = false;
    }

    /// Drop every COMMITTED row, keeping all allocations for the next batch.
    ///
    /// A staged-but-uncommitted row is deliberately left staged: it belongs to the
    /// NEXT batch, which is what makes the test-then-push boundary work.
    pub fn clear(&mut self) {
        for column in &mut self.cells {
            column.clear();
        }
        self.rows = 0;
    }

    /// Re-derive the payload estimate of exactly the COMMITTED rows, charged from
    /// the stored cells (not from a remembered running total).
    ///
    /// This is the O(rows) recomputation an incremental accumulator exists to
    /// avoid; it exists so a consumer can keep an "accumulator describes exactly
    /// the buffered rows" invariant enforced rather than merely documented.
    pub fn recomputed_payload(&self) -> usize {
        (0..self.rows).fold(0usize, |acc, row| {
            acc.saturating_add(self.prepared.row_bytes_columnar(&self.cells, row))
        })
    }

    /// Build the Arrow batch for the committed rows from the ALREADY-transposed
    /// column-major cells.
    ///
    /// Identical output to `rows_to_record_batch(columns, rows)` over the same
    /// rows: the same schema, the same per-column builder dispatch, and the same
    /// cell slices — only the transpose has moved to push time.
    ///
    /// # Errors
    ///
    /// [`ArrowConvertError`] if a value cannot be represented in the target Arrow
    /// type, or if schema/array construction fails.
    pub fn to_record_batch(&self) -> Result<RecordBatch, ArrowConvertError> {
        let schema = Arc::new(build_arrow_schema(self.columns)?);
        // Fail closed rather than build a short array list `RecordBatch::try_new`
        // would have to reject: the two must be the same arity by construction.
        if self.cells.len() != self.columns.len() || self.prepared.len() != self.columns.len() {
            return Err(ArrowConvertError::InvalidValue(format!(
                "columnar accumulator arity {} does not match the {} projected columns",
                self.cells.len(),
                self.columns.len()
            )));
        }
        // One borrowed view, reused across columns, so the per-column reference
        // slice the builders take costs one allocation per BATCH, not per column.
        let mut view: Vec<Option<&Value>> = Vec::with_capacity(self.rows);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        for (col, cells) in self.columns.iter().zip(self.cells.iter()) {
            view.clear();
            view.extend(cells.iter().map(Option::as_ref));
            arrays.push(convert_column_to_array(col, &view)?);
        }
        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

#[cfg(test)]
#[path = "arrow_row_accumulator_tests.rs"]
mod arrow_row_accumulator_tests;
