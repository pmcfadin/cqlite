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
//!   column-major store — into ONE store per distinct projected NAME, so a value
//!   named by several output columns (`SELECT a, a`) is stored once and read twice
//!   through the `canonical` index, never cloned (issue #3552 review B3), and
//!   storing only PRESENT cells, so an absent cell costs no slot (review B4);
//! * [`to_record_batch`](ArrowRowAccumulator::to_record_batch) hands each column's
//!   already-transposed cells straight to its builder — `transpose_columns` is not
//!   run at all on this path.
//!
//! The store is reused across batches ([`clear`](ArrowRowAccumulator::clear)
//! keeps every allocation), so the per-batch `n_cols × vec![None; n_rows]`
//! allocation is gone too.
//!
//! # Memory profile — sparse persistent, dense transient
//!
//! What is RETAINED between batches is one `(row index, value)` pair per PRESENT
//! cell, per distinct projected name. Nothing retained scales with
//! `n_cols × rows`: an absent cell costs no slot, and a duplicate output column
//! costs one `usize` in `canonical`, not a copy. What is DENSE is built only
//! inside [`to_record_batch`](ArrowRowAccumulator::to_record_batch), into ONE
//! reused `Vec<Option<&Value>>` of `rows` borrowed slots, and dropped with the
//! batch.
//!
//! This matters because of what the byte-cap does NOT bound (issue #3552 reviews
//! B1/B3/B4, three instances of one family): the cap is denominated in PAYLOAD
//! bytes, so a wide SPARSE projection has small payload, does not trip the cap
//! early, and fills the buffer to the ROW cap. Any per-row cost proportional to
//! projection WIDTH rather than to payload is therefore effectively unbounded by
//! the governor, and must not be retained. The rule this module is built to: ask
//! of every allocation not just "is it bounded", but "bounded by what, in the
//! worst case over BOTH projection width AND sparsity?" — and "the caps bound it"
//! is a valid answer only for a quantity proportional to PAYLOAD.
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
//! # What deliberately still uses the standalone estimator
//!
//! The AGGREGATE route (`cqlite_flight::producer`'s `split_rows_into_batches`)
//! folds rows into accumulator state and materializes one PARTIAL row per
//! `GROUP BY` group in one go, then applies the batch boundary AFTER THE FACT to
//! an already-materialized row slice. It has no incremental push loop, so this
//! seam does not reach it and it keeps calling
//! [`estimate_arrow_row_bytes`](super::estimate_arrow_row_bytes) per row. Nothing
//! there is duplicated work of the kind this module removes: that route resolves
//! the cells once for the estimate and once for the build, but its rows are
//! already in hand and are a per-GROUP count, not a per-ROW-of-the-scan one.
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
/// Total retained cell slots tolerated after a batch, as a multiple of the largest
/// batch's own present-cell count. Slack, not a hard equality, so an ordinary
/// batch-to-batch density wobble does not reallocate.
const RETAINED_SLOT_SLACK: usize = 2;

/// Floor below which retained slots are never trimmed, so a small-batch workload
/// never pays repeated reallocation to reclaim a few hundred slots.
const RETAINED_SLOT_FLOOR: usize = 1024;

pub struct ArrowRowAccumulator<'a> {
    /// The projected output columns, in output order.
    columns: &'a [ColumnInfo],
    /// Per-column Arrow slot shapes, resolved once (never per row).
    prepared: PreparedColumns<'a>,
    /// `column name → its CANONICAL output index` — the FIRST column with that
    /// name. Duplicate output columns for one name (`SELECT a, a`) resolve to the
    /// same canonical index, which is how one stored value serves all of them.
    name_to_canonical: HashMap<&'a str, usize>,
    /// `output column index → the index whose store holds its cells`.
    /// `canonical[i] == i` for a unique name; for a duplicate it points at the
    /// first column of that name. Every read of a cell — the width charge, the
    /// re-derivation, and the batch build — goes through this, so a value is
    /// stored ONCE however many output columns name it (issue #3552 review B3).
    canonical: Vec<usize>,
    /// Committed cells, column-major and **SPARSE**: `cells[c]` holds one
    /// `(row index, value)` pair per PRESENT cell of canonical column `c`, in
    /// ascending row order (appended as rows commit, so it is sorted for free).
    ///
    /// An ABSENT cell occupies NO slot. That is the whole point of the
    /// representation (issue #3552 review B4): a DENSE
    /// `Vec<Vec<Option<Value>>>` costs `n_canonical_cols × rows` slots of
    /// `size_of::<Option<Value>>()` whatever fraction of them is present, and
    /// `clear` retains that capacity — so a WIDE SPARSE projection (ordinary in
    /// Cassandra) retained tens of MB that NEITHER cap bounds: the byte cap bounds
    /// PAYLOAD, and sparse rows have little payload, so it does not cut early and
    /// the buffer fills to the row cap. Sparse storage makes the persistent cost
    /// proportional to present cells, which is what the payload cap is
    /// proportional to as well. The builders' dense row-aligned slice is
    /// materialized TRANSIENTLY at flush, into one reused view — the same
    /// sparse-persistent / dense-transient shape `main` had when it buffered
    /// `Vec<QueryRow>` (maps hold only present cells) and transposed at flush.
    ///
    /// Only a CANONICAL `c` is ever stored or read: a non-canonical
    /// (duplicate-name) column's store stays EMPTY for the whole scan, and its
    /// cells come from `cells[canonical[c]]` (review B3). Cleared (never
    /// reallocated) per batch.
    cells: Vec<Vec<(usize, Value)>>,
    /// The row under test: one slot per column, all `None` between rows.
    staged: Vec<Option<Value>>,
    /// Whether [`Self::staged`] currently holds a staged, uncommitted row.
    has_staged: bool,
    /// Committed row count, tracked explicitly rather than read off a column's
    /// length — so it is available for a ZERO-column projection, where there is no
    /// column to read.
    ///
    /// Which surface reports it, precisely: [`Self::len`] reports THIS count for
    /// every projection, including a zero-column one. [`Self::to_record_batch`]
    /// does NOT — with no columns there is no array to carry a length, so the batch
    /// cannot report these rows (it reports 0, or refuses; the terminal behaviour is
    /// `RecordBatch::try_new`'s). That divergence is PRE-EXISTING — pre-fold
    /// `rows_to_record_batch` ends in the same `try_new` over an empty array list —
    /// and is deliberately not changed here, since doing so would change Arrow
    /// output inside a behaviour-preserving refactor. Whether a zero-column
    /// projection is reachable on the `do_get`/streaming path is unresolved; issue
    /// #3742 owns both questions.
    rows: usize,
    /// Largest total PRESENT-cell count any single batch has held — the basis for
    /// how much capacity may stay resident between batches. A batch's own present
    /// cells are what the byte cap bounds, so they are the only sound basis.
    peak_cells: usize,
}

impl<'a> ArrowRowAccumulator<'a> {
    /// Prepare an accumulator for `columns`.
    ///
    /// Column shapes and the `name → indices` map are resolved HERE, once, and
    /// reused for every row and every batch. The per-column cell stores start
    /// EMPTY — see [`Self::with_capacity`] for why they are never pre-sized.
    pub fn new(columns: &'a [ColumnInfo]) -> Self {
        // FIRST occurrence of a name wins (`or_insert`), so the canonical slot of
        // a duplicated name is its first output column. One `usize` per NAME, not
        // a `Vec<usize>` per name — the fan-out lives in `canonical` below.
        let mut name_to_canonical: HashMap<&'a str, usize> = HashMap::with_capacity(columns.len());
        for (idx, col) in columns.iter().enumerate() {
            name_to_canonical.entry(col.name.as_str()).or_insert(idx);
        }
        // `unwrap_or(idx)` is unreachable — every column's name was just inserted —
        // and its fallback is the correct answer anyway (a column is its own
        // canonical slot), so it cannot silently mis-resolve.
        let canonical: Vec<usize> = columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                name_to_canonical
                    .get(col.name.as_str())
                    .copied()
                    .unwrap_or(idx)
            })
            .collect();
        Self {
            columns,
            prepared: PreparedColumns::new(columns),
            name_to_canonical,
            canonical,
            // Do NOT pre-size per column: `n_cols × capacity ×
            // size_of::<Option<Value>>()` is the `batch_size × width` product
            // issue #2825's byte-cap exists to BOUND, and pre-sizing pays it
            // eagerly, before a single row arrives, on every per-request drive
            // loop (so a `SELECT … LIMIT 1` point read pays a full batch's
            // reservation). With `size_of::<Value>() <= 40` and an 8192-row
            // batch that is ~65 MB at 200 columns and ~131 MB at 400 — over the
            // <128 MB target, on ONE stream, times `--max-concurrent-scans`, and
            // `batch_size` is operator-settable with no upper clamp. Because
            // `clear` RETAINS capacity, pre-sizing only ever bought the FIRST
            // batch's amortized growth (issue #3552 review B1).
            //
            // An earlier revision added "the store reaches the same steady state after
            // one batch either way" here. That is FALSE for a density pattern that MOVES
            // between columns, and `clear` now BOUNDS what survives rather than resting
            // on that argument — see the trim there.
            cells: (0..columns.len()).map(|_| Vec::new()).collect(),
            staged: vec![None; columns.len()],
            has_staged: false,
            rows: 0,
            peak_cells: 0,
        }
    }

    /// Prepare an accumulator for `columns` whose batches hold up to `capacity`
    /// rows.
    ///
    /// `capacity` is accepted for call-site clarity — the caller knows the row cap
    /// its batches are bounded by — and is DELIBERATELY not used to pre-allocate
    /// the per-column cell stores: that product is exactly the eager
    /// `n_cols × batch_size` residency [`Self::new`] documents (issue #3552
    /// review B1).
    ///
    /// This doc previously argued the store "reaches its steady state after one
    /// batch regardless" because `clear` retains capacity. True only for a STABLE
    /// density pattern; see [`Self::clear`], which bounds the resident total.
    pub fn with_capacity(columns: &'a [ColumnInfo], capacity: usize) -> Self {
        let _ = capacity;
        Self::new(columns)
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
    /// # Precondition — one `stage` per `commit`
    ///
    /// The caller MUST call [`Self::commit`] (or abandon the accumulator) before
    /// the next `stage`; both `do_get` row routes do exactly that, in straight-line
    /// code. There is deliberately NO recovery for staging twice, and debug and
    /// release behave the SAME WAY (issue #3552 review B2): a debug build fails the
    /// assertion below, and a release build resets the staging slot, so the earlier
    /// row's cells are dropped and it never joins a batch.
    ///
    /// That is a lost row in a hypothetical mis-sequenced caller — but it is NOT a
    /// byte-budget violation, which is the property AC2 forbids moving: the width
    /// is `accumulate`d by the caller only AFTER `commit`, so a row that never
    /// commits is never charged either, and `byte_cap.accumulated()` still
    /// describes exactly the committed rows (the invariant `flush_credited`
    /// asserts). An earlier version *recovered* by committing the pending row here,
    /// which was the unsafe direction: that row would have joined the batch with
    /// its width never accumulated, under-counting the accumulator and so
    /// UNDER-RESERVING issue #2821's pre-materialization egress credit — silently,
    /// and only in release, since the `debug_assert` made the recovery branch
    /// unreachable in every debug build and untestable.
    pub fn stage(&mut self, row: QueryRow) -> usize {
        debug_assert!(
            !self.has_staged,
            "ArrowRowAccumulator::stage called with a row still staged — the \
             contract is stage -> (flush) -> commit per row (issue #3552)"
        );
        // Both are built from `columns` in `new` and neither is ever resized, so
        // every index in `name_to_canonical` is in range for `staged` — asserted
        // rather than left to a permissive `get_mut` below (issue #3552 review N3).
        debug_assert_eq!(
            self.staged.len(),
            self.columns.len(),
            "staging arity must equal the projected column count"
        );
        // Reset the staging slot: an ABSENT column must arrive at the charging
        // core as `None`, exactly as a failed `row.values.get(name)` does.
        for slot in &mut self.staged {
            *slot = None;
        }
        // Resolve by iterating the ROW's own entries — the large per-row value map
        // is never probed by column name (issue #1495 / parser epic J1). An entry
        // whose name is not projected is dropped, as the transpose dropped it.
        let staged = &mut self.staged;
        let name_to_canonical = &self.name_to_canonical;
        for (name, value) in row.values {
            if let Some(&canonical_idx) = name_to_canonical.get(name.as_ref()) {
                // MEMORY: each row value is MOVED into exactly one slot and is
                // never cloned — including when several output columns name it
                // (`SELECT a, a`). The duplicate columns read this same canonical
                // slot at charge time and at build time, so the fan-out costs
                // nothing beyond a second `usize` index, exactly as
                // `transpose_columns` fanned out a `&Value` for free.
                //
                // Cloning here instead would deep-copy the payload once per
                // duplicate column — recursively for a collection, tuple, UDT,
                // JSON or decimal — at STAGE time, i.e. BEFORE `cut_before`
                // decides the batch boundary and before the egress reservation
                // exists: unbounded memory taken outside the governed window and
                // ahead of the admission decision (issue #3552 review B3, the same
                // family as review B1's eager reservation). The output would be
                // byte-identical either way, which is precisely why saying only
                // that is not enough.
                //
                // Direct indexing, not a `get_mut` whose `None` arm would SKIP a
                // column the row carries: the index came from `name_to_canonical`,
                // built from the same `columns` that sized `staged`, so it is in
                // range by construction (review N3).
                staged[canonical_idx] = Some(value);
            }
        }
        self.has_staged = true;
        self.prepared.row_bytes(&self.staged, &self.canonical)
    }

    /// Move the staged row's cells into the batch. A no-op when nothing is staged.
    ///
    /// Only CANONICAL columns are appended to: a duplicate-name column has no
    /// store of its own and reads its canonical column's, so a duplicated value is
    /// held once rather than once per output column (issue #3552 review B3). Its
    /// store therefore stays empty for the whole scan — every reader goes through
    /// `canonical`, so nothing ever indexes it directly.
    ///
    /// Only PRESENT cells are appended: an absent cell costs NO slot, so the
    /// store's size tracks present cells rather than `n_cols × rows` (review B4).
    /// An explicit `Value::Null` IS present and IS stored — the absent/null
    /// distinction the builders rely on is preserved exactly, since a row missing
    /// from a column's store reads back as `None` and a stored `Value::Null` reads
    /// back as `Some(&Value::Null)`, which is what `transpose_columns` produced.
    pub fn commit(&mut self) {
        if !self.has_staged {
            return;
        }
        let row_idx = self.rows;
        let Self {
            cells,
            staged,
            canonical,
            ..
        } = self;
        for (idx, slot) in staged.iter_mut().enumerate() {
            // Always TAKE, so a value can never survive into the next row —
            // including for a duplicate-name column, where it is dropped (it is
            // `None` already, since `stage` only ever fills canonical slots, but
            // taking it makes that true by construction rather than by argument).
            let value = slot.take();
            match (canonical.get(idx), cells.get_mut(idx)) {
                // The canonical column for this name owns the storage — and stores
                // the cell ONLY if the row carries one.
                (Some(&c), Some(store)) if c == idx => {
                    if let Some(value) = value {
                        store.push((row_idx, value));
                    }
                }
                // A duplicate-name column, or an arity mismatch: nothing to store.
                _ => {}
            }
        }
        // Saturating for hygiene: a batch is bounded by the row cap long before
        // this could matter, but a wrapping row count would corrupt the arity
        // checks that guard the charge.
        self.rows = self.rows.saturating_add(1);
        self.has_staged = false;
    }

    /// Drop every COMMITTED row, keeping all allocations for the next batch.
    ///
    /// A staged-but-uncommitted row is deliberately left staged: it belongs to the
    /// NEXT batch, which is what makes the test-then-push boundary work.
    pub fn clear(&mut self) {
        // A batch's OWN present-cell count is what the byte cap bounds, so it is the
        // only sound basis for what may stay resident. Measured BEFORE the clear.
        let used: usize = self.cells.iter().map(Vec::len).sum();
        self.peak_cells = self.peak_cells.max(used);

        for column in &mut self.cells {
            column.clear();
        }

        // WHY A TRIM IS NEEDED AT ALL. `Vec::clear` retains capacity, and capacity is
        // retained PER COLUMN — so across batches whose density MOVES BETWEEN COLUMNS
        // the resident total converges on the SUM of per-column high-water marks, not
        // the high-water mark of the sum. One dense column per batch, rotating,
        // reaches the full dense `n_cols × batch_size` residency this sparse
        // representation exists to avoid, and NEITHER cap bounds it: the byte cap
        // bounds a batch's PAYLOAD and says nothing about what survives BETWEEN
        // batches. Two comments in this file asserted the opposite until issue #3552
        // roborev round 6; pinned by
        // `rotating_density_does_not_accumulate_per_column_capacity`.
        let allowance = self
            .peak_cells
            .saturating_mul(RETAINED_SLOT_SLACK)
            .max(RETAINED_SLOT_FLOOR);
        let retained: usize = self.cells.iter().map(Vec::capacity).sum();
        if retained > allowance {
            // An EQUAL per-column share, so the bound holds for ANY density
            // distribution rather than for the one that happened to occur. Stores are
            // shrunk, never dropped: the next batch re-grows only what it uses. The
            // `.max(1)` keeps one warm slot per column on a projection wider than the
            // floor, where an equal share would round to zero and churn every batch.
            let share = (allowance / self.cells.len().max(1)).max(1);
            for column in &mut self.cells {
                if column.capacity() > share {
                    column.shrink_to(share);
                }
            }
        }
        self.rows = 0;
    }

    /// Total retained cell slots across every column store — the quantity
    /// [`Self::clear`]'s trim bounds. Exposed so that bound is TESTED rather than
    /// merely asserted in a comment.
    #[cfg(test)]
    pub(crate) fn retained_cell_slots(&self) -> usize {
        self.cells.iter().map(Vec::capacity).sum()
    }

    /// Re-derive the payload estimate of exactly the COMMITTED rows, charged from
    /// the stored cells (not from a remembered running total).
    ///
    /// This is the O(rows) recomputation an incremental accumulator exists to
    /// avoid; it exists so a consumer can keep an "accumulator describes exactly
    /// the buffered rows" invariant enforced rather than merely documented.
    pub fn recomputed_payload(&self) -> usize {
        (0..self.rows).fold(0usize, |acc, row| {
            acc.saturating_add(
                self.prepared
                    .row_bytes_columnar(&self.cells, &self.canonical, row),
            )
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
        // would have to reject: the three must be the same arity by construction.
        if self.cells.len() != self.columns.len()
            || self.canonical.len() != self.columns.len()
            || self.prepared.len() != self.columns.len()
        {
            return Err(ArrowConvertError::InvalidValue(format!(
                "columnar accumulator arity {} does not match the {} projected columns",
                self.cells.len(),
                self.columns.len()
            )));
        }
        // ONE dense borrowed view, reused across columns: the builders need a
        // row-aligned `&[Option<&Value>]`, but only TRANSIENTLY, so the dense
        // structure is materialized here rather than retained (issue #3552 review
        // B4). It costs `rows × size_of::<Option<&Value>>()` once per BATCH —
        // `transpose_columns` allocated that PER COLUMN, so this term is smaller
        // than `main`'s by a factor of the projection width.
        let mut view: Vec<Option<&Value>> = Vec::with_capacity(self.rows);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        for (idx, col) in self.columns.iter().enumerate() {
            // Read through `canonical`: a duplicate-name column builds its array
            // from the SAME stored cells as the first column of that name, so the
            // value is BORROWED a second time rather than stored a second time —
            // byte-identical output, one copy in memory (issue #3552 review B3).
            let store = match self.canonical.get(idx).and_then(|&c| self.cells.get(c)) {
                Some(store) => store,
                None => {
                    return Err(ArrowConvertError::InvalidValue(format!(
                        "column {idx} has no canonical cell store"
                    )))
                }
            };
            // Fail closed on a stored row index outside the committed range rather
            // than panic on the indexed write below. The store is sorted ascending
            // (appended as rows commit), so its LAST entry bounds every entry — an
            // O(1) check, not a scan.
            if let Some(&(last_row, _)) = store.last() {
                if last_row >= self.rows {
                    return Err(ArrowConvertError::InvalidValue(format!(
                        "column {idx} holds row index {last_row} outside the {} \
                         committed rows",
                        self.rows
                    )));
                }
            }
            // Dense fill: absent rows stay `None`, present rows are BORROWED from
            // the sparse store. O(present cells), walking in row order — no search,
            // and no reallocation after the first column.
            view.clear();
            view.resize(self.rows, None);
            for (row_idx, value) in store {
                // In range by the guard above, so this cannot panic.
                view[*row_idx] = Some(value);
            }
            arrays.push(convert_column_to_array(col, &view)?);
        }
        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

#[cfg(test)]
#[path = "arrow_row_accumulator_tests.rs"]
mod arrow_row_accumulator_tests;
