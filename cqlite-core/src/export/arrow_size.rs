//! Conservative pre-materialization Arrow payload-byte estimator (issue #2825).
//!
//! # Why this exists
//!
//! The `cqlite-flight` egress path finishes a record batch on **row count**
//! alone, so a batch's byte size is `batch_size × row_width` — an unbounded
//! function of schema shape. Bounding it needs a byte decision made **while rows
//! accumulate**, before any `RecordBatch` exists: building the batch to
//! discover it is too big defeats the purpose, so
//! `RecordBatch::get_array_memory_size()` cannot be the production trigger (it
//! is only readable *after* every value has been allocated and copied).
//!
//! [`estimate_arrow_row_bytes`] supplies that pre-batch number: given the
//! authoritative projected [`ColumnInfo`] set and one decoded [`QueryRow`], it
//! reports an **upper bound** on that row's contribution to the resulting Arrow
//! batch's *payload* bytes.
//!
//! # Currency: payload bytes, not `get_array_memory_size()`
//!
//! "Payload bytes" means the sum of Arrow buffer **lengths**, recursively
//! including child data — exactly what [`arrow_payload_bytes`] measures.
//! `get_array_memory_size()` reports buffer **capacity**, which the batch
//! construction path grows by power-of-two doubling, so it runs up to ~2× the
//! payload (measured 1.72–1.80× on realistic shapes). Payload bytes are
//! estimable from the rows in hand, monotonic in row count, and stable across
//! arrow versions and allocator policy; capacity is none of those. Consumers
//! that need a capacity figure convert with the published capacity factor
//! (`cqlite_flight::batch_bytes::BATCH_BYTES_CAPACITY_FACTOR`).
//!
//! # The charging model, from first principles
//!
//! Arrow buffer **lengths are exact** — measured against this tree's arrow 53, a
//! one-row `Int32` column is 4 bytes, a nine-row `Boolean` column is
//! `ceil(9/8) = 2` bytes, and a null buffer is `ceil(n/8)` (absent entirely when
//! a column has no nulls). Nothing is rounded up to an allocator quantum, so the
//! estimate is built from the three real per-slot costs and one small per-column
//! residual — **never** from a large fudge charged per cell:
//!
//! | term | charged | covers |
//! |---|---|---|
//! | `ARROW_VALIDITY_BYTES` | every slot | the slot's validity **bit**; `n` bytes ≥ `ceil(n/8)` |
//! | content | every slot | the slot's data-buffer bytes |
//! | `ARROW_CELL_OVERHEAD_BYTES` | variable-width slots | its offsets entry + the buffer's trailing `n+1`-th entry |
//! | `ARROW_COLUMN_SLACK_BYTES` | once per **column whose builder materializes childless array nodes** | those nodes' empty offsets buffers |
//!
//! The per-cell terms are therefore tight (a 1000-element `list<int>` estimates
//! ~1.2× its realized payload, not ~9×), and the residual is charged ONLY where
//! a childless node can exist — the flat `list`/`set`/`map` builders, whose
//! `MapArray`/`ListArray` always materialize a rendered `Utf8` child (see
//! [`column_slack_bytes`]). A fixed-width column materializes exactly one array
//! node with one data buffer, no offsets and no children, so it pays no residual
//! at all: an `int` column costs `1 + 4 = 5` B/row against ~4.1 B/row realized.
//! A 100-column `int` row therefore estimates 500 B and a full 8192-row batch
//! 4,096,000 B — still inside the 4 MiB default, so the ROW-cap keeps binding on
//! wide narrow-cell schemas up to 102 `int` columns (issue #2825 review C1;
//! charging the residual per column put that cliff at 40 columns).
//!
//! # Conservatism is a contract, not an aspiration
//!
//! `Σ estimate_arrow_row_bytes(columns, row) >= arrow_payload_bytes(batch)` for
//! every shape, enforced by the property test in `arrow_size_tests.rs` over a
//! corpus covering fixed-width columns, `text`, `blob`, `list`/`set`, `map`,
//! `tuple`/UDT, JSON, deeply nested empty collections, all-null rows, empty
//! strings and empty collections.
//!
//! **The production consumer of that contract is the FUSED accounting, not this
//! function (issue #3552).** `cqlite-flight`'s two `do_get` row routes take each
//! row's width from [`super::ArrowRowAccumulator::stage`], which charges it from
//! the cells it resolved for the Arrow build pass instead of re-resolving them
//! here; `estimate_arrow_row_bytes` remains the aggregate route's estimator, this
//! module's tested surface, and the ORACLE the fused width is pinned against.
//! Both charge through the private `charge_row` core and differ ONLY in cell
//! resolution, and their per-row equality over the SHARED shape corpus — absent
//! columns, duplicate output columns and the saturating fan-out included — is
//! asserted by `arrow_row_accumulator`'s
//! `fused_width_equals_the_standalone_estimate_over_the_shape_corpus`. That test
//! is what transfers the conservatism contract above to the fused path: weaken it
//! and the contract stops covering production. Read the `# Cross-issue
//! dependency` section below with that substitution in mind. The three pre-existing per-`Value` estimators
//! are all *under*-estimators for this purpose (see the issue-#2825 design §d):
//! `Value::size_estimate` models the SERIALIZED size (a 1-byte vint prefix where
//! Arrow spends a 4-byte offset plus a validity bit), and
//! `memory::estimate_value_size` / `Memtable::estimate_value_size` are
//! content-only. None is Arrow-aware, and an under-estimator cannot found a
//! memory bound.
//!
//! # Cross-issue dependency: a per-stream MEMORY BOUND rests on that contract
//!
//! `cqlite-flight`'s per-stream in-flight egress ceiling (issue #2821,
//! `cqlite-flight/src/egress_credit.rs`) reserves credit for a batch BEFORE it is
//! materialized, converting THIS estimate into Arrow capacity bytes with
//! `worst_case_batch_capacity_bytes`. The reservation is a true upper bound on
//! the realized `get_array_memory_size()` ONLY while the conservatism above
//! holds; weaken it and that published memory bound is silently voided (the
//! governor then fails closed with a terminal internal error rather than
//! exceeding its pool, but the stream breaks). Any change here that could make
//! the estimate non-conservative must be made together with that consumer.
//!
//! # No-heuristics (issue #28)
//!
//! Width is derived from the authoritative `ColumnInfo` CQL/flat types plus the
//! already-decoded [`Value`]s. Nothing is inferred from byte patterns and no
//! decode decision is influenced.
//!
//! # Hardening
//!
//! The walk is **iterative** (an explicit worklist) and every arithmetic step is
//! saturating. Two budgets, both reset **per column**, bound the work:
//!
//! * [`MAX_ESTIMATE_NODES`] caps the **branching** slots — those that can queue
//!   further slots. Only a branching slot ever enters the worklist, so the
//!   worklist can never hold more entries than the budget (a stronger form of
//!   the pre-push fan-out check it replaces), and a value nested deeper than the
//!   budget fails closed.
//! * [`MAX_ESTIMATE_LEAF_SLOTS`] caps the leaf slots charged inline. A leaf costs
//!   no worklist entry and no structural node, so a collection's ELEMENT COUNT —
//!   the one dimension a legal Cassandra row pushes to 65,535 — no longer
//!   consumes the structural budget (issue #2825 review C2): a 65,535-entry
//!   `map<text,text>` spends ONE node rather than 131,070, and is estimated
//!   exactly instead of failing closed and degrading the stream to one row per
//!   batch for the rest of the scan.
//!
//! Per-column budgets mean one wide column can no longer starve the columns
//! after it. A row that exhausts either budget, or whose widths would overflow
//! `usize`, **fails closed** to `usize::MAX` — which trips the byte-cap and cuts
//! the batch, the safe direction. It never panics and never hangs.

use crate::query::{ColumnInfo, QueryRow};
use crate::schema::CqlType;
use crate::types::{DataType, Value};

// Column shape resolution (`column_shape`, `column_slack_bytes`, `branches`)
// and the rendered-representation bounds (`charge_rendered`,
// `json_render_bytes`) live in child modules so this file stays under the
// campsite threshold (epic #1116). A child module sees this module's private
// items, and this module re-exposes theirs to each other.
#[path = "arrow_size_shape.rs"]
mod shape;

#[path = "arrow_size_render.rs"]
mod render;

// Per-ROW charging over a resolved column set: the shared `charge_row` loop and
// the fused accounting's `PreparedColumns` cache (issue #3552). Its own file for
// the same reason as its two siblings — and re-exported below under its OWN
// visibility, so `arrow_row_accumulator`'s import path is unchanged.
#[path = "arrow_size_prepared.rs"]
mod prepared;

use prepared::charge_row;
use render::RENDER_CONTAINER_BYTES;

// The estimator ⇄ accumulator seam, re-exported at its unchanged
// `pub(in crate::export)` visibility (issue #3552 review N5).
pub(in crate::export) use prepared::PreparedColumns;
use shape::{
    branches, column_shape, column_slack_bytes, unwrap_frozen_type, unwrap_frozen_value, Shape,
    TextFidelity,
};

// ============================================================================
// Structural constants
// ============================================================================
//
// Deliberately NOT part of the crate's public surface (issue #2825 review N4):
// these are tuning parameters of the estimate, not a contract. The public
// surface is `estimate_arrow_row_bytes` / `arrow_payload_bytes` /
// `MAX_ESTIMATE_NODES`.

/// Width of one Arrow 32-bit offset entry (`Utf8`/`Binary`/`List`/`Map`).
const ARROW_OFFSET_BYTES: usize = 4;

/// Validity charged per Arrow slot. Arrow spends one **bit** per slot, so a
/// buffer over `n` slots is `ceil(n / 8)` bytes long; charging one whole byte
/// per slot covers that for every `n` with room to spare.
const ARROW_VALIDITY_BYTES: usize = 1;

/// Per-cell structural overhead of a **variable-width** Arrow slot, charged on
/// top of the universal per-slot validity byte.
///
/// Derivation (not a fitted constant): an offsets buffer over `n` slots is
/// `(n + 1) * 4` bytes — one entry per slot plus a trailing entry. Charging
/// **two** entries per slot covers the trailing entry however few slots the
/// buffer has, with `n = 1` the tight case: realized
/// `4 * (1 + 1) + ceil(1/8) = 9`, charged `4 + 4 + 1 = 9`. The extra validity
/// byte here (the second one a variable-width slot pays) is deliberate margin.
const ARROW_CELL_OVERHEAD_BYTES: usize = 2 * ARROW_OFFSET_BYTES + ARROW_VALIDITY_BYTES;

/// Residual slack charged ONCE per row for a column that materializes Arrow
/// array nodes corresponding to no value slot — never per cell, per element or
/// per field, and **never for a column that has no such node** (see
/// [`column_slack_bytes`]).
///
/// Derivation: each childless node still carries an empty 4-byte offsets buffer.
/// The tight case is the flat `DataType::Map` builder, whose `MapArray` always
/// materializes a key `Utf8` and a value `Utf8` child even for a cell with zero
/// entries: `2 × 4 = 8` bytes with no slot to attach them to. (The high-fidelity
/// path charges such nodes explicitly — see `charge_cql`'s empty-collection
/// rule — so this stays a residual, not the mechanism.)
///
/// Charged per row because the accumulate-as-you-push cap needs a per-row
/// number and a per-batch term has nowhere to live; per COLUMN rather than per
/// SLOT so it cannot multiply by a cell's element count.
const ARROW_COLUMN_SLACK_BYTES: usize = 2 * ARROW_OFFSET_BYTES;

/// Maximum number of **branching** slots one COLUMN's estimate may queue — a
/// slot that can fan out into further slots (a collection, a map, a struct, or a
/// rendered container).
///
/// Leaf slots do not count against it (they never enter the worklist — see
/// [`MAX_ESTIMATE_LEAF_SLOTS`]), so this bounds the value's *structure*: nesting
/// depth and container-of-container fan-out, neither of which a legitimate
/// Cassandra schema drives anywhere near 65,536. A column exceeding it fails
/// closed to `usize::MAX` (cut the batch) instead of spending unbounded time,
/// and because only branching slots are queued the worklist itself can never
/// exceed this many entries.
///
/// Reset per column (issue #2825 review C2): one wide column can no longer
/// starve the columns after it.
pub const MAX_ESTIMATE_NODES: usize = 65_536;

/// Maximum number of **leaf** slots one COLUMN's estimate may charge inline.
///
/// A leaf costs no worklist entry, so this is purely a linear-work bound: it
/// exists so a `Value` tree far larger than any decoded Cassandra row still
/// terminates in bounded time. Sized ~8× above the largest legal single cell
/// (Cassandra's classic 65,535-element collection limit is 131,070 leaf slots
/// for a `map`), so the shapes review C2 called out — one near-limit collection,
/// or several thousand-element collections per row — are estimated exactly
/// rather than failing closed.
pub const MAX_ESTIMATE_LEAF_SLOTS: usize = 1 << 20;

// ============================================================================
// Public API
// ============================================================================

/// Upper bound, in Arrow **payload** bytes, on `row`'s contribution to a batch
/// built from `columns` by [`rows_to_record_batch`](super::rows_to_record_batch).
///
/// Walks `columns` (never the whole `row.values` map — an unprojected cell never
/// reaches the batch) and resolves each cell exactly as `transpose_columns`
/// does, then charges, per Arrow slot, the slot's structural overhead plus its
/// value-driven content bytes.
///
/// Returns `usize::MAX` when the row exhausts [`MAX_ESTIMATE_NODES`] or its
/// widths saturate — a fail-closed signal that cuts the batch.
///
/// # Example
///
/// ```
/// # use std::collections::HashMap;
/// # use std::sync::Arc;
/// # use cqlite_core::export::estimate_arrow_row_bytes;
/// # use cqlite_core::query::{ColumnInfo, QueryRow};
/// # use cqlite_core::types::{DataType, Value};
/// # use cqlite_core::schema::CqlType;
/// # use cqlite_core::RowKey;
/// let columns = vec![ColumnInfo {
///     name: "b".into(),
///     data_type: DataType::Blob,
///     nullable: true,
///     position: 0,
///     table_name: None,
///     cql_type: Some(CqlType::Blob),
/// }];
/// let row_with = |n: usize| {
///     let mut values: HashMap<Arc<str>, Value> = HashMap::new();
///     values.insert(Arc::from("b"), Value::Blob(vec![0u8; n].into()));
///     QueryRow::with_interned_values(RowKey::new(Vec::new()), values)
/// };
/// // Width-sensitive: the estimates differ by at least the content difference.
/// assert!(
///     estimate_arrow_row_bytes(&columns, &row_with(1024))
///         - estimate_arrow_row_bytes(&columns, &row_with(16))
///         >= 1024 - 16
/// );
/// ```
pub fn estimate_arrow_row_bytes(columns: &[ColumnInfo], row: &QueryRow) -> usize {
    // Resolution: one `values.get(name)` probe per projected column. The FUSED
    // accounting (`PreparedColumns`, issue #3552) resolves the same cells the
    // build pass's transpose does instead; both then charge through
    // `charge_row`, so only the RESOLUTION differs between them.
    charge_row(
        columns
            .iter()
            .map(|col| (column_shape(col), row.values.get(col.name.as_str()))),
    )
}

/// Sum of Arrow buffer **lengths** across `batch`, recursively including child
/// data — the cap's currency, and the oracle [`estimate_arrow_row_bytes`] must
/// never under-count.
///
/// Distinct from `RecordBatch::get_array_memory_size()`, which sums buffer
/// *capacity* (allocator growth policy) and so reports up to ~2× this value.
/// Public because the byte-cap is normatively denominated in this quantity: the
/// flight-side tests and issue #2821's per-stream ceiling both need to measure
/// it, and duplicating the walk per consumer would let it drift.
pub fn arrow_payload_bytes(batch: &arrow::record_batch::RecordBatch) -> usize {
    let mut total = 0usize;
    let mut stack: Vec<arrow::array::ArrayData> =
        batch.columns().iter().map(|c| c.to_data()).collect();
    while let Some(data) = stack.pop() {
        for buffer in data.buffers() {
            total = total.saturating_add(buffer.len());
        }
        if let Some(nulls) = data.nulls() {
            total = total.saturating_add(nulls.buffer().len());
        }
        stack.extend(data.child_data().iter().cloned());
    }
    total
}

// ============================================================================
// The iterative estimator
// ============================================================================

/// Iterative worklist over (slot shape, slot value) pairs.
///
/// One popped item is exactly one Arrow array slot; children (collection
/// elements, map entries, struct fields) are charged as further slots. Only
/// BRANCHING children are queued — a leaf child is charged where it is found —
/// so the worklist is bounded by [`MAX_ESTIMATE_NODES`] and is lazily allocated:
/// a row of scalar columns never heap-allocates.
struct Estimator<'a> {
    total: usize,
    /// Remaining branching slots for the CURRENT column.
    budget: usize,
    /// Remaining inline leaf charges for the CURRENT column.
    leaf_budget: usize,
    stack: Vec<(Shape<'a>, Option<&'a Value>)>,
}

impl<'a> Estimator<'a> {
    fn new() -> Self {
        Self {
            total: 0,
            budget: MAX_ESTIMATE_NODES,
            leaf_budget: MAX_ESTIMATE_LEAF_SLOTS,
            stack: Vec::new(),
        }
    }

    /// Start a fresh column: both budgets are per COLUMN, so one wide column's
    /// fan-out cannot starve the columns after it (review C2).
    fn begin_column(&mut self) {
        self.budget = MAX_ESTIMATE_NODES;
        self.leaf_budget = MAX_ESTIMATE_LEAF_SLOTS;
    }

    fn add(&mut self, bytes: usize) {
        self.total = self.total.saturating_add(bytes);
    }

    /// Fail closed: saturate the total and abandon the walk.
    fn saturate(&mut self) {
        self.total = usize::MAX;
        self.stack.clear();
    }

    /// Charge ONE child slot: a branching child is queued (spending one
    /// structural node), a leaf child is charged in place (spending one leaf
    /// slot and no worklist entry).
    ///
    /// Both budgets are checked BEFORE the child is queued or walked, so neither
    /// the worklist nor the walk can transiently overrun. The inline leaf charge
    /// re-enters [`Self::charge_slot`] exactly once — [`branches`] answers
    /// `false` only for shape/value pairs that queue nothing — so the call depth
    /// is 2 and never grows with the value's depth; the debug assertion pins it.
    pub(super) fn charge_child(&mut self, shape: Shape<'a>, value: Option<&'a Value>) {
        if branches(shape, value) {
            if self.budget == 0 {
                self.saturate();
                return;
            }
            self.budget -= 1;
            self.stack.push((shape, value));
            return;
        }
        if self.leaf_budget == 0 {
            self.saturate();
            return;
        }
        self.leaf_budget -= 1;
        let queued = self.stack.len();
        self.charge_slot(shape, value);
        debug_assert_eq!(
            self.stack.len(),
            queued,
            "a slot `branches` called a leaf queued children"
        );
    }

    /// Charge a whole fan-out of child slots, stopping as soon as the estimate
    /// has been saturated so a pathological cell is not walked to its end.
    pub(super) fn charge_children<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = (Shape<'a>, Option<&'a Value>)>,
    {
        for (shape, value) in items {
            if self.total == usize::MAX {
                return;
            }
            self.charge_child(shape, value);
        }
    }

    /// Charge exactly ONE Arrow array slot, charging or queueing any child slots
    /// it implies. The budgets are spent by [`Self::charge_child`], never here.
    fn charge_slot(&mut self, shape: Shape<'a>, value: Option<&'a Value>) {
        // Every slot pays its validity bit, rounded up to a whole byte.
        self.add(ARROW_VALIDITY_BYTES);
        let value = value.map(unwrap_frozen_value);
        match shape {
            Shape::Cql(t) => self.charge_cql(t, value),
            Shape::Flat(dt, fidelity) => self.charge_flat(dt, fidelity, value),
            Shape::RenderedSlot => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                self.charge_rendered(value);
            }
            Shape::RenderedInline => self.charge_rendered(value),
        }
    }

    /// Drain the worklist, charging each queued child slot.
    fn drain(&mut self) {
        while let Some((shape, value)) = self.stack.pop() {
            self.charge_slot(shape, value);
            if self.total == usize::MAX {
                return;
            }
        }
    }

    /// Charge a high-fidelity CQL-typed slot. Exhaustive over [`CqlType`] — a new
    /// variant is a compile error, never a silent under-count.
    fn charge_cql(&mut self, t: &'a CqlType, value: Option<&'a Value>) {
        let t = unwrap_frozen_type(t);
        match t {
            // Fixed-width Arrow primitives: content is the primitive's width.
            CqlType::Boolean | CqlType::TinyInt => self.add(1),
            CqlType::SmallInt => self.add(2),
            CqlType::Int | CqlType::Float | CqlType::Date => self.add(4),
            CqlType::BigInt
            | CqlType::Double
            | CqlType::Counter
            | CqlType::Timestamp
            | CqlType::Time => self.add(8),
            // FixedSizeBinary(16) / Decimal128.
            CqlType::Uuid | CqlType::TimeUuid | CqlType::Decimal | CqlType::Varint => self.add(16),
            // Utf8 / Binary carrying the value's own bytes.
            CqlType::Text | CqlType::Ascii | CqlType::Varchar => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                self.add(text_content_bytes(value));
            }
            CqlType::Blob => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                self.add(binary_content_bytes(value));
            }
            // Utf8 carrying a formatted rendering of a bounded scalar.
            CqlType::Inet => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                self.add(render::RENDER_INET_BYTES);
            }
            CqlType::Duration => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                self.add(render::RENDER_DURATION_BYTES);
            }
            // Opaque custom type: `build_string_array`'s permissive render into
            // this one Utf8 slot.
            CqlType::Custom(_) => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                self.charge_rendered(value);
            }
            // ListArray: the row's own offsets entry, plus one typed child slot
            // per element.
            // #4114: a vector charges exactly as a list — its value is `Value::List`
            // and its Arrow node is the same ListArray.
            CqlType::List(inner) | CqlType::Set(inner) | CqlType::Vector(inner, _) => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                match value {
                    Some(Value::List(items) | Value::Set(items)) if !items.is_empty() => {
                        self.charge_children(items.iter().map(|v| (Shape::Cql(inner), Some(v))));
                    }
                    // Empty, null or absent: `build_typed_value_array` still
                    // materializes one child array per DECLARED nesting level,
                    // each carrying an empty 4-byte offsets buffer. Charging the
                    // declared chain covers a `list<list<…>>` whose depth no
                    // per-slot constant could bound (review B2).
                    Some(Value::List(_) | Value::Set(_) | Value::Null) | None => {
                        self.charge_child(Shape::Cql(inner), None);
                    }
                    // Shape mismatch: the converter either errors (no batch) or
                    // renders. Charge the render bound, never zero.
                    Some(other) => self.charge_child(Shape::RenderedSlot, Some(other)),
                }
            }
            // MapArray: the row's offsets entry, plus a key and a value child
            // slot per entry (the entries struct itself carries no buffers).
            CqlType::Map(key_type, val_type) => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                match value {
                    Some(Value::Map(pairs)) if !pairs.is_empty() => {
                        self.charge_children(pairs.iter().flat_map(|(k, v)| {
                            [
                                (Shape::Cql(key_type), Some(k)),
                                (Shape::Cql(val_type), Some(v)),
                            ]
                        }));
                    }
                    // Empty/null/absent: both child arrays still exist — charge
                    // the declared key and value type chains (review B2).
                    Some(Value::Map(_) | Value::Null) | None => {
                        self.charge_child(Shape::Cql(key_type), None);
                        self.charge_child(Shape::Cql(val_type), None);
                    }
                    Some(other) => self.charge_child(Shape::RenderedSlot, Some(other)),
                }
            }
            // StructArray: no offsets; every DECLARED field materializes a child
            // slot for this row whether or not the value carries it. The extra
            // variable-width overhead covers the Utf8 fallback the converter
            // takes when the declared field list is empty.
            CqlType::Tuple(element_types) => {
                self.add(ARROW_CELL_OVERHEAD_BYTES.saturating_add(RENDER_CONTAINER_BYTES));
                let items: &[Value] = match value {
                    Some(Value::Tuple(items) | Value::List(items)) => items,
                    _ => &[],
                };
                let n = element_types.len().max(items.len());
                self.charge_children((0..n).map(|i| {
                    let shape = element_types.get(i).map_or(Shape::RenderedSlot, Shape::Cql);
                    (shape, items.get(i))
                }));
            }
            CqlType::Udt(_, udt_fields) => {
                self.add(ARROW_CELL_OVERHEAD_BYTES.saturating_add(RENDER_CONTAINER_BYTES));
                let udt = match value {
                    Some(Value::Udt(udt)) => Some(udt.as_ref()),
                    _ => None,
                };
                self.charge_children(udt_fields.iter().map(|(name, field_type)| {
                    let field_value = udt.and_then(|u| {
                        u.fields
                            .iter()
                            .find(|f| &f.name == name)
                            .and_then(|f| f.value.as_ref())
                    });
                    (Shape::Cql(field_type), field_value)
                }));
                // A value carrying fields the declared type does not name still
                // costs something on the render fallback (`format_udt` emits
                // `{name: value, …}`), so charge the NAME and its separators
                // too — matching `charge_rendered`'s UDT arm (review B4).
                if let Some(u) = udt {
                    let extra = u
                        .fields
                        .iter()
                        .filter(|f| !udt_fields.iter().any(|(n, _)| n == &f.name));
                    for f in extra {
                        if self.total == usize::MAX {
                            return;
                        }
                        self.add(f.name.len().saturating_add(RENDER_CONTAINER_BYTES));
                        self.charge_child(Shape::RenderedInline, f.value.as_ref());
                    }
                }
            }
            // Unreachable after `unwrap_frozen_type`, but kept explicit so the
            // match stays exhaustive without a `_` arm.
            CqlType::Frozen(inner) => self.charge_child(Shape::Cql(inner), value),
        }
    }

    /// Charge a flat-`DataType` slot (the legacy builders). Exhaustive over
    /// [`DataType`] — a new variant is a compile error.
    ///
    /// The empty-collection child arrays these builders always materialize
    /// (`ListArray<Utf8>` / `MapArray<Utf8, Utf8>`) are covered by
    /// `ARROW_COLUMN_SLACK_BYTES`, which is derived from exactly that case —
    /// unlike the high-fidelity path, the flat builders have a FIXED one-level
    /// child shape, so a constant suffices.
    fn charge_flat(&mut self, dt: &DataType, fidelity: TextFidelity, value: Option<&'a Value>) {
        // `build_string_array`'s two branches, which both reach this function
        // through the SAME flat `data_type` arms — so the branch is selected
        // here rather than by the shape (review B5).
        let charge_string = |est: &mut Self, value: Option<&'a Value>| match (fidelity, value) {
            // BOTH branches borrow a `Value::Text`'s own bytes after a NON-lossy
            // `str::from_utf8` and hard-error on invalid UTF-8, so a top-level
            // text cell is exactly `s.len()` — no U+FFFD expansion. (The lossy
            // `format_value` path applies only to a `Value::Text` NESTED in a
            // rendered container; `charge_rendered` charges 3x there.)
            (_, Some(Value::Text(s))) => est.add(s.len()),
            // Strict: every other variant makes the builder hard-error, so no
            // batch exists to charge for.
            (TextFidelity::Strict, _) => {}
            // Lossy: `ValueFormatter::format_value` renders it.
            (TextFidelity::Lossy, v) => est.charge_rendered(v),
        };
        match dt {
            DataType::Boolean | DataType::TinyInt => self.add(1),
            DataType::SmallInt => self.add(2),
            DataType::Integer | DataType::Float32 => self.add(4),
            DataType::BigInt | DataType::Float | DataType::Timestamp => self.add(8),
            DataType::Uuid => self.add(16),
            DataType::Blob => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                self.add(binary_content_bytes(value));
            }
            // `build_string_array`: raw text when the column is authoritative
            // text, otherwise the lossy rendered form.
            DataType::Text | DataType::Json => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                charge_string(self, value);
            }
            // `build_list_array`: ListArray<Utf8> whose elements are rendered —
            // each element is a REAL child slot, not an inline sub-render.
            DataType::List | DataType::Set => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                if let Some(Value::List(items) | Value::Set(items)) = value {
                    self.charge_children(items.iter().map(|v| (Shape::RenderedSlot, Some(v))));
                }
            }
            // `build_map_array`: MapArray with rendered Utf8 keys and values.
            DataType::Map => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                if let Some(Value::Map(pairs)) = value {
                    self.charge_children(pairs.iter().flat_map(|(k, v)| {
                        [
                            (Shape::RenderedSlot, Some(k)),
                            (Shape::RenderedSlot, Some(v)),
                        ]
                    }));
                }
            }
            // Fallback to `build_string_array` — same two branches.
            DataType::Tuple
            | DataType::Udt
            | DataType::Frozen
            | DataType::Tombstone
            | DataType::Null => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                charge_string(self, value);
            }
        }
    }
}

/// Bytes a `Value::Text` contributes to a `Utf8` slot (`0` when absent/null).
///
/// Exact, not tripled: this is the STRICT typed path, where `build_string_array`
/// borrows the `&str` after a non-lossy `str::from_utf8` and hard-errors on
/// invalid UTF-8 (so no replacement-character expansion is possible). The lossy
/// rendered path charges 3× instead — see `charge_rendered`.
fn text_content_bytes(value: Option<&Value>) -> usize {
    match value {
        Some(Value::Text(s)) => s.len(),
        // A wrong-variant value makes the converter fail closed (no batch); a
        // rendered fallback is bounded by the render arms. Charge nothing extra
        // here — the slot overhead is already charged.
        _ => 0,
    }
}

/// Bytes a `Value::Blob` contributes to a `Binary` slot (`0` when absent/null).
fn binary_content_bytes(value: Option<&Value>) -> usize {
    match value {
        Some(Value::Blob(b)) => b.len(),
        _ => 0,
    }
}

#[cfg(test)]
#[path = "arrow_size_tests.rs"]
mod arrow_size_tests;
