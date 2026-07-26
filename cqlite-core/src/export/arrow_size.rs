//! Conservative pre-materialization Arrow payload-byte estimator (issue #2825).
//!
//! # Why this exists
//!
//! The `cqlite-flight` egress path finishes a record batch on **row count**
//! alone, so a batch's byte size is `batch_size × row_width` — an unbounded
//! function of schema shape. Bounding it needs a byte decision made **while rows
//! accumulate**, before any [`RecordBatch`] exists: building the batch to
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
//! | [`ARROW_VALIDITY_BYTES`] | every slot | the slot's validity **bit**; `n` bytes ≥ `ceil(n/8)` |
//! | content | every slot | the slot's data-buffer bytes |
//! | [`ARROW_CELL_OVERHEAD_BYTES`] | variable-width slots | its offsets entry + the buffer's trailing `n+1`-th entry |
//! | [`ARROW_COLUMN_SLACK_BYTES`] | once per projected **column** | array nodes that correspond to no slot at all |
//!
//! The per-cell terms are therefore tight (a 1000-element `list<int>` estimates
//! ~1.2× its realized payload, not ~9×), and a wide fixed-width schema pays
//! ~13 B/column/row rather than ~1 KB/row — so the row-cap, not the byte-cap,
//! still binds on narrow shapes.
//!
//! # Conservatism is a contract, not an aspiration
//!
//! `Σ estimate_arrow_row_bytes(columns, row) >= arrow_payload_bytes(batch)` for
//! every shape, enforced by the property test in `arrow_size_tests.rs` over a
//! corpus covering fixed-width columns, `text`, `blob`, `list`/`set`, `map`,
//! `tuple`/UDT, JSON, deeply nested empty collections, all-null rows, empty
//! strings and empty collections. The three pre-existing per-`Value` estimators
//! are all *under*-estimators for this purpose (see the issue-#2825 design §d):
//! `Value::size_estimate` models the SERIALIZED size (a 1-byte vint prefix where
//! Arrow spends a 4-byte offset plus a validity bit), and
//! `memory::estimate_value_size` / `Memtable::estimate_value_size` are
//! content-only. None is Arrow-aware, and an under-estimator cannot found a
//! memory bound.
//!
//! # No-heuristics (issue #28)
//!
//! Width is derived from the authoritative `ColumnInfo` CQL/flat types plus the
//! already-decoded [`Value`]s. Nothing is inferred from byte patterns and no
//! decode decision is influenced.
//!
//! # Hardening
//!
//! The walk is **iterative** (an explicit worklist, never recursion), bounded by
//! [`MAX_ESTIMATE_NODES`], and every arithmetic step is saturating. A fan-out is
//! tested against the remaining budget **before** it is pushed, so a corrupt
//! collection cannot transiently balloon the worklist. A value deeper or wider
//! than the node budget, or one whose widths would overflow `usize`, **fails
//! closed** to `usize::MAX` — which trips the byte-cap and cuts the batch, the
//! safe direction. It never panics and never hangs.

use crate::query::{ColumnInfo, QueryRow};
use crate::schema::CqlType;
use crate::types::{DataType, Value};

// The rendered-representation bounds (`charge_rendered`, `json_render_bytes`)
// live in a child module so this file stays under the campsite threshold
// (epic #1116). A child module sees this module's private items.
#[path = "arrow_size_render.rs"]
mod render;

use render::RENDER_CONTAINER_BYTES;

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

/// Residual slack charged ONCE per projected **column** per row — never per
/// cell, per element or per field.
///
/// Derivation: a column can materialize Arrow array nodes that correspond to no
/// value slot at all, and each such node still carries an empty 4-byte offsets
/// buffer. The tight case is the flat `DataType::Map` builder, whose `MapArray`
/// always materializes a key `Utf8` and a value `Utf8` child even for a cell
/// with zero entries: `2 × 4 = 8` bytes with no slot to attach them to. (The
/// high-fidelity path charges such nodes explicitly — see `charge_cql`'s
/// empty-collection rule — so this stays a residual, not the mechanism.)
///
/// Charged per row because the accumulate-as-you-push cap needs a per-row
/// number and a per-batch term has nowhere to live; per COLUMN rather than per
/// SLOT so it cannot multiply by a cell's element count.
const ARROW_COLUMN_SLACK_BYTES: usize = 2 * ARROW_OFFSET_BYTES;

/// Maximum number of value/type nodes one row's estimate may visit.
///
/// A row whose values nest or fan out past this budget fails closed to
/// `usize::MAX` (cut the batch) instead of spending unbounded time. Sized well
/// above any legitimate Cassandra row shape (a 4-column row of 1000-element
/// collections visits ~4000 nodes).
pub const MAX_ESTIMATE_NODES: usize = 65_536;

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
    let mut est = Estimator::new();
    for col in columns {
        let cell = row.values.get(col.name.as_str());
        // The per-column residual, charged exactly once per column per row.
        est.add(ARROW_COLUMN_SLACK_BYTES);
        // Charge the column's own slot DIRECTLY rather than through the
        // worklist: a scalar column pushes no children, so the narrow path
        // never allocates the stack at all (`Vec::new` does not allocate until
        // its first push). Only collection/struct cells reach `drain`.
        est.charge_slot(column_shape(col), cell);
        est.drain();
        if est.total == usize::MAX {
            return usize::MAX;
        }
    }
    est.total
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
// Shape resolution
// ============================================================================

/// How one Arrow slot is produced, mirroring `convert_column_to_array`'s
/// dispatch so the estimate tracks the converter rather than guessing.
#[derive(Clone, Copy)]
enum Shape<'a> {
    /// High-fidelity CQL-typed slot (`build_typed_value_array` and the typed
    /// scalar builders).
    Cql(&'a CqlType),
    /// Flat `DataType` dispatch (the legacy builders).
    Flat(&'a DataType),
    /// A REAL `Utf8` array slot whose content is `ValueFormatter::format_value`'s
    /// rendering — `build_string_array`, and each element slot of
    /// `build_list_array` / `build_map_array`. Pays the variable-width slot
    /// overhead.
    RenderedSlot,
    /// NOT an array slot of its own: a sub-part of an enclosing slot's single
    /// rendered string (a container element rendered inline). Pays content only.
    RenderedInline,
}

/// Resolve a column to its Arrow slot shape, mirroring `convert_column_to_array`
/// exactly: the high-fidelity CQL arms take the typed path; everything else
/// (numeric CQL types, `Blob`, an absent CQL type) falls through to the flat
/// `data_type` dispatch.
///
/// `Text`/`Ascii`/`Varchar` are routed to the typed arm even though the
/// converter reaches them through `build_string_array`, because that builder's
/// `strict_text` branch — taken for exactly these three CQL types — borrows the
/// `&str` after a NON-lossy `str::from_utf8` and hard-errors on invalid UTF-8.
/// Its byte behaviour is the typed one (`s.len()`), not the lossy rendered one
/// (up to `3 * s.len()`), so charging it as flat text would over-estimate every
/// authoritative text column by ~3x.
fn column_shape(col: &ColumnInfo) -> Shape<'_> {
    if let Some(cql) = &col.cql_type {
        let effective = unwrap_frozen_type(cql);
        match effective {
            CqlType::Text
            | CqlType::Ascii
            | CqlType::Varchar
            | CqlType::Date
            | CqlType::Time
            | CqlType::Decimal
            | CqlType::Varint
            | CqlType::Duration
            | CqlType::Uuid
            | CqlType::TimeUuid
            | CqlType::Inet
            | CqlType::Counter
            | CqlType::List(_)
            | CqlType::Set(_)
            | CqlType::Map(_, _)
            | CqlType::Tuple(_)
            | CqlType::Udt(_, _) => return Shape::Cql(effective),
            // Exhaustive by design (no `_` arm): a new `CqlType` variant is a
            // COMPILE error here, so it can never be silently under-estimated.
            CqlType::Boolean
            | CqlType::TinyInt
            | CqlType::SmallInt
            | CqlType::Int
            | CqlType::BigInt
            | CqlType::Float
            | CqlType::Double
            | CqlType::Blob
            | CqlType::Timestamp
            | CqlType::Frozen(_)
            | CqlType::Custom(_) => {}
        }
    }
    Shape::Flat(&col.data_type)
}

/// Unwrap nested `Frozen` wrappers to the effective type (mirrors
/// `arrow_convert::unwrap_frozen_type`). Bounded: `Frozen` nesting comes from a
/// parsed schema, and the loop is capped so a pathological type cannot spin.
fn unwrap_frozen_type(mut t: &CqlType) -> &CqlType {
    for _ in 0..MAX_FROZEN_DEPTH {
        match t {
            CqlType::Frozen(inner) => t = inner,
            _ => return t,
        }
    }
    t
}

/// Unwrap nested `Value::Frozen` wrappers (mirrors
/// `arrow_convert::unwrap_frozen_value`), bounded the same way.
fn unwrap_frozen_value(mut v: &Value) -> &Value {
    for _ in 0..MAX_FROZEN_DEPTH {
        match v {
            Value::Frozen(inner) => v = inner,
            _ => return v,
        }
    }
    v
}

/// Cap on `Frozen` unwrap iterations — a bound, not a semantic limit: real
/// schemas nest at most once or twice.
const MAX_FROZEN_DEPTH: usize = 16;

// ============================================================================
// The iterative estimator
// ============================================================================

/// Iterative worklist over (slot shape, slot value) pairs.
///
/// One popped item is exactly one Arrow array slot; children (collection
/// elements, map entries, struct fields) are pushed as further slots. The stack
/// is lazily allocated, so a row of scalar columns never heap-allocates.
struct Estimator<'a> {
    total: usize,
    budget: usize,
    stack: Vec<(Shape<'a>, Option<&'a Value>)>,
}

impl<'a> Estimator<'a> {
    fn new() -> Self {
        Self {
            total: 0,
            budget: MAX_ESTIMATE_NODES,
            stack: Vec::new(),
        }
    }

    fn push(&mut self, shape: Shape<'a>, value: Option<&'a Value>) {
        self.stack.push((shape, value));
    }

    fn add(&mut self, bytes: usize) {
        self.total = self.total.saturating_add(bytes);
    }

    /// Fail closed: saturate the total and abandon the walk.
    fn saturate(&mut self) {
        self.total = usize::MAX;
        self.stack.clear();
    }

    /// Charge exactly ONE Arrow array slot, pushing any child slots it implies.
    ///
    /// Spends one node from the budget and fails closed when it is exhausted.
    fn charge_slot(&mut self, shape: Shape<'a>, value: Option<&'a Value>) {
        if self.budget == 0 {
            self.saturate();
            return;
        }
        self.budget -= 1;
        // Every slot pays its validity bit, rounded up to a whole byte.
        self.add(ARROW_VALIDITY_BYTES);
        let value = value.map(unwrap_frozen_value);
        match shape {
            Shape::Cql(t) => self.charge_cql(t, value),
            Shape::Flat(dt) => self.charge_flat(dt, value),
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
            CqlType::List(inner) | CqlType::Set(inner) => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                match value {
                    Some(Value::List(items) | Value::Set(items)) if !items.is_empty() => {
                        self.push_all(items.iter().map(|v| (Shape::Cql(inner), Some(v))));
                    }
                    // Empty, null or absent: `build_typed_value_array` still
                    // materializes one child array per DECLARED nesting level,
                    // each carrying an empty 4-byte offsets buffer. Charging the
                    // declared chain covers a `list<list<…>>` whose depth no
                    // per-slot constant could bound (review B2).
                    Some(Value::List(_) | Value::Set(_) | Value::Null) | None => {
                        self.push(Shape::Cql(inner), None);
                    }
                    // Shape mismatch: the converter either errors (no batch) or
                    // renders. Charge the render bound, never zero.
                    Some(other) => self.push(Shape::RenderedSlot, Some(other)),
                }
            }
            // MapArray: the row's offsets entry, plus a key and a value child
            // slot per entry (the entries struct itself carries no buffers).
            CqlType::Map(key_type, val_type) => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                match value {
                    Some(Value::Map(pairs)) if !pairs.is_empty() => {
                        if !self.reserve(pairs.len().saturating_mul(2)) {
                            return;
                        }
                        for (k, v) in pairs {
                            self.push(Shape::Cql(key_type), Some(k));
                            self.push(Shape::Cql(val_type), Some(v));
                        }
                    }
                    // Empty/null/absent: both child arrays still exist — charge
                    // the declared key and value type chains (review B2).
                    Some(Value::Map(_) | Value::Null) | None => {
                        if !self.reserve(2) {
                            return;
                        }
                        self.push(Shape::Cql(key_type), None);
                        self.push(Shape::Cql(val_type), None);
                    }
                    Some(other) => self.push(Shape::RenderedSlot, Some(other)),
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
                if !self.reserve(n) {
                    return;
                }
                for i in 0..n {
                    let shape = element_types.get(i).map_or(Shape::RenderedSlot, Shape::Cql);
                    self.push(shape, items.get(i));
                }
            }
            CqlType::Udt(_, udt_fields) => {
                self.add(ARROW_CELL_OVERHEAD_BYTES.saturating_add(RENDER_CONTAINER_BYTES));
                let udt = match value {
                    Some(Value::Udt(udt)) => Some(udt.as_ref()),
                    _ => None,
                };
                let extras = udt.map_or(0, |u| u.fields.len());
                if !self.reserve(udt_fields.len().saturating_add(extras)) {
                    return;
                }
                for (name, field_type) in udt_fields {
                    let field_value = udt.and_then(|u| {
                        u.fields
                            .iter()
                            .find(|f| &f.name == name)
                            .and_then(|f| f.value.as_ref())
                    });
                    self.push(Shape::Cql(field_type), field_value);
                }
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
                        self.add(f.name.len().saturating_add(RENDER_CONTAINER_BYTES));
                        self.push(Shape::RenderedInline, f.value.as_ref());
                    }
                }
            }
            // Unreachable after `unwrap_frozen_type`, but kept explicit so the
            // match stays exhaustive without a `_` arm.
            CqlType::Frozen(inner) => self.push(Shape::Cql(inner), value),
        }
    }

    /// Charge a flat-`DataType` slot (the legacy builders). Exhaustive over
    /// [`DataType`] — a new variant is a compile error.
    ///
    /// The empty-collection child arrays these builders always materialize
    /// (`ListArray<Utf8>` / `MapArray<Utf8, Utf8>`) are covered by
    /// [`ARROW_COLUMN_SLACK_BYTES`], which is derived from exactly that case —
    /// unlike the high-fidelity path, the flat builders have a FIXED one-level
    /// child shape, so a constant suffices.
    fn charge_flat(&mut self, dt: &DataType, value: Option<&'a Value>) {
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
            // text, otherwise the rendered form. `charge_rendered` bounds both.
            DataType::Text | DataType::Json => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                self.charge_rendered(value);
            }
            // `build_list_array`: ListArray<Utf8> whose elements are rendered —
            // each element is a REAL child slot, not an inline sub-render.
            DataType::List | DataType::Set => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                if let Some(Value::List(items) | Value::Set(items)) = value {
                    self.push_all(items.iter().map(|v| (Shape::RenderedSlot, Some(v))));
                }
            }
            // `build_map_array`: MapArray with rendered Utf8 keys and values.
            DataType::Map => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                if let Some(Value::Map(pairs)) = value {
                    if !self.reserve(pairs.len().saturating_mul(2)) {
                        return;
                    }
                    for (k, v) in pairs {
                        self.push(Shape::RenderedSlot, Some(k));
                        self.push(Shape::RenderedSlot, Some(v));
                    }
                }
            }
            // Fallback to `build_string_array`'s rendered representation.
            DataType::Tuple
            | DataType::Udt
            | DataType::Frozen
            | DataType::Tombstone
            | DataType::Null => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                self.charge_rendered(value);
            }
        }
    }

    /// Queue a whole fan-out, testing the node budget **before** anything is
    /// pushed: a corrupt collection claiming 1e8 elements fails closed instead
    /// of transiently materializing 1e8 worklist entries (review N1).
    fn push_all<I>(&mut self, items: I)
    where
        I: ExactSizeIterator<Item = (Shape<'a>, Option<&'a Value>)>,
    {
        if !self.reserve(items.len()) {
            return;
        }
        for item in items {
            self.stack.push(item);
        }
    }

    /// Fail closed when a fan-out of `n` slots already exceeds the remaining
    /// node budget. Returns `false` when the estimate has been saturated and the
    /// caller must stop.
    fn reserve(&mut self, n: usize) -> bool {
        if n > self.budget {
            self.saturate();
            return false;
        }
        true
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
