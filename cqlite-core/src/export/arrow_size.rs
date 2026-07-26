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
//! # Conservatism is a contract, not an aspiration
//!
//! `Σ estimate_arrow_row_bytes(columns, row) >= arrow_payload_bytes(batch)` for
//! every shape, enforced by the property test in `arrow_size_tests.rs` over a
//! corpus covering fixed-width columns, `text`, `blob`, `list`/`set`, `map`,
//! `tuple`/UDT, all-null rows, empty strings and empty collections. The three
//! pre-existing per-`Value` estimators are all *under*-estimators for this
//! purpose (see the issue-#2825 design §d): `Value::size_estimate` models the
//! SERIALIZED size (a 1-byte vint prefix where Arrow spends a 4-byte offset plus
//! a validity bit), and `memory::estimate_value_size` /
//! `Memtable::estimate_value_size` are content-only. None is Arrow-aware, and an
//! under-estimator cannot found a memory bound.
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
//! [`MAX_ESTIMATE_NODES`], and every arithmetic step is saturating. A value
//! deeper or wider than the node budget, or one whose widths would overflow
//! `usize`, **fails closed** to `usize::MAX` — which trips the byte-cap and cuts
//! the batch, the safe direction. It never panics and never hangs.

use crate::query::{ColumnInfo, QueryRow};
use crate::schema::CqlType;
use crate::types::{DataType, Value};

// ============================================================================
// Published structural constants
// ============================================================================

/// Width of one Arrow 32-bit offset entry (`Utf8`/`Binary`/`List`/`Map`).
pub const ARROW_OFFSET_BYTES: usize = 4;

/// Validity charged per Arrow slot. Arrow spends one **bit**; this rounds up to
/// a whole byte so a short batch — whose validity buffer length is dominated by
/// allocator rounding rather than by `ceil(n/8)` — is still covered.
pub const ARROW_VALIDITY_BYTES: usize = 1;

/// Per-cell structural overhead of a **variable-width** Arrow slot: its offsets
/// entry plus its validity byte.
///
/// This is the term every pre-existing estimator omits, and the reason a
/// content-only sum is an under-count. A trailing offsets entry (`n + 1` offsets
/// for `n` slots) and small-buffer allocator rounding are absorbed by
/// [`ARROW_SLOT_SLACK_BYTES`].
pub const ARROW_CELL_OVERHEAD_BYTES: usize = ARROW_OFFSET_BYTES + ARROW_VALIDITY_BYTES;

/// Fixed slack charged on **every** Arrow slot (fixed-width or variable-width).
///
/// Covers the per-buffer costs that are NOT proportional to the slot count and
/// so cannot be amortized by a strictly per-row estimator: chiefly the trailing
/// `n + 1`-th offsets entry of each variable-width buffer, which a one-row batch
/// pays in full. Measured against arrow 53 the tightest corpus shape (a one-row
/// 64 KiB blob) needs only 2 bytes here; 32 leaves an order of magnitude of
/// headroom while costing a narrow 3-column row ~100 B — three orders below the
/// 4 MiB default cap, so the row-cap still binds on every narrow shape.
///
/// Charged per slot rather than per batch deliberately: the accumulate-as-you-
/// push cap needs a per-row number, and a per-batch term has nowhere to live.
pub const ARROW_SLOT_SLACK_BYTES: usize = 32;

/// Maximum number of value/type nodes one row's estimate may visit.
///
/// A row whose values nest or fan out past this budget fails closed to
/// `usize::MAX` (cut the batch) instead of spending unbounded time. Sized well
/// above any legitimate Cassandra row shape (a 4-column row of 1000-element
/// collections visits ~4000 nodes).
pub const MAX_ESTIMATE_NODES: usize = 65_536;

// ---- Rendered-representation bounds -----------------------------------------
//
// Columns with no authoritative CQL type (and the `Tuple`/`Udt`/`Frozen`/
// `Tombstone`/`Null` flat arms) are converted by `build_string_array`, which
// renders the value through `ValueFormatter::format_value`. These bound that
// rendering; each is an upper bound on the corresponding `format_value` arm.

/// `"true"` / `"false"`.
const RENDER_BOOL_BYTES: usize = 8;
/// Any integral variant rendered as decimal (`i64::MIN` is 20 chars).
const RENDER_INT_BYTES: usize = 24;
/// `f32`/`f64` via `{}`/`{:e}`, plus `NaN`/`Infinity`.
const RENDER_FLOAT_BYTES: usize = 40;
/// `"a8f167f0-ebe7-4f20-a386-31ff138bec3b"`.
const RENDER_UUID_BYTES: usize = 40;
/// `YYYY-MM-DD HH:MM:SS.fff+0000` or `<invalid-timestamp:-9223372036854775808>`.
const RENDER_TIMESTAMP_BYTES: usize = 48;
/// `YYYY-MM-DD` or the `<invalid-date:…>` fallback.
const RENDER_DATE_BYTES: usize = 48;
/// `HH:MM:SS.nnnnnnnnn` or the `<invalid-time:…>` fallback.
const RENDER_TIME_BYTES: usize = 48;
/// Full IPv6 text form, or the invalid-length fallback.
const RENDER_INET_BYTES: usize = 64;
/// `"{months}mo{days}d{nanos}ns"` at the widest.
const RENDER_DURATION_BYTES: usize = 64;
/// `"<deleted@{i64}>"`.
const RENDER_TOMBSTONE_BYTES: usize = 40;
/// `"null"`.
const RENDER_NULL_BYTES: usize = 8;
/// Brackets plus the `", "` separators a rendered container adds around its
/// (separately charged) children.
const RENDER_CONTAINER_BYTES: usize = 8;
/// Decimal digits produced per magnitude byte (`log10(256) < 2.41`), rounded up.
const DECIMAL_DIGITS_PER_BYTE: usize = 3;
/// `ValueFormatter::format_decimal`'s zero-padding ceiling: past this the render
/// switches to bounded exponent form, so padding can never exceed it.
const DECIMAL_SCALE_RENDER_CAP: usize = 1_000_001;

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
        est.push(column_shape(col), cell);
        est.run();
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
    /// `build_string_array`'s permissive arm: the value is rendered through
    /// `ValueFormatter::format_value` into a `Utf8` slot.
    Rendered,
}

/// Resolve a column to its Arrow slot shape, mirroring `convert_column_to_array`
/// exactly: only the high-fidelity CQL arms take the typed path; everything else
/// (including `Text`/`Blob`/numeric CQL types) falls through to the flat
/// `data_type` dispatch.
fn column_shape(col: &ColumnInfo) -> Shape<'_> {
    if let Some(cql) = &col.cql_type {
        let effective = unwrap_frozen_type(cql);
        match effective {
            CqlType::Date
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
            | CqlType::Text
            | CqlType::Ascii
            | CqlType::Varchar
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

    /// Drain the worklist, charging each slot.
    fn run(&mut self) {
        while let Some((shape, value)) = self.stack.pop() {
            if self.budget == 0 {
                self.saturate();
                return;
            }
            self.budget -= 1;
            // Every slot pays its validity bit (rounded to a byte) plus the
            // fixed per-slot slack that absorbs the trailing offsets entry and
            // arrow's short-buffer length rounding.
            self.add(ARROW_VALIDITY_BYTES.saturating_add(ARROW_SLOT_SLACK_BYTES));
            let value = value.map(unwrap_frozen_value);
            match shape {
                Shape::Cql(t) => self.charge_cql(t, value),
                Shape::Flat(dt) => self.charge_flat(dt, value),
                Shape::Rendered => self.charge_rendered(value),
            }
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
                self.add(RENDER_INET_BYTES);
            }
            CqlType::Duration => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                self.add(RENDER_DURATION_BYTES);
            }
            // Opaque custom type: `build_string_array`'s permissive render.
            CqlType::Custom(_) => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                self.charge_rendered(value);
            }
            // ListArray: the row's own offsets entry, plus one typed child slot
            // per element.
            CqlType::List(inner) | CqlType::Set(inner) => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                if let Some(Value::List(items) | Value::Set(items)) = value {
                    self.push_all(items.iter().map(|v| (Shape::Cql(inner), Some(v))));
                } else if let Some(other) = value {
                    // Shape mismatch: the converter either errors (no batch) or
                    // renders. Charge the render bound, never zero.
                    self.push(Shape::Rendered, Some(other));
                }
            }
            // MapArray: the row's offsets entry, plus a key and a value child
            // slot per entry (the entries struct's own slot is the key slot's
            // slack, which is charged per pop).
            CqlType::Map(key_type, val_type) => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                if let Some(Value::Map(pairs)) = value {
                    for (k, v) in pairs {
                        self.push(Shape::Cql(key_type), Some(k));
                        self.push(Shape::Cql(val_type), Some(v));
                    }
                    self.check_budget(pairs.len().saturating_mul(2));
                } else if let Some(other) = value {
                    self.push(Shape::Rendered, Some(other));
                }
            }
            // StructArray: no offsets; every DECLARED field materializes a child
            // slot for this row whether or not the value carries it.
            CqlType::Tuple(element_types) => {
                let items: &[Value] = match value {
                    Some(Value::Tuple(items) | Value::List(items)) => items,
                    _ => &[],
                };
                let n = element_types.len().max(items.len());
                for i in 0..n {
                    let shape = element_types.get(i).map_or(Shape::Rendered, Shape::Cql);
                    self.push(shape, items.get(i));
                }
                self.check_budget(n);
            }
            CqlType::Udt(_, udt_fields) => {
                let udt = match value {
                    Some(Value::Udt(udt)) => Some(udt.as_ref()),
                    _ => None,
                };
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
                // costs something on any render fallback — charge them too.
                if let Some(u) = udt {
                    let extra = u
                        .fields
                        .iter()
                        .filter(|f| !udt_fields.iter().any(|(n, _)| n == &f.name));
                    for f in extra {
                        self.push(Shape::Rendered, f.value.as_ref());
                    }
                    self.check_budget(udt_fields.len().saturating_add(u.fields.len()));
                } else {
                    self.check_budget(udt_fields.len());
                }
            }
            // Unreachable after `unwrap_frozen_type`, but kept explicit so the
            // match stays exhaustive without a `_` arm.
            CqlType::Frozen(inner) => self.push(Shape::Cql(inner), value),
        }
    }

    /// Charge a flat-`DataType` slot (the legacy builders). Exhaustive over
    /// [`DataType`] — a new variant is a compile error.
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
            // `build_list_array`: ListArray<Utf8> whose elements are rendered.
            DataType::List | DataType::Set => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                if let Some(Value::List(items) | Value::Set(items)) = value {
                    self.push_all(items.iter().map(|v| (Shape::Rendered, Some(v))));
                }
            }
            // `build_map_array`: MapArray with rendered Utf8 keys and values.
            DataType::Map => {
                self.add(ARROW_CELL_OVERHEAD_BYTES);
                if let Some(Value::Map(pairs)) = value {
                    for (k, v) in pairs {
                        self.push(Shape::Rendered, Some(k));
                        self.push(Shape::Rendered, Some(v));
                    }
                    self.check_budget(pairs.len().saturating_mul(2));
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

    /// Charge the `Utf8` rendering of `value` via `ValueFormatter::format_value`.
    ///
    /// Leaf variants get a constant bound; container variants charge their
    /// bracket/separator overhead here and push their children as further
    /// rendered slots (each child's own per-slot slack makes this strictly
    /// conservative versus the single joined string arrow actually stores).
    fn charge_rendered(&mut self, value: Option<&'a Value>) {
        let Some(value) = value else {
            return;
        };
        match unwrap_frozen_value(value) {
            Value::Null => self.add(RENDER_NULL_BYTES),
            Value::Boolean(_) => self.add(RENDER_BOOL_BYTES),
            Value::TinyInt(_)
            | Value::SmallInt(_)
            | Value::Integer(_)
            | Value::BigInt(_)
            | Value::Counter(_) => self.add(RENDER_INT_BYTES),
            Value::Float(_) | Value::Float32(_) => self.add(RENDER_FLOAT_BYTES),
            Value::Text(s) => self.add(s.len()),
            // `0x`-prefixed lowercase hex: two chars per byte.
            Value::Blob(b) => self.add(
                b.len()
                    .saturating_mul(2)
                    .saturating_add(RENDER_CONTAINER_BYTES),
            ),
            Value::Timestamp(_) => self.add(RENDER_TIMESTAMP_BYTES),
            Value::Date(_) => self.add(RENDER_DATE_BYTES),
            Value::Time(_) => self.add(RENDER_TIME_BYTES),
            Value::Uuid(_) => self.add(RENDER_UUID_BYTES),
            Value::Varint(b) => self.add(
                b.len()
                    .saturating_mul(DECIMAL_DIGITS_PER_BYTE)
                    .saturating_add(RENDER_CONTAINER_BYTES),
            ),
            // Digits, plus `format_decimal`'s bounded zero padding (past
            // `DECIMAL_SCALE_RENDER_CAP` it switches to exponent form).
            Value::Decimal { scale, unscaled } => self.add(
                unscaled
                    .len()
                    .saturating_mul(DECIMAL_DIGITS_PER_BYTE)
                    .saturating_add((scale.unsigned_abs() as usize).min(DECIMAL_SCALE_RENDER_CAP))
                    .saturating_add(RENDER_CONTAINER_BYTES),
            ),
            Value::Duration { .. } => self.add(RENDER_DURATION_BYTES),
            Value::Inet(_) => self.add(RENDER_INET_BYTES),
            Value::Tombstone(_) => self.add(RENDER_TOMBSTONE_BYTES),
            Value::Json(json) => {
                let bytes = json_render_bytes(json, &mut self.budget);
                self.add(bytes);
            }
            Value::List(items) | Value::Set(items) | Value::Tuple(items) => {
                self.add(RENDER_CONTAINER_BYTES);
                self.push_all(items.iter().map(|v| (Shape::Rendered, Some(v))));
            }
            Value::Map(pairs) => {
                self.add(RENDER_CONTAINER_BYTES);
                for (k, v) in pairs {
                    self.push(Shape::Rendered, Some(k));
                    self.push(Shape::Rendered, Some(v));
                }
                self.check_budget(pairs.len().saturating_mul(2));
            }
            Value::Udt(udt) => {
                self.add(RENDER_CONTAINER_BYTES);
                for f in &udt.fields {
                    self.add(f.name.len().saturating_add(RENDER_CONTAINER_BYTES));
                    self.push(Shape::Rendered, f.value.as_ref());
                }
                self.check_budget(udt.fields.len());
            }
            // Unreachable after `unwrap_frozen_value`; kept so the match is
            // exhaustive without a `_` arm.
            Value::Frozen(_) => self.add(RENDER_CONTAINER_BYTES),
        }
    }

    fn push_all<I>(&mut self, items: I)
    where
        I: Iterator<Item = (Shape<'a>, Option<&'a Value>)>,
    {
        let mut pushed = 0usize;
        for item in items {
            self.stack.push(item);
            pushed = pushed.saturating_add(1);
        }
        self.check_budget(pushed);
    }

    /// Fail closed when a single fan-out already exceeds the remaining node
    /// budget, so a pathologically wide collection saturates immediately rather
    /// than after draining the worklist.
    fn check_budget(&mut self, pushed: usize) {
        if pushed > self.budget {
            self.saturate();
        }
    }
}

/// Bytes a `Value::Text` contributes to a `Utf8` slot (`0` when absent/null).
fn text_content_bytes(value: Option<&Value>) -> usize {
    match value {
        Some(Value::Text(s)) => s.len(),
        // A wrong-variant value makes the converter fail closed (no batch); a
        // rendered fallback is bounded by the render arms. Charge nothing extra
        // here — the slot overhead and slack are already charged.
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

/// Bounded upper bound on `serde_json::Value::to_string().len()`.
///
/// Iterative with its own share of the caller's node budget, so a deeply nested
/// JSON document cannot recurse or spin. Returns `usize::MAX` when the budget is
/// exhausted (fail closed).
fn json_render_bytes(root: &serde_json::Value, budget: &mut usize) -> usize {
    let mut total = 0usize;
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        if *budget == 0 {
            return usize::MAX;
        }
        *budget -= 1;
        match node {
            serde_json::Value::Null => total = total.saturating_add(RENDER_NULL_BYTES),
            serde_json::Value::Bool(_) => total = total.saturating_add(RENDER_BOOL_BYTES),
            serde_json::Value::Number(_) => total = total.saturating_add(RENDER_FLOAT_BYTES),
            // JSON string escaping can expand a byte to `\u00XX` (6 chars).
            serde_json::Value::String(s) => {
                total = total.saturating_add(
                    s.len()
                        .saturating_mul(6)
                        .saturating_add(RENDER_CONTAINER_BYTES),
                )
            }
            serde_json::Value::Array(items) => {
                total = total.saturating_add(
                    RENDER_CONTAINER_BYTES.saturating_add(items.len().saturating_mul(2)),
                );
                if items.len() > *budget {
                    return usize::MAX;
                }
                stack.extend(items.iter());
            }
            serde_json::Value::Object(map) => {
                total = total.saturating_add(RENDER_CONTAINER_BYTES);
                if map.len() > *budget {
                    return usize::MAX;
                }
                for (k, v) in map {
                    total = total.saturating_add(
                        k.len()
                            .saturating_mul(6)
                            .saturating_add(RENDER_CONTAINER_BYTES),
                    );
                    stack.push(v);
                }
            }
        }
    }
    total
}

#[cfg(test)]
#[path = "arrow_size_tests.rs"]
mod arrow_size_tests;
