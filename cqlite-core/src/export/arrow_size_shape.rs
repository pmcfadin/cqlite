//! Column shape resolution for the Arrow payload-byte estimator (issue #2825).
//!
//! Answers three questions about ONE projected column, all of them by mirroring
//! `arrow_convert::convert_column_to_array`'s dispatch rather than guessing:
//!
//! * [`column_shape`] — which builder the converter will use for it;
//! * [`column_slack_bytes`] — whether that builder materializes an Arrow array
//!   node corresponding to no value slot, and so owes the per-column residual;
//! * [`branches`] — whether charging one of its slots can queue further slots,
//!   which decides whether the slot spends the structural node budget or the
//!   leaf allowance.
//!
//! Split out of `arrow_size.rs` so both files stay under the campsite threshold
//! (epic #1116). Declared as a CHILD module of `arrow_size`, so it can see that
//! module's private constants and be seen by its sibling `render`.

use crate::query::ColumnInfo;
use crate::schema::CqlType;
use crate::types::{DataType, Value};

use super::ARROW_COLUMN_SLACK_BYTES;

/// How one Arrow slot is produced, mirroring `convert_column_to_array`'s
/// dispatch so the estimate tracks the converter rather than guessing.
#[derive(Clone, Copy)]
pub(super) enum Shape<'a> {
    /// High-fidelity CQL-typed slot (`build_typed_value_array` and the typed
    /// scalar builders).
    Cql(&'a CqlType),
    /// Flat `DataType` dispatch (the legacy builders), carrying whether the
    /// column's CQL type makes `build_string_array` take its STRICT branch.
    Flat(&'a DataType, TextFidelity),
    /// A REAL `Utf8` array slot whose content is `ValueFormatter::format_value`'s
    /// rendering — `build_string_array`, and each element slot of
    /// `build_list_array` / `build_map_array`. Pays the variable-width slot
    /// overhead.
    RenderedSlot,
    /// NOT an array slot of its own: a sub-part of an enclosing slot's single
    /// rendered string (a container element rendered inline). Pays content only.
    RenderedInline,
}

/// Whether a column's CQL type is an AUTHORITATIVE text type, which makes
/// `build_string_array` take its `strict_text` branch: it borrows the `&str`
/// after a NON-lossy `str::from_utf8` and hard-errors on invalid UTF-8, rather
/// than rendering through the lossy `ValueFormatter::format_value`. The two
/// branches have different byte behaviour (`s.len()` versus up to `3 * s.len()`
/// of U+FFFD expansion), so the estimate must distinguish them.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TextFidelity {
    /// `cql_type` is `Text`/`Ascii`/`Varchar`: exact, non-lossy.
    Strict,
    /// No authoritative text type: `format_value`'s lossy rendering.
    Lossy,
}

/// Resolve a column to its Arrow slot shape, mirroring `convert_column_to_array`
/// exactly: only the high-fidelity CQL arms take the typed path; everything else
/// (including `Text`/`Blob`/numeric CQL types) falls through to the flat
/// `data_type` dispatch — `convert_column_to_array` dispatches those on
/// `data_type` alone, so the estimate must too.
pub(super) fn column_shape(col: &ColumnInfo) -> Shape<'_> {
    let mut fidelity = TextFidelity::Lossy;
    if let Some(cql) = &col.cql_type {
        let effective = unwrap_frozen_type(cql);
        match effective {
            CqlType::Text | CqlType::Ascii | CqlType::Varchar => {
                // Falls through to the flat dispatch, but tells it which
                // `build_string_array` branch the converter will take.
                fidelity = TextFidelity::Strict;
            }
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
            // #4114: high-fidelity like the other collections — the converter
            // dispatches a vector through `build_typed_value_array`.
            | CqlType::Vector(_, _)
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
    Shape::Flat(&col.data_type, fidelity)
}

/// Residual charged once per row for `shape`'s column — [`ARROW_COLUMN_SLACK_BYTES`]
/// only where the converter can materialize an Arrow array node that corresponds
/// to no value slot, and `0` everywhere else (issue #2825 review C1).
///
/// Only the FLAT `list`/`set`/`map` builders have such a node: `build_list_array`
/// always builds a `Utf8` child and `build_map_array` always builds a key and a
/// value `Utf8` child, each carrying an empty 4-byte offsets buffer, however few
/// entries the cell holds. Every other column is either a single array node with
/// no children (every fixed-width builder, `build_binary_array`,
/// `build_string_array` and the render fallbacks that route through it) or takes
/// the high-fidelity path, which charges each declared child node EXPLICITLY —
/// see `charge_cql`'s empty-collection rule. Charging the residual for those
/// over-counted a fixed-width column by ~3.2× (13 B/row estimated against
/// ~4.1 B/row realized) and put the byte-cap's binding point at 40 `int`
/// columns, where the row-cap must still bind.
///
/// Exhaustive over [`DataType`] — a new variant is a compile error here, so it
/// cannot silently default to "no residual".
pub(super) fn column_slack_bytes(shape: &Shape<'_>) -> usize {
    match shape {
        Shape::Flat(dt, _) => match dt {
            DataType::List | DataType::Set | DataType::Map => ARROW_COLUMN_SLACK_BYTES,
            DataType::Boolean
            | DataType::TinyInt
            | DataType::SmallInt
            | DataType::Integer
            | DataType::BigInt
            | DataType::Float32
            | DataType::Float
            | DataType::Timestamp
            | DataType::Uuid
            | DataType::Blob
            | DataType::Text
            | DataType::Json
            | DataType::Tuple
            | DataType::Udt
            | DataType::Frozen
            | DataType::Tombstone
            | DataType::Null => 0,
        },
        // The typed path charges every declared node itself; the rendered shapes
        // are never a COLUMN shape (`column_shape` returns only `Cql`/`Flat`).
        Shape::Cql(_) | Shape::RenderedSlot | Shape::RenderedInline => 0,
    }
}

/// Whether charging this slot can queue FURTHER slots.
///
/// A `true` slot is queued on the worklist and spends one [`MAX_ESTIMATE_NODES`]
/// node; a `false` slot is a LEAF, charged in place against
/// [`MAX_ESTIMATE_LEAF_SLOTS`] with no worklist entry — which is what stops a
/// wide collection's element count from consuming the structural budget
/// (review C2).
///
/// Conservative in the safe direction: a spurious `true` merely spends a node. A
/// `false` MUST be exact — `charge_slot` must queue nothing for it — and the
/// debug assertion in [`Estimator::charge_child`] pins that invariant.
pub(super) fn branches(shape: Shape<'_>, value: Option<&Value>) -> bool {
    match shape {
        Shape::Cql(t) => match unwrap_frozen_type(t) {
            CqlType::List(_)
            | CqlType::Set(_)
            // #4114: a vector queues one child slot per element, like a list.
            | CqlType::Vector(_, _)
            | CqlType::Map(_, _)
            | CqlType::Tuple(_)
            | CqlType::Udt(_, _)
            | CqlType::Frozen(_) => true,
            // Renders the value: branches exactly when the value does.
            CqlType::Custom(_) => value_branches(value),
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
            | CqlType::Date
            | CqlType::Time
            | CqlType::Decimal
            | CqlType::Varint
            | CqlType::Duration
            | CqlType::Uuid
            | CqlType::TimeUuid
            | CqlType::Inet
            | CqlType::Counter => false,
        },
        Shape::Flat(dt, _) => match dt {
            DataType::List | DataType::Set | DataType::Map => true,
            // `build_string_array`'s render fallback: branches with the value.
            DataType::Text
            | DataType::Json
            | DataType::Tuple
            | DataType::Udt
            | DataType::Frozen
            | DataType::Tombstone
            | DataType::Null => value_branches(value),
            DataType::Boolean
            | DataType::TinyInt
            | DataType::SmallInt
            | DataType::Integer
            | DataType::BigInt
            | DataType::Float32
            | DataType::Float
            | DataType::Timestamp
            | DataType::Uuid
            | DataType::Blob => false,
        },
        Shape::RenderedSlot | Shape::RenderedInline => value_branches(value),
    }
}

/// Whether `ValueFormatter::format_value` would render this value by walking
/// children. `Value::Json` is NOT one: `json_render_bytes` walks it with its own
/// bounded loop rather than through the worklist.
pub(super) fn value_branches(value: Option<&Value>) -> bool {
    matches!(
        value.map(unwrap_frozen_value),
        Some(
            Value::List(_)
                | Value::Set(_)
                | Value::Tuple(_)
                | Value::Map(_)
                | Value::Udt(_)
                | Value::Frozen(_)
        )
    )
}

/// Unwrap nested `Frozen` wrappers to the effective type.
///
/// Loops to a FIXPOINT, exactly as `arrow_convert::unwrap_frozen_type` does: a
/// capped unwrap would diverge from the converter for a chain longer than the
/// cap, routing the estimate to a rendered fallback while the converter still
/// took the typed builder — an UNDER-count (issue #2825 rust-reviewer nit).
/// Termination is structural: `CqlType` is a finite owned tree with no cycles.
pub(super) fn unwrap_frozen_type(mut t: &CqlType) -> &CqlType {
    while let CqlType::Frozen(inner) = t {
        t = inner;
    }
    t
}

/// Unwrap nested `Value::Frozen` wrappers (mirrors
/// `arrow_convert::unwrap_frozen_value`), bounded the same way.
pub(super) fn unwrap_frozen_value(mut v: &Value) -> &Value {
    for _ in 0..MAX_FROZEN_DEPTH {
        match v {
            Value::Frozen(inner) => v = inner,
            _ => return v,
        }
    }
    v
}

/// Cap on `Value::Frozen` unwrap iterations — a bound, not a semantic limit:
/// real values nest at most once or twice, and `arrow_convert` unwraps a single
/// level, so unwrapping FEWER levels than the converter is impossible here.
const MAX_FROZEN_DEPTH: usize = 16;
