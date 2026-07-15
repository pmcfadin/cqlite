//! Per-column decode dispatch tag (Epic J, issue #1635).
//!
//! Type dispatch is constant per column but was resolved per cell: every non-key
//! cell called `column.data_type.to_lowercase()` and walked a ~30-arm string ladder
//! (`cell_value.rs`), plus a second `to_lowercase()` in `is_complex_column`. A
//! 1M-row × 10-col scan performed ~20M transient type-string normalizations
//! producing nothing but a branch target.
//!
//! `CellKind` is the precomputed dispatch tag. It is computed ONCE per column at
//! `RowColumnResolution::build` time (issue #1046 hoist) and stored on
//! `ColumnToParse`; the per-cell decode then `match`es on it (a jump table) with no
//! per-cell allocation. Its scalar variants map 1:1 onto the existing scalar decode
//! arms in [`super::V5CompressedLegacyParser::parse_cell_value_schema_order`]; the
//! [`CellKind::Complex`] variant carries the already-lowercased type string for the
//! frozen / tuple / non-frozen-collection / marshal-UDT / unknown-scalar slow paths
//! (the "thin adapter" Epic J2 later collapses — J1 makes *dispatch* per-column, not
//! the decode bodies).
//!
//! No-heuristics (issue #28): the tag is derived ONLY from the authoritative column
//! type string (supplied schema type, or the on-disk SerializationHeader marshal
//! type for a dropped column) — never from value byte patterns.

use std::sync::Arc;

/// A column's precomputed value-decode dispatch, resolved once per block.
///
/// Scalar variants correspond exactly to the scalar arms of
/// `parse_cell_value_schema_order`; [`CellKind::Complex`] carries the lowercased
/// declared type for the frozen/tuple/collection/marshal-UDT/default decode ladder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CellKind {
    Boolean,
    /// CQL `int` (i32).
    Int,
    /// CQL `text` / `varchar` / `ascii`.
    Text,
    /// CQL `uuid` / `timeuuid`.
    Uuid,
    Decimal,
    /// CQL `bigint` (i64).
    BigInt,
    Counter,
    /// CQL `double` (f64).
    Double,
    Timestamp,
    Date,
    Duration,
    /// CQL `float` (f32).
    Float,
    /// CQL `smallint` / `short`.
    SmallInt,
    /// CQL `tinyint` / `byte`.
    TinyInt,
    Time,
    Inet,
    /// The literal CQL `blob` type (decodes to `Blob`, empty-value → `Blob([])`).
    Blob,
    /// Frozen / tuple / non-frozen-collection / marshal-UDT / unknown-scalar types:
    /// the already-lowercased declared type string, decoded by the retained string
    /// ladder. Empty-value → `Null` (matching the pre-J1 `_ => Null` empty arm).
    Complex(Arc<str>),
}

impl CellKind {
    /// Resolve the decode dispatch for a declared type string. Lowercases the type
    /// ONCE (the same normalization the per-cell path did, now paid once per column
    /// at bind time) and maps it to the matching scalar arm, or [`CellKind::Complex`]
    /// (carrying the lowercased string) for everything the scalar ladder does not
    /// name directly. Pure; never inspects value bytes (no-heuristics, issue #28).
    #[must_use]
    pub(super) fn from_type(data_type: &str) -> CellKind {
        let lowered = data_type.to_lowercase();
        match lowered.as_str() {
            "boolean" => CellKind::Boolean,
            "int" => CellKind::Int,
            "text" | "varchar" | "ascii" => CellKind::Text,
            "uuid" | "timeuuid" => CellKind::Uuid,
            "decimal" => CellKind::Decimal,
            "bigint" => CellKind::BigInt,
            "counter" => CellKind::Counter,
            "double" => CellKind::Double,
            "timestamp" => CellKind::Timestamp,
            "date" => CellKind::Date,
            "duration" => CellKind::Duration,
            "float" => CellKind::Float,
            "smallint" | "short" => CellKind::SmallInt,
            "tinyint" | "byte" => CellKind::TinyInt,
            "time" => CellKind::Time,
            "inet" => CellKind::Inet,
            "blob" => CellKind::Blob,
            // frozen<…>, tuple<…>, list/set/map<…>, marshal forms, varint, and any
            // unrecognized type: decoded by the retained string ladder. Store the
            // lowercased string so the ladder needs no per-cell `to_lowercase`.
            _ => CellKind::Complex(Arc::from(lowered.as_str())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_types_map_to_scalar_kinds() {
        assert_eq!(CellKind::from_type("boolean"), CellKind::Boolean);
        assert_eq!(CellKind::from_type("int"), CellKind::Int);
        assert_eq!(CellKind::from_type("bigint"), CellKind::BigInt);
        assert_eq!(CellKind::from_type("counter"), CellKind::Counter);
        assert_eq!(CellKind::from_type("float"), CellKind::Float);
        assert_eq!(CellKind::from_type("double"), CellKind::Double);
        assert_eq!(CellKind::from_type("decimal"), CellKind::Decimal);
        assert_eq!(CellKind::from_type("timestamp"), CellKind::Timestamp);
        assert_eq!(CellKind::from_type("date"), CellKind::Date);
        assert_eq!(CellKind::from_type("time"), CellKind::Time);
        assert_eq!(CellKind::from_type("duration"), CellKind::Duration);
        assert_eq!(CellKind::from_type("inet"), CellKind::Inet);
        assert_eq!(CellKind::from_type("blob"), CellKind::Blob);
    }

    #[test]
    fn text_family_and_uuid_family_collapse() {
        assert_eq!(CellKind::from_type("text"), CellKind::Text);
        assert_eq!(CellKind::from_type("varchar"), CellKind::Text);
        assert_eq!(CellKind::from_type("ascii"), CellKind::Text);
        assert_eq!(CellKind::from_type("uuid"), CellKind::Uuid);
        assert_eq!(CellKind::from_type("timeuuid"), CellKind::Uuid);
        assert_eq!(CellKind::from_type("smallint"), CellKind::SmallInt);
        assert_eq!(CellKind::from_type("short"), CellKind::SmallInt);
        assert_eq!(CellKind::from_type("tinyint"), CellKind::TinyInt);
        assert_eq!(CellKind::from_type("byte"), CellKind::TinyInt);
    }

    #[test]
    fn declared_type_is_normalized_once_case_insensitively() {
        // The supplied schema may carry uppercase CQL type names; dispatch must be
        // case-insensitive exactly like the removed per-cell `to_lowercase`.
        assert_eq!(CellKind::from_type("TEXT"), CellKind::Text);
        assert_eq!(CellKind::from_type("Int"), CellKind::Int);
        assert_eq!(CellKind::from_type("BigInt"), CellKind::BigInt);
    }

    #[test]
    fn complex_and_unknown_types_carry_lowercased_string() {
        assert_eq!(
            CellKind::from_type("frozen<list<int>>"),
            CellKind::Complex(Arc::from("frozen<list<int>>"))
        );
        assert_eq!(
            CellKind::from_type("Tuple<Int, Text>"),
            CellKind::Complex(Arc::from("tuple<int, text>"))
        );
        // Non-frozen collections reach here only via the recursion/test paths; the
        // hot loop routes them to `parse_complex_column` before dispatch.
        assert_eq!(
            CellKind::from_type("list<int>"),
            CellKind::Complex(Arc::from("list<int>"))
        );
        // `varint` has no dedicated scalar arm — pre-J1 it fell to the default blob
        // decode (live) / Null (empty); it must land in `Complex`, not `Blob`.
        assert_eq!(
            CellKind::from_type("varint"),
            CellKind::Complex(Arc::from("varint"))
        );
    }
}
