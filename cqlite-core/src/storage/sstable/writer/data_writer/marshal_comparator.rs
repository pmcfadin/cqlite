//! Cassandra `AbstractType` comparators for the keys/elements of a FROZEN SORTED
//! collection, driven by the DECLARED MARSHAL string (not by the `Value` variant).
//!
//! Used by [`super::udt_canon`]'s `sort_sorted_collection` to order a frozen
//! `SetType`'s elements / a `MapType`'s keys while canonicalizing a UDT value
//! against its declared type. For a collection FIELD of a frozen UDT that sort is
//! the ONLY one on the write path — `serialization/types.rs`
//! `serialize_collection_elements` iterates without re-sorting — so this module
//! decides those on-disk bytes.
//!
//! This is the SECOND writer-side sorted-collection comparator; the first is
//! [`super::collection_order::compare_collection_elements`], which is driven by the
//! `Value` variant and serves the non-frozen complex-cell path and
//! `encoding.rs`'s frozen `serialize_value`. They are INDEPENDENT implementations
//! of one Cassandra rule and both had the same `time` defect (issue #3935), so
//! `tests/writer_comparator_differential.rs` pins them against each other.
//!
//! Extracted from `udt_canon.rs` (1392 lines, campsite limit 800) as part of epic
//! #1116, so this comparator has a file of its own and `udt_canon` keeps only the
//! canonicalization it is named for. Behaviour is unchanged by the move.

use super::udt_canon::primitive_marshal_name;
use super::*;
use std::cmp::Ordering;

/// Comparator family for a frozen sorted-collection key/element marshal.
pub(super) enum CompareKind {
    /// Compare by the UNSIGNED order of the serialized wire bytes. Correct for
    /// byte-ordered Cassandra `AbstractType`s (UTF8Type/AsciiType/BytesType,
    /// InetAddressType, SimpleDateType — whose epoch is shifted so byte order is
    /// value order — BooleanType) AND for composite frozen UDT/tuple/collection
    /// elements (Cassandra orders those by their serialized bytes here too).
    UnsignedBytes,
    /// Compare as a SIGNED numeric of the given width: Int32Type→i32, LongType/
    /// CounterColumnType/TimestampType→i64, ByteType→i8, ShortType→i16.
    /// Unsigned big-endian byte order disagrees with the Cassandra comparator for
    /// these (e.g. `-1` = 0xFFFFFFFF would sort AFTER `0`), so they are compared on
    /// the decoded signed value.
    ///
    /// `TimeType` is deliberately NOT here — see the arms below (#3935).
    SignedInt,
}

/// Classify the comparator for a key/element marshal `ty`. A non-primitive marshal
/// (UDT/tuple/list/set/map element of a sorted collection) is byte-ordered. A
/// primitive marshal maps to its AbstractType comparator family; a primitive whose
/// comparator we cannot implement confidently returns `None` (caller fails closed).
///
/// `pub(super)` so the data_writer differential test can pin this comparator
/// against `collection_order::compare_collection_elements` (issue #3935): they are
/// two independent implementations of one Cassandra rule, and a second
/// implementation's agreement is only knowable by testing it.
pub(super) fn classify_comparator(ty: &str) -> Option<CompareKind> {
    let Some(name) = primitive_marshal_name(ty) else {
        // Composite frozen element (UDT/tuple/collection): byte-ordered.
        return Some(CompareKind::UnsignedBytes);
    };
    match name {
        // Signed integers — Cassandra compares the decoded signed value.
        // `TimestampType` belongs here and `TimeType` does NOT (#3935): pinned
        // `cassandra-5.0.8` `db/marshal/TimestampType.java:56` is
        // `super(ComparisonType.CUSTOM)` whose `compareCustom` (`:69-71`) is
        // exactly `return LongType.compareLongs(...)` — SIGNED.
        "Int32Type" | "LongType" | "ByteType" | "ShortType" | "CounterColumnType"
        | "TimestampType" => Some(CompareKind::SignedInt),
        // Byte-ordered AbstractTypes: unsigned serialized-byte order == comparator.
        // SimpleDateType is byte-ordered (epoch shifted by 2^31 at serialization).
        // `TimeType` is BYTE_ORDER too, NOT signed (#3935): pinned
        // `cassandra-5.0.8` `db/marshal/TimeType.java:48`
        // `private TimeType() {super(ComparisonType.BYTE_ORDER);}` =
        // `ByteBufferUtil.compareUnsigned` over the 8-byte big-endian nanos long.
        // Cassandra accepts/stores/BYTE_ORDERs an out-of-range NEGATIVE binary
        // `time`, so range validation would not reconcile signed with byte order —
        // that argument lives ONCE in `types::comparator::custom::compare_time`.
        // The two orders coincide for every non-negative `i64`, so no in-range
        // on-disk ordering moved; and this comparator is the ONLY sort for a UDT's
        // `SetType`/`MapType` field (`serialize_collection_elements` never
        // re-sorts), so it decides those bytes.
        "UTF8Type" | "AsciiType" | "BytesType" | "InetAddressType" | "BooleanType"
        | "SimpleDateType" | "TimeType" => Some(CompareKind::UnsignedBytes),
        // FAIL-CLOSED (no-heuristics, issue #28; tracked for #1254): types whose
        // Cassandra comparator is non-trivial and NOT plain unsigned-byte order:
        //   UUIDType/TimeUUIDType/LexicalUUIDType — version- and time-field-aware,
        //     not raw byte order;
        //   IntegerType (varint) / DecimalType — sign+magnitude/scale aware;
        //   FloatType/DoubleType — total-order with NaN/sign handling;
        //   DurationType — not a sortable AbstractType.
        _ => None,
    }
}

/// Ensure the comparator for `ty` can be applied to `value` (fail-closed up front).
pub(super) fn comparator_supported_for(ty: &str, value: &Value) -> Result<()> {
    match classify_comparator(ty) {
        Some(CompareKind::UnsignedBytes) => Ok(()),
        Some(CompareKind::SignedInt) => {
            // Confirm the live value is one of the signed-int variants we decode.
            signed_value(value).map(|_| ())
        }
        None => Err(unsupported_comparator_err(ty)),
    }
}

fn unsupported_comparator_err(ty: &str) -> Error {
    Error::InvalidInput(format!(
        "frozen sorted-collection key/element type '{ty}' has no comparator implemented in the \
         canonicalizer; ordering it by raw serialized bytes could produce NON-Cassandra bytes, so \
         it is rejected rather than guessed (no-heuristics, issue #28; tracked for follow-up #1254)"
    ))
}

/// Decode the SIGNED i128 value of a signed-integer `Value`, or an error if the
/// variant is not one of the signed-int variants (Integer/BigInt/Counter/Timestamp/
/// TinyInt/SmallInt). Widening to `i128` makes all four widths comparable in one
/// ordering. `Value::Time` is deliberately ABSENT (#3935) — `TimeType` is
/// BYTE_ORDER, so a `time` key/element is compared by its serialized bytes via
/// [`CompareKind::UnsignedBytes`] and is never decoded here.
fn signed_value(value: &Value) -> Result<i128> {
    match value {
        Value::Integer(n) => Ok(*n as i128),
        Value::BigInt(n) | Value::Counter(n) | Value::Timestamp(n) => Ok(*n as i128),
        Value::TinyInt(n) => Ok(*n as i128),
        Value::SmallInt(n) => Ok(*n as i128),
        other => Err(Error::InvalidInput(format!(
            "expected a signed-integer value for a signed-comparator key/element type, got {other:?}"
        ))),
    }
}

/// Compare two key/element values for the marshal `ty`, given each value and its
/// precomputed serialized bytes. SIGNED-int marshals compare the decoded signed
/// values; byte-ordered marshals compare the unsigned serialized bytes.
pub(super) fn compare_for_marshal(
    ty: &str,
    a_val: &Value,
    a_bytes: &[u8],
    b_val: &Value,
    b_bytes: &[u8],
) -> Result<Ordering> {
    match classify_comparator(ty) {
        Some(CompareKind::UnsignedBytes) => Ok(a_bytes.cmp(b_bytes)),
        Some(CompareKind::SignedInt) => Ok(signed_value(a_val)?.cmp(&signed_value(b_val)?)),
        None => Err(unsupported_comparator_err(ty)),
    }
}
