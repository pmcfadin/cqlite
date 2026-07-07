//! Type-aware SET-element / MAP-key ordering for the SSTable writer (issue #1275,
//! recursive composites #1296).
//!
//! Cassandra `SetType`/`MapType` are SORTED collections: a frozen set serializes
//! its elements, and a frozen/non-frozen map its entries, in the order defined by
//! the element/key type's own `AbstractType.compare` — NOT raw unsigned
//! serialized-byte order. This module is the SINGLE shared comparator both the
//! non-frozen complex-cell path (`complex.rs`) and the frozen `serialize_value`
//! path (`encoding.rs`) call, so the two cannot drift.
//!
//! Responsibility split (epic #1116):
//!   * [`scalar`] — the #1275 SCALAR leaf comparators (signed integers, Java
//!     `Float/Double.compare`, signed `varint`, scale-aware `decimal`, `UUIDType`).
//!   * [`composite`] — the #1296 RECURSIVE comparators for `tuple`/UDT and frozen
//!     nested `set`/`list`/`map`, which recurse back into [`compare_collection_elements`]
//!     so their leaves reuse the scalar logic above.
//!
//! Part of the `data_writer` responsibility split (issue #1118). `use super::*`
//! provides the crate imports and sibling helpers re-exported from
//! `data_writer/mod.rs` (notably `serialize_value`). No emitted bytes change for
//! types whose comparator is unsigned-lexicographic.

use super::*;
use std::cmp::Ordering;

mod composite;
mod scalar;

/// Total-order comparator for two SET elements / MAP keys, matching the
/// element/key type's Cassandra `AbstractType.compare` (issue #1275, composites
/// #1296).
///
/// For most types (text/ascii/blob/boolean/inet/date) that comparator IS
/// unsigned-lexicographic over the serialized bytes, so raw byte order is
/// correct (see the per-type audit on the fallback arm below). The SCALAR types
/// that order differently (signed integers, float/double, varint, decimal, uuid)
/// are handled by [`scalar`]; the COMPOSITE types that compare field/element-wise
/// (tuple, UDT, nested frozen set/list/map) are handled RECURSIVELY by
/// [`composite`], whose leaves call back into this function.
///
/// The ordering oracle is Cassandra's per-type `AbstractType.compare`
/// (`org.apache.cassandra.db.marshal`). The decision is driven ENTIRELY by the
/// element/key `Value` variant (authoritative CQL type metadata carried by the
/// value itself) — never by inspecting serialized byte patterns (no-heuristics,
/// issue #28). Any pair whose variants are not a known signed/numeric/composite
/// type (or are mixed/unsupported) falls back to comparing their serialized
/// bytes, the historical unsigned-lexicographic behavior that is correct for
/// those types — and the conservative no-guess choice when type metadata for an
/// element/key is otherwise unavailable.
pub(crate) fn compare_collection_elements(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        // Signed fixed-width integers — native signed order.
        (Value::TinyInt(x), Value::TinyInt(y)) => x.cmp(y),
        (Value::SmallInt(x), Value::SmallInt(y)) => x.cmp(y),
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::BigInt(x), Value::BigInt(y)) => x.cmp(y),
        (Value::Counter(x), Value::Counter(y)) => x.cmp(y),
        // Temporal longs (TimestampType/TimeType extend/share LongType).
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        (Value::Time(x), Value::Time(y)) => x.cmp(y),
        // Floating point — Float.compare / Double.compare total order.
        (Value::Float32(x), Value::Float32(y)) => scalar::compare_f32_java(*x, *y),
        (Value::Float(x), Value::Float(y)) => scalar::compare_f64_java(*x, *y),
        // Varint — signed big-integer compare of two's-complement bodies.
        (Value::Varint(x), Value::Varint(y)) => scalar::compare_signed_varint(x, y),
        // Decimal — scale-aware numeric compare.
        (
            Value::Decimal {
                scale: sa,
                unscaled: ua,
            },
            Value::Decimal {
                scale: sb,
                unscaled: ub,
            },
        ) => scalar::compare_decimal(*sa, ua, *sb, ub),
        // UUID/TimeUUID — version-first + v1-timestamp + tail compare (UUIDType).
        (Value::Uuid(x), Value::Uuid(y)) => scalar::compare_uuid_cassandra(x, y),
        // Composite types — recurse field/element-wise per Cassandra's
        // TupleType/UserType/CollectionType comparators (issue #1296). Each
        // bottoms out at this same dispatcher, reusing the scalar leaves above.
        (Value::Tuple(x), Value::Tuple(y)) => composite::compare_tuple(x, y),
        (Value::Udt(x), Value::Udt(y)) => composite::compare_udt(x, y),
        // frozen<set<T>> arrives as Set: compare the SORTED element sequences
        // (Cassandra stores set elements sorted; sorting first canonicalizes nested
        // inner sets BOTTOM-UP before the outer orders by them, #1296).
        (Value::Set(x), Value::Set(y)) => composite::compare_set(x, y),
        // frozen<list<T>> arrives as List: compare in INSERTION order (no sort),
        // shorter-first tiebreak.
        (Value::List(x), Value::List(y)) => composite::compare_list(x, y),
        // frozen<map<K,V>> — per entry compare key then value.
        (Value::Map(x), Value::Map(y)) => composite::compare_map(x, y),
        // Frozen wrappers: compare the inner values by the same rule (a
        // frozen<set<int>> element is Frozen(Set(..)) → unwraps to the Set arm).
        (Value::Frozen(x), other) => compare_collection_elements(x, other),
        (x, Value::Frozen(y)) => compare_collection_elements(x, y),
        // Everything else falls back to unsigned-lexicographic order over the
        // serialized bytes. Per-type audit vs Cassandra `AbstractType.compare`
        // (issue #1275 convergence audit) — each of these IS unsigned-byte-
        // lexicographic, so raw serialized bytes are the correct order:
        //   * `text`/`varchar` (UTF8Type) and `ascii` (AsciiType): both use
        //     `ComparisonType.BYTE_ORDER` — UTF-8 / 7-bit ASCII bytes sort
        //     unsigned-lexicographically, which equals codepoint order.
        //   * `blob` (BytesType): BYTE_ORDER, raw bytes.
        //   * `boolean` (BooleanType): BYTE_ORDER over the single serialized byte
        //     (0x00 false < 0x01 true), which is the natural order.
        //   * `inet` (InetAddressType): BYTE_ORDER — IPv4 (4 bytes) sorts before
        //     any IPv6 (16 bytes) by length-then-byte, matching Cassandra.
        //   * `date` (SimpleDateType): stored as an UNSIGNED 32-bit day count
        //     offset by 2^31, compared `ByteBufferUtil.compareUnsigned`, i.e.
        //     unsigned big-endian bytes — exactly raw-byte order.
        //   * `duration` (DurationType): Cassandra makes duration NON-comparable
        //     (`isEmptyValueMeaningless`/no total order) and FORBIDS it as a set
        //     element or map key, so a `Value::Duration` cannot legitimately reach
        //     this comparator; the fallback keeps the sort total/panic-free if one
        //     somehow does, but it is not an ordering Cassandra would ever emit.
        // A mixed/unsupported variant pair also lands here (byte fallback keeps
        // the sort total) — including a composite whose element/field type
        // metadata is unavailable, where the no-heuristics mandate (#28) forbids
        // guessing, so we keep the conservative byte fallback rather than infer.
        _ => match (serialize_value(a), serialize_value(b)) {
            (Ok(ba), Ok(bb)) => ba.cmp(&bb),
            // Serialization should not fail for a non-null collection element; if
            // it does, treat as equal so the sort stays total and panic-free.
            _ => Ordering::Equal,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Int32Type/SetType: signed order — -1 before 0 before 1, even though
    /// `0xFFFF_FFFF` (-1) sorts LAST under raw unsigned bytes.
    #[test]
    fn int_elements_sort_signed() {
        let mut v = vec![Value::Integer(1), Value::Integer(-1), Value::Integer(0)];
        v.sort_by(compare_collection_elements);
        assert_eq!(
            v,
            vec![Value::Integer(-1), Value::Integer(0), Value::Integer(1)]
        );
    }

    /// LongType (bigint), ShortType (smallint), ByteType (tinyint): all signed.
    #[test]
    fn other_signed_integers_sort_signed() {
        let mut big = vec![Value::BigInt(5), Value::BigInt(-9), Value::BigInt(0)];
        big.sort_by(compare_collection_elements);
        assert_eq!(
            big,
            vec![Value::BigInt(-9), Value::BigInt(0), Value::BigInt(5)]
        );

        let mut small = vec![Value::SmallInt(2), Value::SmallInt(-2)];
        small.sort_by(compare_collection_elements);
        assert_eq!(small, vec![Value::SmallInt(-2), Value::SmallInt(2)]);

        let mut tiny = vec![Value::TinyInt(7), Value::TinyInt(-7)];
        tiny.sort_by(compare_collection_elements);
        assert_eq!(tiny, vec![Value::TinyInt(-7), Value::TinyInt(7)]);
    }

    /// DoubleType/FloatType: Double.compare total order — -1.5 < -0.0 < +0.0 < 2.0,
    /// NaN sorts last; raw big-endian bytes would put negatives high.
    #[test]
    fn floats_sort_by_total_order() {
        let mut v = [
            Value::Float(2.0),
            Value::Float(f64::NAN),
            Value::Float(-1.5),
            Value::Float(0.0),
        ];
        v.sort_by(compare_collection_elements);
        assert_eq!(v[0], Value::Float(-1.5));
        assert_eq!(v[1], Value::Float(0.0));
        assert_eq!(v[2], Value::Float(2.0));
        assert!(matches!(v[3], Value::Float(f) if f.is_nan()));
    }

    /// Unsigned-lexicographic types (text/blob/boolean) keep serialized-byte
    /// order — raw byte comparison is the correct Cassandra comparator there.
    #[test]
    fn text_keeps_byte_order() {
        let mut v = vec![
            Value::Text("c".to_string()),
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
        ];
        v.sort_by(compare_collection_elements);
        assert_eq!(
            v,
            vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
                Value::Text("c".to_string()),
            ]
        );
    }
}
