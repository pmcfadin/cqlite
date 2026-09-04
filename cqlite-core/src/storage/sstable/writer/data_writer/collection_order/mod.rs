//! Type-aware SET-element / MAP-key ordering for the SSTable writer (issue #1275,
//! recursive composites #1296).
//!
//! Cassandra `SetType`/`MapType` are SORTED collections: a frozen set serializes
//! its elements, and a frozen/non-frozen map its entries, in the order defined by
//! the element/key type's own `AbstractType.compare` — NOT raw unsigned
//! serialized-byte order. This module is the SINGLE shared comparator both the
//! non-frozen complex-cell path (`complex.rs`) and the frozen `serialize_value`
//! path (`encoding.rs`) call, so the two cannot drift. Both callers are covered
//! by `tests/issue_3935_collection_time_byte_order.rs`.
//!
//! SCOPED: "single" means for those two callers, NOT for the writer as a whole.
//! [`super::marshal_comparator`] is a SECOND, independent implementation of the
//! same Cassandra rule, dispatched on the DECLARED MARSHAL rather than the
//! `Value` variant, which orders a frozen UDT's `SetType`/`MapType` FIELD. The
//! two are pinned against each other by
//! `tests/writer_comparator_differential.rs` — they had the same `time` defect
//! independently (#3935), so their agreement is tested rather than assumed.
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
//!
//! Issue #3935 corrected the `time` arm from signed to `TimeType`'s
//! `ComparisonType.BYTE_ORDER`. The two orders coincide for EVERY NON-NEGATIVE
//! `i64` — a `0x00`-`0x7F` sign byte makes unsigned byte order and signed
//! numeric order the same order — so they diverge ONLY when a NEGATIVE nanos is
//! present, and only that case moves emitted bytes. That covers strictly more
//! than `time`'s valid range `0..=86_399_999_999_999`: an out-of-range POSITIVE
//! nanos is unmoved too. Every in-range `time` collection is byte-identical
//! before and after.

use super::*;
use std::cmp::Ordering;

mod composite;
mod scalar;

/// Total-order comparator for two SET elements / MAP keys, matching the
/// element/key type's Cassandra `AbstractType.compare` (issue #1275, composites
/// #1296).
///
/// For many types (text/ascii/blob/inet/date, and `boolean` for every
/// canonically-serialized value) that comparator IS unsigned-lexicographic over
/// the serialized bytes, so raw byte order is correct — see the per-type audit
/// on the fallback arm below, which names each type's `ComparisonType` and
/// declares the one residual. The SCALAR types that order differently (signed
/// integers, `timestamp`, float/double, varint, decimal, uuid) are handled by the
/// explicit arms and [`scalar`]; the COMPOSITE types that compare
/// field/element-wise (tuple, UDT, nested frozen set/list/map) are handled
/// RECURSIVELY by [`composite`], whose leaves call back into this function.
///
/// The two temporal types do NOT share a comparator, despite both serializing as
/// an 8-byte big-endian long: `timestamp` is signed (`TimestampType` is CUSTOM ->
/// `LongType.compareLongs`) and `time` is unsigned byte order (`TimeType` is
/// `ComparisonType.BYTE_ORDER`). See the two arms below for the pinned citations;
/// conflating them was issue #3935.
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
        // `timestamp` — SIGNED 64-bit order. `TimestampType` is
        // `ComparisonType.CUSTOM` and its `compareCustom` body is exactly
        // `return LongType.compareLongs(left, accessorL, right, accessorR);`,
        // which compares the SIGNED first byte (Java `getByte` is signed) and
        // then the unsigned tail. So a pre-epoch (negative) timestamp sorts
        // BELOW every non-negative one, and `i64::cmp` is that order verbatim.
        // Authority (never a CQLite file:line, #3041), pinned `cassandra-5.0.8`:
        // `db/marshal/TimestampType.java:56` `super(ComparisonType.CUSTOM)` +
        // `:69-71` `compareCustom` -> `LongType.compareLongs`.
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        // `time` — UNSIGNED BIG-ENDIAN BYTE ORDER, *not* signed numeric order.
        // `TimeType` is `ComparisonType.BYTE_ORDER`, i.e.
        // `ByteBufferUtil.compareUnsigned` over the serialized 8-byte
        // big-endian nanos-since-midnight long. Authority, pinned
        // `cassandra-5.0.8`: `db/marshal/TimeType.java:48`
        // `private TimeType() {super(ComparisonType.BYTE_ORDER);}`.
        //
        // An out-of-range (negative) binary `time` is a value Cassandra ACCEPTS
        // and orders BYTE_ORDER — range validation would not make the two orders
        // agree. That argument, with its `TimeSerializer` citations, is written
        // out ONCE, in `types::comparator::custom::compare_time`; do not restate
        // it here.
        //
        // The two orders coincide for every NON-NEGATIVE `i64` and diverge only
        // for a NEGATIVE nanos, whose sign bit makes the leading byte >= `0x80`:
        // BYTE_ORDER sorts it ABOVE every non-negative value, where `i64::cmp`
        // sorted it below (issue #3935). So no in-range on-disk ordering moves.
        //
        // Comparing `to_be_bytes()` is the serialized form verbatim, which makes
        // this whole-collection path agree with BOTH of the sites it must:
        //   * the per-element write path, `schema_helpers::compare_cell_paths`,
        //     which compares the raw serialized cell-path bytes unsigned; and
        //   * the read comparator `types::comparator::custom` `Custom("time")`
        //     (#3790), which already compares `to_be_bytes()`; and
        //   * the declared-marshal comparator `marshal_comparator`, which orders a
        //     frozen UDT's collection field (corrected in the same issue).
        (Value::Time(x), Value::Time(y)) => x.to_be_bytes().cmp(&y.to_be_bytes()),
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
        //   * `boolean` (BooleanType): `ComparisonType.CUSTOM`, NOT BYTE_ORDER
        //     (`db/marshal/BooleanType.java:39`). Its `compareCustom` (`:53-60`)
        //     normalizes each operand to `getByte(v, 0) == 0 ? 0 : 1` — "false is
        //     0, true is ANYTHING else" — before comparing. DECLARED RESIDUAL,
        //     not fixed here: for a CANONICAL serialization (`0x00` / `0x01`) the
        //     byte fallback agrees with Cassandra exactly, and for a NON-canonical
        //     truthy byte it does not (Cassandra: `0x02` compares EQUAL to `0x01`;
        //     the byte fallback: `0x02` sorts GREATER than `0x01`). CQLite
        //     serializes `Value::Boolean` canonically, so the divergence needs a
        //     non-canonical serialized boolean to observe; that is out of scope
        //     for issue #3935 and is stated rather than left implicit.
        //   * `inet` (InetAddressType): BYTE_ORDER, i.e.
        //     `ByteBufferUtil.compareUnsigned`, which is LEXICOGRAPHIC over the
        //     common prefix with LENGTH only as a tiebreak WHEN one operand is a
        //     prefix of the other — NOT length-first. Rust's `<[u8] as Ord>::cmp`
        //     is exactly that, so the byte fallback matches Cassandra including
        //     the mixed-family case. NOTE the consequence, since an earlier
        //     revision of this audit claimed "IPv4 sorts before any IPv6 by
        //     length-then-byte" and that is FALSE: `::1`
        //     (`00..01`, 16 bytes) sorts BEFORE `9.0.0.1` (`09 00 00 01`,
        //     4 bytes), because the first byte already settles it. A
        //     `set<inet>` mixing families falsifies the length-first reading.
        //     `types::comparator::custom::compare_inet` states the same rule.
        //   * `date` (SimpleDateType): stored as an UNSIGNED 32-bit day count
        //     offset by 2^31, compared `ByteBufferUtil.compareUnsigned`, i.e.
        //     unsigned big-endian bytes — exactly raw-byte order.
        //   * `duration` (DurationType): BYTE_ORDER
        //     (`db/marshal/DurationType.java:46` `super(ComparisonType.BYTE_ORDER)`).
        //     An earlier revision of this audit claimed Cassandra makes duration
        //     NON-comparable; that was FALSE. The CONCLUSION survives for a
        //     different and better reason: CQL FORBIDS `duration` as a set element
        //     or a map key outright — `cql3/CQL3Type.java:830-831` throws
        //     "Durations are not allowed inside sets" and `:837-838` "Durations
        //     are not allowed as map keys" — so a `Value::Duration` cannot legitimately
        //     reach this comparator at all. The fallback keeps the sort
        //     total/panic-free if one somehow does.
        //   * `counter` (CounterColumnType): `ComparisonType.NOT_COMPARABLE`
        //     (`db/marshal/CounterColumnType.java:38`) — THIS is the type
        //     Cassandra makes non-comparable, and it is also forbidden inside a
        //     collection (`cql3/CQL3Type.java:827-828` "Counters are not allowed
        //     inside collections", `:835-836` for map keys). So a `Value::Counter`
        //     cannot legitimately be a set element or map key either. The
        //     `Value::Counter` arm ABOVE compares signed rather than falling
        //     through; it exists solely to keep the sort TOTAL for a value shape
        //     Cassandra would never emit, and asserts nothing about Cassandra's
        //     ordering (there is none to match).
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

    /// `time` (TimeType) — issue #3935: a NEGATIVE nanos sorts ABOVE every
    /// non-negative one, INVERTING where the negatives sit relative to the
    /// signed order this arm used to have. (Not a reversal of the whole
    /// sequence: within each sign class the relative order is unchanged, which
    /// is why the expectation below is spelled out element by element rather
    /// than described as "reversed".)
    ///
    /// Expectation derived from the pinned source, never from CQLite's own
    /// behaviour (#3041): `db/marshal/TimeType.java:48`
    /// `private TimeType() {super(ComparisonType.BYTE_ORDER);}`, and
    /// `ComparisonType.BYTE_ORDER` is `ByteBufferUtil.compareUnsigned` over the
    /// serialized 8-byte big-endian nanos. `-1i64` serializes to
    /// `FF FF FF FF FF FF FF FF`, whose leading byte `0xFF` is unsigned-GREATER
    /// than the `0x00` leading byte of every value in `0..=86_399_999_999_999`
    /// (max `0x00 00 4E 94 91 4E FF FF`).
    #[test]
    fn time_negative_nanos_sorts_above_every_non_negative() {
        // Sanity on the serialized forms the rule is stated over, so the
        // assertion below is anchored on bytes and not on a remembered claim.
        assert_eq!((-1_i64).to_be_bytes(), [0xFF; 8]);
        assert_eq!(0_i64.to_be_bytes()[0], 0x00);
        assert_eq!(86_399_999_999_999_i64.to_be_bytes()[0], 0x00);

        let mut v = vec![
            Value::Time(86_399_999_999_999),
            Value::Time(-1),
            Value::Time(0),
            Value::Time(i64::MIN),
        ];
        v.sort_by(compare_collection_elements);
        assert_eq!(
            v,
            vec![
                // 0x00.. and 0x00.. — the in-range values, ascending.
                Value::Time(0),
                Value::Time(86_399_999_999_999),
                // 0x80.. — i64::MIN, the SMALLEST signed value, sorts here.
                Value::Time(i64::MIN),
                // 0xFF.. — the LARGEST unsigned leading byte sorts last.
                Value::Time(-1),
            ],
            "TimeType is ComparisonType.BYTE_ORDER: negatives sort above every \
             non-negative, and i64::MIN (0x80..) below -1 (0xFF..)"
        );

        // NEGATIVE CONTROL — the OLD signed implementation produces a DIFFERENT
        // sequence, so the assertion above provably has teeth rather than being
        // satisfiable by any total order.
        let mut signed = vec![
            Value::Time(86_399_999_999_999),
            Value::Time(-1),
            Value::Time(0),
            Value::Time(i64::MIN),
        ];
        signed.sort_by(|a, b| match (a, b) {
            (Value::Time(x), Value::Time(y)) => x.cmp(y),
            _ => Ordering::Equal,
        });
        assert_ne!(
            signed, v,
            "signed i64::cmp must NOT reproduce the BYTE_ORDER sequence, else \
             this case cannot distinguish the two implementations"
        );
    }

    /// `timestamp` (TimestampType) stays SIGNED — the regression pin for the
    /// half of the removed "TimestampType/TimeType extend/share LongType"
    /// comment that WAS right.
    ///
    /// Pinned authority: `db/marshal/TimestampType.java:56`
    /// `private TimestampType() {super(ComparisonType.CUSTOM);}` with
    /// `compareCustom` (`:69-71`) delegating verbatim to
    /// `LongType.compareLongs`, which compares the SIGNED first byte (Java
    /// `getByte` is signed) and then the unsigned tail. So a pre-epoch negative
    /// millis sorts BELOW every non-negative one — the opposite of `time`.
    #[test]
    fn timestamp_keeps_signed_order() {
        let mut v = vec![
            Value::Timestamp(1),
            Value::Timestamp(-1),
            Value::Timestamp(0),
            Value::Timestamp(i64::MIN),
        ];
        v.sort_by(compare_collection_elements);
        assert_eq!(
            v,
            vec![
                Value::Timestamp(i64::MIN),
                Value::Timestamp(-1),
                Value::Timestamp(0),
                Value::Timestamp(1),
            ],
            "TimestampType is CUSTOM -> LongType.compareLongs (SIGNED); #3935 \
             changed `time` only and must not have moved `timestamp`"
        );

        // The two temporal types must now DISAGREE on the same nanos/millis
        // sequence — that disagreement IS the corrected rule.
        assert_eq!(
            compare_collection_elements(&Value::Timestamp(-1), &Value::Timestamp(0)),
            Ordering::Less
        );
        assert_eq!(
            compare_collection_elements(&Value::Time(-1), &Value::Time(0)),
            Ordering::Greater
        );
    }

    /// COMPATIBILITY PIN (#3935): for every `time` in Cassandra's valid range
    /// `0..=86_399_999_999_999` the order is UNCHANGED by this fix, so no
    /// in-range on-disk collection ordering moved.
    ///
    /// Why it holds: every such value has a `0x00` sign byte, so unsigned byte
    /// order, unsigned numeric order and SIGNED numeric order all coincide over
    /// the range. Asserted rather than asserted-about: the case compares the new
    /// BYTE_ORDER comparator against a signed `i64::cmp` reference over the
    /// range's boundaries and interior, and requires them EQUAL on every pair.
    #[test]
    fn in_range_time_order_is_unchanged() {
        const MAX_VALID_NANOS: i64 = 86_399_999_999_999; // DAYS.toNanos(1) - 1
        let in_range = [
            0,
            1,
            9_000_000_000,
            10_000_000_000,
            43_200_000_000_000,
            MAX_VALID_NANOS,
        ];
        for &x in &in_range {
            assert_eq!(
                x.to_be_bytes()[0],
                0x00,
                "{x} must have a 0x00 sign byte for the coincidence argument to hold"
            );
            for &y in &in_range {
                assert_eq!(
                    compare_collection_elements(&Value::Time(x), &Value::Time(y)),
                    x.cmp(&y),
                    "in-range time pair ({x}, {y}) must order identically under \
                     BYTE_ORDER and the pre-#3935 signed order"
                );
            }
        }

        // And the whole sorted sequence is the numeric one.
        let mut v: Vec<Value> = in_range.iter().rev().map(|&n| Value::Time(n)).collect();
        v.sort_by(compare_collection_elements);
        let expected: Vec<Value> = in_range.iter().map(|&n| Value::Time(n)).collect();
        assert_eq!(v, expected);
    }

    /// Unsigned-lexicographic types (text/blob) keep serialized-byte order —
    /// raw byte comparison is the correct Cassandra comparator there. (`boolean`
    /// was listed here too and is NOT one: `BooleanType` is CUSTOM — see the
    /// declared residual in the fallback audit above. It agrees with byte order
    /// for every canonically-serialized value, which is why it was mistaken for
    /// BYTE_ORDER.)
    #[test]
    fn text_keeps_byte_order() {
        let mut v = vec![
            Value::text("c".to_string()),
            Value::text("a".to_string()),
            Value::text("b".to_string()),
        ];
        v.sort_by(compare_collection_elements);
        assert_eq!(
            v,
            vec![
                Value::text("a".to_string()),
                Value::text("b".to_string()),
                Value::text("c".to_string()),
            ]
        );
    }
}
