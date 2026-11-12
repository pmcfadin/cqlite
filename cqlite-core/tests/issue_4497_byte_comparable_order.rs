//! Issue #4497 (AC 2/3/4) — the OSS50 byte-comparable CLUSTERING encoding preserves the
//! typed comparator's order, per supported CQL type, ASC and DESC.
//!
//! Subject: `encode_clustering_bound_oss50_with_order` — the encoder that produces the
//! separators the `Rows.db` row-index trie stores and compares against.
//!
//! # Property
//! For a COMPLETE clustering (what Cassandra writes: `ClusteringComparator.
//! ByteComparableClustering`, `40 <component> ... <kind byte>`, kind byte `0x38` =
//! `ByteSource.TERMINATOR` for `Kind.CLUSTERING` under OSS50):
//!
//! ```text
//! cmp_bytes(enc(x) ++ [0x38], enc(y) ++ [0x38]) == typed_cmp(x, y)          (ASC)
//! cmp_bytes(enc(x) ++ [0x38], enc(y) ++ [0x38]) == typed_cmp(y, x)          (DESC)
//! ```
//! The terminator is part of the property on purpose. A bare component is only WEAKLY
//! prefix-free (`ByteSource.AbstractEscaper` javadoc, `ByteSource.java:309-327`: "the
//! encoding of "A" (4100) is a prefix of the encoding of "A\0" (4100FE)"), and
//! `ReversedType.asComparableBytes` (`ReversedType.java:70-87`) inverts every byte on the
//! documented condition that it is "used within a sequence": the byte following the
//! component (`0x38`/`0x40`) is what makes the inverted prefix compare LARGER. Comparing
//! a bare DESC varlen component would assert something Cassandra itself does not claim.
//!
//! # Typed comparators (Cassandra `cassandra-5.0.8`, `src/java/org/apache/cassandra/db/marshal/`)
//! * `int`      `Int32Type.compareCustom:61-71`  — signed (first byte signed, rest unsigned)
//! * `bigint`   `LongType.compareLongs:63-`      — signed
//! * `smallint` `ShortType.compareCustom:49-55`  — signed
//! * `tinyint`  `ByteType.compareCustom:49-52`   — signed
//! * `boolean`  `BooleanType.compareCustom:53-62` — false < true
//! * `timestamp` `TimestampType.compareCustom:69-72` — `LongType.compareLongs`
//! * `text`/`ascii`/`blob`/`inet` — `ValueAccessor.compare`: unsigned lexicographic,
//!   shorter-prefix first (`BytesType`/`UTF8Type`/`AsciiType` inherit it; `inet` is bytes)
//! * `uuid`     `UUIDType.compareCustom:74-119`  — version first; v1 by reordered
//!   timestamp; else unsigned msb; then UNSIGNED lsb
//! * DESC       `ReversedType.compareCustom:95-98` — `baseType.compare(right, left)`
//!
//! # Not covered (the encoder REFUSES them with an explicit error — pinned below)
//! `varint`, `decimal`, `float`, `double`, `date`, `time`, `duration`: no ordering claim
//! exists to test; [`unsupported_clustering_types_are_refused_not_misordered`] pins that
//! they fail closed rather than mis-sort.
//!
//! Known failure (kept, `#[ignore]`d, NOT worked around): `uuid` — see
//! [`uuid_order_preserved`].

use std::cmp::Ordering;

use bytes::Bytes;
use cqlite_core::storage::sstable::bti::encode_clustering_bound_oss50_with_order;
use cqlite_core::types::Value;
use proptest::prelude::*;

#[path = "support/proptest_cfg.rs"]
mod proptest_cfg;

/// `ClusteringPrefix.Kind.CLUSTERING.asByteComparableValue(OSS50)` = `ByteSource.TERMINATOR`.
const KIND_CLUSTERING: u8 = 0x38;

fn enc(vals: &[Value], reversed: &[bool]) -> Vec<u8> {
    let mut b = encode_clustering_bound_oss50_with_order(vals, reversed)
        .expect("supported type must encode");
    b.push(KIND_CLUSTERING);
    b
}

/// Typed comparison of two same-type values (ascending), per the module doc's citations.
fn typed(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::BigInt(x), Value::BigInt(y)) => x.cmp(y),
        (Value::SmallInt(x), Value::SmallInt(y)) => x.cmp(y),
        (Value::TinyInt(x), Value::TinyInt(y)) => x.cmp(y),
        (Value::Boolean(x), Value::Boolean(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        (Value::Text(x), Value::Text(y)) => x.as_ref().cmp(y.as_ref()),
        (Value::Blob(x), Value::Blob(y)) => x.as_ref().cmp(y.as_ref()),
        (Value::Inet(x), Value::Inet(y)) => x.as_ref().cmp(y.as_ref()),
        (Value::Uuid(x), Value::Uuid(y)) => uuid_type_cmp(x, y),
        other => panic!("typed(): mixed or unsupported pair {other:?}"),
    }
}

/// `UUIDType.compareCustom` (`UUIDType.java:74-119`).
fn uuid_type_cmp(a: &[u8; 16], b: &[u8; 16]) -> Ordering {
    let msb = |u: &[u8; 16]| u64::from_be_bytes(u[..8].try_into().expect("8 bytes"));
    let lsb = |u: &[u8; 16]| u64::from_be_bytes(u[8..].try_into().expect("8 bytes"));
    let (m1, m2) = (msb(a), msb(b));
    let (v1, v2) = (((m1 >> 12) & 0xf) as i32, ((m2 >> 12) & 0xf) as i32);
    if v1 != v2 {
        return v1.cmp(&v2);
    }
    let first = if v1 == 1 {
        // TimeUUIDType.reorderTimestampBytes (AbstractTimeUUIDType.java:139-144), compared as signed long
        let re = |m: u64| ((m << 48) | ((m << 16) & 0xFFFF_0000_0000) | (m >> 32)) as i64;
        re(m1).cmp(&re(m2))
    } else {
        m1.cmp(&m2)
    };
    first.then(lsb(a).cmp(&lsb(b)))
}

fn with_dir(o: Ordering, reversed: bool) -> Ordering {
    if reversed {
        o.reverse()
    } else {
        o
    }
}

// ─── generators (edge-biased: signed boundaries, 0x00/0xFF bytes, shared prefixes) ──

fn small_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(
        prop_oneof![
            Just(0x00u8),
            Just(0x01),
            Just(0x41),
            Just(0x7f),
            Just(0xfe),
            Just(0xff)
        ],
        0..5,
    )
}

fn arb_int() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(i32::MIN),
        Just(-1),
        Just(0),
        Just(1),
        Just(i32::MAX),
        any::<i32>()
    ]
    .prop_map(Value::Integer)
}
fn arb_bigint() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(i64::MIN),
        Just(-1),
        Just(0),
        Just(1),
        Just(i64::MAX),
        any::<i64>()
    ]
    .prop_map(Value::BigInt)
}
fn arb_smallint() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(i16::MIN),
        Just(-1),
        Just(0),
        Just(i16::MAX),
        any::<i16>()
    ]
    .prop_map(Value::SmallInt)
}
fn arb_tinyint() -> impl Strategy<Value = Value> {
    prop_oneof![Just(i8::MIN), Just(-1), Just(0), Just(i8::MAX), any::<i8>()]
        .prop_map(Value::TinyInt)
}
fn arb_bool() -> impl Strategy<Value = Value> {
    any::<bool>().prop_map(Value::Boolean)
}
fn arb_timestamp() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(i64::MIN),
        Just(-1),
        Just(0),
        Just(i64::MAX),
        any::<i64>()
    ]
    .prop_map(Value::Timestamp)
}
fn arb_text() -> impl Strategy<Value = Value> {
    // valid UTF-8 containing NUL and a multi-byte char (0xC3 0xA9) — the escaper's hard cases
    prop::collection::vec(
        prop_oneof![Just('\0'), Just('A'), Just('B'), Just('\u{e9}')],
        0..5,
    )
    .prop_map(|cs| Value::text(cs.into_iter().collect::<String>()))
}
fn arb_blob() -> impl Strategy<Value = Value> {
    small_bytes().prop_map(|b| Value::Blob(Bytes::from(b)))
}
fn arb_inet() -> impl Strategy<Value = Value> {
    prop_oneof![
        prop::array::uniform4(prop_oneof![Just(0u8), Just(1), Just(255), any::<u8>()])
            .prop_map(|a| a.to_vec()),
        prop::array::uniform16(prop_oneof![Just(0u8), Just(255), any::<u8>()])
            .prop_map(|a| a.to_vec()),
    ]
    .prop_map(|b| Value::Inet(Bytes::from(b)))
}
fn arb_uuid() -> impl Strategy<Value = Value> {
    (
        prop_oneof![Just(0u8), Just(1), Just(3), Just(4), Just(5)],
        any::<[u8; 16]>(),
    )
        .prop_map(|(ver, mut b)| {
            b[6] = (b[6] & 0x0f) | (ver << 4);
            Value::Uuid(b)
        })
}

macro_rules! order_props {
    ($name:ident, $gen:expr) => {
        proptest! {
            #![proptest_config(proptest_cfg::config(stringify!($name)))]
            #[test]
            fn $name(x in $gen, y in $gen) {
                for reversed in [false, true] {
                    let got = enc(std::slice::from_ref(&x), &[reversed])
                        .cmp(&enc(std::slice::from_ref(&y), &[reversed]));
                    let want = with_dir(typed(&x, &y), reversed);
                    prop_assert_eq!(got, want, "reversed={} x={:?} y={:?}", reversed, x, y);
                }
            }
        }
    };
}

order_props!(int_order_preserved, arb_int());
order_props!(bigint_order_preserved, arb_bigint());
order_props!(smallint_order_preserved, arb_smallint());
order_props!(tinyint_order_preserved, arb_tinyint());
order_props!(boolean_order_preserved, arb_bool());
order_props!(timestamp_order_preserved, arb_timestamp());
order_props!(text_order_preserved, arb_text());
order_props!(blob_order_preserved, arb_blob());
order_props!(inet_order_preserved, arb_inet());

proptest! {
    #![proptest_config(proptest_cfg::config("uuid_order_preserved"))]
    /// FINDING: the encoder writes a `uuid` as its RAW 16 bytes, but `UUIDType.
    /// asComparableBytes` (`UUIDType.java:122-140`) emits version-first / timestamp-reordered
    /// bytes, so bytewise order disagrees with `UUIDType.compareCustom` as soon as versions
    /// differ (or for version 1).
    #[test]
    #[ignore = "#4531 uuid clustering is encoded as raw bytes; UUIDType orders by version then reordered timestamp (UUIDType.java:122-140)"]
    fn uuid_order_preserved(x in arb_uuid(), y in arb_uuid()) {
        for reversed in [false, true] {
            let got = enc(std::slice::from_ref(&x), &[reversed]).cmp(&enc(std::slice::from_ref(&y), &[reversed]));
            prop_assert_eq!(got, with_dir(typed(&x, &y), reversed), "reversed={} x={:?} y={:?}", reversed, x, y);
        }
    }

    /// Two-column clustering, every ASC/DESC combination: the encoding of a (text, int)
    /// clustering sorts exactly as the lexicographic typed comparison with per-column
    /// reversal (`ClusteringComparator.compare`).
    #[test]
    fn two_column_clustering_order_preserved(
        a in (arb_text(), arb_int()),
        b in (arb_text(), arb_int()),
        r0 in any::<bool>(),
        r1 in any::<bool>(),
    ) {
        let rev = [r0, r1];
        let got = enc(&[a.0.clone(), a.1.clone()], &rev).cmp(&enc(&[b.0.clone(), b.1.clone()], &rev));
        let want = with_dir(typed(&a.0, &b.0), r0).then_with(|| with_dir(typed(&a.1, &b.1), r1));
        prop_assert_eq!(got, want, "rev={:?} a={:?} b={:?}", rev, a, b);
    }
}

/// The weak-prefix edge, pinned deterministically: "A" vs "A\0" in both directions.
#[test]
fn varlen_prefix_edge_sorts_correctly_in_asc_and_desc() {
    let a = Value::text("A");
    let a0 = Value::text("A\0");
    assert!(
        enc(std::slice::from_ref(&a), &[false]) < enc(std::slice::from_ref(&a0), &[false]),
        "ASC: A < A\\0"
    );
    assert!(
        enc(&[a], &[true]) > enc(&[a0], &[true]),
        "DESC: A > A\\0 (ReversedType)"
    );
}

/// Types the encoder cannot order are REFUSED, never silently mis-sorted.
#[test]
fn unsupported_clustering_types_are_refused_not_misordered() {
    let unsupported = [
        Value::Varint(Bytes::from_static(&[0x01, 0x00])),
        Value::Decimal {
            scale: 2,
            unscaled: vec![0x01],
        },
        Value::Float(1.5),
        Value::Float32(1.5),
        Value::Date(0),
        Value::Time(0),
        Value::Duration {
            months: 0,
            days: 0,
            nanos: 0,
        },
    ];
    for v in unsupported {
        assert!(
            encode_clustering_bound_oss50_with_order(std::slice::from_ref(&v), &[false]).is_err(),
            "{v:?} must be refused as a byte-comparable clustering component"
        );
    }
}
