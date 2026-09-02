//! Issue #3790: `inet` and `time` must order by VALUE, not by their formatted
//! string.
//!
//! `ComparatorType::from_cql_type` maps `CqlType::Inet` -> `Custom("inet")` and
//! `CqlType::Time` -> `Custom("time")`, and the `Custom(_)` dispatch arm used to
//! fall through to a `format!("{}", value)` string comparison for every custom
//! name. Both are legal Cassandra clustering-column types, so that misordered
//! them.
//!
//! Format authority — pinned Cassandra 5.0.8 (a CQLite `file:line` is never
//! format authority, #3041):
//!   * `src/java/org/apache/cassandra/db/marshal/InetAddressType.java`
//!     `InetAddressType() { super(ComparisonType.BYTE_ORDER); }`
//!   * `src/java/org/apache/cassandra/db/marshal/TimeType.java`
//!     `TimeType() { super(ComparisonType.BYTE_ORDER); }`
//!
//! `ComparisonType.BYTE_ORDER` dispatches to `ByteBufferUtil.compareUnsigned`:
//! UNSIGNED lexicographic comparison of the serialized bytes, and where one
//! operand is a prefix of the other the SHORTER sorts first. Rust's
//! `<[u8] as Ord>::cmp` is exactly that, which is also what settles the
//! IPv4-vs-IPv6 differing-length case.

use cqlite_core::schema::CqlType;
use cqlite_core::types::{ComparatorType, UdtValue, Value};
use cqlite_core::Error;
use std::cmp::Ordering;

fn inet_cmp() -> ComparatorType {
    let c = ComparatorType::from_cql_type(&CqlType::Inet).expect("inet comparator");
    assert_eq!(
        c,
        ComparatorType::Custom("inet".to_string()),
        "wiring pin: CqlType::Inet must reach the Custom(\"inet\") arm"
    );
    c
}

fn time_cmp() -> ComparatorType {
    let c = ComparatorType::from_cql_type(&CqlType::Time).expect("time comparator");
    assert_eq!(
        c,
        ComparatorType::Custom("time".to_string()),
        "wiring pin: CqlType::Time must reach the Custom(\"time\") arm"
    );
    c
}

// ---------------------------------------------------------------------------
// inet
// ---------------------------------------------------------------------------

/// The divergence that names the bug (issue #3790's own example): as FORMATTED
/// STRINGS `"10.0.0.2" < "9.0.0.1"` (`'1' < '9'`), but by ADDRESS BYTES
/// `[10,0,0,2] > [9,0,0,1]` (`10 > 9`). Cassandra compares the bytes.
#[test]
fn inet_orders_by_address_bytes_not_by_formatted_string() {
    let ten = Value::inet(vec![10u8, 0, 0, 2]);
    let nine = Value::inet(vec![9u8, 0, 0, 1]);

    // Precondition: the formatted-string order really does disagree, so this
    // test cannot pass vacuously if the rendering ever changes.
    assert!(
        format!("{}", ten) < format!("{}", nine),
        "precondition: formatted-string order must disagree with byte order"
    );

    let c = inet_cmp();
    assert_eq!(c.compare(&ten, &nine).unwrap(), Ordering::Greater);
    assert_eq!(c.compare(&nine, &ten).unwrap(), Ordering::Less);
    assert_eq!(c.compare(&ten, &ten).unwrap(), Ordering::Equal);
}

/// AC1, differing length: `ByteBufferUtil.compareUnsigned` compares the common
/// prefix first and, when one operand IS a prefix of the other, the SHORTER
/// sorts first. So a 4-byte IPv4 sorts below a 16-byte IPv6 whose first four
/// bytes equal it.
#[test]
fn inet_ipv4_sorts_before_ipv6_that_extends_it() {
    let v4 = Value::inet(vec![10u8, 0, 0, 1]);
    let mut v6_bytes = vec![0u8; 16];
    v6_bytes[..4].copy_from_slice(&[10u8, 0, 0, 1]);
    let v6 = Value::inet(v6_bytes);

    let c = inet_cmp();
    assert_eq!(c.compare(&v4, &v6).unwrap(), Ordering::Less);
    assert_eq!(c.compare(&v6, &v4).unwrap(), Ordering::Greater);
}

/// AC1, differing length: the comparison is UNSIGNED, so a leading `0xFF` byte
/// sorts ABOVE a leading `0x00` byte. A signed byte compare would read `0xFF`
/// as `-1` and answer `Less`; length never gets consulted because the first
/// byte already decides.
#[test]
fn inet_differing_length_compares_leading_bytes_unsigned() {
    let v4_high = Value::inet(vec![255u8, 255, 255, 255]);
    let v6_zero = Value::inet(vec![0u8; 16]);

    let c = inet_cmp();
    assert_eq!(c.compare(&v4_high, &v6_zero).unwrap(), Ordering::Greater);
    assert_eq!(c.compare(&v6_zero, &v4_high).unwrap(), Ordering::Less);
}

// ---------------------------------------------------------------------------
// time
// ---------------------------------------------------------------------------

/// AC2, the property itself: `time` orders by nanos-since-midnight.
///
/// NOTE on the string-order divergence: `Value`'s `fmt_time` renders
/// `TIME(HH:MM:SS.nnnnnnnnn)` with EVERY field zero-padded to a fixed width, and
/// over `time`'s valid range (`0..=86_399_999_999_999`) `HH` never exceeds `23`,
/// so for all IN-RANGE values the formatted-string order happens to coincide
/// with numeric order. There is therefore no in-range string-vs-value
/// counterexample to pin; this test pins the ordering property directly, and
/// `time_byte_order_places_negative_nanos_above_zero` below pins the case where
/// the formatted string genuinely does disagree.
#[test]
fn time_orders_by_nanos() {
    let c = time_cmp();
    // 01:00:00.000000000, 12:34:56.000000007, 23:59:59.999999999
    let ordered = [
        Value::Time(3_600_000_000_000),
        Value::Time(45_296_000_000_007),
        Value::Time(86_399_999_999_999),
    ];
    for i in 0..ordered.len() {
        for j in 0..ordered.len() {
            let expected = i.cmp(&j);
            assert_eq!(
                c.compare(&ordered[i], &ordered[j]).unwrap(),
                expected,
                "time nanos ordering at ({}, {})",
                i,
                j
            );
        }
    }
}

/// AC2, the case where the formatted string DOES disagree, and the case that
/// distinguishes Cassandra's `BYTE_ORDER` from a plain SIGNED `i64::cmp`.
///
/// `TimeType` is `ComparisonType.BYTE_ORDER` over the 8-byte big-endian long, so
/// an out-of-range NEGATIVE nanos has its sign bit set, giving a leading `0xFF`
/// byte, and sorts ABOVE every non-negative value. Both a signed `i64::cmp`
/// (`-1 < 0`) and the formatted string (`'-' < '0'`) answer `Less` here.
#[test]
fn time_byte_order_places_negative_nanos_above_zero() {
    let neg = Value::Time(-1);
    let zero = Value::Time(0);

    // Preconditions: both of the WRONG answers really are `Less`.
    assert_eq!((-1i64).cmp(&0i64), Ordering::Less, "signed cmp says Less");
    assert!(
        format!("{}", neg) < format!("{}", zero),
        "formatted-string order says Less"
    );

    let c = time_cmp();
    assert_eq!(c.compare(&neg, &zero).unwrap(), Ordering::Greater);
    assert_eq!(c.compare(&zero, &neg).unwrap(), Ordering::Less);
    // Among negatives, byte order is still ascending in the unsigned reading:
    // -2 (0xFFFF..FE) < -1 (0xFFFF..FF).
    assert_eq!(
        c.compare(&Value::Time(-2), &neg).unwrap(),
        Ordering::Less,
        "-2 precedes -1 under unsigned byte order"
    );
}

// ---------------------------------------------------------------------------
// AC4 — composite delegation through ComparatorType's own arms
// ---------------------------------------------------------------------------

/// A `Tuple(inet, time)` must delegate to the corrected scalar leaves: the first
/// differing field decides, and each field uses its own comparator.
#[test]
fn tuple_of_inet_and_time_delegates_to_corrected_leaves() {
    let c = ComparatorType::from_cql_type(&CqlType::Tuple(vec![CqlType::Inet, CqlType::Time]))
        .expect("tuple comparator");

    let low_addr = Value::Tuple(vec![
        Value::inet(vec![9u8, 0, 0, 1]),
        Value::Time(86_399_999_999_999),
    ]);
    let high_addr = Value::Tuple(vec![Value::inet(vec![10u8, 0, 0, 2]), Value::Time(0)]);
    // inet leaf decides: [9,..] < [10,..] even though the time field is reversed.
    assert_eq!(c.compare(&low_addr, &high_addr).unwrap(), Ordering::Less);

    // Equal inet leaf: the time leaf decides.
    let same_addr_early = Value::Tuple(vec![Value::inet(vec![10u8, 0, 0, 2]), Value::Time(1)]);
    let same_addr_late = Value::Tuple(vec![Value::inet(vec![10u8, 0, 0, 2]), Value::Time(2)]);
    assert_eq!(
        c.compare(&same_addr_early, &same_addr_late).unwrap(),
        Ordering::Less
    );
}

/// A UDT with an `inet` field and a `time` field must delegate to the corrected
/// scalar leaves, in field-definition order.
#[test]
fn udt_with_inet_and_time_fields_delegates_to_corrected_leaves() {
    let c = ComparatorType::from_cql_type(&CqlType::Udt(
        "endpoint".to_string(),
        vec![
            ("addr".to_string(), CqlType::Inet),
            ("at".to_string(), CqlType::Time),
        ],
    ))
    .expect("udt comparator");

    let mk = |addr: Vec<u8>, nanos: i64| {
        Value::Udt(Box::new(
            UdtValue::new("endpoint".to_string(), "ks".to_string())
                .with_field("addr".to_string(), Some(Value::inet(addr)))
                .with_field("at".to_string(), Some(Value::Time(nanos))),
        ))
    };

    // addr decides: [9,0,0,1] < [10,0,0,2] by bytes (the reverse of string order).
    assert_eq!(
        c.compare(&mk(vec![9, 0, 0, 1], 5), &mk(vec![10, 0, 0, 2], 1))
            .unwrap(),
        Ordering::Less
    );
    // addr equal: `at` decides by nanos.
    assert_eq!(
        c.compare(&mk(vec![10, 0, 0, 2], 1), &mk(vec![10, 0, 0, 2], 2))
            .unwrap(),
        Ordering::Less
    );
}

/// A `List` delegates element-wise to its element comparator, so a
/// `list<inet>` orders by address bytes.
#[test]
fn list_of_inet_delegates_elementwise() {
    let c = ComparatorType::from_cql_type(&CqlType::List(Box::new(CqlType::Inet)))
        .expect("list comparator");
    let a = Value::List(vec![Value::inet(vec![9u8, 0, 0, 1])]);
    let b = Value::List(vec![Value::inet(vec![10u8, 0, 0, 2])]);
    assert_eq!(c.compare(&a, &b).unwrap(), Ordering::Less);
}

/// A `set<inet>` / `map<time, text>` carries the corrected leaf comparator in
/// its element/key position, and asking that leaf directly yields byte order.
///
/// This is the pin available at this layer: CQLite's `compare_set`/`compare_map`
/// are equality-only today (they ignore their element/key comparators
/// entirely), so the composite arms cannot themselves witness leaf ordering.
/// What must hold is that the leaf a collection carries is the CORRECTED one.
#[test]
fn set_and_map_carry_the_corrected_leaf_comparator() {
    let set_cmp = ComparatorType::from_cql_type(&CqlType::Set(Box::new(CqlType::Inet)))
        .expect("set comparator");
    match &set_cmp {
        ComparatorType::Set(element) => {
            assert_eq!(
                element
                    .compare(
                        &Value::inet(vec![10u8, 0, 0, 2]),
                        &Value::inet(vec![9u8, 0, 0, 1])
                    )
                    .unwrap(),
                Ordering::Greater
            );
        }
        other => panic!("expected Set comparator, got {:?}", other),
    }

    let map_cmp = ComparatorType::from_cql_type(&CqlType::Map(
        Box::new(CqlType::Time),
        Box::new(CqlType::Text),
    ))
    .expect("map comparator");
    match &map_cmp {
        ComparatorType::Map(key, _) => {
            assert_eq!(
                key.compare(&Value::Time(-1), &Value::Time(0)).unwrap(),
                Ordering::Greater,
                "map key leaf must use TimeType BYTE_ORDER"
            );
        }
        other => panic!("expected Map comparator, got {:?}", other),
    }

    let set_time = ComparatorType::from_cql_type(&CqlType::Set(Box::new(CqlType::Time)))
        .expect("set<time> comparator");
    match &set_time {
        ComparatorType::Set(element) => {
            assert_eq!(
                element.compare(&Value::Time(1), &Value::Time(2)).unwrap(),
                Ordering::Less
            );
        }
        other => panic!("expected Set comparator, got {:?}", other),
    }
}

/// A frozen tuple of `inet` + `time` delegates through `Frozen`.
#[test]
fn frozen_tuple_of_inet_and_time_delegates() {
    let c = ComparatorType::from_cql_type(&CqlType::Frozen(Box::new(CqlType::Tuple(vec![
        CqlType::Inet,
        CqlType::Time,
    ]))))
    .expect("frozen tuple comparator");
    let a = Value::Frozen(Box::new(Value::Tuple(vec![
        Value::inet(vec![9u8, 0, 0, 1]),
        Value::Time(0),
    ])));
    let b = Value::Frozen(Box::new(Value::Tuple(vec![
        Value::inet(vec![10u8, 0, 0, 2]),
        Value::Time(0),
    ])));
    assert_eq!(c.compare(&a, &b).unwrap(), Ordering::Less);
}

// ---------------------------------------------------------------------------
// Type mismatch
// ---------------------------------------------------------------------------

/// A type mismatch must be an `Error::Schema` type-mismatch, matching every
/// sibling `compare_*` helper — NOT a silent fallback to string comparison.
#[test]
fn type_mismatch_is_a_schema_error_not_a_string_compare() {
    let cases: Vec<(ComparatorType, Value, Value, &str)> = vec![
        (
            inet_cmp(),
            Value::text("10.0.0.2".to_string()),
            Value::inet(vec![9u8, 0, 0, 1]),
            "inet",
        ),
        (
            inet_cmp(),
            Value::inet(vec![9u8, 0, 0, 1]),
            Value::Integer(1),
            "inet",
        ),
        (time_cmp(), Value::BigInt(1), Value::Time(2), "time"),
        (
            time_cmp(),
            Value::Time(2),
            Value::text("00:00:00".to_string()),
            "time",
        ),
    ];

    for (comparator, left, right, expected) in cases {
        match comparator.compare(&left, &right) {
            Err(Error::Schema(msg)) => {
                assert!(
                    msg.contains("Type mismatch") && msg.contains(expected),
                    "unexpected schema error message: {}",
                    msg
                );
            }
            other => panic!(
                "expected Error::Schema type mismatch for {}, got {:?}",
                expected, other
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// supports_ordering + AC5 audit
// ---------------------------------------------------------------------------

/// `supports_ordering()` is public API and must not lie: `inet` and `time` are
/// legal Cassandra clustering-column types and DO order; the residual
/// unresolved-UDT / unknown `Custom(name)` does not.
#[test]
fn supports_ordering_is_true_for_inet_and_time_only() {
    assert!(inet_cmp().supports_ordering());
    assert!(time_cmp().supports_ordering());
    assert!(!ComparatorType::Custom("udt:Address".to_string()).supports_ordering());
    assert!(!ComparatorType::Custom("address_type".to_string()).supports_ordering());

    // Composites propagate it.
    assert!(
        ComparatorType::from_cql_type(&CqlType::Tuple(vec![CqlType::Inet, CqlType::Time]))
            .unwrap()
            .supports_ordering()
    );
    assert!(
        ComparatorType::from_cql_type(&CqlType::List(Box::new(CqlType::Inet)))
            .unwrap()
            .supports_ordering()
    );
}

/// AC5 audit: after this fix, NO native Cassandra scalar type reaches the
/// residual string-compare `Custom` path. Every `CqlType` scalar variant either
/// has its own `ComparatorType` arm or is one of the two corrected custom names,
/// and every one of them orders.
///
/// Note there is no `CqlType::Json` variant at all (the `json` schema keyword
/// reaches `CqlType::Custom("json")` via `CqlType::parse`'s catch-all), and CQL
/// has no `json` COLUMN type — JSON is an INSERT/SELECT modifier — so no
/// clustering column can be `json` and the residual path is correct for it.
#[test]
fn ac5_no_native_scalar_reaches_the_residual_custom_path() {
    let scalars = [
        CqlType::Boolean,
        CqlType::TinyInt,
        CqlType::SmallInt,
        CqlType::Int,
        CqlType::BigInt,
        CqlType::Counter,
        CqlType::Float,
        CqlType::Double,
        CqlType::Decimal,
        CqlType::Text,
        CqlType::Ascii,
        CqlType::Varchar,
        CqlType::Blob,
        CqlType::Timestamp,
        CqlType::Date,
        CqlType::Time,
        CqlType::Uuid,
        CqlType::TimeUuid,
        CqlType::Inet,
        CqlType::Duration,
        CqlType::Varint,
    ];
    for cql in scalars {
        let c = ComparatorType::from_cql_type(&cql).expect("comparator for native scalar");
        if let ComparatorType::Custom(name) = &c {
            assert!(
                name == "inet" || name == "time",
                "native scalar {:?} reaches the residual Custom(\"{}\") string-compare path",
                cql,
                name
            );
        }
    }
}

/// `supports_ordering()` for the scalars that may actually OCCUPY an ordering
/// position — deliberately a SEPARATE audit from AC5 above, and deliberately not
/// a blanket "every scalar orders" assertion (roborev job 44).
///
/// The two questions are different and only one of them is AC5's: "does this type
/// avoid the residual string-compare path" is about #3790's fix, while "may this
/// type sit in an ordering position" is about the predicate's own contract. An
/// earlier version asserted the second over EVERY scalar in one loop, which
/// CODIFIED `ComparatorType::Duration::supports_ordering() == true` — behaviour
/// this issue does not fix and should not freeze.
///
/// **`Duration` is excluded, and the reason is not obvious.** At the marshal level
/// `DurationType` really is byte-comparable — pinned `cassandra-5.0.8`,
/// `src/java/org/apache/cassandra/db/marshal/DurationType.java:46`:
/// `super(ComparisonType.BYTE_ORDER);`. But CQL FORBIDS a `duration` in any
/// ordering position: it cannot be a partition or clustering key, nor a set
/// element or map key. That is what `DurationType.referencesDuration()` (line 96)
/// exists to let the validation layer detect and reject.
///
/// So the predicate is ambiguous: under "Cassandra defines a byte order for this
/// type" `Duration` is `true`; under "this type may occupy an ordering position"
/// it is `false`. #3790 adopted the SECOND reading for `inet`/`time` (both are
/// legal clustering-column types, which is why they now report `true`), and under
/// that reading `Duration` reporting `true` is inconsistent. Resolving it means
/// changing a shared predicate for an unrelated type on a decision this issue did
/// not make, so it is filed as **#3917** rather than changed here (1:1:1:1).
/// This test therefore asserts only what #3790 settled and stays silent on
/// `Duration` rather than pinning either answer, so #3917 can decide either way
/// without fighting a test.
#[test]
fn orderable_native_scalars_report_supports_ordering() {
    let orderable = [
        CqlType::Boolean,
        CqlType::TinyInt,
        CqlType::SmallInt,
        CqlType::Int,
        CqlType::BigInt,
        CqlType::Counter,
        CqlType::Float,
        CqlType::Double,
        CqlType::Decimal,
        CqlType::Text,
        CqlType::Ascii,
        CqlType::Varchar,
        CqlType::Blob,
        CqlType::Timestamp,
        CqlType::Date,
        CqlType::Time,
        CqlType::Uuid,
        CqlType::TimeUuid,
        CqlType::Inet,
        CqlType::Varint,
    ];
    for cql in orderable {
        let c = ComparatorType::from_cql_type(&cql).expect("comparator for native scalar");
        assert!(
            c.supports_ordering(),
            "native scalar {:?} may occupy an ordering position and must report \
             supports_ordering() == true",
            cql
        );
    }
}

/// The residual path is UNCHANGED for a genuinely unknown/unresolved custom
/// name: it still orders by the formatted string.
#[test]
fn residual_custom_name_still_orders_by_formatted_string() {
    let c = ComparatorType::Custom("udt:Address".to_string());
    let a = Value::text("aaa".to_string());
    let b = Value::text("bbb".to_string());
    assert_eq!(c.compare(&a, &b).unwrap(), Ordering::Less);
    assert_eq!(c.compare(&b, &a).unwrap(), Ordering::Greater);
}
