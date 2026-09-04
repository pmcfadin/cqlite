//! DIFFERENTIAL pin between the writer's TWO sorted-collection comparators
//! (issue #3935).
//!
//! CQLite implements one Cassandra rule — "a `SetType`/`MapType` orders by the
//! element/key `AbstractType.compare`" — TWICE, in two places, from two different
//! inputs:
//!
//! * [`compare_collection_elements`] (`collection_order`), dispatched on the
//!   `Value` VARIANT, serving the non-frozen complex-cell path (`complex.rs`) and
//!   `encoding.rs`'s frozen `serialize_value`;
//! * [`compare_for_marshal`] (`marshal_comparator`), dispatched on the DECLARED
//!   MARSHAL string, serving `udt_canon`'s canonicalization of a frozen UDT's
//!   `SetType`/`MapType` field.
//!
//! **Both carried the same `time` defect and neither noticed.** Each classified
//! `TimeType` as SIGNED, beside `TimestampType`; `TimeType` is
//! `ComparisonType.BYTE_ORDER` (pinned `cassandra-5.0.8`,
//! `db/marshal/TimeType.java:48`). They were fixed in the SAME issue, from the
//! same authority, and nothing in the tree compared them — so the fix for one
//! could have landed without the other, which is exactly what happened for the
//! first two rounds of #3935. A second implementation's agreement is only knowable
//! by TESTING it, never by care.
//!
//! # What this asserts, and what it deliberately does not
//!
//! For every type BOTH comparators can order, and every ORDERED PAIR of a value
//! table carrying negatives and boundary values, the two must return the SAME
//! `Ordering`. That is a CONSISTENCY property between two CQLite implementations —
//! it is NOT an oracle for Cassandra's rule (both could be wrong together; #3042's
//! lesson one level up). The per-type Cassandra oracle lives elsewhere:
//! `collection_order::tests`, `issue_3935_collection_time_byte_order.rs`,
//! `issue_1295_signed_collection_order_parity.rs` and
//! `issue_3790_collection_order_cassandra_golden.rs`. This file's job is only to
//! make a FUTURE one-sided change impossible to land silently.
//!
//! The table is DERIVED, not curated per case: adding a `(marshal, values)` row
//! extends the differential automatically, and a marshal `classify_comparator`
//! does not support is a hard FAIL rather than a skip — a row that quietly stops
//! being compared is the vacuous pass this file exists to prevent.
//!
//! # RED-verified, and the blind spot MEASURED rather than merely declared
//!
//! Four trees, measured in this lane over these 3 cases:
//!
//! | tree | result |
//! |---|---|
//! | both comparators corrected (HEAD) | 3 passed |
//! | ONE-SIDED revert, marshal side (`TimeType` back to `SignedInt`) | 2 FAILED |
//! | ONE-SIDED revert, variant side (`time` arm back to `x.cmp(y)`) | 2 FAILED |
//! | BOTH reverted — the pre-#3935 state | **differential PASSES**, 1 FAILED |
//!
//! Either one-sided revert reds `both_writer_comparators_agree_on_every_orderable_type`
//! (naming the disagreeing pair), which is the property this file was asked for: a
//! future fix to one comparator alone cannot land silently.
//!
//! The last row is the DECLARED BLIND SPOT, measured instead of asserted: with both
//! implementations wrong in the SAME way the differential is green, and only
//! `time_is_byte_ordered_and_timestamp_signed_in_both_comparators` — which compares
//! against the PINNED CASSANDRA RULE, not against the other implementation — reds.
//! That is why both cases live here and why neither is sufficient alone.

#![allow(unused_imports)]

use super::super::marshal_comparator::{classify_comparator, compare_for_marshal};
use super::super::*;
use crate::types::Value;
use std::cmp::Ordering;

/// One row of the differential table: a declared marshal plus values of the
/// `Value` variant that marshal declares.
struct Row {
    marshal: &'static str,
    values: Vec<Value>,
}

fn marshal(name: &str) -> String {
    format!("org.apache.cassandra.db.marshal.{name}")
}

/// Every type both comparators can order, with negatives and boundary values.
///
/// `time` and `timestamp` carry the SAME numeric sequence on purpose: they
/// serialize identically (8-byte big-endian) and order DIFFERENTLY (BYTE_ORDER vs
/// signed), so a row that mixed them up, or a comparator that keyed on the
/// serialized shape instead of the declared type, shows up as a disagreement.
fn table() -> Vec<Row> {
    let time_like = vec![
        Value::Time(0),
        Value::Time(1),
        Value::Time(-1),
        Value::Time(-1_000_000_000),
        Value::Time(86_399_999_999_999), // DAYS.toNanos(1) - 1, largest valid
        Value::Time(i64::MIN),
        Value::Time(i64::MAX),
    ];
    let timestamp_like = time_like
        .iter()
        .map(|v| match v {
            Value::Time(n) => Value::Timestamp(*n),
            other => other.clone(),
        })
        .collect();
    vec![
        Row {
            marshal: "TimeType",
            values: time_like,
        },
        Row {
            marshal: "TimestampType",
            values: timestamp_like,
        },
        Row {
            marshal: "SimpleDateType",
            values: vec![
                Value::Date(0),
                Value::Date(1),
                Value::Date(-1),
                Value::Date(i32::MIN),
                Value::Date(i32::MAX),
            ],
        },
        Row {
            marshal: "Int32Type",
            values: vec![
                Value::Integer(0),
                Value::Integer(1),
                Value::Integer(-1),
                Value::Integer(i32::MIN),
                Value::Integer(i32::MAX),
            ],
        },
        Row {
            marshal: "LongType",
            values: vec![
                Value::BigInt(0),
                Value::BigInt(1),
                Value::BigInt(-1),
                Value::BigInt(i64::MIN),
                Value::BigInt(i64::MAX),
            ],
        },
        Row {
            marshal: "CounterColumnType",
            values: vec![
                Value::Counter(0),
                Value::Counter(-1),
                Value::Counter(i64::MIN),
                Value::Counter(i64::MAX),
            ],
        },
        Row {
            marshal: "ByteType",
            values: vec![
                Value::TinyInt(0),
                Value::TinyInt(-1),
                Value::TinyInt(i8::MIN),
                Value::TinyInt(i8::MAX),
            ],
        },
        Row {
            marshal: "ShortType",
            values: vec![
                Value::SmallInt(0),
                Value::SmallInt(-1),
                Value::SmallInt(i16::MIN),
                Value::SmallInt(i16::MAX),
            ],
        },
        Row {
            marshal: "BooleanType",
            values: vec![Value::Boolean(false), Value::Boolean(true)],
        },
        Row {
            marshal: "UTF8Type",
            values: vec![
                Value::text(""),
                Value::text("a"),
                Value::text("ab"),
                Value::text("b"),
                Value::text("é"),
            ],
        },
        Row {
            marshal: "AsciiType",
            values: vec![Value::text("A"), Value::text("Z"), Value::text("a")],
        },
        Row {
            marshal: "BytesType",
            values: vec![
                Value::blob(vec![]),
                Value::blob(vec![0x00]),
                Value::blob(vec![0x00, 0x00]),
                Value::blob(vec![0x7F]),
                Value::blob(vec![0xFF]),
            ],
        },
        Row {
            marshal: "InetAddressType",
            values: vec![
                // Mixed families ON PURPOSE: `compareUnsigned` is lexicographic
                // with length only as a prefix tiebreak, so the 16-byte `::1`
                // sorts BELOW the 4-byte `9.0.0.1`. A length-first comparator on
                // either side would disagree here.
                Value::inet(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
                Value::inet(vec![9, 0, 0, 1]),
                Value::inet(vec![10, 0, 0, 1]),
                Value::inet(vec![255, 255, 255, 255]),
            ],
        },
    ]
}

/// THE DIFFERENTIAL: the two comparators must agree on every ordered pair of
/// every row.
#[test]
fn both_writer_comparators_agree_on_every_orderable_type() {
    let rows = table();
    // CASE FLOOR: an emptied or silently-shrunk table would pass vacuously.
    assert!(
        rows.len() >= 13,
        "differential table lost rows ({} < 13) — a shrunk table passes vacuously",
        rows.len()
    );
    for required in ["TimeType", "TimestampType", "SimpleDateType", "Int32Type"] {
        assert!(
            rows.iter().any(|r| r.marshal == required),
            "{required} must stay in the differential table"
        );
    }

    let mut compared = 0usize;
    for row in &rows {
        let ty = marshal(row.marshal);
        // FAIL-CLOSED, never skip: a marshal whose comparator is unsupported can
        // no longer be compared, and dropping it silently is the vacuous pass.
        assert!(
            classify_comparator(&ty).is_some(),
            "{ty} has no comparator in marshal_comparator, so this row would be \
             silently dropped from the differential"
        );
        assert!(
            row.values.len() >= 2,
            "{ty}: a row needs >= 2 values to form a pair"
        );
        let bytes: Vec<Vec<u8>> = row
            .values
            .iter()
            .map(|v| serialize_value(v).unwrap_or_else(|e| panic!("serialize {v:?}: {e}")))
            .collect();

        for (i, a) in row.values.iter().enumerate() {
            for (j, b) in row.values.iter().enumerate() {
                let by_marshal = compare_for_marshal(&ty, a, &bytes[i], b, &bytes[j])
                    .unwrap_or_else(|e| panic!("{ty}: compare_for_marshal({a:?}, {b:?}): {e}"));
                let by_variant = compare_collection_elements(a, b);
                assert_eq!(
                    by_marshal, by_variant,
                    "issue #3935: the two writer sorted-collection comparators \
                     DISAGREE for {ty} on ({a:?}, {b:?}) — marshal-driven says \
                     {by_marshal:?}, variant-driven says {by_variant:?}. One of \
                     them is wrong about Cassandra; fix BOTH, and pin the correct \
                     order against the pinned tag, never against the other."
                );
                compared += 1;
            }
        }
    }
    // The count is the work: a table that compared nothing must not pass.
    assert!(
        compared >= 200,
        "expected a substantial pair count, compared only {compared}"
    );
}

/// The differential above compares two comparators; this case proves the table it
/// runs over can actually SEE a disagreement, by checking that at least one row
/// yields a NON-trivial ordering in each direction. Without it, a table of
/// all-equal values would satisfy the differential while discriminating nothing.
#[test]
fn differential_table_exercises_both_orderings() {
    for row in &table() {
        let ty = marshal(row.marshal);
        let bytes: Vec<Vec<u8>> = row
            .values
            .iter()
            .map(|v| serialize_value(v).expect("serialize"))
            .collect();
        let mut saw_less = false;
        let mut saw_greater = false;
        for (i, a) in row.values.iter().enumerate() {
            for (j, b) in row.values.iter().enumerate() {
                match compare_for_marshal(&ty, a, &bytes[i], b, &bytes[j]).expect("compare") {
                    Ordering::Less => saw_less = true,
                    Ordering::Greater => saw_greater = true,
                    Ordering::Equal => {}
                }
            }
        }
        assert!(
            saw_less && saw_greater,
            "{ty}: the row's values must produce BOTH Less and Greater, else the \
             differential cannot discriminate for this type"
        );
    }
}

/// REGRESSION PIN, stated as the property #3935 is about: the two temporal types
/// must DISAGREE with each other on a negative operand, in BOTH comparators.
///
/// A one-sided revert (`TimeType` back to signed on either side) makes that
/// comparator answer `Less` for `time`, so this case reds independently of the
/// differential above — which by construction cannot tell "both correct" from
/// "both wrong in the same way".
///
/// Authority (never a CQLite `file:line`, #3041), pinned `cassandra-5.0.8`:
/// `db/marshal/TimeType.java:48` `super(ComparisonType.BYTE_ORDER)` vs
/// `db/marshal/TimestampType.java:56` `super(ComparisonType.CUSTOM)` whose
/// `compareCustom` delegates to `LongType.compareLongs` (SIGNED).
#[test]
fn time_is_byte_ordered_and_timestamp_signed_in_both_comparators() {
    let time_ty = marshal("TimeType");
    let ts_ty = marshal("TimestampType");
    let neg = -1_i64;
    let zero = 0_i64;
    let neg_bytes = neg.to_be_bytes().to_vec();
    let zero_bytes = zero.to_be_bytes().to_vec();
    // The two types serialize IDENTICALLY, so only the declared type can
    // distinguish them — asserted so the case rests on bytes, not on memory.
    assert_eq!(
        serialize_value(&Value::Time(neg)).expect("serialize time"),
        serialize_value(&Value::Timestamp(neg)).expect("serialize timestamp")
    );

    // `time`: BYTE_ORDER — 0xFF.. sorts ABOVE 0x00..
    assert_eq!(
        compare_for_marshal(
            &time_ty,
            &Value::Time(neg),
            &neg_bytes,
            &Value::Time(zero),
            &zero_bytes
        )
        .expect("compare time"),
        Ordering::Greater
    );
    assert_eq!(
        compare_collection_elements(&Value::Time(neg), &Value::Time(zero)),
        Ordering::Greater
    );

    // `timestamp`: SIGNED — a pre-epoch millis sorts BELOW zero.
    assert_eq!(
        compare_for_marshal(
            &ts_ty,
            &Value::Timestamp(neg),
            &neg_bytes,
            &Value::Timestamp(zero),
            &zero_bytes
        )
        .expect("compare timestamp"),
        Ordering::Less
    );
    assert_eq!(
        compare_collection_elements(&Value::Timestamp(neg), &Value::Timestamp(zero)),
        Ordering::Less
    );
}
