//! Unit coverage for the Parquet↔JSONL parity harness's normalizers and type
//! expectations — issue #1490 (AD1), epic #1469.
//!
//! Split out of `issue_1490_parquet_jsonl_parity.rs` under the campsite rule
//! (that file reached 1552 lines against the ~1500 test target, #1135) along a
//! SUBJECT seam: this file holds the cases that need NO corpus fixture and no
//! `ParityCase` declaration — the declared-type parser, the duration-spelling
//! normalizer, the CQL→Arrow type expectation, the accept-list/decoder
//! agreement, and the golden-side value canonicalizations. The corpus parity
//! cases and the negative controls that drive a REAL export stay in the parity
//! file, which is where a fixture is needed to make them mean anything.
//!
//! Split AGAIN, the same way, once this file itself reached 1839 lines: whether
//! a golden may be USED as an oracle at all — which generation's dump it is, the
//! text preparation's own refusals, and physical-dump ELIGIBILITY (#1742) — now
//! lives in `issue_1490_parquet_golden_admission.rs`. What is left here is the
//! NORMALIZERS and the TYPE expectation, i.e. how the two sides are compared
//! once both are admitted.
//!
//! These run in EVERY checkout, including one with no fetched corpus, so the
//! normalizers that decide equality are never untested exactly where the corpus
//! is thinnest.

#![cfg(feature = "state_machine")]

#[path = "support/parquet_parity/mod.rs"]
mod parquet_parity;

// ---------------------------------------------------------------------------
// The ONE entry point these unit tests go through
//
// `declared.rs` is the single declared-type-guided canonicalization door (three
// review rounds each found a different position that canonicalized WITHOUT the
// declared type, so the per-pass helpers this used to call are now private to
// it). These wrappers name the POSITION each unit test is about, so a test
// exercises exactly the path the harness does.
// ---------------------------------------------------------------------------

use parquet_parity::canonical_jsonl::CanonicalValue as CV;
use parquet_parity::cql_type::CqlTypeSpec;
use parquet_parity::declared::{
    canonicalize_arrow, canonicalize_arrow_decimal, canonicalize_golden, Declared,
};

/// A raw golden value at the top-level CELL position.
fn golden_cell(v: CV, spec: &CqlTypeSpec) -> Result<CV, String> {
    canonicalize_golden(v, &Declared::cell(spec, "unit test"))
}

/// The same, for the cases whose value cannot be refused.
fn golden_cell_ok(v: CV, spec: &CqlTypeSpec) -> CV {
    golden_cell(v, spec).expect("this value must canonicalize")
}

/// An exported Arrow cell at the CELL position.
fn arrow_cell(
    array: &dyn arrow::array::Array,
    row: usize,
    spec: &CqlTypeSpec,
) -> Result<CV, String> {
    canonicalize_arrow(array, row, &Declared::cell(spec, "unit test"))
}

/// An exported `Decimal128` cell — the declared type is what decides `varint`
/// (integer domain) from `decimal` (exact unscaled/scale pair), including at
/// scale zero (issue #1490 round 7).
fn arrow_decimal(unscaled: i128, scale: i8, spec: &CqlTypeSpec) -> Result<CV, String> {
    canonicalize_arrow_decimal(unscaled, scale, &Declared::cell(spec, "unit test"))
}

// ---------------------------------------------------------------------------
// Unit coverage for the two normalization pieces
//
// These run in EVERY checkout, including one with no fetched corpus, where the
// only `duration` column in the case list (`test_basic.simple_table`) skips —
// otherwise the parser that decides duration equality would be untested exactly
// where the corpus is thinnest.
// ---------------------------------------------------------------------------

/// The two writers' spellings of the SAME duration must normalize to the same
/// (months, days, nanos) triple.
#[test]
fn duration_spellings_normalize_to_the_same_value() {
    use parquet_parity::spelling::parse_duration;

    // sstabledump's decomposed spelling vs the ValueFormatter's nanos spelling,
    // taken verbatim from a `test_basic.simple_table` row.
    assert_eq!(
        parse_duration("50m33s", "test").expect("cassandra spelling"),
        (0, 0, 3_033_000_000_000)
    );
    assert_eq!(
        parse_duration("3033000000000ns", "test").expect("cqlite spelling"),
        (0, 0, 3_033_000_000_000)
    );
    // Month/day components, and the units only one writer emits.
    assert_eq!(
        parse_duration("1y2mo3w4d5h6m7s8ms9us10ns", "test").expect("full grammar"),
        (
            14,
            25,
            5 * 3_600_000_000_000
                + 6 * 60_000_000_000
                + 7 * 1_000_000_000
                + 8 * 1_000_000
                + 9 * 1_000
                + 10
        )
    );
    // Both negative spellings: Cassandra's single leading sign vs the
    // ValueFormatter's per-component signs.
    assert_eq!(
        parse_duration("-1mo2d", "test").expect("cassandra negative"),
        (-1, -2, 0)
    );
    assert_eq!(
        parse_duration("-1mo-2d", "test").expect("cqlite negative"),
        (-1, -2, 0)
    );
    assert_eq!(parse_duration("0ns", "test").expect("zero"), (0, 0, 0));
}

/// A malformed or unknown-unit duration must ERROR — never normalize to
/// something that quietly compares unequal for an unexplained reason.
#[test]
fn duration_parser_rejects_malformed_spellings() {
    use parquet_parity::spelling::parse_duration;

    for bad in ["", "33", "ns", "1x", "1mo?", "-", "1 mo"] {
        assert!(
            parse_duration(bad, "test").is_err(),
            "{bad:?} must be rejected, not normalized"
        );
    }
}

/// The declared-type parser must REFUSE an unrecognized type rather than fall
/// back to comparing by JSON shape, which would silently weaken the oracle.
#[test]
fn declared_type_parser_refuses_unknown_types() {
    use parquet_parity::cql_type::parse_column;

    assert!(parse_column("c", "int", &[]).is_ok());
    assert!(
        parse_column("c", "SET<Text>", &[]).is_ok(),
        "case-insensitive"
    );
    assert!(parse_column("c", "frozen<list<frozen<person>>>", &["person"]).is_ok());
    // A UDT that the case did not declare, and a type that does not exist.
    let err =
        parse_column("c", "frozen<person>", &[]).expect_err("an undeclared UDT must be refused");
    assert!(err.contains("person"), "{err}");
    assert!(parse_column("c", "quaternion", &[]).is_err());
    assert!(
        parse_column("c", "map<int>", &[]).is_err(),
        "map needs 2 params"
    );
    assert!(parse_column("c", "set<int", &[]).is_err(), "unbalanced");
}

// ---------------------------------------------------------------------------
// Unit coverage for the Arrow TYPE expectation
//
// Value canonicalization folds every integer width into one `Int`, so the type
// check is the ONLY thing standing between a wrong CQL→Arrow mapping and a green
// suite. These cases prove it both accepts the faithful mapping and REJECTS a
// mis-width — the guard has to have been seen to red.
// ---------------------------------------------------------------------------

/// Every declared scalar in the corpus maps to exactly one faithful Arrow type.
#[test]
fn expected_arrow_type_pins_each_scalar() {
    use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
    use parquet_parity::arrow_expect::{expected_shape, ShapeVerdict};
    use parquet_parity::cql_type::parse_column;

    // The verdict is THREE-valued (issue #1490): these cases assert the two
    // AFFIRMATIVE ones by name, never `!= Valid` — "not valid" would also be
    // satisfied by `Unmeasurable`, i.e. by the harness admitting it did not
    // measure, which is exactly the state that must not pass for a scalar.
    let verdict = |declared: &str, actual: &DataType| -> ShapeVerdict {
        let col = parse_column("c", declared, &[]).expect("declared type must parse");
        expected_shape(&col.spec)
            .expect("every corpus scalar has a declared expectation")
            .check(actual)
    };

    for (declared, expected) in [
        ("boolean", DataType::Boolean),
        ("tinyint", DataType::Int8),
        ("smallint", DataType::Int16),
        ("int", DataType::Int32),
        ("bigint", DataType::Int64),
        ("counter", DataType::Int64),
        ("float", DataType::Float32),
        ("double", DataType::Float64),
        ("text", DataType::Utf8),
        ("varchar", DataType::Utf8),
        ("ascii", DataType::Utf8),
        ("inet", DataType::Utf8),
        ("blob", DataType::Binary),
        ("uuid", DataType::FixedSizeBinary(16)),
        ("timeuuid", DataType::FixedSizeBinary(16)),
        ("date", DataType::Date32),
        ("time", DataType::Time64(TimeUnit::Nanosecond)),
        ("decimal", DataType::Decimal128(38, 9)),
        ("varint", DataType::Decimal128(38, 0)),
        ("duration", DataType::Utf8),
        ("duration", DataType::Interval(IntervalUnit::MonthDayNano)),
        (
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        ),
    ] {
        assert_eq!(
            verdict(declared, &expected),
            ShapeVerdict::Valid,
            "'{declared}' must accept {expected:?}"
        );
    }

    // The mis-width family this check exists for: a value round-trips
    // unchanged through any of these, so ONLY the type check can see it.
    for (declared, wrong) in [
        ("tinyint", DataType::Int64),
        ("tinyint", DataType::Int16),
        ("smallint", DataType::Int32),
        ("int", DataType::Int64),
        ("bigint", DataType::Int32),
        ("float", DataType::Float64),
        ("double", DataType::Float32),
        ("varint", DataType::Decimal128(38, 9)),
        ("varint", DataType::Int64),
        ("uuid", DataType::Utf8),
        ("blob", DataType::Utf8),
        ("date", DataType::Utf8),
        ("time", DataType::Utf8),
        ("boolean", DataType::Int8),
        ("inet", DataType::Binary),
        // A timestamp must be UTC epoch MILLIS, not a zone-less local one.
        (
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
        (
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
    ] {
        assert_eq!(
            verdict(declared, &wrong),
            ShapeVerdict::Wrong,
            "'{declared}' must REJECT {wrong:?} — a wrong width round-trips its values \
             unchanged, so nothing else in this harness can catch it"
        );
    }
}

/// A `Decimal128` NARROWER than the export's own precision 38 must be REJECTED
/// for `decimal` and `varint`, at the top level and nested.
///
/// The state this control exists for: the accept-list used to be `p <= 38`, so an
/// export regression narrowing `varint` from `Decimal128(38, 0)` to
/// `Decimal128(9, 0)` stayed `Valid` for as long as every value in the CURRENT
/// fixture fitted — and the values then compared EQUAL, because they do fit. The
/// exported schema can no longer represent the declared domain, and the type
/// check is the only stage that can say so: "the fixture's values fit" is not
/// "the schema is right" (issue #1490).
#[test]
fn decimal128_narrower_precision_than_38_is_rejected() {
    use arrow::datatypes::{DataType, Field};
    use parquet_parity::arrow_expect::{expected_shape, ShapeVerdict, DECIMAL128_EXPORT_PRECISION};
    use parquet_parity::cql_type::parse_column;
    use std::sync::Arc;

    assert_eq!(
        DECIMAL128_EXPORT_PRECISION, 38,
        "the export writes decimal/varint at Decimal128(38, _); this control is about that pin"
    );

    let verdict = |declared: &str, actual: &DataType| -> ShapeVerdict {
        let col = parse_column("c", declared, &[]).expect("declared type must parse");
        expected_shape(&col.spec)
            .expect("decimal/varint have a declared expectation")
            .check(actual)
    };
    let list_of = |t: DataType| DataType::List(Arc::new(Field::new("item", t, true)));

    // The pinned precision is accepted…
    for (declared, ok) in [
        ("decimal", DataType::Decimal128(38, 9)),
        ("varint", DataType::Decimal128(38, 0)),
    ] {
        assert_eq!(
            verdict(declared, &ok),
            ShapeVerdict::Valid,
            "'{declared}' must accept the export's own {ok:?}"
        );
    }

    // …and EVERY narrower one is Wrong, whatever the scale. Each of these holds
    // every value in today's fixtures, so no value comparison could red on it.
    for (declared, narrow) in [
        ("decimal", DataType::Decimal128(18, 9)),
        ("decimal", DataType::Decimal128(9, 9)),
        ("decimal", DataType::Decimal128(37, 9)),
        ("varint", DataType::Decimal128(9, 0)),
        ("varint", DataType::Decimal128(18, 0)),
        ("varint", DataType::Decimal128(37, 0)),
    ] {
        assert_eq!(
            verdict(declared, &narrow),
            ShapeVerdict::Wrong,
            "'{declared}' must REJECT the narrowed {narrow:?} — the current values still fit \
             it, so the type check is the only stage that can see the shrunken domain"
        );
    }

    // A wider-than-Decimal128 precision is not a valid alternative either: it is
    // not what the export writes, and `arrow_rows` decodes only `Decimal128`.
    assert_eq!(
        verdict("decimal", &DataType::Decimal128(39, 9)),
        ShapeVerdict::Wrong,
        "a precision above the type's own 38 must be REJECTED, not tolerated"
    );

    // The narrowing must be caught NESTED too — the recursion carries the same
    // expectation into a collection element.
    let nested = |declared: &str, actual: &DataType| -> ShapeVerdict {
        let col = parse_column("c", declared, &[]).expect("declared type must parse");
        expected_shape(&col.spec)
            .expect("collections of decimal/varint have a declared expectation")
            .check(actual)
    };
    assert_eq!(
        nested("list<varint>", &list_of(DataType::Decimal128(38, 0))),
        ShapeVerdict::Valid,
        "list<varint> must accept the export's own Decimal128(38, 0) element"
    );
    assert_eq!(
        nested("list<varint>", &list_of(DataType::Decimal128(9, 0))),
        ShapeVerdict::Wrong,
        "list<varint> must REJECT a narrowed element precision"
    );
}

/// Nested types are matched structurally: element/key/value types recurse, and a
/// UDT must be a `Struct` (#3556's `Utf8` flattening is exactly this check).
#[test]
fn expected_arrow_type_recurses_into_nested_types() {
    use arrow::datatypes::{DataType, Field, Fields};
    use parquet_parity::arrow_expect::{
        expected_shape, validate_field, FieldVerdict, ShapeVerdict,
    };
    use parquet_parity::cql_type::parse_column;
    use std::sync::Arc;

    let list_of = |t: DataType| DataType::List(Arc::new(Field::new("item", t, true)));
    let map_of = |k: DataType, v: DataType| {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", k, false),
                    Field::new("value", v, true),
                ])),
                false,
            )),
            false,
        )
    };
    let shape = |declared: &str, udts: &[&str]| {
        let col = parse_column("c", declared, udts).expect("declared type must parse");
        expected_shape(&col.spec).expect("expectation must be derivable")
    };

    let valid = |declared: &str, udts: &[&str], actual: &DataType| {
        assert_eq!(
            shape(declared, udts).check(actual),
            ShapeVerdict::Valid,
            "'{declared}' must accept {actual:?}"
        );
    };
    let wrong = |declared: &str, udts: &[&str], actual: &DataType| {
        assert_eq!(
            shape(declared, udts).check(actual),
            ShapeVerdict::Wrong,
            "'{declared}' must REJECT {actual:?}"
        );
    };

    valid("set<int>", &[], &list_of(DataType::Int32));
    valid("list<int>", &[], &list_of(DataType::Int32));
    // …but not a list of the wrong element width.
    wrong("set<int>", &[], &list_of(DataType::Int64));
    valid(
        "map<int, text>",
        &[],
        &map_of(DataType::Int32, DataType::Utf8),
    );
    wrong(
        "map<int, text>",
        &[],
        &map_of(DataType::Utf8, DataType::Utf8),
    );
    // A Utf8 rendering of a UDT is affirmatively WRONG — #3556's flattening, at
    // the top level and nested inside a frozen collection.
    wrong("frozen<person>", &["person"], &DataType::Utf8);
    wrong(
        "frozen<list<frozen<person>>>",
        &["person"],
        &list_of(DataType::Utf8),
    );

    // The mismatch message must name the column, the declared CQL type and both
    // Arrow types — it is what the #3556 known-gap signature pins.
    let col = parse_column("lp", "frozen<list<frozen<person>>>", &["person"]).expect("parses");
    let mismatch = match validate_field(&col, &list_of(DataType::Utf8)) {
        FieldVerdict::Mismatch(m) => m,
        other => panic!("a Utf8-flattened UDT must be a MISMATCH, got {other:?}"),
    };
    // The rendered ACTUAL type is a FIELD, so the known-type-gap record can
    // compare it by equality rather than by substring.
    assert_eq!(mismatch.actual, "list<utf8>");
    assert_eq!(mismatch.expected, "list<struct(udt 'person')>");
    let err = mismatch.to_string();
    assert!(
        err.contains("Arrow type mismatch for column 'lp' declared 'frozen<list<frozen<person>>>'")
            && err.contains("expected list<struct(udt 'person')>")
            && err.contains("got list<utf8>"),
        "{err}"
    );
}

// ---------------------------------------------------------------------------
// The accept-list must not be broader than the decoder
//
// `ArrowShape::accepts` decides which Arrow types the harness declares VALID;
// the declared-type-guided Arrow decode (`declared::canonicalize_arrow`) decides
// which it can DECODE. An accept-list
// that is broader declares a schema valid and then dies during value projection
// — a promise the harness cannot keep, and a confusing late failure instead of a
// clear early one. These two tests pin both halves of that agreement.
// ---------------------------------------------------------------------------

/// EVERY Arrow type the harness accepts for a declared CQL type must also be
/// DECODABLE, sample value and all.
#[test]
fn every_accepted_arrow_type_is_decodable() {
    use arrow::array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
        FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, IntervalMonthDayNanoArray, LargeBinaryArray, LargeStringArray, StringArray,
        Time64NanosecondArray, TimestampMillisecondArray,
    };
    use arrow::datatypes::DataType;
    use arrow::datatypes::IntervalMonthDayNano;
    use parquet_parity::arrow_expect::{expected_shape, ShapeVerdict};
    use parquet_parity::cql_type::parse_column;
    use std::sync::Arc;

    let cases: Vec<(&str, ArrayRef)> = vec![
        ("boolean", Arc::new(BooleanArray::from(vec![true]))),
        ("tinyint", Arc::new(Int8Array::from(vec![1i8]))),
        ("smallint", Arc::new(Int16Array::from(vec![1i16]))),
        ("int", Arc::new(Int32Array::from(vec![1i32]))),
        ("bigint", Arc::new(Int64Array::from(vec![1i64]))),
        ("counter", Arc::new(Int64Array::from(vec![1i64]))),
        ("float", Arc::new(Float32Array::from(vec![1.5f32]))),
        ("double", Arc::new(Float64Array::from(vec![1.5f64]))),
        ("text", Arc::new(StringArray::from(vec!["x"]))),
        // Both members of the text/blob accept-lists, not just the first.
        ("text", Arc::new(LargeStringArray::from(vec!["x"]))),
        ("varchar", Arc::new(StringArray::from(vec!["x"]))),
        ("ascii", Arc::new(StringArray::from(vec!["x"]))),
        ("inet", Arc::new(StringArray::from(vec!["127.0.0.1"]))),
        ("blob", Arc::new(BinaryArray::from(vec![&[1u8, 2][..]]))),
        (
            "blob",
            Arc::new(LargeBinaryArray::from(vec![&[1u8, 2][..]])),
        ),
        (
            "uuid",
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(vec![[0u8; 16]].into_iter())
                    .expect("16-byte uuid"),
            ),
        ),
        ("date", Arc::new(Date32Array::from(vec![19_000i32]))),
        (
            "time",
            Arc::new(Time64NanosecondArray::from(vec![1_000i64])),
        ),
        (
            "timestamp",
            Arc::new(
                TimestampMillisecondArray::from(vec![1_700_000_000_000i64])
                    .with_timezone("UTC".to_string()),
            ),
        ),
        (
            "decimal",
            Arc::new(
                Decimal128Array::from(vec![1_500_000_000i128])
                    .with_precision_and_scale(38, 9)
                    .expect("decimal128(38,9)"),
            ),
        ),
        (
            "varint",
            Arc::new(
                Decimal128Array::from(vec![7i128])
                    .with_precision_and_scale(38, 0)
                    .expect("decimal128(38,0)"),
            ),
        ),
        // BOTH accepted duration representations: the textual substitute…
        ("duration", Arc::new(StringArray::from(vec!["50m33s"]))),
        // …and the faithful Interval(MonthDayNano), whose decoder arm exists so
        // this accept-list is not broader than the decoder.
        (
            "duration",
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNano::new(1, 2, 3),
            ])),
        ),
    ];

    for (declared, array) in cases {
        let col = parse_column("c", declared, &[]).expect("declared type must parse");
        let shape = expected_shape(&col.spec).expect("expectation must be derivable");
        let dt: DataType = array.data_type().clone();
        assert_eq!(
            shape.check(&dt),
            ShapeVerdict::Valid,
            "'{declared}' must accept {dt:?} for this test to be about the decoder"
        );
        arrow_cell(array.as_ref(), 0, &col.spec).unwrap_or_else(|e| {
            panic!(
                "'{declared}' ACCEPTS {dt:?} but the decoder cannot project it ({e}) — an \
                 accept-list broader than the decoder is a promise the harness cannot keep"
            )
        });
    }
}

/// `LargeList` and `FixedSizeList` are REJECTED, because `arrow_rows` decodes
/// only `List`. (They used to be accepted, so the schema check declared such a
/// column valid and the run then died during value projection.)
#[test]
fn list_accept_list_is_narrowed_to_what_the_decoder_handles() {
    use arrow::datatypes::{DataType, Field};
    use parquet_parity::arrow_expect::{expected_shape, ShapeVerdict};
    use parquet_parity::cql_type::parse_column;
    use std::sync::Arc;

    let shape = |declared: &str| {
        let col = parse_column("c", declared, &[]).expect("declared type must parse");
        expected_shape(&col.spec).expect("expectation must be derivable")
    };
    let item = || Arc::new(Field::new("item", DataType::Int32, true));

    assert_eq!(
        shape("list<int>").check(&DataType::List(item())),
        ShapeVerdict::Valid
    );
    for wrong in [
        DataType::LargeList(item()),
        DataType::FixedSizeList(item(), 3),
    ] {
        assert_eq!(
            shape("list<int>").check(&wrong),
            ShapeVerdict::Wrong,
            "{wrong:?} must be REJECTED: arrow_rows has no decoder for it, so accepting it \
             would pass the schema check and then die during value projection"
        );
        assert_eq!(
            shape("set<int>").check(&wrong),
            ShapeVerdict::Wrong,
            "{wrong:?}"
        );
    }
}

/// An Arrow `Interval(MonthDayNano)` duration and the golden's duration TEXT
/// must canonicalize to the SAME value — the interval arm is only useful if it
/// lands in the form `spelling::normalize_spelling` puts the golden into.
#[test]
fn interval_duration_canonicalizes_to_the_golden_spelling() {
    use arrow::array::IntervalMonthDayNanoArray;
    use arrow::datatypes::IntervalMonthDayNano;
    use parquet_parity::canonical_jsonl::CanonicalValue;
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::spelling::normalize_spelling;

    let duration = parse_column("d", "duration", &[]).expect("duration parses");
    // 1 month, 2 days, 3 033 000 000 000 ns — Cassandra spells the nanos part
    // "50m33s", CQLite's ValueFormatter "3033000000000ns".
    let nanos = 3_033_000_000_000i64;
    let array = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(1, 2, nanos)]);
    let exported = arrow_cell(&array, 0, &duration.spec).expect("interval must decode");
    // An already-canonical triple must survive normalization unchanged.
    let exported = normalize_spelling(exported, &duration.spec, "test").expect("normalizes");

    for spelling in ["1mo2d50m33s", "1mo2d3033000000000ns"] {
        let golden = normalize_spelling(
            CanonicalValue::Text(spelling.to_string()),
            &duration.spec,
            "test",
        )
        .expect("golden duration text must normalize");
        assert_eq!(
            golden, exported,
            "the Interval decoder must land on the same canonical triple as {spelling:?}"
        );
    }
    // And a genuinely DIFFERENT duration must still differ — no tolerance.
    let other = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(1, 2, nanos + 1)]);
    assert_ne!(
        arrow_cell(&other, 0, &duration.spec).expect("interval must decode"),
        exported
    );
}

// ---------------------------------------------------------------------------
// EXACT decimals: no `f64` on either side (roborev rounds 4 and 10, #1490)
//
// The decimal comparison used to reduce both sides to a double
// (`unscaled as f64 / 10^scale`) under a `|unscaled| < 2^53` guard. That guard
// made the INTEGER conversion exact and so LOOKED like a bound on the error,
// while the scaling division re-introduced it: one-unit-apart decimals well
// inside the guard divide to the SAME double, so a corrupted `Decimal128` cell
// compared EQUAL. Both sides now carry the exact unscaled/scale pair
// (`parquet_parity::decimal`), and these tests are the permanent controls for
// that: the perturbation control below FAILS if the `f64` path ever returns.
//
// Round 4 fixed the EXPORT side and left the GOLDEN side receiving a double and
// RECOVERING a decimal from it. Round 10 removed that too, because recovery
// cannot work in principle: `0.100000000000000001` and `0.1` are the SAME
// `f64`, so the recovery canonicalized the first as the second and reported
// itself exact. The golden literal's TEXT is now preserved
// (`golden_text.rs`) and a decimal that still arrives as a double is
// REFUSED — `a_golden_decimal_that_lost_its_literal_is_refused` and
// `f64_colliding_decimal_literals_stay_distinct` are the controls.
//
// No corpus table currently carries a scale-0 decimal (measured: 2650 decimal
// cells across every golden, none integer-shaped), so that path is covered by a
// unit test over the two normalizers rather than by inventing a fixture.
// ---------------------------------------------------------------------------

/// A decimal whose golden literal has no fractional part must canonicalize to
/// the SAME canonical value as the exported `Decimal128(38, 9)` cell.
#[test]
fn whole_valued_decimal_canonicalizes_on_both_sides() {
    use parquet_parity::canonical_jsonl::CanonicalValue;
    use parquet_parity::cql_type::parse_column;

    let decimal = parse_column("d", "decimal", &[]).expect("decimal parses");
    let varint = parse_column("v", "varint", &[]).expect("varint parses");

    for whole in [0i128, 1, -1, 42, -31_595] {
        // Golden side: sstabledump writes a whole decimal as a JSON integer.
        let golden = golden_cell(CanonicalValue::Int(whole), &decimal.spec)
            .expect("a whole golden decimal is exact");
        // Export side: Decimal128(38, 9) holds whole * 10^9.
        let exported = arrow_decimal(whole * 1_000_000_000, 9, &decimal.spec)
            .expect("scale-9 decimal must canonicalize");
        assert_eq!(
            golden, exported,
            "a whole decimal {whole} must compare equal across the two sides"
        );
        assert_eq!(
            golden,
            CanonicalValue::Text(format!("decimal({whole})")),
            "the canonical form of a decimal is its EXACT normalized decimal text"
        );
    }

    // A fractional decimal is untouched by the rule, and still compares exactly.
    assert_eq!(
        golden_cell(golden_decimal_literal("31595.67"), &decimal.spec)
            .expect("a preserved golden decimal literal"),
        arrow_decimal(31_595_670_000_000, 9, &decimal.spec).expect("fractional decimal")
    );

    // varint is an integer domain on BOTH sides: it must stay an `Int`, or the
    // rule would turn a type confusion into a silent pass.
    assert_eq!(
        golden_cell(CanonicalValue::Int(7), &varint.spec).expect("varint is exact"),
        CanonicalValue::Int(7)
    );
    assert_eq!(
        arrow_decimal(7, 0, &varint.spec).expect("varint"),
        CanonicalValue::Int(7)
    );

    // The exact representation has NO `2^53` ceiling — that bound existed only
    // because the comparison went through a double. A decimal far beyond it now
    // compares exactly on both sides.
    let huge = 1i128 << 60;
    assert_eq!(
        golden_cell(CanonicalValue::Int(huge), &decimal.spec)
            .expect("a whole golden decimal of any magnitude is exact"),
        arrow_decimal(huge * 1_000_000_000, 9, &decimal.spec).expect("scale-9 decimal"),
        "a whole decimal beyond 2^53 must still compare equal to its exported form"
    );
}

/// The golden literals of `test_basic.simple_table.account_balance` must parse
/// to EXACTLY the decimal they spell, and to the exported cell for that value.
#[test]
fn golden_decimal_literals_parse_exactly() {
    use parquet_parity::cql_type::parse_column;

    let decimal = parse_column("d", "decimal", &[]).expect("decimal parses");
    // Literals copied verbatim from the committed sstabledump golden, with the
    // unscaled value the export's fixed scale-9 `Decimal128` holds for each.
    // `10576.6` is scale 1 and `10375.04` scale 2 in ONE column — which is why
    // the comparison is scale-NORMALIZED (see `decimal.rs`).
    for (literal, unscaled_at_scale_9) in [
        ("10375.04", 10_375_040_000_000i128),
        ("10576.6", 10_576_600_000_000),
        ("31595.67", 31_595_670_000_000),
        ("-10375.04", -10_375_040_000_000),
    ] {
        let golden = golden_cell(golden_decimal_literal(literal), &decimal.spec)
            .expect("a corpus decimal literal must parse exactly");
        assert_eq!(
            golden,
            parquet_parity::decimal::ExactDecimal::new(unscaled_at_scale_9, 9).canonical(),
            "the golden literal {literal} must parse to the decimal it spells"
        );
        assert_eq!(
            golden,
            arrow_decimal(unscaled_at_scale_9, 9, &decimal.spec).expect("scale-9 decimal"),
            "the literal {literal} must equal the exported cell"
        );
    }

    // A signed zero is not a decimal attribute: `BigDecimal` has no negative
    // zero, so `-0.0` and `0.0` both denote the decimal 0.
    for zero in ["0.0", "-0.0"] {
        assert_eq!(
            golden_cell(golden_decimal_literal(zero), &decimal.spec).expect("zero must parse"),
            arrow_decimal(0, 9, &decimal.spec).expect("scale-9 zero")
        );
    }
}

/// A ONE-UNIT `Decimal128` perturbation the old `f64` path could not see must
/// now be reported — the sensitivity control for this whole representation.
///
/// The pair is chosen to be invisible to the OLD comparison and inside its
/// `2^53` guard, so it also demonstrates why that guard was never a bound:
/// `9_007_199_254_740_001` and `…002` at scale 9 are one unit apart, both below
/// `2^53`, and `unscaled as f64 / 10^9` maps them onto the SAME double.
#[test]
fn a_one_unit_decimal_perturbation_the_old_f64_path_collapsed_is_detected() {
    use parquet_parity::canonical_jsonl::CanonicalValue;

    use parquet_parity::cql_type::parse_column;
    let decimal = parse_column("d", "decimal", &[]).expect("decimal parses");
    let unscaled = 9_007_199_254_740_001i128;
    let perturbed = unscaled + 1;

    // The premise: the old rendering collapsed the two, INSIDE its own guard.
    assert!(
        perturbed.unsigned_abs() < (1u128 << 53),
        "inside the 2^53 guard"
    );
    assert_eq!(
        (unscaled as f64 / 1e9).to_bits(),
        (perturbed as f64 / 1e9).to_bits(),
        "premise of this control: the f64 path could not distinguish the two"
    );

    // The exact representation distinguishes them, and names both values.
    let exported = arrow_decimal(unscaled, 9, &decimal.spec).expect("scale-9 decimal");
    let corrupted = arrow_decimal(perturbed, 9, &decimal.spec).expect("scale-9 decimal");
    assert_ne!(
        exported, corrupted,
        "a one-unit Decimal128 corruption must be reported, not absorbed"
    );
    assert_eq!(
        exported,
        CanonicalValue::Text("decimal(9007199.254740001)".to_string())
    );
    assert_eq!(
        corrupted,
        CanonicalValue::Text("decimal(9007199.254740002)".to_string())
    );

    // The GOLDEN side distinguishes them too, because it reads the LITERAL:
    // both are scale-9 literals inside the export's fixed scale, and they are the
    // same double, so a double-mediated golden side could only have refused them
    // (round 4) or, worse, collapsed them.
    let golden_a = golden_cell(golden_decimal_literal("9007199.254740001"), &decimal.spec)
        .expect("a preserved literal is exact whatever its double does");
    let golden_b = golden_cell(golden_decimal_literal("9007199.254740002"), &decimal.spec)
        .expect("a preserved literal is exact whatever its double does");
    assert_eq!(golden_a, exported);
    assert_eq!(golden_b, corrupted);
    assert_ne!(
        golden_a, corrupted,
        "the golden literal must not compare equal to a one-unit-corrupted export"
    );
}

/// Scale NORMALIZATION is exact: `1.10` and `1.1` denote one rational and
/// compare equal, while distinct rationals stay distinct.
#[test]
fn decimal_scale_normalization_is_exact() {
    use parquet_parity::decimal::ExactDecimal;

    assert_eq!(ExactDecimal::new(110, 2), ExactDecimal::new(11, 1));
    assert_eq!(ExactDecimal::new(110, 2).text(), "1.1");
    assert_eq!(ExactDecimal::new(0, 9).text(), "0");
    assert_eq!(ExactDecimal::new(-1, 9).text(), "-0.000000001");
    assert_eq!(ExactDecimal::new(1, 9).text(), "0.000000001");
    assert_eq!(ExactDecimal::from_i128(-42).text(), "-42");
    // One digit apart at the same scale is NOT equal, at any magnitude.
    assert_ne!(ExactDecimal::new(111, 2), ExactDecimal::new(11, 1));
    assert_ne!(
        ExactDecimal::new(i128::MAX, 9),
        ExactDecimal::new(i128::MAX - 1, 9)
    );

    // The canonical form is TAGGED, so a `decimal` can never compare equal to a
    // `text` column holding the same digits — the type stays load-bearing.
    use parquet_parity::canonical_jsonl::CanonicalValue;
    assert_ne!(
        ExactDecimal::new(11, 1).canonical(),
        CanonicalValue::Text("1.1".to_string())
    );
}

/// The harness must REFUSE, never round, when a decimal cannot be compared
/// exactly: a scale beyond the recovery bound, and a literal with more
/// fractional digits than the export can carry.
#[test]
fn decimal_comparison_refuses_what_it_cannot_compare_exactly() {
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::decimal::{exact_from_text, EXPORT_DECIMAL_SCALE};

    let decimal = parse_column("d", "decimal", &[]).expect("decimal parses");

    // A scale beyond the fixed scale the export is declared to write: the
    // harness refuses rather than compare against a scale it cannot account for.
    let err = arrow_decimal(1, 12, &decimal.spec).expect_err("scale 12 must be refused");
    assert!(err.contains("exceeds"), "got: {err}");
    assert!(
        arrow_decimal(1, -1, &decimal.spec).is_err(),
        "negative scale"
    );

    // A literal with more fractional digits than the export's fixed scale: the
    // export refuses to truncate it, and the golden side refuses to round it.
    let err = exact_from_text("0.0000000001", EXPORT_DECIMAL_SCALE, "golden")
        .expect_err("a literal beyond the export scale must be refused");
    assert!(err.contains("fractional digits"), "got: {err}");

    // Exponent notation and a non-numeric literal are refusals, not guesses.
    for bad in ["1e9", "0x10", "", "1.2.3", "NaN"] {
        assert!(
            exact_from_text(bad, EXPORT_DECIMAL_SCALE, "golden").is_err(),
            "{bad:?} must be refused"
        );
    }
}

/// A golden `decimal` cell as the harness receives it: its LITERAL, preserved
/// by `golden_text::preserve_exact_lexemes` before the shared comparator
/// could turn it into an `f64` (round 10).
/// `the_rewrite_preserves_exact_lexemes_and_nothing_else` in
/// `issue_1490_parquet_declaration_and_keys.rs` pins that this really is the
/// shape a `decimal` cell arrives in.
fn golden_decimal_literal(literal: &str) -> parquet_parity::canonical_jsonl::CanonicalValue {
    parquet_parity::canonical_jsonl::CanonicalValue::Text(literal.to_string())
}

// ---------------------------------------------------------------------------
// A FROZEN map: sstabledump writes a JSON object, Arrow reads back a Map
//
// The corpus's only frozen maps (`fm`, `ma` on
// `test_compactionparityudt.udt_collections`) sit behind the #3556 whole-case
// gap and never reach the value comparison, so the conversion is covered by unit
// tests over the normalizer rather than by inventing a fixture. Without it those
// two columns would report a FALSE value difference the day #3556 is fixed.
// ---------------------------------------------------------------------------

/// A frozen map's golden JSON object must canonicalize to the SAME canonical
/// value the Arrow `Map` side produces — including the KEY coercion, which is
/// driven by the declared key type and never applied blindly.
#[test]
fn frozen_map_golden_object_canonicalizes_to_a_map() {
    use parquet_parity::canonical_jsonl::CanonicalValue;
    use parquet_parity::cql_type::parse_column;

    let text = |s: &str| CanonicalValue::Text(s.to_string());

    // `frozen<map<text,int>>` — the shape of `udt_collections.fm`.
    let fm = parse_column("fm", "frozen<map<text,int>>", &[]).expect("parses");
    let golden = CanonicalValue::Tuple(vec![
        ("a".to_string(), CanonicalValue::Int(1)),
        ("b".to_string(), CanonicalValue::Int(2)),
    ]);
    // What the declared-type-guided Arrow decode builds from an Arrow Map.
    let exported = CanonicalValue::Map(vec![
        (text("a"), CanonicalValue::Int(1)),
        (text("b"), CanonicalValue::Int(2)),
    ]);
    assert_eq!(golden_cell_ok(golden, &fm.spec), exported);

    // `frozen<map<text, frozen<address>>>` — the shape of `udt_collections.ma`:
    // the VALUE stays a Tuple (a UDT really is a struct), the OUTER object
    // becomes a Map, and a null inner field folds to Absent as everywhere else.
    let ma =
        parse_column("ma", "frozen<map<text, frozen<address>>>", &["address"]).expect("parses");
    let golden = CanonicalValue::Tuple(vec![(
        "home".to_string(),
        CanonicalValue::Tuple(vec![
            ("city".to_string(), text("Austin")),
            ("zip".to_string(), CanonicalValue::Null),
        ]),
    )]);
    assert_eq!(
        golden_cell_ok(golden, &ma.spec),
        CanonicalValue::Map(vec![(
            text("home"),
            CanonicalValue::Tuple(vec![
                ("city".to_string(), text("Austin")),
                ("zip".to_string(), CanonicalValue::Absent),
            ]),
        )])
    );

    // An INTEGRAL key arrives as the JSON object key STRING `"1"`; the declared
    // key type is what coerces it back, matching the Arrow Int32 key.
    let mi = parse_column("mi", "frozen<map<int,text>>", &[]).expect("parses");
    assert_eq!(
        golden_cell_ok(
            CanonicalValue::Tuple(vec![("-2".to_string(), text("x"))]),
            &mi.spec
        ),
        CanonicalValue::Map(vec![(CanonicalValue::Int(-2), text("x"))])
    );

    // …and a TEXT key that merely LOOKS numeric must stay Text, or a
    // `map<text,int>` holding "5" would false-match a `map<int,int>` holding 5.
    assert_eq!(
        golden_cell_ok(
            CanonicalValue::Tuple(vec![("5".to_string(), CanonicalValue::Int(9))]),
            &fm.spec
        ),
        CanonicalValue::Map(vec![(text("5"), CanonicalValue::Int(9))])
    );

    // A Tuple stays a Tuple for every declared type that is NOT a map: a UDT
    // and a frozen list of UDTs must be untouched by the reshape.
    let person = parse_column("p", "frozen<person>", &["person"]).expect("parses");
    let as_tuple = CanonicalValue::Tuple(vec![("nm".to_string(), text("A"))]);
    assert_eq!(golden_cell_ok(as_tuple.clone(), &person.spec), as_tuple);
    let lp = parse_column("lp", "frozen<list<frozen<person>>>", &["person"]).expect("parses");
    assert_eq!(
        golden_cell_ok(CanonicalValue::List(vec![as_tuple.clone()]), &lp.spec),
        CanonicalValue::List(vec![as_tuple])
    );

    // A frozen map NESTED inside a frozen list is reached too.
    let lm = parse_column("lm", "frozen<list<frozen<map<text,int>>>>", &[]).expect("parses");
    assert_eq!(
        golden_cell_ok(
            CanonicalValue::List(vec![CanonicalValue::Tuple(vec![(
                "k".to_string(),
                CanonicalValue::Int(3)
            )])]),
            &lm.spec
        ),
        CanonicalValue::List(vec![CanonicalValue::Map(vec![(
            text("k"),
            CanonicalValue::Int(3)
        )])])
    );
}

/// `CanonicalValue::Map` compares as an ORDERED sequence, so the golden's JSON
/// object order has to be sstabledump's (i.e. Cassandra's key-comparator order),
/// which is the order the Arrow map carries.
///
/// That holds only because the workspace pins `serde_json`'s `preserve_order`
/// feature. Asserted directly: if the feature is ever dropped, `serde_json`
/// falls back to a `BTreeMap` and object keys come out in STRING order, which
/// diverges from Cassandra's for every non-text key type — and a frozen map
/// would start comparing in the wrong order with no explanation.
#[test]
fn golden_json_object_order_is_preserved() {
    let parsed: serde_json::Value =
        serde_json::from_str(r#"{"b": 1, "a": 2, "10": 3, "2": 4}"#).expect("valid JSON");
    let keys: Vec<&str> = parsed
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        vec!["b", "a", "10", "2"],
        "serde_json must preserve JSON object order (workspace feature \
         `preserve_order`); without it a frozen map's golden entries would be \
         re-sorted into STRING order and stop matching the Arrow map's \
         Cassandra-comparator order"
    );
}

/// A non-frozen collection is multicell (one sstabledump cell per element); a
/// frozen one is not. That distinction drives the whole golden projection, so it
/// is asserted directly rather than only through a corpus case.
#[test]
fn frozen_wrapper_decides_multicell() {
    use parquet_parity::cql_type::parse_column;

    assert!(parse_column("s", "set<int>", &[])
        .expect("set<int>")
        .is_multicell_collection());
    assert!(!parse_column("s", "frozen<set<int>>", &[])
        .expect("frozen<set<int>>")
        .is_multicell_collection());
    assert!(!parse_column("p", "frozen<person>", &["person"])
        .expect("frozen<person>")
        .is_multicell_collection());
    assert!(!parse_column("n", "int", &[])
        .expect("int")
        .is_multicell_collection());
}

// ---------------------------------------------------------------------------
// The THIRD outcome, at unit level: `unsupported-representation`
//
// The type check's job is to catch a wrong CQL→Arrow mapping that the
// width-blind value comparison cannot see. For a UDT it could not do that job —
// it accepted ANY Arrow `Struct`, so a UDT whose CQL `int` field was exported as
// `Int64` passed BOTH halves of the harness. These cases pin the replacement: a
// non-Struct is still an affirmative MISMATCH, a Struct is UNMEASURABLE, and
// `Unmeasurable` is never a pass.
// ---------------------------------------------------------------------------

/// A UDT exported AS an Arrow `Struct` is UNMEASURABLE, not valid.
#[test]
fn a_udt_struct_type_claim_is_refused_not_validated() {
    use arrow::datatypes::{DataType, Field, Fields};
    use parquet_parity::arrow_expect::{validate_field, FieldVerdict, ShapeVerdict};
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::unsupported::UDT_STRUCT_FIELD_TYPES;
    use std::sync::Arc;

    let list_of = |t: DataType| DataType::List(Arc::new(Field::new("item", t, true)));
    let map_of = |k: DataType, v: DataType| {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", k, false),
                    Field::new("value", v, true),
                ])),
                false,
            )),
            false,
        )
    };
    let col = |declared: &str| {
        parse_column("c", declared, &["person"]).expect("declared type must parse")
    };
    // A Struct whose `age` field is a WIDENED CQL `int` — the exact defect the
    // old "any Struct" expectation waved through, and one the value comparison
    // cannot see either (both widths canonicalize to one `Int`).
    let widened_person = DataType::Struct(Fields::from(vec![
        Field::new("nm", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
    ]));

    assert_eq!(
        validate_field(&col("frozen<person>"), &widened_person),
        FieldVerdict::Unmeasurable(UDT_STRUCT_FIELD_TYPES),
        "a UDT Struct's field types are undeclarable, so the harness must REFUSE to claim it \
         validated — accepting it is a pass nothing measured"
    );
    // Not a pass, and not a mismatch either: both would be verdicts the harness
    // did not measure, in opposite directions.
    assert_ne!(
        validate_field(&col("frozen<person>"), &widened_person),
        FieldVerdict::Valid
    );

    // The refusal propagates out of a nested position — that is where #3556's
    // family lives.
    assert_eq!(
        validate_field(
            &col("frozen<list<frozen<person>>>"),
            &list_of(widened_person.clone())
        ),
        FieldVerdict::Unmeasurable(UDT_STRUCT_FIELD_TYPES)
    );
    assert_eq!(
        validate_field(
            &col("frozen<map<text, frozen<person>>>"),
            &map_of(DataType::Utf8, widened_person.clone())
        ),
        FieldVerdict::Unmeasurable(UDT_STRUCT_FIELD_TYPES)
    );

    // A NON-Struct is still an affirmative MISMATCH: #3556's `Utf8` flattening
    // must stay detected, which is why the refusal is not applied to the whole
    // UDT expectation.
    match validate_field(&col("frozen<person>"), &DataType::Utf8) {
        FieldVerdict::Mismatch(m) => assert_eq!(m.actual, "utf8"),
        other => panic!("a Utf8-flattened UDT must be a MISMATCH, got {other:?}"),
    }

    // …and an affirmatively WRONG sibling DOMINATES an unmeasurable one: a
    // reportable type defect is strictly more useful than a report that
    // something next to it could not be measured.
    match validate_field(
        &col("frozen<map<text, frozen<person>>>"),
        &map_of(DataType::Int32, widened_person),
    ) {
        FieldVerdict::Mismatch(m) => assert!(
            m.actual.starts_with("map<int32,"),
            "the mismatch must name the wrong KEY type: {}",
            m.actual
        ),
        other => {
            panic!("a wrong map key beside an unmeasurable value must be a MISMATCH: {other:?}")
        }
    }

    // The shape-level verdict is the same three-valued answer, so a caller
    // reaching for `check` directly cannot collapse it either.
    use parquet_parity::arrow_expect::expected_shape;
    assert_eq!(
        expected_shape(&col("frozen<person>").spec)
            .expect("a UDT has a declared expectation")
            .check(&DataType::Struct(Fields::from(vec![Field::new(
                "nm",
                DataType::Utf8,
                true
            )]))),
        ShapeVerdict::Unmeasurable(UDT_STRUCT_FIELD_TYPES)
    );
}

/// Which DECLARED types have refused VALUE representations — and, just as
/// important, which do not.
#[test]
fn refused_value_representations_are_keyed_on_the_declared_type() {
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::unsupported::{refused_value_representation, CQL_TUPLE_VALUES};

    let refused = |declared: &str| {
        let col = parse_column("c", declared, &["person"]).expect("declared type must parse");
        refused_value_representation(&col.spec)
    };

    // A CQL tuple, at the top level and in every nested position a corpus type
    // can put one — the golden holds it positionally, the export names it.
    for declared in [
        "tuple<int, text>",
        "frozen<tuple<int, text>>",
        "list<frozen<tuple<int, text>>>",
        "set<frozen<tuple<int, text>>>",
        "map<text, frozen<tuple<int, text>>>",
        "frozen<map<frozen<tuple<int, text>>, text>>",
    ] {
        assert_eq!(
            refused(declared),
            Some(CQL_TUPLE_VALUES),
            "'{declared}' carries a CQL tuple, so its VALUES must be refused"
        );
    }

    // And nothing else is refused. `duration` is the case that matters: an Arrow
    // `Interval` duration also decodes to a canonical `Tuple`, but its DECLARED
    // type is a scalar and `spelling.rs` reconciles both sides onto the same
    // (months, days, nanos) triple — that IS a measured comparison, so refusing
    // it would be a false gap.
    for declared in [
        "duration",
        "int",
        "text",
        "frozen<person>",
        "list<frozen<person>>",
        "map<text, frozen<person>>",
        "map<int, text>",
        "frozen<list<int>>",
    ] {
        assert_eq!(
            refused(declared),
            None,
            "'{declared}' is comparable, so refusing it would be a false gap"
        );
    }
}

// ---------------------------------------------------------------------------
// Declared-type-guided STRING typing (#28 no-heuristics; #1490 round 5)
//
// The shared JSON parser types a value's string by its SPELLING: any
// `Z`-suffixed timestamp becomes a `Timestamp`. It must, because its other
// lanes have no schema — but THIS harness declares every column's CQL type, so
// a `text` column holding a legal timestamp-shaped string was turned into a
// `Timestamp` on the golden side while the Arrow `Utf8` side stayed `Text`,
// which can only compare unequal. That is a type inferred from a value's bytes,
// and it produces a FALSE parity failure.
//
// The controls below drive the REAL golden projection (`project_golden`, via the
// real JSONL parser) once per POSITION the finding names — a top-level scalar, a
// collection element, a map key, and a primary-key component — because a
// recursive fix verified only at the top level is not verified. Each carries its
// positive control: a declared `timestamp` in the same document still compares
// as an INSTANT.
// ---------------------------------------------------------------------------

/// A legal `text` value that SPELLS an sstabledump timestamp.
const TS_SPELLING: &str = "2025-10-06 01:12:07.265Z";
/// The same instant in the ISO-8601 spelling, for the `timestamp` control.
const TS_SPELLING_ISO: &str = "2025-10-06T01:12:07.265000Z";

/// The declared columns of the synthetic case: a timestamp-shaped `text`
/// PARTITION KEY, a `text` scalar, a `set<text>`, a `map<text,text>`, a
/// `frozen<map<text,text>>` — and one real `timestamp` column as the control
/// that the guidance types BY the declaration rather than suppressing the
/// variant everywhere.
fn timestamp_spelling_columns() -> Vec<parquet_parity::cql_type::ColumnType> {
    [
        ("id", "text"),
        ("note", "text"),
        ("when", "timestamp"),
        ("tags", "set<text>"),
        ("m", "map<text,text>"),
        ("fm", "frozen<map<text,text>>"),
    ]
    .iter()
    .map(|(n, t)| parquet_parity::cql_type::parse_column(n, t, &[]).expect("declared type parses"))
    .collect()
}

/// Project ONE synthetic golden row carrying `TS_SPELLING` in every position the
/// round-5 finding names, through the real parser and the real projection.
fn project_timestamp_spelling_row() -> parquet_parity::golden_rows::GoldenRow {
    use parquet_parity::canonical_jsonl::{parse_document_str_with_keys, KeySpec};

    let columns = timestamp_spelling_columns();
    // sstabledump's own shape (see any committed `*-Data.db.jsonl`): a
    // stringified key array, one cell per non-frozen collection element with a
    // `path`, and a frozen map as ONE JSON object.
    let doc_json = format!(
        r#"{{"partition":{{"key":["{ts}"],"position":0}},"rows":[{{"type":"row","position":0,
           "liveness_info":{{"tstamp":"2025-10-06T01:12:07.265000Z"}},"cells":[
             {{"name":"note","value":"{ts}"}},
             {{"name":"when","value":"{iso}"}},
             {{"name":"tags","path":["{ts}"]}},
             {{"name":"m","path":["{ts}"],"value":"{ts}"}},
             {{"name":"fm","value":{{"{ts}":"{ts}"}}}}
           ]}}]}}"#,
        ts = TS_SPELLING,
        iso = TS_SPELLING_ISO,
    );
    let doc = parse_document_str_with_keys(
        &doc_json.replace('\n', " "),
        std::path::Path::new("<synthetic timestamp-spelling golden>"),
        true,
        &KeySpec::from_cql_types(&["text"], &[]),
    )
    .expect("the synthetic golden must parse");

    let mut rows = parquet_parity::golden_rows::project_golden(&doc, &columns, &["id"], &[])
        .expect("the synthetic golden must project");
    assert_eq!(
        rows.len(),
        1,
        "the synthetic golden declares exactly one row"
    );
    rows.remove(0)
}

/// What the Arrow `Utf8` side of a `text` column holds for the same value.
fn exported_text(s: &str) -> parquet_parity::canonical_jsonl::CanonicalValue {
    parquet_parity::canonical_jsonl::CanonicalValue::Text(s.to_string())
}

/// POSITION 1 — a top-level `text` scalar. And the positive control in the same
/// document: the declared `timestamp` column is STILL a `Timestamp`, comparing
/// as an instant across the two spellings, so the fix types by the declaration
/// rather than deleting the variant.
#[test]
fn declared_text_keeps_a_timestamp_spelling_as_text_in_a_scalar_column() {
    use parquet_parity::canonical_jsonl::{parse_timestamp_micros, CanonicalValue};

    let row = project_timestamp_spelling_row();
    assert_eq!(
        row.cells.get("note"),
        Some(&exported_text(TS_SPELLING)),
        "a declared `text` value must stay Text however it is spelled — typing it \
         from its bytes is the no-heuristics violation of #28"
    );

    let when = row.cells.get("when").expect("the control column projects");
    assert_eq!(
        when,
        &CanonicalValue::Timestamp {
            micros: parse_timestamp_micros(TS_SPELLING).expect("the spelling is a timestamp"),
            // `raw` is diagnostic and NOT compared, so the ISO spelling the
            // golden carries still equals the space-separated one.
            raw: TS_SPELLING.to_string(),
        },
        "a declared `timestamp` must still compare as an INSTANT"
    );
}

/// POSITION 2 — an element of a non-frozen `set<text>`, whose element arrives as
/// a STRINGIFIED cell path.
#[test]
fn declared_text_keeps_a_timestamp_spelling_as_text_in_a_collection_element() {
    use parquet_parity::canonical_jsonl::CanonicalValue;

    let row = project_timestamp_spelling_row();
    assert_eq!(
        row.cells.get("tags"),
        // A multicell set projects to an ordered `List` on both sides.
        Some(&CanonicalValue::List(vec![exported_text(TS_SPELLING)])),
        "a `set<text>` ELEMENT must be typed by the declared element type, not by \
         its spelling — the recursion has to reach it"
    );
}

/// POSITION 3 — a map KEY, in BOTH map shapes: the non-frozen map's stringified
/// cell path and the frozen map's JSON OBJECT key.
#[test]
fn declared_text_keeps_a_timestamp_spelling_as_text_in_a_map_key() {
    use parquet_parity::canonical_jsonl::CanonicalValue;

    let row = project_timestamp_spelling_row();
    let expected = CanonicalValue::Map(vec![(
        exported_text(TS_SPELLING),
        exported_text(TS_SPELLING),
    )]);
    assert_eq!(
        row.cells.get("m"),
        Some(&expected),
        "a non-frozen `map<text,text>` KEY and VALUE must both be typed by the \
         declared key/value types"
    );
    assert_eq!(
        row.cells.get("fm"),
        Some(&expected),
        "a FROZEN map's JSON object KEY must be too — it only becomes reachable \
         after the object is reshaped into a Map, so the ORDER of the two \
         normalizations is load-bearing"
    );
}

/// POSITION 4 — a primary-KEY component. This one reaches the harness's SORT
/// KEY as well as the cell map, so getting it wrong reports "primary key
/// differs" on every row of the table rather than one cell.
#[test]
fn declared_text_keeps_a_timestamp_spelling_as_text_in_a_key_column() {
    use parquet_parity::render_value;

    let row = project_timestamp_spelling_row();
    assert_eq!(
        row.keys,
        vec![exported_text(TS_SPELLING)],
        "a declared `text` PARTITION KEY component must stay Text"
    );
    assert_eq!(
        row.cells.get("id"),
        Some(&exported_text(TS_SPELLING)),
        "and the key column's cell entry must agree with its key component"
    );
    // The sort key is what the row MATCHING uses, and it renders a Timestamp as
    // `ts:<micros>` and a Text as `text:"…"` — so a mistyped key column could
    // never match its exported row.
    assert_eq!(
        render_value(&row.keys[0]),
        render_value(&exported_text(TS_SPELLING))
    );
}

/// SENSITIVITY control for the whole normalization: it must erase only the
/// TYPE GUESS, never a difference in the VALUE. Two different timestamp-shaped
/// texts stay different, and a `timestamp` column still notices a different
/// instant.
#[test]
fn declared_type_string_typing_does_not_erase_a_real_difference() {
    use parquet_parity::canonical_jsonl::CanonicalValue;
    use parquet_parity::cql_type::parse_column;

    let as_parsed = |s: &str| CanonicalValue::from_json(&serde_json::Value::String(s.to_string()));
    let text = parse_column("c", "text", &[]).expect("parses");
    let ts = parse_column("c", "timestamp", &[]).expect("parses");
    let other = "2025-10-06 01:12:07.266Z";

    assert_ne!(
        golden_cell_ok(as_parsed(TS_SPELLING), &text.spec),
        golden_cell_ok(as_parsed(other), &text.spec),
        "two different timestamp-shaped TEXT values must still differ"
    );
    assert_ne!(
        golden_cell_ok(as_parsed(TS_SPELLING), &ts.spec),
        golden_cell_ok(as_parsed(other), &ts.spec),
        "two different INSTANTS must still differ"
    );
    // Every other string-rendered scalar the Arrow side holds as `Text` is
    // restored the same way — a `blob`/`uuid`/`date`/`inet` value could never
    // legitimately be a Timestamp.
    for declared in ["varchar", "ascii", "blob", "uuid", "date", "time", "inet"] {
        let col = parse_column("c", declared, &[]).expect("parses");
        assert_eq!(
            golden_cell_ok(as_parsed(TS_SPELLING), &col.spec),
            exported_text(TS_SPELLING),
            "'{declared}' is rendered as Text by the Arrow side, so the golden must \
             not hold a Timestamp for it"
        );
    }
}
