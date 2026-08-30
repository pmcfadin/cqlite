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
//! These run in EVERY checkout, including one with no fetched corpus, so the
//! normalizers that decide equality are never untested exactly where the corpus
//! is thinnest.

#![cfg(feature = "state_machine")]

#[path = "support/parquet_parity/mod.rs"]
mod parquet_parity;

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
    use parquet_parity::arrow_expect::expected_shape;
    use parquet_parity::cql_type::parse_column;

    let accepts = |declared: &str, actual: &DataType| -> bool {
        let col = parse_column("c", declared, &[]).expect("declared type must parse");
        expected_shape(&col.spec)
            .expect("every corpus scalar has a declared expectation")
            .accepts(actual)
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
        assert!(
            accepts(declared, &expected),
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
        assert!(
            !accepts(declared, &wrong),
            "'{declared}' must REJECT {wrong:?} — a wrong width round-trips its values \
             unchanged, so nothing else in this harness can catch it"
        );
    }
}

/// Nested types are matched structurally: element/key/value types recurse, and a
/// UDT must be a `Struct` (#3556's `Utf8` flattening is exactly this check).
#[test]
fn expected_arrow_type_recurses_into_nested_types() {
    use arrow::datatypes::{DataType, Field, Fields};
    use parquet_parity::arrow_expect::{expected_shape, validate_field};
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

    assert!(shape("set<int>", &[]).accepts(&list_of(DataType::Int32)));
    assert!(shape("list<int>", &[]).accepts(&list_of(DataType::Int32)));
    // …but not a list of the wrong element width.
    assert!(!shape("set<int>", &[]).accepts(&list_of(DataType::Int64)));
    assert!(shape("map<int, text>", &[]).accepts(&map_of(DataType::Int32, DataType::Utf8)));
    assert!(!shape("map<int, text>", &[]).accepts(&map_of(DataType::Utf8, DataType::Utf8)));
    // A UDT is a Struct with fields the case does not declare…
    let person = DataType::Struct(Fields::from(vec![Field::new("nm", DataType::Utf8, true)]));
    assert!(shape("frozen<person>", &["person"]).accepts(&person));
    // …and a Utf8 rendering of one is NOT.
    assert!(!shape("frozen<person>", &["person"]).accepts(&DataType::Utf8));
    assert!(!shape("frozen<list<frozen<person>>>", &["person"]).accepts(&list_of(DataType::Utf8)));

    // The mismatch message must name the column, the declared CQL type and both
    // Arrow types — it is what the #3556 known-gap signature pins.
    let col = parse_column("lp", "frozen<list<frozen<person>>>", &["person"]).expect("parses");
    let mismatch = validate_field(&col, &list_of(DataType::Utf8))
        .expect_err("a Utf8-flattened UDT must be rejected")
        .expect("it is a type mismatch, not a refusal to answer");
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
// `arrow_rows::canonical_from_arrow` decides which it can DECODE. An accept-list
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
    use parquet_parity::arrow_expect::expected_shape;
    use parquet_parity::arrow_rows::canonical_from_arrow;
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
        assert!(
            shape.accepts(&dt),
            "'{declared}' must accept {dt:?} for this test to be about the decoder"
        );
        canonical_from_arrow(array.as_ref(), 0, "test").unwrap_or_else(|e| {
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
    use parquet_parity::arrow_expect::expected_shape;
    use parquet_parity::cql_type::parse_column;
    use std::sync::Arc;

    let shape = |declared: &str| {
        let col = parse_column("c", declared, &[]).expect("declared type must parse");
        expected_shape(&col.spec).expect("expectation must be derivable")
    };
    let item = || Arc::new(Field::new("item", DataType::Int32, true));

    assert!(shape("list<int>").accepts(&DataType::List(item())));
    for wrong in [
        DataType::LargeList(item()),
        DataType::FixedSizeList(item(), 3),
    ] {
        assert!(
            !shape("list<int>").accepts(&wrong),
            "{wrong:?} must be REJECTED: arrow_rows has no decoder for it, so accepting it \
             would pass the schema check and then die during value projection"
        );
        assert!(!shape("set<int>").accepts(&wrong), "{wrong:?}");
    }
}

/// An Arrow `Interval(MonthDayNano)` duration and the golden's duration TEXT
/// must canonicalize to the SAME value — the interval arm is only useful if it
/// lands in the form `spelling::normalize_spelling` puts the golden into.
#[test]
fn interval_duration_canonicalizes_to_the_golden_spelling() {
    use arrow::array::IntervalMonthDayNanoArray;
    use arrow::datatypes::IntervalMonthDayNano;
    use parquet_parity::arrow_rows::canonical_from_arrow;
    use parquet_parity::canonical_jsonl::CanonicalValue;
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::spelling::normalize_spelling;

    let duration = parse_column("d", "duration", &[]).expect("duration parses");
    // 1 month, 2 days, 3 033 000 000 000 ns — Cassandra spells the nanos part
    // "50m33s", CQLite's ValueFormatter "3033000000000ns".
    let nanos = 3_033_000_000_000i64;
    let array = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(1, 2, nanos)]);
    let exported = canonical_from_arrow(&array, 0, "test").expect("interval must decode");
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
        canonical_from_arrow(&other, 0, "test").expect("interval must decode"),
        exported
    );
}

// ---------------------------------------------------------------------------
// EXACT decimals: no `f64` on either side (roborev round 4, #1490)
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
// No corpus table currently carries a scale-0 decimal (measured: 2650 decimal
// cells across every golden, none integer-shaped), so that path is covered by a
// unit test over the two normalizers rather than by inventing a fixture.
// ---------------------------------------------------------------------------

/// A decimal whose golden literal has no fractional part must canonicalize to
/// the SAME canonical value as the exported `Decimal128(38, 9)` cell.
#[test]
fn whole_valued_decimal_canonicalizes_on_both_sides() {
    use parquet_parity::arrow_rows::decimal_to_canonical;
    use parquet_parity::canonical_jsonl::CanonicalValue;
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::golden_rows::normalize_declared_numbers;

    let decimal = parse_column("d", "decimal", &[]).expect("decimal parses");
    let varint = parse_column("v", "varint", &[]).expect("varint parses");

    for whole in [0i128, 1, -1, 42, -31_595] {
        // Golden side: sstabledump writes a whole decimal as a JSON integer.
        let golden = normalize_declared_numbers(CanonicalValue::Int(whole), &decimal.spec)
            .expect("a whole golden decimal is exact");
        // Export side: Decimal128(38, 9) holds whole * 10^9.
        let exported = decimal_to_canonical(whole * 1_000_000_000, 9, "test")
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
        normalize_declared_numbers(golden_decimal(31_595.67), &decimal.spec)
            .expect("a recoverable golden decimal"),
        decimal_to_canonical(31_595_670_000_000, 9, "test").expect("fractional decimal")
    );

    // varint is an integer domain on BOTH sides: it must stay an `Int`, or the
    // rule would turn a type confusion into a silent pass.
    assert_eq!(
        normalize_declared_numbers(CanonicalValue::Int(7), &varint.spec).expect("varint is exact"),
        CanonicalValue::Int(7)
    );
    assert_eq!(
        decimal_to_canonical(7, 0, "test").expect("varint"),
        CanonicalValue::Int(7)
    );

    // The exact representation has NO `2^53` ceiling — that bound existed only
    // because the comparison went through a double. A decimal far beyond it now
    // compares exactly on both sides.
    let huge = 1i128 << 60;
    assert_eq!(
        normalize_declared_numbers(CanonicalValue::Int(huge), &decimal.spec)
            .expect("a whole golden decimal of any magnitude is exact"),
        decimal_to_canonical(huge * 1_000_000_000, 9, "test").expect("scale-9 decimal"),
        "a whole decimal beyond 2^53 must still compare equal to its exported form"
    );
}

/// The golden literals of `test_basic.simple_table.account_balance` must recover
/// to EXACTLY the decimal they spell, and to the exported cell for that value.
#[test]
fn golden_decimal_literals_recover_exactly() {
    use parquet_parity::arrow_rows::decimal_to_canonical;
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::golden_rows::normalize_declared_numbers;

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
        let via_serde: f64 = serde_json::from_str::<serde_json::Value>(literal)
            .expect("literal must be valid JSON")
            .as_f64()
            .expect("literal must be a JSON number");
        let golden = normalize_declared_numbers(golden_decimal(via_serde), &decimal.spec)
            .expect("a corpus decimal literal must recover exactly");
        assert_eq!(
            golden,
            parquet_parity::decimal::ExactDecimal::new(unscaled_at_scale_9, 9).canonical(),
            "the golden literal {literal} must recover to the decimal it spells"
        );
        assert_eq!(
            golden,
            decimal_to_canonical(unscaled_at_scale_9, 9, "test").expect("scale-9 decimal"),
            "the recovered literal {literal} must equal the exported cell"
        );
    }

    // A signed zero is not a decimal attribute: `BigDecimal` has no negative
    // zero, so `-0.0` and `0.0` both recover to the decimal 0.
    for zero in [0.0f64, -0.0] {
        assert_eq!(
            normalize_declared_numbers(golden_decimal(zero), &decimal.spec)
                .expect("zero must recover"),
            decimal_to_canonical(0, 9, "test").expect("scale-9 zero")
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
    use parquet_parity::arrow_rows::decimal_to_canonical;
    use parquet_parity::canonical_jsonl::CanonicalValue;

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
    let exported = decimal_to_canonical(unscaled, 9, "test").expect("scale-9 decimal");
    let corrupted = decimal_to_canonical(perturbed, 9, "test").expect("scale-9 decimal");
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

    // And the golden side REFUSES that double rather than compare it: two
    // distinct scale-9 decimals share it, so no recovery is exact. A refusal is
    // a loud non-answer; comparing would be the false PASS this control exists
    // to prevent.
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::golden_rows::normalize_declared_numbers;
    let decimal = parse_column("d", "decimal", &[]).expect("decimal parses");
    let err = normalize_declared_numbers(golden_decimal(unscaled as f64 / 1e9), &decimal.spec)
        .expect_err("an ambiguous golden double must be refused");
    assert!(
        err.contains("9007199.254740001") && err.contains("9007199.254740002"),
        "the refusal must name both candidate decimals, got: {err}"
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
    use parquet_parity::arrow_rows::decimal_to_canonical;
    use parquet_parity::decimal::{exact_from_golden_double, EXPORT_DECIMAL_SCALE};

    // A scale beyond the one the golden side can recover a literal at: the
    // harness refuses rather than let two distinct decimals recover to one.
    let err = decimal_to_canonical(1, 12, "test").expect_err("scale 12 must be refused");
    assert!(err.contains("exceeds"), "got: {err}");
    assert!(
        decimal_to_canonical(1, -1, "test").is_err(),
        "negative scale"
    );

    // A literal with more fractional digits than the export's fixed scale: the
    // export refuses to truncate it, and the golden side refuses to round it.
    let too_precise: f64 = "0.0000000001".parse().expect("f64");
    let err = exact_from_golden_double(too_precise, EXPORT_DECIMAL_SCALE, "golden")
        .expect_err("a literal beyond the export scale must be refused");
    assert!(err.contains("fractional digits"), "got: {err}");

    // Non-finite doubles are not decimals at all.
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(exact_from_golden_double(bad, EXPORT_DECIMAL_SCALE, "golden").is_err());
    }
}

/// A golden `decimal` cell as `canonical_jsonl` hands it to the harness: the
/// literal has already been parsed into an `f64` by the shared comparator.
fn golden_decimal(f: f64) -> parquet_parity::canonical_jsonl::CanonicalValue {
    use parquet_parity::canonical_jsonl::{CanonicalValue, NormalizedFloat};
    CanonicalValue::Float(NormalizedFloat(f))
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
    use parquet_parity::golden_rows::coerce_declared_shape;

    let text = |s: &str| CanonicalValue::Text(s.to_string());

    // `frozen<map<text,int>>` — the shape of `udt_collections.fm`.
    let fm = parse_column("fm", "frozen<map<text,int>>", &[]).expect("parses");
    let golden = CanonicalValue::Tuple(vec![
        ("a".to_string(), CanonicalValue::Int(1)),
        ("b".to_string(), CanonicalValue::Int(2)),
    ]);
    // What `arrow_rows::canonical_from_arrow` builds from an Arrow Map.
    let exported = CanonicalValue::Map(vec![
        (text("a"), CanonicalValue::Int(1)),
        (text("b"), CanonicalValue::Int(2)),
    ]);
    assert_eq!(coerce_declared_shape(golden, &fm.spec), exported);

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
        parquet_parity::golden_rows::fold_null(coerce_declared_shape(golden, &ma.spec)),
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
        coerce_declared_shape(
            CanonicalValue::Tuple(vec![("-2".to_string(), text("x"))]),
            &mi.spec
        ),
        CanonicalValue::Map(vec![(CanonicalValue::Int(-2), text("x"))])
    );

    // …and a TEXT key that merely LOOKS numeric must stay Text, or a
    // `map<text,int>` holding "5" would false-match a `map<int,int>` holding 5.
    assert_eq!(
        coerce_declared_shape(
            CanonicalValue::Tuple(vec![("5".to_string(), CanonicalValue::Int(9))]),
            &fm.spec
        ),
        CanonicalValue::Map(vec![(text("5"), CanonicalValue::Int(9))])
    );

    // A Tuple stays a Tuple for every declared type that is NOT a map: a UDT
    // and a frozen list of UDTs must be untouched by the reshape.
    let person = parse_column("p", "frozen<person>", &["person"]).expect("parses");
    let as_tuple = CanonicalValue::Tuple(vec![("nm".to_string(), text("A"))]);
    assert_eq!(
        coerce_declared_shape(as_tuple.clone(), &person.spec),
        as_tuple
    );
    let lp = parse_column("lp", "frozen<list<frozen<person>>>", &["person"]).expect("parses");
    assert_eq!(
        coerce_declared_shape(CanonicalValue::List(vec![as_tuple.clone()]), &lp.spec),
        CanonicalValue::List(vec![as_tuple])
    );

    // A frozen map NESTED inside a frozen list is reached too.
    let lm = parse_column("lm", "frozen<list<frozen<map<text,int>>>>", &[]).expect("parses");
    assert_eq!(
        coerce_declared_shape(
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
