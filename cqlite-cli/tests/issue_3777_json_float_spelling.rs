//! A CQL `float` reaches the `--format json` egress as the shortest decimal that
//! round-trips the **f32**, not the widened f64's (issue #3777).
//!
//! # The defect
//!
//! `serde_json::Number` holds an `f64` unconditionally, so
//! `Number::from_f64(f as f64)` widens the f32 to its exact-but-imprecise f64
//! FIRST and the emitted decimal is then the shortest one round-tripping THAT
//! f64: `1.67f32` printed as `1.6699999570846558`.
//!
//! # The oracle — Cassandra, never CQLite's own output
//!
//! * `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/FloatType.java:115-124`
//!   — `toJSONString` returns the f32's `Float.toString` text (and the literal
//!   `null` for NaN/±Infinity, whose in-source comment is "JSON does not support
//!   NaN, Infinity and -Infinity values").
//! * `cassandra-5.0.8:src/java/org/apache/cassandra/serializers/FloatSerializer.java`
//!   — `toString` is `Float.toString`, i.e. the shortest decimal that round-trips
//!   the **f32**.
//!
//! The expected SPELLINGS below are transcribed from the committed
//! `*-Data.db.jsonl` goldens that `sstabledump` wrote:
//!
//! ```text
//! $ grep -o '{"name":"height","value":[^,}]*' \
//!     test-data/datasets/sstables/test_basic/simple_table-*/nb-1-big-Data.db.jsonl
//! {"name":"height","value":1.84
//! {"name":"height","value":1.65
//! ```
//!
//! # Why this is an integration target and not a `--lib` unit test
//!
//! `cqlite-cli`'s lib/bin unit tests execute in NO gate component and NO CI job
//! (`scripts/tests/workspace-test-disposition.txt` records the crate as
//! `PARTIAL / contradicts-doctrine`; the gate's `cli-tests` passes no `--lib`, and
//! `.github/workflows/ci.yml` runs only `--test unit_tests`), so a case beside the
//! fix in `cqlite-cli/src/output/json_cell.rs` would be maintained and never run —
//! which is exactly what that module's own closing note says. `cli-tests`
//! ENUMERATES the `cqlite-cli/tests/*.rs` glob (#2039), so these cases run.
//!
//! Every assertion is on the BYTES `JSONWriter` emits, through the PUBLIC writer,
//! rather than on an intermediate `serde_json::Value` — the crate-private
//! `JsonCell` is deliberately not reachable from here, and a `Value` round trip
//! would re-widen and defeat the point.

use std::collections::HashMap;

use cqlite_cli::config::OutputConfig;
use cqlite_cli::output::JSONWriter;
use cqlite_core::query::{ColumnInfo, QueryResult, QueryRow};
use cqlite_core::types::DataType;
use cqlite_core::{RowKey, Value};

/// The exact text `JSONWriter` emits for one cell, extracted from the real
/// document rather than reconstructed.
///
/// A one-column, one-row result renders (pretty) as
///
/// ```text
/// [
///   {
///     "v": 1.67
///   }
/// ]
/// ```
///
/// so the cell's LEXEME is the text between `"v": ` and the end of that line.
/// Taking it from the emitted document is what makes this a test of the writer's
/// bytes: nothing here re-serializes, and no `f64` is parsed on the way.
fn emitted_cell_text(value: &Value, declared: DataType) -> String {
    let mut result = QueryResult::new();
    result.metadata.columns = vec![ColumnInfo::new("v".to_string(), declared, false, 0)];
    let mut values = HashMap::new();
    values.insert("v".to_string(), value.clone());
    result
        .rows
        .push(QueryRow::with_values(RowKey::new(vec![0]), values));

    let json = JSONWriter::write(&result, &OutputConfig::default())
        .unwrap_or_else(|e| panic!("JSONWriter::write failed for {value:?}: {e}"));

    let marker = "\"v\": ";
    let start = json
        .find(marker)
        .unwrap_or_else(|| panic!("no `v` cell in emitted document:\n{json}"))
        + marker.len();
    let rest = &json[start..];
    let end = rest.find('\n').unwrap_or(rest.len());
    rest[..end].trim_end_matches(',').to_string()
}

fn float32_text(f: f32) -> String {
    emitted_cell_text(&Value::Float32(f), DataType::Float32)
}

fn float64_text(f: f64) -> String {
    emitted_cell_text(&Value::Float(f), DataType::Float)
}

/// Significant digits of a decimal rendering: sign, decimal point, exponent,
/// leading zeros and trailing zeros carry no precision.
fn significant_digits(s: &str) -> usize {
    let mantissa = s.split(['e', 'E']).next().unwrap_or(s);
    let digits: String = mantissa.chars().filter(|c| c.is_ascii_digit()).collect();
    digits.trim_start_matches('0').trim_end_matches('0').len()
}

/// Issue #3777: a CQL `float` must serialize as the shortest decimal that
/// round-trips the **f32**, which is what `sstabledump` prints (Cassandra
/// `FloatSerializer` → `Float.toString`).
#[test]
fn float32_renders_shortest_decimal_that_round_trips_the_f32() {
    // (f32 literal, the spelling the sstabledump golden carries)
    let cases: &[(f32, &str)] = &[
        // test_basic.simple_table `height FLOAT`
        (1.67, "1.67"),
        (1.84, "1.84"),
        (1.65, "1.65"),
        (1.56, "1.56"),
        (1.87, "1.87"),
        // test_timeseries.sensor_data `temperature`/`humidity` FLOAT
        (92.88221, "92.88221"),
        (-16.172066, "-16.172066"),
        (1.5052613, "1.5052613"),
        (8.8656225, "8.8656225"),
        // An EXACT TIE, and the reason the conversion does not use `f32::Display`:
        // 36.6015625 is exactly representable, four 8-digit decimals round-trip it
        // and two are equidistant. The dump (Cassandra `Float.toString`) rounds the
        // tie to an even last digit; Rust's `Display` rounds away from zero and
        // emits `36.601563`. Measured on the real `test_timeseries.sensor_data`
        // `temperature` cell that the AD2 lane compares. Written as the exact
        // fraction 4685/128 because the decimal literal `36.6015625` trips
        // `clippy::excessive_precision` (an f32 literal may carry no more digits
        // than the f32 needs, and this value's shortest form is 8 of its 9) —
        // the same reason the spread test below spells it that way.
        (4685.0 / 128.0, "36.601562"),
        // Integral and zero spellings.
        (0.0, "0.0"),
        (-0.0, "-0.0"),
        (1.0, "1.0"),
        (-2.5, "-2.5"),
    ];

    for (f, expected) in cases {
        assert_eq!(
            float32_text(*f),
            *expected,
            "FLOAT {f} must render as the shortest f32 round-trip, not its widened f64"
        );
    }
}

/// The property behind the case list above: whatever the writer emits must parse
/// back to the SAME f32, and must carry no more digits than the f32's own
/// shortest round-trip spelling.
#[test]
fn float32_json_round_trips_through_f32_for_a_spread_of_values() {
    let values: &[f32] = &[
        1.67,
        -16.172066,
        f32::MIN_POSITIVE,
        f32::MAX,
        f32::MIN,
        1e-7,
        1e10,
        1.0 / 3.0,
        core::f32::consts::PI,
        16_777_215.0,
        0.1,
        1234.5678,
        // A true SUBNORMAL, below `MIN_POSITIVE`: the f32 shortest form (`1e-45`)
        // and the f64 re-parse are furthest apart in exponent here, so this is the
        // hardest case for the `f32 text -> f64 -> Number` chain.
        f32::from_bits(1),
        // NEGATIVE ZERO: the sign bit must survive the round trip. The assertion
        // below compares `to_bits()`, so a `-0.0` collapsing to `+0.0` fails.
        -0.0,
        // The EXACT TIE (36.6015625 = 4685/128, written as a fraction because a
        // decimal literal trips `clippy::excessive_precision`). It matters to two
        // separate assertions now: the exact-spelling case list above pins
        // `36.601562`, and the significant-digit bound here must still hold —
        // Display's `36.601563` and serde_json's `36.601562` are both 8 digits.
        4685.0 / 128.0,
    ];
    for &f in values {
        let text = float32_text(f);
        let parsed: f32 = text
            .parse()
            .unwrap_or_else(|e| panic!("emitted {text} for {f} is not parseable as f32: {e}"));
        assert_eq!(
            parsed.to_bits(),
            f.to_bits(),
            "emitted {text} does not round-trip {f}"
        );
        // No more SIGNIFICANT digits than `f32`'s own shortest spelling. Counted
        // rather than string-compared because JSON float notation legitimately
        // differs from Rust's `Display` in ways that carry no precision: ryu
        // renders `1e10` as `10000000000.0` (a trailing `.0`) and `f32::MAX` in
        // exponent form (`3.4028235e38`) where `Display` writes it out in full.
        assert!(
            significant_digits(&text) <= significant_digits(&f.to_string()),
            "emitted {text} carries more significant digits than the f32 shortest form {f}"
        );
    }
}

/// PRESERVED, deliberately: a non-finite `float`/`double` renders as JSON `null`
/// because JSON has no literal for `NaN`/`±Infinity` — Cassandra's own choice at
/// `FloatType.java:115-124` / `DoubleType.java:114-123`. It is also a DECLARED
/// 3-way asymmetry (CLAUDE.md, `bindings/parity` declared gap 4; AD2's
/// `Divergence::NonFiniteFloatRendersAsJsonNull`) and is NOT in scope for #3777 —
/// this test pins it so the preservation is visible rather than accidental.
#[test]
fn nonfinite_float_renders_as_json_null_unchanged() {
    for (value, declared) in [
        (Value::Float32(f32::NAN), DataType::Float32),
        (Value::Float32(f32::INFINITY), DataType::Float32),
        (Value::Float32(f32::NEG_INFINITY), DataType::Float32),
        (Value::Float(f64::NAN), DataType::Float),
        (Value::Float(f64::INFINITY), DataType::Float),
        (Value::Float(f64::NEG_INFINITY), DataType::Float),
    ] {
        assert_eq!(
            emitted_cell_text(&value, declared),
            "null",
            "non-finite {value:?} must stay JSON null (declared divergence, not #3777)"
        );
    }
}

/// `Value::Float` is a CQL `double` and is ALREADY correct: no widening happens,
/// so its shortest f64 round-trip is exactly what `sstabledump` prints (measured
/// against `test_timeseries.sensor_data`'s `pressure DOUBLE`). Pinned so the
/// #3777 fix to the `Float32` arm cannot drift the `Float` one.
#[test]
fn float64_double_is_not_widened_and_keeps_its_shortest_decimal() {
    let cases: &[(f64, &str)] = &[
        // test_timeseries.sensor_data `pressure DOUBLE`, from the golden.
        (1017.9518806690071, "1017.9518806690071"),
        (1002.1829379523564, "1002.1829379523564"),
        (1.67, "1.67"),
        (0.1, "0.1"),
    ];
    for (f, expected) in cases {
        assert_eq!(float64_text(*f), *expected);
    }
}

/// MEASUREMENT for the route decision (#3777): serde_json cannot fix this arm
/// from the outside. `Number` stores an `f64` unconditionally — `Number::from_f32`
/// itself is `N::Float(f as f64)` — so `serde_json::Value::from(1.67f32)` carries
/// the widened f64 and prints the widened spelling. The `float_roundtrip` feature
/// only touches DESERIALIZATION (`src/de.rs`, `src/value/de.rs`); nothing in
/// `ser.rs`/`number.rs` reads it. So only the streaming `Serializer::serialize_f32`
/// path preserves f32 shortest form, and a `JsonCell::Plain` carries a
/// `serde_json::Value`.
#[test]
fn serde_json_value_from_f32_still_widens_so_the_fix_must_be_local() {
    assert_eq!(
        serde_json::to_string(&serde_json::Value::from(1.67f32)).expect("serialize"),
        "1.6699999570846558",
        "if this ever changes, the local shortest-form conversion can be dropped"
    );
}
