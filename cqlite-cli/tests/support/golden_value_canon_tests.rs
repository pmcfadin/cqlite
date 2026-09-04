//! Unit coverage for the scalar CANONICALIZATION rules of
//! `golden_value_parity.rs` (split out under the campsite rule, CLAUDE.md epic
//! #1135, when the parent reached the ~1500-line test-file target).
//!
//! Declared with `#[path]` as that module's own `tests` child, so every case below
//! sees the same private declarations it did inline — there is no second copy of
//! `canon_typed`'s rules to drift.

use super::*;
use serde_json::json;

fn text() -> CqlType {
    CqlType::Text("text".to_string())
}

fn int() -> CqlType {
    CqlType::Numeric("int".to_string())
}

/// Canonicalize at a position `sstabledump` STRINGIFIES (a partition key, a
/// multicell cell path, a map key) — the only JSON positions where a numeric
/// string may be read as a number.
fn canon(v: &Value, ty: &CqlType) -> Canon {
    canon_at(v, ty, Kinding::Stringified)
}

/// Canonicalize at an ordinary position, where the golden keeps its natural
/// JSON kind and the two sides must therefore agree on kind.
fn canon_natural(v: &Value, ty: &CqlType) -> Canon {
    canon_at(v, ty, Kinding::Natural)
}

fn canon_at(v: &Value, ty: &CqlType, kinding: Kinding) -> Canon {
    match canon_typed(v, Egress::Json, ty, Depth::TopLevel, kinding, Side::Golden) {
        Ok(canon) => canon,
        Err(why) => panic!("{why}"),
    }
}

fn untyped(v: &Value) -> Canon {
    match canon_scalar(v, Egress::Json) {
        Ok(canon) => canon,
        Err(why) => panic!("{why}"),
    }
}

/// The review finding, pinned from BOTH sides so neither half can drift: the
/// untyped rule — still used, but only as an ORDERING key — reads `"22201"`
/// and `22201` as the same value, which is exactly why value equality had to
/// move onto the declared type.
#[test]
fn the_untyped_rule_is_permissive_and_the_typed_one_is_not() {
    assert_eq!(
        untyped(&json!("22201")),
        untyped(&json!(22201)),
        "the ordering key is deliberately permissive"
    );
    assert_ne!(
        canon(&json!("22201"), &text()),
        canon(&json!(22201), &text()),
        "a `text` value must never equal a number"
    );
    assert_eq!(
        canon(&json!("22201"), &int()),
        canon(&json!(22201), &int()),
        "a numeric column must still pair the dump's string spelling with the \
         CLI's number"
    );
}

/// Review finding R1, pinned from both sides. The cross-kind numeric reading
/// is scoped to the positions `sstabledump` stringifies
/// (`writeString(type.getString(v))`), so an ORDINARY numeric cell — which the
/// dump writes with `writeRawValue(type.toJSONString(v))`, i.e. as a JSON
/// number — must compare by KIND as well as by value.
#[test]
fn a_numeric_cell_outside_a_stringified_position_compares_by_kind_too() {
    assert_ne!(
        canon_natural(&json!(1), &int()),
        canon_natural(&json!("1"), &int()),
        "an ordinary int cell rendered `\"1\"` is a divergence from the dump's `1`"
    );
    assert_eq!(
        canon(&json!(1), &int()),
        canon(&json!("1"), &int()),
        "a partition key / cell path IS stringified by the dump, so there the \
         two spellings denote the same value"
    );
    // CSV carries no JSON kinds at all, so the value comparison stands
    // whatever the kinding says.
    for kinding in [Kinding::Natural, Kinding::Stringified] {
        assert_eq!(
            canon_typed(
                &json!(1),
                Egress::Csv,
                &int(),
                Depth::TopLevel,
                kinding,
                Side::Golden
            )
            .expect("number"),
            canon_typed(
                &json!("1"),
                Egress::Csv,
                &int(),
                Depth::TopLevel,
                kinding,
                Side::Golden
            )
            .expect("text"),
            "every CSV cell arrives as text, so `1` and `\"1\"` are one value"
        );
    }
}

/// Review finding K1, pinned on the reason text of the one format-scoped gap
/// in the lane.
///
/// The `set<double>` gap is a property of JSON's VALUE VOCABULARY, not of the
/// value: JSON has no literal for `Infinity`/`-Infinity`/`NaN`, so the JSON
/// egress renders them `null` (measured on
/// `test_signed_coll.signed_special_collections`) and the value is lost. CSV
/// renders every cell as text and carries the same three tokens the golden
/// names, so nothing is lost there and the column must stay compared.
///
/// Expectations are the GOLDEN's own tokens (`sstabledump` writes a
/// non-frozen `set<double>`'s elements as the cell `path`, i.e.
/// `writeString(DoubleType.getString(v))` → `"Infinity"`, `"NaN"`, `"-0.0"`)
/// and the CSV egress's measured field text; nothing here is derived from
/// CQLite's JSON output being correct.
#[test]
fn the_float_special_value_gap_is_a_json_vocabulary_gap_not_a_value_gap() {
    let double = CqlType::Numeric("double".to_string());
    let canon_in = |v: &Value, egress: Egress| {
        canon_typed(
            v,
            egress,
            &double,
            Depth::Inside,
            Kinding::Stringified,
            Side::Golden,
        )
        .expect("a set<double> element canonicalizes")
    };
    for token in ["Infinity", "-Infinity", "NaN"] {
        // `null` is a DIFFERENT value from the token the golden names, in
        // EITHER format — which is why the JSON gap is a real gap and why a
        // CSV egress that ever regressed to `null` would be caught.
        for egress in [Egress::Json, Egress::Csv] {
            assert_ne!(
                canon_in(&json!(token), egress),
                canon_in(&Value::Null, egress),
                "{egress:?}: `null` must never satisfy the golden's `{token}`"
            );
        }
        // The token itself survives the CSV text projection unchanged, so the
        // CSV lane can compare it: it is not read as a number and not coerced.
        assert_eq!(
            canon_in(&json!(token), Egress::Csv),
            Canon::Text(token.to_string()),
            "CSV must carry `{token}` as the opaque token it is"
        );
    }
    // The measured CSV spellings of the signed zeros beside them: `-0e0`/`0e0`
    // against the golden's `-0.0`/`0.0`. Same value, and the sign is NOT
    // collapsed.
    assert_eq!(
        canon_in(&json!("-0.0"), Egress::Csv),
        canon_in(&json!("-0e0"), Egress::Csv),
        "`-0e0` is the same double as the golden's `-0.0`"
    );
    assert_ne!(
        canon_in(&json!("0.0"), Egress::Csv),
        canon_in(&json!("-0e0"), Egress::Csv),
        "Cassandra distinguishes -0.0 from 0.0, so the canonicalization must too"
    );
}

#[test]
fn zero_padding_survives_in_text_and_not_in_a_number() {
    assert_ne!(canon(&json!("00000"), &text()), canon(&json!("0"), &text()));
    assert_eq!(canon(&json!("00000"), &int()), canon(&json!("0"), &int()));
}

/// The timestamp normalization is bound to the timestamp TYPE: a `text` column
/// holding a timestamp spelling is still compared exactly.
#[test]
fn a_timestamp_is_canonicalized_only_for_a_timestamp_column() {
    let dump = json!("2025-01-15 10:00:00.000Z");
    let cli = json!("2025-01-15 10:00:00.000+0000");
    assert_eq!(
        canon(&dump, &CqlType::Timestamp),
        canon(&cli, &CqlType::Timestamp)
    );
    assert_ne!(
        canon(&dump, &text()),
        canon(&cli, &text()),
        "two spellings of an instant are NOT the same `text` value"
    );
    // A non-zero offset stays opaque rather than being silently shifted.
    assert_ne!(
        canon(&dump, &CqlType::Timestamp),
        canon(&json!("2025-01-15 10:00:00.000+0100"), &CqlType::Timestamp)
    );
}

/// Exact decimal text, with no `f64` round-trip: the `set<decimal>` fixture
/// carries 30-digit values.
#[test]
fn a_long_decimal_keeps_every_digit() {
    let long = "123456789012345678901234567890.000000000000000000000000000001";
    assert_eq!(
        normalize_decimal(long).as_deref(),
        Some(long),
        "a 30-digit decimal must survive canonicalization"
    );
    assert_eq!(normalize_decimal("-0.0").as_deref(), Some("-0"));
    assert_eq!(normalize_decimal("1e3").as_deref(), Some("1000"));
    assert_eq!(
        normalize_decimal("1e999999999"),
        None,
        "an unbounded exponent is refused, not padded"
    );
    assert_eq!(normalize_decimal("0x1f"), None);
    assert_eq!(normalize_decimal("NaN"), None);
}

/// A blob/uuid value is opaque text on both sides — no numeric reading, ever.
#[test]
fn a_blob_is_compared_exactly() {
    assert_eq!(
        canon(&json!("0x00ff"), &CqlType::Blob),
        Canon::Text("0x00ff".to_string())
    );
    assert_ne!(
        canon(&json!("0x00ff"), &CqlType::Blob),
        canon(&json!("0x00FF"), &CqlType::Blob),
        "blob hex casing is a divergence, not a normalization"
    );
}

/// A container arriving where the schema declares a scalar is REPORTED, never
/// coerced.
#[test]
fn a_container_in_a_scalar_position_is_an_error() {
    let why = canon_typed(
        &json!([1, 2]),
        Egress::Json,
        &int(),
        Depth::TopLevel,
        Kinding::Natural,
        Side::Golden,
    )
    .expect_err("a container where the DDL says int must not canonicalize");
    assert!(why.contains("int"), "{why}");
}

/// Review finding F1, pinned from both sides.
///
/// A TOP-LEVEL CSV field genuinely cannot distinguish an absent value from an
/// empty `text` — the writer emits an empty field for both — so the two
/// canonicalize alike. INSIDE a container the format spells a null member
/// `null`, so an empty member and a null member are different renderings and
/// must NOT canonicalize alike; collapsing them there made a null UDT field
/// pass even if the CLI rendered it as empty text.
#[test]
fn the_csv_empty_field_rule_stops_at_the_top_level() {
    let empty = json!("");
    let null = json!(null);
    assert_eq!(
        canon_typed(
            &empty,
            Egress::Csv,
            &text(),
            Depth::TopLevel,
            Kinding::Natural,
            Side::Golden
        )
        .expect("empty text"),
        canon_typed(
            &null,
            Egress::Csv,
            &text(),
            Depth::TopLevel,
            Kinding::Natural,
            Side::Golden
        )
        .expect("null"),
        "a top-level CSV field has one spelling for both, so they must compare alike"
    );
    assert_ne!(
        canon_typed(
            &empty,
            Egress::Csv,
            &text(),
            Depth::Inside,
            Kinding::Natural,
            Side::Golden
        )
        .expect("empty member"),
        canon_typed(
            &null,
            Egress::Csv,
            &text(),
            Depth::Inside,
            Kinding::Natural,
            Side::Golden
        )
        .expect("null member"),
        "inside a container `{{f: }}` and `{{f: null}}` are distinguishable, so an \
         empty member must not canonicalize onto null"
    );
    // JSON keeps the distinction at every depth, which is what makes the CSV
    // top-level collapse a format property rather than a lost assertion.
    for depth in [Depth::TopLevel, Depth::Inside] {
        assert_ne!(
            canon_typed(
                &empty,
                Egress::Json,
                &text(),
                depth,
                Kinding::Natural,
                Side::Golden
            )
            .expect("empty text"),
            canon_typed(
                &null,
                Egress::Json,
                &text(),
                depth,
                Kinding::Natural,
                Side::Golden
            )
            .expect("null"),
            "JSON distinguishes `\"\"` from `null` at {depth:?}"
        );
    }
}

// =======================================================================
// The two NON-NUMERIC types whose `getString` spelling differs from their
// `toJSONString` one (issue #1491 review finding T1)
// =======================================================================

/// `boolean` is the numeric case's shape, and it USED TO RED A CORRECT CLI.
///
/// `cassandra-5.0.8` `BooleanSerializer.toString` returns `value.toString()`, so
/// `serializePartitionKey`'s `writeString(getString(v))` spells a boolean partition
/// key `"true"`, while `BooleanType.toJSONString` returns the same
/// `Boolean.toString()` through `writeRawValue`, so the CLI's own JSON — and every
/// non-stringified golden position — carries the raw `true`. Before this the golden's
/// `"true"` canonicalized as opaque text and could never equal a CLI boolean: a lane
/// that reds on correct input, which is the lane agents learn to waive.
#[test]
fn a_stringified_boolean_golden_equals_the_clis_json_boolean() {
    assert_eq!(
        canon(&json!("true"), &CqlType::Boolean),
        canon_natural(&json!(true), &CqlType::Boolean),
        "`writeString(BooleanType.getString(v))` and `writeRawValue(toJSONString(v))` \
         are two spellings of the same value"
    );
    assert_eq!(
        canon(&json!("false"), &CqlType::Boolean),
        canon_natural(&json!(false), &CqlType::Boolean),
    );
}

/// And the relaxation is a SPELLING relaxation only — the value still has to match,
/// so a genuinely wrong boolean fails.
#[test]
fn a_stringified_boolean_still_compares_by_value() {
    assert_ne!(
        canon(&json!("true"), &CqlType::Boolean),
        canon_natural(&json!(false), &CqlType::Boolean),
        "`\"true\"` must not equal `false`"
    );
    // A spelling `BooleanSerializer.toString` cannot emit stays opaque rather than
    // being coerced onto one of the two booleans.
    assert_eq!(
        canon(&json!("TRUE"), &CqlType::Boolean),
        Canon::Text("TRUE".to_string()),
    );
    assert_eq!(
        canon(&json!("1"), &CqlType::Boolean),
        Canon::Text("1".to_string()),
    );
}

/// The ASYMMETRY, which is what stops the relaxation from licensing a CLI spelling:
/// the CLI is held to [`Kinding::Natural`] at every position, so a CLI that rendered
/// a boolean column as the string `"true"` still fails — in BOTH egresses, because
/// the two new arms are keyed on `Kinding::Stringified` and not on the broader
/// `cross_kind` (which is unconditionally true for CSV).
#[test]
fn the_cli_may_not_spell_a_boolean_as_a_string() {
    assert_ne!(
        canon_natural(&json!(true), &CqlType::Boolean),
        canon_natural(&json!("true"), &CqlType::Boolean),
        "at a NATURAL position the two kinds are different renderings"
    );
    let cli_string = canon_typed(
        &json!("true"),
        Egress::Json,
        &CqlType::Boolean,
        Depth::TopLevel,
        Kinding::Natural,
        Side::Golden,
    )
    .expect("canonicalizes");
    assert_eq!(cli_string, Canon::Text("true".to_string()));
}

/// `blob` is the same family one step over: the divergence is in the SPELLING, not
/// the kind. `BytesSerializer.toString` is the bare lowercase hex `Hex.bytesToHex`
/// builds, so a stringified blob golden reads `"deadbeef"` (and the EMPTY blob reads
/// `""`), while `BytesType.toJSONString` returns `"0x" + <the same hex>`.
#[test]
fn a_stringified_blob_golden_is_read_as_the_0x_form_it_denotes() {
    assert_eq!(
        canon(&json!("deadbeef"), &CqlType::Blob),
        canon_natural(&json!("0xdeadbeef"), &CqlType::Blob),
    );
    assert_eq!(
        canon(&json!(""), &CqlType::Blob),
        canon_natural(&json!("0x"), &CqlType::Blob),
        "the empty blob: `getString` gives `\"\"`, `toJSONString` gives `\"0x\"`"
    );
    // Still a value comparison: different bytes still diverge.
    assert_ne!(
        canon(&json!("deadbeef"), &CqlType::Blob),
        canon_natural(&json!("0xdeadbeee"), &CqlType::Blob),
    );
}

/// A spelling `BytesSerializer.toString` cannot produce is left EXACT, so the
/// relaxation cannot absorb an unrelated blob rendering.
#[test]
fn a_blob_spelling_getstring_cannot_produce_stays_exact() {
    for spelling in ["0xdead", "DEAD", "dea", "0x", "nothex"] {
        assert_eq!(
            canon(&json!(spelling), &CqlType::Blob),
            Canon::Text(spelling.to_string()),
            "`{spelling}` is not bare lowercase even-length hex, so it stays opaque"
        );
    }
}

/// And the blob relaxation never reaches the CLI's own cells — the reason it is
/// keyed on `Kinding::Stringified` rather than on `cross_kind`. `cross_kind` is
/// unconditionally true for CSV, so keying it there would have made a CSV egress
/// that dropped the `0x` prefix compare EQUAL to the golden that carries it.
#[test]
fn the_blob_relaxation_does_not_reach_a_cli_csv_cell() {
    let cli_bare = canon_typed(
        &json!("deadbeef"),
        Egress::Csv,
        &CqlType::Blob,
        Depth::TopLevel,
        Kinding::Natural,
        Side::Golden,
    )
    .expect("canonicalizes");
    let golden_prefixed = canon_typed(
        &json!("0xdeadbeef"),
        Egress::Csv,
        &CqlType::Blob,
        Depth::TopLevel,
        Kinding::Natural,
        Side::Golden,
    )
    .expect("canonicalizes");
    assert_ne!(
        cli_bare, golden_prefixed,
        "a CSV cell that dropped the `0x` prefix must still fail"
    );
}

/// The PAIRING-key half of finding T1. `compare::describe` reads the UNTYPED
/// projection, so a key that cannot see through `sstabledump`'s two spellings pairs
/// the golden's stringified value against the wrong row and reports a divergence in
/// every column of both.
///
/// Pinned for all three relaxations, and against the typed rule, so the
/// permissiveness stays confined to the pairing key — `compare::row_order_divergence`
/// is typed (finding V2), and value EQUALITY always was.
#[test]
fn the_untyped_key_pairs_the_two_spellings_of_a_boolean_and_a_blob() {
    assert_eq!(untyped(&json!("true")), untyped(&json!(true)));
    assert_eq!(untyped(&json!("false")), untyped(&json!(false)));
    assert_eq!(untyped(&json!("1")), untyped(&json!(1)));
    assert_eq!(
        untyped(&json!("0xdeadbeef")),
        untyped(&json!("deadbeef")),
        "the `0x` spelling is normalized onto the bare hex `getString` emits"
    );
    assert_eq!(
        untyped(&json!("0x")),
        untyped(&json!("")),
        "and so is the empty blob's pair of spellings"
    );
    // A `0x` prefix on something that is not bare hex is left alone.
    assert_eq!(
        untyped(&json!("0xNOTHEX")),
        Canon::Text("0xNOTHEX".to_string())
    );
    // TYPED, the permissiveness is gone: a `text` column holding either spelling is
    // still compared as the exact string it is.
    assert_ne!(
        canon(&json!("true"), &text()),
        canon_natural(&json!(true), &text()),
        "a `text` column's `\"true\"` is not the boolean `true`"
    );
    assert_ne!(
        canon(&json!("0xdeadbeef"), &text()),
        canon(&json!("deadbeef"), &text()),
        "a `text` column keeps both spellings distinct"
    );
}
