//! Unit tests for the single JSON-number classification (issue #3505).
//!
//! Every case parses from **JSON TEXT** with `serde_json::from_str` rather than
//! hand-building a `Number`, so the test exercises the real parse → classify
//! path a binding sees — including the parser's own integer-overflow fallback,
//! which is where the residual this module cannot fix comes from.
//!
//! Expectations were derived from Rust's own integer/float literals and from
//! CPython (`int` is arbitrary precision), never from this module's output.

use super::*;

/// Parse a JSON number literal exactly as a binding's cell decode would.
fn num(text: &str) -> serde_json::Number {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("`{text}` must be legal JSON: {e}"))
}

fn class(text: &str) -> JsonNumberClass {
    classify_json_number(&num(text))
}

// =============================================================================
// The boundaries issue #3505 enumerates
// =============================================================================

#[test]
fn i64_max_classifies_as_i64_exactly() {
    assert_eq!(class("9223372036854775807"), JsonNumberClass::I64(i64::MAX));
}

#[test]
fn i64_max_plus_one_classifies_as_u64_not_f64() {
    // The first value the old `as_i64() -> as_f64()` fallthrough lost.
    assert_eq!(
        class("9223372036854775808"),
        JsonNumberClass::U64(i64::MAX as u64 + 1)
    );
}

#[test]
fn two_pow_53_classifies_as_i64() {
    assert_eq!(
        class("9007199254740992"),
        JsonNumberClass::I64(9_007_199_254_740_992)
    );
}

#[test]
fn two_pow_53_plus_one_classifies_as_i64_and_keeps_the_odd_bit() {
    // Inside `i64`, so this one was never lost — but it IS the value that proves
    // an f64 cannot be the carrier: `9007199254740993 as f64` is
    // `9007199254740992.0`.
    assert_eq!(
        class("9007199254740993"),
        JsonNumberClass::I64(9_007_199_254_740_993)
    );
    assert_eq!(9_007_199_254_740_993_i64 as f64, 9_007_199_254_740_992.0);
}

#[test]
fn u64_max_classifies_as_u64_exactly() {
    assert_eq!(
        class("18446744073709551615"),
        JsonNumberClass::U64(u64::MAX)
    );
}

#[test]
fn i64_min_classifies_as_i64_exactly() {
    assert_eq!(
        class("-9223372036854775808"),
        JsonNumberClass::I64(i64::MIN)
    );
}

#[test]
fn zero_and_a_negative_classify_as_i64() {
    assert_eq!(class("0"), JsonNumberClass::I64(0));
    assert_eq!(class("-1"), JsonNumberClass::I64(-1));
    assert_eq!(class("-42"), JsonNumberClass::I64(-42));
}

#[test]
fn a_genuine_float_literal_classifies_as_f64() {
    assert_eq!(class("1.5"), JsonNumberClass::F64(1.5));
    assert_eq!(class("-0.25"), JsonNumberClass::F64(-0.25));
    assert_eq!(class("1e308"), JsonNumberClass::F64(1e308));
}

#[test]
fn the_lexical_form_decides_not_the_value() {
    // `1e19` is INTEGRAL in value but a float LITERAL in JSON, so it is an
    // `f64` by construction and there is nothing to preserve. Contrast with the
    // integer literal of the same magnitude, which stays exact.
    assert_eq!(class("1e19"), JsonNumberClass::F64(1e19));
    assert_eq!(
        class("10000000000000000000"),
        JsonNumberClass::U64(10_000_000_000_000_000_000)
    );
}

// =============================================================================
// The defect, pinned directly
// =============================================================================

/// The #3505 regression pin, named for the loss it prevents.
///
/// If `classify_json_number` ever reorders its arms so `as_f64()` runs before
/// `as_u64()`, `u64::MAX` classifies as `F64(1.8446744073709552e19)` and 5 of
/// its 20 digits are gone. This test states that number explicitly, so the
/// failure message names the precision loss instead of just an enum variant.
#[test]
fn u64_max_must_not_classify_as_f64_which_would_round_it_to_1_8446744073709552e19() {
    let lossy = u64::MAX as f64;
    assert_eq!(
        lossy, 1.8446744073709552e19,
        "premise: this is the rounding"
    );
    // The premise, stated exactly: that f64's mathematical value is
    // 18446744073709551616 — one MORE than u64::MAX, which it cannot represent.
    // (A Rust `f64 as u64` cast SATURATES, so it is not a witness of the loss;
    // the decimal expansion is.)
    assert_eq!(
        format!("{lossy:.0}"),
        "18446744073709551616",
        "premise: the f64 is not u64::MAX"
    );

    let classified = class("18446744073709551615");
    assert_ne!(
        classified,
        JsonNumberClass::F64(lossy),
        "u64::MAX must NOT be classified as f64: that silently delivers \
         1.8446744073709552e19 instead of 18446744073709551615 (issue #3505)"
    );
    assert_eq!(classified, JsonNumberClass::U64(u64::MAX));
}

/// Every value in the `u64` range must survive classification bit-for-bit.
///
/// Compared against the `u64` LITERAL, never against a float, so a lossy
/// classification cannot pass by comparing equal after its own rounding.
#[test]
fn every_u64_range_boundary_round_trips_exactly() {
    let cases: [(&str, u64); 6] = [
        ("9223372036854775808", 9_223_372_036_854_775_808),
        ("9223372036854775809", 9_223_372_036_854_775_809),
        ("18446744073709551614", 18_446_744_073_709_551_614),
        ("18446744073709551615", u64::MAX),
        ("10000000000000000001", 10_000_000_000_000_000_001),
        ("12345678901234567890", 12_345_678_901_234_567_890),
    ];
    for (text, expected) in cases {
        match class(text) {
            JsonNumberClass::U64(u) => assert_eq!(u, expected, "`{text}` must be exact"),
            other => panic!("`{text}` classified as {other:?}, expected U64({expected})"),
        }
        // And the digits are recoverable, which is the property the host `int`
        // ultimately relies on.
        assert_eq!(expected.to_string(), text);
    }
}

// =============================================================================
// `Beyond`: unreachable in this build, asserted rather than faked
// =============================================================================

/// `Beyond` cannot be produced without `arbitrary_precision`, and this test says
/// so by MEASUREMENT rather than by comment.
///
/// Each input below is outside `[i64::MIN, u64::MAX]`, so it is exactly the
/// input a `Beyond` case would need — and `serde_json`'s parser has already
/// collapsed every one of them to an `f64` before this code runs. The loss is
/// the parser's, and no binding-side change can recover it (see the module docs,
/// AC6).
#[test]
fn beyond_is_unreachable_because_the_parser_collapses_overflow_to_f64() {
    for text in [
        "18446744073709551616",            // u64::MAX + 1
        "-9223372036854775809",            // i64::MIN - 1
        "123456789012345678901234567890",  // far beyond either
        "-123456789012345678901234567890", // and negative
    ] {
        match class(text) {
            JsonNumberClass::F64(_) => {}
            other => panic!(
                "`{text}` classified as {other:?}, not F64. NOTE: enabling \
                 `arbitrary_precision` would NOT produce this — under that \
                 feature `as_f64()` re-parses the stored text and still answers \
                 `Some(_)` for anything inside f64 range, so these inputs stay \
                 F64 (see json_number.rs). A `Beyond` here means an \
                 exact-integer parse was added AHEAD of the `as_f64()` arm, or \
                 serde_json's parser changed; either way the module docs and \
                 the AC6 decision need revisiting"
            ),
        }
    }
}

/// `1e400` is not even legal input: the parser refuses it.
#[test]
fn a_float_beyond_f64_range_never_reaches_the_classifier() {
    assert!(serde_json::from_str::<serde_json::Number>("1e400").is_err());
    assert!(serde_json::from_str::<serde_json::Number>("-1e400").is_err());
}

/// The `Beyond` payload interpretation, tested directly on its own helper since
/// the enum arm itself is unreachable. This is what makes the arm CORRECT if
/// `arbitrary_precision` is ever turned on.
#[test]
fn beyond_text_to_bigint_parses_exact_integers_and_refuses_everything_else() {
    let big = beyond_text_to_bigint("123456789012345678901234567890")
        .expect("an exact integer literal must parse");
    assert_eq!(big.to_string(), "123456789012345678901234567890");

    let negative = beyond_text_to_bigint("-18446744073709551616").expect("negatives too");
    assert_eq!(negative.to_string(), "-18446744073709551616");

    // Not exact integers: the caller must refuse, never substitute a float.
    assert!(beyond_text_to_bigint("1e400").is_none());
    assert!(beyond_text_to_bigint("1.5").is_none());
    assert!(beyond_text_to_bigint("").is_none());
    assert!(beyond_text_to_bigint("not a number").is_none());
}

/// The napi word form must be a faithful PROJECTION of the `BigInt`, so the two
/// bindings deliver the same value. Reassembled with `num-bigint` rather than
/// re-derived, so the check is independent of the projection code.
#[test]
fn beyond_word_form_reassembles_to_the_same_bigint() {
    for text in [
        "0",
        "1",
        "-1",
        "18446744073709551616",
        "-18446744073709551616",
        "123456789012345678901234567890",
        "-123456789012345678901234567890",
    ] {
        let expected = beyond_text_to_bigint(text).expect("exact integer literal");
        let (is_negative, words) =
            beyond_text_to_sign_and_le_words(text).expect("same acceptance as the BigInt form");
        assert_eq!(
            is_negative,
            expected.sign() == num_bigint::Sign::Minus,
            "`{text}` sign"
        );
        let digits: Vec<u32> = words
            .iter()
            .flat_map(|w| [*w as u32, (*w >> 32) as u32])
            .collect();
        let sign = if words.iter().all(|w| *w == 0) {
            num_bigint::Sign::NoSign
        } else if is_negative {
            num_bigint::Sign::Minus
        } else {
            num_bigint::Sign::Plus
        };
        assert_eq!(
            num_bigint::BigInt::from_slice(sign, &digits),
            expected,
            "`{text}` must reassemble exactly"
        );
    }
    // Refused inputs are refused identically by both shapes.
    assert!(beyond_text_to_sign_and_le_words("1.5").is_none());
    assert!(beyond_text_to_sign_and_le_words("1e400").is_none());
}

#[test]
fn the_refusal_message_names_the_value_and_the_issue() {
    let msg = beyond_range_message("123456789012345678901234567890");
    assert!(msg.contains("123456789012345678901234567890"), "{msg}");
    assert!(msg.contains("#3505"), "{msg}");
}
