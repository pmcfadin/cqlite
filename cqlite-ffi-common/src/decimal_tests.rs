//! Unit tests for the single DECIMAL rendering implementation (issue #1452).
//!
//! Coverage is the list the change's spec enumerates: empty `unscaled`;
//! `scale == 0`; a positive `scale` shorter than, equal to and longer than the
//! digit count; a negative `scale`; `scale == i32::MIN` and `scale == i32::MAX`;
//! negative unscaled values in each of those shapes; a magnitude at the
//! positional boundary and just past it; and a magnitude at and just past the
//! refusal ceiling.
//!
//! Digit counts asserted below for multi-kilobyte magnitudes were derived with
//! an INDEPENDENT bignum implementation (CPython's `int`) and cross-checked
//! analytically with `floor(log10(v)) + 1` — never read off this code's output.

use super::*;

/// `-123` as a single two's-complement byte (`256 - 123 == 133 == 0x85`).
const NEG_123: &[u8] = &[0x85];
/// `123` as a single byte.
const POS_123: &[u8] = &[123];
/// `1` as a single byte.
const ONE: &[u8] = &[0x01];

#[test]
fn empty_unscaled_is_zero() {
    assert_eq!(decimal_to_string(0, &[]), Ok("0".to_string()));
    // The zero value is scale-independent: there is no magnitude to shift.
    assert_eq!(decimal_to_string(7, &[]), Ok("0".to_string()));
    assert_eq!(decimal_to_string(-7, &[]), Ok("0".to_string()));
    assert_eq!(decimal_to_string(i32::MIN, &[]), Ok("0".to_string()));
}

#[test]
fn scale_zero_renders_the_bare_integer() {
    assert_eq!(decimal_to_string(0, POS_123), Ok("123".to_string()));
    assert_eq!(decimal_to_string(0, NEG_123), Ok("-123".to_string()));
}

#[test]
fn positive_scale_shorter_than_digit_count_inserts_a_point() {
    assert_eq!(decimal_to_string(2, POS_123), Ok("1.23".to_string()));
    assert_eq!(decimal_to_string(2, NEG_123), Ok("-1.23".to_string()));
}

#[test]
fn positive_scale_equal_to_digit_count_pads_one_leading_zero() {
    assert_eq!(decimal_to_string(3, POS_123), Ok("0.123".to_string()));
    assert_eq!(decimal_to_string(3, NEG_123), Ok("-0.123".to_string()));
}

#[test]
fn positive_scale_longer_than_digit_count_pads_leading_zeros() {
    assert_eq!(decimal_to_string(5, POS_123), Ok("0.00123".to_string()));
    assert_eq!(decimal_to_string(5, NEG_123), Ok("-0.00123".to_string()));
}

#[test]
fn negative_scale_renders_exponent_form() {
    assert_eq!(decimal_to_string(-2, POS_123), Ok("123e2".to_string()));
    assert_eq!(decimal_to_string(-2, NEG_123), Ok("-123e2".to_string()));
}

/// A huge positive `scale` used to drive an unbounded `format!` padding width
/// (a PANIC, "Formatting argument out of range"). It is NOT corrupt — scale is
/// just the decimal exponent — so it renders faithfully in exponent form.
#[test]
fn i32_max_scale_renders_exponent_form() {
    assert_eq!(
        decimal_to_string(i32::MAX, ONE),
        Ok(format!("1e{}", -(i32::MAX as i64)))
    );
    assert_eq!(
        decimal_to_string(i32::MAX, NEG_123),
        Ok(format!("-123e{}", -(i32::MAX as i64)))
    );
}

/// `scale == i32::MIN` exercises the `-(scale as i64)` widening: a plain
/// `-scale` overflow-panics under `overflow-checks`.
#[test]
fn i32_min_scale_renders_exponent_form_without_overflow() {
    assert_eq!(
        decimal_to_string(i32::MIN, ONE),
        Ok(format!("1e{}", -(i32::MIN as i64)))
    );
    assert_eq!(
        decimal_to_string(i32::MIN, NEG_123),
        Ok(format!("-123e{}", -(i32::MIN as i64)))
    );
}

/// A large-but-representable scale just under the exponent-form threshold still
/// renders positionally, so the policy boundary is not accidentally widened.
#[test]
fn large_representable_scale_still_renders_positionally() {
    let rendered = decimal_to_string(100, ONE).expect("scale 100 is representable");
    // "0." followed by 99 zeros then "1" == 102 characters.
    assert_eq!(rendered.len(), 102);
    assert!(rendered.starts_with("0.0") && rendered.ends_with('1'));
    assert_eq!(
        decimal_to_string(DECIMAL_MAX_SCALE_DIGITS as i32, ONE)
            .expect("|scale| == the threshold is still positional")
            .len(),
        DECIMAL_MAX_SCALE_DIGITS + 2,
    );
    // One past the threshold switches to exponent form.
    assert_eq!(
        decimal_to_string(DECIMAL_MAX_SCALE_DIGITS as i32 + 1, ONE),
        Ok(format!("1e-{}", DECIMAL_MAX_SCALE_DIGITS + 1))
    );
}

/// A magnitude exactly at the positional threshold renders POSITIONALLY, and one
/// byte past it switches to exponent form. `0x7f`-filled keeps the high bit
/// clear, so the value is a large positive one (all-`0xff` would be `-1`).
#[test]
fn positional_boundary_is_exact() {
    let at = vec![0x7f; DECIMAL_POSITIONAL_MAX_BYTES];
    let rendered = decimal_to_string(2, &at).expect("at the threshold must render");
    // 2466 digits (CPython `int`, cross-checked with floor(log10(v))+1), of
    // which the last two are the fractional part.
    assert_eq!(rendered.len(), 2466 + 1, "expected `<2464>.<2>` positional form");
    assert_eq!(rendered.matches('.').count(), 1);
    assert!(!rendered.contains('e'));

    let past = vec![0x7f; DECIMAL_POSITIONAL_MAX_BYTES + 1];
    let rendered = decimal_to_string(2, &past).expect("one past the threshold must render");
    let digits = rendered
        .strip_suffix("e-2")
        .expect("one byte past the threshold must use exponent form");
    assert_eq!(digits.len(), 2469);
    assert!(digits.chars().all(|c| c.is_ascii_digit()));
}

/// The one behaviour the #1741 guard exists to protect, restated for the shared
/// implementation: a large-but-WELL-FORMED magnitude renders faithfully (every
/// digit preserved) instead of being misclassified as corruption. 2000 bytes
/// with `scale = 3` is the exact input on which the two old implementations
/// disagreed (Node rendered, Python raised) — see `CHANGELOG.md`.
#[test]
fn large_well_formed_magnitude_renders_with_full_precision() {
    let unscaled = vec![0x7f; 2000];
    let rendered = decimal_to_string(3, &unscaled).expect("a well-formed value must render");
    let digits = rendered
        .strip_suffix("e-3")
        .expect("a 2000-byte magnitude uses exponent form");
    assert_eq!(digits.len(), 4817);
    assert!(digits.chars().all(|c| c.is_ascii_digit()));
}

/// A negative large magnitude keeps its sign through the exponent-form branch.
#[test]
fn large_negative_magnitude_keeps_its_sign() {
    // 0xff-filled is -1; make a genuinely large negative by clearing low bytes:
    // 0x80 followed by zeros is the most-negative value of that width.
    let mut unscaled = vec![0u8; 2000];
    unscaled[0] = 0x80;
    let rendered = decimal_to_string(3, &unscaled).expect("a well-formed value must render");
    let digits = rendered
        .strip_prefix('-')
        .and_then(|r| r.strip_suffix("e-3"))
        .expect("expected a negative exponent-form rendering");
    // -2^15999 has 4817 digits (CPython `int`).
    assert_eq!(digits.len(), 4817);
    assert!(digits.chars().all(|c| c.is_ascii_digit()));
}

/// A magnitude exactly AT the refusal ceiling is well-formed and must render.
/// This is the single superlinear conversion the ceiling exists to bound, so the
/// test also pins that it stays comfortably fast.
#[test]
fn magnitude_at_the_ceiling_renders() {
    let unscaled = vec![0x7f; DECIMAL_MAX_UNSCALED_BYTES];
    let rendered = decimal_to_string(0, &unscaled).expect("at the ceiling must render");
    // 78913 digits (CPython `int`, cross-checked analytically).
    assert_eq!(rendered.len(), 78913);
    assert!(rendered.chars().all(|c| c.is_ascii_digit()));
}

/// One byte past the ceiling fails closed with the typed error, carrying the
/// scale, the length and the ceiling.
#[test]
fn magnitude_past_the_ceiling_is_refused() {
    let unscaled = vec![0x7f; DECIMAL_MAX_UNSCALED_BYTES + 1];
    assert_eq!(
        decimal_to_string(3, &unscaled),
        Err(DecimalError::UnscaledTooLarge {
            scale: 3,
            unscaled_len: DECIMAL_MAX_UNSCALED_BYTES + 1,
            max_unscaled_bytes: DECIMAL_MAX_UNSCALED_BYTES,
        })
    );
}

/// The refusal is an O(1) length check, taken BEFORE the base conversion: a
/// ~415 KB magnitude must be rejected fast, not converted and then rejected.
#[test]
fn refusal_is_taken_before_the_base_conversion() {
    let unscaled = vec![0xff; 415_000];
    let start = std::time::Instant::now();
    let err = decimal_to_string(0, &unscaled).expect_err("beyond the ceiling must fail closed");
    let elapsed = start.elapsed();
    assert!(matches!(err, DecimalError::UnscaledTooLarge { .. }));
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "expected an O(1) rejection, took {elapsed:?} — the base conversion ran"
    );
}

/// The refusal message is the ONE spelling both bindings surface, and it names
/// all three facts the spec requires.
#[test]
fn refusal_message_names_scale_length_and_ceiling() {
    let err = DecimalError::UnscaledTooLarge {
        scale: 3,
        unscaled_len: 40_000,
        max_unscaled_bytes: DECIMAL_MAX_UNSCALED_BYTES,
    };
    let message = err.to_string();
    assert!(message.contains("scale=3"), "{message}");
    assert!(message.contains("unscaled_len=40000 bytes"), "{message}");
    assert!(message.contains("max_unscaled=32768 bytes"), "{message}");
    assert!(message.contains("corrupt SSTable"), "{message}");
}

/// Regression, issue #1452: a well-formed value whose `scale` sits between the
/// `core::fmt` width limit (`u16::MAX`) and the exponent-form threshold used to
/// PANIC ("Formatting argument out of range") because the positional branch fed
/// `scale` to a `{:0>width$}` format spec. Below the ceiling the render must be
/// infallible, so every scale in that band renders.
#[test]
fn scales_above_the_format_width_limit_still_render_positionally() {
    for scale in [
        u16::MAX as i32,
        u16::MAX as i32 + 1,
        100_000,
        DECIMAL_MAX_SCALE_DIGITS as i32,
    ] {
        let rendered = decimal_to_string(scale, ONE)
            .unwrap_or_else(|e| panic!("scale {scale} must render, got {e}"));
        assert_eq!(
            rendered.len(),
            scale as usize + 2,
            "scale {scale} must render as `0.` + {scale} digits"
        );
        assert!(rendered.starts_with("0.0") && rendered.ends_with('1'));
    }
}
