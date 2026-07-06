//! Total ordering for IEEE-754 floats matching Apache Cassandra.
//!
//! Cassandra orders `float`/`double` columns (ORDER BY, MIN, MAX, and
//! partition/clustering comparison) with Java's `Double.compare` /
//! `Float.compare` contract, which is a *total* order:
//!
//! * `NaN` sorts **last** — greater than every other value, including
//!   `+Infinity`. All `NaN` bit-patterns compare **equal** to each other
//!   (Java canonicalises via `doubleToLongBits`).
//! * `-0.0 < +0.0` — the signed zeros are **distinct and ordered**, unlike the
//!   IEEE `==` used by Rust's `partial_cmp` (which reports them Equal).
//! * every other pair follows normal numeric order.
//!
//! We deliberately implement this explicitly rather than reuse
//! [`f64::total_cmp`] (which places *negative* NaN first and orders signed
//! zeros as `-0.0 < +0.0` but NaN-first, diverging from Java) or
//! [`f64::partial_cmp`] (which returns `None` on NaN and reports `-0.0 == +0.0`).
//! See the CLAUDE.md "float ordering vs Java" roborev-findings note and
//! issues #1870 / #2010.

use std::cmp::Ordering;

/// Compare two `f64` values with Cassandra/Java `Double.compare` total-order
/// semantics: `NaN` last (all `NaN`s equal), `-0.0 < +0.0`, else numeric.
#[inline]
pub(crate) fn cassandra_double_cmp(a: f64, b: f64) -> Ordering {
    match a.partial_cmp(&b) {
        // Finite (or infinite) comparison succeeded. `partial_cmp` reports
        // `-0.0 == +0.0`; Java distinguishes them, so break that tie by sign.
        Some(Ordering::Equal) => sign_bit_tiebreak(a, b),
        Some(ord) => ord,
        // `None` ⇒ at least one operand is NaN. NaN sorts last; two NaNs equal.
        None => match (a.is_nan(), b.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            // Unreachable: `partial_cmp` only yields `None` when a NaN is present.
            (false, false) => Ordering::Equal,
        },
    }
}

/// Compare two `f32` values with Cassandra/Java `Float.compare` semantics.
///
/// Widening `f32`→`f64` is lossless and preserves NaN-ness and the sign of
/// zero, so we reuse the `f64` comparator.
#[inline]
pub(crate) fn cassandra_float_cmp(a: f32, b: f32) -> Ordering {
    cassandra_double_cmp(a as f64, b as f64)
}

/// When two floats compare Equal under IEEE rules, Java still orders `-0.0`
/// before `+0.0` (their `doubleToLongBits` differ in the sign bit). Only the
/// signed zeros reach this branch with differing sign bits; equal non-zero
/// magnitudes always share a sign, so they stay `Equal`.
#[inline]
fn sign_bit_tiebreak(a: f64, b: f64) -> Ordering {
    match (a.is_sign_negative(), b.is_sign_negative()) {
        (true, false) => Ordering::Less,    // -0.0 < +0.0
        (false, true) => Ordering::Greater, // +0.0 > -0.0
        _ => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- f64 (CQL `double`) -------------------------------------------------

    #[test]
    fn nan_sorts_last_greater_than_infinity() {
        assert_eq!(
            cassandra_double_cmp(f64::NAN, f64::INFINITY),
            Ordering::Greater
        );
        assert_eq!(
            cassandra_double_cmp(f64::INFINITY, f64::NAN),
            Ordering::Less
        );
        assert_eq!(cassandra_double_cmp(f64::NAN, 1.0), Ordering::Greater);
        assert_eq!(cassandra_double_cmp(-1.0e300, f64::NAN), Ordering::Less);
    }

    #[test]
    fn all_nan_bit_patterns_compare_equal() {
        let quiet = f64::NAN;
        // A distinct (signalling-ish / negative) NaN bit-pattern.
        let other = f64::from_bits(0xFFF8_0000_0000_0001);
        assert!(other.is_nan());
        assert_eq!(cassandra_double_cmp(quiet, other), Ordering::Equal);
        assert_eq!(cassandra_double_cmp(other, quiet), Ordering::Equal);
        assert_eq!(cassandra_double_cmp(quiet, quiet), Ordering::Equal);
    }

    #[test]
    fn signed_zero_is_ordered() {
        assert_eq!(cassandra_double_cmp(-0.0, 0.0), Ordering::Less);
        assert_eq!(cassandra_double_cmp(0.0, -0.0), Ordering::Greater);
        assert_eq!(cassandra_double_cmp(-0.0, -0.0), Ordering::Equal);
        assert_eq!(cassandra_double_cmp(0.0, 0.0), Ordering::Equal);
    }

    #[test]
    fn normal_numeric_order_preserved() {
        assert_eq!(cassandra_double_cmp(1.0, 2.0), Ordering::Less);
        assert_eq!(cassandra_double_cmp(2.0, 1.0), Ordering::Greater);
        assert_eq!(cassandra_double_cmp(-1.0, 1.0), Ordering::Less);
        assert_eq!(cassandra_double_cmp(5.0, 5.0), Ordering::Equal);
        assert_eq!(
            cassandra_double_cmp(f64::NEG_INFINITY, f64::INFINITY),
            Ordering::Less
        );
    }

    /// The headline oracle: sorting the mixed input must yield
    /// `[-Inf, -0.0, +0.0, 1.0, +Inf, NaN, NaN]`.
    #[test]
    fn full_sort_matches_cassandra_oracle_f64() {
        let mut v = [
            1.0,
            f64::NAN,
            -0.0,
            0.0,
            f64::NEG_INFINITY,
            f64::INFINITY,
            f64::NAN,
        ];
        v.sort_by(|a, b| cassandra_double_cmp(*a, *b));

        assert_eq!(v[0], f64::NEG_INFINITY);
        assert!(
            v[1] == 0.0 && v[1].is_sign_negative(),
            "index 1 must be -0.0"
        );
        assert!(
            v[2] == 0.0 && v[2].is_sign_positive(),
            "index 2 must be +0.0"
        );
        assert_eq!(v[3], 1.0);
        assert_eq!(v[4], f64::INFINITY);
        assert!(v[5].is_nan(), "index 5 must be NaN");
        assert!(v[6].is_nan(), "index 6 must be NaN");
    }

    /// MIN over `{-0.0, +0.0}` must select `-0.0` (they are distinguished).
    #[test]
    fn min_of_signed_zeros_selects_negative_zero() {
        let data = [0.0_f64, -0.0_f64];
        let min = data
            .iter()
            .copied()
            .min_by(|a, b| cassandra_double_cmp(*a, *b))
            .unwrap();
        assert!(min == 0.0 && min.is_sign_negative(), "MIN must be -0.0");
    }

    /// MIN over data containing NaN must NOT return NaN (NaN is the max).
    #[test]
    fn min_ignores_nan_max_is_nan() {
        let data = [f64::NAN, 3.0, -2.0, f64::NAN, 10.0];
        let min = data
            .iter()
            .copied()
            .min_by(|a, b| cassandra_double_cmp(*a, *b))
            .unwrap();
        assert_eq!(min, -2.0);
        let max = data
            .iter()
            .copied()
            .max_by(|a, b| cassandra_double_cmp(*a, *b))
            .unwrap();
        assert!(max.is_nan(), "MAX must be NaN (sorts last)");
    }

    // ---- f32 (CQL `float`) --------------------------------------------------

    #[test]
    fn full_sort_matches_cassandra_oracle_f32() {
        let mut v = [
            1.0_f32,
            f32::NAN,
            -0.0,
            0.0,
            f32::NEG_INFINITY,
            f32::INFINITY,
            f32::NAN,
        ];
        v.sort_by(|a, b| cassandra_float_cmp(*a, *b));

        assert_eq!(v[0], f32::NEG_INFINITY);
        assert!(
            v[1] == 0.0 && v[1].is_sign_negative(),
            "index 1 must be -0.0"
        );
        assert!(
            v[2] == 0.0 && v[2].is_sign_positive(),
            "index 2 must be +0.0"
        );
        assert_eq!(v[3], 1.0);
        assert_eq!(v[4], f32::INFINITY);
        assert!(v[5].is_nan());
        assert!(v[6].is_nan());
    }

    #[test]
    fn f32_nan_last_and_signed_zero() {
        assert_eq!(
            cassandra_float_cmp(f32::NAN, f32::INFINITY),
            Ordering::Greater
        );
        assert_eq!(cassandra_float_cmp(-0.0, 0.0), Ordering::Less);
        assert_eq!(cassandra_float_cmp(f32::NAN, f32::NAN), Ordering::Equal);
    }
}
