//! EXACT decimal arithmetic for the float tie-break declared gap (issue #3777).
//!
//! Split into its own file under the campsite rule: proving that an f32 sits
//! exactly midway between two decimal spellings is an ARITHMETIC question, and
//! `golden_value_compare_gap.rs` only asks it. Nothing here reads a `Value`, a
//! schema or an egress.
//!
//! Every entry point is TOTAL and FAILS CLOSED: a text outside the accepted
//! grammar, a digit count or exponent beyond the stated bounds, or any comparison
//! that cannot be decided yields `None`/`false` — "I cannot tell" — which the
//! caller must treat as "not this gap".

use num_bigint::BigInt;

/// An exact decimal, as an integer significand and a base-10 scale: the value is
/// `digits * 10^-scale`, with NO rounding anywhere. `num_bigint::BigInt` because
/// the quantities genuinely do not fit a machine integer: an f32's dyadic
/// expansion reaches `2^104` and its subnormals `10^-45`, so `i128` would silently
/// overflow exactly where the answer matters (CLAUDE.md's pre-roborev list
/// prescribes `BigInt` for unscaled decimal math for this reason).
pub struct ExactDecimal {
    digits: BigInt,
    scale: u32,
}

/// The accepted grammar, deliberately narrow: `-?<digits>[.<digits>][eE[+-]<digits>]`,
/// ASCII digits only, at least one mantissa digit, and no leading `+` (neither
/// formatter emits one). Anything else is refused rather than guessed at.
const MAX_MANTISSA_DIGITS: usize = 64;
/// A finite f32 lives within roughly `1e-45 .. 1e39`, so no formatter spelling of
/// one needs an exponent anywhere near this. The bound exists so a hostile or
/// corrupt text cannot make this predicate allocate a `10^huge`.
const MAX_ABS_EXPONENT: i64 = 400;

impl ExactDecimal {
    /// Read a decimal text EXACTLY, or refuse. `None` is "I cannot decide this",
    /// which every caller must treat as "not this gap".
    pub fn parse(text: &str) -> Option<Self> {
        let (negative, rest) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text),
        };
        let (mantissa, exponent) = match rest.split_once(['e', 'E']) {
            Some((m, e)) => {
                let e = e.strip_prefix('+').unwrap_or(e);
                let parsed: i64 = e.parse().ok()?;
                if parsed.abs() > MAX_ABS_EXPONENT {
                    return None;
                }
                (m, parsed)
            }
            None => (rest, 0),
        };
        let (int_part, frac_part) = match mantissa.split_once('.') {
            Some((i, f)) => (i, f),
            None => (mantissa, ""),
        };
        let all_digits = format!("{int_part}{frac_part}");
        if all_digits.is_empty()
            || all_digits.len() > MAX_MANTISSA_DIGITS
            || !all_digits.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let mut digits = BigInt::parse_bytes(all_digits.as_bytes(), 10)?;
        if negative {
            digits = -digits;
        }
        // scale = fraction digits - exponent; a NEGATIVE scale is normalised away
        // by scaling the significand up, so `scale` is always a plain power of ten
        // in the denominator.
        let scale = i64::try_from(frac_part.len()).ok()?.checked_sub(exponent)?;
        if scale < 0 {
            let shift = u32::try_from(-scale).ok()?;
            digits *= BigInt::from(10u8).pow(shift);
            return Some(Self { digits, scale: 0 });
        }
        Some(Self {
            digits,
            scale: u32::try_from(scale).ok()?,
        })
    }

    /// Is `v` EXACTLY the arithmetic mean of these two decimals, `v == (self + other) / 2`,
    /// AND are the two decimals two DIFFERENT values? `None` means undecidable.
    ///
    /// No division and no floats. An f32 is a dyadic rational `m * 2^e` with
    /// integer `m`, `e`; each decimal is `D * 10^-k`. With `k = max(k1, k2)` and
    /// `s = D1*10^(k-k1) + D2*10^(k-k2)`, the claim `2v == s * 10^-k` is
    /// `m * 5^k * 2^(e+1+k) == s`, and the negative-power case is cleared by
    /// multiplying the OTHER side instead — so every comparison is between two
    /// exact integers.
    pub fn is_exact_midpoint_of(&self, other: &Self, v: f32) -> Option<bool> {
        let k = self.scale.max(other.scale);
        let lifted = |d: &Self| -> BigInt { &d.digits * BigInt::from(10u8).pow(k - d.scale) };
        let (a, b) = (lifted(self), lifted(other));
        if a == b {
            // Two spellings of ONE exact value (`-0.0` vs `-0`, `1.0` vs `1`).
            // Nothing is being approximated, so there is no tie to break.
            return Some(false);
        }
        let sum = a + b;
        let (m, e) = f32_as_dyadic(v);
        let t = e.checked_add(1)?.checked_add(i32::try_from(k).ok()?)?;
        let mut lhs = m * BigInt::from(5u8).pow(k);
        let mut rhs = sum;
        let shift = usize::try_from(t.unsigned_abs()).ok()?;
        if t >= 0 {
            lhs <<= shift;
        } else {
            rhs <<= shift;
        }
        Some(lhs == rhs)
    }
}

/// A finite f32 as the exact dyadic rational `m * 2^e`, sign carried by `m`.
/// Subnormals (biased exponent 0) have no implicit leading bit and a fixed
/// exponent; every other finite value carries the implicit `1`.
fn f32_as_dyadic(v: f32) -> (BigInt, i32) {
    let bits = v.to_bits();
    let negative = bits >> 31 == 1;
    let biased = ((bits >> 23) & 0xff) as i32;
    let frac = u64::from(bits & 0x007f_ffff);
    let (magnitude, exponent) = if biased == 0 {
        (frac, -149)
    } else {
        (frac | 0x0080_0000, biased - 127 - 23)
    };
    let m = BigInt::from(magnitude);
    (if negative { -m } else { m }, exponent)
}
