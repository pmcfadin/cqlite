//! EXACT decimal canonicalization for the Parquet↔JSONL parity harness (#1490).
//!
//! # Why this module exists: `f64` cannot carry a CQL `decimal`
//!
//! The harness previously canonicalized a `decimal` cell as
//! `unscaled as f64 / 10^scale`, guarded by `|unscaled| < 2^53`. That guard is
//! exactly the trap it looks like a fix for: it makes the INTEGER conversion
//! exact and therefore appears to bound the error, while the SCALING DIVISION
//! re-introduces it. Measured: unscaled `9_007_199_254_740_001` and
//! `9_007_199_254_740_002` at scale 9 — both admitted by the `2^53` guard, one
//! unit apart — divide to the SAME double, so a one-unit `Decimal128`
//! corruption in the export compared EQUAL and the parity run PASSED. A harness
//! that can silently accept corrupted decimals is worse for decimals than no
//! harness, because it invites reliance it cannot support.
//!
//! So no value on either side of the decimal comparison passes through `f64`
//! here. A decimal is held as an [`ExactDecimal`] — an exact
//! unscaled-value/scale pair — and compared exactly.
//!
//! # Round 10: the GOLDEN side stopped going through `f64` too
//!
//! Round 4 fixed the EXPORT side but left the golden side receiving a double
//! from the shared comparator and RECOVERING a decimal from it (render at the
//! export scale, then check that neither one-unit neighbour parses to the same
//! double). That could not work in principle: `0.100000000000000001` and `0.1`
//! are the SAME `f64`, both neighbours of the recovered `0.1` round elsewhere,
//! so the recovery declared itself EXACT and canonicalized an eighteen-digit
//! literal as `0.1` — a lossy export would have compared EQUAL. Recovery from a
//! double is now GONE, not improved: `golden_text.rs` preserves the literal's
//! TEXT before the shared parser can turn it into a double, every golden decimal
//! is read by [`exact_from_text`], and `declared.rs` REFUSES a declared-`decimal`
//! position that still arrives as an `f64`.
//!
//! # SCALE EQUALITY: normalized (`1.10` == `1.1`), deliberately
//!
//! Two [`ExactDecimal`]s are equal iff they denote the same RATIONAL: trailing
//! zeros are stripped, so unscaled `110` scale 2 equals unscaled `11` scale 1.
//!
//! Justification, from the two FORMATS rather than from CQLite's output:
//!
//! * Cassandra's `decimal` is a `java.math.BigDecimal`, whose scale is a
//!   per-VALUE attribute; `sstabledump` prints it with `BigDecimal.toString`, so
//!   the golden literal carries whatever scale that row's value was written
//!   with — the corpus shows `10576.6` (scale 1) next to `10375.04` (scale 2) in
//!   ONE column.
//! * Arrow/Parquet's `DECIMAL` logical type carries ONE scale per COLUMN — it is
//!   a SCHEMA-level property of the column, not of the cell. A column holding
//!   mixed source scales therefore CANNOT reproduce them, whatever the writer
//!   does.
//!
//! Scale is thus not a comparable attribute across these two representations,
//! and a scale-preserving comparison would report a divergence on ordinary
//! correct data (`10576.6` vs a scale-9 column) — a false FAIL, not a real one.
//! What IS comparable, and what this harness asserts, is the denoted VALUE, to
//! the last digit. Normalization does not weaken that: stripping a trailing zero
//! is an exact division with a zero remainder, so distinct rationals stay
//! distinct. (A claim about scale FIDELITY would need its own oracle, and would
//! be about a Parquet format limitation, not an export defect.)

#![allow(dead_code)]

use super::canonical_jsonl::CanonicalValue;

/// The fixed `Decimal128` scale the Parquet export writes every `decimal`
/// column at, and therefore the greatest fractional precision an exported cell
/// can CARRY (see [`exact_from_text`]).
///
/// Mirrors `cqlite-core`'s `export::arrow_schema::DECIMAL_FIXED_SCALE`, which is
/// crate-private. It is used ONLY as a PRECISION BOUND, never as an authority
/// for a value: the bound is enforced against the scale actually read from the
/// exported file ([`exact_from_decimal128`] refuses a larger one), so a future
/// change to the export scale makes the harness REFUSE rather than silently
/// compare a literal the export could not have written.
pub const EXPORT_DECIMAL_SCALE: u32 = 9;

/// The largest scale this module will represent: `Decimal128`'s precision.
/// A larger scale cannot come from a `Decimal128(38, s)` column, so it is a
/// refusal rather than something to render.
const MAX_SCALE: u32 = 38;

/// An exact decimal: `unscaled × 10^-scale`, normalized so that equal rationals
/// have identical representations (see the module docs on scale equality).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExactDecimal {
    unscaled: i128,
    scale: u32,
}

impl ExactDecimal {
    /// Normalize `unscaled × 10^-scale`.
    ///
    /// All arithmetic is exact: `unscaled` is an `i128` by construction (a
    /// `Decimal128` unscaled value IS an `i128`; the golden-side literal parse
    /// is range-checked before it gets here), and stripping a trailing zero is a
    /// division by 10 with a zero remainder. `10^scale` is NEVER materialized,
    /// so an unbounded exponent cannot drive an allocation.
    pub fn new(unscaled: i128, scale: u32) -> Self {
        let mut unscaled = unscaled;
        let mut scale = scale;
        while scale > 0 && unscaled % 10 == 0 {
            unscaled /= 10;
            scale -= 1;
        }
        // A zero is zero at every scale.
        if unscaled == 0 {
            scale = 0;
        }
        Self { unscaled, scale }
    }

    /// An integer-valued decimal (scale 0) — e.g. an integer-shaped golden
    /// literal, which `sstabledump` writes for a whole-valued `decimal`.
    pub fn from_i128(v: i128) -> Self {
        Self::new(v, 0)
    }

    pub fn unscaled(&self) -> i128 {
        self.unscaled
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    /// Exact decimal text, normalized: `-10576.6`, `0`, `9007199.254740001`.
    ///
    /// Because the representation is normalized, this text is a CANONICAL form:
    /// two decimals denote the same rational iff their texts are identical.
    pub fn text(&self) -> String {
        let digits = self.unscaled.unsigned_abs().to_string();
        let sign = if self.unscaled < 0 { "-" } else { "" };
        if self.scale == 0 {
            return format!("{sign}{digits}");
        }
        let frac_len = self.scale as usize;
        let padded = if digits.len() <= frac_len {
            format!("{}{}", "0".repeat(frac_len + 1 - digits.len()), digits)
        } else {
            digits
        };
        let split = padded.len() - frac_len;
        format!("{sign}{}.{}", &padded[..split], &padded[split..])
    }

    /// The canonical value both sides of the comparison land on.
    ///
    /// A `Text` carrying the exact decimal text, tagged so a diff message
    /// cannot be mistaken for a string cell and so a `decimal` can never
    /// compare equal to a `text` column that happens to hold the same digits.
    /// (`CanonicalValue` is the SHARED comparator in
    /// `cqlite-core/tests/support/canonical_jsonl.rs` and has no decimal
    /// variant; encoding the canonical form as a tagged exact text keeps the
    /// comparison EXACT without reaching outside this harness.)
    pub fn canonical(&self) -> CanonicalValue {
        CanonicalValue::Text(format!("{CANONICAL_PREFIX}{})", self.text()))
    }
}

/// The tag [`ExactDecimal::canonical`] wraps its exact text in.
const CANONICAL_PREFIX: &str = "decimal(";

/// Is this text ALREADY the canonical form of an exact decimal?
///
/// The declared-type descent is idempotent by design (`declared.rs`), and a
/// decimal is the one position where that matters twice: a stringified position
/// (a primary-key or collection-path component) is converted where it is
/// assembled, and the assembled value then goes through the descent again. So
/// the second pass must recognise its own output as canonical rather than try to
/// parse `decimal(1.1)` as a literal. A tagged form is unambiguous: a decimal
/// LITERAL can only contain `-+.0123456789`, so it can never spell this tag.
pub fn is_canonical_text(text: &str) -> bool {
    text.starts_with(CANONICAL_PREFIX) && text.ends_with(')')
}

/// Project an exported `Decimal128(_, scale)` cell — unscaled value and scale
/// read straight from Arrow, no division, no `f64`.
pub fn exact_from_decimal128(unscaled: i128, scale: i8, ctx: &str) -> Result<ExactDecimal, String> {
    if scale < 0 {
        return Err(format!("{ctx}: negative Decimal128 scale {scale}"));
    }
    let scale = scale as u32;
    if scale > MAX_SCALE {
        return Err(format!(
            "{ctx}: Decimal128 scale {scale} exceeds the {MAX_SCALE}-digit precision of the type"
        ));
    }
    if scale > EXPORT_DECIMAL_SCALE {
        return Err(format!(
            "{ctx}: exported Decimal128 scale {scale} exceeds {EXPORT_DECIMAL_SCALE}, the \
             fixed scale the export is declared to write; the harness refuses to compare \
             against a scale it cannot account for \
             (raise EXPORT_DECIMAL_SCALE together with the export's fixed scale)"
        ));
    }
    Ok(ExactDecimal::new(unscaled, scale))
}

/// Parse an EXACT decimal from its literal TEXT — the WHOLE golden side, and no
/// `f64` anywhere in it.
///
/// Every golden decimal reaches this function, from both shapes `sstabledump`
/// writes:
///
/// * a PRIMARY-KEY component or a multicell collection PATH component, which
///   Cassandra writes as a quoted STRING through `AbstractType.getString`, i.e.
///   `BigDecimal.toString` (issue #1490 round 6);
/// * a decimal CELL, which `sstabledump` writes as a bare JSON NUMBER — whose
///   literal is preserved as text by `golden_text.rs` BEFORE the shared
///   comparator can parse it into an `f64` (round 10).
///
/// The literal digits are therefore always present and are read exactly, with no
/// recovery step and no ambiguity to refuse. The recovery this replaced
/// (`exact_from_golden_double`, round 4→10) could not work even in principle:
/// `0.100000000000000001` and `0.1` are the SAME double, so a double cannot
/// identify the decimal it was parsed from, and its neighbour checks called that
/// collision unique and canonicalized the eighteen-digit literal as `0.1`.
///
/// Refuses rather than rounds or truncates:
///
/// * a scale beyond `max_scale` — such a value cannot be exported at all (the
///   export refuses to truncate), so comparing it would compare two different
///   numbers;
/// * exponent notation, an empty digit run, a stray character, or a magnitude
///   that does not fit a `Decimal128` unscaled `i128`.
pub fn exact_from_text(text: &str, max_scale: u32, ctx: &str) -> Result<ExactDecimal, String> {
    if max_scale > MAX_SCALE {
        return Err(format!(
            "{ctx}: recovery scale {max_scale} exceeds {MAX_SCALE}"
        ));
    }
    let trimmed = text.trim();
    let (sign, rest) = match trimmed.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1i128, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    let digits_ok = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if int_part.is_empty() || !digits_ok(int_part) || !digits_ok(frac_part) {
        return Err(format!(
            "{ctx}: '{text}' is not a plain decimal literal (the harness refuses exponent \
             notation and any non-digit rather than guessing what it denotes)"
        ));
    }
    let scale = frac_part.len() as u32;
    if scale > max_scale {
        return Err(format!(
            "{ctx}: '{text}' carries {scale} fractional digits, more than the {max_scale} the \
             export's fixed Decimal128 scale can represent; the harness refuses to compare \
             rather than truncate"
        ));
    }
    let magnitude: i128 = format!("{int_part}{frac_part}")
        .parse()
        .map_err(|_| format!("{ctx}: '{text}' does not fit a Decimal128 unscaled value (i128)"))?;
    let unscaled = magnitude
        .checked_mul(sign)
        .ok_or_else(|| format!("{ctx}: '{text}' sits at the edge of the unscaled range"))?;
    Ok(ExactDecimal::new(unscaled, scale))
}
