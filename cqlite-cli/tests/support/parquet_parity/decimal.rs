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
/// column at, and therefore the maximum fractional precision a golden literal
/// can be RECOVERED at (see [`exact_from_golden_double`]).
///
/// Mirrors `cqlite-core`'s `export::arrow_schema::DECIMAL_FIXED_SCALE`, which is
/// crate-private. It is used ONLY as a PRECISION BOUND for the golden-side
/// recovery, never as an authority for a value: the bound is enforced against
/// the scale actually read from the exported file
/// ([`exact_from_decimal128`] refuses a larger one), so a future change to the
/// export scale makes the harness REFUSE rather than silently under-recover.
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
    /// `Decimal128` unscaled value IS an `i128`; the golden-side recovery is
    /// range-checked before it gets here), and stripping a trailing zero is a
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
        CanonicalValue::Text(format!("decimal({})", self.text()))
    }
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
             precision the golden side can RECOVER a literal at; the harness refuses to \
             compare rather than let two distinct decimals recover to one value \
             (raise EXPORT_DECIMAL_SCALE together with the export's fixed scale)"
        ));
    }
    Ok(ExactDecimal::new(unscaled, scale))
}

/// Recover the EXACT decimal a golden `double` was parsed FROM, or refuse.
///
/// `sstabledump` writes a `decimal` as a bare JSON number, and the shared
/// `canonical_jsonl` comparator parses it into an `f64` before this harness sees
/// it — so the literal's text is already gone. This recovers it, and is exact
/// WHEN IT SUCCEEDS because it verifies both halves of the recovery instead of
/// assuming them:
///
/// * FIDELITY — the recovered decimal, re-rendered and re-parsed with Rust's
///   correctly-rounded float parser, must give back the SAME double. (A literal
///   with more than `max_scale` fractional digits fails here, loudly: such a
///   value cannot be exported at all, since the export refuses to truncate.)
/// * UNIQUENESS — neither one-unit neighbour (`±10^-max_scale`) may parse to
///   that same double. The `max_scale` decimals rounding to one double form a
///   CONTIGUOUS run, so if both neighbours round elsewhere the recovered value
///   is the ONLY decimal of at most `max_scale` fractional digits the golden
///   literal could have been. If a neighbour collides, the double is genuinely
///   ambiguous and the harness REFUSES rather than compare — a refusal is a
///   loud non-answer; comparing would be the false PASS this module exists to
///   remove.
///
/// Both checks rely on `cqlite-cli`'s `serde_json` dev-dependency enabling
/// `float_roundtrip` (guarded by `golden_float_literals_parse_exactly`): without
/// it serde_json's parse is up to 1 ULP off (#3557) and the FIDELITY check
/// fails — again a refusal, never a silent pass.
pub fn exact_from_golden_double(g: f64, max_scale: u32, ctx: &str) -> Result<ExactDecimal, String> {
    if !g.is_finite() {
        return Err(format!(
            "{ctx}: golden decimal literal parsed to {g:?}, which is not a finite decimal"
        ));
    }
    if max_scale > MAX_SCALE {
        return Err(format!(
            "{ctx}: recovery scale {max_scale} exceeds {MAX_SCALE}"
        ));
    }
    // `{:.p}` prints the correctly-rounded decimal expansion of the double's
    // EXACT binary value, so this is the nearest `max_scale`-digit decimal —
    // computed textually, never as `g * 10^max_scale` in floating point.
    let rendered = format!("{g:.prec$}", prec = max_scale as usize);
    let unscaled = parse_fixed_point(&rendered, max_scale).ok_or_else(|| {
        format!(
            "{ctx}: golden decimal {g:?} renders as {rendered}, which does not fit a \
             Decimal128 unscaled value; the harness refuses a lossy comparison"
        )
    })?;

    // FIDELITY: does the recovered decimal round back to exactly this double?
    // Signed zero is compared by VALUE, not by bits, on purpose: BigDecimal has
    // no negative zero, so `-0.0` and `0.0` denote the same decimal.
    if !round_trips_to(unscaled, max_scale, g)? {
        return Err(format!(
            "{ctx}: golden decimal {g:?} is not the double of any decimal with at most \
             {max_scale} fractional digits (nearest is {rendered}); the harness refuses to \
             compare rather than round"
        ));
    }
    // UNIQUENESS: a one-unit neighbour must NOT share the double.
    for delta in [-1i128, 1] {
        let neighbour = unscaled.checked_add(delta).ok_or_else(|| {
            format!("{ctx}: golden decimal {g:?} sits at the edge of the unscaled range")
        })?;
        if round_trips_to(neighbour, max_scale, g)? {
            let a = ExactDecimal::new(unscaled, max_scale).text();
            let b = ExactDecimal::new(neighbour, max_scale).text();
            return Err(format!(
                "{ctx}: the golden double {g:?} is shared by the distinct decimals {a} and \
                 {b}, so the literal cannot be recovered exactly; the harness refuses to \
                 compare (a one-unit divergence would be invisible)"
            ));
        }
    }
    Ok(ExactDecimal::new(unscaled, max_scale))
}

/// Does `unscaled × 10^-scale`, rendered exactly and re-parsed, equal `g`?
fn round_trips_to(unscaled: i128, scale: u32, g: f64) -> Result<bool, String> {
    let text = ExactDecimal { unscaled, scale }.text();
    let parsed: f64 = text
        .parse()
        .map_err(|e| format!("exact decimal text {text} does not re-parse as f64: {e}"))?;
    Ok(parsed == g)
}

/// Parse `[-]D+.D{scale}` (the output of `{:.scale}`) into an unscaled `i128`.
/// `None` if it does not fit — a refusal, never a truncation.
fn parse_fixed_point(text: &str, scale: u32) -> Option<i128> {
    let (sign, rest) = match text.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1i128, text),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None if scale == 0 => (rest, ""),
        None => return None,
    };
    if frac_part.len() != scale as usize {
        return None;
    }
    if int_part.is_empty() || !int_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !frac_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let digits = format!("{int_part}{frac_part}");
    let magnitude: i128 = digits.parse().ok()?;
    magnitude.checked_mul(sign)
}
