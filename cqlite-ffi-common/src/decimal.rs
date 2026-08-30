//! The single implementation of CQL `decimal` rendering (issue #1452).
//!
//! A Cassandra `decimal` cell is a pair `(scale: i32, unscaled: bytes)` where
//! `unscaled` is a two's-complement big-endian Java `BigInteger` and the value
//! is `unscaled × 10^(-scale)`. Rendering it exactly is pure arithmetic with no
//! FFI in it, so it lives here and both bindings call it.
//!
//! # The rendering policy, defined once
//!
//! The policy is the #1754 policy (previously the Node binding's; adopted for
//! both bindings by the issue #1452 owner ruling, which is the one
//! binding-visible behaviour change of that change — see `CHANGELOG.md`):
//!
//! * **Refusal ceiling** — [`DECIMAL_MAX_UNSCALED_BYTES`]. Above it the value is
//!   refused as corrupt with a typed [`DecimalError`]. Checked in O(1) *before*
//!   the single superlinear base conversion.
//! * **Exponent form** — used instead of a positional expansion when the
//!   magnitude exceeds [`DECIMAL_POSITIONAL_MAX_BYTES`] or `|scale|` exceeds
//!   [`DECIMAL_MAX_SCALE_DIGITS`]. Exponent form is exact and bounded, so no
//!   *well-formed* value is ever rejected for having an extreme scale.
//! * **Below the ceiling the render is infallible**, so a well-formed value can
//!   never abort a host interpreter (the abort-safety guarantee of #1437/#1440,
//!   preserved from #1741).

use num_bigint::BigInt;

/// Sanity ceiling on the unscaled-magnitude byte length this converter will
/// render (issue #1754).
///
/// A Cassandra `decimal` unscaled value is a Java `BigInteger` — legitimately
/// arbitrary-precision — so a merely-large value is NOT corrupt and must render
/// faithfully. The only hard cost is the single `BigInt` → decimal-string base
/// conversion (superlinear in digit count); a 32 KB magnitude (~79k digits)
/// still converts in tens of milliseconds even in a debug build. Only a
/// genuinely pathological magnitude beyond that could stall a host thread, so we
/// fail closed with a typed corruption error ONLY above this ceiling.
pub const DECIMAL_MAX_UNSCALED_BYTES: usize = 32 * 1024;

/// Byte-length threshold above which a well-formed magnitude is rendered in
/// precision-preserving exponent form rather than an O(digits)-wide positional
/// expansion (issue #1754).
///
/// At/under 1024 bytes (~2466 digits) the positional render is cheap and
/// byte-identical to the historical output; beyond it we emit `<digits>e<-scale>`
/// (exact, bounded) to avoid superlinear padding work.
pub const DECIMAL_POSITIONAL_MAX_BYTES: usize = 1024;

/// Threshold on `scale.abs()` above which the value is rendered in exponent form
/// instead of positional (issue #1754).
///
/// `scale` would otherwise drive a `format!` padding width / leading-zero
/// `repeat`; a huge scale (e.g. `i32::MAX`) would panic ("Formatting argument
/// out of range") or allocate an unbounded string. Exponent form is exact and
/// bounded, so no scale value is rejected — a well-formed decimal always
/// renders.
pub const DECIMAL_MAX_SCALE_DIGITS: usize = 1_000_000;

/// Why a `decimal` cell was refused.
///
/// FFI-free by construction: each binding maps this onto
/// [`cqlite_core::Error::corruption`] and then through its own existing
/// production error path, so the resulting Python exception class and JS
/// `error.code` still come from the one FFI error contract
/// ([`crate::error_contract`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecimalError {
    /// The unscaled magnitude is beyond the sanity ceiling — a size no
    /// legitimate value can reach, i.e. a corrupt SSTable. Fail closed.
    UnscaledTooLarge {
        /// The cell's scale, reported for diagnosis.
        scale: i32,
        /// The offending unscaled magnitude length, in bytes.
        unscaled_len: usize,
        /// The ceiling that was exceeded ([`DECIMAL_MAX_UNSCALED_BYTES`]).
        max_unscaled_bytes: usize,
    },
}

impl std::fmt::Display for DecimalError {
    /// THE single spelling of the refusal message, in both bindings.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecimalError::UnscaledTooLarge {
                scale,
                unscaled_len,
                max_unscaled_bytes,
            } => write!(
                f,
                "DECIMAL cell not representable (scale={scale}, \
                 unscaled_len={unscaled_len} bytes, \
                 max_unscaled={max_unscaled_bytes} bytes): corrupt SSTable — \
                 refusing to enter a superlinear render on a pathological \
                 magnitude (issue #1754)"
            ),
        }
    }
}

impl std::error::Error for DecimalError {}

/// Render a CQL `decimal` exactly, as `unscaled × 10^(-scale)`.
///
/// An empty `unscaled` is the zero value (`"0"`), matching Cassandra's encoding
/// of a zero-length `BigInteger` payload.
///
/// # Errors
///
/// Returns [`DecimalError::UnscaledTooLarge`] — and never panics — only when the
/// unscaled magnitude exceeds [`DECIMAL_MAX_UNSCALED_BYTES`]. A merely-large but
/// well-formed decimal is NOT corrupt: it renders faithfully in
/// precision-preserving exponent form.
pub fn decimal_to_string(scale: i32, unscaled: &[u8]) -> Result<String, DecimalError> {
    if unscaled.is_empty() {
        return Ok("0".to_string());
    }

    // Sanity ceiling (issue #1754): an O(1) length check BEFORE the one
    // superlinear base conversion. Only a genuinely pathological magnitude is
    // rejected; a well-formed arbitrary-precision value renders below.
    if unscaled.len() > DECIMAL_MAX_UNSCALED_BYTES {
        return Err(DecimalError::UnscaledTooLarge {
            scale,
            unscaled_len: unscaled.len(),
            max_unscaled_bytes: DECIMAL_MAX_UNSCALED_BYTES,
        });
    }

    // Cassandra encodes the unscaled value as a two's-complement big-endian Java
    // BigInteger. ONE base-10 conversion (the sole superlinear step) yields the
    // digit string; every branch below is a single O(digits) pass over it — no
    // repeated division, no scale-width padding blowup.
    let full = BigInt::from_signed_bytes_be(unscaled).to_string();
    let (is_negative, digits) = match full.strip_prefix('-') {
        Some(rest) => (true, rest.to_string()),
        None => (false, full),
    };

    // Precision-preserving exponent form for over-bound cases (issue #1754): a
    // large magnitude (thousands+ of digits) or a pathological scale (which as a
    // padding width would panic / allocate unbounded, and at `i32::MIN` would
    // overflow `-scale`). `<digits>e<-scale>` preserves every digit exactly.
    let result = if unscaled.len() > DECIMAL_POSITIONAL_MAX_BYTES
        || (scale.unsigned_abs() as usize) > DECIMAL_MAX_SCALE_DIGITS
    {
        if scale == 0 {
            digits
        } else {
            // `i64` avoids the `(-scale)` overflow at `scale == i32::MIN`.
            format!("{digits}e{}", -(scale as i64))
        }
    } else if scale == 0 {
        digits
    } else if scale > 0 {
        // Positive scale means the decimal point moves left.
        let scale_usize = scale as usize;
        if digits.len() <= scale_usize {
            // Need leading zeros: 123 with scale 5 -> 0.00123.
            //
            // Built EXPLICITLY rather than with a `{:0>width$}` format spec:
            // `core::fmt` packs the width into a `u16`, so a spec width above
            // 65535 panics with "Formatting argument out of range" — which the
            // exponent-form threshold (`DECIMAL_MAX_SCALE_DIGITS`, 1_000_000)
            // does NOT bound. That made a well-formed value with, say,
            // `scale = 100_000` a PANIC on the render path (issue #1452,
            // inherited from the #1754 body). Explicit construction has no such
            // limit and the threshold bounds the allocation instead.
            let padding = scale_usize - digits.len();
            let mut rendered = String::with_capacity(scale_usize + 2);
            rendered.push_str("0.");
            for _ in 0..padding {
                rendered.push('0');
            }
            rendered.push_str(&digits);
            rendered
        } else {
            // Insert the decimal point.
            let split_point = digits.len() - scale_usize;
            let int_part = &digits[..split_point];
            let frac_part = &digits[split_point..];
            format!("{int_part}.{frac_part}")
        }
    } else {
        // Negative scale means multiply by a power of 10.
        format!("{digits}e{}", -scale)
    };

    if is_negative {
        Ok(format!("-{result}"))
    } else {
        Ok(result)
    }
}

// Unit tests live in a sibling file to keep this module small (#1116).
#[cfg(test)]
#[path = "decimal_tests.rs"]
mod tests;
