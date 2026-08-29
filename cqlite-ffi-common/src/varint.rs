//! The single implementation of the CQL `varint` semantic (issue #1452).
//!
//! A Cassandra `varint` cell is a **big-endian two's-complement** integer of any
//! byte length; an empty payload is zero. That is the whole semantic, and it is
//! the part that drifts, so it is decided here once.
//!
//! # Two shapes, one value
//!
//! The two FFI ABIs want genuinely different shapes and neither can be derived
//! from the other *inside a binding* without re-implementing the arithmetic this
//! module exists to own:
//!
//! * [`varint_to_bigint`] is the lossless, FFI-neutral value. The Python binding
//!   hands it to `pyo3`'s `num-bigint` conversion.
//! * [`varint_to_sign_and_le_words`] is the sign-magnitude, little-endian `u64`
//!   word form napi's `create_bigint_from_words(sign_bit, words)` requires. It is
//!   a **projection** of the `BigInt` (`BigInt::to_u64_digits`), never computed
//!   independently, so the two can never disagree. The Node binding previously
//!   hand-rolled the padding, word assembly and a carry-propagating
//!   two's-complement negate loop; all of it is gone.

use num_bigint::{BigInt, Sign};

/// Decode a CQL `varint` payload: big-endian two's complement, sign-extending at
/// any byte length, with an empty payload meaning zero.
pub fn varint_to_bigint(bytes: &[u8]) -> BigInt {
    // `from_signed_bytes_be` IS the semantic: it sign-extends from the high bit
    // of the first byte at any width and yields zero for an empty slice.
    BigInt::from_signed_bytes_be(bytes)
}

/// The same value in napi's `create_bigint_from_words` shape: a sign flag plus
/// little-endian `u64` magnitude words.
///
/// Zero is reported as `(false, vec![])` — the empty magnitude — which is what
/// `BigInt::to_u64_digits` returns for it and what Node-API renders as `0n`.
pub fn varint_to_sign_and_le_words(bytes: &[u8]) -> (bool, Vec<u64>) {
    let (sign, words) = varint_to_bigint(bytes).to_u64_digits();
    (sign == Sign::Minus, words)
}

/// Reassemble a `(is_negative, little-endian u64 words)` pair back into a
/// [`BigInt`].
///
/// Exists so tests can prove [`varint_to_sign_and_le_words`] is a faithful
/// projection of [`varint_to_bigint`] for every input, and so a reader can see
/// exactly what the word form means.
pub fn bigint_from_sign_and_le_words(is_negative: bool, words: &[u64]) -> BigInt {
    let sign = if words.iter().all(|w| *w == 0) {
        Sign::NoSign
    } else if is_negative {
        Sign::Minus
    } else {
        Sign::Plus
    };
    BigInt::from_slice(
        sign,
        &words
            .iter()
            .flat_map(|word| [*word as u32, (*word >> 32) as u32])
            .collect::<Vec<u32>>(),
    )
}

// Unit tests live in a sibling file to keep this module small (#1116).
#[cfg(test)]
#[path = "varint_tests.rs"]
mod tests;
