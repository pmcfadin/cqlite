//! Truncation-free narrowing of a VInt/VUInt-decoded length for the `nom`
//! parsers (issue #3848).
//!
//! `parse_vuint` yields a `u64` and `parse_vint` an `i64` — both wider than
//! `usize` on a 32-bit target (`wasm32-unknown-unknown` is a configured target
//! of this workspace). The historical shape at these call sites was
//!
//! ```text
//! let (rest, len) = parse_vint(input)?;   // i64
//! if len < 0 { return Err(..) }           // sign checked...
//! let (rest, body) = take(len as usize)(rest)?;   // ...but NOT the width
//! ```
//!
//! On a 32-bit target `(1i64 << 32) as usize == 0`, so `take(0)` SUCCEEDS: a
//! declared 4 GiB run is silently reinterpreted as an empty one and every
//! following field is misparsed. Nothing in the sign check reaches that, and no
//! upper bound was applied in the original width.
//!
//! The two parser constructors below take the length in its ORIGINAL width and
//! narrow it through [`narrow_vuint_len`] / [`narrow_vint_len`], so a value that
//! does not fit the target width is a `nom::error::ErrorKind::TooLarge` failure
//! rather than a truncated length.
//!
//! They are parser CONSTRUCTORS (`take_vint_length(len)(input)`) rather than
//! `usize`-returning helpers so a call site converts one-for-one from
//! `take(len as usize)(input)` with no extra statement — the sites live in files
//! that are over the campsite-rule size threshold.
//!
//! ## Why the narrowing step is WIDTH-PARAMETERIZED (test efficacy)
//!
//! The runtime target width is always `usize`. But on a 64-bit host — every
//! gate component and every CI lane, since nothing here builds a 32-bit target —
//! `len as usize` is the IDENTITY for a `u64`, so a test that only observes
//! `take_vuint_length(1u64 << 32)` returning *some* error cannot tell the guard
//! from a reintroduced truncating cast: with the guard the rejection is
//! `TooLarge`, without it `take(4294967296)` rejects with `Eof`, and an
//! `is_err()` assertion is satisfied either way. Such a test pins NOTHING —
//! deleting the guard leaves it green (#3042: a test invariant to the defect it
//! claims to pin).
//!
//! So the narrowing itself is a generic function over the [`LengthWidth`] target.
//! Production code instantiates it at `usize`; the tests ALSO instantiate it at
//! `u32`, which is exactly the semantics a 32-bit `usize` has, on any host. There
//! a plain cast and the checked conversion give DIFFERENT answers
//! (`((1u64 << 32) + 3) as u32 == 3` vs `None`), so the regression is caught on
//! every lane that runs.

use core::fmt;

use nom::bytes::complete::take;
use nom::IResult;

/// A width a decoded length may be narrowed TO.
///
/// Implemented for `usize` (what production code narrows to) and for `u32` (the
/// same semantics a 32-bit `usize` has, so the guards above it can be exercised
/// at that width on a 64-bit host — see the module docs). Both are sealed by
/// being `pub(crate)`.
pub(crate) trait LengthWidth:
    Copy + Eq + TryFrom<u64> + TryFrom<i64> + fmt::Display + fmt::Debug
{
    /// Widen back to `u64`. Lossless for every implementor: Rust's `usize` is at
    /// most 64 bits on every supported target, and `u32` trivially fits.
    fn widen(self) -> u64;
}

impl LengthWidth for usize {
    fn widen(self) -> u64 {
        self as u64
    }
}

impl LengthWidth for u32 {
    fn widen(self) -> u64 {
        u64::from(self)
    }
}

/// Narrow an UNSIGNED VUInt-decoded length to the target width, or `None` if it
/// does not fit.
///
/// This is the ONE place the crate narrows an untrusted unsigned length. It is a
/// CHECKED conversion, never a cast: a value wider than `T` is rejected, not
/// reinterpreted as its low bits.
pub(crate) fn narrow_vuint_len<T: LengthWidth>(len: u64) -> Option<T> {
    T::try_from(len).ok()
}

/// Narrow a SIGNED (ZigZag) VInt-decoded length to the target width, or `None`
/// if it is negative or does not fit.
///
/// Counterpart of [`narrow_vuint_len`]; the same checked-conversion rule applies,
/// and a negative length is rejected here rather than becoming a huge unsigned
/// one via `as`.
pub(crate) fn narrow_vint_len<T: LengthWidth>(len: i64) -> Option<T> {
    T::try_from(len).ok()
}

/// `take` exactly `len` bytes, where `len` is a SIGNED (ZigZag) VInt-decoded
/// length. A negative length, or one too large for `usize` on this target, is a
/// `TooLarge` parse error rather than a truncated or wrapped byte count.
///
/// Callers that treat a specific negative value as a sentinel (`-1` = null in
/// the Cassandra collection framing) must handle it BEFORE calling this.
pub fn take_vint_length(len: i64) -> impl Fn(&[u8]) -> IResult<&[u8], &[u8]> {
    move |input: &[u8]| match narrow_vint_len::<usize>(len) {
        Some(n) => take(n)(input),
        None => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        ))),
    }
}

/// `take` exactly `len` bytes, where `len` is an UNSIGNED VUInt-decoded length
/// (`writeUnsignedVInt`, i.e. Cassandra-written bytes). A length too large for
/// `usize` on this target is a `TooLarge` parse error rather than a truncated
/// byte count.
pub fn take_vuint_length(len: u64) -> impl Fn(&[u8]) -> IResult<&[u8], &[u8]> {
    move |input: &[u8]| match narrow_vuint_len::<usize>(len) {
        Some(n) => take(n)(input),
        None => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lengths whose LOW 32 BITS are a small, plausible byte count: exactly what
    /// an adversary picks, because a truncating cast to a 32-bit width turns each
    /// of them into a value a later bounds/equality check accepts.
    const TRUNCATING: [u64; 3] = [1u64 << 32, (1u64 << 32) + 3, u64::MAX];

    /// The happy path, so neither parser is trivially over-strict: a length that
    /// fits consumes exactly that many bytes.
    #[test]
    fn a_length_that_fits_consumes_exactly_that_many_bytes() {
        let data = [1u8, 2, 3, 4, 5];
        let (rest, body) = take_vint_length(3)(&data).expect("3 of 5 bytes");
        assert_eq!(body, &[1, 2, 3]);
        assert_eq!(rest, &[4, 5]);
        let (rest, body) = take_vuint_length(0)(&data).expect("an empty run");
        assert_eq!(body, &[] as &[u8]);
        assert_eq!(rest, &data);
    }

    /// THE discriminating pin for the unsigned narrowing, at 32-bit width on any
    /// host: were `narrow_vuint_len` a plain `len as u32` cast, each of these
    /// would come back as the small `Some(..)` value asserted below and the case
    /// would FAIL. The checked conversion rejects all three.
    #[test]
    fn narrowing_an_unsigned_length_at_32_bit_width_rejects_what_a_cast_would_truncate() {
        // The two low-colliding cases truncate to a small, plausible count —
        // exactly the values a later bounds or equality check accepts.
        assert_eq!((1u64 << 32) as u32, 0, "case setup: the low bits");
        assert_eq!(((1u64 << 32) + 3) as u32, 3, "case setup: the low bits");
        for len in TRUNCATING {
            // What a reintroduced truncating cast would produce: a DIFFERENT
            // length from the one the file declared, which is why "some error
            // happened" is not a pin.
            let truncated = len as u32;
            assert_ne!(
                u64::from(truncated),
                len,
                "case setup: the cast must lose information for {len}"
            );
            assert_eq!(
                narrow_vuint_len::<u32>(len),
                None,
                "length {len} does not fit 32 bits and must be REJECTED, \
                 not narrowed to {truncated}"
            );
        }
    }

    /// The same discriminating pin for the signed narrowing. `(1i64 << 32) + 3`
    /// casts to `3u32`; a negative length casts to a huge unsigned one. Neither
    /// may be narrowed.
    #[test]
    fn narrowing_a_signed_length_at_32_bit_width_rejects_what_a_cast_would_truncate() {
        assert_eq!(((1i64 << 32) + 3) as u32, 3, "case setup: the low bits");
        for len in [1i64 << 32, (1i64 << 32) + 3, i64::MAX, -1, i64::MIN] {
            assert_eq!(
                narrow_vint_len::<u32>(len),
                None,
                "length {len} must be rejected, never narrowed"
            );
        }
    }

    /// The narrowing must not be over-strict at either width: a legitimate count
    /// converts to itself.
    #[test]
    fn a_representable_length_narrows_to_itself_at_both_widths() {
        assert_eq!(narrow_vuint_len::<u32>(7), Some(7u32));
        assert_eq!(narrow_vuint_len::<usize>(7), Some(7usize));
        assert_eq!(narrow_vint_len::<u32>(7), Some(7u32));
        assert_eq!(narrow_vint_len::<usize>(0), Some(0usize));
        assert_eq!(
            narrow_vuint_len::<u32>(u32::MAX as u64),
            Some(u32::MAX),
            "the widest 32-bit count is representable and must be accepted"
        );
    }

    /// A negative length is rejected with the GUARD's own error kind. This one
    /// discriminates at `usize` width even on a 64-bit host: `(-1i64) as usize`
    /// is a huge count, so a cast-first regression would reject with `Eof` from
    /// `take` — asserting `TooLarge` specifically is what makes it a pin.
    #[test]
    fn a_negative_length_is_rejected_as_too_large_not_as_eof() {
        let data = [1u8, 2, 3];
        for len in [-1i64, -2, i64::MIN] {
            match take_vint_length(len)(&data) {
                Err(nom::Err::Error(e)) => assert_eq!(
                    e.code,
                    nom::error::ErrorKind::TooLarge,
                    "len {len} must be rejected by the width guard, not by `take`"
                ),
                other => panic!("len {len}: expected a TooLarge parse error, got {other:?}"),
            }
        }
    }

    /// The parser-constructor surface at `usize` width, for the record: a length
    /// wider than the remaining input never resolves to a short run.
    ///
    /// DECLARED LIMIT — this case is deliberately NOT a pin, and no test at this
    /// surface can be one on a 64-bit host: there `u64 -> usize` is the identity,
    /// so with the guard the rejection is `TooLarge` and without it `take`'s
    /// `Eof`, and no assertion can prefer one without failing on the other
    /// target. The discriminating coverage is
    /// [`narrowing_an_unsigned_length_at_32_bit_width_rejects_what_a_cast_would_truncate`]
    /// plus the `TooLarge` assertion on negative lengths above.
    #[test]
    fn a_length_wider_than_the_input_is_never_a_short_run() {
        let data = [1u8, 2, 3, 4, 5];
        for len in TRUNCATING {
            assert!(
                take_vuint_length(len)(&data).is_err(),
                "declared length {len} must not resolve to a short run"
            );
        }
        for len in [1i64 << 32, (1i64 << 32) + 3, i64::MAX] {
            assert!(
                take_vint_length(len)(&data).is_err(),
                "declared length {len} must not resolve to a short run"
            );
        }
    }
}
