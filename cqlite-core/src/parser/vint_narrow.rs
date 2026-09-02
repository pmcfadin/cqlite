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
//! The two parsers below take the length in its ORIGINAL width and narrow it
//! with a CHECKED conversion, so a value that does not fit `usize` is a
//! `nom::error::ErrorKind::TooLarge` failure rather than a truncated length.
//! On a 64-bit target the conversion always succeeds, so behaviour is unchanged
//! there: the subsequent `take` already failed with `Eof` on a short buffer.
//!
//! They are parser CONSTRUCTORS (`take_vint_length(len)(input)`) rather than
//! `usize`-returning helpers so a call site converts one-for-one from
//! `take(len as usize)(input)` with no extra statement — the sites live in files
//! that are over the campsite-rule size threshold.

use nom::bytes::complete::take;
use nom::IResult;

/// `take` exactly `len` bytes, where `len` is a SIGNED (ZigZag) VInt-decoded
/// length. A negative length, or one too large for `usize` on this target, is a
/// `TooLarge` parse error rather than a truncated or wrapped byte count.
///
/// Callers that treat a specific negative value as a sentinel (`-1` = null in
/// the Cassandra collection framing) must handle it BEFORE calling this.
pub fn take_vint_length(len: i64) -> impl Fn(&[u8]) -> IResult<&[u8], &[u8]> {
    move |input: &[u8]| match usize::try_from(len) {
        Ok(n) => take(n)(input),
        Err(_) => Err(nom::Err::Error(nom::error::Error::new(
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
    move |input: &[u8]| match usize::try_from(len) {
        Ok(n) => take(n)(input),
        Err(_) => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// A length wider than `usize` on a 32-bit target is REJECTED rather than
    /// narrowed. On a 64-bit host `u64::MAX` is representable, so the rejection
    /// there comes from `take` (`Eof`) — either way it is an error, never a
    /// silently truncated byte count.
    #[test]
    fn a_length_wider_than_usize_is_never_silently_truncated() {
        let data = [1u8, 2, 3, 4, 5];
        for len in [1u64 << 32, (1u64 << 32) + 3, u64::MAX] {
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

    /// A negative length is a parse error, not a huge unsigned one.
    #[test]
    fn a_negative_length_is_rejected() {
        let data = [1u8, 2, 3];
        for len in [-1i64, -2, i64::MIN] {
            assert!(take_vint_length(len)(&data).is_err(), "len {len}");
        }
    }
}
