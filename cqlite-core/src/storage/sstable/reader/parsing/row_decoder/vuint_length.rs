//! One place that turns an untrusted VUInt-decoded length into a `usize`
//! (issue #3848, truncation axis).
//!
//! `parse_vuint` yields a `u64` — up to `u64::MAX` from a 9-byte encoding — and
//! the historical shape at every call site was `let len = len_raw as usize;`
//! FOLLOWED by a bounds check. That ordering is unsound on any target where
//! `usize` is narrower than 64 bits (`wasm32-unknown-unknown` is a configured
//! target of this workspace): `1u64 << 32` casts to `0usize`, so the bounds
//! check — however carefully written, saturating or not — is handed a value that
//! is not the length the file declared, and the input is silently misparsed
//! instead of rejected.
//!
//! The rule these helpers enforce is therefore ORDERING, not arithmetic:
//! **compare in `u64` space FIRST, convert only once the value is proven to fit
//! in `usize`.** `available` is widened `usize -> u64` (always lossless), so the
//! comparison is exact on every target, and the surviving `as usize` cast is
//! provably lossless because the value is `<= available`, itself a `usize`.
//!
//! This is the same guarantee `read_frozen_preamble` and the `#1795` guard
//! `read_vint_length_prefixed_bytes` already get from capping the raw `u64`
//! against `MAX_CELL_VALUE_LENGTH` before the cast. These helpers are for the
//! sites where a byte-count bound — not a cell-value ceiling — is the right
//! limit: framing fields, and value arms whose only bound is "the bytes that
//! remain".
//!
//! ## The narrowing TARGET is a type parameter, so the guards are TESTABLE
//!
//! Every gate component and every CI lane builds a 64-bit target, where
//! `len_raw as usize` is the IDENTITY for a `u64` — so at `usize` width a
//! cast-first implementation and the guard AGREE on every input, and a test that
//! only observes "an error came back" pins nothing (#3042: a test invariant to
//! the defect it claims to pin). Deleting the guard would leave such a case
//! green.
//!
//! The comparison-and-narrowing step is therefore generic over the
//! [`LengthWidth`] target. Production code calls the `usize` wrappers below and
//! its behaviour is unchanged; the tests ALSO instantiate the generic at `u32` —
//! exactly the semantics a 32-bit `usize` has — where a plain cast and the guard
//! give DIFFERENT verdicts (`((1u64 << 32) + 8) as u32 == 8` is "8 of 8 bytes
//! available", i.e. ACCEPTED, while the guard rejects). That regression is then
//! caught on every lane that runs, with no 32-bit host and no
//! `cfg(target_pointer_width)` case that would execute nowhere.

use crate::parser::vint_narrow::{narrow_vuint_len, LengthWidth};
use crate::{Error, Result};

/// The length as a `usize` if it fits within `available` bytes, else `None`.
///
/// The comparison is performed in `u64` space (see the module docs): no cast
/// happens until the value is known to be representable, so a length that would
/// truncate on a 32-bit target is rejected rather than silently reinterpreted.
///
/// Callers that want the standard "need N bytes for X, only M available"
/// corruption message should use [`checked_vuint_length`]; this variant exists
/// for sites whose diagnostic has a different shape (e.g. the range-tombstone
/// `marker_body_size`, a framing size rather than a cell value).
pub(super) fn vuint_length_within(len_raw: u64, available: usize) -> Option<usize> {
    vuint_length_within_as::<usize>(len_raw, available)
}

/// [`vuint_length_within`] at an arbitrary target width (see the module docs).
///
/// The comparison happens in `u64` space — `available` is WIDENED, never
/// `len_raw` narrowed — and the conversion that follows is CHECKED, so neither
/// step can reinterpret an out-of-range length as its low bits.
pub(super) fn vuint_length_within_as<T: LengthWidth>(len_raw: u64, available: T) -> Option<T> {
    if len_raw > available.widen() {
        return None;
    }
    // Provably `Some`: `len_raw <= available`, itself a `T`. Still a CHECKED
    // conversion rather than a cast, so a future edit to the comparison above
    // cannot turn this into a truncation.
    narrow_vuint_len(len_raw)
}

/// [`vuint_length_within`] plus the canonical `Error::corruption` diagnostic.
///
/// The message is `"<subject> '<name>': need <len_raw> bytes for <what>, only
/// <available> available"`, reporting the RAW `u64` length so an adversarial
/// prefix is named in full rather than in its truncated form.
pub(super) fn checked_vuint_length(
    len_raw: u64,
    available: usize,
    subject: &str,
    name: &str,
    what: &str,
) -> Result<usize> {
    checked_vuint_length_as::<usize>(len_raw, available, subject, name, what)
}

/// [`checked_vuint_length`] at an arbitrary target width (see the module docs).
pub(super) fn checked_vuint_length_as<T: LengthWidth>(
    len_raw: u64,
    available: T,
    subject: &str,
    name: &str,
    what: &str,
) -> Result<T> {
    vuint_length_within_as(len_raw, available).ok_or_else(|| {
        Error::corruption(format!(
            "{} '{}': need {} bytes for {}, only {} available",
            subject, name, len_raw, what, available
        ))
    })
}

/// A FIXED-WIDTH field's declared length, validated in `u64` space against the
/// sizes the on-disk format allows, and narrowed only on success.
///
/// # Why an equality check AFTER the cast is not a guard (issue #3848)
///
/// The historical shape at these sites was
///
/// ```text
/// let date_len = date_len as usize;      // raw u64 -> usize
/// if date_len != 4 { return Err(...) }   // "so a truncated value is caught"
/// ```
///
/// That reasoning is WRONG. Truncation is not a randomising operation: an
/// adversary chooses the length so its LOW BITS are the expected size. On a
/// 32-bit target `((1u64 << 32) + 4) as usize == 4usize`, which PASSES
/// `!= 4` — so a corrupt length is silently ACCEPTED as a valid 4-byte field
/// instead of being rejected, and the following bytes are misparsed. The
/// equality check provides no protection against truncation whatsoever; it only
/// looks as if it does.
///
/// So the comparison happens in `u64` space. `allowed` holds `usize` sizes,
/// widened `usize -> u64` (always lossless), which makes the equality exact on
/// every target; the surviving `as usize` cast is provably lossless because the
/// value equals one of the `allowed` sizes, each itself a `usize`.
fn vuint_length_exact_as<T: LengthWidth>(len_raw: u64, allowed: &[T]) -> Option<T> {
    // Returns the MATCHED `allowed` entry, so nothing is narrowed at all: the
    // value handed back is one the caller supplied, already at the target width.
    allowed.iter().copied().find(|size| len_raw == size.widen())
}

/// [`vuint_length_exact_as`] plus the canonical `Error::corruption` diagnostic.
///
/// The message keeps the two shapes the fixed-width arms historically used, and
/// reports the RAW `u64` length so an adversarial prefix is named in full rather
/// than in its truncated form:
/// - one allowed size: `"<subject> '<name>': expected <what> length 4, got 17"`
/// - several:          `"<subject> '<name>': invalid <what> length 17, expected 4 or 16"`
pub(super) fn checked_vuint_exact_length(
    len_raw: u64,
    allowed: &[usize],
    subject: &str,
    name: &str,
    what: &str,
) -> Result<usize> {
    checked_vuint_exact_length_as::<usize>(len_raw, allowed, subject, name, what)
}

/// [`checked_vuint_exact_length`] at an arbitrary target width (see the module
/// docs): the width the tests instantiate to make the truncation guard
/// discriminating on a 64-bit host.
pub(super) fn checked_vuint_exact_length_as<T: LengthWidth>(
    len_raw: u64,
    allowed: &[T],
    subject: &str,
    name: &str,
    what: &str,
) -> Result<T> {
    vuint_length_exact_as(len_raw, allowed).ok_or_else(|| {
        let sizes = allowed
            .iter()
            .map(|size| size.to_string())
            .collect::<Vec<_>>()
            .join(" or ");
        if allowed.len() == 1 {
            Error::corruption(format!(
                "{} '{}': expected {} length {}, got {}",
                subject, name, what, sizes, len_raw
            ))
        } else {
            Error::corruption(format!(
                "{} '{}': invalid {} length {}, expected {}",
                subject, name, what, len_raw, sizes
            ))
        }
    })
}
