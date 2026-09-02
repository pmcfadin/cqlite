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
    if len_raw > available as u64 {
        return None;
    }
    // Lossless on every target: `len_raw <= available`, and `available: usize`.
    Some(len_raw as usize)
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
    vuint_length_within(len_raw, available).ok_or_else(|| {
        Error::corruption(format!(
            "{} '{}': need {} bytes for {}, only {} available",
            subject, name, len_raw, what, available
        ))
    })
}
