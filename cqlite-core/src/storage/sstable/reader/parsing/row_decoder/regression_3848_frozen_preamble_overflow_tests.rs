//! Regression tests for issue #3848 — unbounded VUInt blob length in the frozen
//! collection preamble.
//!
//! [`V5CompressedLegacyParser::read_frozen_preamble`] decoded the frozen
//! collection's outer blob length with `parse_vuint` (which yields up to
//! `u64::MAX` from a 9-byte encoding), cast it straight to `usize` with NO
//! `MAX_CELL_VALUE_LENGTH` cap, and then bounds-checked it as
//! `*offset + blob_len > data.len()`. On an adversarial `Data.db` length prefix
//! that ADD overflowed `usize` — in an overflow-checked build (debug/test) an
//! `attempt to add with overflow` panic; in release a wraparound that made the
//! guard pass, after which the slice was taken against a nonsense length. The
//! input is untrusted on-disk bytes, i.e. exactly the surface `fuzz/` exists for.
//!
//! This is a site MISSED by the #1795 sweep of the same class; its two siblings
//! on the identical framing (`parse_tuple_value` here, and the frozen-UDT arms in
//! `cell_value_complex`) both already guarded.
//!
//! ## Two independent axes
//!
//! The fix covers BOTH axes, so neither depends on the other:
//!
//! 1. **The cap** — `blob_len_raw > MAX_CELL_VALUE_LENGTH` is rejected BEFORE the
//!    `as usize` cast (the `parse_tuple_value` idiom).
//! 2. **The comparison** — the bounds test is the saturating form
//!    `blob_len > data.len().saturating_sub(*offset)` (the `cell_value_complex`
//!    idiom), which cannot overflow for ANY `blob_len`.
//!
//! [`max_guard_fires_independently_of_the_saturating_comparison`] pins axis 1
//! firing on its own, with a length that is over the cap but far too small to
//! overflow the add — so removing the cap would red that test even though the
//! saturating comparison is still in place.
//!
//! ## Dataset independence
//!
//! These tests call the associated function DIRECTLY. It needs no
//! `SSTableReader`, no `CQLITE_DATASETS_ROOT`, no fixture and no feature flag, so
//! they run unconditionally in EVERY build and lane and can never pass vacuously
//! (there is no dataset-absent path that could silently `return`). Debug/test
//! builds run with `overflow-checks = true`, so a regressed add would abort the
//! process here rather than silently wrap.

use super::V5CompressedLegacyParser;
use crate::error::Error;
use crate::parser::vint::encode_vuint;
use crate::storage::sstable::reader::parsing::row_decoder::MAX_CELL_VALUE_LENGTH;

/// The adversarial framing: a maximal 9-byte unsigned-VInt length prefix
/// (`u64::MAX`) followed by a handful of trailing bytes. `*offset + blob_len`
/// overflows `usize`; the guard must return `Err` instead of panicking.
fn adversarial_preamble() -> Vec<u8> {
    let mut framing = encode_vuint(u64::MAX);
    framing.extend_from_slice(&[0u8; 8]);
    framing
}

/// Axis 1 + axis 2 together, on the reported input: a `u64::MAX` blob length in
/// the frozen preamble must yield a named `Error::Corruption`, never an
/// overflow panic.
#[test]
fn maximal_vuint_blob_length_returns_corruption_not_panic() {
    let data = adversarial_preamble();
    let mut offset = 0usize;
    let err = V5CompressedLegacyParser::read_frozen_preamble(&data, &mut offset, "list", "c")
        .expect_err("a u64::MAX frozen blob length must be rejected");
    match &err {
        Error::Corruption(msg) => {
            assert!(
                msg.contains("Frozen list 'c'") && msg.contains("exceeds maximum"),
                "corruption message must name the frozen collection and the cap (got {msg:?})"
            );
        }
        other => panic!("expected Error::Corruption, got {other:?}"),
    }
}

/// Axis 1 in isolation: a blob length just ABOVE `MAX_CELL_VALUE_LENGTH` but far
/// too small for `*offset + blob_len` to overflow `usize`. The saturating
/// comparison alone would also reject this (as "exceeds available data"), so the
/// assertion is on the CAP's message — proving the max guard fires on its own
/// rather than being masked by the comparison.
#[test]
fn max_guard_fires_independently_of_the_saturating_comparison() {
    let over_cap = MAX_CELL_VALUE_LENGTH + 1;
    let mut data = encode_vuint(over_cap);
    data.extend_from_slice(&[0u8; 8]);
    let mut offset = 0usize;
    let err = V5CompressedLegacyParser::read_frozen_preamble(&data, &mut offset, "set", "c")
        .expect_err("a frozen blob length above the cap must be rejected");
    match &err {
        Error::Corruption(msg) => {
            assert!(
                msg.contains(&format!("blob_len {over_cap} exceeds maximum")),
                "the CAP must reject this, not the availability check (got {msg:?})"
            );
        }
        other => panic!("expected Error::Corruption, got {other:?}"),
    }
}

/// A blob length within the cap but longer than the remaining data is still
/// rejected, and the reported "available" byte count stays correct (the old code
/// computed `data.len() - *offset`; the saturating rewrite must not change that
/// information content).
#[test]
fn in_cap_but_past_end_of_data_reports_available_bytes() {
    // 4-byte element count + 60 bytes of payload claimed, but only 12 bytes follow.
    let mut data = encode_vuint(64);
    data.extend_from_slice(&[0u8; 12]);
    let prefix_len = data.len() - 12;
    let mut offset = 0usize;
    let err = V5CompressedLegacyParser::read_frozen_preamble(&data, &mut offset, "map", "c")
        .expect_err("a blob length past the end of the data must be rejected");
    match &err {
        Error::Corruption(msg) => {
            assert!(
                msg.contains("exceeds available data 12"),
                "must report the 12 bytes actually available after the {prefix_len}-byte prefix \
                 (got {msg:?})"
            );
        }
        other => panic!("expected Error::Corruption, got {other:?}"),
    }
}

/// Happy path: a blob length within the cap AND within the data decodes cleanly —
/// neither guard rejects valid input — and `offset` advances past the VUInt
/// prefix and the i32 BE element count, with `blob_end` at the end of the blob.
#[test]
fn valid_preamble_decodes_and_advances_offset() {
    // blob = [i32 BE count = 2][8 bytes of element payload] => blob_len 12.
    let mut data = encode_vuint(12);
    let prefix_len = data.len();
    data.extend_from_slice(&2i32.to_be_bytes());
    data.extend_from_slice(&[0u8; 8]);

    let mut offset = 0usize;
    let (count, blob_end) =
        V5CompressedLegacyParser::read_frozen_preamble(&data, &mut offset, "list", "c")
            .expect("a valid frozen preamble must decode");
    assert_eq!(count, 2, "element count comes from the i32 BE header");
    assert_eq!(
        blob_end,
        prefix_len + 12,
        "blob_end bounds the blob body after the length prefix"
    );
    assert_eq!(
        offset,
        prefix_len + 4,
        "offset advances past the VUInt prefix and the element count"
    );
}
