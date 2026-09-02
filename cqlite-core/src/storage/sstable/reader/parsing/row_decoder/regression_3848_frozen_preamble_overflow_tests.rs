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
//! ## The third axis: TRUNCATION (roborev job 66 on the round-1 fix)
//!
//! The sweep of the same class across the sibling decode sites (commit
//! `cf09db960`) rewrote their bounds checks into the saturating form but left
//! `let len = len_raw as usize;` sitting BEFORE the check. `usize` is 32 bits on
//! `wasm32-unknown-unknown` — a configured target of this workspace — so a
//! declared length of `1u64 << 32` truncates to `0usize` and a declared
//! `(1u64 << 32) + 8` truncates to exactly `8usize`: both then SATISFY a
//! saturating comparison against 8 remaining bytes, and the input is silently
//! misparsed instead of rejected. That is an ORDERING defect, not an arithmetic
//! one, so no amount of care in the comparison reaches it.
//!
//! 3. **The order** — [`vuint_length_within`] compares the raw `u64` against the
//!    available byte count widened to `u64` (always lossless) and casts ONLY
//!    once the value is proven to fit, so the surviving `as usize` is provably
//!    lossless on every target. [`checked_vuint_length`] adds the canonical
//!    corruption message, which reports the RAW `u64` rather than its truncated
//!    form.
//!
//! No gate component or CI lane builds a 32-bit target, so a
//! `#[cfg(target_pointer_width = "32")]` test would execute NOWHERE — and at
//! `usize` width on a 64-bit host the guard and a cast-first implementation
//! AGREE on every input, so a case that only asserts "an error came back" pins
//! nothing at all and would stay green if the guard were deleted (#3042).
//!
//! The guards are therefore parameterized over the narrowing TARGET WIDTH
//! (`vuint_length_within_as`, `checked_vuint_length_as`,
//! `checked_vuint_exact_length_as`, over `crate::parser::vint_narrow::LengthWidth`).
//! Production code calls the `usize` wrappers and behaves exactly as before; the
//! cases below instantiate the SAME code at `u32`, which IS the semantics of a
//! 32-bit `usize`, on any host. There the two implementations differ —
//! `((1u64 << 32) + 8) as u32 == 8` is accepted by a cast-first check and
//! rejected by the guard — so every case marked "DISCRIMINATING" below fails if
//! the guard is replaced by a plain narrowing cast. That was verified by
//! actually making the substitution and observing the reds, not by reasoning.
//!
//! ## The fourth axis: A POST-CAST EQUALITY CHECK IS NOT A GUARD (roborev job 67)
//!
//! The round-2 fix skipped the FIXED-WIDTH arms (`date`, `time`, `inet`,
//! `smallint`, `tinyint`) on the reasoning that "a truncated length fails the
//! `!= 4` equality check, so the cast is safe". That reasoning is WRONG, and it
//! is worth stating why in the test file that falsifies it: truncation is not a
//! randomising operation. An attacker CHOOSES the declared length so that its
//! low 32 bits equal the expected size — `(1u64 << 32) + 4` narrows to exactly
//! `4usize` on a 32-bit target and PASSES `!= 4`. The corrupt length is then
//! silently ACCEPTED as a valid 4-byte field and the following bytes are
//! misparsed. An equality test after the cast provides no protection against
//! truncation whatsoever; it only looks as if it does.
//!
//! 4. **Fixed widths** — [`checked_vuint_exact_length`] compares the raw `u64`
//!    against each allowed size widened to `u64` (lossless), so the equality is
//!    exact on every target, and returns the MATCHED allowed size, narrowing
//!    nothing. Its cases are made discriminating the same way as axis 3's, by
//!    instantiating the guard at `u32` width: a cast-first
//!    `((1u64 << 32) + 4) as u32 == 4` is accepted as a valid 4-byte date, the
//!    guard rejects it.
//!
//! ## Dataset independence
//!
//! These tests call the associated function DIRECTLY. It needs no
//! `SSTableReader`, no `CQLITE_DATASETS_ROOT`, no fixture and no feature flag, so
//! they run unconditionally in EVERY build and lane and can never pass vacuously
//! (there is no dataset-absent path that could silently `return`). Debug/test
//! builds run with `overflow-checks = true`, so a regressed add would abort the
//! process here rather than silently wrap.

use super::vuint_length::{
    checked_vuint_exact_length_as, checked_vuint_length_as, vuint_length_within_as,
};
use super::{
    checked_vuint_exact_length, checked_vuint_length, vuint_length_within, V5CompressedLegacyParser,
};
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

/// Every length that would TRUNCATE on a 32-bit target, and that would pass a
/// post-cast availability check there. The guard compares before it converts, so
/// each one is rejected on every target.
///
/// `(1u64 << 32) + 8` is the sharpest case: it narrows to exactly `8usize`, so a
/// post-cast `len > available` against 8 remaining bytes would be satisfied and
/// 8 bytes of a claimed 4 GiB run would be decoded as the whole value.
const TRUNCATING_LENGTHS: [u64; 4] = [1u64 << 32, (1u64 << 32) + 8, u32::MAX as u64 + 1, u64::MAX];

/// Axis 3, DISCRIMINATING: the shared guard instantiated at 32-BIT width — the
/// semantics of a 32-bit `usize`, reproduced on any host.
///
/// **If the guard were replaced by a plain narrowing cast, THIS TEST WOULD
/// FAIL.** A cast-first check narrows `(1u64 << 32) + 8` to exactly `8u32` and
/// then finds it within the 8 available bytes, so it returns `Some(8)`; the
/// asserted `truncated` value below is what such an implementation hands back.
/// Verified by substitution: with `vuint_length_within_as` rewritten as
/// `let len = len_raw as T; (len <= available).then_some(len)`, this case reds.
#[test]
fn truncating_vuint_lengths_are_rejected_at_32_bit_narrowing_width() {
    for len_raw in TRUNCATING_LENGTHS {
        let truncated = len_raw as u32;
        assert_ne!(
            u64::from(truncated),
            len_raw,
            "case setup: the cast must lose information for {len_raw}"
        );
        assert_eq!(
            vuint_length_within_as::<u32>(len_raw, u32::MAX),
            None,
            "length {len_raw} is not representable in 32 bits and must be REJECTED, \
             not narrowed to {truncated}"
        );
    }
    // The sharpest case, stated explicitly: the declared length narrows to
    // exactly the number of available bytes, so a post-cast availability check
    // is SATISFIED and 8 bytes of a claimed 4 GiB run would be decoded as the
    // whole value.
    assert_eq!(((1u64 << 32) + 8) as u32, 8, "case setup: the low bits");
    assert_eq!(
        vuint_length_within_as::<u32>((1u64 << 32) + 8, 8u32),
        None,
        "a length that truncates to exactly the available byte count must be rejected"
    );
}

/// Axis 3, DISCRIMINATING, the message half at 32-bit width: the rejection is a
/// NAMED `Error::Corruption` reporting the RAW `u64`.
///
/// **If the guard were replaced by a plain narrowing cast, THIS TEST WOULD
/// FAIL** — a cast-first check returns `Ok(8)` here and there is no error to
/// inspect. Verified by substitution.
#[test]
fn truncating_vuint_lengths_yield_corruption_at_32_bit_narrowing_width() {
    let len_raw = (1u64 << 32) + 8;
    let err = checked_vuint_length_as::<u32>(len_raw, 8u32, "Frozen element", "c", "blob")
        .expect_err("a length that only FITS once truncated must be rejected");
    match &err {
        Error::Corruption(msg) => assert!(
            msg.contains(&format!("need {len_raw} bytes for blob"))
                && msg.contains("only 8 available"),
            "must name the RAW length, not its truncated form (got {msg:?})"
        ),
        other => panic!("expected Error::Corruption, got {other:?}"),
    }
}

/// Axis 3 at the production `usize` width, for the record: the guard rejects a
/// length past the available bytes.
///
/// DECLARED LIMIT — not a pin. On a 64-bit host `len_raw as usize` is the
/// identity, so a cast-first implementation rejects these too (as "past the
/// available bytes"). The discriminating coverage is the 32-bit-width cases
/// above; this one holds the `usize` wrapper to the same contract.
#[test]
fn truncating_vuint_lengths_are_rejected_before_the_usize_cast() {
    for len_raw in TRUNCATING_LENGTHS {
        assert_eq!(
            vuint_length_within(len_raw, 8),
            None,
            "length {len_raw} exceeds the 8 available bytes and must be rejected, \
             not narrowed to a value that fits"
        );
    }
}

/// Axis 3, the message half: the rejection is a NAMED `Error::Corruption` and it
/// reports the RAW `u64`, so an adversarial prefix is diagnosable in full rather
/// than in whatever it happened to truncate to.
#[test]
fn truncating_vuint_lengths_yield_named_corruption_naming_the_raw_length() {
    for len_raw in TRUNCATING_LENGTHS {
        let err = checked_vuint_length(len_raw, 8, "Frozen element", "c", "blob")
            .expect_err("a length past the available bytes must be rejected");
        match &err {
            Error::Corruption(msg) => {
                assert!(
                    msg.contains(&format!("need {len_raw} bytes for blob"))
                        && msg.contains("Frozen element 'c'")
                        && msg.contains("only 8 available"),
                    "must name the subject, the RAW length and the available bytes (got {msg:?})"
                );
            }
            other => panic!("expected Error::Corruption, got {other:?}"),
        }
    }
}

/// The guard must not be trivially over-strict: a legitimate length within the
/// available bytes converts cleanly, including the two boundary cases (an empty
/// run, and a run that consumes every remaining byte).
#[test]
fn vuint_length_guard_accepts_a_legitimate_length() {
    assert_eq!(vuint_length_within(5, 8), Some(5), "5 of 8 bytes fits");
    assert_eq!(
        vuint_length_within(8, 8),
        Some(8),
        "exactly the remainder fits"
    );
    assert_eq!(
        vuint_length_within(0, 0),
        Some(0),
        "an empty run at the end fits"
    );
    assert_eq!(
        checked_vuint_length(5, 8, "Frozen element", "c", "text")
            .expect("a length within the available bytes must be accepted"),
        5
    );
}

/// The boundary in the other direction: one byte past the remainder is rejected,
/// so the accept case above is not passing because the guard accepts everything.
#[test]
fn vuint_length_guard_rejects_one_byte_past_the_available_bytes() {
    assert_eq!(vuint_length_within(9, 8), None, "9 of 8 bytes must not fit");
    assert_eq!(vuint_length_within(1, 0), None, "no bytes remain");
}

/// The fixed-width fields of the scalar/frozen decode arms, as
/// `(what, allowed widths)`. Derived from the arms that route through
/// [`checked_vuint_exact_length`]: `date` (4), `time` (8), `smallint` (2),
/// `tinyint` (1) and `inet` (4 or 16).
const FIXED_WIDTH_FIELDS: [(&str, &[usize]); 5] = [
    ("date", &[4]),
    ("time", &[8]),
    ("smallint", &[2]),
    ("tinyint", &[1]),
    ("inet", &[4, 16]),
];

/// Axis 4, DISCRIMINATING, and the case that FALSIFIES the "the `!= 4` check
/// catches it" reasoning: a declared length whose LOW 32 BITS equal the expected
/// fixed width, checked at 32-bit narrowing width.
///
/// **If the guard were replaced by a plain narrowing cast, THIS TEST WOULD
/// FAIL**: `((1u64 << 32) + 4) as u32 == 4` passes an equality check against the
/// allowed width, so the corrupt length is ACCEPTED as a valid 4-byte date and
/// there is no error to inspect. Verified by substitution, not by reasoning.
#[test]
fn fixed_width_lengths_colliding_in_the_low_32_bits_are_rejected_at_32_bit_width() {
    for (what, allowed) in FIXED_WIDTH_FIELDS {
        let allowed32 = allowed
            .iter()
            .map(|&size| size as u32)
            .collect::<Vec<u32>>();
        for &width in allowed {
            let colliding = (1u64 << 32) + width as u64;
            // What a cast-first implementation computes: EXACTLY the expected
            // width, so its equality check passes and the corrupt length is
            // ACCEPTED as a valid field.
            assert_eq!(
                colliding as u32, width as u32,
                "case setup: {colliding} must collide with the {what} width in 32 bits"
            );
            let err =
                checked_vuint_exact_length_as::<u32>(colliding, &allowed32, "Cell", "c", what)
                    .expect_err(
                        "a length colliding with the fixed width in its low 32 bits must be \
                 rejected, not accepted as that width",
                    );
            match &err {
                Error::Corruption(msg) => assert!(
                    msg.contains(&colliding.to_string()) && msg.contains("Cell 'c'"),
                    "must name the subject and the RAW length {colliding} (got {msg:?})"
                ),
                other => panic!("expected Error::Corruption, got {other:?}"),
            }
        }
    }
}

/// Axis 4 at the production `usize` width, for the record.
///
/// DECLARED LIMIT — not a pin: on a 64-bit host a cast-first implementation
/// rejects `(1u64 << 32) + 4` too, because `4294967300usize != 4`. The
/// discriminating case is the 32-bit-width one above; this one holds the `usize`
/// wrapper to the same contract and message shape.
#[test]
fn fixed_width_lengths_colliding_in_the_low_32_bits_are_rejected() {
    for (what, allowed) in FIXED_WIDTH_FIELDS {
        for &width in allowed {
            // The collision: identical to `width` in the low 32 bits, so every
            // post-cast comparison against `width` is satisfied on a 32-bit target.
            let colliding = (1u64 << 32) + width as u64;
            let err = checked_vuint_exact_length(colliding, allowed, "Cell", "c", what).expect_err(
                "a length colliding with the fixed width in its low 32 bits \
                             must be rejected, not accepted as that width",
            );
            match &err {
                Error::Corruption(msg) => assert!(
                    msg.contains(&colliding.to_string()) && msg.contains("Cell 'c'"),
                    "must name the subject and the RAW length {colliding} (got {msg:?})"
                ),
                other => panic!("expected Error::Corruption, got {other:?}"),
            }
        }
    }
}

/// The same axis at the other truncating lengths, at BOTH widths: none may be
/// accepted for any fixed width.
///
/// The 32-bit-width half is the one that can fail against a cast-first
/// implementation (`(1u64 << 32) + 1` narrows to `1u32`, the legal `tinyint`
/// width); the `usize` half is the wrapper's contract at production width. The
/// error is matched as a NAMED `Error::Corruption` rather than "some error", so
/// the case cannot pass for an unrelated reason.
#[test]
fn fixed_width_guard_rejects_every_truncating_length() {
    for (what, allowed) in FIXED_WIDTH_FIELDS {
        let allowed32 = allowed
            .iter()
            .map(|&size| size as u32)
            .collect::<Vec<u32>>();
        for len_raw in TRUNCATING_LENGTHS {
            for err in [
                checked_vuint_exact_length(len_raw, allowed, "Frozen element", "c", what)
                    .expect_err("truncating length must be rejected at usize width"),
                checked_vuint_exact_length_as::<u32>(
                    len_raw,
                    &allowed32,
                    "Frozen element",
                    "c",
                    what,
                )
                .expect_err("truncating length must be rejected at 32-bit width"),
            ] {
                match &err {
                    Error::Corruption(msg) => assert!(
                        msg.contains(&len_raw.to_string()),
                        "{what}: rejection of {len_raw} must name the RAW length (got {msg:?})"
                    ),
                    other => panic!("{what}: expected Error::Corruption, got {other:?}"),
                }
            }
        }
    }
}

/// The happy path per guarded width, so the guard cannot be trivially
/// over-strict: every width the format allows converts to itself.
#[test]
fn fixed_width_guard_accepts_every_allowed_width() {
    for (what, allowed) in FIXED_WIDTH_FIELDS {
        for &width in allowed {
            assert_eq!(
                checked_vuint_exact_length(width as u64, allowed, "Cell", "c", what)
                    .expect("an allowed width must be accepted"),
                width,
                "{what}: width {width} is the on-disk size and must be accepted"
            );
        }
    }
}

/// The ordinary-corruption direction, so the accept case above is not passing
/// because the guard accepts everything: a width the format does not allow is
/// rejected, and the two message shapes name the allowed size(s).
#[test]
fn fixed_width_guard_rejects_a_disallowed_width_naming_the_allowed_sizes() {
    let date = checked_vuint_exact_length(3, &[4], "Cell", "c", "date")
        .expect_err("3 is not a legal date length");
    match &date {
        Error::Corruption(msg) => assert!(
            msg.contains("expected date length 4, got 3"),
            "single-width shape must name the required size (got {msg:?})"
        ),
        other => panic!("expected Error::Corruption, got {other:?}"),
    }

    let inet = checked_vuint_exact_length(5, &[4, 16], "Cell", "c", "inet")
        .expect_err("5 is neither an IPv4 nor an IPv6 address length");
    match &inet {
        Error::Corruption(msg) => assert!(
            msg.contains("invalid inet length 5, expected 4 or 16"),
            "multi-width shape must name every allowed size (got {msg:?})"
        ),
        other => panic!("expected Error::Corruption, got {other:?}"),
    }
}
