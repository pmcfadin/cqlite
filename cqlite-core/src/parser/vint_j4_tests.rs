//! J4 (issue #1638) equivalence proofs for the one canonical read-side VInt
//! decoder (`parser::vint::{decode_unsigned, decode_signed}`).
//!
//! These tests prove the consolidation changes NO decode result:
//! 1. round-trip identity vs the exemplary write-side `serialization::vint`
//!    encoders (proptest over the full `u64`/`i64` range);
//! 2. a corpus differential vs a byte-identical snapshot of the pre-refactor
//!    unsigned bit-assembly (the old `parse_vuint`), over an exhaustive small
//!    byte space plus sampled wider widths and truncation boundaries;
//! 3. decoder-level truncation/framing guarantees (the I2 #1624 intent, asserted
//!    directly on the new pair).

use crate::parser::vint::{decode_signed, decode_unsigned, encode_vuint, MAX_VINT_SIZE};
use proptest::prelude::*;

/// Byte-identical snapshot of the pre-refactor `parse_vuint` unsigned
/// bit-assembly (per-byte index loop), returning `(value, consumed)` or `None`
/// on the same error conditions. This is the differential oracle — it MUST NOT
/// be "improved"; it exists to prove the new decoder matches the old one.
fn legacy_decode_unsigned(input: &[u8]) -> Option<(u64, usize)> {
    if input.is_empty() {
        return None;
    }
    let first_byte = input[0];
    let num_extra_bytes = first_byte.leading_ones() as usize;
    if num_extra_bytes > 8 || num_extra_bytes + 1 > input.len() {
        return None;
    }
    let value = if num_extra_bytes == 0 {
        (first_byte & 0x7F) as u64
    } else if num_extra_bytes == 8 {
        let mut value = 0u64;
        for &byte in input.iter().skip(1).take(num_extra_bytes) {
            value = (value << 8) | (byte as u64);
        }
        value
    } else {
        let data_bits_first = 8 - num_extra_bytes - 1;
        let mask = (1u8 << data_bits_first) - 1;
        let mut value = (first_byte & mask) as u64;
        for &byte in input.iter().skip(1).take(num_extra_bytes) {
            value = (value << 8) | (byte as u64);
        }
        value
    };
    Some((value, num_extra_bytes + 1))
}

/// Assert the new `decode_unsigned` matches the legacy oracle on `buf`.
fn assert_matches_legacy(buf: &[u8]) {
    let new = decode_unsigned(buf).ok();
    let old = legacy_decode_unsigned(buf);
    assert_eq!(
        new, old,
        "decode_unsigned diverged from the pre-refactor decoder on {buf:02x?}"
    );
}

#[test]
fn corpus_differential_all_one_and_two_byte_buffers() {
    // Exhaustive over every 1-byte and every 2-byte buffer: covers all lead
    // bytes (every leading-ones count) and every continuation byte, plus every
    // truncation of a multi-byte lead down to one byte.
    for a in 0u16..=255 {
        assert_matches_legacy(&[a as u8]);
        for b in 0u16..=255 {
            assert_matches_legacy(&[a as u8, b as u8]);
        }
    }
}

#[test]
fn corpus_differential_sampled_wide_and_truncated() {
    // Deterministic LCG-sampled wider buffers (widths up to 12) so the value
    // AND consumed length agree for 3..=9-byte vints and for over-long input
    // with trailing junk, plus truncated prefixes of each.
    let mut state = 0x1234_5678_9abc_def0u64;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) as u8
    };
    for _ in 0..20_000 {
        let len = 1 + (next() as usize % 12);
        let buf: Vec<u8> = (0..len).map(|_| next()).collect();
        assert_matches_legacy(&buf);
        // And every truncated prefix (framing/truncation boundaries).
        for cut in 1..buf.len() {
            assert_matches_legacy(&buf[..cut]);
        }
    }
}

#[test]
fn empty_input_errors_on_both() {
    assert!(decode_unsigned(&[]).is_err());
    assert!(decode_signed(&[]).is_err());
}

#[test]
fn truncated_multibyte_lead_errors() {
    // Bare multi-byte leads declare more bytes than are present.
    assert!(decode_unsigned(&[0x80]).is_err(), "0x80 declares 2 bytes");
    assert!(decode_unsigned(&[0xC0]).is_err(), "0xC0 declares 3 bytes");
    assert!(decode_signed(&[0xC0]).is_err());
    // 0xFF declares 8 continuation bytes (9 total); only 8 present → Err.
    assert!(decode_unsigned(&[0xFF, 1, 2, 3, 4, 5, 6, 7]).is_err());
}

#[test]
fn complete_value_is_framing_independent() {
    let (v_alone, n_alone) = decode_unsigned(&[0x80, 0x80]).expect("complete 2-byte vint");
    let (v_junk, n_junk) =
        decode_unsigned(&[0x80, 0x80, 0xAA, 0xBB]).expect("2-byte vint + trailing junk");
    assert_eq!(v_alone, v_junk, "value must not depend on framing");
    assert_eq!(n_alone, 2, "consumes exactly the encoded width");
    assert_eq!(
        n_junk, 2,
        "consumes exactly the encoded width, ignoring junk"
    );
}

#[test]
fn max_width_nine_byte_vint_decodes() {
    // 0xFF + 8 continuation bytes = the maximum 9-byte vint.
    let buf = [0xFF, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99];
    let (value, consumed) = decode_unsigned(&buf).expect("9-byte vint");
    assert_eq!(consumed, MAX_VINT_SIZE);
    assert_eq!(value, 0x1122_3344_5566_7788);
}

proptest! {
    /// The read-side `encode_vuint` and the new `decode_unsigned` round-trip
    /// (guards the read-side encoder against the decoder). Ungated — this holds
    /// on every feature set.
    #[test]
    fn roundtrip_read_side_encoder(value in any::<u64>()) {
        let buf = encode_vuint(value);
        let (decoded, consumed) = decode_unsigned(&buf).expect("decode read-side encoded vint");
        prop_assert_eq!(decoded, value);
        prop_assert_eq!(consumed, buf.len());
    }
}

/// Round-trip proofs against the exemplary write-side `serialization::vint`
/// encoders. That module is `#[cfg(feature = "write-support")]`, so these tests
/// only compile/run with `write-support`; the minimal-feature lib-test build
/// (`--no-default-features --features all-compression`) skips them, keeping the
/// ungated decode/proptest/corpus-differential coverage above building on every
/// feature set (roborev #1638 finding 1).
#[cfg(feature = "write-support")]
mod write_side_roundtrip {
    use super::{decode_signed, decode_unsigned};
    use crate::storage::serialization::vint as write_vint;
    use proptest::prelude::*;

    proptest! {
        /// Encode any u64 with the exemplary write-side encoder, decode with the
        /// new canonical decoder → identity of value and consumed length.
        #[test]
        fn roundtrip_unsigned_identity(value in any::<u64>()) {
            let mut buf = Vec::new();
            write_vint::encode_unsigned(value, &mut buf);
            let (decoded, consumed) = decode_unsigned(&buf).expect("decode encoded unsigned vint");
            prop_assert_eq!(decoded, value);
            prop_assert_eq!(consumed, buf.len());
        }

        /// Encode any i64 with the write-side signed (ZigZag) encoder, decode with
        /// the new signed decoder → identity of value and consumed length.
        #[test]
        fn roundtrip_signed_identity(value in any::<i64>()) {
            let mut buf = Vec::new();
            write_vint::encode_signed(value, &mut buf);
            let (decoded, consumed) = decode_signed(&buf).expect("decode encoded signed vint");
            prop_assert_eq!(decoded, value);
            prop_assert_eq!(consumed, buf.len());
        }
    }
}
