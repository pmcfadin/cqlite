//! Unit tests for the single VARINT decoding implementation (issue #1452).
//!
//! Coverage is the list the change's spec enumerates: empty input; a single
//! positive byte; a single negative byte; exactly 8 bytes; 9 and 17 bytes in
//! both signs (crossing the word boundary the old Node path special-cased);
//! `i64::MIN` and `i64::MAX`; and a magnitude whose negation carries across a
//! word boundary. Every case also asserts the word projection reassembles to the
//! same `BigInt`.

use super::*;
use num_bigint::BigInt;

/// Every byte string the suite decodes, paired with its expected decimal string.
///
/// Expectations were derived with an INDEPENDENT bignum implementation
/// (CPython's `int.from_bytes(..., "big", signed=True)`), never from this code.
fn cases() -> Vec<(&'static str, Vec<u8>, &'static str)> {
    vec![
        ("empty", vec![], "0"),
        ("single positive byte", vec![0x7f], "127"),
        ("single negative byte", vec![0x80], "-128"),
        ("single negative byte -1", vec![0xff], "-1"),
        ("zero byte", vec![0x00], "0"),
        (
            "exactly 8 bytes, i64::MAX",
            vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            "9223372036854775807",
        ),
        (
            "exactly 8 bytes, i64::MIN",
            vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            "-9223372036854775808",
        ),
        (
            "9 bytes positive (crosses the u64 word boundary)",
            vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            "18446744073709551616",
        ),
        (
            "9 bytes negative (crosses the u64 word boundary)",
            vec![0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            "-18446744073709551616",
        ),
        (
            "17 bytes positive",
            vec![
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00,
            ],
            "340282366920938463463374607431768211456",
        ),
        (
            "17 bytes negative",
            vec![
                0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0xff, 0xff, 0xff,
            ],
            "-340282366920938463463374607431768211457",
        ),
        (
            // Negating this magnitude propagates a carry across both u64 word
            // boundaries — the case the old hand-rolled negate loop existed for.
            "negation carries across two word boundaries",
            vec![
                0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x01,
            ],
            "-340282366920938463463374607431768211455",
        ),
        (
            "sign extension from a 3-byte negative",
            vec![0xff, 0xff, 0x01],
            "-255",
        ),
    ]
}

#[test]
fn decodes_big_endian_twos_complement_at_every_width() {
    for (name, bytes, expected) in cases() {
        assert_eq!(
            varint_to_bigint(&bytes).to_string(),
            expected,
            "case `{name}`"
        );
    }
}

#[test]
fn word_projection_reassembles_to_the_same_bigint() {
    for (name, bytes, _) in cases() {
        let value = varint_to_bigint(&bytes);
        let (is_negative, words) = varint_to_sign_and_le_words(&bytes);
        assert_eq!(
            bigint_from_sign_and_le_words(is_negative, &words),
            value,
            "case `{name}`: the word projection must reproduce the BigInt exactly"
        );
        assert_eq!(
            is_negative,
            value < BigInt::from(0),
            "case `{name}`: the sign flag must match the value's sign"
        );
    }
}

#[test]
fn zero_projects_to_an_empty_positive_magnitude() {
    assert_eq!(varint_to_sign_and_le_words(&[]), (false, vec![]));
    assert_eq!(varint_to_sign_and_le_words(&[0x00]), (false, vec![]));
}
