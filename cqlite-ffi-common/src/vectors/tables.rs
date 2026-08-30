//! The committed cross-binding vector tables (issue #1452).
//!
//! Every expectation here was derived with an **independent** bignum
//! implementation — CPython's arbitrary-precision `int` — and, for the
//! multi-kilobyte magnitudes, cross-checked analytically with
//! `floor(log10(v)) + 1`. None of it was read off this crate's output.
//!
//! Coverage mirrors the edge cases the change's spec enumerates for DECIMAL,
//! VARINT and INET, and each table carries at least one entry whose expected
//! outcome is a typed error.
//!
//! # The three [`Expect::Digested`] entries, and where their hashes come from
//!
//! Three DECIMAL renderings run to thousands of digits, so committing them
//! literally would be unreadable. They carry a readable `digest` PLUS the
//! SHA-256 hex of the FULL rendering, which is the check that actually binds the
//! digits — a digest alone pins only the digit COUNT and the surrounding form,
//! so two bindings emitting different digits of the same length would both
//! satisfy it.
//!
//! Each hash was derived in CPython, from the entry's input bytes and NOT from
//! this crate's output:
//!
//! ```text
//! sys.set_int_max_str_digits(0)
//! v = int.from_bytes(b"\x7f" * n, "big", signed=True)
//! positional (n <= DECIMAL_POSITIONAL_MAX_BYTES): text = str(v)[:-scale] + "." + str(v)[-scale:]
//! exponent   (n >  DECIMAL_POSITIONAL_MAX_BYTES): text = str(v) + "e-" + str(scale)
//! hashlib.sha256(text.encode("utf-8")).hexdigest()
//! ```
//!
//! The two derivations are independent in the part that matters: CPython's
//! bignum produced the digits, while the committed `digest` beside each hash
//! independently pins the rendering FORM (positional vs exponent, exponent
//! value, digit count) that the script above assumes — so a form disagreement
//! surfaces as a digest failure and a digit disagreement as a hash failure.

use super::{DecimalVector, Expect, InetVector, Input, VarintVector};
use crate::decimal::{DECIMAL_MAX_UNSCALED_BYTES, DECIMAL_POSITIONAL_MAX_BYTES};

/// `123` as one byte.
const P123: Input = Input::Literal(&[123]);
/// `-123` as one two's-complement byte (`0x85`).
const N123: Input = Input::Literal(&[0x85]);
/// `1` as one byte.
const ONE: Input = Input::Literal(&[0x01]);

/// The canonical refusal message for a magnitude one byte past the ceiling with
/// `scale = 3`. Written out in full because it is the ONE spelling both bindings
/// must surface; `crate::decimal::DecimalError`'s `Display` is pinned against it
/// by the crate's own vector test.
const PAST_CEILING_MESSAGE: &str = "DECIMAL cell not representable (scale=3, unscaled_len=32769 \
     bytes, max_unscaled=32768 bytes): corrupt SSTable — refusing to enter a \
     superlinear render on a pathological magnitude (issue #1754)";

/// DECIMAL entries: `(scale, unscaled)` → rendering, or a typed refusal.
pub const DECIMAL_VECTORS: &[DecimalVector] = &[
    DecimalVector {
        name: "decimal/empty-unscaled-is-zero",
        scale: 0,
        unscaled: Input::Literal(&[]),
        expect: Expect::Value("0"),
    },
    DecimalVector {
        name: "decimal/empty-unscaled-ignores-scale",
        scale: 5,
        unscaled: Input::Literal(&[]),
        expect: Expect::Value("0"),
    },
    DecimalVector {
        name: "decimal/scale-zero",
        scale: 0,
        unscaled: P123,
        expect: Expect::Value("123"),
    },
    DecimalVector {
        name: "decimal/scale-shorter-than-digits",
        scale: 2,
        unscaled: P123,
        expect: Expect::Value("1.23"),
    },
    DecimalVector {
        name: "decimal/scale-equal-to-digits",
        scale: 3,
        unscaled: P123,
        expect: Expect::Value("0.123"),
    },
    DecimalVector {
        name: "decimal/scale-longer-than-digits",
        scale: 5,
        unscaled: P123,
        expect: Expect::Value("0.00123"),
    },
    DecimalVector {
        name: "decimal/negative-scale",
        scale: -2,
        unscaled: P123,
        expect: Expect::Value("123e2"),
    },
    DecimalVector {
        name: "decimal/negative-magnitude-scale-zero",
        scale: 0,
        unscaled: N123,
        expect: Expect::Value("-123"),
    },
    DecimalVector {
        name: "decimal/negative-magnitude-scale-shorter-than-digits",
        scale: 2,
        unscaled: N123,
        expect: Expect::Value("-1.23"),
    },
    DecimalVector {
        name: "decimal/negative-magnitude-scale-equal-to-digits",
        scale: 3,
        unscaled: N123,
        expect: Expect::Value("-0.123"),
    },
    DecimalVector {
        name: "decimal/negative-magnitude-scale-longer-than-digits",
        scale: 5,
        unscaled: N123,
        expect: Expect::Value("-0.00123"),
    },
    DecimalVector {
        name: "decimal/negative-magnitude-negative-scale",
        scale: -2,
        unscaled: N123,
        expect: Expect::Value("-123e2"),
    },
    // `i32::MAX` / `i32::MIN` scales: a well-formed value with an extreme
    // exponent renders in exponent form. `i32::MIN` is the case a plain `-scale`
    // would overflow on.
    DecimalVector {
        name: "decimal/scale-i32-max",
        scale: i32::MAX,
        unscaled: ONE,
        expect: Expect::Value("1e-2147483647"),
    },
    DecimalVector {
        name: "decimal/scale-i32-min",
        scale: i32::MIN,
        unscaled: ONE,
        expect: Expect::Value("1e2147483648"),
    },
    DecimalVector {
        name: "decimal/scale-i32-min-negative-magnitude",
        scale: i32::MIN,
        unscaled: N123,
        expect: Expect::Value("-123e2147483648"),
    },
    // The positional/exponent boundary. AT the threshold the render is
    // positional: 2466 digits, the last two of which are the fractional part,
    // so the digest keeps them literally.
    DecimalVector {
        name: "decimal/positional-boundary-at-threshold",
        scale: 2,
        unscaled: Input::Repeated {
            byte: 0x7f,
            len: DECIMAL_POSITIONAL_MAX_BYTES,
        },
        expect: Expect::Digested {
            digest: "{2464}.83",
            sha256: "37a4b95da17180c651e4941c13565a13f91d7cc315ed77d27067d7cebb734245",
        },
    },
    // ONE byte past the threshold switches to exponent form, preserving all
    // 2469 digits.
    DecimalVector {
        name: "decimal/positional-boundary-one-past",
        scale: 2,
        unscaled: Input::Repeated {
            byte: 0x7f,
            len: DECIMAL_POSITIONAL_MAX_BYTES + 1,
        },
        expect: Expect::Digested {
            digest: "{2469}e-2",
            sha256: "3aae6f7d370ae9d471ff85a78d69f8d78f931319ed791e9d6456b42ea8c38724",
        },
    },
    // THE convergence case (issue #1452): before the shared implementation, Node
    // rendered this and Python raised `CqliteError`. Both must now render it.
    DecimalVector {
        name: "decimal/large-well-formed-2000-bytes-scale-3",
        scale: 3,
        unscaled: Input::Repeated {
            byte: 0x7f,
            len: 2000,
        },
        expect: Expect::Digested {
            digest: "{4817}e-3",
            sha256: "e1ec7b41fe833049052e89e01d3cdda36fcfc6dd69ec5deb03d52c116aa55214",
        },
    },
    // One byte past the refusal ceiling: a typed refusal in BOTH bindings,
    // carrying the one canonical message.
    DecimalVector {
        name: "decimal/past-refusal-ceiling",
        scale: 3,
        unscaled: Input::Repeated {
            byte: 0x7f,
            len: DECIMAL_MAX_UNSCALED_BYTES + 1,
        },
        expect: Expect::Error(PAST_CEILING_MESSAGE),
    },
];

/// VARINT entries: big-endian two's-complement payload → canonical decimal
/// string. Every expectation is short enough to be committed verbatim.
pub const VARINT_VECTORS: &[VarintVector] = &[
    VarintVector {
        name: "varint/empty-is-zero",
        bytes: Input::Literal(&[]),
        expect: Expect::Value("0"),
    },
    VarintVector {
        name: "varint/zero-byte",
        bytes: Input::Literal(&[0x00]),
        expect: Expect::Value("0"),
    },
    VarintVector {
        name: "varint/single-positive-byte",
        bytes: Input::Literal(&[0x7f]),
        expect: Expect::Value("127"),
    },
    VarintVector {
        name: "varint/single-negative-byte",
        bytes: Input::Literal(&[0x80]),
        expect: Expect::Value("-128"),
    },
    VarintVector {
        name: "varint/minus-one",
        bytes: Input::Literal(&[0xff]),
        expect: Expect::Value("-1"),
    },
    VarintVector {
        name: "varint/sign-extension-three-bytes",
        bytes: Input::Literal(&[0xff, 0xff, 0x01]),
        expect: Expect::Value("-255"),
    },
    VarintVector {
        name: "varint/eight-bytes-i64-max",
        bytes: Input::Literal(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        expect: Expect::Value("9223372036854775807"),
    },
    VarintVector {
        name: "varint/eight-bytes-i64-min",
        bytes: Input::Literal(&[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        expect: Expect::Value("-9223372036854775808"),
    },
    VarintVector {
        name: "varint/nine-bytes-positive",
        bytes: Input::Literal(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        expect: Expect::Value("18446744073709551616"),
    },
    VarintVector {
        name: "varint/nine-bytes-negative",
        bytes: Input::Literal(&[0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        expect: Expect::Value("-18446744073709551616"),
    },
    VarintVector {
        name: "varint/seventeen-bytes-positive",
        bytes: Input::Literal(&[
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00,
        ]),
        expect: Expect::Value("340282366920938463463374607431768211456"),
    },
    VarintVector {
        name: "varint/seventeen-bytes-negative",
        bytes: Input::Literal(&[
            0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff,
        ]),
        expect: Expect::Value("-340282366920938463463374607431768211457"),
    },
    VarintVector {
        // Negating this magnitude propagates a carry across both u64 word
        // boundaries — the case the deleted hand-rolled negate loop existed for.
        name: "varint/negation-carries-across-word-boundaries",
        bytes: Input::Literal(&[
            0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01,
        ]),
        expect: Expect::Value("-340282366920938463463374607431768211455"),
    },
];

/// INET entries: packed address bytes → address text, or the one
/// malformed-length message.
pub const INET_VECTORS: &[InetVector] = &[
    InetVector {
        name: "inet/ipv4-dotted-quad",
        bytes: Input::Literal(&[192, 168, 1, 1]),
        expect: Expect::Value("192.168.1.1"),
    },
    InetVector {
        name: "inet/ipv4-all-zeros",
        bytes: Input::Literal(&[0, 0, 0, 0]),
        expect: Expect::Value("0.0.0.0"),
    },
    InetVector {
        name: "inet/ipv4-broadcast",
        bytes: Input::Literal(&[255, 255, 255, 255]),
        expect: Expect::Value("255.255.255.255"),
    },
    InetVector {
        name: "inet/ipv6-compressible",
        bytes: Input::Literal(&[
            0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70,
            0x73, 0x34,
        ]),
        expect: Expect::Value("2001:db8:85a3::8a2e:370:7334"),
    },
    InetVector {
        name: "inet/ipv6-loopback",
        bytes: Input::Literal(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
        expect: Expect::Value("::1"),
    },
    InetVector {
        name: "inet/ipv6-unspecified",
        bytes: Input::Repeated { byte: 0, len: 16 },
        expect: Expect::Value("::"),
    },
    InetVector {
        name: "inet/malformed-empty",
        bytes: Input::Literal(&[]),
        expect: Expect::Error("Invalid inet address length: 0 (expected 4 or 16)"),
    },
    InetVector {
        name: "inet/malformed-five-bytes",
        bytes: Input::Repeated { byte: 1, len: 5 },
        expect: Expect::Error("Invalid inet address length: 5 (expected 4 or 16)"),
    },
    InetVector {
        name: "inet/malformed-fifteen-bytes",
        bytes: Input::Repeated { byte: 1, len: 15 },
        expect: Expect::Error("Invalid inet address length: 15 (expected 4 or 16)"),
    },
    InetVector {
        name: "inet/malformed-seventeen-bytes",
        bytes: Input::Repeated { byte: 1, len: 17 },
        expect: Expect::Error("Invalid inet address length: 17 (expected 4 or 16)"),
    },
];
