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

use super::{
    DecimalVector, Expect, InetVector, Input, JsonHostKind, JsonNumberVector, VarintVector,
};
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

/// Committed JSON-number cross-binding vectors (issue #3505).
///
/// # What these bind that no other test does
///
/// `json_number_tests.rs` pins [`crate::json_number::classify_json_number`], and
/// the bindings' `values_equal` harness tests pin their comparison rules — but
/// NEITHER proves a binding's production adapter (`json_number_to_py`,
/// `json_number_to_napi`) actually calls the classifier. Before this table, the
/// mutation "make the `U64` arm `u as f64`" reddened **nothing in the
/// repository**, so #3505's observable claim — `u64::MAX` reaches Python as an
/// exact `int` and JS as a `BigInt` — was asserted by no test at all.
///
/// # Why every entry is a VALUE and none is a refusal
///
/// The other three tables each carry at least one entry whose expected outcome
/// is a typed error. This one structurally cannot: the only refusing arm is
/// [`crate::json_number::JsonNumberClass::Beyond`], which is UNREACHABLE in a
/// default build — `serde_json`'s parser collapses an over-range integer literal
/// to an `f64` before any CQLite code runs (measured in `json_number.rs`, pinned
/// by `beyond_is_unreachable_because_the_parser_collapses_overflow_to_f64`).
/// Committing a refusal entry would mean faking an input that cannot occur.
///
/// # Why over-range integer literals are excluded
///
/// `18446744073709551616` and friends classify `F64` (the parser already lost
/// them), and the two hosts stringify the resulting `f64` DIFFERENTLY —
/// CPython's `str` gives `1.8446744073709552e+19` while JS's `String` gives
/// `18446744073709552000`. A single committed rendering could not satisfy both,
/// and the residual is a documented parser limitation rather than a
/// cross-binding contract. Same reason the float entries are restricted to short
/// decimal literals: `1e19` stringifies as `1e+19` in Python and
/// `10000000000000000000` in JS.
///
/// Expectations were derived from the literals themselves (CPython `int`, which
/// is arbitrary precision), never from either binding's output.
pub const JSON_NUMBER_VECTORS: &[JsonNumberVector] = &[
    JsonNumberVector {
        name: "zero",
        json_text: "0",
        host_kind: JsonHostKind::Integer,
        expect: Expect::Value("0"),
    },
    JsonNumberVector {
        name: "i32_max",
        json_text: "2147483647",
        host_kind: JsonHostKind::Integer,
        expect: Expect::Value("2147483647"),
    },
    JsonNumberVector {
        name: "i32_min",
        json_text: "-2147483648",
        host_kind: JsonHostKind::Integer,
        expect: Expect::Value("-2147483648"),
    },
    // Node switches from `number` to `BigInt` here; Python does not switch at
    // all. Both must still render the same digits.
    JsonNumberVector {
        name: "i32_max_plus_1",
        json_text: "2147483648",
        host_kind: JsonHostKind::Integer,
        expect: Expect::Value("2147483648"),
    },
    JsonNumberVector {
        name: "i64_min",
        json_text: "-9223372036854775808",
        host_kind: JsonHostKind::Integer,
        expect: Expect::Value("-9223372036854775808"),
    },
    JsonNumberVector {
        name: "i64_max",
        json_text: "9223372036854775807",
        host_kind: JsonHostKind::Integer,
        expect: Expect::Value("9223372036854775807"),
    },
    // THE #3505 CLASS: above `i64::MAX`, so `as_i64()` returns `None` and the
    // pre-fix code fell to `as_f64()`, which succeeded LOSSILY.
    JsonNumberVector {
        name: "i64_max_plus_1",
        json_text: "9223372036854775808",
        host_kind: JsonHostKind::Integer,
        expect: Expect::Value("9223372036854775808"),
    },
    JsonNumberVector {
        name: "u64_max_minus_1",
        json_text: "18446744073709551614",
        host_kind: JsonHostKind::Integer,
        expect: Expect::Value("18446744073709551614"),
    },
    // The headline case: `f64` rounds this to 18446744073709551616, so a lossy
    // arm renders 18446744073709552000 (JS) / 1.8446744073709552e+19 (Python).
    JsonNumberVector {
        name: "u64_max",
        json_text: "18446744073709551615",
        host_kind: JsonHostKind::Integer,
        expect: Expect::Value("18446744073709551615"),
    },
    // Float LITERALS must keep arriving as the host's double — the fix must not
    // over-reach and turn a genuine float column into an integer.
    JsonNumberVector {
        name: "float_one_and_a_half",
        json_text: "1.5",
        host_kind: JsonHostKind::Float,
        expect: Expect::Value("1.5"),
    },
    JsonNumberVector {
        name: "float_negative_two_and_a_quarter",
        json_text: "-2.25",
        host_kind: JsonHostKind::Float,
        expect: Expect::Value("-2.25"),
    },
];
