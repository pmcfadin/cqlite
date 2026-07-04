//! Issue #1630 — the `CqlTypeId::Blob` arm of the primitive decoder must NOT
//! infer framing from byte patterns (no-heuristics mandate, issue #28).
//!
//! `parse_cql_value` is the schema-framed decoder: the caller has already
//! extracted exactly the bytes belonging to a cell (length-framing happens at
//! the cell level), so the entire `input` slice IS the blob value verbatim —
//! mirroring the sibling Ascii/Varchar arm, `parse_cql_value_raw` and
//! `parse_blob_value`.
//!
//! Historically this arm carried two heuristics that these tests forbid:
//!   1. a literal match on the 16-byte fixture `[0x00,0x01,…,0x0F]`, and
//!   2. a guessed 4-byte big-endian length prefix that re-framed the blob.
//!
//! Both violated the mandate and corrupted legitimate blob payloads whose
//! leading bytes happened to look like a length or the fixture.
//!
//! The genuinely VInt-framed path (`parse_blob`, used by the write side's
//! tagged serialization and by `parse_cql_value_with_schema`) is unaffected
//! and is exercised separately below.

use cqlite_core::parser::types::{parse_blob, parse_cql_value, serialize_cql_value, CqlTypeId};
use cqlite_core::parser::vint::encode_vint;
use cqlite_core::types::Value;
use proptest::prelude::*;

/// Adversarial: a framed blob whose content is exactly the old hardcoded
/// fixture must round-trip verbatim through the ordinary decode path — not
/// because it is special-cased, but because the whole slice is the value.
#[test]
fn framed_blob_exact_fixture_roundtrips_verbatim() {
    let content: [u8; 16] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
        0x0F,
    ];
    let (rem, value) = parse_cql_value(&content, CqlTypeId::Blob).expect("blob must decode");
    assert!(rem.is_empty(), "framed blob must consume the whole slice");
    assert_eq!(value, Value::Blob(content.to_vec()));
}

/// Adversarial: a 16-byte blob that is NOT the exact fixture (last byte 0x10)
/// must ALSO round-trip verbatim. On the buggy decoder this fell through to
/// VInt parsing (leading 0x00 → length 0 → empty blob), proving the fixture
/// match was a hijack rather than correct framing.
#[test]
fn framed_blob_near_fixture_is_not_special_cased() {
    let content: [u8; 16] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
        0x10,
    ];
    let (rem, value) = parse_cql_value(&content, CqlTypeId::Blob).expect("blob must decode");
    assert!(rem.is_empty());
    assert_eq!(value, Value::Blob(content.to_vec()));
}

/// Adversarial: a blob whose first four bytes spell a plausible big-endian
/// length (0x00000002) must decode as the FULL seven bytes, never re-framed to
/// the two bytes the phantom length would select.
#[test]
fn framed_blob_with_be_length_prefix_is_not_reframed() {
    let content = [0x00u8, 0x00, 0x00, 0x02, 0xAA, 0xBB, 0xCC];
    let (rem, value) = parse_cql_value(&content, CqlTypeId::Blob).expect("blob must decode");
    assert!(rem.is_empty(), "must not leave a trailing byte");
    assert_eq!(value, Value::Blob(content.to_vec()));
}

/// An empty framed blob decodes to an empty blob.
#[test]
fn framed_blob_empty_roundtrips() {
    let (rem, value) = parse_cql_value(&[], CqlTypeId::Blob).expect("empty blob must decode");
    assert!(rem.is_empty());
    assert_eq!(value, Value::Blob(Vec::new()));
}

/// The genuinely VInt-framed path (write-side tagged serialization → skip the
/// type byte → `parse_blob`) must still round-trip. This guards that removing
/// the heuristics from the framed arm did not disturb the real length-prefixed
/// decoder.
#[test]
fn vint_framed_blob_write_side_roundtrips() {
    for content in [
        vec![],
        vec![0xAA],
        vec![0x00, 0x00, 0x00, 0x02, 0xAA, 0xBB, 0xCC],
        vec![
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
            0x0E, 0x0F,
        ],
    ] {
        let serialized =
            serialize_cql_value(&Value::Blob(content.clone())).expect("serialize must succeed");
        assert_eq!(serialized[0], CqlTypeId::Blob as u8);
        let (rem, value) = parse_blob(&serialized[1..]).expect("vint-framed blob must decode");
        assert!(rem.is_empty());
        assert_eq!(value, Value::Blob(content));
    }
}

proptest! {
    /// Framed path: for ANY byte vector the whole slice is the blob verbatim,
    /// with nothing left over — no byte-pattern ever changes the result.
    #[test]
    fn prop_framed_blob_roundtrips_verbatim(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let (rem, value) = parse_cql_value(&bytes, CqlTypeId::Blob).expect("blob must decode");
        prop_assert!(rem.is_empty());
        prop_assert_eq!(value, Value::Blob(bytes));
    }

    /// Write-side VInt-framed path: encode with `serialize_cql_value`, strip the
    /// type byte, decode with `parse_blob`, recover the exact bytes.
    #[test]
    fn prop_vint_framed_blob_roundtrips(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let mut framed = encode_vint(bytes.len() as i64);
        framed.extend_from_slice(&bytes);
        let (rem, value) = parse_blob(&framed).expect("vint-framed blob must decode");
        prop_assert!(rem.is_empty());
        prop_assert_eq!(value, Value::Blob(bytes));
    }
}
