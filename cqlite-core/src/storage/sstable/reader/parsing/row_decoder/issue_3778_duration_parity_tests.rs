//! Characterisation tests for issue #3778 — a `duration` whose declared extent
//! carries TRAILING BYTES after its three VInts is ACCEPTED, at both sites.
//!
//! # These pin DELIBERATE PARITY TOLERANCE, not a latent bug
//!
//! Everything asserted below is the behaviour CQLite is SUPPOSED to have. If a
//! future change makes one of these tests fail by adding a refusal, that change is
//! the regression — read this header before "fixing" anything here.
//!
//! ## The oracle: Cassandra does not check consumption for a duration either
//!
//! Read at the pinned tag (`git show cassandra-5.0.8:<path>`):
//!
//! * `src/java/org/apache/cassandra/serializers/DurationSerializer.java`
//!   `deserialize` reads THREE VInts (months, days, nanoseconds) off the buffer and
//!   returns. It never asks whether the buffer still has bytes left.
//! * the same file's `validate` (`:80-105`) enforces exactly three things: `size >= 3`
//!   ("Expected at least 3 bytes for a duration (%d)"), that months and days fit in
//!   32 bits, and that all three components share a sign. It enforces NO upper bound
//!   on the size.
//! * `grep -n "remaining\|hasRemaining\|limit()"` over that file plus
//!   `src/java/org/apache/cassandra/db/marshal/DurationType.java` yields ZERO hits —
//!   there is no consumption check anywhere on Cassandra's duration path.
//!
//! `TupleType.split`'s rule 4 (trailing bytes are a `MarshalException`) is the
//! authority CQLite's `require_fully_consumed` cites, and it applies at the TUPLE
//! FRAMING level — trailing bytes after the last declared COMPONENT — not inside one
//! element's own declared length. So a duration body that is longer than its three
//! VInts is data Cassandra ITSELF reads without complaint.
//!
//! ## The lead ruling: #3778, Option A — parity-correct tolerance
//!
//! #3778 considered refusing these bytes (Option B) and REFUSED that option: it
//! would convert reads Cassandra performs successfully into hard CQLite failures,
//! i.e. knowingly stricter than the format authority with no oracle supporting the
//! strictness. Option A — keep the tolerance, PIN it with these characterisation
//! tests, and DECLARE the residual — is the standing disposition.
//!
//! The scope consequence the ruling accepted explicitly, and which
//! [`two_encodings_differing_only_in_trailing_bytes_decode_equal_nested`] and its
//! cell-level twin assert: two byte strings differing ONLY in trailing bytes inside
//! the declared duration length decode to ONE, EQUAL `Value::Duration`. That is
//! deliberate, and these tests are what say so.
//!
//! ## The two tolerant sites, one NESTED and one CELL-LEVEL
//!
//! | site | framing | how the leftover bytes are handled |
//! |---|---|---|
//! | `raw_type_value.rs`'s `"duration"` arm (a duration NESTED inside a frozen composite; its errors read `Frozen element '{}'`) | `[VInt len][months][days][nanos]` | the third `parse_vint` binds `_remaining` and DISCARDS it, then `offset += duration_len` advances by the DECLARED length |
//! | `cell_value_scalar.rs`'s [`CellKind::Duration`] arm (a plain top-level CELL, NOT nested) | `[VInt len][months][days][nanos]` | leftover bytes emit one `warn!("… has {} extra bytes after parsing")` and the value is returned |
//!
//! ## The `raw_value` path is STRICTER than Cassandra, and is NOT changed here
//!
//! The third duration arm — `raw_value/reporting.rs`'s, reached through the bounded
//! `parse_value_from_raw_bytes` — reports where its third VInt actually ended, and
//! its caller's `require_fully_consumed` (`typed_value.rs:91`) then REFUSES the same
//! trailing bytes these tests accept. [`the_raw_value_path_refuses_what_these_two_sites_accept`]
//! pins that asymmetry so it is visible rather than surprising. It is stricter than
//! the oracle above; #3778 filed that as follow-up **#4038** and deliberately did
//! NOT relax it in this change. Do not "unify" the three arms by tightening these
//! two — that is Option B under another name.

use super::*;
use crate::parser::vint::encode_vuint;
use test_support::helpers::encode_unsigned;

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// The three-VInt duration BODY, with no length prefix: months, days, nanos as
/// consecutive signed (zigzag) VInts, exactly as `DurationSerializer.deserialize`
/// reads them.
fn duration_body(months: i64, days: i64, nanos: i64) -> Vec<u8> {
    let mut body = Vec::new();
    encode_unsigned(zigzag(months), &mut body);
    encode_unsigned(zigzag(days), &mut body);
    encode_unsigned(zigzag(nanos), &mut body);
    body
}

/// `[VInt len][body][trailing…]` where the length prefix counts the trailing bytes
/// too — so the bytes are INSIDE the value's declared extent, which is the whole
/// point: no outer framing rule is violated, only the three VInts stop early.
fn duration_value_with_trailing(months: i64, days: i64, nanos: i64, trailing: &[u8]) -> Vec<u8> {
    let mut body = duration_body(months, days, nanos);
    body.extend_from_slice(trailing);
    let mut out = encode_vuint(body.len() as u64);
    out.extend_from_slice(&body);
    out
}

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, None)
}

fn duration_column() -> crate::schema::Column {
    crate::schema::Column {
        name: "d".to_string(),
        data_type: "duration".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// Drive the NESTED site: `raw_type_value.rs`'s `"duration"` arm.
fn decode_nested(data: &[u8]) -> Result<(Value, usize)> {
    parser().parse_raw_type_value(data, 0, "duration", "d", 0)
}

/// Drive the CELL-LEVEL site: `cell_value_scalar.rs`'s [`CellKind::Duration`] arm.
fn decode_cell(data: &[u8]) -> Result<(Value, usize)> {
    let column = duration_column();
    let mut offset = 0usize;
    let value = V5CompressedLegacyParser::decode_scalar_cell_value(
        data,
        &mut offset,
        &CellKind::Duration,
        &column,
    )?;
    Ok((value, offset))
}

const EXPECTED: Value = Value::Duration {
    months: 1,
    days: 2,
    nanos: 3,
};

// ---------------------------------------------------------------------------
// Site 1 — NESTED (`raw_type_value.rs`, "Frozen element" errors)
// ---------------------------------------------------------------------------

/// CONTROL: the exact encoding decodes, so the tolerance cases below are not
/// passing because the whole arm is broken.
#[test]
fn nested_exact_duration_decodes_ok() {
    let bytes = duration_value_with_trailing(1, 2, 3, &[]);
    let (value, offset) = decode_nested(&bytes).expect("an exact duration decodes");
    assert_eq!(value, EXPECTED);
    assert_eq!(offset, bytes.len(), "the exact encoding consumes it all");
}

/// CHARACTERISATION (#3778 Option A): trailing bytes INSIDE the declared duration
/// length are TOLERATED at the nested site — the oracle
/// (`DurationSerializer.deserialize`, and `validate` `:80-105`, which bounds only
/// `size >= 3`) performs no consumption check either.
#[test]
fn nested_duration_with_trailing_bytes_inside_its_extent_decodes_ok() {
    let bytes = duration_value_with_trailing(1, 2, 3, &[0xAA, 0xBB, 0xCC]);
    let (value, offset) = decode_nested(&bytes)
        .expect("#3778 Option A: trailing bytes inside a duration's extent are TOLERATED");
    assert_eq!(value, EXPECTED, "the three VInts still decode");
    assert_eq!(
        offset,
        bytes.len(),
        "the arm advances by the DECLARED duration_len, so the caller's framing \
         accounting stays balanced even though the VInts stopped early"
    );
}

/// CHARACTERISATION (#3778 Option A) — the scope consequence the lead ruling
/// accepted EXPLICITLY: two byte strings differing ONLY in trailing bytes inside
/// `duration_len` decode to ONE, EQUAL `Value::Duration`. Deliberate, not a bug.
#[test]
fn two_encodings_differing_only_in_trailing_bytes_decode_equal_nested() {
    let a = duration_value_with_trailing(1, 2, 3, &[0xAA]);
    let b = duration_value_with_trailing(1, 2, 3, &[0xBB, 0xBB, 0xBB, 0xBB]);
    assert_ne!(a, b, "the two inputs really are different byte strings");

    let (va, _) = decode_nested(&a).expect("#3778: tolerated");
    let (vb, _) = decode_nested(&b).expect("#3778: tolerated");
    assert_eq!(
        va, vb,
        "#3778 Option A accepted this collapse deliberately: the trailing bytes are \
         not part of the value's identity"
    );
    assert_eq!(va, EXPECTED);
}

// ---------------------------------------------------------------------------
// Site 2 — CELL-LEVEL (`cell_value_scalar.rs`; NOT nested inside anything)
// ---------------------------------------------------------------------------

/// CONTROL, cell level.
#[test]
fn cell_exact_duration_decodes_ok() {
    let bytes = duration_value_with_trailing(1, 2, 3, &[]);
    let (value, offset) = decode_cell(&bytes).expect("an exact duration cell decodes");
    assert_eq!(value, EXPECTED);
    assert_eq!(offset, bytes.len());
}

/// CHARACTERISATION (#3778 Option A) at the CELL level — the second tolerant site
/// is a plain top-level cell, not a nested value. Leftover bytes raise one `warn!`
/// and the value is returned; the oracle tolerates them, so CQLite does too.
#[test]
fn cell_duration_with_trailing_bytes_inside_its_extent_decodes_ok() {
    let bytes = duration_value_with_trailing(1, 2, 3, &[0xAA, 0xBB, 0xCC]);
    let (value, offset) = decode_cell(&bytes)
        .expect("#3778 Option A: trailing bytes inside a duration cell are TOLERATED");
    assert_eq!(value, EXPECTED, "the three VInts still decode");
    assert_eq!(
        offset,
        bytes.len(),
        "the cell arm consumes the whole VInt-length-prefixed extent"
    );
}

/// CHARACTERISATION (#3778 Option A), cell level: the same deliberate collapse.
#[test]
fn two_encodings_differing_only_in_trailing_bytes_decode_equal_cell() {
    let a = duration_value_with_trailing(1, 2, 3, &[0xAA]);
    let b = duration_value_with_trailing(1, 2, 3, &[0xBB, 0xBB, 0xBB, 0xBB]);
    assert_ne!(a, b, "the two inputs really are different byte strings");

    let (va, _) = decode_cell(&a).expect("#3778: tolerated");
    let (vb, _) = decode_cell(&b).expect("#3778: tolerated");
    assert_eq!(
        va, vb,
        "#3778 Option A accepted this collapse deliberately at the cell level too"
    );
    assert_eq!(va, EXPECTED);
}

// ---------------------------------------------------------------------------
// The asymmetry — the THIRD duration arm refuses what these two accept
// ---------------------------------------------------------------------------

/// The `raw_value` bounded path (`parse_value_from_raw_bytes` ->
/// `raw_value/reporting.rs`'s `"duration"` arm -> `require_fully_consumed` at
/// `typed_value.rs:91`) REFUSES the very trailing bytes the two sites above accept.
///
/// Pinned so the divergence is VISIBLE rather than surprising. That path is
/// STRICTER than the oracle (Cassandra's duration serializer performs no
/// consumption check at all) — #3778 filed it as follow-up #4038 and
/// deliberately did not relax it here. Note the framing difference: on this path
/// the slice handed in IS the value, so there is no `[VInt len]` prefix.
#[test]
fn the_raw_value_path_refuses_what_these_two_sites_accept() {
    let mut body = duration_body(1, 2, 3);
    let exact = body.len();
    body.extend_from_slice(&[0xAA, 0xBB, 0xCC]);

    let result = parser().parse_value_from_raw_bytes(&body, "duration", "d", 0);
    let err = result.expect_err(
        "the bounded raw_value path refuses trailing bytes (stricter than Cassandra; \
         follow-up #4038)",
    );
    let msg = err.to_string();
    assert!(
        msg.contains(&format!("decoded only {exact} of {} byte(s)", body.len())),
        "expected the shared bounded-consumption refusal, got: {msg}"
    );
}
