//! Regression tests for issue #1795 — usize add-overflow in the V5CompressedLegacy
//! per-cell bounds guards.
//!
//! The decimal (and sibling text/blob/counter/duration) value arms decoded an
//! UNCAPPED `parse_vuint` length into `usize` and then evaluated
//! `offset + total_len > data.len()` as a bounds check. On an adversarial length
//! prefix the ADD overflowed `usize` (in an overflow-checked build: a panic
//! `attempt to add with overflow`; in release: a wraparound that defeated the
//! guard) BEFORE the guard could reject the input. Found by the #1614 parser fuzz
//! crate (`fuzz_block_emit`).
//!
//! These tests craft the adversarial framing directly (dataset-independent) and
//! assert every affected arm now returns `Err` cleanly and NEVER panics. Debug/test
//! builds run with `overflow-checks = true`, so a regressed add would abort the
//! process here rather than silently wrap.

use super::decoder_lockstep_tests::{open_reader, test_column, v5_parser};
use crate::parser::vint::encode_vuint;

/// Build a live-cell body (`flags = 0x08` = USE_ROW_TIMESTAMP, has value) whose
/// value length prefix is a maximal unsigned VInt — the exact shape that overflowed
/// `offset + len` before the fix.
fn adversarial_length_prefixed_cell() -> Vec<u8> {
    let mut cell = vec![0x08u8];
    // u64::MAX as a length: `offset + (u64::MAX as usize)` overflows usize.
    cell.extend_from_slice(&encode_vuint(u64::MAX));
    // A handful of trailing bytes so the value read (if the guard were absent)
    // would have somewhere to start.
    cell.extend_from_slice(&[0u8; 8]);
    cell
}

/// The reported panic: a `decimal` cell with an adversarial length prefix must
/// return `Err`, not overflow-panic.
#[tokio::test]
async fn decimal_adversarial_length_returns_err_not_panic() {
    let Some(reader) = open_reader().await else {
        return;
    };
    let parser = v5_parser();
    let col = test_column("decimal");
    let cell = adversarial_length_prefixed_cell();
    let res = parser.parse_cell_value_schema_order(&cell, 0, &col, None, None, &reader);
    assert!(
        res.is_err(),
        "adversarial decimal length must return Err (got {res:?})"
    );
}

/// The same overflow shape recurs across the other VInt-length-prefixed arms; each
/// must reject cleanly (hardening sweep, issue #1795).
#[tokio::test]
async fn sibling_vint_length_arms_reject_adversarial_length() {
    let Some(reader) = open_reader().await else {
        return;
    };
    let parser = v5_parser();
    for cql_type in ["text", "blob", "counter", "duration", "varint"] {
        let col = test_column(cql_type);
        let cell = adversarial_length_prefixed_cell();
        let res = parser.parse_cell_value_schema_order(&cell, 0, &col, None, None, &reader);
        assert!(
            res.is_err(),
            "adversarial {cql_type} length must return Err (got {res:?})"
        );
    }
}
