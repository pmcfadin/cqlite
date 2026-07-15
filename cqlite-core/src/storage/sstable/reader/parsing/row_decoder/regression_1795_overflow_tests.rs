//! Regression tests for issue #1795 — usize add-overflow in the V5CompressedLegacy
//! per-cell bounds guards.
//!
//! The decimal (and sibling text/blob/counter/duration/varint) value arms decoded
//! an UNCAPPED `parse_vuint` length into `usize` and then evaluated
//! `offset + total_len > data.len()` as a bounds check. On an adversarial length
//! prefix the ADD overflowed `usize` (in an overflow-checked build: a panic
//! `attempt to add with overflow`; in release: a wraparound that defeated the
//! guard) BEFORE the guard could reject the input. Found by the #1614 parser fuzz
//! crate (`fuzz_block_emit`).
//!
//! ## Truly dataset-independent coverage (issue #1795 roborev)
//!
//! The shared VInt-length guard now lives in
//! [`V5CompressedLegacyParser::read_vint_length_prefixed_bytes`] — a free
//! associated function needing NO `SSTableReader`, no dataset, and no feature
//! flag. The core regression assertions below call it DIRECTLY, so they exercise
//! the overflow-safe guard **unconditionally in every build/lane**, regardless of
//! whether `CQLITE_DATASETS_ROOT` is populated. (Previously these tests took an
//! `open_reader().await` handle that the adversarial path never dereferences — the
//! guard rejects the bad length first — so when the dataset was absent they
//! silently returned, passing vacuously without ever running the guard.)
//!
//! The `counter` arm keeps a hand-inlined copy of the same guard inside
//! `parse_cell_value_schema_order` (it must peek the context bytes before choosing
//! the CounterContext vs raw-i64 interpretation), so it is exercised through the
//! full per-cell dispatch. That full-dispatch test is `write-support`-gated because
//! obtaining an `SSTableReader` requires the `SSTableWriter` to synthesize a
//! dataset-free fixture; in the `write-support` build (the default, and what the
//! `--lite` gate runs) that synthetic reader is ALWAYS constructible, so the test
//! runs with no dataset and — being `.expect()`, not a silent `return` — can never
//! no-op vacuously. In a non-`write-support` build it is compiled out entirely
//! (never a runtime skip); the always-on direct-helper tests still cover the
//! identical guard logic there.
//!
//! Debug/test builds run with `overflow-checks = true`, so a regressed add would
//! abort the process here rather than silently wrap.

use super::V5CompressedLegacyParser;
use crate::parser::vint::encode_vuint;
use crate::schema::Column;

/// A single-column schema `Column` for the given CQL type (mirrors the lockstep
/// helper; kept local so these tests carry no reader/dataset dependency).
fn column(cql_type: &str) -> Column {
    Column {
        name: "c".to_string(),
        data_type: cql_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// The exact adversarial framing the fuzzer found: a maximal unsigned-VInt length
/// prefix (`u64::MAX`) followed by a handful of trailing bytes. `offset + len`
/// overflows `usize`; the overflow-safe guard must return `Err` instead. This is
/// the value-framing WITHOUT the outer cell flags byte, so it can be fed straight
/// to `read_vint_length_prefixed_bytes` (which starts at the length prefix).
fn adversarial_length_prefix() -> Vec<u8> {
    let mut framing = encode_vuint(u64::MAX);
    framing.extend_from_slice(&[0u8; 8]);
    framing
}

/// The reported panic: decoding a `decimal` value length prefix of `u64::MAX` must
/// return `Err`, not overflow-panic. Calls the shared guard directly, so it runs
/// in EVERY build/lane with no dataset dependency.
#[test]
fn decimal_adversarial_length_returns_err_not_panic() {
    let data = adversarial_length_prefix();
    let mut offset = 0usize;
    let col = column("decimal");
    let res = V5CompressedLegacyParser::read_vint_length_prefixed_bytes(
        &data,
        &mut offset,
        &col,
        "decimal",
    );
    assert!(
        res.is_err(),
        "adversarial decimal length must return Err (got {res:?})"
    );
}

/// The same overflow shape recurs across the other VInt-length-prefixed arms that
/// share the guard (`text`/`blob`/`duration`/`varint`); each must reject cleanly.
/// Dataset-independent (direct guard call).
#[test]
fn sibling_vint_length_arms_reject_adversarial_length() {
    for what in ["text", "blob", "duration", "varint"] {
        let data = adversarial_length_prefix();
        let mut offset = 0usize;
        let col = column(what);
        let res = V5CompressedLegacyParser::read_vint_length_prefixed_bytes(
            &data,
            &mut offset,
            &col,
            what,
        );
        assert!(
            res.is_err(),
            "adversarial {what} length must return Err (got {res:?})"
        );
    }
}

/// A legitimate length prefix within `MAX_CELL_VALUE_LENGTH` with enough
/// trailing bytes still decodes cleanly — the guard rejects nothing valid. Pins
/// that the #1795 cap did not regress the happy path.
#[test]
fn in_bounds_length_prefix_still_decodes() {
    let payload = b"hello";
    let mut data = encode_vuint(payload.len() as u64);
    data.extend_from_slice(payload);
    let mut offset = 0usize;
    let col = column("text");
    let bytes =
        V5CompressedLegacyParser::read_vint_length_prefixed_bytes(&data, &mut offset, &col, "text")
            .expect("an in-bounds length prefix must decode");
    assert_eq!(bytes, payload);
    assert_eq!(offset, data.len(), "offset must advance past the value");
}

/// Full per-cell dispatch (`parse_cell_value_schema_order`) rejects the adversarial
/// length across ALL VInt-length arms — INCLUDING `counter`, whose guard is
/// inlined in the dispatch method (not the shared helper) so it is only reachable
/// end-to-end. `write-support`-gated: the synthetic reader is dataset-free and
/// ALWAYS constructible here (default + `--lite`), so this runs unconditionally
/// and can never skip vacuously. Compiled out (not skipped) without `write-support`.
#[cfg(feature = "write-support")]
#[tokio::test]
async fn adversarial_lengths_rejected_via_full_cell_dispatch() {
    use super::decoder_lockstep_tests::{open_reader, test_column, v5_parser};

    let reader = open_reader()
        .await
        .expect("write-support build synthesizes a dataset-free reader");
    let parser = v5_parser();

    for cql_type in ["decimal", "text", "blob", "counter", "duration", "varint"] {
        let col = test_column(cql_type);
        // Full on-disk cell body: flags 0x08 (live, has value) + adversarial length.
        let mut cell = vec![0x08u8];
        cell.extend_from_slice(&adversarial_length_prefix());
        let res = parser.parse_cell_value_schema_order(&cell, 0, &col, None, None, &reader);
        assert!(
            res.is_err(),
            "adversarial {cql_type} length must return Err via full dispatch (got {res:?})"
        );
    }
}
