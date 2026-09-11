//! Unit tests for `finalize_loss_chunks`/`MAX_CHUNKS_PER_LOSS` in
//! `recover.rs`. Split out (round 22, campsite rule / epic #1116) when
//! `recover.rs` crossed the ~800-line source threshold — a PURE MOVE, no
//! behavior changed. `recover.rs` is a file THIS PR created (issue #4196),
//! so the `CQLITE_ALLOW_FILE_GROWTH=1` opt-out — reserved for pre-existing
//! files — does not apply to it; split rather than carried under an
//! opt-out. Wired via `#[path = "recover_chunks_cap_tests.rs"] mod
//! chunks_cap_tests;` inside `recover.rs` so `super::` here still resolves
//! to `recover.rs`'s own scope, matching the flat-sibling-file convention
//! `chunks.rs`/`chunks_tests.rs` already established.

use super::{finalize_loss_chunks, MAX_CHUNKS_PER_LOSS};

/// Roborev, issue #4196, round 17 Medium finding: at or under the cap,
/// `chunks` and `message` must both pass through UNCHANGED — no
/// truncation note when nothing was truncated.
#[test]
fn at_or_under_the_cap_is_unchanged() {
    let chunks: Vec<u64> = (0..MAX_CHUNKS_PER_LOSS as u64).collect();
    let (out_chunks, out_message) =
        finalize_loss_chunks(chunks.clone(), "original message".to_string());
    assert_eq!(out_chunks, chunks);
    assert_eq!(out_message, "original message");

    // One under the cap too.
    let fewer: Vec<u64> = (0..(MAX_CHUNKS_PER_LOSS as u64 - 1)).collect();
    let (out_chunks, out_message) =
        finalize_loss_chunks(fewer.clone(), "original message".to_string());
    assert_eq!(out_chunks, fewer);
    assert_eq!(out_message, "original message");
}

/// Over the cap: truncated to exactly `MAX_CHUNKS_PER_LOSS` entries
/// (the FIRST ones, order preserved), and the truncation is folded into
/// `message` — never silent, per the finding's own "cap/truncate-with-
/// a-count" wording.
#[test]
fn over_the_cap_truncates_and_notes_it_in_the_message() {
    let total = MAX_CHUNKS_PER_LOSS + 500;
    let chunks: Vec<u64> = (0..total as u64).collect();
    let (out_chunks, out_message) =
        finalize_loss_chunks(chunks.clone(), "original message".to_string());
    assert_eq!(
        out_chunks.len(),
        MAX_CHUNKS_PER_LOSS,
        "must truncate to exactly the cap, not silently keep growing"
    );
    assert_eq!(
        out_chunks,
        &chunks[..MAX_CHUNKS_PER_LOSS],
        "the RETAINED entries must be the first N, in order — not an arbitrary subset"
    );
    assert!(
        out_message.starts_with("original message"),
        "the original message must still be present, not replaced"
    );
    assert!(
        out_message.contains(&MAX_CHUNKS_PER_LOSS.to_string())
            && out_message.contains(&total.to_string()),
        "the truncation note must name both the cap and the true total, so an operator \
         reading `Loss.chunks` is never silently told fewer intersecting chunks exist than \
         really do; got: {out_message}"
    );
}

/// Every real caller passes a NON-EMPTY `touched_chunks` (it only calls
/// `build_loss` inside branches gated on chunk data being available at
/// all), but the function itself must not special-case zero either.
#[test]
fn empty_chunks_is_a_no_op() {
    let (out_chunks, out_message) =
        finalize_loss_chunks(Vec::new(), "original message".to_string());
    assert!(out_chunks.is_empty());
    assert_eq!(out_message, "original message");
}
