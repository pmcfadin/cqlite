//! Panic/early-exit drop-guard for the windowed streaming scan's raw-chunk feed
//! loops (roborev finding, issue #1593; see also issue #1143 finding 2).
//!
//! Kept in a sibling file so the parent `scan_stream_windowed` driver stays under
//! the campsite-rule size limit (epic #1116). Included via `#[path = ...] mod
//! guard;` + a private re-export, so `use super::*` in the parent's test module
//! resolves [`FeedFailureGuard`] directly.

use std::sync::atomic::{AtomicBool, Ordering};

/// Drop-guard that flips `io_failed` to `true` on ANY early exit — including a
/// PANIC/unwind — of a raw-chunk feed loop, UNLESS explicitly
/// [`disarm`](Self::disarm)ed on the clean-EOF path.
///
/// # Why this exists
///
/// A feed loop MOVES `raw_tx` in and drops it on return. The blocking parse half
/// treats a `raw_tx` close with `io_failed == false` as a CLEAN EOF and runs its
/// terminal (`at_final_chunk = true`) drain, which parses the trailing window AS
/// IF it were a complete final partition. The normal read-error path sets
/// `io_failed = true` before returning, so that is handled. But if the feed
/// closure PANICS, `raw_tx` is dropped during unwind and the only other place
/// that sets `io_failed = true` — the `Err(join_err)` join arm — runs LATER, only
/// after the (already-closed) task is joined. The parse half would then observe a
/// spurious CLEAN EOF, run the terminal drain, and emit a TRUNCATED trailing
/// partition as a row before the scan surfaces its `Err`.
///
/// Arming this guard as a BODY-LOCAL of the feed closure fixes the ordering: body
/// locals are dropped BEFORE a `move` closure's captured environment, so on a
/// panic the guard's `Drop` flips `io_failed = true` BEFORE the captured `raw_tx`
/// drops. The parse half's `io_failed.load` (sequenced after the `raw_tx`-close
/// happens-before) therefore observes the failure and correctly SKIPS the terminal
/// drain — no spurious trailing partition.
///
/// The clean-EOF path (and the consumer-ended-early break, which is likewise not
/// an I/O failure) reaches [`disarm`](Self::disarm), so a normal finish leaves
/// `io_failed == false` and the terminal drain runs EXACTLY as before — the happy
/// path is byte-identical. `Drop` uses only `AtomicBool::store`, which never
/// panics, honoring the no-panic-in-`Drop` rule.
pub(super) struct FeedFailureGuard<'a> {
    io_failed: &'a AtomicBool,
    armed: bool,
}

impl<'a> FeedFailureGuard<'a> {
    /// Arm a guard over `io_failed` (starts armed; fires on drop unless disarmed).
    pub(super) fn armed(io_failed: &'a AtomicBool) -> Self {
        Self {
            io_failed,
            armed: true,
        }
    }

    /// Disarm on the clean-EOF (or consumer-ended-early) path so a normal finish
    /// leaves `io_failed == false`.
    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for FeedFailureGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            // SeqCst matches the explicit stores in the feed loops so the parse
            // half's `io_failed.load` observes it via the raw_tx-close
            // happens-before. `store` never panics (no-panic-in-Drop).
            self.io_failed.store(true, Ordering::SeqCst);
        }
    }
}
