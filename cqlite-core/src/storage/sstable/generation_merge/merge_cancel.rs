//! Per-CALL cancellation for the detached `spawn_blocking` merges (issue #1695).
//!
//! # Why future-drop is not enough here
//!
//! The query budget (`query.max_execution_time`) is enforced by ONE
//! `tokio::time::timeout` at the engine chokepoint (`query::engine::deadline`),
//! and its whole cancellation mechanism is DROPPING the inner future. That is
//! correct for async cooperative code — a dropped future stops being polled — but
//! tokio CANNOT cancel a `spawn_blocking` closure by dropping its `JoinHandle`:
//! the closure runs to completion on its blocking thread regardless.
//!
//! The materializing cross-generation merges build a `Vec` inside such a closure
//! with no intermediate channel send that could fail, so a dropped handle left the
//! ENTIRE multi-generation merge running after `Error::QueryTimeout` had already
//! been returned to the caller — the timed-out query kept burning a blocking
//! thread (and its scan-admission permit) to produce rows nobody would read.
//! (The STREAMING driver has no such gap: its `blocking_send` fails as soon as the
//! consumer is gone, which ends its loop within one merge step.)
//!
//! # The shape: a guard in the async scope, a flag in the closure
//!
//! [`per_call`] mints a FRESH token per call and returns it paired with a guard
//! that [`ScanCancel::cancel`]s it on `Drop`. The guard lives in the async fn's
//! own scope, so whatever destroys that future — a timeout elapse, a client
//! disconnect, a caller's `drop` — trips the flag, and the blocking merge loop
//! observes it at its next per-partition check and abandons the merge.
//!
//! It is per-call BY CONSTRUCTION, which is the load-bearing property: the token
//! on a shared [`crate::storage::sstable::reader::SSTableReader`]
//! (`SSTableReader::scan_cancel`) is reachable from every query using that reader,
//! so tripping THAT one would cancel other queries' in-flight scans. Nothing but a
//! freshly-minted token is safe here.
//!
//! No deadline knowledge crosses this boundary: the merge never reads a clock and
//! never learns the budget (issue #1695's mandate is one wrapper at the
//! chokepoint, never ad-hoc clock checks in a scan loop). It only makes the
//! blocking walk ABANDONABLE, at the same coarse per-partition cadence at which
//! the async paths are already cancellable.

use crate::storage::scan_cancel::ScanCancel;

/// Trips its token when dropped. Held in the async scope of a merge helper; the
/// clone of the token it cancels lives inside that helper's blocking closure.
pub(super) struct CancelOnDrop(ScanCancel);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// A fresh per-call token plus the guard that cancels it when the calling future
/// is dropped. Bind the guard (`let (_guard, cancel) = per_call();`) so it lives
/// as long as the async fn's scope, and move `cancel` into the blocking closure.
pub(super) fn per_call() -> (CancelOnDrop, ScanCancel) {
    let token = ScanCancel::new();
    (CancelOnDrop(token.clone()), token)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The guard trips its token on drop — the mechanism the timed-out query
    /// relies on — and does not trip it before.
    #[test]
    fn guard_cancels_its_token_on_drop() {
        let (guard, cancel) = per_call();
        assert!(!cancel.is_cancelled(), "must start un-cancelled");
        drop(guard);
        assert!(
            cancel.is_cancelled(),
            "dropping the async-scope guard must trip the blocking closure's flag"
        );
    }

    /// Each call mints an INDEPENDENT token, so one query's cancellation can never
    /// reach another's in-flight merge (the reason a shared reader token must not
    /// be reused here).
    #[test]
    fn tokens_are_independent_per_call() {
        let (guard_a, cancel_a) = per_call();
        let (_guard_b, cancel_b) = per_call();
        drop(guard_a);
        assert!(cancel_a.is_cancelled());
        assert!(
            !cancel_b.is_cancelled(),
            "a per-call token must be private to its own call"
        );
    }
}
