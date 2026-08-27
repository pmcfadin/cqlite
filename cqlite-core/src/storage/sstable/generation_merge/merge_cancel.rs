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
//! # Two granularities, and why the token also goes INTO the merger
//!
//! [`check`] is called once per partition, which is the merge's own unit of work
//! — but a single `KWayMerger::step()` can itself be long, because the merger's
//! producer threads are doing the actual SSTable decode. So the token is ALSO
//! handed to the merger's input readers via `KWayMerger::new_cancellable` /
//! `build_single_partition_merger_from_readers` (#2264's cooperative reader
//! cancellation), letting an abandoned merge stop MID-step instead of finishing
//! the partition it is decoding. This costs nothing and removes nothing: the
//! tokens it displaces were `ScanCancel::default()` (what plain `KWayMerger::new`
//! installs) and one literal `ScanCancel::new()` — neither reachable by any
//! caller, so neither could ever be tripped. It must stay the PER-CALL token for
//! the same reason as below; the shared reader token would cancel other queries.
//! Producer TEARDOWN was already safe without this (#2361: dropping a
//! `KWayMerger` closes the channel, trips the readers' token and joins the
//! threads) — this is about promptness, not leaks.
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
    probe::record_armed();
    let token = ScanCancel::new();
    (CancelOnDrop(token.clone()), token)
}

/// The blocking merge loop's per-partition poll: `Err(Error::Cancelled)` once the
/// caller's future is gone, else `Ok(())`.
///
/// Call it at the TOP of the loop, BEFORE `step()`, so an abandoned merge does no
/// further work at all — that ordering is what makes "abandoned" mean "zero
/// partitions merged since the flag was tripped", which is what the probe's
/// [`probe::abandoned`] count lets a test assert without any timing.
pub(super) fn check(cancel: &ScanCancel) -> crate::Result<()> {
    if cancel.is_cancelled() {
        probe::record_abandoned();
        return Err(crate::Error::Cancelled);
    }
    Ok(())
}

/// Observability for the abandonment mechanism, on the `stream_merge_probe`
/// pattern: the RECORD calls are unconditional `#[inline(always)]` functions whose
/// BODIES are cfg-gated, so a default/release build links no atomic and pays
/// nothing. Without them "the merge abandoned instead of running to completion" is
/// unobservable from outside, and any test of it would have to time something.
///
/// Gated on `cfg(test)` ALONE — deliberately narrower than `stream_merge_probe`'s
/// `any(test, feature = "work-counters")`. The only consumer is the in-crate
/// [`super::abandon_tests`], which needs a `max_blocking_threads(1)` runtime it
/// builds itself, so nothing in `tests/` can use these; adding the feature arm
/// would only make the getters dead code in a `--all-features` build. A future
/// integration consumer adds `feature = "work-counters"` back to all of the cfgs
/// below, together. The READERS carry `not(feature = "tombstones")` as well,
/// because their one caller does: that build has no `multi_gen_fixture` (its
/// `scan_stream` routes through the materializing `scan`), so under
/// `--all-features` the getters would be dead code.
pub(super) mod probe {
    #[cfg(test)]
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Merges that ARMED a per-call guard (i.e. reached the spawn point).
    #[cfg(test)]
    static ARMED: AtomicU64 = AtomicU64::new(0);
    /// Merge loops that exited via the cancel check rather than completing.
    #[cfg(test)]
    static ABANDONED: AtomicU64 = AtomicU64::new(0);

    #[inline(always)]
    pub(super) fn record_armed() {
        #[cfg(test)]
        ARMED.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(super) fn record_abandoned() {
        #[cfg(test)]
        ABANDONED.fetch_add(1, Ordering::Relaxed);
    }

    /// Merges that armed a guard since the last [`reset`].
    #[cfg(all(test, not(feature = "tombstones")))]
    pub(super) fn armed() -> u64 {
        ARMED.load(Ordering::Relaxed)
    }

    /// Merge loops ABANDONED since the last [`reset`] — each one is a blocking
    /// merge that stopped instead of building a `Vec` nobody could receive.
    #[cfg(all(test, not(feature = "tombstones")))]
    pub(super) fn abandoned() -> u64 {
        ABANDONED.load(Ordering::Relaxed)
    }

    /// Zero both counters. The statics are process-global, so callers serialize on
    /// the shared test mutex (the counter-test convention).
    #[cfg(all(test, not(feature = "tombstones")))]
    pub(super) fn reset() {
        ARMED.store(0, Ordering::Relaxed);
        ABANDONED.store(0, Ordering::Relaxed);
    }
}

#[cfg(all(test, not(feature = "tombstones")))]
mod abandon_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// Structural pin (#1695 roborev round 2): the three MATERIALIZING merges must
    /// hand their per-call token to the merger's own input readers, not the inert
    /// default. Behavioural coverage cannot reach this — "a reader stops mid-step"
    /// is only observable as a latency difference, and a wall-clock assert in the
    /// correctness path is forbidden — so the invariant is asserted on the source.
    ///
    /// It is deliberately NOT "no `KWayMerger::new` anywhere": the STREAMING driver
    /// (`stream_generations_for_read`) keeps the plain constructor on purpose, since
    /// its `blocking_send` already fails the instant the consumer is dropped.
    #[test]
    fn the_materializing_merges_pass_their_token_into_the_merger() {
        let src = include_str!("../generation_merge.rs");

        assert_eq!(
            src.matches("KWayMerger::new_cancellable(").count(),
            2,
            "`merge_generations_for_read` and `merge_generations_for_read_with_metadata` \
             must each build a CANCELLABLE merger; a plain `KWayMerger::new` there installs \
             `ScanCancel::default()`, which no caller can ever trip"
        );

        // AFFIRMATIVE, not "no inert constructor survives": an absence assert over
        // source is defeated by any COMMENT that names the thing it forbids (the
        // first draft of this test was, by a comment two lines from the call site).
        // Counting the tokens actually handed out cannot be satisfied by prose.
        assert_eq!(
            src.matches("cancel.clone()").count(),
            3,
            "each of the 3 per-call tokens must be handed to a merger — the two \
             cancellable `KWayMerger`s and `build_single_partition_merger_from_readers`, \
             whose token was a discarded `ScanCancel` that no caller could ever trip"
        );

        assert_eq!(
            src.matches("merge_cancel::per_call()").count(),
            3,
            "one fresh token per materializing merge — a token shared across calls \
             would let one query's timeout cancel another query's merge"
        );
    }

    /// The guard trips its token on drop — the mechanism the timed-out query
    /// relies on — and does not trip it before.
    // #[serial] with the abandonment test (roborev round 4): `per_call()` arms the
    // probe and `check()` on a cancelled token records an abandonment, so this test
    // increments the SAME process-global counters `abandon_tests` resets and then
    // asserts on. `#[serial]` does not exclude UNANNOTATED tests, so without this
    // annotation a concurrent run of this test could satisfy that test's
    // `armed() > 0` anti-vacuity guard AND its `abandoned() >= 1` assertion while
    // the merge under test did neither — a vacuous pass in the one test whose whole
    // job is proving the abandonment really happened.
    #[test]
    #[serial_test::serial]
    fn guard_cancels_its_token_on_drop() {
        let (guard, cancel) = per_call();
        assert!(
            check(&cancel).is_ok(),
            "an un-cancelled token must not stop a merge"
        );
        assert!(!cancel.is_cancelled(), "must start un-cancelled");
        drop(guard);
        assert!(
            cancel.is_cancelled(),
            "dropping the async-scope guard must trip the blocking closure's flag"
        );
        assert!(
            matches!(check(&cancel), Err(crate::Error::Cancelled)),
            "the merge loop's poll must abandon once the guard has dropped"
        );
    }

    /// Each call mints an INDEPENDENT token, so one query's cancellation can never
    /// reach another's in-flight merge (the reason a shared reader token must not
    /// be reused here).
    // #[serial] for the same reason as above: `per_call()` arms the probe.
    #[test]
    #[serial_test::serial]
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
