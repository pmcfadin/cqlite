//! Thread-local scoping for [`seek_calls`](super::seek_calls) /
//! [`index_probes`](super::index_probes) delta assertions (issue #2470), mirroring
//! [`work_counters::stream_walk_scope`](crate::storage::sstable::work_counters)
//! (issue #2428).
//!
//! Kept in a sibling file (campsite rule, epic #1116): `read_work_counters.rs` is
//! already over the ~800-line source target, so this test-only scope lives here
//! rather than growing that file further.
//!
//! # Why this exists
//!
//! [`seek_calls`](super::seek_calls) and [`index_probes`](super::index_probes) are
//! process-global `AtomicU64`s bumped by EVERY read-path seek / BIG `Index.db` probe
//! across the whole crate. A test that `reset()`s one, drives a scan, then reads it
//! back measures a delta contaminated the moment ANY other test in the same `--lib`
//! binary drives a read concurrently between the reset and the read — under
//! thread-parallel `cargo test --lib` (the CI Required-PR-Gate invocation, which does
//! NOT isolate tests per-process like nextest) the observed value jumps to an
//! arbitrary inflated number. That is the issue-#2470 flake:
//! `windowed_stream_read_pattern_is_sequential` observed `seek_calls() == 144`
//! against its `< 125` bound. `#[serial(work_counters)]` only serialises tests that
//! BOTH carry the tag, so a single untagged read-driving test ANYWHERE in the crate
//! reintroduces the flake — a fragile, easy-to-miss invariant across a large and
//! growing test set (the same reasoning that motivated the #2428
//! [`stream_walk_scope`](crate::storage::sstable::work_counters)).
//!
//! # The structural fix
//!
//! cargo runs each `#[test]` on its own OS thread, and a default (current-thread)
//! `#[tokio::test]` drives all of its `.await`s on that one thread. The uncompressed
//! non-stitching full-index streaming walk records its window-refill seek INLINE on
//! that thread (`data_access/full_index_stream.rs` — the seek this scope's client
//! asserts on), so a thread-local scope activated for the duration of one test's scan
//! records ONLY the increments that execute on that test's own thread — structurally
//! immune to any concurrent test on another thread mutating the process-global. A
//! delta assertion opens a [`ReadWorkScope`] before its scan and reads
//! [`ReadWorkScope::seeks`] / [`ReadWorkScope::index_probes`] after; no `reset()`, no
//! global read, no serial tag, and no way for a future read-driving test to
//! contaminate it.
//!
//! # Boundaries
//!
//! The scope only sees increments on THE THREAD that opened it. A read path that fans
//! I/O onto a `spawn_blocking` thread (the cursor windowed feed in
//! `reader/scan_stream_windowed_read.rs`, the compaction stitching path) bumps the
//! global on that thread WITHOUT touching the scope, so a test measuring THAT path
//! keeps using the global getter. The uncompressed non-stitching walk this scope
//! serves records its seek inline, so it is captured. This module is `#[cfg(test)]`:
//! it exists only in the library's own test build (the binary where the contamination
//! occurs); integration tests in `tests/` compile the library without its `test` cfg
//! and never see it.

use std::cell::Cell;

thread_local! {
    /// `Some(count)` while a [`ReadWorkScope`] is active on this thread, `None`
    /// otherwise. Only [`record_seek`] calls that execute on this thread bump it.
    static SEEKS: Cell<Option<u64>> = const { Cell::new(None) };
    /// `Some(count)` while a [`ReadWorkScope`] is active on this thread, `None`
    /// otherwise. Only [`record_index_probe`] calls that execute on this thread
    /// bump it.
    static INDEX_PROBES: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Bump the active scope's seek count on the current thread, if any. A no-op on
/// threads (production reads, `spawn_blocking` feeds, other tests) with no active
/// scope.
pub(crate) fn record_seek() {
    SEEKS.with(|c| {
        if let Some(v) = c.get() {
            c.set(Some(v.saturating_add(1)));
        }
    });
}

/// Bump the active scope's index-probe count on the current thread, if any.
pub(crate) fn record_index_probe() {
    INDEX_PROBES.with(|c| {
        if let Some(v) = c.get() {
            c.set(Some(v.saturating_add(1)));
        }
    });
}

/// A per-thread recording scope for [`seek_calls`](super::seek_calls) /
/// [`index_probes`](super::index_probes). Open one before an inline scan whose seek
/// / index-probe count you assert, and read [`seeks`](Self::seeks) /
/// [`index_probes`](Self::index_probes) after. Immune to concurrent tests on other
/// threads (issue #2470). Dropping it clears the scope.
///
/// Deliberately `!Send` (holds a `PhantomData<*const ()>`): the scope is meaningful
/// only on the thread that opened it, so the type system forbids moving it to another
/// thread where its counts would be wrong.
pub(crate) struct ReadWorkScope {
    _not_send: std::marker::PhantomData<*const ()>,
}

impl ReadWorkScope {
    /// Begin recording on the current thread. Panics if a scope is already active on
    /// this thread (one scope per assertion; nesting unsupported).
    pub(crate) fn new() -> Self {
        SEEKS.with(|c| {
            assert!(
                c.get().is_none(),
                "a ReadWorkScope is already active on this thread (nesting unsupported)"
            );
            c.set(Some(0));
        });
        INDEX_PROBES.with(|c| c.set(Some(0)));
        Self {
            _not_send: std::marker::PhantomData,
        }
    }

    /// Read-path seek increments recorded on this thread since the scope opened.
    pub(crate) fn seeks(&self) -> u64 {
        SEEKS.with(|c| c.get().unwrap_or(0))
    }

    /// BIG `Index.db` probe increments recorded on this thread since the scope
    /// opened.
    pub(crate) fn index_probes(&self) -> u64 {
        INDEX_PROBES.with(|c| c.get().unwrap_or(0))
    }
}

impl Drop for ReadWorkScope {
    fn drop(&mut self) {
        SEEKS.with(|c| c.set(None));
        INDEX_PROBES.with(|c| c.set(None));
    }
}

#[cfg(test)]
mod tests {
    use super::ReadWorkScope;
    use crate::storage::sstable::read_work_counters::{record_index_probe, record_seek};
    use std::sync::mpsc;

    /// Structural regression for issue #2470: a [`ReadWorkScope`] records ONLY the
    /// `record_seek`/`record_index_probe` increments that execute on its own thread,
    /// so a *concurrent* thread bumping the same process-global counters cannot
    /// contaminate a same-thread delta assertion.
    ///
    /// This is the mechanism that made `windowed_stream_read_pattern_is_sequential`
    /// flake under thread-parallel `cargo test --lib` (the CI Required-PR-Gate
    /// invocation, which does NOT isolate tests per-process like nextest): another
    /// read-driving test running between that test's `reset()` and its read inflated
    /// the observed global delta to `seek_calls() == 144` against a `< 125` bound.
    /// Here we reproduce that exact shape — a foreign thread hammering the globals
    /// while a scope is open — and prove the scoped counts stay exactly the number of
    /// increments made on THIS thread.
    #[test]
    fn read_work_scope_is_immune_to_a_concurrent_thread() {
        const LOCAL_SEEKS: u64 = 3;
        const LOCAL_PROBES: u64 = 2;
        const FOREIGN: u64 = 500; // the contaminating "other test" load

        let work = ReadWorkScope::new();

        // A foreign thread bumps the SAME process-global counters (its own thread has
        // no active scope, so the scope-record is a no-op there). Handshakes force its
        // increments to interleave DURING this thread's scope, exactly like a
        // concurrent read-driving test.
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (proceed_tx, proceed_rx) = mpsc::channel::<()>();
        let foreign = std::thread::spawn(move || {
            started_tx.send(()).expect("send started");
            proceed_rx.recv().expect("recv proceed");
            for _ in 0..FOREIGN {
                record_seek();
                record_index_probe();
            }
        });

        started_rx.recv().expect("foreign thread must start");
        // Bump on THIS thread before, ...
        record_seek();
        record_index_probe();
        // ... let the foreign thread run its whole contaminating load, ...
        proceed_tx.send(()).expect("release foreign thread");
        foreign.join().expect("foreign thread must not panic");
        // ... and after. The foreign 500+500 landed squarely between our increments.
        record_seek();
        record_seek();
        record_index_probe();

        assert_eq!(
            work.seeks(),
            LOCAL_SEEKS,
            "the scope must count only this thread's {LOCAL_SEEKS} seeks, never the \
             foreign thread's {FOREIGN} on the shared global (issue #2470)"
        );
        assert_eq!(
            work.index_probes(),
            LOCAL_PROBES,
            "the scope must count only this thread's {LOCAL_PROBES} index probes, never \
             the foreign thread's {FOREIGN} on the shared global (issue #2470)"
        );

        drop(work);
        // After drop the scope is cleared: a fresh scope starts at zero and a bare
        // increment (no active scope) records nowhere.
        let fresh = ReadWorkScope::new();
        assert_eq!(
            fresh.seeks(),
            0,
            "a fresh scope starts at zero (the previous scope's count did not leak)"
        );
        assert_eq!(fresh.index_probes(), 0, "a fresh scope starts at zero");
    }
}
