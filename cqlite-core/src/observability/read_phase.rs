//! Per-SCAN read-phase wall-time accumulator (issue #1707, AI7 of epic #1686).
//!
//! # Why this exists
//!
//! [`catalog::READ_DURATION`](super::catalog::READ_DURATION) says a read was slow.
//! Nothing said WHERE the time went, so "why was this query slow?" could not be
//! answered from metrics at all — it needed a profiler on the box. This module
//! accumulates ONE scan's wall time into four buckets — io / decompress / decode /
//! merge — at the read path's EXISTING function seams, and the owning
//! `ReadOpMeter` (`super::read_metrics`) emits them as exactly ONE
//! sample per phase when the scan completes.
//!
//! # Why it is a sibling of [`super::stream_subphase`] and not an extension of it
//!
//! `stream_subphase` looks almost identical, and reusing it was considered and
//! rejected for two structural reasons:
//!
//! * its six variants are PINNED by a `cqlite-flight` test asserting the set is
//!   exactly six, because they map 1:1 onto the documented bounded cardinality of
//!   [`catalog::RPC_PHASE_DURATION`](super::catalog::RPC_PHASE_DURATION)'s `phase`
//!   attribute — adding a variant silently widens a metric dimension; and
//! * its sink is installed ONLY by the Flight `do_get` path, so a core scan (CLI,
//!   embedded, query engine) installs nothing and every sample would be dropped.
//!
//! The two accumulators therefore coexist and can both be installed on one thread:
//! a Flight read attributes the same work to `cqlite.rpc.phase.duration` and to
//! `cqlite.read.phase.*` independently, which is correct — they are two accountings
//! of one pipeline, not two halves of one accounting.
//!
//! # Ownership, and why emission cannot double-count
//!
//! The `Arc<ReadPhaseTimings>` is created by `ReadOpMeter::start` and owned by that
//! meter, so it inherits the meter's whole lifecycle for free: `finish()` is
//! idempotent, `Drop` calls it, and `ReadOpMeter::inert()` (every sub-scan of a
//! fan-out merge, and the per-row → batch re-chunker) has no accumulator at all.
//! That is what makes "ONE sample per phase per completed scan" a property of the
//! design rather than of a convention every call site must remember.
//!
//! # Thread propagation is EXPLICIT
//!
//! Thread-locals are not inherited across a spawn, and the phases physically happen
//! on threads that never see the meter: a `spawn_blocking` IO feed thread, a
//! `spawn_blocking` parse thread, and a merge producer thread. So the `Arc` is
//! passed to each of those closures at its SPAWN SITE and re-[`install`]ed there —
//! the same shape `stream_subphase` uses, and deliberately not a "walk up some
//! ambient context" scheme, which cannot work across a thread boundary.
//!
//! # Zero cost when off
//!
//! `ReadOpMeter::start` consults [`obs::metrics_active`](super::metrics_active)
//! ONCE and builds NO accumulator when metrics are not being collected, so no sink
//! is installed, [`current`] returns `None`, and [`timed`] runs the closure with a
//! single thread-local peek — no `Instant::now()`, no atomic write. With the
//! `observability` feature off, `metrics_active()` is a compile-time `false`, so
//! the whole thing degenerates to that one branch.

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// One of the four coarse read phases (issue #1707).
///
/// Deliberately COARSE and deliberately four: each is measured at a function seam
/// the read path already has, and none is per-row or per-cell. The row/cell decoder
/// is the hottest loop in the read path and is never instrumented — `Decode` is
/// accumulated once per PARTITION, at the parse boundary above it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPhase {
    /// `Data.db` reads: the positional chunk/piece reads the scan performs,
    /// CRC verify included, decompression excluded.
    Io,
    /// Chunk decompression, measured in the single chunk-decode plane around the
    /// compressor call only.
    Decompress,
    /// Row/cell decode out of already-resident decompressed bytes, accumulated per
    /// partition at the parse boundary.
    Decode,
    /// k-way merge + reconcile, with the blocking merge-input recv-wait excluded
    /// (that wait is producer starvation, i.e. io on another thread).
    Merge,
}

/// Per-scan accumulator of read-phase wall time, in nanoseconds.
///
/// Four `AtomicU64`s so the concurrent pipeline threads (IO feed, blocking parse,
/// merge producer) all `fetch_update` into the same shared instance lock-free. The
/// phases OVERLAP in wall-clock — the pipeline is concurrent — so they are NOT
/// expected to sum to the scan's `read.duration`; the load-bearing signal is which
/// phase dominates and how that moves between runs.
#[derive(Debug, Default)]
pub struct ReadPhaseTimings {
    io_nanos: AtomicU64,
    decompress_nanos: AtomicU64,
    decode_nanos: AtomicU64,
    merge_nanos: AtomicU64,
}

impl ReadPhaseTimings {
    fn counter(&self, phase: ReadPhase) -> &AtomicU64 {
        match phase {
            ReadPhase::Io => &self.io_nanos,
            ReadPhase::Decompress => &self.decompress_nanos,
            ReadPhase::Decode => &self.decode_nanos,
            ReadPhase::Merge => &self.merge_nanos,
        }
    }

    /// Add `nanos` to `phase` (SATURATING, `Relaxed` — independent per-phase totals
    /// with no ordering dependency between them). `fetch_update` rather than
    /// `fetch_add` SOLELY so the add saturates instead of wrapping on the
    /// (practically unreachable) `u64` nanosecond overflow: a wrap would turn a huge
    /// total into a tiny one, which reads as "this phase was fast".
    pub fn add_nanos(&self, phase: ReadPhase, nanos: u64) {
        let _ = self
            .counter(phase)
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(nanos))
            });
    }

    /// Read `phase`'s accumulated nanoseconds (a snapshot).
    pub fn nanos(&self, phase: ReadPhase) -> u64 {
        self.counter(phase).load(Ordering::Relaxed)
    }
}

thread_local! {
    /// This thread's installed sink, or `None` for every unmetered caller.
    /// `const`-initialised so the common (unset) path is a cheap read with no lazy
    /// init.
    static CURRENT_SINK: RefCell<Option<Arc<ReadPhaseTimings>>> = const { RefCell::new(None) };

    /// Fast `Cell<bool>` mirror of "a sink is installed", kept in lock-step with
    /// `CURRENT_SINK` by [`install`] and the guard's `Drop`. A hot caller gates its
    /// `Instant::now()` on this single load instead of a `RefCell` borrow plus an
    /// `Arc` clone.
    static SINK_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

/// RAII guard restoring the previous sink on drop — panic-safe, so a reused
/// blocking-pool thread never leaks one scan's sink into the next.
#[must_use = "the sink is uninstalled when the guard is dropped"]
pub struct ReadPhaseGuard {
    prev: Option<Arc<ReadPhaseTimings>>,
    prev_active: bool,
}

impl Drop for ReadPhaseGuard {
    fn drop(&mut self) {
        let prev = self.prev.take();
        CURRENT_SINK.with(|c| *c.borrow_mut() = prev);
        SINK_ACTIVE.with(|c| c.set(self.prev_active));
    }
}

/// Install `sink` as this thread's read-phase sink for the lifetime of the returned
/// guard, restoring the previous value on drop. Passing `None` installs "no sink"
/// (what a spawn site propagates when its parent had none) — still restoring the
/// prior value on drop, so nesting is sound.
pub fn install(sink: Option<Arc<ReadPhaseTimings>>) -> ReadPhaseGuard {
    let active = sink.is_some();
    let prev = CURRENT_SINK.with(|c| std::mem::replace(&mut *c.borrow_mut(), sink));
    let prev_active = SINK_ACTIVE.with(|c| c.replace(active));
    ReadPhaseGuard { prev, prev_active }
}

/// Whether a read-phase sink is installed on this thread — a cheap `Cell<bool>`
/// load (no `RefCell` borrow, no `Arc` clone), for a hot caller that wants to skip
/// `Instant::now()` entirely when unmetered.
pub fn sink_active() -> bool {
    SINK_ACTIVE.with(|c| c.get())
}

/// This thread's installed sink, if any. A spawn site calls this on the PARENT
/// thread and re-[`install`]s the captured value on the CHILD thread, so the child's
/// io/decompress/decode/merge reach the scan's accumulator.
pub fn current() -> Option<Arc<ReadPhaseTimings>> {
    CURRENT_SINK.with(|c| c.borrow().clone())
}

/// Clamped nanoseconds elapsed since `start` — the ONE place the `Instant`→`u64`
/// clamp lives, reused by every timing site here. A scan long enough to overflow
/// `u64` nanoseconds (~584 years) is unreachable.
pub fn elapsed_nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

/// Time `f` and, IF a sink is installed on this thread, attribute its elapsed wall
/// time to `phase`. When no sink is installed this is a single thread-local peek
/// plus the bare closure — no `Instant::now()`, no atomic write.
pub fn timed<T>(phase: ReadPhase, f: impl FnOnce() -> T) -> T {
    // Fast path FIRST: one `Cell<bool>` load, so an unmetered read pays neither a
    // `RefCell` borrow nor an `Arc` refcount bump on a per-chunk seam.
    if !sink_active() {
        return f();
    }
    match current() {
        None => f(),
        Some(sink) => {
            let start = Instant::now();
            let out = f();
            sink.add_nanos(phase, elapsed_nanos(start));
            out
        }
    }
}

/// Time `f` (a k-way MERGE step) and attribute its elapsed wall time to
/// [`ReadPhase::Merge`] MINUS the merge-input recv-wait accrued inside it.
///
/// Raw wall time around a merge step is mostly BLOCKING RECV on the merge inputs —
/// producer starvation, i.e. io happening on another thread — so charging it to
/// `merge` would make every disk-bound read look merge-bound. The recv sites already
/// accumulate that wait per thread for the #2819 Flight sub-phases
/// ([`super::stream_subphase::pull_wait_nanos`]); this reads the SAME accumulator's
/// delta and subtracts it, rather than duplicating a second thread-local and a
/// second call site at every recv — two accumulators of one quantity is exactly the
/// "two statements of one fact can disagree" shape.
///
/// Saturating: if a nested/foreign recv were somehow attributed a longer wait than
/// this step's wall time, the phase gets 0 rather than a wrapped enormous value.
pub fn timed_merge_excluding_recv_wait<T>(f: impl FnOnce() -> T) -> T {
    if !sink_active() {
        return f();
    }
    match current() {
        None => f(),
        Some(sink) => {
            let wait_before = super::stream_subphase::pull_wait_nanos();
            let start = Instant::now();
            let out = f();
            let wall = elapsed_nanos(start);
            let wait = super::stream_subphase::pull_wait_nanos().saturating_sub(wait_before);
            sink.add_nanos(ReadPhase::Merge, wall.saturating_sub(wait));
            out
        }
    }
}

/// Add `nanos` directly to `phase` on this thread's sink, if installed (no-op
/// otherwise) — for a caller that measured the elapsed time itself.
pub fn record_nanos(phase: ReadPhase, nanos: u64) {
    CURRENT_SINK.with(|c| {
        if let Some(sink) = c.borrow().as_ref() {
            sink.add_nanos(phase, nanos);
        }
    });
}

/// RAII timer recording elapsed wall time into `phase` on drop — the tight-scope
/// counterpart of [`timed`] for a region a sync closure cannot wrap (an `.await`ed
/// read, or a region with early returns).
///
/// It CAPTURES the sink `Arc` at construction, never re-resolving the thread-local
/// at drop time, so it stays correct even if it is held across an `.await` that
/// resumes the future on a DIFFERENT executor thread.
pub struct ReadPhaseTimer {
    phase: ReadPhase,
    start: Instant,
    sink: Arc<ReadPhaseTimings>,
}

impl Drop for ReadPhaseTimer {
    fn drop(&mut self) {
        self.sink.add_nanos(self.phase, elapsed_nanos(self.start));
    }
}

/// A [`ReadPhaseTimer`] for `phase`, or `None` (and zero `Instant::now`) when no
/// sink is installed.
pub fn scoped(phase: ReadPhase) -> Option<ReadPhaseTimer> {
    // Same fast path as [`timed`]: the io seam calls this once per chunk read, and
    // an unmetered scan must pay only a `Cell<bool>` load — no `RefCell` borrow, no
    // `Arc` clone, no `Instant::now()`.
    if !sink_active() {
        return None;
    }
    scoped_captured(&current(), phase)
}

/// A [`ReadPhaseTimer`] built from an ALREADY-CAPTURED sink, so NO thread-local read
/// happens here. Use when the timer must be constructed after an `.await` that may
/// resume on a different executor thread: capture the sink ONCE before the await,
/// then build each timer from that captured `Option`.
pub fn scoped_captured(
    sink: &Option<Arc<ReadPhaseTimings>>,
    phase: ReadPhase,
) -> Option<ReadPhaseTimer> {
    sink.clone().map(|sink| ReadPhaseTimer {
        phase,
        start: Instant::now(),
        sink,
    })
}

/// TEST-ONLY artificial delay inside the io phase (issue #1707).
///
/// # Why an injected delay is the only honest way to pin the io phase
///
/// The property under test is ATTRIBUTION: "time spent reading `Data.db` is charged
/// to `read.phase.io`". On a warm page cache over a small committed fixture the real
/// io time is microseconds, so any assertion about its share would be a wall-clock
/// race (#2642) — the test would be measuring the host, not the code. Injecting a
/// known, dominant delay AT THE READ makes the assertion STRUCTURAL instead: with
/// milliseconds of deliberate delay per read, io must dominate unless the seam is
/// mis-wired, and no timing luck can change that verdict.
///
/// Compiled out entirely unless `observability-testing` (the feature that already
/// gates the in-memory metric capture these tests need) or `cfg(test)` is on: a
/// production build has no arming surface, no atomic, and no branch. Same shape as
/// `storage::producer_fault` — a test-only seam that is a compile-time no-op.
#[cfg(any(test, feature = "observability-testing"))]
pub mod io_delay {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Nanoseconds to sleep at each instrumented read, or 0 for "not armed".
    static DELAY_NANOS: AtomicU64 = AtomicU64::new(0);

    /// Disarms on drop, so a panicking test cannot leave every later scan in the
    /// process slowed down.
    #[must_use = "the delay is disarmed when the guard is dropped"]
    pub struct ArmedIoDelay;

    impl Drop for ArmedIoDelay {
        fn drop(&mut self) {
            DELAY_NANOS.store(0, Ordering::SeqCst);
        }
    }

    /// Sleep `per_read` inside every instrumented `Data.db` read until the returned
    /// guard drops.
    ///
    /// Process-global, deliberately: the seam is reached from several threads of one
    /// scan, so a per-thread arm would cover only the arming thread. That is sound
    /// because the arming test runs SERIALLY in its own test binary — a
    /// process-global metric capture already forces that (see the `#[serial]`
    /// attribute on every test in `issue_1707_read_phase_timings.rs`).
    pub fn arm(per_read: std::time::Duration) -> ArmedIoDelay {
        DELAY_NANOS.store(
            per_read.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::SeqCst,
        );
        ArmedIoDelay
    }

    /// Sleep the armed delay, if any. Called INSIDE the io-phase timed region, so an
    /// armed delay is charged to `read.phase.io` exactly as real read latency is.
    pub(crate) fn sleep_if_armed() {
        let nanos = DELAY_NANOS.load(Ordering::Relaxed);
        if nanos > 0 {
            std::thread::sleep(std::time::Duration::from_nanos(nanos));
        }
    }
}

/// Production no-op twin of [`io_delay::sleep_if_armed`] — no atomic, no branch.
#[cfg(not(any(test, feature = "observability-testing")))]
pub(crate) mod io_delay {
    #[inline(always)]
    pub(crate) fn sleep_if_armed() {}
}

/// Unit tests live in a sibling file so this module stays inside the campsite-rule
/// source target (#1116); they are logically the `tests` submodule.
#[cfg(test)]
#[path = "read_phase_tests.rs"]
mod tests;
