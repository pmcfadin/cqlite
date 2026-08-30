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
//! # Coverage boundary — which read surfaces record phases, and which do NOT
//!
//! A sink only reaches the code that does the work if it is INSTALLED on the thread
//! doing it, and installation happens at SPAWN SITES (see above). So coverage is
//! exactly the set of surfaces whose work runs on a thread this crate spawns for
//! them:
//!
//! **Measured** — the windowed scan driver, reached by both streaming surfaces
//! (`scan_stream` per-row and `scan_stream_batched`) for a chunk-stitching reader
//! (io + decompress + decode); and `generation_merge::stream_generations_for_read`,
//! the streaming cross-generation reconciling merge (merge, plus the DECOMPRESS its
//! per-input producer threads perform).
//!
//! **PARTIALLY measured, and the omission is named rather than implied**: that
//! cross-generation merge route records NO `io` sample. The sink IS propagated into
//! both producer-thread spawn sites (`merge::from_readers`, `merge::producer_iter`),
//! so the work those threads do through the SHARED chunk-decode plane
//! (`reader::chunk_source`) is attributed — that is where `decompress` comes from.
//! But the `io` seam itself exists ONLY in the windowed scan's read helpers
//! (`scan_stream_windowed_read`), and a merge producer reads through
//! `stream_all_partitions_for_compaction` / `_for_query` instead, which has no io
//! seam at any depth. So io on this route is unmeasured for want of a SEAM, not for
//! want of propagation, and closing it means instrumenting a second read route —
//! deliberately not smuggled in here. An earlier version of this paragraph claimed
//! the route recorded "the io/decompress its producer thread performs", which was
//! false in its io half and, under the rule stated below, would have taught an
//! operator to read an absent `io` as "io was free" on exactly the path where io is
//! most likely the problem.
//!
//! **NOT measured — these emit `read.duration` with NO `read.phase.*` series at
//! all**: the materializing `SSTableManager::scan` / `scan_with_meter` and the
//! materializing `merge_generations_for_read` beneath it; the BIG reverse-clustering
//! scan (`reverse_scan.rs`); the BTI trie walk (`stream_bti_scan`); the
//! non-chunk-stitching block-by-block branch; point reads (`get`, the manager point
//! read); and compaction reads.
//!
//! **An absent phase series from one of those surfaces means NOT MEASURED — never
//! "fast".** The rule for distinguishing the two cases is the surface, not the
//! metric: a measured surface's absent phase is a real absence (an uncompressed
//! SSTable decompresses nothing), while these surfaces are silent about every phase
//! at once. If you see `read.duration` rising with no phase breakdown, you are
//! looking at one of them.
//!
//! # What ABSENCE and `0.0` each mean, and why they are tracked separately
//!
//! Within a MEASURED surface, absence of a phase series means the phase DID NOT RUN
//! — that is the whole content of "no `decompress` means uncompressed", "no `merge`
//! means a single generation". A `0.0` sample means something else: the phase RAN
//! and measured zero.
//!
//! Those two are only distinguishable because [`ReadPhaseTimings`] tracks phase
//! ENTRY separately from accumulated duration. Deriving absence from `nanos == 0`
//! — which emission used to do — collapses them, and the collapse is not academic:
//! [`timed_merge_excluding_recv_wait`] SATURATES to `0` whenever the recv-wait it
//! subtracts exceeds the step's wall time, so a real multi-generation merge whose
//! producers starved recorded `0`, was skipped, and told the operator "single
//! generation" (issue #1707). The mechanism that exists to keep merge honest was
//! manufacturing a false statement.
//!
//! ## Why they are not simply instrumented too (issue #1707)
//!
//! Their phase work sits BELOW `.await` points, on the async worker threads, reached
//! through the shared seams that read this thread-local (`chunk_source`,
//! `block_io`). Two consequences:
//!
//! * installing a sink around such a call would hold the guard ACROSS an `.await`,
//!   and a parked task's worker thread runs OTHER tasks — so another scan's decode
//!   would be attributed to this one's counters. Cross-attribution is worse than no
//!   data, because it is indistinguishable from data.
//! * installing it only around the SYNCHRONOUS prologue instead would be worse
//!   still: it would produce phase samples that systematically UNDERSTATE the read
//!   (an `io` of microseconds for a read that spent tens of milliseconds in io),
//!   which an operator cannot tell from a genuinely fast read. Absence they can at
//!   least look up; a plausible wrong number they cannot.
//!
//! Covering them needs async-safe propagation (a task-local carried across awaits,
//! or the sink threaded explicitly through `SSTableReader::scan` and the reverse
//! walk) — a read-path design change, deliberately not smuggled in here.
//!
//! # Why the decode timer is scoped to the parse call (issue #1707)
//!
//! The `Decode` seam in `scan_stream_windowed` wraps
//! `parse_one_partition_with_timestamps` and NOTHING ELSE, by a block expression.
//! That tightness is load-bearing rather than tidiness: bound at loop-iteration
//! scope — which it was — the timer also covered `window.consume`, the
//! `scratch.drain`/`batch.push` re-chunking, the batch `Vec` allocation, and
//! decisively `tx.blocking_send`, which PARKS the parse thread whenever the consumer
//! is slow.
//!
//! A client that pages slowly would then make `read.phase.decode` dominated by
//! waiting for the CONSUMER. The operator follows the runbook — "decode dominant →
//! wide partitions, many collection/UDT cells" — investigates the schema and finds
//! nothing, because the schema was never the problem. It would also contradict the
//! catalogued definition of the phase ("decode out of already-resident decompressed
//! bytes") and invert the care taken for [`ReadPhase::Merge`], which deliberately
//! SUBTRACTS its recv-wait for exactly this reason. Any future phase seam gets the
//! same treatment: a timer's scope must contain only work the phase names, never a
//! blocking handoff to someone else.
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
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
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

impl ReadPhase {
    /// This phase's bit in [`ReadPhaseTimings::entered`]. Kept beside the enum so a
    /// future fifth variant is a compile error here rather than a silently
    /// unrepresented phase.
    const fn bit(self) -> u32 {
        match self {
            ReadPhase::Io => 1 << 0,
            ReadPhase::Decompress => 1 << 1,
            ReadPhase::Decode => 1 << 2,
            ReadPhase::Merge => 1 << 3,
        }
    }
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
    /// Bitmask of the phases that ACTUALLY RAN, tracked SEPARATELY from their
    /// accumulated nanos (issue #1707, roborev job 145).
    ///
    /// Emission needs to distinguish "this phase never ran" from "this phase ran and
    /// measured zero", and a duration cannot answer that: both are `0`. The docs
    /// assign load-bearing meaning to ABSENCE — no `decompress` series means the
    /// SSTable is uncompressed, no `merge` series means a single generation — so
    /// deriving absence from `nanos == 0` makes a real phase that measured zero
    /// report the opposite of the truth.
    ///
    /// That is not hypothetical for [`ReadPhase::Merge`]:
    /// [`timed_merge_excluding_recv_wait`] deliberately SATURATES to `0` when the
    /// recv-wait it subtracts exceeds the step's wall time, so a genuine
    /// multi-generation merge whose producer starved records `0` — and would have
    /// been reported to the operator as "single generation", a false statement
    /// manufactured by the very mechanism that exists to keep the number honest.
    entered: AtomicU32,
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
        // Entry is marked HERE, in the one funnel every timing site already passes
        // through — `timed`, `timed_merge_excluding_recv_wait` and
        // `ReadPhaseTimer::drop` all end in this call, unconditionally, whatever
        // elapsed. A separate `mark_entered` at each site would be a second thing to
        // remember and a second thing to drift; reaching this function IS the
        // evidence that a timed region for `phase` completed, including one that
        // completed in zero measurable time.
        self.entered.fetch_or(phase.bit(), Ordering::Relaxed);
        let _ = self
            .counter(phase)
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(nanos))
            });
    }

    /// Whether `phase` RAN during this scan — true even if it accumulated zero
    /// nanoseconds (issue #1707). This, not `nanos(phase) > 0`, is what emission
    /// gates on: see [`ReadPhaseTimings::entered`] for why the two are different
    /// questions.
    pub fn entered(&self, phase: ReadPhase) -> bool {
        self.entered.load(Ordering::Relaxed) & phase.bit() != 0
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

    /// Monotonically increasing id stamped on each [`ReadPhaseGuard`] this thread
    /// creates, and asserted (debug builds only) on drop — see the guard's `Drop`.
    static INSTALL_GENERATION: Cell<u64> = const { Cell::new(0) };

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
    /// This guard's position in its thread's install stack, checked on drop.
    generation: u64,
}

impl Drop for ReadPhaseGuard {
    fn drop(&mut self) {
        // Drop restores UNCONDITIONALLY, which is right for correct (LIFO) nesting
        // and wrong-but-silent for out-of-order nesting: dropping an OUTER guard
        // first would restore its `prev` over the inner sink and leave the inner one
        // installed forever on a POOLED blocking thread — the cross-scan attribution
        // leak this module exists to prevent, and the one failure mode that is
        // indistinguishable from data. All four call sites are LIFO today; the
        // `debug_assert` is what makes a future fifth one fail LOUDLY in tests
        // instead of silently mis-attributing in production.
        debug_assert_eq!(
            INSTALL_GENERATION.with(|c| c.get()),
            self.generation,
            "ReadPhaseGuard dropped OUT OF ORDER: guards must nest LIFO, or an inner \
             sink stays installed on this (possibly pooled) thread and the next scan \
             to run on it is attributed to the previous scan's counters"
        );
        INSTALL_GENERATION.with(|c| c.set(self.generation.saturating_sub(1)));

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
    // Depth of this thread's install stack; the guard's `Drop` asserts it is still
    // the top when it unwinds. `saturating_add` so the counter can never wrap into
    // a value a live guard already holds.
    let generation = INSTALL_GENERATION.with(|c| {
        let next = c.get().saturating_add(1);
        c.set(next);
        next
    });
    ReadPhaseGuard {
        prev,
        prev_active,
        generation,
    }
}

/// Whether a read-phase sink is installed on this thread — a cheap `Cell<bool>`
/// load (no `RefCell` borrow, no `Arc` clone), for a hot caller that wants to skip
/// `Instant::now()` entirely when unmetered.
#[inline]
pub fn sink_active() -> bool {
    SINK_ACTIVE.with(|c| c.get())
}

/// This thread's installed sink, if any. A spawn site calls this on the PARENT
/// thread and re-[`install`]s the captured value on the CHILD thread, so the child's
/// io/decompress/decode/merge reach the scan's accumulator.
#[inline]
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
#[inline]
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
/// That 0 is still an OBSERVATION and is emitted as a `0.0` sample, not swallowed:
/// entry is recorded by the `add_nanos` call below independently of the value, so a
/// merge that ran cannot be reported as a scan with no merge at all (#1707).
///
/// Carries the EXACT cfg of its only call site, `generation_merge::
/// stream_generations_for_read`, which needs BOTH conditions to exist:
/// `write-support` gates the whole `generation_merge` module at its `mod`
/// declaration in `storage/sstable/mod.rs`, and `not(tombstones)` gates the
/// streaming function inside it. Either one off and there is no merge step to time,
/// so an ungated definition is provably dead code — which is exactly what the
/// minimal-features build (`--no-default-features --features all-compression`, i.e.
/// write-support OFF) turns into a `-D dead-code` hard error. Keep this cfg in sync
/// with the call site rather than silencing the lint: if the last real caller ever
/// disappears, the build SHOULD say so.
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
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
///
/// This module deliberately exposes a SMALLER entry surface than its
/// [`super::stream_subphase`] twin: the twin's `record_nanos` and `scoped_captured`
/// have no counterpart here because nothing in this crate calls them, and the module
/// is `pub(crate)` so nothing outside can either (issue #1707). Dead code kept alive
/// "for symmetry" is still dead code; the twin's versions remain where they have real
/// callers (`data_access`). Add them back the day a seam needs them.
#[inline]
pub fn scoped(phase: ReadPhase) -> Option<ReadPhaseTimer> {
    // Same fast path as [`timed`]: the io seam calls this once per chunk read, and
    // an unmetered scan must pay only a `Cell<bool>` load — no `RefCell` borrow, no
    // `Arc` clone, no `Instant::now()`.
    if !sink_active() {
        return None;
    }
    // ONE `Arc` clone, not two: `current()` already clones out of the thread-local,
    // so handing its `Option` to `scoped_captured` (which clones its borrowed
    // argument) doubled the refcount traffic on a per-chunk seam.
    current().map(|sink| ReadPhaseTimer {
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
