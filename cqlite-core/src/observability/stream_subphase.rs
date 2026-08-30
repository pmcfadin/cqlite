//! In-`stream` sub-phase wall-time accumulator for the Flight `do_get` data plane
//! (issue #2819).
//!
//! The Flight `do_get` `stream` phase (`cqlite-flight`) is a CONCURRENT pipeline:
//! the merge consumer runs on one `spawn_blocking` thread and each input SSTable
//! is scanned on its own producer thread, where the cold body-chunk page-in + LZ4
//! decompress physically happen (synchronously, on that producer thread — NOT a
//! separate `spawn_blocking` feed thread). To attribute WHERE the in-`stream` time
//! goes (cold-IO vs decompress vs merge vs encode vs gRPC-write) WITHOUT a
//! profiler, the flight side installs a per-request accumulator and each pipeline
//! thread adds its own elapsed wall time into the right bucket.
//!
//! Thread-locals are NOT inherited across a thread spawn, so the per-request sink
//! is propagated EXPLICITLY at each spawn site: the merge closure installs it on
//! the merge consumer thread, and [`current`] captures it there so the per-SSTable
//! producer thread can re-[`install`] the SAME `Arc`. A deeper `spawn_blocking`
//! feed thread (the windowed-scan page-in path) is NOT reached by this
//! propagation and is therefore NOT covered — the instrumented Summary-guided /
//! full-ring compaction read paths run their page-in on the producer thread, which
//! IS covered.
//!
//! This module owns the crate-side half of that seam so the `cqlite-core` scan
//! path (which is where cold-fault + decompress physically happen) can push into
//! it WITHOUT taking a new parameter on every hot reader signature:
//!
//! * [`StreamSubPhaseTimings`] — five `AtomicU64` nanosecond counters shared via an
//!   `Arc`, so scopes on the concurrent pipeline threads accumulate lock-free.
//! * A thread-local `Option<Arc<StreamSubPhaseTimings>>` sink installed for the
//!   duration of one RPC's work on a thread, via the panic-safe RAII
//!   [`StreamSubPhaseGuard`]. [`current`] reads it (so a spawn site can propagate
//!   the SAME `Arc` onto a child scan thread); [`timed`] times a closure and
//!   attributes it to a sub-phase on the installed sink.
//! * A thread-local recv-wait accumulator ([`add_pull_wait_nanos`] /
//!   [`pull_wait_nanos`]) the merge consumer thread uses to EXCLUDE the blocking
//!   merge-input channel wait from `stream_merge` (issue #2819 B2 — that wait is
//!   producer starvation / cold-IO, not merge CPU).
//!
//! # Zero-cost when absent
//!
//! Every non-flight caller (compaction, CLI, point reads) never installs a sink,
//! so [`current`] returns `None`, [`timed`] runs the closure with a single
//! thread-local peek and NO `Instant::now()` and NO atomic write, and
//! [`record_nanos`] is a no-op. The instrumentation is paid for only on the flight
//! streaming path that opted in.
//!
//! # Isolation
//!
//! The sink is scoped to the request's scan threads only: it is installed with an
//! RAII guard that restores the previous thread-local value on drop (including on
//! panic / unwind), so a reused blocking-pool thread never leaks one RPC's `Arc`
//! into another RPC or an unrelated thread.

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// One of the six in-`stream` data-plane sub-phases (issue #2819; `EncodeFraming`
/// added by issue #3096). Which thread records each is fixed by the pipeline
/// architecture: `ColdFault`/`Decompress` on the per-SSTable PRODUCER thread(s)
/// (where the page-in + decompress run synchronously), `Merge`/`Encode`/`GrpcWrite`
/// all on the MERGE-CONSUMER `spawn_blocking` thread (`GrpcWrite` is
/// `ChannelSink::emit`, which runs on that thread, NOT a separate egress thread),
/// and `EncodeFraming` on the ASYNC gRPC task that polls the response stream. The
/// flight side maps these to the bounded `cqlite.rpc.phase` values
/// `stream_cold_fault` / `stream_decompress` / `stream_merge` / `stream_encode` /
/// `stream_encode_framing` / `stream_grpc_write`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSubPhase {
    /// Synchronous SSTable body-chunk page-in (cold-IO latency).
    ColdFault,
    /// LZ4 chunk decompression of a read body chunk.
    Decompress,
    /// k-way merge + LWW/tombstone/TTL reconcile + per-row materialize.
    Merge,
    /// Arrow `RecordBatch` conversion (encode) — the ARRAY BUILD only.
    Encode,
    /// Arrow-flight IPC FRAMING of an already-built `RecordBatch`: everything
    /// `FlightDataEncoderBuilder`'s stream does — dictionary hydration, splitting
    /// a batch that exceeds the encoder's `GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES`, and
    /// the IPC serialization into `FlightData` messages (issue #3096).
    ///
    /// # Why this is a SEPARATE bucket from [`Self::Encode`]
    ///
    /// [`Self::Encode`] wraps only the Arrow array build (`flush_buffer` →
    /// `rows_to_record_batch`) on the merge-consumer thread. It has never covered
    /// the encoder stage, which runs LATER and on a DIFFERENT thread (the async
    /// gRPC task). Any change aimed at the framing stage — the batch-size/encoder
    /// target alignment, or the dictionary-hydration rebuild — was therefore
    /// UNFALSIFIABLE from in-process timings alone: the only bucket that could
    /// have moved does not span the code being changed. This variant closes that
    /// blind spot.
    ///
    /// It measures the framing stage's POLL time, so it also contains the (cheap,
    /// non-blocking) downstream channel poll that returns `Pending` when the merge
    /// has not yet produced a batch. It is a server-side CPU bucket in the same
    /// sense as [`Self::Encode`], not a client-paced one like [`Self::GrpcWrite`].
    EncodeFraming,
    /// Egress channel `reserve()`/send, including backpressure park/wake.
    GrpcWrite,
}

/// Per-request accumulator of in-`stream` sub-phase wall time, in nanoseconds.
///
/// Six `AtomicU64` counters so RAII scopes running on the CONCURRENT pipeline
/// threads (the per-SSTable producer thread(s), the merge-consumer thread, and —
/// for [`StreamSubPhase::EncodeFraming`] — the async gRPC task) all `fetch_add`
/// into the same shared instance lock-free. The sub-phases OVERLAP in
/// wall-clock (the pipeline is concurrent), so the counters are NOT expected to
/// sum to the `stream` phase's duration — the load-bearing signal is the
/// cold−warm delta on the cold-fault counter (issue #2819, amended accounting
/// model).
#[derive(Debug, Default)]
pub struct StreamSubPhaseTimings {
    cold_fault_nanos: AtomicU64,
    decompress_nanos: AtomicU64,
    merge_nanos: AtomicU64,
    encode_nanos: AtomicU64,
    encode_framing_nanos: AtomicU64,
    grpc_write_nanos: AtomicU64,
}

impl StreamSubPhaseTimings {
    fn counter(&self, phase: StreamSubPhase) -> &AtomicU64 {
        match phase {
            StreamSubPhase::ColdFault => &self.cold_fault_nanos,
            StreamSubPhase::Decompress => &self.decompress_nanos,
            StreamSubPhase::Merge => &self.merge_nanos,
            StreamSubPhase::Encode => &self.encode_nanos,
            StreamSubPhase::EncodeFraming => &self.encode_framing_nanos,
            StreamSubPhase::GrpcWrite => &self.grpc_write_nanos,
        }
    }

    /// Add `nanos` to `phase`'s counter (SATURATING, `Relaxed` — the counters are
    /// independent per-sub-phase totals with no ordering dependency). A plain
    /// `fetch_add` would already be atomic and correct under the several concurrent
    /// producer threads that share one `Arc` and write `cold_fault`/`decompress`;
    /// the CAS loop (`fetch_update`) is used SOLELY so the add SATURATES instead of
    /// wrapping on the (practically unreachable) `u64` nanosecond overflow.
    pub fn add_nanos(&self, phase: StreamSubPhase, nanos: u64) {
        let _ = self
            .counter(phase)
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(nanos))
            });
    }

    /// Read `phase`'s accumulated nanoseconds (a snapshot; a concurrently-running
    /// producer thread may add more after this read — acceptable, the emission is a
    /// best-effort per-RPC snapshot at teardown).
    pub fn nanos(&self, phase: StreamSubPhase) -> u64 {
        self.counter(phase).load(Ordering::Relaxed)
    }
}

thread_local! {
    /// The current thread's installed sub-phase sink, or `None` for every
    /// non-flight caller. `const`-initialised so the common (unset) path is a
    /// cheap thread-local read with no lazy init.
    static CURRENT_SINK: RefCell<Option<Arc<StreamSubPhaseTimings>>> =
        const { RefCell::new(None) };

    /// Fast `Cell<bool>` mirror of "a sink is installed on this thread", kept in
    /// lock-step with `CURRENT_SINK` by [`install`] + the guard's `Drop`. A hot
    /// caller (the merge-input recv-wait site) gates its `Instant::now()` on this
    /// single `Cell` load instead of a `RefCell` borrow + `Arc` clone (issue #2819
    /// B3) — so a non-flight scan pays effectively nothing.
    static SINK_ACTIVE: Cell<bool> = const { Cell::new(false) };

    /// Running merge-input channel recv-wait accrued on THIS (merge consumer)
    /// thread, in nanoseconds (issue #2819 B2). The row-drive loop snapshots it
    /// around each `step_row` and subtracts the delta from that step's wall time,
    /// so the blocking wait for a producer to deliver the next entry (producer
    /// starvation / cold-IO) is EXCLUDED from `stream_merge` — leaving merge CPU.
    static PULL_WAIT_NANOS: Cell<u64> = const { Cell::new(0) };
}

/// RAII guard restoring the previous sub-phase sink on drop — panic-safe, so a
/// reused blocking-pool thread never leaks one RPC's sink into another.
#[must_use = "the sink is uninstalled when the guard is dropped"]
pub struct StreamSubPhaseGuard {
    prev: Option<Arc<StreamSubPhaseTimings>>,
    prev_active: bool,
}

impl Drop for StreamSubPhaseGuard {
    fn drop(&mut self) {
        let prev = self.prev.take();
        CURRENT_SINK.with(|c| *c.borrow_mut() = prev);
        SINK_ACTIVE.with(|c| c.set(self.prev_active));
    }
}

/// Install `sink` as the current thread's sub-phase sink for the lifetime of the
/// returned guard, restoring the previous value on drop. Passing `None` installs
/// "no sink" (used when a scan-thread spawn captured `None` from a non-flight
/// caller) — still restoring the prior value on drop.
pub fn install(sink: Option<Arc<StreamSubPhaseTimings>>) -> StreamSubPhaseGuard {
    let active = sink.is_some();
    let prev = CURRENT_SINK.with(|c| std::mem::replace(&mut *c.borrow_mut(), sink));
    let prev_active = SINK_ACTIVE.with(|c| c.replace(active));
    StreamSubPhaseGuard { prev, prev_active }
}

/// Whether a flight sub-phase sink is installed on this thread — a cheap
/// `Cell<bool>` load (no `RefCell` borrow, no `Arc` clone). A hot caller uses it
/// to skip `Instant::now()` entirely on the non-flight path (issue #2819 B3).
pub fn sink_active() -> bool {
    SINK_ACTIVE.with(|c| c.get())
}

/// Add merge-input channel recv-wait (`nanos`) to this thread's running pull-wait
/// total (issue #2819 B2). Recorded only by the merge consumer thread, at the
/// blocking `SSTableRowIterator::next` recv site, so the row-drive loop can
/// subtract it from `stream_merge`. A plain thread-local `Cell` add — cheap, and
/// never touched on the non-flight path (the caller gates on [`sink_active`]).
pub fn add_pull_wait_nanos(nanos: u64) {
    PULL_WAIT_NANOS.with(|c| c.set(c.get().saturating_add(nanos)));
}

/// This thread's running merge-input recv-wait total, in nanoseconds (issue #2819
/// B2). The drive loop reads it before/after each `step_row` and uses the delta.
pub fn pull_wait_nanos() -> u64 {
    PULL_WAIT_NANOS.with(|c| c.get())
}

/// The current thread's installed sub-phase sink, if any. A scan-thread spawn
/// site calls this on the PARENT thread and re-[`install`]s the captured value on
/// the CHILD thread, so the per-SSTable producer thread's page-in/decompress reach
/// the request's accumulator.
pub fn current() -> Option<Arc<StreamSubPhaseTimings>> {
    CURRENT_SINK.with(|c| c.borrow().clone())
}

/// Clamped nanoseconds elapsed since `start` — the ONE place the `Instant`→`u64`
/// clamp lives (issue #2819 L3), reused by every sub-phase timing site. A scan
/// long enough to overflow `u64` nanoseconds (~584 years) is unreachable.
pub fn elapsed_nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

/// Time `f` and, IF a sink is installed on this thread, attribute its elapsed
/// wall time to `phase`. When no sink is installed (every non-flight caller) this
/// is a single thread-local peek plus the bare closure — no `Instant::now()`, no
/// atomic write — so the hot scan path pays effectively nothing.
pub fn timed<T>(phase: StreamSubPhase, f: impl FnOnce() -> T) -> T {
    // Clone the Arc only when a sink is actually installed, so the unset path
    // never touches a refcount.
    let sink = current();
    match sink {
        None => f(),
        Some(sink) => {
            let start = Instant::now();
            let out = f();
            sink.add_nanos(phase, elapsed_nanos(start));
            out
        }
    }
}

/// Add `nanos` directly to `phase` on the current thread's sink, if installed
/// (no-op otherwise). For a caller that measured elapsed time itself rather than
/// wrapping a closure via [`timed`].
pub fn record_nanos(phase: StreamSubPhase, nanos: u64) {
    CURRENT_SINK.with(|c| {
        if let Some(sink) = c.borrow().as_ref() {
            sink.add_nanos(phase, nanos);
        }
    });
}

/// RAII timer that records the elapsed wall time into `phase` when dropped. The
/// tight-scope counterpart of [`timed`] for an ASYNC region that a sync closure
/// cannot wrap (e.g. an `.await`ed page-in): bind it in a `{ let _t = …; expr }`
/// block so it drops the instant the region ends.
///
/// It CAPTURES the sink `Arc` at construction (like [`timed`]), NOT via the
/// thread-local at drop time — so it stays correct even if the guard is held
/// across an `.await` that resumes the future on a DIFFERENT executor thread
/// (issue #2819 L1). A `None` sink means "not installed" — a no-op on drop.
pub struct SubPhaseTimer {
    phase: StreamSubPhase,
    start: Instant,
    sink: Arc<StreamSubPhaseTimings>,
}

impl Drop for SubPhaseTimer {
    fn drop(&mut self) {
        self.sink.add_nanos(self.phase, elapsed_nanos(self.start));
    }
}

/// A [`SubPhaseTimer`] for `phase`, or `None` (zero `Instant::now`) when no sink
/// is installed — so a non-flight caller pays nothing. The sink `Arc` is captured
/// here (correct-by-construction across an `.await`), never re-resolved at drop.
pub fn scoped(phase: StreamSubPhase) -> Option<SubPhaseTimer> {
    scoped_captured(&current(), phase)
}

/// A [`SubPhaseTimer`] for `phase` built from an ALREADY-CAPTURED sink (from a
/// prior [`current`] call), so NO thread-local read happens here (issue #2819
/// L1). Use when the timer must be constructed AFTER an `.await` that may resume
/// on a different executor thread — capture the sink ONCE before the await, then
/// build each timer from that captured `Option`.
pub fn scoped_captured(
    sink: &Option<Arc<StreamSubPhaseTimings>>,
    phase: StreamSubPhase,
) -> Option<SubPhaseTimer> {
    sink.clone().map(|sink| SubPhaseTimer {
        phase,
        start: Instant::now(),
        sink,
    })
}

/// Time `f` (a BLOCKING merge-input recv) and add its elapsed to this thread's
/// pull-wait accumulator IF a sink is installed, so the row-drive loop can
/// EXCLUDE it from `stream_merge` (issue #2819 B2). Zero `Instant::now` when
/// inert, so the non-flight compaction merge pays nothing.
pub fn time_recv<T>(f: impl FnOnce() -> T) -> T {
    // Also armed by an installed READ-phase sink (issue #1707): `read.phase.merge`
    // subtracts this SAME per-thread accumulator's delta from each merge step, so
    // producer starvation is not charged to merge CPU. ONE accumulator and ONE call
    // site serve both accountings — duplicating a second thread-local and a second
    // recv-site call is the "two statements of one fact can disagree" shape.
    if sink_active() || super::read_phase::sink_active() {
        let start = Instant::now();
        let out = f();
        add_pull_wait_nanos(elapsed_nanos(start));
        out
    } else {
        f()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn timed_is_noop_when_no_sink_installed() {
        // No sink on this thread: `timed` must still run the closure and record
        // nothing anywhere (there is nowhere to record).
        assert!(current().is_none());
        let ran = AtomicBool::new(false);
        let out = timed(StreamSubPhase::ColdFault, || {
            ran.store(true, Ordering::Relaxed);
            42
        });
        assert_eq!(out, 42);
        assert!(ran.load(Ordering::Relaxed));
        assert!(current().is_none(), "no sink leaked onto the thread");
    }

    #[test]
    fn timed_accumulates_into_the_installed_sink() {
        let sink = Arc::new(StreamSubPhaseTimings::default());
        let _g = install(Some(sink.clone()));
        timed(StreamSubPhase::Decompress, || {
            std::thread::sleep(std::time::Duration::from_millis(2));
        });
        assert!(
            sink.nanos(StreamSubPhase::Decompress) > 0,
            "a wrapped op accumulates into its bucket"
        );
        assert_eq!(
            sink.nanos(StreamSubPhase::ColdFault),
            0,
            "an unentered bucket stays zero"
        );
    }

    #[test]
    fn guard_restores_previous_sink_on_drop() {
        assert!(current().is_none());
        {
            let sink = Arc::new(StreamSubPhaseTimings::default());
            let _g = install(Some(sink));
            assert!(current().is_some());
        }
        assert!(
            current().is_none(),
            "dropping the guard uninstalls the sink (no leak across RPCs)"
        );
    }

    #[test]
    fn sink_propagates_across_a_spawned_thread() {
        // Mirrors the scan-thread propagation: capture `current()` on the parent,
        // re-install on the child, and the child's recording lands in the SAME Arc.
        let sink = Arc::new(StreamSubPhaseTimings::default());
        let _g = install(Some(sink.clone()));
        let captured = current();
        std::thread::spawn(move || {
            let _child = install(captured);
            record_nanos(StreamSubPhase::ColdFault, 1234);
        })
        .join()
        .expect("child thread joins");
        assert_eq!(
            sink.nanos(StreamSubPhase::ColdFault),
            1234,
            "the child thread accumulated into the propagated per-request sink"
        );
    }

    #[test]
    fn record_nanos_is_noop_without_a_sink() {
        assert!(current().is_none());
        record_nanos(StreamSubPhase::Merge, 999); // must not panic
        assert!(current().is_none());
    }

    #[test]
    fn sink_active_mirrors_install_and_restores_on_drop() {
        assert!(!sink_active(), "no sink installed at start");
        {
            let sink = Arc::new(StreamSubPhaseTimings::default());
            let _g = install(Some(sink));
            assert!(sink_active(), "sink_active tracks an installed sink");
            {
                // A nested `None` install (a non-flight child capture) flips the
                // fast bool off, then restores the outer `true` on drop.
                let _none = install(None);
                assert!(!sink_active(), "nested None install deactivates");
            }
            assert!(sink_active(), "outer sink restored after nested drop");
        }
        assert!(!sink_active(), "sink_active cleared once the guard drops");
    }

    #[test]
    fn pull_wait_accumulates_on_this_thread() {
        // A fresh thread starts at zero (thread-locals do not leak across
        // threads); the accumulator is a plain running total.
        std::thread::spawn(|| {
            assert_eq!(pull_wait_nanos(), 0);
            add_pull_wait_nanos(100);
            add_pull_wait_nanos(50);
            assert_eq!(
                pull_wait_nanos(),
                150,
                "recv-wait accumulates monotonically"
            );
        })
        .join()
        .expect("thread joins");
    }

    #[test]
    fn add_nanos_saturates_rather_than_wrapping() {
        let sink = StreamSubPhaseTimings::default();
        sink.add_nanos(StreamSubPhase::Merge, u64::MAX);
        sink.add_nanos(StreamSubPhase::Merge, 10);
        assert_eq!(
            sink.nanos(StreamSubPhase::Merge),
            u64::MAX,
            "a second add saturates at u64::MAX instead of wrapping to a tiny value"
        );
    }
}
