//! Unit tests for the per-scan read-phase accumulator (issue #1707), in a sibling
//! file so `read_phase.rs` stays inside the campsite-rule source target (#1116).

use super::*;
use std::sync::atomic::AtomicBool;

#[test]
fn timed_is_noop_when_no_sink_installed() {
    assert!(current().is_none());
    let ran = AtomicBool::new(false);
    let out = timed(ReadPhase::Io, || {
        ran.store(true, Ordering::Relaxed);
        7
    });
    assert_eq!(out, 7);
    assert!(ran.load(Ordering::Relaxed));
    assert!(current().is_none(), "no sink leaked onto the thread");
}

#[test]
fn timed_accumulates_into_the_installed_bucket_only() {
    let sink = Arc::new(ReadPhaseTimings::default());
    let _g = install(Some(sink.clone()));
    timed(ReadPhase::Decompress, || {
        std::thread::sleep(std::time::Duration::from_millis(2));
    });
    assert!(sink.nanos(ReadPhase::Decompress) > 0);
    for other in [ReadPhase::Io, ReadPhase::Decode, ReadPhase::Merge] {
        assert_eq!(
            sink.nanos(other),
            0,
            "an unentered phase stays zero, so an absent phase can be reported as \
             ABSENT rather than as a fabricated 0.0 sample"
        );
    }
}

#[test]
fn guard_restores_the_previous_sink_on_drop() {
    assert!(current().is_none());
    {
        let _g = install(Some(Arc::new(ReadPhaseTimings::default())));
        assert!(current().is_some());
        assert!(sink_active());
        {
            let _nested = install(None);
            assert!(!sink_active(), "a nested None install deactivates");
        }
        assert!(sink_active(), "the outer sink is restored");
    }
    assert!(
        current().is_none() && !sink_active(),
        "dropping the guard uninstalls the sink (no leak across scans)"
    );
}

#[test]
fn sink_propagates_across_a_spawned_thread() {
    // The exact shape every spawn site uses: capture on the parent, install on the
    // child, and the child's recording lands in the SAME Arc the meter will read.
    let sink = Arc::new(ReadPhaseTimings::default());
    let _g = install(Some(sink.clone()));
    let captured = current();
    std::thread::spawn(move || {
        let _child = install(captured);
        timed(ReadPhase::Io, || {
            std::thread::sleep(std::time::Duration::from_millis(2))
        });
    })
    .join()
    .expect("child thread joins");
    assert!(
        sink.nanos(ReadPhase::Io) > 0,
        "the child thread's io must land in the SAME Arc the parent's meter reads"
    );
}

#[test]
fn scoped_is_a_noop_without_a_sink() {
    assert!(current().is_none());
    assert!(
        scoped(ReadPhase::Merge).is_none(),
        "no sink means no timer is even constructed (no Instant::now)"
    );
}

#[test]
fn a_timer_records_into_the_sink_it_captured_not_the_one_installed_at_drop() {
    // `ReadPhaseTimer` binds the `Arc` at CONSTRUCTION and never re-reads the
    // thread-local at drop. That is what keeps it correct when it is held across an
    // `.await` that resumes the future on a DIFFERENT executor thread — a thread
    // that may have no sink installed, or another scan's.
    let sink = Arc::new(ReadPhaseTimings::default());
    let guard = install(Some(sink.clone()));
    let timer = scoped(ReadPhase::Decode);
    assert!(timer.is_some(), "a metered thread builds a timer");

    // Uninstall the sink while the timer is still alive: a drop-time thread-local
    // read would now find nothing and silently lose the measurement.
    drop(guard);
    assert!(current().is_none());
    std::thread::sleep(std::time::Duration::from_millis(2));
    drop(timer);

    assert!(
        sink.nanos(ReadPhase::Decode) > 0,
        "the timer must record into the sink it CAPTURED, not into whatever is \
         installed when it happens to drop"
    );
}

#[test]
fn add_nanos_saturates_rather_than_wrapping() {
    let sink = ReadPhaseTimings::default();
    sink.add_nanos(ReadPhase::Decode, u64::MAX);
    sink.add_nanos(ReadPhase::Decode, 10);
    assert_eq!(
        sink.nanos(ReadPhase::Decode),
        u64::MAX,
        "a saturating add keeps a huge total huge; a wrap would read as 'fast'"
    );
}

#[cfg(not(feature = "tombstones"))]
#[test]
fn merge_timing_subtracts_the_recv_wait_accrued_inside_it() {
    // The property: producer starvation (blocking recv on the merge inputs) is NOT
    // charged to merge CPU. Asserted structurally — the recorded merge time must be
    // strictly LESS than the wall time of a step that spent most of it waiting —
    // never against a wall-clock threshold (#2642).
    let sink = Arc::new(ReadPhaseTimings::default());
    let _g = install(Some(sink.clone()));
    let wall = std::time::Instant::now();
    timed_merge_excluding_recv_wait(|| {
        // Stand in for the recv site, which records its own wait (the SAME
        // accumulator the #2819 flight sub-phases use).
        let waited = std::time::Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(20));
        super::super::stream_subphase::add_pull_wait_nanos(
            super::super::stream_subphase::elapsed_nanos(waited),
        );
    });
    let wall = elapsed_nanos(wall);
    let merge = sink.nanos(ReadPhase::Merge);
    assert!(
        merge < wall,
        "recv-wait must be excluded: merge={merge}ns is not below the step's own \
         wall time {wall}ns"
    );
}

#[cfg(not(feature = "tombstones"))]
#[test]
fn merge_timing_never_underflows_when_the_wait_exceeds_the_wall_time() {
    let sink = Arc::new(ReadPhaseTimings::default());
    let _g = install(Some(sink.clone()));
    timed_merge_excluding_recv_wait(|| {
        // A wait larger than this step could possibly have taken (a foreign/nested
        // recv attribution). Must clamp to 0, never wrap to ~u64::MAX.
        super::super::stream_subphase::add_pull_wait_nanos(u64::MAX);
    });
    assert_eq!(sink.nanos(ReadPhase::Merge), 0);
}

#[test]
fn an_unmetered_seam_never_builds_a_timer() {
    // The zero-cost-when-off promise at the seams: with no sink installed, the io
    // seam's `scoped` must return `None` (so no `Instant::now()` and no `Arc` clone
    // happen), and `timed` must still run its closure exactly once.
    assert!(!sink_active());
    assert!(scoped(ReadPhase::Io).is_none());
    let mut runs = 0;
    timed(ReadPhase::Decode, || runs += 1);
    #[cfg(not(feature = "tombstones"))]
    timed_merge_excluding_recv_wait(|| runs += 1);
    #[cfg(feature = "tombstones")]
    {
        runs += 1; // the merge helper is configured out with its only call site
    }
    assert_eq!(runs, 2, "the closure runs on the unmetered fast path too");
}
