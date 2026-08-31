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
        assert_eq!(sink.nanos(other), 0, "only the timed phase accumulates");
        assert!(
            sink.snapshot(other).is_none(),
            "a phase no timed region ever covered must report NOT ENTERED, which is \
             what lets emission report it as ABSENT"
        );
    }
    assert_eq!(
        sink.snapshot(ReadPhase::Decompress),
        Some(sink.nanos(ReadPhase::Decompress)),
        "the timed phase reports as ENTERED, carrying its accumulated duration"
    );
}

#[test]
fn a_phase_that_ran_and_measured_zero_is_not_a_phase_that_never_ran() {
    // The distinction emission depends on (issue #1707, roborev job 145): a duration
    // alone cannot express it, because both cases are 0 nanos. Absence is documented
    // to MEAN "did not run" — no decompress series means an uncompressed SSTable, no
    // merge series means a single generation — so a phase that ran and measured zero
    // must be reportable as a real zero, not as an absence that says the opposite.
    let sink = ReadPhaseTimings::default();
    sink.add_nanos(ReadPhase::Merge, 0);

    assert_eq!(sink.nanos(ReadPhase::Merge), 0);
    assert!(
        sink.snapshot(ReadPhase::Merge) == Some(0),
        "completing a timed region IS the evidence the phase ran, whatever it measured"
    );
    assert!(
        sink.snapshot(ReadPhase::Io).is_none(),
        "a phase nothing timed stays unentered — the two zeros are distinguishable"
    );
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

#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
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

#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
#[test]
fn merge_timing_never_underflows_when_the_wait_exceeds_the_wall_time() {
    // NOTE: this saturates the per-thread `PULL_WAIT_NANOS` accumulator to `u64::MAX`
    // and there is no reset API, so from here on THIS THREAD's recv-wait delta is
    // permanently 0 and every later `timed_merge_excluding_recv_wait` on it charges
    // its full wall time to merge. Harmless because cargo gives each test its own
    // thread and this test asserts last on its own sink — but a case added BELOW that
    // relies on a non-zero wait must not assume it runs on a clean thread.
    let sink = Arc::new(ReadPhaseTimings::default());
    let _g = install(Some(sink.clone()));
    timed_merge_excluding_recv_wait(|| {
        // A wait larger than this step could possibly have taken (a foreign/nested
        // recv attribution). Must clamp to 0, never wrap to ~u64::MAX.
        super::super::stream_subphase::add_pull_wait_nanos(u64::MAX);
    });
    assert_eq!(sink.nanos(ReadPhase::Merge), 0);
    // ...and that saturated 0 is an OBSERVATION, not an absence. This is the exact
    // case that used to be silently dropped: a real multi-generation merge whose
    // producers starved recorded 0 nanos, was skipped by the `nanos > 0` emit gate,
    // and told the operator "single generation" (issue #1707, roborev job 145).
    assert!(
        sink.snapshot(ReadPhase::Merge) == Some(0),
        "a merge step whose recv-wait consumed its whole wall time still RAN; \
         reporting it as absent states there was nothing to merge, which is false"
    );
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
    #[cfg(all(feature = "write-support", not(feature = "tombstones")))]
    timed_merge_excluding_recv_wait(|| runs += 1);
    #[cfg(not(all(feature = "write-support", not(feature = "tombstones"))))]
    {
        runs += 1; // the merge helper is configured out with its only call site
    }
    assert_eq!(runs, 2, "the closure runs on the unmetered fast path too");
}

/// A snapshot taken WHILE a producer is still accumulating must never pair a fresh
/// entry bit with a stale zero counter (issue #1707, roborev job 149).
///
/// # What this can and cannot demonstrate — stated plainly
///
/// The defect is a MEMORY-ORDERING bug: publishing `entered` before the counter let
/// a concurrent reader see `entered == true` beside the counter's old value and emit
/// a fabricated `0.0`. That reordering is architecture-dependent. On x86-64 (TSO)
/// stores are not reordered with stores and loads are not reordered with loads, so
/// this test does NOT go red against the pre-fix code on an x86 host — it was run
/// against the pre-fix ordering and passed. It is red-capable on a weakly-ordered
/// target (aarch64/ppc64), and it is what pins the *observable* invariant: every
/// accumulation here is NON-ZERO, so `Some(0)` from `snapshot` can only mean the
/// reader saw an entry bit whose counter update was not yet visible.
///
/// The ordering itself — which atomic carries the release, which carries the
/// acquire, and in what order each side touches them — is pinned STRUCTURALLY by
/// `the_entry_bit_is_published_after_the_counter_with_release_acquire` below, which
/// IS red against the pre-fix code on every architecture.
#[test]
fn a_concurrent_snapshot_never_sees_an_entry_bit_without_its_duration() {
    use std::sync::atomic::AtomicBool;

    const PRODUCERS: usize = 4;
    const ROUNDS: usize = 2_000;

    for _ in 0..64 {
        let sink = Arc::new(ReadPhaseTimings::default());
        let stop = Arc::new(AtomicBool::new(false));

        let producers: Vec<_> = (0..PRODUCERS)
            .map(|_| {
                let sink = sink.clone();
                std::thread::spawn(move || {
                    for _ in 0..ROUNDS {
                        // Every add is strictly positive, so a `Some(0)` observed by
                        // the reader below is necessarily a torn read, never a real
                        // "ran and measured zero".
                        sink.add_nanos(ReadPhase::Decode, 1);
                    }
                })
            })
            .collect();

        // The reader models `ReadOpMeter::finish()` running on the dropping stream's
        // thread while the detached feed/parse threads keep accumulating — the
        // documented asymmetry in `read_metrics::finish`.
        let reader = {
            let sink = sink.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                // Loops until it has BOTH seen the phase entered and been told to
                // stop: the producers are fast enough to finish before a freshly
                // spawned reader's first iteration, and a reader that exited having
                // observed nothing would assert nothing at all. Termination is
                // guaranteed — `stop` is only set after every producer joined, so
                // the entry bit is set by then and cannot be unset.
                let mut seen_entered = false;
                loop {
                    if let Some(nanos) = sink.snapshot(ReadPhase::Decode) {
                        assert!(
                            nanos > 0,
                            "snapshot observed the entry bit for a phase whose \
                             accumulated duration was not yet visible: emission \
                             would publish a fabricated 0.0 for work that took \
                             time (issue #1707, roborev job 149)"
                        );
                        seen_entered = true;
                    }
                    if seen_entered && stop.load(Ordering::Relaxed) {
                        break;
                    }
                }
                seen_entered
            })
        };

        for t in producers {
            t.join().expect("producer");
        }
        stop.store(true, Ordering::Relaxed);
        let seen_entered = reader.join().expect("reader");

        assert!(
            seen_entered,
            "the reader must actually have observed the phase as entered, or this \
             case asserted nothing"
        );
        assert_eq!(
            sink.snapshot(ReadPhase::Decode),
            Some((PRODUCERS * ROUNDS) as u64),
            "and once quiesced the total is exact"
        );
    }
}

/// The ordering contract itself, pinned structurally because no single-threaded (and
/// on x86 no multi-threaded) execution can observe it (issue #1707, roborev job 149).
///
/// Two halves of ONE synchronizes-with pair, both on `self.entered`:
///
/// * producer — update the COUNTER, then `entered.fetch_or(.., Release)`;
/// * reader — `entered.load(Acquire)`, then read the counter.
///
/// Swap either order, or weaken either ordering, and a concurrent `finish()` can
/// publish a `0.0` for a phase that really took time. That is exactly the
/// fabrication tracking entry separately from duration exists to remove, so the
/// property is asserted against the source rather than left to a reviewer's memory.
#[test]
fn the_entry_bit_is_published_after_the_counter_with_release_acquire() {
    const SRC: &str = include_str!("read_phase.rs");

    let body = |name: &str| -> &'static str {
        let start = SRC
            .find(name)
            .unwrap_or_else(|| panic!("{name} not found in read_phase.rs"));
        let rest = &SRC[start..];
        let end = rest
            .find("\n    }\n")
            .unwrap_or_else(|| panic!("no end of body for {name}"));
        &rest[..end]
    };

    // Producer half.
    let add = body("pub fn add_nanos(");
    let counter_at = add
        .find(".fetch_update(")
        .expect("add_nanos must update the counter");
    let publish_at = add
        .find("self.entered.fetch_or(phase.bit(), Ordering::Release)")
        .expect(
            "add_nanos must publish the entry bit with RELEASE ordering — a Relaxed \
             fetch_or orders nothing, so a reader can see the bit without the \
             counter update that preceded it",
        );
    assert!(
        counter_at < publish_at,
        "the COUNTER must be updated BEFORE the entry bit is published: with the \
         bit first, a concurrent snapshot pairs a fresh entry bit with the old \
         counter and emits a fabricated 0.0"
    );

    // Reader half.
    let snap = body("pub fn snapshot(");
    let acquire_at = snap.find("self.entered.load(Ordering::Acquire)").expect(
        "snapshot must load the entry bit with ACQUIRE ordering — it is the \
             matching half of add_nanos's Release store on the SAME atomic",
    );
    let read_at = snap
        .find("self.nanos(phase)")
        .expect("snapshot must read the counter");
    assert!(
        acquire_at < read_at,
        "the entry bit must be ACQUIRE-loaded BEFORE the counter is read, or the \
         acquire establishes nothing about the counter load that preceded it"
    );
}

/// Emission-level coverage: what the meter actually PUBLISHES for a phase that ran
/// and measured zero (issue #1707, roborev job 145).
///
/// These live in the library's own test build because `ReadOpMeter` is `pub(crate)`
/// — no integration test can construct one, and the case under test (a merge step
/// whose recv-wait subtraction saturates to zero) cannot be provoked deterministically
/// through the public read path. Gated on `observability-testing` because the
/// assertions read the emitted series back through the in-memory capture; the
/// `observability-gate` workflow runs them with a fail-closed zero-match filter, so
/// a rename cannot turn them into a silently empty target.
#[cfg(feature = "observability-testing")]
mod phase_emission_tests {
    use super::*;
    use crate::observability::read_metrics::ReadOpMeter;
    use crate::observability::{catalog, testing};

    /// A phase that RAN and measured zero must publish a `0.0` sample, because
    /// absence is documented to mean "did not run".
    ///
    /// The concrete case: `timed_merge_excluding_recv_wait` saturates to 0 when the
    /// recv-wait it subtracts exceeds the step's wall time (see
    /// `merge_timing_never_underflows_when_the_wait_exceeds_the_wall_time`). Under
    /// the old `nanos > 0` emit gate that real multi-generation merge was SKIPPED,
    /// and the operator doc reads a missing merge series as "single generation" — a
    /// false statement manufactured by the honesty mechanism itself.
    #[test]
    #[serial_test::serial(read_metrics)]
    fn a_phase_that_ran_and_measured_zero_is_published_as_a_zero_sample() {
        let mc = testing::metrics_capture();
        mc.reset();
        {
            let mut meter = ReadOpMeter::start(None);
            let sink = meter
                .phase_sink()
                .expect("an installed capture makes the meter live, not inert");
            // Byte-for-byte what the saturating merge helper records in that case.
            sink.add_nanos(ReadPhase::Merge, 0);
            meter.finish();
        }
        let metrics = mc.flush_and_collect();

        let entry = metrics.find(catalog::READ_PHASE_MERGE).unwrap_or_else(|| {
            panic!(
                "a merge that RAN must publish a sample even when it measured zero — \
                 skipping it tells the operator there was nothing to merge, which is \
                 the opposite of the truth; collected: {:?}",
                metrics
                    .entries()
                    .iter()
                    .map(|m| m.name.as_str())
                    .collect::<Vec<_>>()
            )
        });
        assert_eq!(
            entry
                .points
                .iter()
                .map(|p| p.count.unwrap_or(0))
                .sum::<u64>(),
            1,
            "exactly ONE sample, and its presence — not its value — is what proves \
             the phase ran; points: {:?}",
            entry.points
        );
        assert_eq!(
            entry.points.iter().map(|p| p.value).sum::<f64>(),
            0.0,
            "and its value is an honest zero: the phase ran and measured zero; \
             points: {:?}",
            entry.points
        );
    }

    /// The other half of the same contract: a phase NOTHING ever timed is still
    /// ABSENT. Tracking entry must not turn every scan into four samples — that
    /// would drag the percentiles of every real phase toward zero and destroy the
    /// documented meaning of absence in the other direction.
    #[test]
    #[serial_test::serial(read_metrics)]
    fn a_phase_that_never_ran_is_still_absent() {
        let mc = testing::metrics_capture();
        mc.reset();
        {
            let mut meter = ReadOpMeter::start(None);
            let sink = meter.phase_sink().expect("live meter");
            sink.add_nanos(ReadPhase::Decode, 5_000);
            meter.finish();
        }
        let metrics = mc.flush_and_collect();

        assert!(
            metrics.contains(catalog::READ_PHASE_DECODE),
            "the phase that ran is published; collected: {:?}",
            metrics
                .entries()
                .iter()
                .map(|m| m.name.as_str())
                .collect::<Vec<_>>()
        );
        for absent in [
            catalog::READ_PHASE_IO,
            catalog::READ_PHASE_DECOMPRESS,
            catalog::READ_PHASE_MERGE,
        ] {
            assert!(
                !metrics.contains(absent),
                "{absent} must be ABSENT — no timed region for it ever ran, and \
                 absence is how an operator learns the SSTable was uncompressed / \
                 the scan was single-generation"
            );
        }
    }
}
