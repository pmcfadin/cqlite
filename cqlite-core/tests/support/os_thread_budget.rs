//! Shared OS-thread-budget observation oracle for the merge producer-thread pins
//! (issues #2316 and #2370; extracted on #3438/#3514).
//!
//! # Why this module exists
//!
//! Two sibling pins observe the SAME property by the SAME mechanism:
//!
//! * `issue_2316_merge_thread_budget.rs` — ONE k-way merge, bound `O(M)`.
//! * `issue_2370_concurrent_merge_thread_budget.rs` — `C` concurrent merges,
//!   bound `O(C·M)`.
//!
//! Both need three pieces, and #3385 established the correct shape of all three
//! while de-flaking only the first. #2370 then red in the field with the same
//! mechanism (deltas 31/37/39/43 over a bound of 24 on a starved host, while the
//! identical tree passed 6/6 standalone on a quiet box and passed full gates on
//! two other hosts) — the sibling had the fix and could not share it. Copying it
//! would have produced a second divergent copy of a subtle oracle, so the three
//! pieces live here ONCE:
//!
//! 1. [`os_thread_count`] — direct kernel observation of the live thread count.
//! 2. [`poll_until_stable`] — lifecycle-synchronized peak/settled sampling.
//! 3. [`poll_until_reaped`] / [`ReapOutcome`] — AFFIRMATIVE post-reap acceptance.
//!
//! Plus [`min_cpus_for_amplification`] (the vacuity guard, derived from each
//! caller's own constants) and [`CpuPressureWindow`] (a purely DIAGNOSTIC PSI
//! reading, so a future red is classifiable on sight).
//!
//! # Measured noise mechanism: reapable blocking-pool threads (issue #3385)
//!
//! Under CPU starvation the #2316 pin red by exactly one thread in a FULL gate
//! (`delta=16` vs `bound=15`, `peak=18`, `baseline=2`, `num_cpus=16`) while
//! passing 3/3 standalone. Instrumenting the thread NAMES
//! (`/proc/self/task/*/comm`) identified the overshoot precisely — it is NOT
//! runtime workers:
//!
//! ```text
//! [issue-2316] cpus=16 M=4 baseline=2 peak=10 settled=10 delta=8 bound=15
//! census   {"issue_2316_merg": 1, "merge_bounds_pr": 5, "tokio-rt-worker": 4}
//! after a 13s hold: peak2=6 settled2=6 delta2=4
//! census2  {"issue_2316_merg": 1, "merge_bounds_pr": 5}   # ZERO tokio threads
//! ```
//!
//! Each producer builds a `current_thread` runtime (ZERO workers) plus
//! demand-driven `spawn_blocking` threads (named `tokio-rt-worker` by tokio),
//! whose pool GROWS under starvation (measured 3/producer in the contended gate
//! vs 1/producer idle). Those threads are REAPED once idle past tokio's blocking
//! pool `thread_keep_alive`, so after a hold the delta settles to the producer
//! count alone — a `num_cpus`-INDEPENDENT steady state.
//!
//! A genuine amplification behaves the OPPOSITE way: a multi-threaded `Runtime`'s
//! worker threads live for the LIFETIME of the runtime, and every producer's
//! runtime stays alive while the producer blocks on its full `sync_channel`. So
//! holding the producers past the keep-alive and re-sampling separates the two
//! hypotheses BY MECHANISM, not by a widened tolerance: it can only ever convert
//! a jitter FAIL into a PASS, never mask a real amplification. The bound itself
//! is NEVER widened to swallow starvation deltas — those measure the host, not
//! the code under test.
//!
//! # ACCEPTED RESIDUAL — what these pins do NOT cover (#3438 item 3, decided)
//!
//! **Unbounded TRANSIENT blocking-pool growth is pinned NOWHERE, by either
//! caller, and this module deliberately does not pretend otherwise.**
//!
//! Both pins assert the PERSISTENT (post-reap-confirmed) thread count and report
//! the momentary PEAK as diagnostic only. That is forced, not lazy: with CORRECT
//! code the measured peak ranged **8 → 76** over five runs on a contended 16-core
//! host while the persistent count stayed at exactly the producer count, so any
//! peak allowance wide enough not to flake is far too wide to mean anything. A
//! jitter peak of 76 and an amplified peak of 72 differ by nothing.
//!
//! Concretely NOT DETECTED by either caller today:
//!
//! * a regression that CONSTRUCTS and DROPS a per-core runtime (or grows and then
//!   releases a large `spawn_blocking` pool) per producer, per input, or per
//!   merge — the threads exist only transiently, so the reap confirmation
//!   legitimately drains them and both pins pass;
//! * any bound on the RATE of thread creation (thrash), as opposed to the
//!   population at a sampled instant;
//! * `spawn_blocking` pool sizing itself (`max_blocking_threads`), which is a
//!   per-runtime property these tests only ever see through a whole-process
//!   aggregate.
//!
//! The reason is instrumental, not a tuning question: **the whole-process peak is
//! the wrong instrument for a transient, by construction** — it cannot attribute
//! a thread to a pool, cannot see a thread that was created and joined between
//! two polls, and is perturbed by every unrelated thread in the process. Closing
//! this gap needs a DIFFERENT oracle that observes the pool directly (e.g. a
//! tokio `RuntimeMetrics::num_blocking_threads` sample taken from inside each
//! producer's own runtime, or a `/proc/self/task` census keyed on thread NAME
//! with per-name high-water marks, correlated with runtime construction events) —
//! a new instrument, not a re-tuned threshold. It is recorded here as an
//! **explicitly accepted residual** rather than implemented, and was accepted by
//! the owner on #3438 rather than being an oversight. Do not "fix" it by widening
//! a peak allowance in either caller; that reintroduces the flake and still would
//! not detect the transient.
//!
//! ## SECOND ACCEPTED RESIDUAL — the sample point has no producer-readiness
//! barrier, so a REGRESSION can be missed under starvation (#3603 owns the fix)
//!
//! Same instrument, same section, deliberately: this is the other face of "the
//! whole-process count cannot see what happened between two polls".
//!
//! [`poll_until_stable`] treats **200 ms** of unchanged thread count (8 polls ×
//! 25 ms) as evidence that the producers have reached their steady state. They
//! have not necessarily: `KWayMerger::new` spawns each producer thread and returns
//! with NO barrier, and each producer builds its own Tokio runtime *inside* the
//! spawned thread — i.e. strictly after the count already includes that thread.
//! So a pre-#2316 amplified worker pool is created LATER than the point at which
//! the count is stable at `baseline + producers`. If that first stable window
//! elapses before any producer has built its runtime, the fast path samples a
//! within-bound delta, the reap confirmation is SKIPPED, and the amplification
//! goes undetected.
//!
//! Three facts bound how much this matters, and all three are load-bearing:
//!
//! * **Only the FIRST stable window is exposed.** The poll returns on the first
//!   completed streak, so the race is a 200 ms window at the start of sampling —
//!   not the whole 15 s/20 s budget.
//! * **It needs ALL `M` (or `C·M`) producers unscheduled for 200 CONSECUTIVE ms.**
//!   Partial construction is harmless: a rising count RESETS the streak, the poll
//!   keeps going, and the run lands in the confirm branch — i.e. behaves correctly.
//! * **The direction is FALSE-NEGATIVE ONLY.** It can lose coverage for one run;
//!   it can never fail a healthy one. Nothing here is a flake source.
//!
//! **It is PRE-EXISTING on `main`, verbatim** — see
//! `origin/main:cqlite-core/tests/issue_2316_merge_thread_budget.rs:264-291` for
//! the identical function and `:568/:577/:594` for the identical
//! construct-then-sample-then-`if delta > bound` structure. `main`'s #2370 is
//! strictly worse: it ends in a bare `delta <= bound` with no confirm branch to
//! skip at all. This module did not introduce the gap; it centralized the oracle,
//! which is what made the gap visible. **Do not re-triage it as a #3514
//! regression.**
//!
//! **No sound TEST-ONLY fix exists**, which is why it is recorded rather than
//! closed here. Four candidates were evaluated and rejected, one line each so
//! nobody re-derives them:
//!
//! 1. **A longer stable span** (e.g. 2 s instead of 200 ms) makes the race rarer
//!    by ~10× and closes nothing — a rarer defect is not a fix.
//! 2. **A lifetime high-water sampler** spanning construction→drain would see the
//!    pool, but the confirmation can only run while the producers are still BLOCKED
//!    (after the drain they are gone), and a bound on the lifetime peak is exactly
//!    what the FIRST residual above rules out (correct code peaked 8→76 vs an
//!    amplified ~96 — the ranges overlap).
//! 3. **Waiting for `>= baseline + 2·producers`** (assuming one blocking thread per
//!    producer) infers behaviour from tokio's internal thread population — a
//!    no-heuristics violation that would degrade into a hang-then-panic flake on a
//!    tokio bump.
//! 4. **Polling the existing `cqlite.merge.producer_threads` gauge** cannot work
//!    even if it were reachable (`producer_gauge` is a private `mod` at
//!    `write_engine/merge/mod.rs`, everything `pub(super)`): its `spawned()`
//!    increment happens deliberately BEFORE the `std::thread::Builder::spawn`, for
//!    gauge-pairing correctness, so it is already satisfied at the sample point and
//!    signals nothing about readiness. This is the least discoverable of the four.
//!
//! The fix therefore needs a real readiness signal in `cqlite-core/src` (a
//! ready-counter in `producer_gauge`, incremented once each producer's runtime
//! exists, awaited by the test before sampling) — production code added for a
//! test's benefit, which needs its own justification and review rather than a
//! round-3 expansion of a test-only change. **#3603 owns it**, and must first
//! settle the question this module deliberately does not guess: whether tokio
//! spawns multi-thread worker threads EAGERLY in `Builder::build()` (in which case
//! a post-`build()` signal is a sound barrier) or lazily on first poll (in which
//! case it is not, and the signal must move).

#![allow(dead_code)] // shared module: not every consumer uses every item.

use std::time::{Duration, Instant};

// ── Direct, no-heuristics OS thread-count observation ───────────────────────

/// Count OS threads in the current process by direct kernel observation.
///
/// Returns `None` on a platform that exposes no direct thread-count API (callers
/// then guard rather than assert a bound they cannot measure).
#[cfg(target_os = "linux")]
pub fn os_thread_count() -> Option<usize> {
    // The number of entries under /proc/self/task IS the live kernel thread count.
    std::fs::read_dir("/proc/self/task")
        .ok()
        .map(|it| it.flatten().count())
}

#[cfg(target_os = "macos")]
pub fn os_thread_count() -> Option<usize> {
    // proc_pidinfo(PROC_PIDTASKINFO).pti_threadnum is the kernel's live task
    // (thread) count for the process — a direct observation, not an estimate.
    let pid = unsafe { libc::getpid() };
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    let ret = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTASKINFO,
            0,
            &mut info as *mut libc::proc_taskinfo as *mut libc::c_void,
            size,
        )
    };
    if ret == size {
        Some(info.pti_threadnum as usize)
    } else {
        None
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn os_thread_count() -> Option<usize> {
    None
}

/// Number of consecutive identical readings required to treat the OS thread
/// count as STABILIZED (issue #2316, roborev job 1604 finding 1).
pub const STABLE_STREAK: usize = 8;

/// Delay between polls while waiting for stabilization.
pub const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Poll the process's OS thread count until it STABILIZES — the same reading
/// observed across [`STABLE_STREAK`] consecutive polls — synchronizing on the
/// actual thread LIFECYCLE instead of a fixed elapsed-time window. Thread
/// creation itself is a synchronous kernel operation (a new `/proc/self/task`
/// entry — or macOS `pti_threadnum` increment — appears the instant the OS
/// creates the thread, regardless of scheduling delay), so once every producer
/// has been created the count settles quickly and reliably; this poll simply
/// waits out that settling instead of assuming a fixed window covers it.
///
/// Returns `(peak, settled)`: `peak` is the highest reading observed at ANY
/// point while polling (so a transient spike — e.g. a defect's burst of
/// per-producer runtime-worker threads — is captured even if the count later
/// settles lower), while `settled` is the reading that satisfied the
/// stabilization streak.
///
/// `timeout` is a fail-loud BOUND ONLY, never the synchronization mechanism: if
/// the count never stabilizes within it, this panics with a clear diagnostic
/// rather than silently returning an under-sampled value.
///
/// ## ACCEPTED RESIDUAL — this timeout has NO confirmation path (#3514 nit 5)
///
/// [`poll_until_reaped`] was given an affirmative/asymmetric confirmation because
/// its subject is a starvation-SENSITIVE VALUE (a thread population compared
/// against a bound). This function's subject is different in kind — a LIFECYCLE
/// SETTLING — and it is left as a bare fail-loud panic deliberately, not as an
/// unswept remnant. Recorded so the next person need not re-derive the risk:
///
/// * **The bar is tiny relative to the budget.** Success needs
///   [`STABLE_STREAK`] (8) consecutive identical readings at
///   [`POLL_INTERVAL`] (25 ms) — a **200 ms** window — inside a caller budget of
///   10 s (baseline) or 15–20 s (peak): **50–100× headroom**. Starvation does not
///   change the thread POPULATION, only when this thread is scheduled to read it,
///   and thread creation is synchronous in the kernel (the `/proc/self/task` entry
///   exists the instant the thread does). So a spurious panic needs the count to
///   keep CHANGING for the whole budget, not merely for this thread to run late.
/// * **It is materially safer than what #3514 removed.** The old failure was a
///   peak VALUE assert whose red was textually indistinguishable from a real
///   regression — that is what cost the field investigation. A panic here says
///   `never stabilized`, carries the streak, the last reading and (below) the PSI
///   stall percentage, so it is classifiable on sight and cannot be mistaken for
///   an amplification.
/// * **What would settle it, since nothing here does:** a starvation repro — N
///   CPU spinners at 2–4× the core count — measuring whether 8 consecutive 25 ms
///   polls ever fail to land inside the 10 s/20 s budgets. Until someone runs
///   that, the 200 ms-bar argument above is reasoning, not measurement. If such a
///   run ever produces a `never stabilized` panic, the fix is a confirmation path
///   shaped like [`poll_until_reaped`]'s (patient before failing), NOT a longer
///   fixed timeout.
pub fn poll_until_stable(timeout: Duration) -> (usize, usize) {
    // Annotate a would-be panic with host CPU stall, so a spurious one is
    // classifiable without a re-run (#3514 nit 5). Diagnostic only; one /proc read.
    let pressure = open_cpu_pressure_window();
    let deadline = Instant::now() + timeout;
    let mut peak = 0usize;
    let mut last: Option<usize> = None;
    let mut streak = 0usize;
    while Instant::now() < deadline {
        let n = os_thread_count().expect(
            "thread count observation must remain available (the platform guard already \
             confirmed it)",
        );
        peak = peak.max(n);
        if last == Some(n) {
            streak += 1;
            if streak >= STABLE_STREAK {
                return (peak, n);
            }
        } else {
            last = Some(n);
            streak = 1;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    let pressure_note = pressure.report();
    panic!(
        "OS thread count never stabilized within {timeout:?} (last reading {last:?}, \
         streak {streak}/{STABLE_STREAK} required, peak observed {peak}); this is a \
         fail-loud BOUND, not a synchronization mechanism — producer startup may be \
         stalled under extreme contention, or the lifecycle signal never settles. \
         This is NOT a thread-budget regression: no bound was compared. Host CPU-stall \
         context, DIAGNOSTIC ONLY: {pressure_note}"
    );
}

// ── Vacuity guard: is the regression observable on THIS host at all? ─────────

/// Smallest `num_cpus` at which the PRE-CHANGE cost is actually DETECTABLE by a
/// caller's bound, derived from the caller's own constants rather than assumed
/// (issue #3385, generalized on #3438).
///
/// `producers` is the number of OS producer threads the scenario starts (`M` for
/// a single merge, `C·M` for `C` concurrent merges); `bound` is the caller's
/// thread bound for that scenario.
///
/// The pre-change merge built one multi-threaded runtime PER PRODUCER, costing
/// `producers · (1 + num_cpus)` threads. The regression is observable only where
/// that STRICTLY EXCEEDS `bound`:
///
/// ```text
/// producers · (1 + c) > bound
///   ⟺ 1 + c > bound / producers
///   ⟺ 1 + c ≥ floor(bound / producers) + 1
///   ⟺ c     ≥ floor(bound / producers)
/// ```
///
/// so the threshold is exactly integer division `bound / producers`. Worked for
/// both callers:
///
/// * #2316: `producers = M = 4`, `bound = 3·4 + 3 = 15` → `15 / 4 = 3`. At
///   `c = 2` the pre-change cost is `4·3 = 12`, UNDER 15 — so that file's
///   original `num_cpus >= 2` guard was never sufficient.
/// * #2370: `producers = C·M = 2·3 = 6`, `bound = 3·3·2 + 6 = 24` → `24 / 6 = 4`.
///   At `c = 3` the pre-change cost is `6·4 = 24`, which EQUALS the bound and so
///   does not exceed it — meaning on a 2-core or 3-core host that pin passed
///   VACUOUSLY against the very defect it exists to catch, and its hardcoded
///   `num_cpus < 2` guard hid that.
///
/// Below the returned threshold a caller must guard explicitly instead of
/// asserting a bound that holds either way.
pub fn min_cpus_for_amplification(producers: usize, bound: usize) -> usize {
    if producers == 0 {
        // No producers means no amplification at any core count.
        return usize::MAX;
    }
    bound / producers
}

// ── Affirmative post-reap acceptance ────────────────────────────────────────

/// tokio's documented default blocking-pool `thread_keep_alive`: an idle
/// `spawn_blocking` thread is reaped this long after it goes idle.
pub const TOKIO_BLOCKING_KEEP_ALIVE: Duration = Duration::from_secs(10);

/// Span over which the OS thread count must be CONTINUOUSLY UNCHANGED before a
/// post-reap reading is accepted as final (issue #3385).
///
/// It MUST exceed [`TOKIO_BLOCKING_KEEP_ALIVE`], and is deliberately INDEPENDENT
/// of the producer count: the span's job is to prove that no reap happened
/// within one keep-alive period, which is a property of tokio's timer, not of
/// how many producers are waiting. Making it longer buys nothing (a reap
/// DECREMENTS the count and so RESETS the span, which is what actually
/// serializes late finishers — see [`reap_confirm_timeout`], which is the knob
/// that scales).
///
/// Why a QUIESCENCE SPAN and not a fixed sleep: the keep-alive clock starts when
/// a thread goes IDLE, which is NOT when the hold starts. Under starvation a
/// blocking thread can finish its work late into a fixed hold and still be
/// unreaped when the re-sample lands — the re-sample then stabilizes within a few
/// polls and reports jitter as persistent, preserving the very flake this exists
/// to remove (roborev job 59 finding 2). An unchanged span longer than the
/// keep-alive rules that out: a reap resets the span, therefore a span of this
/// length proves no reap occurred within it, and any thread idle at its start
/// would have been reaped inside it. Since the producers are blocked on `send`
/// and submit no new blocking work, every in-flight task must finish, go idle and
/// be reaped — each resetting the span — so the span can only be achieved once
/// reaping has genuinely quiesced.
pub const REAP_QUIESCENCE_SPAN: Duration = Duration::from_secs(12);

/// Fail-loud bound on the whole reap-confirm wait, SCALED by the producer count.
///
/// ## What the scaling is derived from
///
/// The worst case is not one keep-alive: it is late-finishing blocking work
/// arriving SERIALLY under starvation, because each finish-then-reap DECREMENTS
/// the count and so RESETS [`REAP_QUIESCENCE_SPAN`]. The number of such resets is
/// bounded by the number of producers that can still hold in-flight blocking
/// work — `M` for one merge, `C·M` for `C` concurrent merges. So the budget must
/// be LINEAR IN THE PRODUCER COUNT, which is exactly the axis #2370 differs from
/// #2316 on (6 producers vs 4), and is why #2316's constant could not simply be
/// copied.
///
/// The per-producer coefficient is anchored on #2316's shipped-and-measured
/// figure rather than invented: that pin ships `60 s` for its `M = 4` producers,
/// i.e. **15 s per producer** — one [`TOKIO_BLOCKING_KEEP_ALIVE`] (10 s) plus 5 s
/// of late-finish slack each. Holding that coefficient fixed reproduces #2316's
/// 60 s EXACTLY (no behaviour change there) and yields **90 s** at #2370's
/// `C·M = 6`.
///
/// The floor keeps a tiny-producer-count scenario sane: one keep-alive plus one
/// full quiescence span is the minimum in which ANY drain can be observed.
///
/// This budget is reached ONLY when the count is STILL over bound — i.e. only on
/// the path that is about to fail the pin anyway — so the common (passing) case
/// pays zero extra latency.
///
/// ## Harness-budget coupling, QUANTIFIED (#3514 nit 4)
///
/// `.config/nextest.toml` hard-kills a test at `slow-timeout` 60 s × 4 = **240 s**.
/// If a caller ever breaches that, a genuine FAIL presents as a hung-test kill and
/// NONE of the assert diagnostics above are printed — the worst possible failure
/// mode for this pin, so the margin is measured rather than asserted. Two of the
/// three costs are HARD-CAPPED by construction (they are `poll_until_stable`
/// fail-loud bounds, not elapsed work), and the third is measured:
///
/// ```text
/// #2370 (C·M = 6 producers), worst case:
///   baseline poll   ≤  10 s   (hard cap)
///   peak poll       ≤  20 s   (hard cap)
///   confirm         =  90 s   (hard cap, this function)
///   build + 2 drains ≈  4.9 s (MEASURED, see below)
///   ────────────────────────
///   total          ≈ 124.9 s  against 240 s  →  115.1 s margin
/// ```
///
/// The `build + drains` figure is measured, not estimated: a full RED run (producers
/// restored to a multi-threaded runtime, so the 90 s confirm is spent in full) took
/// **93.9 s** wall on a 16-core host and **94.9 s** on the same host restricted to 4
/// cores with `taskset` — i.e. 3.9 s and 4.9 s respectively outside the confirm, and
/// a **4× core cut cost only ~24%**. For the non-capped work to consume the 115 s of
/// remaining margin it would have to inflate **~24×**, against that measured 1.24×
/// at a 4× core cut. Capacity, not comfort. #2316 (M = 4, 60 s confirm) is measured
/// at 65.4 s total, i.e. ~175 s of margin, and both binaries are additionally
/// enrolled in nextest's `max-threads = 1` `timing` group (#3514) so they never
/// co-schedule with each other or with the suite's two heaviest CPU consumers.
///
/// A future caller with many more producers MUST redo this arithmetic: the confirm
/// term grows linearly in the producer count, so at ~14 producers (210 s) the budget
/// alone exceeds the harness kill. At that point raise nextest's `terminate-after`
/// for that binary — do not silently shrink the confirm, which would reintroduce the
/// premature condemnation this whole mechanism exists to prevent.
pub fn reap_confirm_timeout(producers: usize) -> Duration {
    /// Per-producer budget: one blocking-pool keep-alive (10 s) plus 5 s of
    /// late-finish slack. See the derivation above.
    const PER_PRODUCER_SECS: u64 = 15;
    let scaled = Duration::from_secs(PER_PRODUCER_SECS.saturating_mul(producers as u64));
    let floor = TOKIO_BLOCKING_KEEP_ALIVE + REAP_QUIESCENCE_SPAN;
    if scaled > floor {
        scaled
    } else {
        floor
    }
}

/// Outcome of the reap confirmation. Deliberately NOT a bare count: acceptance
/// must be an AFFIRMATIVE measurement, never the absence of a bad one (roborev
/// job 61). An earlier version returned the latest reading on timeout, so a count
/// that merely DIPPED within budget as the deadline passed — without ever holding
/// there — was indistinguishable from a genuine drain, and passed the pin.
pub enum ReapOutcome {
    /// The pool drained to within budget AND held there for the quiescence span.
    /// This is the ONLY value that may satisfy a pin.
    Drained { peak: usize, settled: usize },
    /// The deadline expired without such a reading. A pin FAILS on this
    /// regardless of what the instantaneous count happened to be.
    Unconfirmed { peak: usize, last: usize },
}

/// Poll until the blocking pool has demonstrably drained to `accept_at_or_below`
/// and held there for `min_span`, or until `timeout` expires.
///
/// The discriminator is mechanical, not statistical:
///
/// * `spawn_blocking` pool threads are IDLE-REAPED after
///   [`TOKIO_BLOCKING_KEEP_ALIVE`], so a starvation-inflated pool DRAINS on its
///   own while the producers stay blocked on `send`.
/// * A multi-threaded `Runtime`'s worker threads are NOT reapable — they live for
///   the lifetime of the runtime, and each producer's runtime stays alive for as
///   long as that producer blocks on its full `sync_channel` (which it does until
///   the caller drains the merge, i.e. strictly after this call).
///
/// ## Why acceptance and failure are deliberately ASYMMETRIC
///
/// An unchanged thread count does NOT prove no blocking task is still running: a
/// busy or parked task keeps the count constant without ever having started its
/// keep-alive clock (roborev job 60). So quiescence is sufficient to ACCEPT a
/// clean reading but NOT to condemn a dirty one — if it were used for both, a
/// task that finishes late would be reaped after the re-sample and its thread
/// miscounted as persistent, which is the false failure this removes.
///
/// This function therefore keeps waiting for as long as the count is over the
/// threshold, spending the full `timeout` before giving up. That is sound rather
/// than "retry until green": the only threads that can disappear during the wait
/// are REAPABLE ones, and an amplification's runtime workers are not reapable
/// while their runtime lives. No amount of extra patience can make an
/// amplification pass; it can only give late-finishing blocking work the reap
/// window it is owed.
///
/// Timing out yields [`ReapOutcome::Unconfirmed`], which FAILS the pin. Failing
/// closed is the right direction: an unconfirmable measurement is not evidence of
/// good behaviour.
pub fn poll_until_reaped(
    accept_at_or_below: usize,
    min_span: Duration,
    timeout: Duration,
) -> ReapOutcome {
    let deadline = Instant::now() + timeout;
    let mut peak = 0usize;
    let mut last: Option<usize> = None;
    let mut unchanged_since = Instant::now();
    let mut latest = 0usize;
    while Instant::now() < deadline {
        let n = os_thread_count().expect(
            "thread count observation must remain available (the platform guard already \
             confirmed it)",
        );
        peak = peak.max(n);
        latest = n;
        if last == Some(n) {
            // Accept ONLY a reading that is both within budget AND has HELD there
            // for the full span. Either half alone is not a confirmation.
            if n <= accept_at_or_below && unchanged_since.elapsed() >= min_span {
                return ReapOutcome::Drained { peak, settled: n };
            }
        } else {
            last = Some(n);
            // ORDERING SWEEP (#3514 r2), recorded so nobody "fixes" it the wrong
            // way: this timestamp is taken AFTER the observation that reset the
            // span, so `elapsed()` UNDER-reports the true unchanged span by the
            // read latency. That skew makes acceptance HARDER (a bit more than
            // `min_span` is required), which is the fail-closed direction — the
            // opposite of the PSI window, where an analogous inversion would
            // over-report. Moving this capture before the read would shorten the
            // required hold under starvation, i.e. weaken the confirmation.
            unchanged_since = Instant::now();
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    ReapOutcome::Unconfirmed { peak, last: latest }
}

// ── PSI: a DIAGNOSTIC-ONLY starvation reading ───────────────────────────────

/// Path to the kernel's CPU pressure-stall information.
const PSI_CPU_PATH: &str = "/proc/pressure/cpu";

/// Read the monotonic `some` CPU-pressure total, in microseconds.
///
/// The `some` line's `total=` field is a monotonically increasing microsecond
/// counter of time during which AT LEAST ONE runnable task was stalled waiting
/// for CPU. Two readings therefore bracket a window; the DIFFERENCE is the
/// stalled time within it (an instantaneous read of a lifetime counter would
/// answer a different question).
///
/// Returns `Err` with a specific reason on any platform/kernel/container where
/// the file is absent, unreadable, or not in the expected shape. An unmeasurable
/// value is NOT the value zero, so callers must propagate the reason rather than
/// substitute `0`.
fn read_cpu_pressure_some_total() -> Result<u64, String> {
    let text = std::fs::read_to_string(PSI_CPU_PATH)
        .map_err(|e| format!("{PSI_CPU_PATH} unreadable: {e}"))?;
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        if fields.next() != Some("some") {
            continue;
        }
        for field in fields {
            if let Some(v) = field.strip_prefix("total=") {
                return v
                    .parse::<u64>()
                    .map_err(|e| format!("{PSI_CPU_PATH} 'some' total={v:?} unparseable: {e}"));
            }
        }
        return Err(format!(
            "{PSI_CPU_PATH} 'some' line carries no total= field"
        ));
    }
    Err(format!("{PSI_CPU_PATH} carries no 'some' line"))
}

/// A bracketed CPU-pressure observation over a measured window.
///
/// ## Purely informational — NEVER an input to pass/fail
///
/// This exists so that a future red is classifiable as *starved host* vs *real
/// regression* on sight, from the panic message alone, without re-running
/// anything. It deliberately does NOT gate, skip, widen, or otherwise influence
/// the assertion: a load/PSI precondition that skips above a threshold would make
/// the pin unrun on exactly the hosts where it reds, which is the vacuous-guard
/// failure these pins exist to avoid.
///
/// Correspondingly, NO threshold is encoded here and none should be. The field
/// evidence behind #3514 is `n = 1` on the failing side; a number embedded now
/// would read as established when it is not. Report the percentage, let a human
/// judge it.
///
/// ## THE NESTING INVARIANT — state it once, enforce it at BOTH ends (#3514 r2)
///
/// The reported percentage is `stall_delta / wall_delta`, i.e. a ratio of two
/// intervals measured by two different clocks (the kernel's PSI counter and this
/// process's `Instant`). It is meaningful only if:
///
/// ```text
///   [ wall interval ]  ⊇  [ PSI interval ]        (strict superset, both ends)
///
///   Instant::now()  ──►  read PSI(start)   ...   read PSI(end)  ──►  elapsed()
///   ^ wall opens FIRST      ^ numerator opens      ^ numerator closes   ^ wall closes LAST
/// ```
///
/// Because PSI's `some total=` advances by at most one microsecond per
/// microsecond of wall time, that nesting is exactly what makes the ratio
/// **≤ 100%**. Violating it at EITHER end lets the numerator span an interval the
/// denominator does not cover, and the excess is reported as a percentage above
/// 100% — a number the module's own contract says is impossible.
///
/// **Both ends have now been wrong, one per review round, and that is the point
/// of writing the invariant down here rather than a fix at each site.** Round 1
/// fixed the CLOSING end (the closing PSI read happened AFTER `elapsed()`);
/// round 2 found the OPENING end had the identical asymmetry (`start` was read
/// before `opened` was captured) and had survived precisely because the previous
/// fix was applied to the reported instance instead of to the invariant. Do not
/// fix one end again: if you touch either read, re-check both against the diagram
/// above.
///
/// Third layer, because two orderings in two functions are exactly the kind of
/// thing a later refactor reorders: [`describe_stall`] REJECTS an inverted window
/// at the point the ratio is FORMED. So an ordering regression at either end can
/// only ever degrade to `UNMEASURED`, never print a false percentage — and that
/// enforcement is unit-testable, whereas an ordering is not.
pub struct CpuPressureWindow {
    /// Opening PSI reading — the numerator's start. Read AFTER `opened` (see the
    /// nesting invariant on this struct).
    start: Result<u64, String>,
    /// Wall-clock anchor — the denominator's start. Captured BEFORE `start`.
    opened: Instant,
}

/// Open a CPU-pressure window. Call [`CpuPressureWindow::report`] at any later
/// point to describe the window from here to now; it may be called more than
/// once (e.g. once after the peak sample and again after a reap confirmation).
pub fn open_cpu_pressure_window() -> CpuPressureWindow {
    // ORDER MATTERS, and this is the OPENING half of the nesting invariant stated
    // on `CpuPressureWindow` (#3514 r2). Take the wall anchor FIRST, then read the
    // PSI counter: any scheduling delay (or the /proc read itself blocking, which
    // on a starved host is precisely when it does) then lands INSIDE the wall
    // interval instead of outside it. Written as two statements rather than a
    // struct literal because Rust evaluates field initialisers in written order,
    // so the ordering would be silently reversible by a field reorder — which is
    // how it was wrong.
    let opened = Instant::now();
    let start = read_cpu_pressure_some_total();
    CpuPressureWindow { start, opened }
}

impl CpuPressureWindow {
    /// A one-line, human-readable description of CPU stall over this window,
    /// normalised by wall time.
    ///
    /// Degrades to an explicit `UNMEASURED (<reason>)` — never `0%` — wherever
    /// PSI is unavailable (non-Linux, a kernel built without
    /// `CONFIG_PSI`/booted `psi=0`, or a restricted container).
    pub fn report(&self) -> String {
        let started = match &self.start {
            Ok(v) => *v,
            Err(why) => {
                return format!(
                    "cpu-pressure: UNMEASURED at window open ({why}) — an unmeasurable \
                     value is not 0%"
                )
            }
        };
        // ORDER MATTERS (#3514 nit 3): read the CLOSING counter FIRST, then take the
        // wall denominator. The other order closes the denominator interval BEFORE
        // the numerator's, so the numerator spans a strictly LONGER interval and the
        // percentage is biased high — and the bias is worst exactly when it matters,
        // because on a fully-stalled host the closing /proc read is itself what
        // blocks. Taken this way the denominator interval is a superset of the
        // numerator's, so the ratio cannot exceed 100%.
        let ended = match read_cpu_pressure_some_total() {
            Ok(v) => v,
            Err(why) => {
                return format!(
                    "cpu-pressure: UNMEASURED at window close ({why}; it WAS readable at \
                     open) — an unmeasurable value is not 0%"
                )
            }
        };
        let wall_micros = self.opened.elapsed().as_micros();
        describe_stall(started, ended, wall_micros)
    }
}

/// The PURE half of [`CpuPressureWindow::report`]: turn two bracketing `some total=`
/// readings and a wall-time denominator into the diagnostic line.
///
/// Split out so every branch is unit-testable without a real starved host — see
/// `cqlite-core/tests/issue_3514_psi_report_contract.rs`. Both callers' own files
/// hold exactly one `#[test]` each by design (whole-process thread counting needs
/// process isolation), so this contract could not otherwise be tested at all.
pub fn describe_stall(started: u64, ended: u64, wall_micros: u128) -> String {
    // A BACKWARDS counter is UNMEASURABLE, never 0% (#3514 blocker 1). The `some
    // total=` field is monotonic only within one PSI accounting domain: a cgroup
    // or namespace change between the two reads, or a PSI reset, can move it
    // backwards. A `saturating_sub` would render that as "0.0% stall" — printed on
    // a genuinely starved host, i.e. this diagnostic would actively MISCLASSIFY
    // the one red it exists to classify, in the worst direction. The rule is this
    // module's own, stated at `read_cpu_pressure_some_total`: an unmeasurable value
    // is not the value zero.
    let Some(stalled) = ended.checked_sub(started) else {
        return format!(
            "cpu-pressure: UNMEASURED (counter went BACKWARDS: 'some total=' read \
             {started}us at window open and {ended}us at close, so the two readings \
             are not from one monotonic accounting domain — a cgroup/namespace move \
             or a PSI reset between them will do this). Reporting 0% here would print \
             'no stall' on a possibly-starved host: an unmeasurable value is not 0%"
        );
    };
    if wall_micros == 0 {
        return "cpu-pressure: UNMEASURED (zero-width window; nothing to normalise by) \
                — an unmeasurable value is not 0%"
            .to_string();
    }
    let stalled_micros = u128::from(stalled);
    // THIRD LAYER of the nesting invariant (see `CpuPressureWindow`): the ratio is
    // formed HERE, so the impossibility is checked HERE (#3514 r2). Under a correct
    // nesting `stalled <= wall`, so an excess means the two intervals were not
    // nested — an ordering regression at either end, or two clocks that do not
    // relate — and the measurement is therefore VOID.
    //
    // REJECT, do not CAP. Clamping to "100.0%" would put a plausible-looking number
    // where an impossible measurement was, which is the SAME mistake as blocker 1's
    // `0.0%` for a backwards counter: the next person reads a value and trusts it.
    // Every other unmeasurable state in this module reports UNMEASURED with a cause,
    // and so does this one.
    if stalled_micros > wall_micros {
        return format!(
            "cpu-pressure: UNMEASURED (window INVERTED: {stalled_micros}us of stall over only \
             {wall_micros}us of wall, which is impossible under the nesting invariant — the PSI \
             interval must be contained in the wall interval at BOTH ends; suspect an ordering \
             regression in `open_cpu_pressure_window` or `CpuPressureWindow::report`). Capping \
             this to 100% would report a plausible number for an impossible measurement — an \
             unmeasurable value is not a value"
        );
    }
    let pct = (stalled_micros as f64) * 100.0 / (wall_micros as f64);
    format!(
        "cpu-pressure: some-stall {pct:.1}% of wall ({stalled_micros}us stalled over \
         {wall_micros}us, from the monotonic 'some total=' counter in {PSI_CPU_PATH}) \
         — DIAGNOSTIC ONLY: never an input to pass/fail, and no threshold is implied \
         (the #3514 field evidence is n=1 on the failing side)"
    )
}
