//! Issue #2316 — the k-way merge must bound its producer-thread cost to O(M).
//!
//! The write-engine k-way merge (shared by compaction/maintenance and the Flight
//! `do_get` streaming egress) opens one OS producer thread per input SSTable.
//! Before the fix each producer built a full multi-threaded `tokio::runtime::Runtime`,
//! spinning up `num_cpus` worker threads — so ONE merge over `M` inputs cost
//! `~M + M·num_cpus` OS threads. On a many-core node a handful of concurrent Flight
//! queries multiplied that into a context-switch storm well before disk bandwidth
//! saturated (see `openspec/changes/flight-merge-runtime-amplification/`).
//!
//! This test PINS the bound by driving a REAL multi-SSTable merge and observing
//! the process's PEAK OS thread count DIRECTLY — the Linux `/proc/self/task` entry
//! count, or the macOS `proc_pidinfo(PROC_PIDTASKINFO).pti_threadnum` kernel task
//! count. Both are direct kernel observations, never a heuristic inference.
//!
//! The producers block on the bounded streaming channel (`sync_channel`) once it
//! fills, so once the merger is constructed — and BEFORE it is drained — every
//! producer is alive at once and STAYS alive (thread count cannot decrease) until
//! this test starts draining. Rather than sampling over a fixed elapsed-time
//! window (which can under-sample on a contended host where producer startup
//! drifts outside the window — roborev job 1604 finding 1), the peak is captured
//! by polling until the process's OS thread count STABILIZES — the same reading
//! across several consecutive polls — synchronizing on the producer LIFECYCLE
//! itself, never on elapsed time. A generous timeout bounds the poll and FAILS
//! LOUDLY (a clear panic, never a silent under-sample) if the count never
//! settles. The assertion FAILS on the pre-change code (each producer's
//! multi-threaded runtime adds `num_cpus` workers) and PASSES after the
//! `current_thread`-runtime fix — but ONLY on a host with enough cores for the
//! pre-change cost `M·(1 + num_cpus)` to actually EXCEED the `O(M)` bound. That
//! threshold is derived from the constants below (see
//! [`min_cpus_for_amplification`]), NOT assumed: at `M = 4` with the current
//! `PER_INPUT`/`THREAD_SLACK` the pre-change delta is 12 on a 2-core host, which
//! is UNDER the bound of 15 — so `num_cpus >= 2` (the claim this docstring made
//! before issue #3385) was never sufficient to detect the regression. Below the
//! derived threshold, or where the platform exposes no direct thread-count API,
//! the test guards deterministically rather than flake.
//!
//! ## Measured noise mechanism: reapable blocking-pool threads (issue #3385)
//!
//! Under CPU starvation this pin red by exactly one thread in a FULL gate
//! (`delta=16` vs `bound=15`, `peak=18`, `baseline=2`, `num_cpus=16`) while
//! passing 3/3 standalone. Instrumenting the thread NAMES (`/proc/self/task/*/comm`)
//! identified the overshoot precisely — it is NOT runtime workers:
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
//! pool `thread_keep_alive`, so after a hold the delta settles to `M` — the
//! producer threads alone — a `num_cpus`-INDEPENDENT steady state.
//!
//! A genuine #2316 amplification behaves the OPPOSITE way: a multi-threaded
//! `Runtime`'s worker threads live for the LIFETIME of the runtime, and every
//! producer's runtime stays alive while the producer blocks on the full
//! `sync_channel`. So holding the producers past the keep-alive and re-sampling
//! separates the two hypotheses BY MECHANISM, not by a widened tolerance: it can
//! only ever convert a jitter FAIL into a PASS, never mask a real amplification.
//! That confirmation runs ONLY when the fast-path delta already exceeds the
//! bound, so the common (passing) case pays zero extra latency, and the bound
//! itself is NOT weakened.
//!
//! Measured on a contended 16-core host (48 spinners, 5 runs) with CORRECT code,
//! the fast-path delta was 18 / 76 / 41 / 8 / 22 against a bound of 15 — four of
//! five would have redded the old assertion — and EVERY one confirmed at exactly
//! `4` (= `M`). Against that, the RED control (producers restored to a
//! multi-threaded runtime) confirmed at `68`. The two regimes are separated by a
//! gulf after the reap, and are indistinguishable before it: a jitter peak of 76
//! and an amplified peak of 72 differ by nothing meaningful. That is precisely
//! why the peak alone could never pin this property, and why `PER_INPUT` bounds
//! the PERSISTENT count only — see its doc comment for what is consequently NOT
//! pinned here.
//!
//! ## Process-isolation requirement
//!
//! The peak-thread observation is a WHOLE-PROCESS count, not a count scoped to
//! this test's own threads — so it is only meaningful if this test runs ALONE in
//! its process. The gate's default runner is `nextest` (see `accelerators:` in
//! every `AGENT-GATE`/`AGENT-GATE LITE` summary), which isolates every `#[test]`
//! in its OWN process, so this holds unconditionally under the gate. Under plain
//! `cargo test` (no nextest), cargo instead runs every `#[test]` fn WITHIN one
//! compiled test binary CONCURRENTLY, as sibling threads sharing ONE process —
//! any sibling test's threads (its own producer threads, tokio workers, etc.)
//! would inflate the peak sampled here and could false-fail (never false-PASS,
//! since extra threads only push the observed delta up) or otherwise make the
//! bound assertion meaningless. This file is therefore kept to EXACTLY this one
//! `#[test]` function so plain `cargo test --test issue_2316_merge_thread_budget`
//! stays isolated too (one file = one binary = one process; no siblings to race
//! against) — do not add a second `#[test]` fn to this file; add a sibling test
//! FILE instead if another scenario is needed.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::merge::compute_baseline_min;
use cqlite_core::storage::write_engine::{
    CellOperation, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

/// Rows per input SSTable. Must EXCEED the merge's per-channel capacity (up to
/// `STREAMING_CHANNEL_CAPACITY` = 256, adaptively reduced under concurrent merges
/// — #2765) so every producer fills its bounded channel and blocks on `send`
/// before the merge starts draining — guaranteeing all `M` producers are alive at once
/// at the sampling point (deterministic peak, no timing race).
const ROWS_PER_INPUT: i32 = 400;

/// Number of input SSTables (`M`) merged in one pass.
const NUM_INPUTS: usize = 4;

/// Max PERSISTENT OS threads a single fixed (`current_thread`-runtime) producer
/// contributes: the producer thread itself plus headroom (a small constant,
/// INDEPENDENT of `num_cpus`). The pre-change multi-threaded runtime instead
/// added `num_cpus` worker threads PER producer on top of this, and those are
/// NOT reapable, so the `O(M)` bound (coefficient fixed at `PER_INPUT`) rejects
/// it.
///
/// This is deliberately NOT a bound on the momentary PEAK (issue #3385): the
/// `spawn_blocking` pool is demand-driven, so with CORRECT code the measured peak
/// ranged 8 → 76 over five runs on a contended 16-core host while the persistent
/// count stayed at exactly `M`. Any peak allowance wide enough not to flake would
/// be too wide to mean anything, so the peak is reported for DIAGNOSIS and the
/// pin is on the persistent count. Consequence, recorded rather than implied:
/// unbounded TRANSIENT blocking-pool growth is not pinned by this test (roborev
/// job 59 finding 1) — it needs a different oracle, one that observes the pool
/// directly instead of inferring it from a whole-process peak.
const PER_INPUT: usize = 3;

/// Fixed slack over the `PER_INPUT · M` bound for incidental/settle threads.
const THREAD_SLACK: usize = 3;

/// The O(M) bound this test pins for `m` inputs.
fn thread_bound(m: usize) -> usize {
    PER_INPUT * m + THREAD_SLACK
}

/// Smallest `num_cpus` at which the PRE-CHANGE cost is actually DETECTABLE by
/// this bound, derived from the constants rather than assumed (issue #3385).
///
/// The pre-change merge cost `M + M·num_cpus = M·(1 + num_cpus)` threads. The
/// regression is observable only where that EXCEEDS [`thread_bound`]:
/// `M·(1 + c) > PER_INPUT·M + THREAD_SLACK`, i.e. the smallest integer
/// `c > (bound - M)/M`. With the current constants at `M = 4` that is `c >= 3`
/// (pre-change delta 16 vs bound 15) — NOT `c >= 2`, where the pre-change delta
/// is 12 and sits UNDER the bound. Below this threshold the test would hold
/// vacuously either way, so it guards explicitly instead of pretending to pin.
fn min_cpus_for_amplification(m: usize) -> usize {
    if m == 0 {
        return usize::MAX;
    }
    thread_bound(m).saturating_sub(m) / m + 1
}

/// Span over which the OS thread count must be CONTINUOUSLY UNCHANGED before a
/// post-reap reading is accepted as final (issue #3385).
///
/// tokio's default blocking-pool `thread_keep_alive` is **10 s**: an idle
/// `spawn_blocking` thread is reaped that long after it goes idle. This span
/// MUST exceed that.
///
/// Why a QUIESCENCE SPAN and not a fixed sleep: the keep-alive clock starts when
/// a thread goes IDLE, which is NOT when the hold starts. Under starvation a
/// blocking thread can finish its work late into a fixed hold and still be
/// unreaped when the re-sample lands — the re-sample then stabilizes within a few
/// polls and reports jitter as persistent, preserving the very flake this change
/// removes (roborev job 59 finding 2). An unchanged span longer than the
/// keep-alive rules that out: a reap DECREMENTS the count and so resets the span,
/// therefore a span of this length proves no reap occurred within it, and any
/// thread idle at its start would have been reaped inside it. Since the producers
/// are blocked on `send` and submit no new blocking work, every in-flight task
/// must finish, go idle and be reaped — each resetting the span — so the span can
/// only be achieved once reaping has genuinely quiesced.
const REAP_QUIESCENCE_SPAN: Duration = Duration::from_secs(12);

/// Fail-loud bound on the whole reap-confirm wait. Worst case is work finishing
/// late, plus the 10 s keep-alive, plus [`REAP_QUIESCENCE_SPAN`]; this is a
/// generous multiple of that, and is reached ONLY on the overshoot path.
const REAP_CONFIRM_TIMEOUT: Duration = Duration::from_secs(90);

// ── Direct, no-heuristics OS thread-count observation ───────────────────────

/// Count OS threads in the current process by direct kernel observation.
///
/// Returns `None` on a platform that exposes no direct thread-count API (the
/// test then guards rather than assert a bound it cannot measure).
#[cfg(target_os = "linux")]
fn os_thread_count() -> Option<usize> {
    // The number of entries under /proc/self/task IS the live kernel thread count.
    std::fs::read_dir("/proc/self/task")
        .ok()
        .map(|it| it.flatten().count())
}

#[cfg(target_os = "macos")]
fn os_thread_count() -> Option<usize> {
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
fn os_thread_count() -> Option<usize> {
    None
}

/// Number of consecutive identical readings required to treat the OS thread
/// count as STABILIZED (issue #2316, roborev job 1604 finding 1).
const STABLE_STREAK: usize = 8;

/// Delay between polls while waiting for stabilization.
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Poll the process's OS thread count until it STABILIZES — the same reading
/// observed across [`STABLE_STREAK`] consecutive polls — synchronizing on the
/// actual thread LIFECYCLE instead of a fixed elapsed-time window. Thread
/// creation itself is a synchronous kernel operation (a new `/proc/self/task`
/// entry — or macOS `pti_threadnum` increment — appears the instant the OS
/// creates the thread, regardless of scheduling delay), so once every producer
/// this test spawns has been created, the count settles quickly and reliably;
/// this poll simply waits out that settling instead of assuming a fixed window
/// covers it.
///
/// Returns `(peak, settled)`: `peak` is the highest reading observed at ANY
/// point while polling (so a transient spike — e.g. the pre-fix defect's burst
/// of per-producer runtime-worker threads — is captured even if the count later
/// settles lower), while `settled` is the reading that satisfied the
/// stabilization streak.
///
/// `timeout` is a fail-loud BOUND ONLY, never the synchronization mechanism: if
/// the count never stabilizes within it, this panics with a clear diagnostic
/// (producer startup may be stalled under extreme contention) rather than
/// silently returning an under-sampled value.
fn poll_until_stable(timeout: Duration) -> (usize, usize) {
    let deadline = Instant::now() + timeout;
    let mut peak = 0usize;
    let mut last: Option<usize> = None;
    let mut streak = 0usize;
    while Instant::now() < deadline {
        let n = os_thread_count().expect(
            "thread count observation must remain available (guard 2 already confirmed it)",
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
    panic!(
        "OS thread count never stabilized within {timeout:?} (last reading {last:?}, \
         streak {streak}/{STABLE_STREAK} required, peak observed {peak}); this is a \
         fail-loud BOUND, not a synchronization mechanism — producer startup may be \
         stalled under extreme contention, or the lifecycle signal never settles"
    );
}

/// Re-sample the OS thread count after holding the (still-blocked) producers
/// past tokio's blocking-pool keep-alive, so a reading can be CONFIRMED as a
/// PERSISTENT overshoot rather than reaping jitter (issue #3385).
///
/// The discriminator is mechanical, not statistical:
///
/// * `spawn_blocking` pool threads are IDLE-REAPED 10 s after going idle, and a
///   reap changes the thread count — so waiting for the count to hold unchanged
///   for [`REAP_QUIESCENCE_SPAN`] (> that keep-alive) is positive evidence that a
///   starvation-inflated pool has finished draining, rather than an assumption
///   that a fixed hold was long enough (roborev job 59 finding 2).
/// * A multi-threaded `Runtime`'s worker threads are NOT reapable — they live
///   for the lifetime of the runtime, and each producer's runtime stays alive
///   for as long as that producer blocks on the full `sync_channel` (which it
///   does until this test drains the merge, i.e. strictly after this call).
///
/// So this confirmation can only ever turn a jitter FAIL into a PASS; a genuine
/// #2316 amplification survives it unchanged. Returns `(peak, settled)` of the
/// post-reap window; `settled` is the CONFIRMED reading (the stabilized steady
/// state), while `peak` is reported for diagnosis only.
fn reap_settle_and_resample() -> (usize, usize) {
    poll_until_quiescent(REAP_QUIESCENCE_SPAN, REAP_CONFIRM_TIMEOUT)
}

/// Poll until the OS thread count has been CONTINUOUSLY UNCHANGED for
/// `min_span`, returning `(peak, settled)` of the polling window.
///
/// Distinct from [`poll_until_stable`], which accepts a fixed number of
/// consecutive identical readings (~200 ms) — enough to detect that thread
/// CREATION has finished, but not that idle-time-based REAPING has. Reaping
/// resets the span whenever it fires, so a span longer than tokio's keep-alive
/// is positive evidence that no reap is still pending, rather than an assumption
/// that enough time has passed.
///
/// `timeout` is a fail-loud BOUND ONLY, never the synchronization mechanism.
fn poll_until_quiescent(min_span: Duration, timeout: Duration) -> (usize, usize) {
    let deadline = Instant::now() + timeout;
    let mut peak = 0usize;
    let mut last: Option<usize> = None;
    let mut unchanged_since = Instant::now();
    while Instant::now() < deadline {
        let n = os_thread_count().expect(
            "thread count observation must remain available (guard 2 already confirmed it)",
        );
        peak = peak.max(n);
        if last == Some(n) {
            if unchanged_since.elapsed() >= min_span {
                return (peak, n);
            }
        } else {
            last = Some(n);
            unchanged_since = Instant::now();
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    panic!(
        "OS thread count never quiesced within {timeout:?} (last reading {last:?}, unchanged for \
         {:?} of the {min_span:?} required, peak observed {peak}); this is a fail-loud BOUND, not \
         a synchronization mechanism — the blocking pool may still be churning under extreme \
         contention",
        unchanged_since.elapsed()
    );
}

// ── Real multi-SSTable input construction (never an empty dataset) ──────────

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "merge_budget_ks".to_string(),
        table: "items".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_row(id: i32, val: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("merge_budget_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "val".to_string(),
            value: Value::text(val.to_string()),
        }],
        ts,
        None,
    )
}

/// Discover published `nb-*-big-Data.db` files under `dir` (recursively, since the
/// WriteEngine nests them under keyspace/table subdirs), newest-generation first.
fn discover_inputs(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    collect_inputs(dir, &mut found, 8);
    found.sort_by_key(|b| std::cmp::Reverse(b.0));
    found.into_iter().map(|(_, p)| p).collect()
}

fn collect_inputs(dir: &std::path::Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
            let base = name.trim_end_matches("-Data.db");
            if !path.with_file_name(format!("{base}-TOC.txt")).exists() {
                continue;
            }
            let generation = name
                .strip_prefix("nb-")
                .and_then(|s| s.split("-big-").next())
                .and_then(|g| g.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((generation, path));
        } else if depth > 0 && path.is_dir() {
            collect_inputs(&path, out, depth - 1);
        }
    }
}

/// Build `NUM_INPUTS` REAL nb SSTables (each `ROWS_PER_INPUT` live rows over a
/// disjoint partition range) by flushing a `WriteEngine`. Returns them newest-first.
/// Never empty: every input carries real, non-empty partitions the merge scans.
fn build_inputs() -> (TempDir, Vec<PathBuf>, TableSchema) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime");
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("inputs");
    let wal_dir = temp.path().join("wal");
    let schema = make_schema();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");

    for input in 0..NUM_INPUTS {
        let base = input as i32 * ROWS_PER_INPUT;
        for r in 0..ROWS_PER_INPUT {
            let id = base + r;
            engine
                .write(write_row(id, &format!("v-{input}-{r}"), 100 + input as i64))
                .expect("write row");
        }
        rt.block_on(engine.flush())
            .expect("flush")
            .expect("flush info");
    }
    rt.block_on(engine.close()).expect("close engine");

    let inputs = discover_inputs(&data_dir);
    assert!(
        inputs.len() >= NUM_INPUTS,
        "expected >= {NUM_INPUTS} real input SSTables, got {}",
        inputs.len()
    );
    // Drop the driver runtime BEFORE the caller measures the thread baseline so
    // its (now-joined) worker threads are not in the baseline.
    drop(rt);
    (temp, inputs, schema)
}

#[test]
fn merge_bounds_producer_threads_to_o_m() {
    // Guard 1: the pre-change amplification is `M·(1 + num_cpus)`, which only
    // EXCEEDS this test's O(M) bound above a threshold DERIVED from the bound's
    // own constants (issue #3385 — the old `num_cpus < 2` guard was provably
    // wrong: at M=4, num_cpus=2 the pre-change delta is 12, under the bound of
    // 15, so the pin could not have detected the regression there either).
    // Below the threshold the bound cannot distinguish pre- from post-change
    // code, so hold trivially and say why rather than assert a vacuous pass.
    let cpus = num_cpus::get();
    let min_cpus = min_cpus_for_amplification(NUM_INPUTS);
    if cpus < min_cpus {
        eprintln!(
            "[skip] num_cpus={cpus} < {min_cpus} — with M={NUM_INPUTS}, PER_INPUT={PER_INPUT}, \
             THREAD_SLACK={THREAD_SLACK} the pre-change cost M·(1+num_cpus)={} does not exceed \
             the O(M) bound {}, so the #2316 amplification is not observable here; \
             holding trivially",
            NUM_INPUTS * (1 + cpus),
            thread_bound(NUM_INPUTS)
        );
        return;
    }
    // Guard 2: no direct thread-count API on this platform → cannot observe.
    if os_thread_count().is_none() {
        eprintln!("[skip] no direct OS thread-count API on this platform; holding trivially");
        return;
    }

    let (_temp, inputs, schema) = build_inputs();
    let m = inputs.len();
    let out = TempDir::new().expect("out tempdir");

    // Settle the baseline via LIFECYCLE synchronization (issue #2316, roborev job
    // 1604 finding 1): the input-build runtime has been dropped, but its
    // teardown may still be in flight — poll until the process thread count
    // STABILIZES (rather than sleeping a fixed, potentially-too-short duration
    // under contention) so the baseline reflects the genuinely quiesced state.
    // The settled reading (not the transient peak while winding down) is the
    // correct baseline.
    let (_settle_peak, baseline) = poll_until_stable(Duration::from_secs(10));

    // Construct the merger: this spawns all M producer threads. Each producer
    // opens its reader and streams into the bounded channel; with ROWS_PER_INPUT
    // (> the channel capacity, up to 256 and adaptively reduced under concurrent
    // merges — #2765) every producer fills its channel and
    // blocks on `send` — so all M are alive at once and stay alive until `merge()`
    // below drains them. Pre-change: each producer ALSO holds a multi-threaded
    // runtime (num_cpus workers). Post-change: a current_thread runtime, 0 workers.
    let (baseline_ts, baseline_ldt, baseline_ttl) = compute_baseline_min(&inputs);
    let merger = KWayMerger::new(inputs, &schema).expect("KWayMerger::new");

    // Wait for the producer threads to finish starting up and reach their
    // steady (blocked-on-send) state via the SAME lifecycle synchronization
    // (issue #2316, roborev job 1604 finding 1) — never a fixed sampling
    // window, which could miss the true peak if producer startup drifts outside
    // it on a contended host. `peak` also captures any transient spike seen
    // while ramping up (the pre-fix defect: a burst of per-producer runtime
    // worker threads), even if the count later settles slightly lower.
    let (peak, settled) = poll_until_stable(Duration::from_secs(15));
    let delta = peak.saturating_sub(baseline);
    let bound = thread_bound(m);

    eprintln!(
        "[issue-2316] cpus={cpus} M={m} baseline={baseline} peak={peak} settled={settled} \
         delta={delta} bound={bound}"
    );

    // Issue #3385 — CONFIRM an over-bound reading before failing. The fast path
    // above is unchanged and pays ZERO extra latency: a within-bound delta is
    // accepted exactly as before. Only an overshoot takes this branch, and it
    // does so with the producers STILL blocked on `send` (the merge is drained
    // further below), which is what makes the discriminator sound: reapable
    // blocking-pool threads disappear over the hold, while a multi-threaded
    // runtime's workers — the actual #2316 defect — cannot be reaped while their
    // runtime lives. See `reap_settle_and_resample`. The bound is NOT widened.
    let confirmed = if delta > bound {
        eprintln!(
            "[issue-2316-reap] fast-path delta={delta} exceeds bound={bound}; holding the blocked \
             producers until the thread count holds unchanged for {REAP_QUIESCENCE_SPAN:?} \
             (> tokio's 10s blocking-pool thread_keep_alive), i.e. until reaping has quiesced"
        );
        let (reap_peak, reap_settled) = reap_settle_and_resample();
        let reap_delta = reap_settled.saturating_sub(baseline);
        eprintln!(
            "[issue-2316-reap] post-reap peak={reap_peak} settled={reap_settled} \
             confirmed_delta={reap_delta} bound={bound} ({})",
            if reap_delta <= bound {
                "within bound — the overshoot was reapable blocking-pool jitter, absorbed"
            } else {
                "STILL over bound — persistent (non-reapable) threads"
            }
        );
        reap_delta
    } else {
        delta
    };

    // Drain the merge so producers exit cleanly (no leaked threads / temp files).
    let mut writer =
        SSTableWriter::new(out.path().to_path_buf(), 1, &schema).expect("SSTableWriter::new");
    writer.pre_seed_encoding_baselines(baseline_ts, baseline_ldt, baseline_ttl);
    let stats = merger.merge(&mut writer).expect("merge into writer");
    let finish_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("finish runtime");
    finish_rt.block_on(writer.finish()).expect("writer finish");

    // Sanity: the merge actually processed the real rows (never an empty dataset).
    assert!(
        stats.output_rows >= (NUM_INPUTS as u64 * ROWS_PER_INPUT as u64),
        "merge should emit all input rows; got {} (expected >= {})",
        stats.output_rows,
        NUM_INPUTS as u64 * ROWS_PER_INPUT as u64
    );

    // THE PIN: the merge's OS-thread delta over baseline must be within the O(M)
    // bound. Pre-change this is `M + M·num_cpus` (>> bound); post-change `M`.
    // `confirmed` equals the fast-path delta unless that exceeded the bound, in
    // which case it is the delta that SURVIVED the reap settle.
    assert!(
        confirmed <= bound,
        "merge over M={m} inputs holds {confirmed} PERSISTENT OS threads over baseline \
         (fast-path peak={peak}, settled={settled}, delta={delta}; confirmed after the \
         blocking pool quiesced for {REAP_QUIESCENCE_SPAN:?}: {confirmed}; \
         baseline={baseline}); O(M) bound is {bound} (= {PER_INPUT}·M + {THREAD_SLACK}). \
         These threads outlived tokio's blocking-pool keep-alive while every producer was \
         still blocked on `send`, so they are NOT reapable spawn_blocking jitter — they are \
         held by something with the producers' lifetime. A confirmed delta >= num_cpus \
         (num_cpus={cpus}) is consistent with the #2316 amplification (a producer building a \
         multi-threaded runtime, whose workers live as long as the runtime); a smaller \
         confirmed delta is a persistent overshoot of another origin — either way it is a \
         real, non-transient cost this pin rejects. (confirmed >= num_cpus: {})",
        confirmed >= cpus
    );
}
