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
//! the process's PEAK OS thread count DIRECTLY — a kernel observation, never a
//! heuristic inference. The observation oracle itself (direct thread count,
//! lifecycle-synchronized peak sampling, and the AFFIRMATIVE post-reap
//! acceptance that de-flakes it) is SHARED with the concurrent sibling
//! `issue_2370_concurrent_merge_thread_budget.rs` and lives in
//! `support/os_thread_budget.rs` (extracted on #3438/#3514). Read that module's
//! doc comment for WHY each piece is shaped the way it is — including the
//! measured reapable-blocking-pool noise mechanism, and the ACCEPTED RESIDUAL
//! recording what these pins do NOT cover.
//!
//! The producers block on the bounded streaming channel (`sync_channel`) once it
//! fills, so once the merger is constructed — and BEFORE it is drained — every
//! producer is alive at once and STAYS alive (thread count cannot decrease) until
//! this test starts draining. The peak is captured by
//! [`os_thread_budget::poll_until_stable`], synchronizing on the producer
//! LIFECYCLE rather than a fixed elapsed-time window (which can under-sample on a
//! contended host where producer startup drifts outside the window — roborev job
//! 1604 finding 1).
//!
//! The assertion FAILS on the pre-change code (each producer's multi-threaded
//! runtime adds `num_cpus` workers) and PASSES after the `current_thread`-runtime
//! fix — but ONLY on a host with enough cores for the pre-change cost
//! `M·(1 + num_cpus)` to actually EXCEED the `O(M)` bound. That threshold is
//! DERIVED from the constants below via
//! [`os_thread_budget::min_cpus_for_amplification`], NOT assumed: at `M = 4` with
//! the current `PER_INPUT`/`THREAD_SLACK` the pre-change delta is 12 on a 2-core
//! host, which is UNDER the bound of 15 — so `num_cpus >= 2` (the claim this
//! docstring made before issue #3385) was never sufficient to detect the
//! regression. Below the derived threshold, or where the platform exposes no
//! direct thread-count API, the test guards deterministically rather than flake.
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
use std::time::Duration;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::merge::compute_baseline_min;
use cqlite_core::storage::write_engine::{
    CellOperation, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

/// The SHARED thread-budget observation oracle, used identically by
/// `issue_2370_concurrent_merge_thread_budget.rs` (#3438/#3514). Never copy a
/// piece of it back in here: a second divergent copy of this oracle is exactly
/// what let #2370 red in the field with the fix already sitting in its sibling.
#[path = "support/os_thread_budget.rs"]
mod os_thread_budget;
use os_thread_budget::{
    min_cpus_for_amplification, open_cpu_pressure_window, os_thread_count, poll_until_reaped,
    poll_until_stable, reap_confirm_timeout, ReapOutcome, REAP_QUIESCENCE_SPAN,
};

/// Rows per input SSTable so every producer parks in `send` — the shared
/// derivation in `support/egress_backpressure.rs` (issue #2820 review round 2;
/// six verbatim copies of this sum reintroduced, one level up, exactly the
/// drift the probe exists to prevent).
#[path = "support/egress_backpressure.rs"]
mod egress_backpressure;
use egress_backpressure::rows_that_park_the_producer as rows_per_input;

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
/// pin is on the persistent count. The consequent gap — unbounded TRANSIENT
/// blocking-pool growth is pinned by neither this test nor #2370 — is recorded as
/// an ACCEPTED RESIDUAL in `support/os_thread_budget.rs` (#3438 item 3), which
/// names what a different oracle would have to observe instead.
const PER_INPUT: usize = 3;

/// Fixed slack over the `PER_INPUT · M` bound for incidental/settle threads.
const THREAD_SLACK: usize = 3;

/// The O(M) bound this test pins for `m` inputs.
fn thread_bound(m: usize) -> usize {
    PER_INPUT * m + THREAD_SLACK
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

/// Build `NUM_INPUTS` REAL nb SSTables (each `rows_per_input()` live rows over a
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
        let base = input as i32 * rows_per_input();
        for r in 0..rows_per_input() {
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
    // Guard A (cheap, platform): no direct thread-count API here → cannot observe.
    // Ordered FIRST because it needs no inputs; the vacuity guard deliberately does
    // not run until the scenario size is KNOWN (see Guard B).
    if os_thread_count().is_none() {
        eprintln!("[skip] no direct OS thread-count API on this platform; holding trivially");
        return;
    }

    let cpus = num_cpus::get();

    let (_temp, inputs, schema) = build_inputs();
    let out = TempDir::new().expect("out tempdir");

    // ── THE SINGLE SOURCE OF SCENARIO SIZE (#3514 blocker 2) ────────────────────
    // `m` is the DISCOVERED input count, and here `m` == the producer count. Every
    // quantity the THREAD-BUDGET PIN depends on — the bound, the reap-confirm budget
    // and the vacuity threshold — is derived from THIS ONE binding; nothing on that
    // path may re-derive any of them from the `NUM_INPUTS` constant.
    //
    // ONE quantity deliberately still uses the constant, and it is not on that path:
    // the row-count sanity assert after the drain (`NUM_INPUTS * rows_per_input()`) is
    // a FLOOR on rows actually merged, guarding against an empty dataset. Since
    // `m >= NUM_INPUTS` the constant makes it conservative, so it stays correct while
    // `m` varies and decides nothing about which host can observe the amplification. `build_inputs` asserts
    // only `>= NUM_INPUTS`, so a constant-derived guard beside an `m`-derived bound
    // can disagree about which host can observe the defect, which SKIPS the pin on a
    // host where the amplification is plainly detectable. Deriving both from one
    // binding makes that divergence UNREPRESENTABLE rather than merely forbidden (an
    // `assert!(len() == NUM_INPUTS)` would also close it, but by adding one more
    // thing that must be true — and a false FAIL if an extra generation is ever
    // legitimately published). Same fix, same reasoning, as the #2370 sibling.
    let m = inputs.len();
    let bound = thread_bound(m);

    // Guard B (vacuity, DERIVED — never a literal): the pre-change amplification is
    // `m·(1 + num_cpus)`, which EXCEEDS this test's O(M) bound only at
    // `num_cpus >= bound / m` (issue #3385 — the old `num_cpus < 2` guard was
    // provably wrong: at M=4, num_cpus=2 the pre-change delta is 12, under the bound
    // of 15, so the pin could not have detected the regression there either). Below
    // the threshold the bound cannot distinguish pre- from post-change code, so hold
    // trivially and say why rather than assert a vacuous pass.
    let min_cpus = min_cpus_for_amplification(m, bound);
    if cpus < min_cpus {
        eprintln!(
            "[skip] num_cpus={cpus} < {min_cpus} — with M={m}, PER_INPUT={PER_INPUT}, \
             THREAD_SLACK={THREAD_SLACK} the pre-change cost M·(1+num_cpus)={} does not exceed \
             the O(M) bound {bound}, so the #2316 amplification is not observable here; \
             holding trivially",
            m * (1 + cpus),
        );
        return;
    }

    // The reap-confirm budget SCALES with the producer count (`m` here) — see
    // `os_thread_budget::reap_confirm_timeout` for the derivation and its quantified
    // harness-budget note.
    let confirm_timeout = reap_confirm_timeout(m);

    // Settle the baseline via LIFECYCLE synchronization (issue #2316, roborev job
    // 1604 finding 1): the input-build runtime has been dropped, but its
    // teardown may still be in flight — poll until the process thread count
    // STABILIZES (rather than sleeping a fixed, potentially-too-short duration
    // under contention) so the baseline reflects the genuinely quiesced state.
    // The settled reading (not the transient peak while winding down) is the
    // correct baseline.
    let (_settle_peak, baseline) = poll_until_stable(Duration::from_secs(10));

    // Open the DIAGNOSTIC-ONLY CPU-pressure window over the measured region, so a
    // future red is classifiable as starved-host vs real-regression from the panic
    // message alone. It never influences the verdict (#3514).
    let pressure = open_cpu_pressure_window();

    // Construct the merger: this spawns all M producer threads. Each producer
    // opens its reader and streams into the bounded channel; with `rows_per_input()`
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
    // `bound` is the single-source value derived above from `m` — never re-derived
    // here, so it cannot drift from the vacuity guard.
    let pressure_at_peak = pressure.report();

    eprintln!(
        "[issue-2316] cpus={cpus} M={m} baseline={baseline} peak={peak} settled={settled} \
         delta={delta} bound={bound}"
    );
    eprintln!("[issue-2316] {pressure_at_peak}");

    // Issue #3385 — CONFIRM an over-bound reading before failing. The fast path
    // above is unchanged and pays ZERO extra latency: a within-bound delta is
    // accepted exactly as before. Only an overshoot takes this branch, and it
    // does so with the producers STILL blocked on `send` (the merge is drained
    // further below), which is what makes the discriminator sound: reapable
    // blocking-pool threads disappear over the hold, while a multi-threaded
    // runtime's workers — the actual #2316 defect — cannot be reaped while their
    // runtime lives. See `os_thread_budget::poll_until_reaped`. The bound is NOT
    // widened.
    let mut pressure_at_confirm = None;
    let (pin_satisfied, confirm_note) = if delta > bound {
        eprintln!(
            "[issue-2316-reap] fast-path delta={delta} exceeds bound={bound}; holding the blocked \
             producers until the pool drains within budget and holds there for \
             {REAP_QUIESCENCE_SPAN:?} (> tokio's 10s blocking-pool thread_keep_alive), or up to \
             {confirm_timeout:?} before condemning the reading"
        );
        let outcome = poll_until_reaped(baseline + bound, REAP_QUIESCENCE_SPAN, confirm_timeout);
        let report = pressure.report();
        eprintln!("[issue-2316-reap] {report}");
        pressure_at_confirm = Some(report);
        match outcome {
            ReapOutcome::Drained { peak: rp, settled } => {
                let d = settled.saturating_sub(baseline);
                eprintln!(
                    "[issue-2316-reap] post-reap peak={rp} settled={settled} confirmed_delta={d} \
                     bound={bound} (within bound and HELD for {REAP_QUIESCENCE_SPAN:?} — the \
                     overshoot was reapable blocking-pool jitter, absorbed)"
                );
                (true, format!("drained to {d} within bound"))
            }
            ReapOutcome::Unconfirmed { peak: rp, last } => {
                let d = last.saturating_sub(baseline);
                eprintln!(
                    "[issue-2316-reap] post-reap peak={rp} last={last} last_delta={d} \
                     bound={bound} (UNCONFIRMED after {confirm_timeout:?} — never held \
                     within budget for {REAP_QUIESCENCE_SPAN:?})"
                );
                (
                    false,
                    format!(
                        "UNCONFIRMED after {confirm_timeout:?}: last reading {last} \
                         (delta {d}), peak {rp} — the pool never drained to within budget and \
                         HELD there for {REAP_QUIESCENCE_SPAN:?}"
                    ),
                )
            }
        }
    } else {
        (true, format!("fast-path delta {delta} within bound"))
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
        stats.output_rows >= (NUM_INPUTS as u64 * rows_per_input() as u64),
        "merge should emit all input rows; got {} (expected >= {})",
        stats.output_rows,
        NUM_INPUTS as u64 * rows_per_input() as u64
    );

    // THE PIN: the merge's OS-thread delta over baseline must be within the O(M)
    // bound. Pre-change this is `M + M·num_cpus` (>> bound); post-change `M`.
    // `pin_satisfied` is true only when the fast-path delta was already within
    // bound, or the reap confirmation returned `Drained` — never merely because
    // nothing bad was observed. `confirm_note` records which, for the message.
    let pressure_note = match &pressure_at_confirm {
        Some(confirm) => format!("{pressure_at_peak} | over the confirm window too: {confirm}"),
        None => pressure_at_peak.clone(),
    };
    assert!(
        pin_satisfied,
        "merge over M={m} inputs failed the O(M) producer-thread pin: {confirm_note}. \
         (fast-path peak={peak}, settled={settled}, delta={delta}, baseline={baseline}; \
         O(M) bound is {bound} = {PER_INPUT}·M + {THREAD_SLACK}.) \
         The confirmation holds every producer blocked on `send` and waits up to \
         {confirm_timeout:?} for tokio's blocking pool to drain, so threads still present \
         at the end are NOT reapable spawn_blocking jitter — they are held by something with the \
         producers' lifetime. A surviving delta >= num_cpus (num_cpus={cpus}) is consistent with \
         the #2316 amplification (a producer building a multi-threaded runtime, whose workers \
         live as long as the runtime); a smaller one is a persistent overshoot of another origin. \
         An UNCONFIRMED outcome means the measurement itself never settled — that FAILS closed, \
         because an unconfirmable reading is not evidence of good behaviour. \
         Host CPU-stall context, DIAGNOSTIC ONLY (it did not affect this verdict): {pressure_note}"
    );
}
