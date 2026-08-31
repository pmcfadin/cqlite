//! Issue #2370 — the AGGREGATE thread budget: with ≥2 k-way merges live at once
//! the process OS-thread count must stay O(C·M), not collapse.
//!
//! `issue_2316_merge_thread_budget.rs` pins ONE merge's internal fan-out to O(M).
//! The field failure (#2316 thread collapse, #2361 stall) was CONCURRENCY: several
//! Flight `do_get`s — each a k-way merge — running at once. Before the #2316 fix
//! each producer built a full multi-threaded `tokio::runtime::Runtime`
//! (`num_cpus` workers), so `C` concurrent merges over `M` inputs each cost
//! `~C·M·num_cpus` OS threads — a context-switch storm well before disk
//! saturated. This test drives `C ≥ 2` REAL merges SIMULTANEOUSLY (all producers
//! alive, parked on their bounded channels) and pins the process's OS-thread
//! delta to the O(C·M) bound via the same direct kernel observation the #2316
//! test uses — now literally the same code, from `support/os_thread_budget.rs`.
//!
//! ## Why the raw PEAK was the wrong instrument (issues #3438 / #3514)
//!
//! This pin originally asserted the whole-process PEAK against the bound with NO
//! reap confirmation, and red in the field with deltas of 31 / 37 / 39 / 43 over
//! a bound of 24 — while the IDENTICAL tree passed 6/6 standalone on a quiet box
//! and passed full gates on two other hosts. Those deltas measure DELAYED THREAD
//! REAPING UNDER CPU STARVATION, not concurrency: a genuine #2316 amplification
//! on a 16-core host would land near `C·M·num_cpus` (~96), not at `bound + 5..19`.
//! The steady-state peak genuinely fits under 24.
//!
//! The fix is the one #3385 established for the sibling, applied here and SHARED
//! rather than copied: when the fast-path reading exceeds the bound, hold the
//! (still-blocked) producers and poll until the thread count has drained to
//! within budget AND HELD there for a quiescence span longer than tokio's
//! blocking-pool `thread_keep_alive`, accepting only an AFFIRMATIVE
//! [`os_thread_budget::ReapOutcome::Drained`]. A timeout is `Unconfirmed` and
//! FAILS. The bound is NOT widened, no load/PSI precondition skips the test, and
//! the confirmation can only convert reap JITTER into a pass — an amplification's
//! runtime workers are not reapable while their runtime lives, and every
//! producer's runtime lives until this test drains its merger. See that module's
//! doc comment for the measured mechanism and the ACCEPTED RESIDUAL (#3438 item
//! 3) recording what these pins do NOT cover.
//!
//! ## Process-isolation requirement (same as #2316)
//!
//! The peak-thread observation is WHOLE-PROCESS, so it is only meaningful if this
//! test runs ALONE in its process. Under nextest (the gate default) every
//! `#[test]` is its own process. Under plain `cargo test` all `#[test]`s in one
//! binary run as sibling threads in ONE process, inflating the sampled peak — so
//! this file holds EXACTLY ONE `#[test]`. Do not add a second; add a sibling FILE.

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

/// The SHARED thread-budget observation oracle — the ONE copy, also used by
/// `issue_2316_merge_thread_budget.rs` (#3438/#3514). Never copy a piece of it
/// back in here: a second divergent copy is exactly what let this pin red in the
/// field with the fix already sitting in its sibling.
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

/// Number of input SSTables (`M`) merged per merger.
const NUM_INPUTS: usize = 3;

/// Number of CONCURRENT mergers (`C`) — the aggregate case (≥2, the field shape).
const NUM_MERGERS: usize = 2;

/// Max PERSISTENT OS threads a single fixed (`current_thread`-runtime) producer
/// contributes: the producer thread itself plus headroom, INDEPENDENT of
/// `num_cpus`. The pre-#2316 multi-threaded runtime added `num_cpus` worker
/// threads PER producer on top of this, and those are NOT reapable, so the
/// O(C·M) bound below rejects it.
///
/// As in #2316 this is deliberately NOT a bound on the momentary PEAK (#3438):
/// the `spawn_blocking` pool is demand-driven and inflates under starvation, so
/// the peak is reported for DIAGNOSIS and the pin is on the reap-confirmed
/// count. The consequent gap — unbounded TRANSIENT blocking-pool growth is
/// pinned by neither this test nor #2316 — is recorded as an ACCEPTED RESIDUAL in
/// `support/os_thread_budget.rs`.
const PER_INPUT: usize = 3;

/// Fixed slack over the `PER_INPUT · M · C` bound for incidental/settle threads.
const THREAD_SLACK: usize = 6;

/// The O(C·M) bound this test pins for `m` inputs across `c` concurrent mergers.
fn thread_bound(m: usize, c: usize) -> usize {
    PER_INPUT * m * c + THREAD_SLACK
}

// ── Real multi-SSTable input construction (never an empty dataset) ──────────

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "conc_merge_ks".to_string(),
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
        TableId::new("conc_merge_ks", "items"),
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

/// Build `NUM_INPUTS` REAL nb SSTables (disjoint partition ranges) by flushing a
/// `WriteEngine`. Returns them newest-first; never empty.
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
    // Drop the driver runtime BEFORE the caller measures baseline so its
    // (now-joined) worker threads are not counted in the baseline.
    drop(rt);
    (temp, inputs, schema)
}

#[test]
fn concurrent_merges_bound_aggregate_threads_to_o_c_m() {
    // Guard A (cheap, platform): no direct thread-count API here → cannot observe.
    // Ordered FIRST because it needs no inputs; the vacuity guard deliberately does
    // not run until the scenario size is KNOWN (see Guard B).
    if os_thread_count().is_none() {
        eprintln!("[skip] no direct OS thread-count API on this platform; holding trivially");
        return;
    }

    let cpus = num_cpus::get();
    let c = NUM_MERGERS;

    let (_temp, inputs, schema) = build_inputs();

    // ── THE SINGLE SOURCE OF SCENARIO SIZE (#3514 blocker 2) ────────────────────
    // `m` is the DISCOVERED input count, and every quantity the THREAD-BUDGET PIN
    // depends on is derived from THIS ONE binding: the producer count, the asserted
    // bound, the reap-confirm budget and the vacuity threshold. Nothing on that path
    // may re-derive any of them from the `NUM_INPUTS` constant — that divergence is
    // the defect this binding exists to remove.
    //
    // ONE quantity deliberately still uses the constant, and it is not on that path:
    // the row-count sanity assert after the drain (`NUM_INPUTS * rows_per_input()`) is
    // a FLOOR on rows actually merged, guarding against an empty dataset. Since
    // `m >= NUM_INPUTS`, the constant makes it conservative — a larger `m` can only
    // exceed it — so it stays correct while `m` varies, and it decides nothing about
    // which host can observe the amplification.
    //
    // Why this is structural and not merely tidier: `build_inputs` asserts only
    // `inputs.len() >= NUM_INPUTS`, so `m` CAN exceed the constant if a flush ever
    // publishes an extra `nb-` SSTable. Previously the guard used `NUM_INPUTS` while
    // the bound used `m`, and the two then disagree about which host can observe the
    // defect: at `m=4, c=2` the real producer count is 8 and the real bound 30, whose
    // true threshold is `floor(30/8) = 3` — but a constant-derived guard computes
    // `floor(24/6) = 4` and SKIPS on a 3-core host where the amplification (`8·4 = 32
    // > 30`) is plainly observable. That is a second path to exactly the vacuity AC2
    // exists to remove, which is how #3385 fixing an instance left #3514's class open.
    //
    // An `assert!(inputs.len() == NUM_INPUTS)` would also close it, but only by
    // FORBIDDING the divergence — one more thing to be true, and a false FAIL if the
    // write engine ever legitimately publishes an extra generation. Deriving both
    // from one binding makes divergence UNREPRESENTABLE instead, which is the
    // stronger property, so that is what is done here.
    let m = inputs.len();
    let producers = m * c;
    let bound = thread_bound(m, c);

    // Guard B (vacuity, DERIVED — never a literal; #3438 AC2). The pre-#2316 cost is
    // `producers·(1 + num_cpus)`, and the regression is observable only where that
    // STRICTLY EXCEEDS `bound`, i.e. at `num_cpus >= bound / producers`. At the
    // nominal C=2, M=3 (producers=6, bound=24) that is `24 / 6 = 4`: at num_cpus=3
    // the pre-change cost is `6·4 = 24`, which EQUALS the bound and so does not
    // exceed it. The previous hardcoded `num_cpus < 2` let this pin pass VACUOUSLY
    // against the very defect it exists to catch on any 2- or 3-core host — measured:
    // with the amplification restored and the old guard in place, a 3-core run
    // reported `confirmed_delta=24 bound=24` and PASSED.
    let min_cpus = min_cpus_for_amplification(producers, bound);
    if cpus < min_cpus {
        eprintln!(
            "[skip] num_cpus={cpus} < {min_cpus} — with C={c}, M={m} (producers={producers}), \
             PER_INPUT={PER_INPUT}, THREAD_SLACK={THREAD_SLACK} the pre-#2316 cost \
             C·M·(1+num_cpus)={} does not exceed the O(C·M) bound {bound}, so the \
             amplification is not observable here; holding trivially rather than asserting a \
             bound that holds either way",
            producers * (1 + cpus),
        );
        return;
    }

    // The reap-confirm budget SCALES with the producer count — C·M = 6 nominally, vs
    // #2316's M = 4 — because each late finish-and-reap resets the quiescence span,
    // so the number of resets is bounded by the producers that can still hold
    // in-flight blocking work. See `os_thread_budget::reap_confirm_timeout` for the
    // full derivation (and its quantified harness-budget note).
    let confirm_timeout = reap_confirm_timeout(producers);

    // Baseline via LIFECYCLE synchronization (the input-build runtime is dropped
    // but its teardown may still be in flight): poll until quiesced.
    let (_settle_peak, baseline) = poll_until_stable(Duration::from_secs(10));

    // Open the DIAGNOSTIC-ONLY CPU-pressure window over the measured region so a
    // future red is classifiable as starved-host vs real-regression from the panic
    // message alone (#3514). It never influences the verdict, and it never skips
    // this test: a load precondition would leave the pin unrun on exactly the
    // hosts where it reds.
    let pressure = open_cpu_pressure_window();

    // Construct C mergers over the SAME inputs (read-only) — each spawns its own M
    // producers, which fill their bounded channels and park. All C·M producers are
    // alive at once and stay alive until we drain below. Pre-#2316: each producer
    // ALSO holds a multi-threaded runtime (num_cpus workers) → C·M·num_cpus.
    let (baseline_ts, baseline_ldt, baseline_ttl) = compute_baseline_min(&inputs);
    let mut mergers = Vec::with_capacity(c);
    for _ in 0..c {
        mergers.push(KWayMerger::new(inputs.clone(), &schema).expect("KWayMerger::new"));
    }

    // Wait for all producers to reach their steady (blocked-on-send) state via the
    // SAME lifecycle synchronization — never a fixed window. `peak` also captures a
    // transient spike (the pre-fix per-producer runtime-worker burst).
    let (peak, settled) = poll_until_stable(Duration::from_secs(20));
    let delta = peak.saturating_sub(baseline);
    // `bound` and `producers` are the single-source values derived above from `m` —
    // never re-derived here, so they cannot drift from the vacuity guard.
    let pressure_at_peak = pressure.report();

    eprintln!(
        "[issue-2370] cpus={cpus} C={c} M={m} baseline={baseline} peak={peak} settled={settled} \
         delta={delta} bound={bound}"
    );
    eprintln!("[issue-2370] {pressure_at_peak}");

    // #3438 AC1 — CONFIRM an over-bound reading before condemning it. The fast
    // path is unchanged and pays ZERO extra latency: a within-bound delta is
    // accepted immediately. Only an overshoot takes this branch, and it does so
    // with all C·M producers STILL blocked on `send` (every merger is drained
    // strictly below), which is what makes the discriminator sound: reapable
    // blocking-pool threads disappear over the hold, while the runtime workers of
    // an actual amplification cannot be reaped while their runtime lives. The
    // bound is NOT widened.
    let mut pressure_at_confirm = None;
    let (pin_satisfied, confirm_note) = if delta > bound {
        eprintln!(
            "[issue-2370-reap] fast-path delta={delta} exceeds bound={bound}; holding the {producers} \
             blocked producers until the pool drains within budget and holds there for \
             {REAP_QUIESCENCE_SPAN:?} (> tokio's 10s blocking-pool thread_keep_alive), or up to \
             {confirm_timeout:?} before condemning the reading"
        );
        let outcome = poll_until_reaped(baseline + bound, REAP_QUIESCENCE_SPAN, confirm_timeout);
        let report = pressure.report();
        eprintln!("[issue-2370-reap] {report}");
        pressure_at_confirm = Some(report);
        match outcome {
            ReapOutcome::Drained { peak: rp, settled } => {
                let d = settled.saturating_sub(baseline);
                eprintln!(
                    "[issue-2370-reap] post-reap peak={rp} settled={settled} confirmed_delta={d} \
                     bound={bound} (within bound and HELD for {REAP_QUIESCENCE_SPAN:?} — the \
                     overshoot was reapable blocking-pool jitter, absorbed)"
                );
                (true, format!("drained to {d} within bound"))
            }
            ReapOutcome::Unconfirmed { peak: rp, last } => {
                let d = last.saturating_sub(baseline);
                eprintln!(
                    "[issue-2370-reap] post-reap peak={rp} last={last} last_delta={d} \
                     bound={bound} (UNCONFIRMED after {confirm_timeout:?} — never held within \
                     budget for {REAP_QUIESCENCE_SPAN:?})"
                );
                (
                    false,
                    format!(
                        "UNCONFIRMED after {confirm_timeout:?}: last reading {last} (delta {d}), \
                         peak {rp} — the pool never drained to within budget and HELD there for \
                         {REAP_QUIESCENCE_SPAN:?}"
                    ),
                )
            }
        }
    } else {
        (true, format!("fast-path delta {delta} within bound"))
    };

    // Drain every merger so producers exit cleanly (no leaked threads/temp files).
    let out = TempDir::new().expect("out tempdir");
    let finish_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("finish runtime");
    for (i, merger) in mergers.into_iter().enumerate() {
        let mut writer = SSTableWriter::new(out.path().join(format!("m{i}")), 1, &schema)
            .expect("SSTableWriter::new");
        writer.pre_seed_encoding_baselines(baseline_ts, baseline_ldt, baseline_ttl);
        let stats = merger.merge(&mut writer).expect("merge into writer");
        finish_rt.block_on(writer.finish()).expect("writer finish");
        assert!(
            stats.output_rows >= (NUM_INPUTS as u64 * rows_per_input() as u64),
            "merger {i} should emit all input rows; got {}",
            stats.output_rows
        );
    }

    // THE PIN: the aggregate OS-thread delta over baseline must be within the
    // O(C·M) bound. Pre-#2316 this is ~C·M·num_cpus (>> bound); post-fix ~C·M.
    // `pin_satisfied` is true only when the fast-path delta was already within
    // bound, or the reap confirmation returned an AFFIRMATIVE `Drained` — never
    // merely because nothing bad was observed.
    let pressure_note = match &pressure_at_confirm {
        Some(confirm) => format!("{pressure_at_peak} | over the confirm window too: {confirm}"),
        None => pressure_at_peak.clone(),
    };
    assert!(
        pin_satisfied,
        "C={c} concurrent merges over M={m} inputs each failed the O(C·M) producer-thread pin: \
         {confirm_note}. (fast-path peak={peak}, settled={settled}, delta={delta}, \
         baseline={baseline}; O(C·M) bound is {bound} = {PER_INPUT}·M·C + {THREAD_SLACK}.) \
         The confirmation holds all {producers} producers blocked on `send` and waits up to \
         {confirm_timeout:?} for tokio's blocking pool to drain, so threads still present at the \
         end are NOT reapable spawn_blocking jitter — they are held by something with the \
         producers' lifetime. A surviving delta scaling with num_cpus (num_cpus={cpus}; the \
         pre-#2316 shape is C·M·num_cpus ≈ {}) means a producer built a multi-threaded runtime — \
         the #2316 amplification, unbounded under concurrency (#2370); a smaller persistent \
         overshoot has another origin. An UNCONFIRMED outcome means the measurement itself never \
         settled — that FAILS closed, because an unconfirmable reading is not evidence of good \
         behaviour. Host CPU-stall context, DIAGNOSTIC ONLY (it did not affect this verdict): \
         {pressure_note}",
        producers * cpus,
    );
}
