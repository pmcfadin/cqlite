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
//! alive, parked on their bounded channels) and pins the process's PEAK OS-thread
//! delta to the O(C·M) bound via the same direct `/proc/self/task` /
//! `proc_pidinfo` kernel observation the #2316 test uses.
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
use std::time::{Duration, Instant};

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::merge::compute_baseline_min;
use cqlite_core::storage::write_engine::{
    CellOperation, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

/// Rows per input SSTable. Must EXCEED the merge's `STREAMING_CHANNEL_CAPACITY`
/// (256) so every producer fills its bounded channel and blocks on `send`, so all
/// producers of all mergers are alive at once at the sampling point.
const ROWS_PER_INPUT: i32 = 400;

/// Number of input SSTables (`M`) merged per merger.
const NUM_INPUTS: usize = 3;

/// Number of CONCURRENT mergers (`C`) — the aggregate case (≥2, the field shape).
const NUM_MERGERS: usize = 2;

/// Max OS threads a single fixed (`current_thread`-runtime) producer contributes
/// at its peak (producer thread + bounded `spawn_blocking` parse/feed threads),
/// INDEPENDENT of `num_cpus`. The pre-#2316 multi-threaded runtime added
/// `num_cpus` worker threads PER producer on top of this — so the pre-change peak
/// scales with `num_cpus` and the O(C·M) bound below rejects it.
const PER_INPUT: usize = 3;

/// Fixed slack over the `PER_INPUT · M · C` bound for incidental/settle threads.
const THREAD_SLACK: usize = 6;

// ── Direct, no-heuristics OS thread-count observation (mirrors #2316) ────────

#[cfg(target_os = "linux")]
fn os_thread_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/task")
        .ok()
        .map(|it| it.flatten().count())
}

#[cfg(target_os = "macos")]
fn os_thread_count() -> Option<usize> {
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

/// Consecutive identical readings required to treat the count as STABILIZED.
const STABLE_STREAK: usize = 8;
/// Delay between polls while waiting for stabilization.
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Poll the process OS-thread count until it STABILIZES (same reading across
/// [`STABLE_STREAK`] consecutive polls) — synchronizing on the thread LIFECYCLE,
/// not a fixed elapsed window. Returns `(peak, settled)`. `timeout` is a fail-loud
/// BOUND only: a never-settling count panics with a clear diagnostic rather than
/// silently under-sampling.
fn poll_until_stable(timeout: Duration) -> (usize, usize) {
    let deadline = Instant::now() + timeout;
    let mut peak = 0usize;
    let mut last: Option<usize> = None;
    let mut streak = 0usize;
    while Instant::now() < deadline {
        let n = os_thread_count()
            .expect("thread count observation must remain available (guard 2 confirmed it)");
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
        "OS thread count never stabilized within {timeout:?} (last {last:?}, streak \
         {streak}/{STABLE_STREAK}, peak {peak}); fail-loud BOUND, not a sync mechanism"
    );
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
            value: Value::Text(val.to_string()),
        }],
        ts,
        None,
    )
}

fn discover_inputs(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    collect_inputs(dir, &mut found, 8);
    found.sort_by(|a, b| b.0.cmp(&a.0));
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
    // Drop the driver runtime BEFORE the caller measures baseline so its
    // (now-joined) worker threads are not counted in the baseline.
    drop(rt);
    (temp, inputs, schema)
}

#[test]
fn concurrent_merges_bound_aggregate_threads_to_o_c_m() {
    // Guard 1: the amplification (M·num_cpus) collapses on a single-core host.
    let cpus = num_cpus::get();
    if cpus < 2 {
        eprintln!("[skip] num_cpus={cpus} < 2 — amplification not observable; holding trivially");
        return;
    }
    // Guard 2: no direct thread-count API on this platform.
    if os_thread_count().is_none() {
        eprintln!("[skip] no direct OS thread-count API on this platform; holding trivially");
        return;
    }

    let (_temp, inputs, schema) = build_inputs();
    let m = inputs.len();
    let c = NUM_MERGERS;

    // Baseline via LIFECYCLE synchronization (the input-build runtime is dropped
    // but its teardown may still be in flight): poll until quiesced.
    let (_settle_peak, baseline) = poll_until_stable(Duration::from_secs(10));

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
    let (peak, _settled) = poll_until_stable(Duration::from_secs(20));
    let delta = peak.saturating_sub(baseline);
    let bound = PER_INPUT * m * c + THREAD_SLACK;

    eprintln!(
        "[issue-2370] cpus={cpus} C={c} M={m} baseline={baseline} peak={peak} delta={delta} bound={bound}"
    );

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
        finish_rt
            .block_on(writer.finish())
            .expect("writer finish");
        assert!(
            stats.output_rows >= (NUM_INPUTS as u64 * ROWS_PER_INPUT as u64),
            "merger {i} should emit all input rows; got {}",
            stats.output_rows
        );
    }

    // THE PIN: the aggregate peak OS-thread delta over baseline must be within the
    // O(C·M) bound. Pre-#2316 this is ~C·M·num_cpus (>> bound); post-fix ~C·M.
    assert!(
        delta <= bound,
        "C={c} concurrent merges over M={m} inputs each added {delta} OS threads over baseline \
         (peak={peak}, baseline={baseline}); O(C·M) bound is {bound} (= {PER_INPUT}·M·C + {THREAD_SLACK}). \
         A delta scaling with num_cpus (num_cpus={cpus}) means a producer built a multi-threaded \
         runtime — issue #2316 amplification, unbounded under concurrency (#2370)."
    );
}
