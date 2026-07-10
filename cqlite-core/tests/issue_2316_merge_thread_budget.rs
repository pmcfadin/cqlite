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
//! producer is alive at once. That gives a deterministic window in which to sample
//! the peak: no concurrent-sampler timing race. The assertion FAILS on the
//! pre-change code (each producer's multi-threaded runtime adds `num_cpus` workers)
//! and PASSES after the `current_thread`-runtime fix, on any host where
//! `num_cpus >= 2`. Where the amplification collapses (`num_cpus < 2`) or the
//! platform exposes no direct thread-count API, the test guards deterministically
//! rather than flake.

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
/// (256) so every producer fills its bounded channel and blocks on `send` before
/// the merge starts draining — guaranteeing all `M` producers are alive at once
/// at the sampling point (deterministic peak, no timing race).
const ROWS_PER_INPUT: i32 = 400;

/// Number of input SSTables (`M`) merged in one pass.
const NUM_INPUTS: usize = 4;

/// Max OS threads a single fixed (`current_thread`-runtime) producer contributes
/// at its peak: the producer thread itself plus the bounded `spawn_blocking`
/// parse/feed threads the compaction scan uses (a small constant, INDEPENDENT of
/// `num_cpus`). The pre-change multi-threaded runtime instead added `num_cpus`
/// worker threads PER producer on top of this, so the pre-change peak scales with
/// `num_cpus` and this `O(M)` bound (coefficient fixed at `PER_INPUT`) rejects it.
const PER_INPUT: usize = 3;

/// Fixed slack over the `PER_INPUT · M` bound for incidental/settle threads.
const THREAD_SLACK: usize = 3;

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

/// Sample the peak OS thread count over a fixed window. The producers stay blocked
/// on the bounded channel for the whole window (nothing is draining them), so the
/// peak is stable; polling simply waits out the brief interval in which each
/// producer builds its runtime and reaches the blocked state.
fn sample_peak_threads(window: Duration) -> Option<usize> {
    let deadline = Instant::now() + window;
    let mut peak = os_thread_count()?;
    while Instant::now() < deadline {
        if let Some(n) = os_thread_count() {
            peak = peak.max(n);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Some(peak)
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
            value: Value::Text(val.to_string()),
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
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

fn collect_inputs(dir: &std::path::Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
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
    // Guard 1: the amplification (M·num_cpus) collapses on a single-core host, so
    // the bound cannot be distinguished from the O(M) target there. Hold trivially.
    let cpus = num_cpus::get();
    if cpus < 2 {
        eprintln!("[skip] num_cpus={cpus} < 2 — amplification not observable; holding trivially");
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

    // Settle the baseline: the input-build runtime has been dropped; wait for the
    // process thread count to quiesce so the baseline is not inflated by threads
    // still winding down.
    std::thread::sleep(Duration::from_millis(50));
    let baseline = os_thread_count().expect("baseline thread count");

    // Construct the merger: this spawns all M producer threads. Each producer
    // opens its reader and streams into the bounded channel; with ROWS_PER_INPUT
    // (> the 256-entry channel capacity) every producer fills its channel and
    // blocks on `send` — so all M are alive at once and stay alive until `merge()`
    // below drains them. Pre-change: each producer ALSO holds a multi-threaded
    // runtime (num_cpus workers). Post-change: a current_thread runtime, 0 workers.
    let (baseline_ts, baseline_ldt, baseline_ttl) = compute_baseline_min(&inputs);
    let merger = KWayMerger::new(inputs, &schema).expect("KWayMerger::new");

    // Sample the peak while the producers are blocked (a wide, stable window).
    let peak = sample_peak_threads(Duration::from_millis(1200)).expect("peak thread count");
    let delta = peak.saturating_sub(baseline);
    let bound = PER_INPUT * m + THREAD_SLACK;

    eprintln!(
        "[issue-2316] cpus={cpus} M={m} baseline={baseline} peak={peak} delta={delta} bound={bound}"
    );

    // Drain the merge so producers exit cleanly (no leaked threads / temp files).
    let mut writer = SSTableWriter::new(out.path().to_path_buf(), 1, &schema)
        .expect("SSTableWriter::new");
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

    // THE PIN: the merge's peak OS-thread delta over baseline must be within the
    // O(M) bound. Pre-change this is `M + M·num_cpus` (>> bound); post-change `M`.
    assert!(
        delta <= bound,
        "merge over M={m} inputs added {delta} OS threads over baseline \
         (peak={peak}, baseline={baseline}); O(M) bound is {bound} (= {PER_INPUT}·M + {THREAD_SLACK}). \
         A delta scaling with num_cpus (num_cpus={cpus}) means a producer built a \
         multi-threaded runtime — issue #2316 amplification."
    );
}
