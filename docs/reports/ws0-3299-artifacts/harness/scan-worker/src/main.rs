//! `ws0-3299-scan-worker` — ONE bare scan, pinned to ONE physical core, emitting
//! timestamped progress records so an **aligned concurrency window** can be
//! computed across S of these running at once (issue #3299, AC1/AC2).
//!
//! # Why this exists rather than `ws0-scan-bench`
//!
//! `ws0-scan-bench` (the #3096/#3272 rig's bare-scan arm) reports one wall time
//! per PASS. A pass over the #3096 corpus is seconds long, so pass boundaries are
//! far too coarse to attribute rows to a window shared by S independent scans:
//! attributing to a 60 s window at ~8 s granularity leaves a ±27% hole. This
//! worker drives the **same code path** — the same `cli-helpers` feature set, the
//! same `ingest_with_selection(TableDirSelection::Exact)` setup, the rig's own
//! `scan_scope::verify_exact_scope`, the same
//! `Database::execute_streaming(sql, StreamingConfig::default())` loop with the
//! same `black_box` — and adds only a progress record every `--progress-rows`
//! rows plus a steady-state loop. The equivalence is not asserted by this comment:
//! `sweep.sh --equivalence` measures this worker against `ws0-scan-bench` on one
//! core in the same session and refuses a divergence beyond a stated band.
//!
//! # The three properties the S-sweep needs from a worker
//!
//! 1. **A clock comparable ACROSS PROCESSES.** `std::time::Instant` is monotonic
//!    but opaque and not comparable between processes, so every timestamp here is
//!    a raw `clock_gettime(CLOCK_MONOTONIC)` in nanoseconds — the same clock, via
//!    the same syscall, that the window controller reads (`--print-monotonic-ns`
//!    exists so the driver can take T0/T1 from this very code path).
//! 2. **Steady state.** After the barrier the worker scans in a continuous loop
//!    until told to stop, so the aligned window can sit entirely inside an
//!    interval where all S scans are producing rows.
//! 3. **Affinity OBSERVED, not assumed.** The worker reads back its own
//!    `sched_getaffinity` mask and records it, so the driver's pinning guard
//!    checks what the kernel actually did rather than trusting that `taskset`
//!    was passed the right string.
//!
//! # Zero rows is a failure, never a measurement
//!
//! Inherited deliberately from `ws0-scan-bench` (spec R4): a pass that observed
//! nothing, or a run whose measured window contains no rows, exits non-zero.

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use cqlite_core::query::result::StreamingConfig;

/// Nanoseconds on `CLOCK_MONOTONIC` — the one clock every timestamp in this rig
/// comes from, in every process.
fn monotonic_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `ts` is a valid, properly aligned `timespec` we own for the call.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    assert!(rc == 0, "clock_gettime(CLOCK_MONOTONIC) failed");
    (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_nsec as u64)
}

/// The CPUs this process is actually allowed to run on, read back from the
/// kernel. Recorded so pinning is verified rather than trusted.
fn observed_affinity() -> Vec<u32> {
    // SAFETY: `set` is zeroed before use and only read through the libc macros.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        let mut cpus = Vec::new();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) == 0 {
            for cpu in 0..libc::CPU_SETSIZE as usize {
                if libc::CPU_ISSET(cpu, &set) {
                    cpus.push(cpu as u32);
                }
            }
        }
        cpus
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "ws0-3299-scan-worker",
    about = "One pinned bare scan with timestamped progress (issue #3299 S-sweep)"
)]
struct Cli {
    /// Print `clock_gettime(CLOCK_MONOTONIC)` in ns and exit. The driver uses
    /// this so its window boundaries come from the identical clock source.
    #[arg(long)]
    print_monotonic_ns: bool,

    /// Corpus root holding `<keyspace>/<table>/`.
    #[arg(long, required_unless_present = "print_monotonic_ns")]
    corpus: Option<PathBuf>,

    /// Schema `.cql`; defaults to `<corpus>/ws0-events.cql`, as `ws0-scan-bench` does.
    #[arg(long)]
    schema: Option<PathBuf>,

    #[arg(long, default_value = "ws0")]
    keyspace: String,

    #[arg(long, default_value = "events")]
    table: String,

    /// Index of this worker within the rep, `0..S`.
    #[arg(long, default_value_t = 0)]
    worker_id: u32,

    /// Shared rep directory: barrier files (`go`, `stop`) are read from here and
    /// `ready-<id>`, `worker-<id>.progress.jsonl`, `worker-<id>.summary.json` are
    /// written here.
    #[arg(long, required_unless_present = "print_monotonic_ns")]
    rundir: Option<PathBuf>,

    /// Rows between progress records. This sets the attribution granularity of
    /// the aligned window, so it is a MEASUREMENT parameter, not a log verbosity
    /// knob: the driver's shortfall guard rejects a rep whose window boundaries
    /// could not be pinned down closely enough (see README, "aligned window").
    #[arg(long, default_value_t = 16384, value_parser = at_least_one_row)]
    progress_rows: u64,

    /// Full passes to run BEFORE signalling ready. The protocol here is WARM, so
    /// this must be >= 1: the measured window must never contain first-touch page
    /// cache population.
    #[arg(long, default_value_t = 1, value_parser = at_least_one)]
    prewarm_passes: u32,

    /// Hard ceiling on the post-barrier steady-state loop. A missing `stop` file
    /// (dead driver) must not leave a scan spinning on a metered box.
    #[arg(long, default_value_t = 900)]
    max_secs: u64,
}

/// `--progress-rows` must be >= 1. At 0 the sample countdown would wrap on its
/// first decrement and the worker would emit no usable progress at all, so the
/// aligned window could not be computed — refused at parse time rather than
/// producing a rep the guards would have to reject later.
fn at_least_one_row(s: &str) -> Result<u64, String> {
    let n: u64 = s.parse().map_err(|_| format!("`{s}` is not an integer"))?;
    if n == 0 {
        return Err("must be >= 1: a 0-row sample interval emits no progress records, \
                    so no window could be attributed"
            .to_string());
    }
    Ok(n)
}

fn at_least_one(s: &str) -> Result<u32, String> {
    let n: u32 = s.parse().map_err(|_| format!("`{s}` is not an integer"))?;
    if n == 0 {
        return Err("must be >= 1: this issue's protocol is WARM, and a worker that \
                    signals ready without prewarming would put first-touch page-cache \
                    population inside the measured window"
            .to_string());
    }
    Ok(n)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    if cli.print_monotonic_ns {
        println!("{}", monotonic_ns());
        return ExitCode::SUCCESS;
    }
    match run(cli).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("ws0-3299-scan-worker: ERROR: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let corpus = cli.corpus.clone().expect("clap requires --corpus");
    let rundir = cli.rundir.clone().expect("clap requires --rundir");
    let id = cli.worker_id;
    let schema_path = cli
        .schema
        .clone()
        .unwrap_or_else(|| corpus.join("ws0-events.cql"));
    if !schema_path.exists() {
        return Err(format!("schema not found: {}", schema_path.display()).into());
    }
    let table_dir = corpus.join(&cli.keyspace).join(&cli.table);
    let has_data = std::fs::read_dir(&table_dir)
        .map(|d| {
            d.flatten()
                .any(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
        })
        .unwrap_or(false);
    if !has_data {
        return Err(format!(
            "{} holds no *-Data.db — a 0-row scan is a FAILURE, never a measurement",
            table_dir.display()
        )
        .into());
    }

    // --- setup: the SAME path ws0-scan-bench uses, including its exact-scope
    // --- verification. Never inside the measured window (the barrier is below).
    let cfg = cqlite_core::ingestion::IngestionConfig {
        schema_paths: vec![schema_path.clone()],
        data_dir: corpus.clone(),
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        table_directory_filter: None,
    };
    let wanted = [table_dir.clone()];
    let ingested = cqlite_core::ingestion::ingest_with_selection(
        cfg,
        cqlite_core::ingestion::TableDirSelection::Exact(&wanted),
    )
    .await?;
    let selected = &ingested.discovery_summary.table_directories;
    ws0_corpus_gen::scan_scope::verify_exact_scope(selected, &table_dir, true)?;
    let ingested_dirs: Vec<String> = selected.iter().map(|d| d.display().to_string()).collect();
    let db = ingested.database;
    let sql = format!("SELECT * FROM {}.{}", cli.keyspace, cli.table);

    // --- prewarm (WARM protocol): untimed, outside the barrier ----------------
    let mut prewarm_rows = 0u64;
    for _ in 0..cli.prewarm_passes {
        let mut it = db
            .execute_streaming(&sql, StreamingConfig::default())
            .await?;
        while let Some(row) = it.next_async().await {
            let row = row?;
            std::hint::black_box(&row);
            prewarm_rows += 1;
        }
    }
    if prewarm_rows == 0 {
        return Err(format!(
            "prewarm observed ZERO rows over {} — exiting non-zero rather than \
             reporting a measurement",
            table_dir.display()
        )
        .into());
    }

    // --- barrier: announce ready, then wait for `go` --------------------------
    let progress_path = rundir.join(format!("worker-{id}.progress.jsonl"));
    let mut progress = BufWriter::new(std::fs::File::create(&progress_path)?);
    std::fs::write(
        rundir.join(format!("ready-{id}")),
        format!("{}\n", monotonic_ns()),
    )?;
    let go = rundir.join("go");
    let stop = rundir.join("stop");
    wait_for(&go, cli.max_secs)?;

    // --- steady state: scan continuously, emitting timestamped progress -------
    //
    // Each record is (monotonic ns, cumulative rows since the barrier). The
    // driver attributes rows to the aligned window by DIFFERENCING two of these
    // records — never by assuming a rate — which is why they are flushed as they
    // are written rather than buffered to exit.
    let t_start = monotonic_ns();
    let mut rows: u64 = 0;
    let mut cells: u64 = 0;
    let mut passes: u64 = 0;
    let mut samples: u64 = 0;
    let deadline = t_start + cli.max_secs.saturating_mul(1_000_000_000);
    emit(&mut progress, t_start, 0)?;
    samples += 1;
    let mut stopping = false;
    while !stopping {
        let mut it = db
            .execute_streaming(&sql, StreamingConfig::default())
            .await?;
        let mut until_sample = cli.progress_rows;
        while let Some(row) = it.next_async().await {
            let row = row?;
            cells += row.values.len() as u64;
            std::hint::black_box(&row);
            rows += 1;
            until_sample -= 1;
            if until_sample == 0 {
                until_sample = cli.progress_rows;
                let now = monotonic_ns();
                emit(&mut progress, now, rows)?;
                samples += 1;
                // Stop and deadline are checked on the sample boundary, so the
                // hot loop carries no extra syscall per row.
                if stop.exists() || now >= deadline {
                    stopping = true;
                    break;
                }
            }
        }
        if !stopping {
            passes += 1;
            if stop.exists() || monotonic_ns() >= deadline {
                stopping = true;
            }
        }
    }
    let t_end = monotonic_ns();
    emit(&mut progress, t_end, rows)?;
    samples += 1;
    progress.flush()?;

    if rows == 0 {
        return Err("steady state observed ZERO rows — exiting non-zero rather than \
                    reporting a measurement"
            .into());
    }

    let summary = serde_json::json!({
        "arm": "bare_scan",
        "surface": "cqlite_core::Database::execute_streaming",
        "worker_id": id,
        "pid": std::process::id(),
        // Read back from the kernel, so the driver's pinning guard checks what
        // actually happened rather than what taskset was asked for.
        "observed_affinity": observed_affinity(),
        "corpus": corpus.display().to_string(),
        "schema": schema_path.display().to_string(),
        "table_dirs_ingested": ingested_dirs,
        "query": sql,
        "prewarm_passes": cli.prewarm_passes,
        "prewarm_rows": prewarm_rows,
        "progress_rows": cli.progress_rows,
        "t_start_ns": t_start,
        "t_end_ns": t_end,
        "rows_total": rows,
        "cells_total": cells,
        "full_passes_completed": passes,
        "progress_samples": samples,
    });
    std::fs::write(
        rundir.join(format!("worker-{id}.summary.json")),
        format!("{}\n", serde_json::to_string(&summary)?),
    )?;
    println!("{}", serde_json::to_string(&summary)?);
    Ok(ExitCode::SUCCESS)
}

fn emit(w: &mut BufWriter<std::fs::File>, t_ns: u64, rows: u64) -> std::io::Result<()> {
    writeln!(w, "{{\"t_ns\":{t_ns},\"rows\":{rows}}}")?;
    w.flush()
}

/// Poll for a barrier file. Polling (10 ms) rather than a fifo keeps the barrier
/// robust to worker start-order and to a worker dying: nothing blocks forever,
/// and the release skew is irrelevant because the aligned window is computed
/// from OBSERVED timestamps, never from the barrier.
fn wait_for(path: &Path, max_secs: u64) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = monotonic_ns() + max_secs.saturating_mul(1_000_000_000);
    while !path.exists() {
        if monotonic_ns() >= deadline {
            return Err(format!(
                "timed out after {max_secs}s waiting for barrier file {}",
                path.display()
            )
            .into());
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Ok(())
}
