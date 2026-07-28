//! WS0 scan-throughput harness for issue #3026 (umbrella #3023).
//!
//! THROWAWAY measurement tool. Lives outside the cqlite repo so the wt-3026
//! worktree stays git-clean. Drives ONLY public `cqlite-core` API.
//!
//! Three surfaces, one process, so the "bare scan" vs "scan + Arrow encode"
//! range is measured with identical codegen, identical corpus, identical pass
//! structure — no cross-binary confound:
//!
//!   --mode scan        streaming `execute_streaming` scan; every value of every
//!                      row is folded into a checksum (see `fold_value`), so no
//!                      cell can be dead-code-eliminated and every value is
//!                      genuinely materialized. This is the BARE CORE SCAN.
//!   --mode scan-arrow  same scan, but rows are accumulated into `--batch-size`
//!                      chunks and run through the PUBLIC
//!                      `cqlite_core::export::rows_to_record_batch` — the exact
//!                      CQL->Arrow converter the Flight `do_get` data plane
//!                      uses. Adds Arrow encode, excludes gRPC/IPC framing.
//!   --mode scan-collect materializing `db.execute()` (whole result set in a Vec)
//!                      — the shape `benches/read.rs::full_scan` uses. Included
//!                      only so the criterion bench number is comparable; it is
//!                      NOT a throughput surface (peak RSS is O(rows)).
//!
//! Anti-flattery guarantees:
//!   * 0 rows on a present dataset => exit 2. Never a pass.
//!   * The checksum over every cell is PRINTED, so a compiler that elided decode
//!     would show a changed/zero digest.
//!   * `--reopen-each-pass` opens a FRESH `Database` per pass, so a reader/index
//!     cache cannot be amortized across iterations. Per-pass timings are printed
//!     individually and never silently averaged; pass 0 is labelled `cold`.
//!   * Cell count is reported alongside row count, so cycles/row can be
//!     cross-checked against cycles/cell.

use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use cqlite_core::query::result::{QueryRow, StreamingConfig};
use cqlite_core::types::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Scan,
    ScanArrow,
    ScanCollect,
}

impl std::str::FromStr for Mode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "scan" => Ok(Mode::Scan),
            "scan-arrow" => Ok(Mode::ScanArrow),
            "scan-collect" => Ok(Mode::ScanCollect),
            o => Err(format!("unknown mode {o:?} (scan|scan-arrow|scan-collect)")),
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "ws0-scan-harness", about = "issue #3026 WS0 scan-throughput harness")]
struct Cli {
    /// Dataset root (the dir holding `sstables/`). Mirrors CQLITE_DATASETS_ROOT.
    #[arg(long, env = "CQLITE_DATASETS_ROOT")]
    datasets_root: PathBuf,

    /// Keyspace directory name under `<root>/sstables/`.
    #[arg(long)]
    keyspace: String,

    /// Table name (WITHOUT the `-<cfid>` suffix).
    #[arg(long)]
    table: String,

    /// Schema .cql under `<root>/../schemas/` (or an absolute path).
    #[arg(long)]
    schema: String,

    /// Surface to time.
    #[arg(long, default_value = "scan")]
    mode: Mode,

    /// Number of timed passes. Pass 0 is reported as `cold`.
    #[arg(long, default_value_t = 3)]
    passes: u32,

    /// Open a FRESH Database for every pass (defeats reader-cache amortization).
    #[arg(long)]
    reopen_each_pass: bool,

    /// Rows per Arrow RecordBatch in `scan-arrow` (matches cqlite-flight's
    /// `--batch-size` default of 8192).
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    /// Tokio worker threads. `1` = single-threaded runtime (see README).
    #[arg(long, default_value_t = 1)]
    worker_threads: usize,

    /// Skip the anti-elision digest fold. Quantifies the harness's OWN cost:
    /// run once with and once without, and the delta is harness overhead. The
    /// row is still received and dropped, so the scan itself is unchanged.
    #[arg(long)]
    no_fold: bool,

    /// Optional projection; default `*`.
    #[arg(long, default_value = "*")]
    project: String,

    /// PERSISTENT stage dir instead of a per-pid tempdir. The fixture is copied
    /// in only if absent, so a caller can stage ONCE, then `drop_caches`, then
    /// re-invoke for a genuinely COLD timed pass (a per-pid tempdir always
    /// re-copies, which warms the page cache and makes "cold" a lie).
    #[arg(long)]
    stage_dir: Option<PathBuf>,
}

/// `read_bytes` / `rchar` / `syscr` from /proc/self/io. Reported SEPARATELY and
/// never divided by one another (a ratio of rchar to syscr is meaningless when
/// a single syscall's size varies).
fn proc_io() -> std::collections::BTreeMap<String, u64> {
    let mut m = std::collections::BTreeMap::new();
    if let Ok(s) = std::fs::read_to_string("/proc/self/io") {
        for l in s.lines() {
            if let Some((k, v)) = l.split_once(':') {
                if let Ok(n) = v.trim().parse::<u64>() {
                    m.insert(k.to_string(), n);
                }
            }
        }
    }
    m
}

/// Fold one CQL value into the digest, TOUCHING its payload bytes.
///
/// This is what makes the measurement honest: every variant's actual data is
/// hashed, so the optimizer cannot skip a decode, and a scan that returned
/// structurally-present but empty values would produce a different digest.
fn fold_value(h: &mut std::collections::hash_map::DefaultHasher, v: &Value, cells: &mut u64) {
    *cells += 1;
    match v {
        Value::Null => 0u8.hash(h),
        Value::Boolean(b) => b.hash(h),
        Value::Integer(i) => i.hash(h),
        Value::BigInt(i) | Value::Counter(i) | Value::Timestamp(i) | Value::Time(i) => i.hash(h),
        Value::Float(f) => f.to_bits().hash(h),
        Value::Float32(f) => f.to_bits().hash(h),
        Value::Text(b) => b.as_ref().hash(h),
        Value::Blob(b) | Value::Varint(b) => b.as_ref().hash(h),
        Value::Date(d) => d.hash(h),
        Value::Uuid(u) => u.hash(h),
        Value::Decimal { scale, unscaled } => {
            scale.hash(h);
            unscaled.hash(h);
        }
        Value::Duration {
            months,
            days,
            nanos,
        } => {
            months.hash(h);
            days.hash(h);
            nanos.hash(h);
        }
        Value::Json(j) => j.to_string().hash(h),
        Value::TinyInt(i) => i.hash(h),
        Value::SmallInt(i) => i.hash(h),
        Value::List(xs) | Value::Set(xs) => {
            for x in xs {
                fold_value(h, x, cells);
            }
        }
        Value::Map(kvs) => {
            for (k, val) in kvs {
                fold_value(h, k, cells);
                fold_value(h, val, cells);
            }
        }
        other => {
            // Remaining variants (UDT/tuple/frozen wrappers etc.): hash the
            // Debug rendering. Slower, but it still forces full materialization
            // rather than silently skipping the cell.
            format!("{other:?}").hash(h);
        }
    }
}

fn fold_row(h: &mut std::collections::hash_map::DefaultHasher, row: &QueryRow, cells: &mut u64) {
    // Sort keys so the digest is order-independent across HashMap iteration.
    let mut names: Vec<&str> = row.values.keys().map(|k| k.as_ref()).collect();
    names.sort_unstable();
    for n in names {
        n.hash(h);
        if let Some(v) = row.values.get(n) {
            fold_value(h, v, cells);
        }
    }
}

struct PassStats {
    rows: u64,
    cells: u64,
    digest: u64,
    /// Arrow payload bytes produced (scan-arrow only).
    arrow_bytes: u64,
    secs: f64,
}

fn table_dir(root: &Path, keyspace: &str, table: &str) -> Result<PathBuf, String> {
    let ks = root.join("sstables").join(keyspace);
    let rd = std::fs::read_dir(&ks).map_err(|e| format!("read_dir {}: {e}", ks.display()))?;
    let prefix = format!("{table}-");
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name == table || name.starts_with(&prefix) {
            // Fail closed: a dir with only .jsonl goldens and no real Data.db
            // would otherwise silently yield a 0-row "pass".
            let has_data = std::fs::read_dir(e.path())
                .map(|d| {
                    d.flatten()
                        .any(|f| f.file_name().to_string_lossy().ends_with("-Data.db"))
                })
                .unwrap_or(false);
            if !has_data {
                return Err(format!(
                    "{} exists but holds no *-Data.db binary — run test-data/scripts/fetch-datasets.sh",
                    e.path().display()
                ));
            }
            return Ok(e.path());
        }
    }
    Err(format!("no table dir for {keyspace}.{table} under {}", ks.display()))
}

fn copy_tree(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for e in std::fs::read_dir(src)? {
        let e = e?;
        let to = dst.join(e.file_name());
        if e.file_type()?.is_dir() {
            copy_tree(&e.path(), &to)?;
        } else {
            std::fs::copy(e.path(), &to)?;
        }
    }
    Ok(())
}

/// Stage the fixture into a scratch dir laid out as `<ks>/<table-cfid>/` and
/// open a Database over it via the public `ingestion::ingest`.
async fn open_db(
    cli: &Cli,
    scratch: &Path,
) -> Result<cqlite_core::Database, Box<dyn std::error::Error>> {
    use cqlite_core::ingestion::{ingest, IngestionConfig};

    let src = table_dir(&cli.datasets_root, &cli.keyspace, &cli.table)?;
    let leaf = src.file_name().ok_or("fixture dir has no final component")?;
    let dst = scratch.join(&cli.keyspace).join(leaf);
    if !dst.exists() {
        copy_tree(&src, &dst)?;
    }

    let schema_path = if Path::new(&cli.schema).is_absolute() {
        PathBuf::from(&cli.schema)
    } else {
        cli.datasets_root
            .parent()
            .ok_or("datasets_root has no parent")?
            .join("schemas")
            .join(&cli.schema)
    };
    if !schema_path.exists() {
        return Err(format!("schema not found: {}", schema_path.display()).into());
    }

    let cfg = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: scratch.to_path_buf(),
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{}/{}", cli.keyspace, cli.table)),
    };
    Ok(ingest(cfg).await?.database)
}

async fn run_pass(
    cli: &Cli,
    db: &cqlite_core::Database,
    sql: &str,
) -> Result<PassStats, Box<dyn std::error::Error>> {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    let mut rows = 0u64;
    let mut cells = 0u64;
    let mut arrow_bytes = 0u64;

    let t0 = Instant::now();
    match cli.mode {
        Mode::Scan => {
            let mut it = db
                .execute_streaming(sql, StreamingConfig::default())
                .await?;
            while let Some(r) = it.next_async().await {
                let r = r?;
                if !cli.no_fold {
                    fold_row(&mut h, &r, &mut cells);
                }
                std::hint::black_box(&r);
                rows += 1;
            }
        }
        Mode::ScanArrow => {
            use cqlite_core::export::rows_to_record_batch;
            let mut it = db
                .execute_streaming(sql, StreamingConfig::default())
                .await?;
            let cols = it.metadata.columns.clone();
            let mut buf: Vec<QueryRow> = Vec::with_capacity(cli.batch_size);
            let flush = |buf: &mut Vec<QueryRow>,
                             arrow_bytes: &mut u64|
             -> Result<(), Box<dyn std::error::Error>> {
                if buf.is_empty() {
                    return Ok(());
                }
                let batch = rows_to_record_batch(&cols, buf)?;
                // Sum Arrow buffer PAYLOAD lengths — forces the encoder's
                // buffers to be walked, so the encode cannot be elided.
                let mut b = 0u64;
                for c in batch.columns() {
                    b += c.to_data().buffers().iter().map(|x| x.len() as u64).sum::<u64>();
                }
                *arrow_bytes += b;
                buf.clear();
                Ok(())
            };
            while let Some(r) = it.next_async().await {
                let r = r?;
                if !cli.no_fold {
                    fold_row(&mut h, &r, &mut cells);
                }
                rows += 1;
                buf.push(r);
                if buf.len() >= cli.batch_size {
                    flush(&mut buf, &mut arrow_bytes)?;
                }
            }
            flush(&mut buf, &mut arrow_bytes)?;
        }
        Mode::ScanCollect => {
            let res = db.execute(sql).await?;
            for r in &res.rows {
                if !cli.no_fold {
                    fold_row(&mut h, r, &mut cells);
                }
                std::hint::black_box(r);
                rows += 1;
            }
        }
    }
    let secs = t0.elapsed().as_secs_f64();

    Ok(PassStats {
        rows,
        cells,
        digest: h.finish(),
        arrow_bytes,
        secs,
    })
}

fn peak_rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse().ok()))
        })
        .unwrap_or(0)
}

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let rt = match cli.worker_threads {
        1 => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build(),
        n => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(n)
            .enable_all()
            .build(),
    };
    let rt = match rt {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("harness: tokio runtime: {e}");
            return std::process::ExitCode::from(1);
        }
    };

    let scratch = match cli.stage_dir.clone() {
        Some(d) => match std::fs::create_dir_all(&d).map(|_| d) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("harness: stage dir: {e}");
                return std::process::ExitCode::from(1);
            }
        },
        None => match tempdir() {
            Ok(d) => d,
            Err(e) => {
                eprintln!("harness: scratch dir: {e}");
                return std::process::ExitCode::from(1);
            }
        },
    };
    let io_at_start = proc_io();

    let sql = format!(
        "SELECT {} FROM {}.{}",
        cli.project, cli.keyspace, cli.table
    );
    eprintln!("harness: mode={:?} sql={sql} passes={} reopen_each_pass={} worker_threads={}",
        cli.mode, cli.passes, cli.reopen_each_pass, cli.worker_threads);

    let res: Result<Vec<PassStats>, Box<dyn std::error::Error>> = rt.block_on(async {
        let mut out = Vec::new();
        let mut held: Option<cqlite_core::Database> = None;
        for p in 0..cli.passes {
            if cli.reopen_each_pass || held.is_none() {
                // Fresh open: the per-pass open cost is INSIDE nothing timed
                // below, but a fresh open means no warm reader/index cache.
                held = Some(open_db(&cli, &scratch).await?);
            }
            let db = held.as_ref().expect("db opened above");
            let io_pre = proc_io();
            let st = run_pass(&cli, db, &sql).await?;
            let io_post = proc_io();
            let d = |k: &str| {
                io_post.get(k).copied().unwrap_or(0) - io_pre.get(k).copied().unwrap_or(0)
            };
            eprintln!(
                "pass {p}{}: rows={} cells={} secs={:.6} rows_per_sec={:.1} arrow_payload_bytes={} digest={:#x} \
                 io_read_bytes={} io_rchar={} io_syscr={}",
                if p == 0 { " (cold)" } else { " (warm)" },
                st.rows, st.cells, st.secs,
                st.rows as f64 / st.secs, st.arrow_bytes, st.digest,
                d("read_bytes"), d("rchar"), d("syscr")
            );
            out.push(st);
        }
        Ok(out)
    });

    let passes = match res {
        Ok(p) => p,
        Err(e) => {
            eprintln!("harness: FAILED: {e}");
            return std::process::ExitCode::from(1);
        }
    };

    // Doctrine: 0 rows on a present dataset is a FAILURE, never a pass.
    let rows = passes.first().map(|p| p.rows).unwrap_or(0);
    if rows == 0 {
        eprintln!(
            "harness: FAILURE — scan returned 0 rows on a PRESENT dataset. \
             Refusing to report a vacuous measurement."
        );
        return std::process::ExitCode::from(2);
    }
    // Every pass must agree on rows/cells/digest, else the measurement is unstable.
    for (i, p) in passes.iter().enumerate() {
        if p.rows != passes[0].rows || p.digest != passes[0].digest {
            eprintln!(
                "harness: FAILURE — pass {i} disagrees with pass 0 \
                 (rows {} vs {}, digest {:#x} vs {:#x}); non-deterministic scan.",
                p.rows, passes[0].rows, p.digest, passes[0].digest
            );
            return std::process::ExitCode::from(3);
        }
    }

    let total_rows: u64 = passes.iter().map(|p| p.rows).sum();
    let total_secs: f64 = passes.iter().map(|p| p.secs).sum();
    let warm: Vec<&PassStats> = passes.iter().skip(1).collect();
    let warm_rows: u64 = warm.iter().map(|p| p.rows).sum();
    let warm_secs: f64 = warm.iter().map(|p| p.secs).sum();

    let json = serde_json::json!({
        "schema": "ws0-scan-harness/v1",
        "mode": format!("{:?}", cli.mode),
        "keyspace": cli.keyspace,
        "table": cli.table,
        "sql": sql,
        "passes": cli.passes,
        "reopen_each_pass": cli.reopen_each_pass,
        "no_fold": cli.no_fold,
        "worker_threads": cli.worker_threads,
        "rows_per_pass": passes[0].rows,
        "cells_per_pass": passes[0].cells,
        "cells_per_row": passes[0].cells as f64 / passes[0].rows as f64,
        "digest": format!("{:#x}", passes[0].digest),
        "arrow_payload_bytes_per_pass": passes[0].arrow_bytes,
        "cold_secs": passes[0].secs,
        "cold_rows_per_sec": passes[0].rows as f64 / passes[0].secs,
        "warm_rows_per_sec": if warm_secs > 0.0 { warm_rows as f64 / warm_secs } else { f64::NAN },
        "all_pass_rows_total": total_rows,
        "all_pass_secs_total": total_secs,
        // Denominators for perf-stat post-processing: divide the counter deltas
        // by this to get per-row / per-cell figures over the WHOLE process.
        "perf_denominator_rows": total_rows,
        "perf_denominator_cells": passes.iter().map(|p| p.cells).sum::<u64>(),
        "peak_rss_kib": peak_rss_kib(),
        // Whole-process I/O. Reported SEPARATELY; do NOT divide these by each other.
        "proc_io_start": io_at_start,
        "proc_io_end": proc_io(),
    });
    println!("{json}");
    std::process::ExitCode::SUCCESS
}

/// Minimal scratch dir (avoids a tempfile dependency); leaked deliberately —
/// the Database holds live file handles into it for the process lifetime.
fn tempdir() -> std::io::Result<PathBuf> {
    let base = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".into());
    let p = PathBuf::from(base).join(format!("ws0-scan-{}", std::process::id()));
    std::fs::create_dir_all(&p)?;
    Ok(p)
}
