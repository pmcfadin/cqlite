//! Timed warm/cold full-scan harness for the #3234 BTI perf corpus (issue #3234 AC3).
//!
//! WHY THIS EXISTS, and why it is not a criterion bench or the Flight harness:
//!
//! - AC3 requires a **sustained >= 10 s warm full scan** so a profiler
//!   (`perf record`, a flamegraph) has a real steady-state window to sample. A
//!   criterion bench cannot express that cheaply: criterion would run warm-up
//!   *plus* N sample iterations of a ~10 s scan, i.e. minutes of wall clock per
//!   invocation, and it reports a distribution rather than the single sustained
//!   window a profile needs.
//! - The Flight `do_get` harness under `docs/reports/ws0-3217-artifacts/harness/`
//!   measures the Flight plane, and per issue #3233 BTI is denied the Flight
//!   bypass arm — so it cannot measure the BTI read path at all.
//!
//! This binary therefore drives the BARE read path: `Database::execute_streaming`
//! to exhaustion over the whole table, timed with a monotonic clock.
//!
//! ## Fail-closed contract
//!
//! A dataset-dependent measurement must never "pass" on an empty corpus
//! (CLAUDE.md, Testing). Hence:
//!
//! - a scan that yields **zero rows** exits non-zero (code 4), never 0;
//! - `--expect-rows N` (optional) makes a row-count disagreement with the
//!   generator manifest a hard failure (code 5);
//! - `--min-seconds S` (default 10.0, the AC3 floor) makes a scan that does not
//!   sustain the window exit non-zero (code 6) — so an under-sized corpus is
//!   reported, never silently accepted.
//!
//! ## Containment
//!
//! The corpus is multi-GB and mmap-backed. ALWAYS run it under
//! `test-data/scripts/perf-run-contained.sh` — an uncontained multi-GB scan
//! livelocked a swapless host for 75 minutes (issue #3068).
//!
//! ```text
//! bash test-data/scripts/perf-run-contained.sh --mem 12G --swap 0 -- \
//!   ./target/release/examples/bti_perf_scan \
//!     --corpus /data/corpus-3234-bti-full \
//!     --keyspace perf_bti --table wide_multiclustering
//! ```
//!
//! Build (the `ingestion` module that opens a corpus lives behind `cli-helpers`,
//! exactly as for `heap_profile`):
//!
//! ```text
//! cargo build --release --package cqlite-core --example bti_perf_scan \
//!     --features cli-helpers
//! ```

#[cfg(not(all(feature = "cli-helpers", feature = "state_machine")))]
fn main() {
    eprintln!(
        "bti_perf_scan requires `cli-helpers` (corpus ingestion) + `state_machine`\n  \
         cargo build --release -p cqlite-core --example bti_perf_scan --features cli-helpers"
    );
    std::process::exit(2);
}

#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn main() {
    std::process::exit(real_main());
}

/// Exit codes, so a caller can distinguish "corpus missing" from "too fast".
#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
mod exit {
    pub const OK: i32 = 0;
    pub const USAGE: i32 = 2;
    pub const OPEN_FAILED: i32 = 3;
    pub const ZERO_ROWS: i32 = 4;
    pub const ROW_COUNT_MISMATCH: i32 = 5;
    pub const WINDOW_TOO_SHORT: i32 = 6;
}

#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
struct Args {
    corpus: std::path::PathBuf,
    keyspace: String,
    table: String,
    /// Discarded page-cache-warming passes before the measured pass.
    warm_passes: usize,
    /// AC3 sustained-window floor in seconds; 0 disables the assert.
    min_seconds: f64,
    /// Expected row count (generator manifest); 0 disables the assert.
    expect_rows: u64,
}

#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn usage() -> String {
    "usage: bti_perf_scan --corpus DIR [--keyspace KS] [--table T]\n\
     \x20                    [--warm-passes N] [--min-seconds S] [--expect-rows N]\n\
     \n\
     --corpus DIR        corpus root holding sstables/<ks>/<table>-<uuid>/ and schema.cql\n\
     --keyspace KS       keyspace [perf_bti]\n\
     --table T           table [wide_multiclustering]\n\
     --warm-passes N     discarded warming scans before the measured one [1]\n\
     --min-seconds S     fail if the measured scan is shorter (AC3 floor); 0 = off [10.0]\n\
     --expect-rows N     fail if the scanned row count differs; 0 = off [0]\n"
        .to_string()
}

#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn parse_args() -> std::result::Result<Args, String> {
    let mut corpus: Option<std::path::PathBuf> = None;
    let mut keyspace = "perf_bti".to_string();
    let mut table = "wide_multiclustering".to_string();
    let mut warm_passes: usize = 1;
    let mut min_seconds: f64 = 10.0;
    let mut expect_rows: u64 = 0;

    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        let mut value = || argv.next().ok_or_else(|| format!("{arg} requires a value"));
        match arg.as_str() {
            "--corpus" => corpus = Some(std::path::PathBuf::from(value()?)),
            "--keyspace" => keyspace = value()?,
            "--table" => table = value()?,
            "--warm-passes" => {
                warm_passes = value()?
                    .parse()
                    .map_err(|e| format!("--warm-passes: {e}"))?
            }
            "--min-seconds" => {
                min_seconds = value()?
                    .parse()
                    .map_err(|e| format!("--min-seconds: {e}"))?
            }
            "--expect-rows" => {
                expect_rows = value()?
                    .parse()
                    .map_err(|e| format!("--expect-rows: {e}"))?
            }
            "-h" | "--help" => {
                print!("{}", usage());
                std::process::exit(exit::OK);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    let corpus = corpus.ok_or_else(|| "--corpus is required".to_string())?;
    Ok(Args {
        corpus,
        keyspace,
        table,
        warm_passes,
        min_seconds,
        expect_rows,
    })
}

#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn real_main() -> i32 {
    use cqlite_core::ingestion::{ingest, IngestionConfig};
    use cqlite_core::query::result::StreamingConfig;

    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}\n\n{}", usage());
            return exit::USAGE;
        }
    };

    let data_dir = args.corpus.join("sstables");
    let schema = args.corpus.join("schema.cql");
    for (label, path) in [("sstables dir", &data_dir), ("schema.cql", &schema)] {
        if !path.exists() {
            eprintln!(
                "error: {label} not found at {}\n  \
                 generate the corpus first: bash test-data/scripts/gen-perf-corpus-bti.sh --out {}",
                path.display(),
                args.corpus.display()
            );
            return exit::OPEN_FAILED;
        }
    }

    let qualified = format!("{}.{}", args.keyspace, args.table);
    let sql = format!("SELECT * FROM {qualified}");

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: tokio runtime: {e}");
            return exit::OPEN_FAILED;
        }
    };

    let cfg = IngestionConfig {
        schema_paths: vec![schema],
        data_dir: data_dir.clone(),
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{}/{}", args.keyspace, args.table)),
    };

    let open_start = std::time::Instant::now();
    let ingested = match rt.block_on(ingest(cfg)) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("error: ingest of {} failed: {e}", data_dir.display());
            return exit::OPEN_FAILED;
        }
    };
    let open_elapsed = open_start.elapsed();
    let db = ingested.database;
    println!(
        "open: {} sstables discovered in {:.3} s ({})",
        ingested.discovery_summary.sstables_found,
        open_elapsed.as_secs_f64(),
        data_dir.display()
    );

    // One streaming scan to exhaustion. Returns the row count; rows are dropped
    // as they arrive so peak RSS stays bounded by the 1024-row channel buffer,
    // not by the corpus size.
    let scan = |label: &str| -> (u64, std::time::Duration) {
        let start = std::time::Instant::now();
        let rows = rt.block_on(async {
            let mut iter = match db.execute_streaming(&sql, StreamingConfig::default()).await {
                Ok(it) => it,
                Err(e) => {
                    eprintln!("error: {label}: execute_streaming failed: {e}");
                    std::process::exit(exit::OPEN_FAILED);
                }
            };
            let mut n: u64 = 0;
            while let Some(row) = iter.next_async().await {
                match row {
                    Ok(_) => n += 1,
                    Err(e) => {
                        eprintln!("error: {label}: row {n} failed to decode: {e}");
                        std::process::exit(exit::OPEN_FAILED);
                    }
                }
            }
            n
        });
        (rows, start.elapsed())
    };

    for pass in 0..args.warm_passes {
        let (rows, elapsed) = scan(&format!("warm pass {pass}"));
        println!(
            "warm-pass {pass}: {rows} rows in {:.3} s ({:.0} rows/s) [discarded]",
            elapsed.as_secs_f64(),
            rows as f64 / elapsed.as_secs_f64()
        );
    }

    let (rows, elapsed) = scan("measured pass");
    let secs = elapsed.as_secs_f64();
    let rows_per_s = if secs > 0.0 { rows as f64 / secs } else { 0.0 };
    println!("--- AC3 measured WARM full scan ---");
    println!("query:            {sql}");
    println!("rows_scanned:     {rows}");
    println!("wall_clock_s:     {secs:.3}");
    println!("rows_per_s:       {rows_per_s:.0}");
    println!("warm_passes:      {}", args.warm_passes);
    println!("min_seconds_gate: {:.3}", args.min_seconds);

    // Fail-closed: a dataset-dependent measurement must never pass on 0 rows.
    if rows == 0 {
        eprintln!(
            "FAIL: zero rows scanned from {qualified} — corpus empty or unreadable. \
             A measurement over an empty corpus is a failure, not a pass."
        );
        return exit::ZERO_ROWS;
    }
    if args.expect_rows != 0 && rows != args.expect_rows {
        eprintln!(
            "FAIL: scanned {rows} rows but --expect-rows {} (manifest row count)",
            args.expect_rows
        );
        return exit::ROW_COUNT_MISMATCH;
    }
    if args.min_seconds > 0.0 && secs < args.min_seconds {
        // Report the corpus size that WOULD reach the floor, so an under-sized
        // corpus is diagnosed rather than silently scaled up.
        let needed_rows = (rows_per_s * args.min_seconds).ceil() as u64;
        eprintln!(
            "FAIL: warm scan sustained only {secs:.3} s, under the {:.3} s AC3 floor.\n  \
             At {rows_per_s:.0} rows/s a >= {:.3} s window needs ~{needed_rows} rows \
             ({:.2}x this corpus).",
            args.min_seconds,
            args.min_seconds,
            needed_rows as f64 / rows as f64
        );
        return exit::WINDOW_TOO_SHORT;
    }
    println!("RESULT: PASS (warm window {secs:.3} s >= {:.3} s)", args.min_seconds);
    exit::OK
}
