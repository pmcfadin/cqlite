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
//! ## WHICH PLANE THIS MEASURES (read this before quoting a number)
//!
//! This binary drives `Database::execute_streaming` to exhaustion. That is a
//! *library-level* entry point, **not** a single fixed storage route — and on the
//! #3234 production corpus (27 generations + a resolved schema) it does **NOT**
//! measure the mmap plane and does **NOT** measure `run_scan_stream`'s BTI trie
//! branch:
//!
//! - **Multi-generation + schema** (`readers.len() > 1 && schema.is_some()`,
//!   `write-support`; `storage/sstable/mod.rs:2141-2148`) routes into
//!   `generation_merge::stream_generations_for_read`, whose `KWayMerger` drives one
//!   sequential producer per generation. Each producer **RE-OPENS its SSTable with
//!   `use_mmap = false` / `DiskAccessMode::Buffered`**
//!   (`storage/write_engine/merge/producer_iter.rs:364-388`) and walks `Data.db`
//!   via `stream_all_partitions_for_compaction`. So the measured work is the
//!   **compaction-style BTI `Data.db` stitch + decode plus the k-way merge** over
//!   buffered I/O.
//! - **Single generation (or no resolved schema)** falls through to the per-reader
//!   `scan_stream` path, where a BTI reader takes the trie branch — a *different*
//!   plane with a *different* memory profile (see "Containment").
//!
//! Because the route is a function of the corpus and the schema, the harness
//! **prints the route it actually took** (`access_path:`, `generations:`,
//! `schema_resolved:`, `storage_route:`) in the result block, so a number can never
//! again be reported without the plane it describes. Profiling the mmap/trie plane
//! is a *separate* measurement — run it on a single-generation corpus (and expect
//! the memory behaviour below).
//!
//! ## Fail-closed contract
//!
//! A dataset-dependent measurement must never "pass" on an empty *or truncated*
//! corpus (CLAUDE.md, Testing). Hence:
//!
//! - a scan that yields **zero rows** exits `ZERO_ROWS` (4), never 0;
//! - the row-count assert is **ON by default**, read from the authoritative
//!   committed manifest (`test-data/perf-corpus-bti-manifest.json`, field
//!   `rows_per_partition.rows`, recorded "observed, not requested") or from the
//!   corpus-local `manifest-bti-3234.json`. A disagreement exits
//!   `ROW_COUNT_MISMATCH` (5); an absent/unparseable/wrong-table manifest exits
//!   `MANIFEST_UNREADABLE` (8) — it never degrades to "assert off". This is the
//!   guard that catches a **silently truncated** scan: `execute_streaming` surfaces
//!   producer *errors* as a terminal `Err`, but a producer *panic* drops its
//!   `JoinHandle` and closes the channel (the #3124 class), which the consumer sees
//!   as a clean end-of-stream — a short row count is the only signal;
//! - `--min-seconds S` (default 10.0, the AC3 floor) makes a scan that does not
//!   sustain the window exit `WINDOW_TOO_SHORT` (6) — so an under-sized corpus is
//!   reported, never silently accepted. `S` must be finite and positive; the floor
//!   is disabled only by the explicit, loudly-reported `--no-min-seconds`.
//!
//! Both asserts have an explicit opt-**out** (`--no-expect-rows`,
//! `--no-min-seconds`) for a hand-built corpus. Each prints a `*** DISABLED ***`
//! banner in the result block and in the `RESULT:` line, so a measurement taken
//! without its guards is self-identifying.
//!
//! ## Containment
//!
//! The corpus is multi-GB. ALWAYS run it under
//! `test-data/scripts/perf-run-contained.sh` — an uncontained multi-GB scan
//! livelocked a swapless host for 75 minutes (issue #3068).
//!
//! Peak RSS is **not** bounded by the 1024-row streaming channel on every route.
//! On the multi-generation merge route above, rows are consumed and dropped as they
//! arrive, so the window is bounded. But a **single-generation** (or
//! schema-less) invocation against a BTI corpus takes the `run_scan_stream` BTI
//! branch, which **pre-materializes the whole reconciled table** before streaming
//! (issue #1577 — the exact condition `scan_stream_materializes` reports `true` on,
//! `storage/sstable/mod.rs:2045-2054`). On a multi-GB BTI corpus that is a
//! multi-GB allocation and is precisely the #3068 livelock shape. Do not assume
//! the channel bounds it; run contained.
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
//!
//! Guard coverage: `scripts/tests/test_bti_perf_scan.sh` observes every exit code
//! above actually firing, hermetically, against the committed `test_da` BTI
//! fixture (468 rows) — no perf corpus and no multi-minute run required.

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

/// Exit codes, so a caller can distinguish "corpus missing" from "too fast" from
/// "the scan blew up half way through". Pinned by `scripts/tests/test_bti_perf_scan.sh`.
#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
mod exit {
    pub const OK: i32 = 0;
    pub const USAGE: i32 = 2;
    /// Corpus/schema absent, or ingest (discovery + open) failed. Nothing was measured.
    pub const OPEN_FAILED: i32 = 3;
    pub const ZERO_ROWS: i32 = 4;
    pub const ROW_COUNT_MISMATCH: i32 = 5;
    pub const WINDOW_TOO_SHORT: i32 = 6;
    /// A scan that STARTED then failed: `execute_streaming` errored, or a row
    /// failed to decode mid-stream. Deliberately distinct from `OPEN_FAILED` — the
    /// corpus is present and openable, so this is a read-path defect, not a
    /// missing fixture.
    pub const SCAN_FAILED: i32 = 7;
    /// The authoritative row count could not be established: no manifest, an
    /// unparseable one, or one describing a different table. Fail-closed rather
    /// than silently running without the truncation guard.
    pub const MANIFEST_UNREADABLE: i32 = 8;
}

/// How the measured row count is verified. ON by default (`Manifest`).
#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
enum RowsAssert {
    /// Read from a manifest: `(rows, provenance)`.
    Manifest(u64, String),
    /// Operator-supplied `--expect-rows N`.
    Explicit(u64),
    /// Operator opted out with `--no-expect-rows`.
    Disabled,
}

#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
impl RowsAssert {
    fn expected(&self) -> Option<u64> {
        match self {
            RowsAssert::Manifest(n, _) | RowsAssert::Explicit(n) => Some(*n),
            RowsAssert::Disabled => None,
        }
    }

    fn describe(&self) -> String {
        match self {
            RowsAssert::Manifest(n, src) => format!("{n} (authoritative: {src})"),
            RowsAssert::Explicit(n) => {
                format!("{n} (--expect-rows, OPERATOR-SUPPLIED — not the committed manifest)")
            }
            RowsAssert::Disabled => "*** DISABLED (--no-expect-rows) — a SILENTLY TRUNCATED scan \
                                     CANNOT be detected; this measurement is unverified ***"
                .to_string(),
        }
    }
}

#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
struct Args {
    corpus: std::path::PathBuf,
    keyspace: String,
    table: String,
    /// Discarded page-cache-warming passes before the measured pass. 0 = the
    /// measured pass is COLD and is labelled as such (it is NOT the AC3 number).
    warm_passes: usize,
    /// AC3 sustained-window floor in seconds. `None` only via `--no-min-seconds`;
    /// any value here is finite and > 0 (validated in `parse_args`).
    min_seconds: Option<f64>,
    /// `Some(n)` = `--expect-rows n`; `None` + `expect_rows_off == false` = read
    /// the manifest (the default).
    expect_rows: Option<u64>,
    expect_rows_off: bool,
    /// Explicit manifest path; otherwise resolved from the corpus / the checkout.
    manifest: Option<std::path::PathBuf>,
}

#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn usage() -> String {
    "usage: bti_perf_scan --corpus DIR [--keyspace KS] [--table T]\n\
     \x20                    [--warm-passes N] [--min-seconds S | --no-min-seconds]\n\
     \x20                    [--manifest PATH | --expect-rows N | --no-expect-rows]\n\
     \n\
     --corpus DIR        corpus root holding sstables/<ks>/<table>-<uuid>/ and schema.cql\n\
     --keyspace KS       keyspace [perf_bti]\n\
     --table T           table [wide_multiclustering]\n\
     --warm-passes N     discarded warming scans before the measured one [1];\n\
     \x20                   0 makes the measured pass COLD (labelled, not the AC3 number)\n\
     --min-seconds S     fail if the measured scan is shorter (AC3 floor) [10.0];\n\
     \x20                   S must be finite and > 0\n\
     --no-min-seconds    disable the AC3 window floor (reported loudly)\n\
     --manifest PATH     manifest to read the authoritative row count from\n\
     \x20                   [<corpus>/manifest-bti-3234.json, else the committed\n\
     \x20                    test-data/perf-corpus-bti-manifest.json]\n\
     --expect-rows N     override the manifest row count with N (reported loudly)\n\
     --no-expect-rows    disable the row-count assert (reported loudly)\n\
     \n\
     exit: 0 pass | 2 usage | 3 corpus missing/open failed | 4 zero rows\n\
     \x20     5 row-count mismatch | 6 window under the floor | 7 scan failed mid-stream\n\
     \x20     8 authoritative row count unavailable (manifest missing/unparseable/other table)\n"
        .to_string()
}

#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn parse_args() -> std::result::Result<Args, String> {
    let mut corpus: Option<std::path::PathBuf> = None;
    let mut keyspace = "perf_bti".to_string();
    let mut table = "wide_multiclustering".to_string();
    let mut warm_passes: usize = 1;
    let mut min_seconds: Option<f64> = Some(10.0);
    let mut expect_rows: Option<u64> = None;
    let mut expect_rows_off = false;
    let mut manifest: Option<std::path::PathBuf> = None;

    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        let mut value = || argv.next().ok_or_else(|| format!("{arg} requires a value"));
        match arg.as_str() {
            "--corpus" => corpus = Some(std::path::PathBuf::from(value()?)),
            "--keyspace" => keyspace = value()?,
            "--table" => table = value()?,
            "--manifest" => manifest = Some(std::path::PathBuf::from(value()?)),
            "--warm-passes" => {
                warm_passes = value()?
                    .parse()
                    .map_err(|e| format!("--warm-passes: {e}"))?
            }
            "--min-seconds" => {
                // Issue #3234 (rust-reviewer B2): `f64::parse` happily accepts
                // `nan`, `inf` and `-5`, every one of which silently DISABLED the
                // AC3 floor under the old `min_seconds > 0.0` test while the header
                // still printed a gate value. Reject them here instead.
                let raw = value()?;
                let parsed: f64 = raw
                    .parse()
                    .map_err(|e| format!("--min-seconds '{raw}': {e}"))?;
                if !parsed.is_finite() || parsed <= 0.0 {
                    return Err(format!(
                        "--min-seconds '{raw}': expected a finite, positive number of seconds \
                         (use --no-min-seconds to disable the AC3 window floor)"
                    ));
                }
                min_seconds = Some(parsed);
            }
            "--no-min-seconds" => min_seconds = None,
            "--expect-rows" => {
                expect_rows = Some(
                    value()?
                        .parse()
                        .map_err(|e| format!("--expect-rows: {e}"))?,
                )
            }
            "--no-expect-rows" => expect_rows_off = true,
            "-h" | "--help" => {
                print!("{}", usage());
                std::process::exit(exit::OK);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    if expect_rows_off && expect_rows.is_some() {
        return Err("--expect-rows and --no-expect-rows are mutually exclusive".to_string());
    }
    if let Some(0) = expect_rows {
        // 0 used to mean "off"; it now means "I expect zero rows", which the
        // ZERO_ROWS guard rejects unconditionally. Say so rather than confusing.
        return Err(
            "--expect-rows 0: a zero-row scan is always a failure (exit 4); use \
             --no-expect-rows to disable the assert"
                .to_string(),
        );
    }

    let corpus = corpus.ok_or_else(|| "--corpus is required".to_string())?;
    Ok(Args {
        corpus,
        keyspace,
        table,
        warm_passes,
        min_seconds,
        expect_rows,
        expect_rows_off,
        manifest,
    })
}

/// Candidate manifests, most specific first: the corpus's own manifest (written by
/// the generator for *these* bytes), then the committed one resolved from the
/// checkout this binary was built in, then a CWD-relative fallback.
#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn manifest_candidates(corpus: &std::path::Path) -> Vec<std::path::PathBuf> {
    vec![
        corpus.join("manifest-bti-3234.json"),
        std::path::PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../test-data/perf-corpus-bti-manifest.json"
        )),
        std::path::PathBuf::from("test-data/perf-corpus-bti-manifest.json"),
    ]
}

/// Read the authoritative row count out of a #3234 corpus manifest.
///
/// Returns `(keyspace, table, rows)`. Every failure is an `Err`: a manifest that
/// is present but unreadable must NEVER degrade into "assert off" (rust-reviewer
/// B1) — that is exactly how a truncated scan measures as a PASS.
#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn read_manifest_rows(
    path: &std::path::Path,
) -> std::result::Result<(String, String, u64), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let json: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| format!("{}: not valid JSON: {e}", path.display()))?;

    let string_field = |name: &str| -> std::result::Result<String, String> {
        json.get(name)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| format!("{}: missing string field `{name}`", path.display()))
    };
    let keyspace = string_field("keyspace")?;
    let table = string_field("table")?;

    // `rows_per_partition.rows` is the count OBSERVED while writing the CSV chunks
    // (the manifest records its own provenance as "observed, not requested"), which
    // is why it — and not `row_driver_config.rows_requested` — is authoritative.
    let rows = json
        .get("rows_per_partition")
        .and_then(|v| v.get("rows"))
        .and_then(|v| v.as_u64())
        .ok_or_else(|| {
            format!(
                "{}: missing unsigned-integer field `rows_per_partition.rows`",
                path.display()
            )
        })?;
    if rows == 0 {
        return Err(format!(
            "{}: `rows_per_partition.rows` is 0 — a manifest describing an empty corpus \
             cannot verify a measurement",
            path.display()
        ));
    }

    // The production manifest also records the generator's fail-closed cross-check
    // (row-driver plan vs each `Statistics.db`). If it is present, the two sides must
    // agree with each other AND with the count we are about to assert on.
    //
    // This reads the four NUMBERS. It used to read an `agree: true` flag beside them,
    // which the writer emitted as a literal — i.e. it trusted a claim where the evidence
    // for that claim was in the same object. The flag is gone from the manifest
    // (issue #3234 review round 10: a field is observed or absent), and comparing the
    // numbers is a strictly stronger check than believing the flag was.
    if let Some(x) = json.get("row_count_cross_check") {
        for (a, b) in [
            ("row_driver_rows", "statistics_db_rows"),
            ("row_driver_partitions", "statistics_db_partitions"),
        ] {
            match (
                x.get(a).and_then(|v| v.as_u64()),
                x.get(b).and_then(|v| v.as_u64()),
            ) {
                (Some(va), Some(vb)) if va != vb => {
                    return Err(format!(
                        "{}: `row_count_cross_check.{a}` = {va} disagrees with `{b}` = {vb} — \
                         the manifest itself reports a cross-check disagreement",
                        path.display()
                    ))
                }
                _ => {}
            }
        }
        for name in ["row_driver_rows", "statistics_db_rows"] {
            if let Some(v) = x.get(name).and_then(|v| v.as_u64()) {
                if v != rows {
                    return Err(format!(
                        "{}: `row_count_cross_check.{name}` = {v} disagrees with \
                         `rows_per_partition.rows` = {rows}",
                        path.display()
                    ));
                }
            }
        }
    }

    Ok((keyspace, table, rows))
}

/// Resolve the row-count assert, fail-closed. `Err(message)` => exit `MANIFEST_UNREADABLE`.
#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn resolve_rows_assert(args: &Args) -> std::result::Result<RowsAssert, String> {
    if args.expect_rows_off {
        return Ok(RowsAssert::Disabled);
    }
    if let Some(n) = args.expect_rows {
        return Ok(RowsAssert::Explicit(n));
    }

    let candidates: Vec<std::path::PathBuf> = match &args.manifest {
        Some(p) => vec![p.clone()],
        None => manifest_candidates(&args.corpus),
    };
    // The FIRST candidate that exists is authoritative. A present-but-broken
    // manifest is an error, never a reason to fall through to another one.
    let found = candidates.iter().find(|p| p.exists()).ok_or_else(|| {
        format!(
            "no #3234 corpus manifest found, so the authoritative row count is unknown and a \
             truncated scan could not be detected.\n  looked at:\n{}\n  \
             remedy: pass --manifest PATH, or --expect-rows N, or --no-expect-rows to measure \
             without the truncation guard (reported loudly).",
            candidates
                .iter()
                .map(|p| format!("    {}", p.display()))
                .collect::<Vec<_>>()
                .join("\n")
        )
    })?;

    let (ks, tbl, rows) = read_manifest_rows(found)?;
    if ks != args.keyspace || tbl != args.table {
        return Err(format!(
            "{} describes {ks}.{tbl}, but this run scans {}.{} — its row count is not \
             authoritative here.\n  remedy: pass --manifest PATH for the right corpus, or \
             --expect-rows N, or --no-expect-rows.",
            found.display(),
            args.keyspace,
            args.table
        ));
    }
    Ok(RowsAssert::Manifest(
        rows,
        format!("{} rows_per_partition.rows", found.display()),
    ))
}

/// Rows/second as text. Never prints `inf`/`NaN`: a window at or below the clock
/// resolution has no meaningful rate (rust-reviewer B2).
#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn rate_text(rows: u64, secs: f64) -> String {
    match rate(rows, secs) {
        Some(r) => format!("{r:.0}"),
        None => "n/a (window at or below clock resolution)".to_string(),
    }
}

#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn rate(rows: u64, secs: f64) -> Option<f64> {
    if secs.is_finite() && secs > 0.0 {
        Some(rows as f64 / secs)
    } else {
        None
    }
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

    // Resolve the row-count assert BEFORE spending minutes on a scan whose result
    // could not be verified anyway.
    let rows_assert = match resolve_rows_assert(&args) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {e}");
            return exit::MANIFEST_UNREADABLE;
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
    let generations = ingested.discovery_summary.sstables_found;
    println!(
        "open: {generations} sstables discovered in {:.3} s ({})",
        open_elapsed.as_secs_f64(),
        data_dir.display()
    );

    // One streaming scan to exhaustion. Returns the row count. Rows are dropped as
    // they arrive, so THIS consumer holds only the streaming window — but see the
    // module docs: on a single-generation BTI corpus the PRODUCER side
    // pre-materializes the whole reconciled table (issue #1577), so peak RSS is a
    // property of the route, not of the 1024-row channel.
    let scan = |label: &str| -> (u64, std::time::Duration) {
        let start = std::time::Instant::now();
        let rows = rt.block_on(async {
            let mut iter = match db.execute_streaming(&sql, StreamingConfig::default()).await {
                Ok(it) => it,
                Err(e) => {
                    eprintln!("error: {label}: execute_streaming failed: {e}");
                    std::process::exit(exit::SCAN_FAILED);
                }
            };
            let mut n: u64 = 0;
            while let Some(row) = iter.next_async().await {
                match row {
                    Ok(_) => n += 1,
                    Err(e) => {
                        eprintln!("error: {label}: row {n} failed to decode: {e}");
                        std::process::exit(exit::SCAN_FAILED);
                    }
                }
            }
            n
        });
        (rows, start.elapsed())
    };

    // A short count in ANY pass means the scan was truncated, which invalidates the
    // whole measurement — so the warming passes are gated too, even though their
    // timings are discarded.
    let check_rows = |label: &str, rows: u64| -> Option<i32> {
        if rows == 0 {
            eprintln!(
                "FAIL: {label}: zero rows scanned from {qualified} — corpus empty or unreadable. \
                 A measurement over an empty corpus is a failure, not a pass."
            );
            return Some(exit::ZERO_ROWS);
        }
        match rows_assert.expected() {
            Some(expected) if rows != expected => {
                eprintln!(
                    "FAIL: {label}: scanned {rows} rows, expected {expected} — {}.\n  \
                     A short count is how a SILENTLY TRUNCATED scan shows up: a producer panic \
                     drops its JoinHandle and closes the channel, which the consumer sees as a \
                     clean end-of-stream (issue #3124 class).",
                    rows_assert.describe()
                );
                Some(exit::ROW_COUNT_MISMATCH)
            }
            _ => None,
        }
    };

    for pass in 0..args.warm_passes {
        let label = format!("warm pass {pass}");
        let (rows, elapsed) = scan(&label);
        println!(
            "warm-pass {pass}: {rows} rows in {:.3} s ({} rows/s) [discarded]",
            elapsed.as_secs_f64(),
            rate_text(rows, elapsed.as_secs_f64())
        );
        if let Some(code) = check_rows(&label, rows) {
            return code;
        }
    }

    let (rows, elapsed) = scan("measured pass");
    let secs = elapsed.as_secs_f64();

    // Issue #3234 (rust-reviewer S3): report the ROUTE beside the number. The
    // access-path probe is reset per query by the streaming executor
    // (`select_executor/mod.rs:525`), so this reads THIS query's path.
    let access_path = cqlite_core::query::access_path::last()
        .map(|p| p.to_string())
        .unwrap_or_else(|| "<none recorded>".to_string());
    let schema_resolved = rt.block_on(async {
        db.has_schema_for_table(&qualified).await || db.has_schema_for_table(&args.table).await
    });
    // The exact branch predicate at `storage/sstable/mod.rs:2141-2148`, evaluated on
    // the inputs printed above it — not a guess about what "should" have happened.
    let multi_gen_merge = cfg!(feature = "write-support") && generations > 1 && schema_resolved;
    let storage_route = if multi_gen_merge {
        "generation_merge::stream_generations_for_read — KWayMerger over one sequential \
         compaction-style producer per generation; each producer RE-OPENS its SSTable with \
         use_mmap=false / DiskAccessMode::Buffered (write_engine/merge/producer_iter.rs:364-388) \
         and walks Data.db via stream_all_partitions_for_compaction. NOT the mmap plane, NOT \
         run_scan_stream's BTI trie branch."
    } else {
        "per-reader SSTableManager::scan_stream — for a BTI reader this is run_scan_stream's trie \
         branch, which PRE-MATERIALIZES the whole reconciled table before streaming (issue #1577)."
    };

    let label = if args.warm_passes == 0 {
        "COLD"
    } else {
        "WARM"
    };
    println!("--- AC3 measured {label} full scan ---");
    println!("query:            {sql}");
    println!("rows_scanned:     {rows}");
    println!("wall_clock_s:     {secs:.3}");
    println!("rows_per_s:       {}", rate_text(rows, secs));
    if args.warm_passes == 0 {
        println!(
            "warm_passes:      0  *** COLD: this harness warmed nothing — NOT the AC3 warm \
             measurement ***"
        );
    } else {
        println!("warm_passes:      {}", args.warm_passes);
    }
    println!("row_count_assert: {}", rows_assert.describe());
    match args.min_seconds {
        Some(s) => println!("min_seconds_gate: {s:.3}"),
        None => println!(
            "min_seconds_gate: *** DISABLED (--no-min-seconds) — a window too short to profile \
             CANNOT be detected ***"
        ),
    }
    println!("generations:      {generations}");
    println!("schema_resolved:  {schema_resolved}");
    println!("access_path:      {access_path}");
    println!("storage_route:    {storage_route}");

    // Fail-closed: zero rows, or a row count that disagrees with the authority.
    if let Some(code) = check_rows("measured pass", rows) {
        return code;
    }
    if let Some(floor) = args.min_seconds {
        if secs < floor {
            // Report the corpus size that WOULD reach the floor, so an under-sized
            // corpus is diagnosed rather than silently scaled up.
            let scale = match rate(rows, secs) {
                Some(r) => {
                    let needed = (r * floor).ceil() as u64;
                    format!(
                        "At {r:.0} rows/s a >= {floor:.3} s window needs ~{needed} rows \
                         ({:.2}x this corpus).",
                        needed as f64 / rows as f64
                    )
                }
                None => "The window was at or below the clock resolution, so no rows/s rate \
                         (and no target row count) can be derived."
                    .to_string(),
            };
            eprintln!(
                "FAIL: {label} scan sustained only {secs:.3} s, under the {floor:.3} s AC3 \
                 floor.\n  {scale}"
            );
            return exit::WINDOW_TOO_SHORT;
        }
    }

    let mut caveats: Vec<&str> = Vec::new();
    if matches!(rows_assert, RowsAssert::Disabled) {
        caveats.push("row-count assert DISABLED — truncation undetectable");
    }
    if args.min_seconds.is_none() {
        caveats.push("window floor DISABLED");
    }
    if args.warm_passes == 0 {
        caveats.push("COLD, not warm");
    }
    let window = match args.min_seconds {
        Some(floor) => format!("{label} window {secs:.3} s >= {floor:.3} s"),
        None => format!("{label} window {secs:.3} s, no floor"),
    };
    if caveats.is_empty() {
        println!("RESULT: PASS ({window})");
    } else {
        println!(
            "RESULT: PASS ({window}) *** UNGUARDED: {} ***",
            caveats.join("; ")
        );
    }
    exit::OK
}
