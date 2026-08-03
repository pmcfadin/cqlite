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
//! There is a third fail-closed guard, and it is not about the row count at all: the
//! **ingest scope** (`scope.rs`, roborev #3234 M1/F1). The row-count assert cannot see a
//! workload change — a retained `<table>-<uuid>` generation beside the measured one
//! (the shape is `scope::is_table_dir`: `<table>-` + exactly `TABLE_ID_HEX_LEN` hex)
//! holds the SAME rows, so reconciliation yields the same count while the GENERATION
//! COUNT (which selects the scan route) silently doubles. So ingestion is confined to
//! the manifest's exact `tables[].sstable_dir`, an ambiguous root is refused
//! (`OPEN_FAILED`), and the resolved directory + how it was chosen are printed as
//! `ingest_scope:`.
//!
//! "Confined" is EXACT, not a filter: the scope is passed as
//! `TableDirSelection::Exact`, which compares complete path components, because a
//! substring `table_directory_filter` of `/<ks>/<dir>` also matches a sibling whose full
//! name extends it (`<table>-<uuid>-backup`). And `generations:` is the count OBSERVED in
//! what ingestion selected, never in what this run intended to select — reporting the
//! intended count is how extra SSTables could be scanned while the smaller number was
//! printed (roborev #3234 F1).
//!
//! Module layout (split per the campsite rule, epic #1116): `main.rs` = flags, the
//! timed scan and the result block; `manifest.rs` = the authoritative row count and the
//! documented scope, read fail-closed; `scope.rs` = which directory may be ingested.
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

/// The authoritative row count + the documented ingest scope (fail-closed manifest
/// reading, roborev #3234 L3/M2).
#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
mod manifest;
/// The ONE corpus directory this run may ingest (roborev #3234 M1).
#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
mod scope;

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
     \x20                   (<uuid> = 32 hex digits, per scope::is_table_dir)\n\
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

/// The generation count OBSERVED in the directories ingestion actually selected
/// (roborev #3234 F1).
///
/// This is deliberately NOT `scope.generations` (the count of the directory this run
/// INTENDED to ingest). Reporting the intended count is exactly how round 11's substring
/// filter could open a `<table>-<uuid>-backup` sibling's SSTables (a name `is_table_dir`
/// rejects, being `<table>-` + something that is not 32 hex) and still print the
/// smaller number — and since the generation count selects the scan route, that
/// misattributes every figure printed beside it. An unreadable selected directory is an
/// error, never a count silently short by one.
#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn observed_generations(selected: &[std::path::PathBuf]) -> std::result::Result<usize, String> {
    let mut total = 0usize;
    for dir in selected {
        total += scope::count_generations(dir)?;
    }
    Ok(total)
}

/// The selected set must be EXACTLY the resolved scope — one complete-path-component
/// identity, nothing extending it, nothing missing (roborev #3234 F1).
#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn verify_scope_exact(
    selected: &[std::path::PathBuf],
    scope: &scope::IngestScope,
) -> std::result::Result<(), String> {
    let same = |a: &std::path::Path, b: &std::path::Path| match (a.canonicalize(), b.canonicalize())
    {
        (Ok(x), Ok(y)) => x == y,
        _ => false,
    };
    let foreign: Vec<String> = selected
        .iter()
        .filter(|d| !same(d, &scope.dir))
        .map(|d| d.display().to_string())
        .collect();
    if !foreign.is_empty() {
        return Err(format!(
            "ingestion selected {} directory/ies OUTSIDE the resolved scope {}:\n    {}\n  \
             The generation count selects the scan route, so a measurement over an unintended \
             union describes a workload no manifest documents. Refusing.",
            foreign.len(),
            scope.dir.display(),
            foreign.join("\n    ")
        ));
    }
    // Nothing selected is legitimate only when there was nothing to select: an EMPTY
    // table directory. Otherwise discovery skipped the scope and a scan would measure
    // nothing at all while the scope claims generations.
    if selected.is_empty() && scope.generations > 0 {
        return Err(format!(
            "discovery returned NONE of the {} `*-Data.db` generation(s) in the resolved scope \
             {} — nothing was ingested, so the scan below would measure nothing while the scope \
             claims otherwise. Refusing.",
            scope.generations,
            scope.dir.display()
        ));
    }
    Ok(())
}

#[cfg(all(feature = "cli-helpers", feature = "state_machine"))]
fn real_main() -> i32 {
    use cqlite_core::ingestion::{ingest_with_selection, IngestionConfig, TableDirSelection};
    use cqlite_core::query::result::StreamingConfig;
    use manifest::{resolve_rows_assert, RowsAssert};

    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}\n\n{}", usage());
            return exit::USAGE;
        }
    };

    // Resolve the row-count assert BEFORE spending minutes on a scan whose result
    // could not be verified anyway. It also yields the ingest scope the manifest
    // DOCUMENTS, which is what the scan is then confined to (roborev #3234 M1).
    let (rows_assert, documented_scope) = match resolve_rows_assert(&args) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {e}");
            return exit::MANIFEST_UNREADABLE;
        }
    };

    let schema = args.corpus.join("schema.cql");
    for (label, path) in [
        ("sstables dir", &args.corpus.join("sstables")),
        ("schema.cql", &schema),
    ] {
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

    // The ONE directory this run may ingest, resolved and NAMED before anything opens:
    // the manifest's exact `sstable_dir` when it documents one, else the sole
    // `<table>-<uuid>` directory as `scope::is_table_dir` defines it — an ambiguous root
    // is refused, never silently unioned.
    let scope = match scope::resolve(
        &args.corpus,
        &args.keyspace,
        &args.table,
        documented_scope.as_ref(),
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: {e}");
            return exit::OPEN_FAILED;
        }
    };

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
        data_dir: scope.data_dir.clone(),
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        // NOT a filter: a substring filter of `/<ks>/<dir>` also matches a sibling whose
        // full name extends it (roborev #3234 F1). The scope is expressed as EXACT path
        // identity below, and this field is left off so no second, looser scope exists.
        table_directory_filter: None,
    };

    let open_start = std::time::Instant::now();
    let wanted = [scope.dir.clone()];
    let ingested = match rt.block_on(ingest_with_selection(
        cfg,
        TableDirSelection::Exact(&wanted),
    )) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("error: ingest of {} failed: {e}", scope.data_dir.display());
            return exit::OPEN_FAILED;
        }
    };
    let open_elapsed = open_start.elapsed();
    // The generation count is OBSERVED in the directories ingestion actually selected,
    // never in the one this run intended to select (roborev #3234 F1). Reporting the
    // intended count is precisely how round 11's substring filter could scan extra
    // SSTables while printing the smaller number — and the generation count selects the
    // scan route, so that misattributes every figure below it.
    let selected = ingested.discovery_summary.table_directories.clone();
    let generations = match observed_generations(&selected) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: {e}");
            return exit::OPEN_FAILED;
        }
    };
    let db = ingested.database;
    let discovered_total = ingested.discovery_summary.sstables_found;
    // Printed BEFORE the exactness refusal below, so an over-wide ingest is VISIBLE as
    // the number it really was — the diagnosis is "3 generations were selected", not a
    // bare exit code.
    println!(
        "open: {generations} sstables discovered in {:.3} s ({})",
        open_elapsed.as_secs_f64(),
        scope.dir.display()
    );
    println!("ingest_scope: {}", scope.provenance);
    println!("generations_observed_in: {} directory/ies", selected.len());
    if let Err(e) = verify_scope_exact(&selected, &scope) {
        eprintln!("error: {e}");
        return exit::OPEN_FAILED;
    }
    if discovered_total > generations {
        println!(
            "note: {} sstable(s) under {} are OUTSIDE this scope and were NOT ingested",
            discovered_total - generations,
            scope.data_dir.display()
        );
    }

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
    // The workload travels WITH the number: which directory, decided how.
    println!("ingest_scope:     {}", scope.provenance);
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
