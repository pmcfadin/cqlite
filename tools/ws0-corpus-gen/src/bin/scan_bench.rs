//! `ws0-scan-bench` — ARM A of the #3096 measurement: the BARE SCAN.
//!
//! Drives the public `cqlite_core::Database::execute_streaming` surface over the
//! committed corpus and reports rows, cells, and per-pass wall time as JSON on
//! stdout. It is the same-session comparator the Flight `do_get` arm's rows/s and
//! cycles/row are divided by (spec R1's ratio), so it MUST be run in the same
//! session, on the same pinned cores, over the same bytes.
//!
//! # Deliberate properties
//!
//! * **Setup is timed SEPARATELY** (`setup_secs`) and is never inside the scan
//!   interval, so the caller can subtract it from the cycles/row denominator
//!   (spec R2). `--rows-denominator` is printed back so every derived figure
//!   names the denominator it used.
//! * **An anti-elision fold** over every cell (`--fold`, off by default) proves
//!   the values were genuinely materialized. It is OFF by default because it
//!   measurably inflates the scan (the #3026 harness measured +28.6% cycles);
//!   report the unfolded number, use the folded one to prove materialization.
//!   The fold is **REPRODUCIBLE between runs**: cells are folded in the pinned
//!   `schema::COLUMNS` order, never in `HashMap` order, which is randomly seeded
//!   per process — see [`fold_row`].
//! * **Zero rows exits non-zero.** A scan that observed nothing is a failure, not
//!   a measurement (spec R4).
//! * **No Arrow.** This crate does not enable `cqlite-core`'s `arrow` feature: the
//!   bare-scan arm must exclude Arrow encode, which is the cost under study.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use cqlite_core::query::result::StreamingConfig;

#[derive(Parser, Debug)]
#[command(
    name = "ws0-scan-bench",
    about = "Bare-scan (execute_streaming) arm of the issue #3096 measurement rig"
)]
struct Cli {
    /// Corpus root written by `ws0-corpus-gen` (the dir holding `<ks>/<table>/`).
    #[arg(long)]
    corpus: PathBuf,

    /// Schema `.cql`. Defaults to `<corpus>/ws0-events.cql`, which the generator
    /// emits alongside the data so the scan reads the DDL the corpus was written
    /// from (no-heuristics: the column set is never inferred from bytes).
    #[arg(long)]
    schema: Option<PathBuf>,

    /// Keyspace to scan.
    #[arg(long, default_value = "ws0")]
    keyspace: String,

    /// Table to scan.
    #[arg(long, default_value = "events")]
    table: String,

    /// Timed passes. Every pass's wall time is printed individually; the caller
    /// takes the median and reports the spread (never a silent average).
    ///
    /// MUST be >= 1, enforced by clap's value parser rather than only by the shell
    /// driver (issue #3272 review). At `--passes 0` the `for pass in 0..0` loop body
    /// never ran, so this binary skipped the scan entirely, printed
    /// `rows_denominator: 0` / `timed_scan_secs: 0.0`, and exited **SUCCESS** — the
    /// exact contradiction of the "zero rows exits non-zero" guarantee stated in this
    /// file's own doc comment. `ws0-baseline.sh` happens to validate `--scan-passes`
    /// before invoking this, but a guarantee that holds only because one caller is
    /// careful is a property of the caller, not of the binary; a direct invocation is
    /// an ordinary thing to do with a bench tool.
    #[arg(long, default_value_t = 3, value_parser = at_least_one_pass)]
    passes: u32,

    /// Fold every cell into a digest (anti-elision proof; inflates the number).
    /// Reproducible run to run: folded in pinned schema-column order, not
    /// `HashMap` order.
    #[arg(long)]
    fold: bool,

    /// Projection. `*` reads every column — the shape the Flight arm streams.
    #[arg(long, default_value = "*")]
    project: String,

    /// Do the SETUP (corpus open + schema ingest) and exit WITHOUT scanning.
    ///
    /// This is how setup is subtracted from the cycles/row denominator (spec R2):
    /// the driver runs this binary twice under `perf stat` — once `--setup-only`
    /// and once with `--passes P` — and reports
    /// `(cycles_total - cycles_setup) / rows`. That is a MEASURED subtraction of a
    /// separately-observed cost, not a model. The exit is deliberately success:
    /// the "zero rows is a failure" rule applies to a SCAN, and this mode declares
    /// up front that it does not scan.
    #[arg(long)]
    setup_only: bool,
}

/// `--passes` value parser: a positive count, or a reason.
///
/// Rejects at PARSE time rather than after setup, so a vacuous invocation cannot open
/// the corpus, ingest the schema and then exit zero having measured nothing (#3272
/// review B7). The message states WHY rather than only the bound: the failure mode
/// being closed is a vacuous SUCCESS, which is materially different from an ordinary
/// out-of-range argument.
fn at_least_one_pass(s: &str) -> Result<u32, String> {
    let n: u32 = s
        .parse()
        .map_err(|_| format!("`{s}` is not a non-negative integer"))?;
    if n == 0 {
        return Err(
            "must be at least 1. A run with 0 timed passes performs no scan and \
             observes no rows, so it would report `rows_denominator: 0` and exit \
             SUCCESS — a vacuous measurement, which contradicts this binary's \
             zero-rows-is-a-failure guarantee (issue #3272)."
                .to_string(),
        );
    }
    Ok(n)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("ws0-scan-bench: ERROR: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let schema_path = cli
        .schema
        .clone()
        .unwrap_or_else(|| cli.corpus.join("ws0-events.cql"));
    if !schema_path.exists() {
        return Err(format!(
            "schema not found: {} — run ws0-corpus-gen, which emits it beside the corpus",
            schema_path.display()
        )
        .into());
    }
    let table_dir = cli.corpus.join(&cli.keyspace).join(&cli.table);
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

    // --- setup, timed separately so the caller can subtract it (spec R2) -------
    //
    // EXACT DIRECTORY IDENTITY, NEVER A SUBSTRING FILTER (#3272 round 10, F-B).
    //
    // This used to pass `table_directory_filter: Some(format!("/{ks}/{table}"))`. `cqlite-core`
    // documents that field as a SUBSTRING match, "loose by design", which "cannot express 'exactly
    // this directory': any sibling whose full name extends the filter also matches". MEASURED
    // against this corpus: with a `ws0/events-backup/` sibling present, the filter selected
    // BOTH `…/ws0/events` and `…/ws0/events-backup`, while `Exact` selected only `…/ws0/events`.
    //
    // Why that voids the rig rather than merely adding a directory: this binary is ARM A, and the
    // ONLY thing the rig reports is arm A against arm B over THE SAME BYTES. The two arms reach
    // ingestion by different routes (this `IngestionConfig`, versus `cqlite-flight --data-dir`), so
    // a sibling silently absorbed here and not there means the arms measured DIFFERENT SSTable
    // SETS — and the cross-arm ratio, which is the rig's entire output, compares nothing. It is
    // also not only a row-count effect: an extra directory changes the GENERATION COUNT, and the
    // generation count selects the scan route.
    //
    // `Exact` compares complete path components after canonicalization (issue #3234), so a
    // `<table>-<uuid>-backup`, a `<table>-backup`, or any other name-extending sibling contributes
    // nothing. `table_directory_filter` is left `None` so no second, looser scope exists at all —
    // the same shape `cqlite-core/examples/bti_perf_scan` uses.
    let setup_start = Instant::now();
    let cfg = cqlite_core::ingestion::IngestionConfig {
        schema_paths: vec![schema_path.clone()],
        data_dir: cli.corpus.clone(),
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
    // The selected set is OBSERVED and refused unless it is exactly the intended directory, rather
    // than assumed from having asked for `Exact`. `Exact` is the mechanism; this is the affirmative
    // verification that the mechanism did what was asked — reporting the INTENDED scope is exactly
    // how the substring filter could scan extra SSTables while printing the smaller number.
    //
    // The predicate lives in `ws0_corpus_gen::scan_scope` so BOTH its refusal branches are
    // unit-testable: reached from a shell they are (correctly) near-unprovokable once `Exact` is in
    // use, and a guard no test can watch fail is the very defect this issue is about. `true` because
    // the `*-Data.db` precondition above ESTABLISHED there is something to ingest, so an empty
    // selection here is a refusal rather than a legitimately empty corpus.
    let selected = &ingested.discovery_summary.table_directories;
    ws0_corpus_gen::scan_scope::verify_exact_scope(selected, &table_dir, true)?;
    // Recorded so the scope this arm measured is READABLE in the artifact rather than inferred
    // from the code, on both this branch and the scanning one below. It is what the refusals above
    // observed, so an artifact and a claim about the artifact cannot disagree.
    let ingested_dirs: Vec<String> = selected.iter().map(|d| d.display().to_string()).collect();
    let db = ingested.database;
    let setup_secs = setup_start.elapsed().as_secs_f64();

    let sql = format!("SELECT {} FROM {}.{}", cli.project, cli.keyspace, cli.table);

    if cli.setup_only {
        // Keep the opened database alive across the measurement boundary so the
        // perf window covers the whole setup, then drop it explicitly.
        std::hint::black_box(&db);
        drop(db);
        let out = serde_json::json!({
            "arm": "bare_scan_setup_only",
            "surface": "cqlite_core::ingestion::ingest_with_selection (TableDirSelection::Exact)",
            "corpus": cli.corpus.display().to_string(),
            "schema": schema_path.display().to_string(),
            "table_dirs_ingested": ingested_dirs,
            "setup_secs": setup_secs,
            "passes": [],
            "rows_denominator": 0,
            "timed_scan_secs": 0.0,
        });
        println!("{}", serde_json::to_string(&out)?);
        return Ok(ExitCode::SUCCESS);
    }

    // The value parser above rejects 0 at parse time; this is the same rule stated
    // where the loop is, so a future `Cli` constructed in code (a test, a library
    // caller) cannot reach the loop with a vacuous count either.
    if cli.passes == 0 {
        return Err(
            "--passes must be at least 1: a run with 0 timed passes observes no rows \
             and would exit SUCCESS having measured nothing (issue #3272)"
                .into(),
        );
    }
    let mut passes = Vec::new();
    for pass in 0..cli.passes {
        let mut rows = 0u64;
        let mut cells = 0u64;
        let mut digest = 0u64;
        let t0 = Instant::now();
        let mut it = db
            .execute_streaming(&sql, StreamingConfig::default())
            .await?;
        while let Some(row) = it.next_async().await {
            let row = row?;
            cells += row.values.len() as u64;
            if cli.fold {
                digest = fold_row(digest, &row)?;
            }
            std::hint::black_box(&row);
            rows += 1;
        }
        let secs = t0.elapsed().as_secs_f64();
        if rows == 0 {
            return Err(format!(
                "pass {pass} observed ZERO rows over {} — exiting non-zero rather than \
                 reporting a measurement",
                table_dir.display()
            )
            .into());
        }
        eprintln!(
            "  pass {pass}: {rows} rows, {cells} cells, {secs:.3}s, {:.0} rows/s{}",
            rows as f64 / secs,
            if pass == 0 { "  [first pass]" } else { "" }
        );
        passes.push(serde_json::json!({
            "pass": pass,
            "rows": rows,
            "cells": cells,
            "secs": secs,
            "rows_per_sec": rows as f64 / secs,
            "digest": if cli.fold { Some(format!("{digest:016x}")) } else { None },
        }));
    }

    // The row denominator every derived figure must be divided by: rows summed
    // over the TIMED passes (setup excluded above, and printed separately).
    let rows_denominator: u64 = passes.iter().filter_map(|p| p["rows"].as_u64()).sum();
    let scan_secs: f64 = passes.iter().filter_map(|p| p["secs"].as_f64()).sum();

    let out = serde_json::json!({
        "arm": "bare_scan",
        "surface": "cqlite_core::Database::execute_streaming",
        "corpus": cli.corpus.display().to_string(),
        "schema": schema_path.display().to_string(),
        // The EXACT table directories ingestion selected (#3272 round 10, F-B). A substring filter
        // silently absorbed a name-extending sibling, so the set this arm measured is recorded
        // rather than left to be assumed equal to the intended one.
        "table_dirs_ingested": ingested_dirs,
        "query": sql,
        "fold": cli.fold,
        "setup_secs": setup_secs,
        "passes": passes,
        "rows_denominator": rows_denominator,
        "timed_scan_secs": scan_secs,
    });
    println!("{}", serde_json::to_string(&out)?);
    Ok(ExitCode::SUCCESS)
}

/// Fold one row into the running digest in the **pinned [`schema::COLUMNS`]
/// order**, never in `HashMap` iteration order.
///
/// # Why this is not a style preference (issue #3096 review, blocker 2)
///
/// `QueryRow::values` is a `HashMap<Arc<str>, Value>`, and Rust's default hasher
/// is **randomly seeded per process**. Folding `values.iter()` therefore produced
/// a DIFFERENT digest on every run over byte-identical data — while the digest's
/// whole stated purpose (see [`fnv1a64_update`]) is to be compared BETWEEN runs.
/// Anyone using `--fold` as a cross-lever invariance check got a spurious
/// mismatch, and a match would have meant nothing.
///
/// Fail-closed on an unexpected column name rather than skipping it: a digest that
/// silently omits a column is worse than one that is merely unstable, because it
/// would report invariance over data it never read. A projection narrower than
/// `*` is fine — absent columns simply do not contribute.
fn fold_row(mut digest: u64, row: &cqlite_core::query::QueryRow) -> Result<u64, String> {
    let mut folded = 0usize;
    for (name, _) in ws0_corpus_gen::schema::COLUMNS {
        if let Some(value) = row.values.get(name) {
            digest = fnv1a64_update(digest, name.as_bytes());
            digest = fnv1a64_update(digest, format!("{value:?}").as_bytes());
            folded += 1;
        }
    }
    if folded != row.values.len() {
        let known: Vec<&str> = ws0_corpus_gen::schema::COLUMNS
            .iter()
            .map(|c| c.0)
            .collect();
        let unknown: Vec<&str> = row
            .values
            .keys()
            .map(|k| k.as_ref())
            .filter(|k| !known.contains(k))
            .collect();
        return Err(format!(
            "row carries {} column(s) absent from the pinned ws0.events schema ({unknown:?}) — \
             folding only the known ones would digest less than was read, so this exits rather \
             than reporting an invariance it did not check",
            row.values.len() - folded
        ));
    }
    Ok(digest)
}

/// FNV-1a 64 — a fixed, version-stable hash. `DefaultHasher` is explicitly NOT
/// guaranteed stable across Rust releases, so it could never anchor a digest that
/// is compared between runs.
///
/// Stability needs BOTH halves: a fixed hash function AND a fixed fold ORDER —
/// see [`fold_row`], which supplies the second half.
fn fnv1a64_update(mut h: u64, bytes: &[u8]) -> u64 {
    if h == 0 {
        h = 0xcbf2_9ce4_8422_2325;
    }
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// NON-VACUITY for [`at_least_one_pass`] (issue #3272 review B7).
    ///
    /// MEASURED against the pre-fix binary — `#[arg(long, default_value_t = 3)] passes:
    /// u32` with no parser — `--passes 0` parsed fine, `for pass in 0..0` ran zero
    /// times, and the binary printed
    /// `{"rows_denominator":0,"timed_scan_secs":0.0,"passes":[]}` and exited **0**. The
    /// only thing preventing that was `ws0-baseline.sh` validating `--scan-passes`
    /// first, i.e. a property of one caller rather than of the binary.
    #[test]
    fn passes_zero_is_refused_at_parse_time() {
        let err = Cli::try_parse_from([
            "ws0-scan-bench",
            "--corpus",
            "/nonexistent",
            "--passes",
            "0",
        ])
        .expect_err("--passes 0 must not parse");
        let msg = err.to_string();
        assert!(
            msg.contains("at least 1"),
            "the refusal must state the bound; got: {msg}"
        );
        assert!(
            msg.contains("vacuous"),
            "the refusal must say WHY (a vacuous SUCCESS, not merely out of range); \
             got: {msg}"
        );
    }

    /// The ACCEPT direction: the guard is not one that refuses every value. Without
    /// this, `at_least_one_pass` hardcoded to `Err` would satisfy the case above — the
    /// #3249 shape.
    #[test]
    fn a_positive_passes_count_parses_and_the_default_is_positive() {
        for n in ["1", "3", "17"] {
            let cli = Cli::try_parse_from(["ws0-scan-bench", "--corpus", "/x", "--passes", n])
                .unwrap_or_else(|e| panic!("--passes {n} must parse: {e}"));
            assert_eq!(cli.passes.to_string(), n);
        }
        let cli = Cli::try_parse_from(["ws0-scan-bench", "--corpus", "/x"]).expect("defaults");
        assert!(
            cli.passes >= 1,
            "the DEFAULT must itself be a non-vacuous count, got {}",
            cli.passes
        );
    }

    /// A negative or non-numeric value is refused too, with a reason rather than a
    /// panic — the parser owns the whole domain, not just the zero case.
    #[test]
    fn a_negative_or_non_numeric_passes_count_is_refused() {
        for bad in ["-1", "abc", "1.5", ""] {
            let err = Cli::try_parse_from(["ws0-scan-bench", "--corpus", "/x", "--passes", bad])
                .expect_err("must reject {bad}");
            assert!(
                !err.to_string().is_empty(),
                "the refusal of `{bad}` must carry a message"
            );
        }
    }

    /// The value parser is what the CLI actually uses. A parser defined but not wired
    /// is the #3249 shape ("present" vs "observed to fire"), and `try_parse_from` above
    /// only proves the CLI refuses `0` — not that it refuses it THROUGH this function.
    #[test]
    fn the_value_parser_itself_refuses_zero_and_accepts_one() {
        assert!(at_least_one_pass("0").is_err());
        assert_eq!(at_least_one_pass("1").expect("1 is valid"), 1);
    }
}
