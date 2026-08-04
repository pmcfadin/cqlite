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
    #[arg(long, default_value_t = 3)]
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
    let setup_start = Instant::now();
    let cfg = cqlite_core::ingestion::IngestionConfig {
        schema_paths: vec![schema_path.clone()],
        data_dir: cli.corpus.clone(),
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{}/{}", cli.keyspace, cli.table)),
    };
    let db = cqlite_core::ingestion::ingest(cfg).await?.database;
    let setup_secs = setup_start.elapsed().as_secs_f64();

    let sql = format!("SELECT {} FROM {}.{}", cli.project, cli.keyspace, cli.table);

    if cli.setup_only {
        // Keep the opened database alive across the measurement boundary so the
        // perf window covers the whole setup, then drop it explicitly.
        std::hint::black_box(&db);
        drop(db);
        let out = serde_json::json!({
            "arm": "bare_scan_setup_only",
            "surface": "cqlite_core::ingestion::ingest",
            "corpus": cli.corpus.display().to_string(),
            "schema": schema_path.display().to_string(),
            "setup_secs": setup_secs,
            "passes": [],
            "rows_denominator": 0,
            "timed_scan_secs": 0.0,
        });
        println!("{}", serde_json::to_string(&out)?);
        return Ok(ExitCode::SUCCESS);
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
