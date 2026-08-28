//! DataFusion-spike benchmark harness (issue #2605).
//!
//! Runs the 3 scenarios x 4 arms matrix against a real Cassandra-5.0-written
//! corpus and emits one JSON record per run into
//! `docs/reports/2605-datafusion-spike-artifacts/`.
//!
//! # It fails closed rather than reporting a comfortable number
//!
//! * The corpus must resolve to >= 2 post-prune `*-Data.db` sources, and the
//!   k-way MERGE arm must be observed to have run (via
//!   `cqlite_core::storage::read_path_probe`). With one generation the
//!   single-generation bypass could serve the scan, the two arms would consume
//!   different (un-reconciled) row sets, and the benchmark would be measuring a
//!   correctness difference rather than an engine difference.
//! * Every arm of a scenario must scan the SAME number of rows (except the
//!   `row_pushdown` reference arm, whose scan is narrowed on purpose). A
//!   divergence means the arms are not comparable and the run is rejected.
//! * A scan that ends in an error, or a run whose measurements are unavailable,
//!   is an error — never a zero.
//!
//! Usage:
//! ```text
//! df_spike_bench --dir <table-dir> --ddl-file <create-table.cql> \
//!     [--projection a,b,c] [--filter-column c --filter-op lt --filter-value 100] \
//!     [--iterations 3] [--batch-size 8192] [--out <results.json>] \
//!     [--scenario <id>] [--arm <id>]
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use cqlite_flight::df_spike::bench::{BenchConfig, BenchError, BenchOutcome};
use cqlite_flight::df_spike::rowwise::{RowLiteral, RowOp};
use cqlite_flight::df_spike::{ArmKind, BenchRunner, Scenario, ScenarioKind};

/// Parsed command line.
struct Args {
    dir: PathBuf,
    ddl_file: PathBuf,
    projection: Option<Vec<String>>,
    filter_column: Option<String>,
    filter_op: RowOp,
    filter_value: Option<String>,
    iterations: usize,
    iteration_base: usize,
    batch_size: usize,
    out: Option<PathBuf>,
    df_target_partitions: Option<usize>,
    scenarios: Vec<ScenarioKind>,
    arms: Vec<ArmKind>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("df_spike_bench: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let args = parse_args()?;
    let ddl = std::fs::read_to_string(&args.ddl_file)
        .map_err(|e| format!("cannot read --ddl-file {}: {e}", args.ddl_file.display()))?;
    // Skip anything before the first `CREATE TABLE` (a comment header, or a
    // `CREATE KEYSPACE` when the file is Cassandra's own `schema.cql`) —
    // `parse_create_table` requires its input to START at the statement. The
    // FIRST such statement is used and nothing after it is inspected, so a file
    // holding several tables is a usage error the caller must not make.
    let statement = ddl
        .find("CREATE TABLE")
        .map(|at| &ddl[at..])
        .ok_or_else(|| {
            format!(
                "{} contains no CREATE TABLE statement",
                args.ddl_file.display()
            )
        })?;
    let (_, schema) = cqlite_core::schema::parse_create_table(statement).map_err(|_| {
        format!(
            "cannot parse a CREATE TABLE statement out of {}",
            args.ddl_file.display()
        )
    })?;

    // Defaults derived from the SCHEMA, never guessed from data: the projection
    // is the first three declared columns in key-first order, and the filter
    // column defaults to the first clustering key (always present, always typed).
    let all_columns = schema_column_names(&schema);
    let projection = match args.projection.clone() {
        Some(cols) => cols,
        None => all_columns.iter().take(3).cloned().collect(),
    };
    let filter_column = match args.filter_column.clone() {
        Some(c) => c,
        None => schema
            .clustering_keys
            .first()
            .map(|c| c.name.clone())
            .ok_or("no --filter-column given and the table has no clustering key")?,
    };
    let filter_value = RowLiteral::parse(args.filter_value.as_deref().unwrap_or("5"));

    for name in projection.iter().chain(std::iter::once(&filter_column)) {
        if !all_columns.iter().any(|c| c == name) {
            return Err(format!(
                "column '{name}' is not declared in the table schema"
            ));
        }
    }

    let config = BenchConfig {
        dir: args.dir.clone(),
        schema,
        batch_size: args.batch_size,
        projection: projection.clone(),
        filter_column: filter_column.clone(),
        filter_op: args.filter_op,
        filter_value,
        iterations: args.iterations,
        df_target_partitions: args.df_target_partitions,
    };
    let runner = BenchRunner::new(config);

    let mut results: Vec<BenchOutcome> = Vec::new();
    for kind in &args.scenarios {
        for arm in &args.arms {
            // Labelled from `--iteration-base`, not from a 1-based local
            // counter: the driver runs ONE PROCESS PER CELL, so every record
            // would otherwise be stamped `iteration=1` and a cell would not be
            // attributable to its place in the run order from its own contents.
            for offset in 0..args.iterations {
                let iteration = args.iteration_base + offset;
                let scenario = Scenario {
                    kind: *kind,
                    arm: *arm,
                };
                let outcome = runner
                    .run_one(scenario, iteration)
                    .map_err(|e| describe(kind, arm, iteration, &e))?;
                report_line(&outcome);
                results.push(outcome);
            }
        }
    }

    assert_preconditions(&results)?;
    write_results(&args, &results)?;
    Ok(())
}

/// Every declared column name, key-first (partition keys, clustering keys, then
/// regular columns) — the order `MergeProducer` emits.
fn schema_column_names(schema: &cqlite_core::schema::TableSchema) -> Vec<String> {
    // Three distinct column types, so three separate maps rather than one
    // `chain` over incompatible iterators.
    let mut names: Vec<String> = schema
        .partition_keys
        .iter()
        .map(|c| c.name.clone())
        .collect();
    names.extend(schema.clustering_keys.iter().map(|c| c.name.clone()));
    names.extend(schema.columns.iter().map(|c| c.name.clone()));
    names
}

/// One human-readable progress line per run.
fn report_line(o: &BenchOutcome) {
    let secs = o.elapsed_nanos as f64 / 1e9;
    let rows_per_sec = if secs > 0.0 {
        o.rows_scanned as f64 / secs
    } else {
        f64::NAN
    };
    println!(
        "{}/{} iter={} elapsed={:.3}s scanned={} result={} batches={} sources={} \
         merge_arm={} rows/s={:.0} peak_rss={} encode_ms={:.1} merge_ms={:.1} decompress_ms={:.1}",
        o.scenario.id(),
        o.arm.id(),
        o.iteration,
        secs,
        o.rows_scanned,
        o.rows_result,
        o.batches,
        o.sources,
        o.merge_arm_observed,
        rows_per_sec,
        o.peak_rss_bytes
            .map(|b| format!("{:.1}MiB", b as f64 / (1024.0 * 1024.0)))
            .unwrap_or_else(|| "unmeasured".to_string()),
        o.subphase_encode_nanos as f64 / 1e6,
        o.subphase_merge_nanos as f64 / 1e6,
        o.subphase_decompress_nanos as f64 / 1e6,
    );
}

/// A failure message that names exactly which run failed.
fn describe(kind: &ScenarioKind, arm: &ArmKind, iteration: usize, e: &BenchError) -> String {
    format!("{}/{} iter={iteration} failed: {e}", kind.id(), arm.id())
}

/// The comparability preconditions. Violations are ERRORS: a benchmark whose
/// arms did not read the same rows reports an engine delta that is really a
/// correctness delta.
fn assert_preconditions(results: &[BenchOutcome]) -> Result<(), String> {
    if results.is_empty() {
        return Err("no runs were executed".to_string());
    }
    for o in results {
        if o.sources < 2 {
            return Err(format!(
                "{}/{}: {} post-prune source(s); the corpus must have >= 2 overlapping \
                 generations so the scan is post-reconciliation",
                o.scenario.id(),
                o.arm.id(),
                o.sources
            ));
        }
        if !o.merge_arm_observed {
            return Err(format!(
                "{}/{}: the k-way MERGE arm was NOT observed (reconcile_entries={}, \
                 cell_metadata_maps={}); the bypass arm would make the arms incomparable",
                o.scenario.id(),
                o.arm.id(),
                o.reconcile_entries,
                o.cell_metadata_maps
            ));
        }
        if o.rows_scanned == 0 {
            return Err(format!(
                "{}/{}: scanned 0 rows — an empty corpus is a failure, not a fast result",
                o.scenario.id(),
                o.arm.id()
            ));
        }
    }

    // Within a scenario, every arm EXCEPT the deliberately-narrowed production
    // reference arm must have scanned the same rows.
    for kind in ScenarioKind::all() {
        let comparable: Vec<&BenchOutcome> = results
            .iter()
            .filter(|o| o.scenario == kind && o.arm != ArmKind::RowPushdown)
            .collect();
        if let Some(first) = comparable.first() {
            for o in &comparable {
                if o.rows_scanned != first.rows_scanned {
                    return Err(format!(
                        "{}: arm '{}' scanned {} rows but arm '{}' scanned {} — the arms are \
                         not consuming the same batches",
                        kind.id(),
                        o.arm.id(),
                        o.rows_scanned,
                        first.arm.id(),
                        first.rows_scanned
                    ));
                }
            }
        }
    }

    assert_results_agree(results)?;
    report_floor_violations(results);
    Ok(())
}

/// Every ANSWERING arm of a (scenario, iteration) must produce the SAME query
/// result.
///
/// `rows_scanned` agreement is not sufficient: `row_pushdown` narrows its scan
/// on purpose, so it is excluded from the `rows_scanned` check above and would
/// otherwise be unchecked entirely — a pushdown that dropped rows the other arms
/// keep would read as a free speed-up. `rows_result` is the quantity that must
/// hold across a narrowed scan, so it is checked HERE for `row_pushdown` too.
///
/// `floor` is excluded, and only `floor`: it discards every batch by
/// construction, so its `rows_result` is 0 BY DESIGN and comparing it would
/// reject every correct run.
fn assert_results_agree(results: &[BenchOutcome]) -> Result<(), String> {
    for kind in ScenarioKind::all() {
        for iteration in iterations_of(results) {
            let answering: Vec<&BenchOutcome> = results
                .iter()
                .filter(|o| o.scenario == kind && o.iteration == iteration)
                .filter(|o| o.arm != ArmKind::Floor)
                .collect();
            let Some(first) = answering.first() else {
                continue;
            };
            for o in &answering {
                if o.rows_result != first.rows_result {
                    return Err(format!(
                        "{} iter={iteration}: arm '{}' produced {} result row(s) but arm '{}' \
                         produced {} — the arms answered DIFFERENT queries, so any timing \
                         delta between them is a correctness delta",
                        kind.id(),
                        o.arm.id(),
                        o.rows_result,
                        first.arm.id(),
                        first.rows_result
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Report — but do NOT reject — (scenario, iteration) pairs where an executing
/// arm finished FASTER than the discard-only `floor`.
///
/// The floor produces the same batches every other arm consumes and then throws
/// them away, so in a noise-free world it bounds them from below. It is a
/// DIAGNOSTIC and not an assertion because on this corpus it is routinely
/// violated for a reason that is understood and reported: wall time is dominated
/// by cold page-fault stalls whose magnitude depends on cache state at the moment
/// a cell runs, while the order-independent sub-phase counters (`encode`,
/// `batches`) are identical across arms. Measured on the committed 45-cell
/// matrix, the floor is beaten in 24 of 36 arm-comparisons — so a hard abort
/// would reject nearly every legitimate run and teach the operator to disable the
/// check. Printing it keeps the noise VISIBLE, which is the honest reading.
fn report_floor_violations(results: &[BenchOutcome]) {
    let mut violations: Vec<String> = Vec::new();
    for kind in ScenarioKind::all() {
        for iteration in iterations_of(results) {
            let floor = results.iter().find(|o| {
                o.scenario == kind && o.iteration == iteration && o.arm == ArmKind::Floor
            });
            let Some(floor) = floor else {
                continue;
            };
            for o in results
                .iter()
                .filter(|o| o.scenario == kind && o.iteration == iteration)
                .filter(|o| o.arm != ArmKind::Floor)
            {
                if o.elapsed_nanos < floor.elapsed_nanos {
                    violations.push(format!(
                        "{} iter={iteration}: arm '{}' {:.1}s < floor {:.1}s",
                        kind.id(),
                        o.arm.id(),
                        o.elapsed_nanos as f64 / 1e9,
                        floor.elapsed_nanos as f64 / 1e9,
                    ));
                }
            }
        }
    }
    if violations.is_empty() {
        return;
    }
    eprintln!(
        "df_spike_bench: WARNING — the discard-only floor was BEATEN in {} arm-comparison(s); \
         wall-time deltas of this size are page-fault noise, not engine effects. Read the \
         order-independent sub-phase counters instead:",
        violations.len()
    );
    for v in &violations {
        eprintln!("  {v}");
    }
}

/// The distinct iteration indices present in the results, ascending.
fn iterations_of(results: &[BenchOutcome]) -> Vec<usize> {
    let mut seen: Vec<usize> = results.iter().map(|o| o.iteration).collect();
    seen.sort_unstable();
    seen.dedup();
    seen
}

/// Write the machine-readable results.
fn write_results(args: &Args, results: &[BenchOutcome]) -> Result<(), String> {
    let Some(path) = &args.out else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
    }
    let document = serde_json::json!({
        "issue": 2605,
        "corpus_dir": args.dir,
        "ddl_file": args.ddl_file,
        "iterations": args.iterations,
        "iteration_base": args.iteration_base,
        "batch_size": args.batch_size,
        "df_target_partitions": args.df_target_partitions,
        "runs": results,
    });
    let text = serde_json::to_string_pretty(&document)
        .map_err(|e| format!("cannot serialize results: {e}"))?;
    std::fs::write(path, text).map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    println!("wrote {}", path.display());
    Ok(())
}

/// Minimal hand-rolled argument parsing — the harness is a spike binary and
/// adding a `clap` derive surface to it would outlive the measurement.
fn parse_args() -> Result<Args, String> {
    let mut dir: Option<PathBuf> = None;
    let mut ddl_file: Option<PathBuf> = None;
    let mut projection: Option<Vec<String>> = None;
    let mut filter_column: Option<String> = None;
    let mut filter_op = RowOp::Lt;
    let mut filter_value: Option<String> = None;
    let mut iterations = 3usize;
    let mut iteration_base = 1usize;
    let mut batch_size = 8192usize;
    let mut out: Option<PathBuf> = None;
    let mut df_target_partitions: Option<usize> = None;
    let mut scenarios: Vec<ScenarioKind> = Vec::new();
    let mut arms: Vec<ArmKind> = Vec::new();

    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || {
            argv.next()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        match flag.as_str() {
            "--dir" => dir = Some(PathBuf::from(value()?)),
            "--ddl-file" => ddl_file = Some(PathBuf::from(value()?)),
            "--projection" => {
                projection = Some(
                    value()?
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect(),
                )
            }
            "--filter-column" => filter_column = Some(value()?),
            "--filter-op" => {
                let raw = value()?;
                filter_op =
                    RowOp::parse(&raw).ok_or_else(|| format!("unknown --filter-op '{raw}'"))?;
            }
            "--filter-value" => filter_value = Some(value()?),
            "--iterations" => {
                iterations = value()?
                    .parse()
                    .map_err(|e| format!("--iterations must be a positive integer: {e}"))?;
            }
            "--iteration-base" => {
                iteration_base = value()?
                    .parse()
                    .map_err(|e| format!("--iteration-base must be a positive integer: {e}"))?;
            }
            "--batch-size" => {
                batch_size = value()?
                    .parse()
                    .map_err(|e| format!("--batch-size must be a positive integer: {e}"))?;
            }
            "--out" => out = Some(PathBuf::from(value()?)),
            "--df-target-partitions" => {
                let n: usize = value()?.parse().map_err(|e| {
                    format!("--df-target-partitions must be a positive integer: {e}")
                })?;
                if n == 0 {
                    return Err("--df-target-partitions must be >= 1".to_string());
                }
                df_target_partitions = Some(n);
            }
            "--scenario" => {
                let raw = value()?;
                scenarios.push(
                    ScenarioKind::parse(&raw)
                        .ok_or_else(|| format!("unknown --scenario '{raw}'"))?,
                );
            }
            "--arm" => {
                let raw = value()?;
                arms.push(ArmKind::parse(&raw).ok_or_else(|| format!("unknown --arm '{raw}'"))?);
            }
            "--help" | "-h" => {
                println!("{}", HELP);
                std::process::exit(0);
            }
            // Unknown flags are REJECTED rather than ignored: a silently-dropped
            // `--filter-column` would produce a plausible number for the wrong query.
            other => return Err(format!("unrecognized argument '{other}' (try --help)")),
        }
    }

    if iterations == 0 {
        return Err("--iterations must be >= 1".to_string());
    }
    if iteration_base == 0 {
        return Err("--iteration-base must be >= 1".to_string());
    }
    if batch_size == 0 {
        return Err("--batch-size must be >= 1".to_string());
    }
    Ok(Args {
        dir: dir.ok_or("--dir is required")?,
        ddl_file: ddl_file.ok_or("--ddl-file is required")?,
        projection,
        filter_column,
        filter_op,
        filter_value,
        iterations,
        iteration_base,
        batch_size,
        out,
        df_target_partitions,
        scenarios: if scenarios.is_empty() {
            ScenarioKind::all().to_vec()
        } else {
            scenarios
        },
        arms: if arms.is_empty() {
            ArmKind::all().to_vec()
        } else {
            arms
        },
    })
}

/// `--help` text.
const HELP: &str = "\
df_spike_bench (issue #2605) — DataFusion vs row-engine over the CQLite Flight scan path

Required:
  --dir <path>          table directory holding *-Data.db components
  --ddl-file <path>     file containing the table's CREATE TABLE statement

Optional:
  --projection a,b,c    columns for the projected-scan scenario (default: first 3, key-first)
  --filter-column <c>   filter column (default: the first clustering key)
  --filter-op <op>      eq|ne|lt|lte|gt|gte (default: lt)
  --filter-value <v>    operand; parsed as bool, then integer, then float, then text (default: 5)
  --iterations <n>      iterations per (scenario, arm) (default: 3)
  --iteration-base <n>  index the first iteration from here (default: 1). One process per
                        cell means the internal counter always restarts at 1; this stamps the
                        run's real place in the matrix into the result record.
  --batch-size <n>      rows per Arrow batch (default: 8192, the production default)
  --scenario <id>       full_scan_count|projected_scan|filtered_scan (repeatable; default: all)
  --arm <id>            floor|row_engine|datafusion|row_pushdown (repeatable; default: all)
  --out <path>          write the JSON results document here
";
