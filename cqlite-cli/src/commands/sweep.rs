//! `cqlite sweep` — verify every SSTable table directory under a data
//! directory in one pass (issue #4194).
//!
//! Thin CLI wrapper over the SAME
//! [`cqlite_core::storage::sstable::verify::verify_sstable`] a single-directory
//! `cqlite verify` calls — one call per discovered `<keyspace>/<table>-<id>/`
//! directory, bounded to at most `--jobs` concurrently. Memory profile per
//! table is therefore identical to today's `verify --mode full` (design.md §S3):
//! no data-dir-wide structure is materialized before rendering.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::verify::{
    verify_sstable, VerifyErrorClass, VerifyFinding, VerifyMode,
};
use cqlite_core::Config;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::cli_types::{SweepArgs, VerifyModeArg, VerifyOutputArg};
use crate::commands::verify::{finding_to_json, json_str};

/// Per-table severity (design.md §D3/§S1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Severity {
    Ok,
    /// A CQLite-only detection with no Cassandra-verified corruption (today
    /// exactly `FilterFalseNegative`, issue #1398) — worth attention, not
    /// proof the SSTable is unreadable.
    Degraded,
    Corrupt,
    /// The directory could not even be opened for verification (no readable
    /// `Data.db`, unopenable directory, or `verify_sstable` failed to resolve
    /// components).
    Unreadable,
}

impl Severity {
    fn as_str(self) -> &'static str {
        match self {
            Severity::Ok => "ok",
            Severity::Degraded => "degraded",
            Severity::Corrupt => "corrupt",
            Severity::Unreadable => "unreadable",
        }
    }
}

/// One swept table directory's outcome.
struct SweepRow {
    path: PathBuf,
    severity: Severity,
    /// Human-readable cause; `None` only when `severity == Ok`.
    cause: Option<String>,
    /// The underlying `VerifyReport.findings` (empty for `Unreadable`, since
    /// `verify_sstable` never returned a report for that directory).
    findings: Vec<VerifyFinding>,
}

/// Discover every `<keyspace>/<table>-<id>/` directory under `data_dir`,
/// structurally (readdir only — no `Data.db` content is touched here), so a
/// directory that turns out unreadable still becomes exactly one row later,
/// never a silent omission (design.md §D3).
///
/// `unreadable_keyspaces` (roborev round-1 MEDIUM finding): a keyspace
/// directory this walk cannot even `read_dir` used to be silently skipped —
/// no row, no cause, no exit-code effect, "totals: ok=N, exit 0" for a sweep
/// that never looked under it. Returning it separately, by (path, cause), lets
/// the caller turn it directly into an `unreadable` [`SweepRow`] WITHOUT
/// calling `verify_sstable` on it (there is nothing under it to verify).
struct Discovered {
    table_dirs: Vec<PathBuf>,
    unreadable_keyspaces: Vec<(PathBuf, String)>,
}

fn discover_table_dirs(data_dir: &Path) -> Result<Discovered> {
    if !data_dir.is_dir() {
        anyhow::bail!(
            "sweep target does not exist or is not a directory: {}",
            data_dir.display()
        );
    }
    let mut table_dirs = Vec::new();
    let mut unreadable_keyspaces = Vec::new();
    let keyspaces = std::fs::read_dir(data_dir)
        .map_err(|e| anyhow::anyhow!("cannot read data dir {}: {e}", data_dir.display()))?;
    for ks_entry in keyspaces.flatten() {
        let ks_path = ks_entry.path();
        if !ks_path.is_dir() {
            continue;
        }
        match std::fs::read_dir(&ks_path) {
            Ok(tables) => {
                for table_entry in tables.flatten() {
                    let table_path = table_entry.path();
                    if table_path.is_dir() {
                        table_dirs.push(table_path);
                    }
                }
            }
            Err(e) => unreadable_keyspaces.push((ks_path, e.to_string())),
        }
    }
    table_dirs.sort();
    unreadable_keyspaces.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(Discovered {
        table_dirs,
        unreadable_keyspaces,
    })
}

/// Map a completed [`VerifyReport`] to its severity + cause (design.md §D3):
/// `FilterFalseNegative`-only is `Degraded`; any other non-empty finding set
/// is `Corrupt`. A closed function of `VerifyErrorClass`, stated once.
fn classify_report(findings: &[VerifyFinding]) -> (Severity, Option<String>) {
    if findings.is_empty() {
        return (Severity::Ok, None);
    }
    let only_filter_false_negative = findings
        .iter()
        .all(|f| f.class == VerifyErrorClass::FilterFalseNegative);
    if only_filter_false_negative {
        (
            Severity::Degraded,
            Some(VerifyErrorClass::FilterFalseNegative.code().to_string()),
        )
    } else {
        let cause = findings
            .first()
            .map(|f| f.class.code().to_string())
            .unwrap_or_default();
        (Severity::Corrupt, Some(cause))
    }
}

async fn verify_one(
    dir: PathBuf,
    mode: VerifyMode,
    config: Config,
    platform: Arc<Platform>,
) -> SweepRow {
    match verify_sstable(&dir, mode, &config, platform).await {
        Ok(report) => {
            let (severity, cause) = classify_report(&report.findings);
            SweepRow {
                path: dir,
                severity,
                cause,
                findings: report.findings,
            }
        }
        Err(e) => SweepRow {
            path: dir,
            severity: Severity::Unreadable,
            cause: Some(e.to_string()),
            findings: Vec::new(),
        },
    }
}

/// Execute `cqlite sweep <data-dir> [--mode] [--out] [--jobs]`.
///
/// Exit codes (design.md §S2, `std::process::exit` — the SAME
/// environmental-vs-verification-failure split `verify` already established):
/// `1` on a usage error (bad `--data-dir`, before any verification is
/// attempted); `2` if any row is `corrupt`/`unreadable`; `0` otherwise
/// (`degraded` rows alone never trip a non-zero exit).
pub async fn execute_sweep_command(args: &SweepArgs) -> Result<()> {
    let discovered = match discover_table_dirs(&args.data_dir) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    // Issue #4194, roborev round-1 MEDIUM finding: a keyspace directory this
    // walk could not even `read_dir` becomes its own `unreadable` row here,
    // directly — never passed to `verify_sstable` (there is nothing under it
    // to verify) and never silently absent from `rows`.
    let mut rows: Vec<SweepRow> = discovered
        .unreadable_keyspaces
        .into_iter()
        .map(|(path, cause)| SweepRow {
            path,
            severity: Severity::Unreadable,
            cause: Some(cause),
            findings: Vec::new(),
        })
        .collect();
    let dirs = discovered.table_dirs;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let mode = match args.mode {
        VerifyModeArg::Quick => VerifyMode::Quick,
        VerifyModeArg::Full => VerifyMode::Full,
    };
    let jobs = args
        .jobs
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
        .max(1);

    // Bounded concurrency (design.md §S3): at most `jobs` `verify_sstable`
    // calls in flight, each with the SAME single-table memory profile as
    // `verify` already has — no data-dir-wide structure is ever resident.
    let semaphore = Arc::new(Semaphore::new(jobs));
    let mut set = JoinSet::new();
    for dir in dirs {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore never closed");
        let config = config.clone();
        let platform = platform.clone();
        set.spawn(async move {
            let row = verify_one(dir, mode, config, platform).await;
            drop(permit);
            row
        });
    }

    rows.reserve(set.len());
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(row) => rows.push(row),
            Err(e) => {
                // A panicked/cancelled task is itself an unreadable-class
                // outcome — named rather than silently dropping the row
                // (design.md §D3's "never a silent skip").
                rows.push(SweepRow {
                    path: PathBuf::from("<unknown: sweep worker task failed>"),
                    severity: Severity::Unreadable,
                    cause: Some(format!("sweep worker task failed: {e}")),
                    findings: Vec::new(),
                });
            }
        }
    }
    rows.sort_by(|a, b| a.path.cmp(&b.path));

    // Issue #4194, roborev round-1 MEDIUM finding: a data dir that exists but
    // holds zero table directories previously produced `totals: ok=0 …` and
    // exit 0 — a "clean bill of health" indistinguishable from an all-healthy
    // corpus, for a sweep that verified NOTHING. Pointing `sweep` one level
    // too high (or at an unpopulated root) must not read as success —
    // affirmative-zero doctrine, and the "never let a dataset-dependent
    // operation pass on an empty dataset" rule.
    if rows.is_empty() {
        eprintln!(
            "Error: no table directories found under {} (expected <keyspace>/<table>-<id>/ \
             subdirectories) — nothing was verified",
            args.data_dir.display()
        );
        std::process::exit(2);
    }

    match args.out {
        VerifyOutputArg::Text => print_text(&rows),
        VerifyOutputArg::Json => print_json(&rows),
    }

    let any_bad = rows
        .iter()
        .any(|r| matches!(r.severity, Severity::Corrupt | Severity::Unreadable));
    if any_bad {
        std::process::exit(2);
    }
    Ok(())
}

fn print_text(rows: &[SweepRow]) {
    let mut totals = [0usize; 4]; // ok, degraded, corrupt, unreadable
    for row in rows {
        let idx = match row.severity {
            Severity::Ok => 0,
            Severity::Degraded => 1,
            Severity::Corrupt => 2,
            Severity::Unreadable => 3,
        };
        totals[idx] += 1;
        match row.severity {
            Severity::Ok => println!("ok         {}", row.path.display()),
            _ => {
                let cause = row.cause.as_deref().unwrap_or("(no cause recorded)");
                println!(
                    "{:<11}{} — {}",
                    row.severity.as_str(),
                    row.path.display(),
                    cause
                );
                for f in &row.findings {
                    println!(
                        "             - [{}] {}: {}",
                        f.class.code(),
                        f.component,
                        f.detail
                    );
                }
            }
        }
    }
    println!(
        "totals: ok={} degraded={} corrupt={} unreadable={} (rows={})",
        totals[0],
        totals[1],
        totals[2],
        totals[3],
        rows.len()
    );
}

fn print_json(rows: &[SweepRow]) {
    let mut totals = [0usize; 4];
    let row_json: Vec<String> = rows
        .iter()
        .map(|row| {
            let idx = match row.severity {
                Severity::Ok => 0,
                Severity::Degraded => 1,
                Severity::Corrupt => 2,
                Severity::Unreadable => 3,
            };
            totals[idx] += 1;
            let cause = row
                .cause
                .as_deref()
                .map(json_str)
                .unwrap_or_else(|| "null".to_string());
            let findings: Vec<String> = row.findings.iter().map(finding_to_json).collect();
            format!(
                "{{\"path\":{},\"severity\":{},\"cause\":{},\"findings\":[{}]}}",
                json_str(&row.path.display().to_string()),
                json_str(row.severity.as_str()),
                cause,
                findings.join(","),
            )
        })
        .collect();

    // Affirmative-zero doctrine: every severity key is present even at 0.
    println!(
        "{{\"rows\":[{}],\"totals\":{{\"ok\":{},\"degraded\":{},\"corrupt\":{},\"unreadable\":{}}}}}",
        row_json.join(","),
        totals[0],
        totals[1],
        totals[2],
        totals[3],
    );
}
