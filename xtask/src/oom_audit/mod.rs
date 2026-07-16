//! `oom-audit` — static AST audit for the no-unbounded-materialization
//! invariant (issue #2012). Orchestrates: resolve scope → parse each in-scope
//! `.rs` → run the `STREAM_RETURNS_VEC` rule → apply the allowlist → report or
//! enforce. See `rule`, `allowlist`, `scope`, `fingerprint`.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

pub mod allowlist;
pub mod fingerprint;
pub mod rule;
pub mod scope;

use allowlist::{Allowlist, AllowlistProblem};
use rule::Finding;

/// The committed allowlist, repo-root-relative.
pub const ALLOWLIST_PATH: &str = "xtask/oom-audit-allowlist.toml";

/// Result of an audit run.
pub struct Outcome {
    /// Findings not suppressed by any allowlist entry.
    pub findings: Vec<Finding>,
    /// Allowlist entries that fail (orphaned / malformed / expired).
    pub allowlist_problems: Vec<AllowlistProblem>,
    /// Files that failed to parse (reported, never fatal).
    pub parse_errors: Vec<String>,
}

impl Outcome {
    /// True if enforce mode should fail: any live finding or allowlist problem.
    pub fn has_failures(&self) -> bool {
        !self.findings.is_empty() || !self.allowlist_problems.is_empty()
    }

    /// Whether the process should exit non-zero: only in enforce mode, and only
    /// when there is at least one live finding or allowlist problem.
    pub fn should_fail(&self, enforce: bool) -> bool {
        enforce && self.has_failures()
    }

    /// Exit code: report-only always `0`; enforce non-zero on any failure.
    pub fn exit_code(&self, enforce: bool) -> ExitCode {
        if self.should_fail(enforce) {
            ExitCode::from(1)
        } else {
            ExitCode::SUCCESS
        }
    }
}

/// Locate the workspace root. When run via `cargo run -p xtask`, the crate
/// manifest dir is `<root>/xtask`, so the parent is the workspace root.
pub fn repo_root() -> Result<PathBuf, String> {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dir = Path::new(manifest);
    dir.parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| format!("no parent of manifest dir {manifest}"))
}

/// Run the audit over the v1 scope roots under `repo_root`, print a human report
/// to stdout, and return the outcome. `enforce` only affects the exit code (via
/// `Outcome::exit_code`) and the framing of the printed report.
pub fn run(repo_root: &Path, enforce: bool) -> Result<Outcome, String> {
    let today = today_utc();
    let (findings_all, parse_errors) = collect_findings(repo_root);

    let allowlist_path = repo_root.join(ALLOWLIST_PATH);
    let allowlist = Allowlist::load(&allowlist_path)?;

    let mut allowlist_problems = allowlist.structural_problems(&today);
    let (remaining, orphans) = allowlist.apply(&findings_all);
    allowlist_problems.extend(orphans);

    let findings: Vec<Finding> = remaining.into_iter().cloned().collect();
    let outcome = Outcome {
        findings,
        allowlist_problems,
        parse_errors,
    };
    print_report(&outcome, enforce);
    Ok(outcome)
}

/// Walk the scope roots and analyze every in-scope `.rs`. Returns all raw
/// findings (pre-allowlist) plus any parse-error messages.
fn collect_findings(repo_root: &Path) -> (Vec<Finding>, Vec<String>) {
    let mut findings = Vec::new();
    let mut parse_errors = Vec::new();
    for dir in scope::walk_dirs(repo_root) {
        if !dir.is_dir() {
            continue;
        }
        for entry in walkdir::WalkDir::new(&dir)
            .into_iter()
            .filter_map(Result::ok)
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let Some(rel) = scope::rel_path(repo_root, entry.path()) else {
                continue;
            };
            if !scope::in_scope(&rel) {
                continue;
            }
            let src = match std::fs::read_to_string(entry.path()) {
                Ok(s) => s,
                Err(e) => {
                    parse_errors.push(format!("{rel}: read error: {e}"));
                    continue;
                }
            };
            match rule::analyze_file(&rel, &src) {
                Ok(mut fs) => findings.append(&mut fs),
                Err(e) => parse_errors.push(format!("{rel}: parse error: {e}")),
            }
        }
    }
    findings.sort_by(|a, b| {
        (a.file.as_str(), a.function.as_str(), a.fingerprint.as_str()).cmp(&(
            b.file.as_str(),
            b.function.as_str(),
            b.fingerprint.as_str(),
        ))
    });
    (findings, parse_errors)
}

fn print_report(outcome: &Outcome, enforce: bool) {
    println!("oom-audit: STREAM_RETURNS_VEC over v1 scope (mode: {})", {
        if enforce {
            "enforce"
        } else {
            "report-only"
        }
    });

    if !outcome.parse_errors.is_empty() {
        println!(
            "  note: {} file(s) skipped (unparseable):",
            outcome.parse_errors.len()
        );
        for e in &outcome.parse_errors {
            println!("    - {e}");
        }
    }

    if outcome.findings.is_empty() {
        println!("  findings: none (0 unallowlisted)");
    } else {
        println!("  findings: {} unallowlisted", outcome.findings.len());
        for f in &outcome.findings {
            println!(
                "    [{}] {}::{}\n        expr: {}\n        fingerprint: {}",
                f.rule, f.file, f.function, f.expr, f.fingerprint
            );
        }
    }

    if !outcome.allowlist_problems.is_empty() {
        println!("  allowlist problems: {}", outcome.allowlist_problems.len());
        for p in &outcome.allowlist_problems {
            println!("    - {}", p.describe());
        }
    }

    if enforce && outcome.has_failures() {
        println!("oom-audit: FAIL (enforce)");
    } else if outcome.has_failures() {
        println!("oom-audit: findings present (report-only: not failing the build)");
    } else {
        println!("oom-audit: clean");
    }
}

/// Today's UTC date as "YYYY-MM-DD", dependency-free (civil date from Unix days
/// via Howard Hinnant's algorithm). Only used for expiry comparison.
fn today_utc() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = (secs / 86_400) as i64;
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 -> (year, month, day).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome_with_finding() -> Outcome {
        Outcome {
            findings: vec![Finding {
                rule: "STREAM_RETURNS_VEC",
                file: "cqlite-core/src/query/x.rs".to_string(),
                function: "scan_all".to_string(),
                expr: "it.collect::<Vec<Row>>()".to_string(),
                fingerprint: "f1:abc".to_string(),
            }],
            allowlist_problems: vec![],
            parse_errors: vec![],
        }
    }

    #[test]
    fn report_only_never_fails_even_with_findings() {
        // Spec: report-only prints findings but does not fail the build.
        let outcome = outcome_with_finding();
        assert!(outcome.has_failures());
        assert!(!outcome.should_fail(false));
    }

    #[test]
    fn enforce_fails_on_any_finding_or_problem() {
        let outcome = outcome_with_finding();
        assert!(outcome.should_fail(true));
    }

    #[test]
    fn clean_outcome_never_fails() {
        let clean = Outcome {
            findings: vec![],
            allowlist_problems: vec![],
            parse_errors: vec![],
        };
        assert!(!clean.has_failures());
        assert!(!clean.should_fail(true));
    }

    #[test]
    fn civil_date_conversion_matches_known_epochs() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // 2000-01-01 is 10957 days after 1970-01-01.
        assert_eq!(civil_from_days(10_957), (2000, 1, 1));
        // 2026-07-16 is 20650 days after epoch.
        assert_eq!(civil_from_days(20_650), (2026, 7, 16));
    }
}
