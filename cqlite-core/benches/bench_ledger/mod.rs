//! Unified append-only perf history ledger (Issue #1566, Epic A / A5).
//!
//! This is the single append path every A-series harness bench uses to persist its
//! run metrics, replacing the fragmented pair of ledgers A5 was chartered to unify
//! (the A2 `tail_latency` bench's bespoke `tail-latency-history.jsonl` and
//! `profile_report.py`'s run-summary line). Each bench includes it via `#[path]`
//! (the same pattern `tail_latency/mod.rs` and `fixtures/mod.rs` use), so every
//! bench compiles its own copy and refers to it as `crate::bench_ledger`. Like the
//! other shared bench-support modules, each includer uses only a subset, hence the
//! module-wide `dead_code` allowance.
//!
//! # Schema (one JSON object per line, one record PER metric)
//!
//! ```text
//! {"ts": <unix_secs>, "commit": "<sha|unknown>", "bench": "<id>",
//!  "metric": "<name>", "value": <number>, "unit": "<str>"}
//! ```
//!
//! A single harness run emits several lines — e.g. the `tail_latency` bench writes
//! `tail_latency`/`mixed_p99` (`ns`), `tail_latency`/`p99_over_p50` (`ratio`), … as
//! separate records. `profile_report.py` writes the SAME schema for its criterion
//! medians (`<group>/<bench>`/`median_ns`) and peak heap, and
//! `./scripts/profile.sh report` reads the whole ledger back into a longitudinal
//! per-metric table.
//!
//! # Path
//!
//! `target/profiling/history.jsonl` — the path `profile_report.py` and
//! `docs/profiling.md` already document — resolvable from a bench via the
//! `CQLITE_BENCH_LEDGER` env override, else `<CARGO_MANIFEST_DIR>/../target/
//! profiling/history.jsonl`. It is machine-specific generated run data: gitignored
//! (it lives under `target/`, which is ignored), and CI may upload it as an
//! artifact — we do not commit it (a committed, per-machine, churning ledger would
//! be noise and a merge-race magnet).
//!
//! # Best-effort
//!
//! [`append_metrics`] returns `io::Result` so a test can assert success, but a
//! ledger write must NEVER abort a measurement run: bench mains log the error to
//! stderr and continue (see `tail_latency.rs` / `open.rs`). The spec's "a ledger
//! write failure does not fail the bench" guarantee lives at those call sites.

#![allow(dead_code)]

use std::io::Write as _;
use std::path::PathBuf;

/// One appended ledger record: a single metric of a single bench run.
///
/// Serialized as one JSON line. `commit` is owned (stamped per call);
/// `bench`/`metric`/`unit` borrow the caller's strings to avoid per-metric
/// allocation across a batch.
#[derive(serde::Serialize)]
struct LedgerRecord<'a> {
    ts: u64,
    commit: String,
    bench: &'a str,
    metric: &'a str,
    value: f64,
    unit: &'a str,
}

/// Resolve the ledger path: the `CQLITE_BENCH_LEDGER` env override if set and
/// non-empty, else `<CARGO_MANIFEST_DIR>/../target/profiling/history.jsonl`
/// (anchored to the crate dir, not the CWD, so the same file is appended
/// regardless of where cargo runs the bench).
pub fn ledger_path() -> PathBuf {
    if let Ok(p) = std::env::var("CQLITE_BENCH_LEDGER") {
        let p = p.trim();
        if !p.is_empty() {
            return PathBuf::from(p);
        }
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/profiling/history.jsonl")
}

/// Best-effort current commit SHA: `GIT_COMMIT` env override, else `git rev-parse
/// HEAD`, else `"unknown"`. Never fails (ledger append must not abort a run).
/// Reused from the A2 `tail_latency` harness, now homed in the single writer.
fn current_commit() -> String {
    if let Ok(sha) = std::env::var("GIT_COMMIT") {
        let sha = sha.trim().to_string();
        if !sha.is_empty() {
            return sha;
        }
    }
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Unix-seconds timestamp (0 if the clock is before the epoch — never panics).
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Append one JSON-line record PER metric for `bench` to the unified ledger
/// (creating the file and its parent dirs if absent). All records in one call
/// share the same `ts` and `commit`. Generated run data; the ledger is gitignored.
///
/// Returns `io::Result` so a test can assert the write; bench mains treat a failure
/// as best-effort (log to stderr, continue) — a ledger write must not fail a run.
pub fn append_metrics(bench: &str, metrics: &[(&str, f64, &str)]) -> std::io::Result<()> {
    let path = ledger_path();
    if let Some(parent) = path.parent() {
        // A bare filename (e.g. `CQLITE_BENCH_LEDGER=history.jsonl`) yields an empty
        // parent; `create_dir_all("")` errors, so only create a real parent dir.
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let ts = now_secs();
    let commit = current_commit();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    for (metric, value, unit) in metrics {
        let record = LedgerRecord {
            ts,
            commit: commit.clone(),
            bench,
            metric,
            value: *value,
            unit,
        };
        let line = serde_json::to_string(&record).map_err(std::io::Error::other)?;
        writeln!(file, "{line}")?;
    }
    Ok(())
}

// Tests live in `tests/bench_ledger.rs` (an integration test harness), not inline:
// Cargo compiles bench targets with `cfg(test)` set, so an inline `#[cfg(test)] mod
// tests` would be pulled into every harness bench binary (where `#[test]` fns are
// dead and their imports flagged under `-D warnings`). The integration crate that
// includes this module via `#[path]` is a real test harness and runs them cleanly —
// the same convention `tail_latency/mod.rs` follows.
