//! Shared support for the strict Cassandra parity lanes (issue #1024).
//!
//! This is a **subdirectory module**, not a top-level integration test file, so
//! Cargo does NOT compile it as its own test binary. Include it from each strict
//! parity test via:
//!
//! ```ignore
//! #[path = "parity_support/mod.rs"]
//! mod parity_support;
//! ```
//!
//! It provides three things the hardened required-parity gate (issue #1024)
//! depends on:
//!
//!   * [`parity_datasets_required`] — the CI fail-closed switch. When
//!     `CQLITE_PARITY_REQUIRE_DATASETS=1` is set (the workflow sets it), a strict
//!     lane that would otherwise SKIP on absent binaries must instead PANIC, so a
//!     vanished/unfetched dataset can never silently green the required gate.
//!   * [`ParityFailure`] — a structured diagnostic builder. Every failure (or
//!     fail-closed dataset-absent panic) is formatted with the manifest scenario
//!     ID, the Cassandra source test / format rule, the fixture path, the
//!     component list, and a copy-pasteable reproduction command.
//!   * Diff writers ([`write_diff`], [`write_summary`], [`LaneStatus`]) — on a
//!     parity discrepancy each lane emits its diff to
//!     `target/cassandra-parity/<lane>.diff` and records a row in
//!     `target/cassandra-parity/summary.json`, which CI uploads as an artifact.
//!
//! Allowing dead code: not every consumer uses every helper, and each strict
//! test compiles this module independently.
#![allow(dead_code)]

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// CI fail-closed switch.
///
/// Returns `true` when `CQLITE_PARITY_REQUIRE_DATASETS=1` is set. In that mode a
/// strict parity lane must FAIL (panic) instead of skipping when the dataset
/// binaries are absent — a vanished dataset is a gate failure, not a free pass.
/// Locally (env unset) the lanes keep their existing skip-on-absence behavior.
pub fn parity_datasets_required() -> bool {
    std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
        .map(|v| v == "1")
        .unwrap_or(false)
}

/// Manifest scenario IDs (the `suite:` key in
/// `test-data/cassandra-parity-manifest.yml`) for the lanes wired through
/// [`ParityFailure`]. Centralised so a rename in the manifest is a one-line fix.
pub mod scenario {
    pub const INDEX_DB_BIG: &str = "sstable_parity_index_db_big";
    pub const SUMMARY_DB_BIG: &str = "sstable_parity_summary_db_big";
    pub const STATISTICS_DB: &str = "sstable_parity_statistics_db";
    pub const DATA_DB_JSONL: &str = "sstable_parity_data_db_jsonl";
    pub const COMPONENT_MANIFEST: &str = "sstable_parity_component_manifest";
    pub const COMPRESSION_INFO_CHUNKS: &str = "sstable_parity_compression_info_chunks";
    pub const DELTA_SCAN: &str = "sstable_parity_delta_scan";
}

/// Structured parity-failure / fail-closed diagnostic.
///
/// Build with the manifest scenario ID, then attach the Cassandra source rule,
/// the fixture path, the component list, and the reproduction command. Render
/// with [`ParityFailure::render`] (for an `eprintln!`/`panic!` message) or pass
/// straight to [`ParityFailure::panic`].
pub struct ParityFailure {
    scenario_id: String,
    cassandra_source: Option<String>,
    fixture: Option<PathBuf>,
    components: Vec<String>,
    repro: Option<String>,
    detail: Option<String>,
}

impl ParityFailure {
    /// Start a diagnostic for the given manifest scenario ID (see [`scenario`]).
    pub fn new(scenario_id: &str) -> Self {
        Self {
            scenario_id: scenario_id.to_string(),
            cassandra_source: None,
            fixture: None,
            components: Vec::new(),
            repro: None,
            detail: None,
        }
    }

    /// The Cassandra source test or format rule this lane mirrors
    /// (e.g. `RowIndexEntryTest.java`, "BTI Partitions.db replaces Summary.db").
    pub fn cassandra_source(mut self, source: &str) -> Self {
        self.cassandra_source = Some(source.to_string());
        self
    }

    /// The on-disk fixture path involved in the failure.
    pub fn fixture(mut self, fixture: impl Into<PathBuf>) -> Self {
        self.fixture = Some(fixture.into());
        self
    }

    /// The SSTable component list relevant to the failure (e.g. the TOC manifest
    /// or the components compared).
    pub fn components<I, S>(mut self, components: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.components = components.into_iter().map(Into::into).collect();
        self
    }

    /// A copy-pasteable local reproduction command.
    pub fn repro(mut self, repro: &str) -> Self {
        self.repro = Some(repro.to_string());
        self
    }

    /// The concrete discrepancy / reason (what was expected vs found).
    pub fn detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Render the full multi-line diagnostic string.
    pub fn render(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "PARITY FAILURE [scenario: {}]", self.scenario_id);
        if let Some(detail) = &self.detail {
            let _ = writeln!(s, "  reason          : {detail}");
        }
        if let Some(src) = &self.cassandra_source {
            let _ = writeln!(s, "  cassandra source: {src}");
        }
        if let Some(fixture) = &self.fixture {
            let _ = writeln!(s, "  fixture         : {}", fixture.display());
        }
        if !self.components.is_empty() {
            let _ = writeln!(s, "  components      : {}", self.components.join(", "));
        }
        if let Some(repro) = &self.repro {
            let _ = writeln!(s, "  reproduce       : {repro}");
        }
        s
    }

    /// Render and panic — the fail-closed terminal for a strict lane.
    pub fn panic(&self) -> ! {
        panic!("{}", self.render())
    }
}

/// Per-lane status recorded in `summary.json`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LaneStatus {
    Pass,
    Fail,
    Skip,
}

impl LaneStatus {
    fn as_str(self) -> &'static str {
        match self {
            LaneStatus::Pass => "pass",
            LaneStatus::Fail => "fail",
            LaneStatus::Skip => "skip",
        }
    }
}

/// The shared artifact directory: `target/cassandra-parity/`.
///
/// Resolved relative to `CARGO_MANIFEST_DIR`'s workspace `target/` so a single
/// directory collects every lane's diffs regardless of the test binary cwd.
fn parity_artifact_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("CQLITE_PARITY_ARTIFACT_DIR") {
        return PathBuf::from(dir);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|ws| ws.join("target/cassandra-parity"))
        .unwrap_or_else(|| PathBuf::from("target/cassandra-parity"))
}

/// Write a lane's diff body to `target/cassandra-parity/<lane>.diff`.
///
/// `lane` is the diff file stem (e.g. `index_db_big`, `data_db_jsonl`). `body`
/// is the already-formatted diff (byte diff, offset-delta diff, checksum diff,
/// or normalized-JSONL diff). Returns the written path. Errors are surfaced via
/// the returned `io::Result` so callers can include the path in a panic without
/// masking a write failure.
pub fn write_diff(lane: &str, body: &str) -> std::io::Result<PathBuf> {
    let dir = parity_artifact_dir();
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{lane}.diff"));
    std::fs::write(&path, body)?;
    Ok(path)
}

/// A single lane's row in `summary.json`.
struct SummaryRow {
    lane: String,
    status: LaneStatus,
    scenario_id: String,
    artifacts: Vec<String>,
}

static SUMMARY_ROWS: Mutex<Vec<SummaryRow>> = Mutex::new(Vec::new());

/// Record (and rewrite) one lane's entry in `target/cassandra-parity/summary.json`.
///
/// The summary maps `lane -> { status, scenario_id, artifacts }`. It is rewritten
/// on every call so the file is valid JSON at any point even if a lane panics
/// mid-suite. `artifacts` are the diff/artifact paths produced for the lane.
pub fn write_summary(
    lane: &str,
    status: LaneStatus,
    scenario_id: &str,
    artifacts: &[PathBuf],
) -> std::io::Result<()> {
    let mut rows = SUMMARY_ROWS.lock().unwrap_or_else(|e| e.into_inner());
    let artifact_strs: Vec<String> = artifacts.iter().map(|p| p.display().to_string()).collect();
    if let Some(existing) = rows.iter_mut().find(|r| r.lane == lane) {
        existing.status = status;
        existing.scenario_id = scenario_id.to_string();
        existing.artifacts = artifact_strs;
    } else {
        rows.push(SummaryRow {
            lane: lane.to_string(),
            status,
            scenario_id: scenario_id.to_string(),
            artifacts: artifact_strs,
        });
    }

    let json = render_summary_json(&rows);
    let dir = parity_artifact_dir();
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("summary.json"), json)?;
    Ok(())
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

fn render_summary_json(rows: &[SummaryRow]) -> String {
    let mut s = String::from("{\n");
    for (i, row) in rows.iter().enumerate() {
        let artifacts = row
            .artifacts
            .iter()
            .map(|a| format!("\"{}\"", json_escape(a)))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = write!(
            s,
            "  \"{}\": {{\"status\": \"{}\", \"scenario_id\": \"{}\", \"artifacts\": [{}]}}",
            json_escape(&row.lane),
            row.status.as_str(),
            json_escape(&row.scenario_id),
            artifacts,
        );
        if i + 1 < rows.len() {
            s.push(',');
        }
        s.push('\n');
    }
    s.push_str("}\n");
    s
}

// ----------------------------------------------------------------------------
// Diff body formatters
// ----------------------------------------------------------------------------

/// Byte-diff body: the first differing byte offset plus a short hex window from
/// each side. Used for Data.db / Index.db raw-byte lanes.
pub fn byte_diff(label_a: &str, a: &[u8], label_b: &str, b: &[u8]) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "byte diff: {label_a} ({} B) vs {label_b} ({} B)",
        a.len(),
        b.len()
    );
    let max = a.len().min(b.len());
    let mut first = None;
    for i in 0..max {
        if a[i] != b[i] {
            first = Some(i);
            break;
        }
    }
    let first = first.unwrap_or(max);
    let _ = writeln!(s, "first difference at byte offset {first}");
    let lo = first.saturating_sub(8);
    let _ = writeln!(s, "  {label_a}[{lo}..]: {}", hex_window(a, lo, 16));
    let _ = writeln!(s, "  {label_b}[{lo}..]: {}", hex_window(b, lo, 16));
    s
}

fn hex_window(buf: &[u8], start: usize, len: usize) -> String {
    buf.iter()
        .skip(start)
        .take(len)
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Offset-delta diff body: paired (expected, actual) offsets with their delta.
/// Used for the Index.db offset-delta lane.
pub fn offset_delta_diff(label: &str, pairs: &[(i64, i64)]) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "offset-delta diff: {label}");
    let _ = writeln!(s, "  idx  expected        actual          delta");
    for (i, (exp, act)) in pairs.iter().enumerate() {
        let _ = writeln!(s, "  {i:<4} {exp:<15} {act:<15} {}", act - exp);
    }
    s
}

/// Checksum / scalar-field diff body: expected vs actual for named fields.
/// Used for Statistics.db / Digest.crc32 lanes.
pub fn checksum_diff(label: &str, fields: &[(&str, String, String)]) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "checksum/field diff: {label}");
    for (name, expected, actual) in fields {
        let _ = writeln!(s, "  {name}: expected={expected} actual={actual}");
    }
    s
}

/// Normalized-JSONL diff body: the first line index whose normalized form
/// differs, plus both lines. Used for the Data.db JSONL lane.
pub fn jsonl_diff(label: &str, expected: &[String], actual: &[String]) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "normalized-JSONL diff: {label} (expected {} line(s), actual {} line(s))",
        expected.len(),
        actual.len()
    );
    let max = expected.len().max(actual.len());
    for i in 0..max {
        let e = expected.get(i).map(String::as_str).unwrap_or("<missing>");
        let a = actual.get(i).map(String::as_str).unwrap_or("<missing>");
        if e != a {
            let _ = writeln!(s, "first differing line {i}:");
            let _ = writeln!(s, "  expected: {e}");
            let _ = writeln!(s, "  actual  : {a}");
            return s;
        }
    }
    let _ = writeln!(
        s,
        "no line-level difference detected (length mismatch only)"
    );
    s
}

/// Convenience: assert a writable diff file path exists for inclusion in a
/// failure message; never panics on write failure (the artifact is best-effort,
/// the panic carries the body inline regardless).
pub fn artifact_hint(lane: &str, body: &str) -> Option<PathBuf> {
    let _ = (lane, body);
    write_diff(lane, body).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_includes_all_fields() {
        let f = ParityFailure::new(scenario::INDEX_DB_BIG)
            .cassandra_source("RowIndexEntryTest.java")
            .fixture(Path::new("/x/nb-1-big-Index.db"))
            .components(["Index.db", "Data.db"])
            .repro("cargo test ...")
            .detail("offset mismatch");
        let out = f.render();
        assert!(out.contains("sstable_parity_index_db_big"));
        assert!(out.contains("RowIndexEntryTest.java"));
        assert!(out.contains("nb-1-big-Index.db"));
        assert!(out.contains("Index.db, Data.db"));
        assert!(out.contains("cargo test ..."));
        assert!(out.contains("offset mismatch"));
    }

    #[test]
    fn byte_diff_reports_first_offset() {
        let body = byte_diff("a", &[1, 2, 3, 4], "b", &[1, 2, 9, 4]);
        assert!(body.contains("first difference at byte offset 2"));
    }

    #[test]
    fn jsonl_diff_reports_first_line() {
        let body = jsonl_diff("x", &["a".into(), "b".into()], &["a".into(), "c".into()]);
        assert!(body.contains("first differing line 1"));
    }

    #[test]
    fn summary_json_is_valid_shape() {
        let json = render_summary_json(&[SummaryRow {
            lane: "index_db_big".into(),
            status: LaneStatus::Fail,
            scenario_id: scenario::INDEX_DB_BIG.into(),
            artifacts: vec!["target/cassandra-parity/index_db_big.diff".into()],
        }]);
        assert!(json.contains("\"index_db_big\""));
        assert!(json.contains("\"status\": \"fail\""));
        assert!(json.contains("\"scenario_id\": \"sstable_parity_index_db_big\""));
    }
}
