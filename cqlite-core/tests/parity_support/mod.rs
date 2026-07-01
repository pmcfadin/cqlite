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
//!   * Diff writers ([`write_diff`], [`write_summary`], [`LaneStatus`]) and the
//!     four diff-body formatters ([`byte_diff`], [`offset_delta_diff`],
//!     [`checksum_diff`], [`jsonl_diff`]). A `target/cassandra-parity/<lane>.diff`
//!     (plus a `Fail` row in `target/cassandra-parity/summary.json`, which CI
//!     uploads as an artifact) is written before aborting on TWO kinds of failure.
//!     (a) The dataset-absent fail-closed path: `CQLITE_PARITY_REQUIRE_DATASETS=1`
//!     with the binaries missing — the lane diff carries the structured
//!     diagnostic. (b) The wired real-mismatch sites, one per diff type:
//!     [`byte_diff`] at the Index.db raw-key byte mismatch (`index_db_big`);
//!     [`offset_delta_diff`] at the Index.db offset-delta vs JSONL position-delta
//!     mismatch (`index_db_big`); [`checksum_diff`] at the Statistics.db
//!     accumulated-TOC-CRC32 mismatch (`statistics_db`); and [`jsonl_diff`] at the
//!     Data.db JSONL row/value parity failure (`data_db_jsonl`). All OTHER parity
//!     assertion sites still fail closed via `assert_eq!`/`assert!`/`panic!` with
//!     the discrepancy printed to the test stdout (no `<lane>.diff` is written for
//!     those); they remain real, build-failing assertions regardless.
//!
//! Allowing dead code: not every consumer uses every helper, and each strict
//! test compiles this module independently.
#![allow(dead_code)]

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

// The shared scenario-id-keyed failure-bundle emitter (issue #1027 Wave 2a). It
// lives beside this module; including it here routes the REAL required-parity
// failure terminal ([`ParityFailure::panic`]) through the same emitter the
// synthetic test exercises, so a real red gate produces
// `parity-failures/<tier>/<scenario_id>/failure-artifact.json`.
#[path = "../parity_bundle/mod.rs"]
mod parity_bundle;

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

/// The manifest join data the shared failure-artifact bundle needs but a
/// suite-keyed [`ParityFailure`] does not itself carry (issue #1027 finding 1).
///
/// A `ParityFailure` is keyed by the manifest *suite* (e.g.
/// `sstable_parity_data_db_jsonl`), but the uniform failure-artifact record is
/// keyed by a manifest `cass.*` *scenario id* and needs a `tier` + `evidence_type`.
/// A suite spans many `cass.*` scenarios; we bind each wired suite to ONE
/// representative REAL manifest scenario so a red required-parity run emits a
/// bundle that joins straight back to the manifest (never an invented id).
#[derive(Clone, Copy, Debug)]
pub struct BundleDescriptor {
    /// A REAL `cass.*` manifest scenario id for this suite (schema join key).
    pub cass_scenario_id: &'static str,
    /// The manifest `ci.tier` for that scenario.
    pub tier: &'static str,
    /// The manifest `evidence.type` for that scenario.
    pub evidence_type: &'static str,
    /// What the scenario compares (record `artifacts_compared`).
    pub artifacts_compared: &'static [&'static str],
}

/// Map a wired suite id to its representative manifest [`BundleDescriptor`], or
/// `None` for suites not wired to the shared bundle (e.g. the `manual_debug`
/// delta-scan suite). The `cass_scenario_id`s are asserted to exist in the real
/// manifest by the wiring test `bundle_descriptors_are_real_manifest_ids`.
pub fn bundle_descriptor_for_suite(suite: &str) -> Option<BundleDescriptor> {
    const BYTE: &[&str] = &["bytes", "offsets", "checksums", "component_files"];
    const JSONL: &[&str] = &["jsonl"];
    let d = match suite {
        s if s == scenario::INDEX_DB_BIG => BundleDescriptor {
            cass_scenario_id: "cass.index_db.RowIndexEntryTest.partition_offsets",
            tier: "required_parity",
            evidence_type: "byte_for_byte",
            artifacts_compared: BYTE,
        },
        s if s == scenario::SUMMARY_DB_BIG => BundleDescriptor {
            cass_scenario_id: "cass.summary_db.IndexSummaryTest.serialization_round_trip",
            tier: "required_parity",
            evidence_type: "byte_for_byte",
            artifacts_compared: BYTE,
        },
        s if s == scenario::STATISTICS_DB => BundleDescriptor {
            cass_scenario_id: "cass.statistics_db.MetadataSerializerTest.metadata_components",
            tier: "required_parity",
            evidence_type: "byte_for_byte",
            artifacts_compared: BYTE,
        },
        s if s == scenario::DATA_DB_JSONL => BundleDescriptor {
            cass_scenario_id: "cass.data_db_decode.row_cell_flags_and_vint",
            tier: "required_parity",
            evidence_type: "canonical_semantic",
            artifacts_compared: JSONL,
        },
        s if s == scenario::COMPONENT_MANIFEST => BundleDescriptor {
            cass_scenario_id: "cass.sstable_format.toc_component_manifest",
            tier: "fast_pr",
            evidence_type: "byte_for_byte",
            artifacts_compared: BYTE,
        },
        s if s == scenario::COMPRESSION_INFO_CHUNKS => BundleDescriptor {
            cass_scenario_id:
                "cass.compression_info.CompressionMetadataTest.metadata_serialization",
            tier: "required_parity",
            evidence_type: "byte_for_byte",
            artifacts_compared: BYTE,
        },
        _ => return None,
    };
    Some(d)
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
    /// Diff-artifact lane stem (e.g. `index_db_big`). When set, [`ParityFailure::panic`]
    /// writes `target/cassandra-parity/<lane>.diff` and a `summary.json` row before
    /// panicking, so CI uploads a structured diff for the failure.
    lane: Option<String>,
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
            lane: None,
        }
    }

    /// Set the diff-artifact lane stem so [`ParityFailure::panic`] emits a
    /// `target/cassandra-parity/<lane>.diff` and records the lane in `summary.json`.
    pub fn lane(mut self, lane: &str) -> Self {
        self.lane = Some(lane.to_string());
        self
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
    ///
    /// If a [`ParityFailure::lane`] was set, this first (best-effort) writes the
    /// rendered diagnostic to `target/cassandra-parity/<lane>.diff` and records a
    /// `Fail` row in `summary.json`, so the CI artifact upload captures a
    /// structured diff for the failure before the test process aborts.
    ///
    /// Issue #1027 finding 1: this is the SINGLE common failure terminal for every
    /// concrete required-parity mismatch site, so it ALSO emits the shared,
    /// scenario-id-keyed failure bundle (`parity-failures/<tier>/<scenario_id>/`)
    /// through the same emitter the synthetic test exercises — a real red gate now
    /// produces the same `failure-artifact.json` the workflow upload globs collect.
    /// Both the bundle and the legacy `target/cassandra-parity/**` diff are kept.
    pub fn panic(&self) -> ! {
        let rendered = self.render();
        if let Some(lane) = &self.lane {
            let artifact = write_diff(lane, &rendered).ok();
            let artifacts: Vec<PathBuf> = artifact.into_iter().collect();
            let _ = write_summary(lane, LaneStatus::Fail, &self.scenario_id, &artifacts);
        }
        self.emit_failure_bundle(&rendered);
        panic!("{rendered}")
    }

    /// Best-effort: emit the shared scenario-id-keyed failure bundle for this
    /// failure. No-ops (logging to stderr) rather than masking the parity panic if
    /// the suite is not wired to the bundle, no fixture was attached (its dataset
    /// SHA-256 is a schema-required field), or the write fails — the panic below
    /// still fails the build regardless (fail-closed, owner decision 2).
    fn emit_failure_bundle(&self, rendered: &str) {
        let Some(desc) = bundle_descriptor_for_suite(&self.scenario_id) else {
            return;
        };
        let Some(fixture) = self.fixture.clone() else {
            eprintln!(
                "issue-1027: no fixture on ParityFailure[{}]; skipping shared bundle emit",
                self.scenario_id
            );
            return;
        };
        let repro = parity_bundle::ReproContext {
            cassandra_version: "5.0.2".to_string(),
            cassandra_git_sha: "f278f6774fc76465c182041e081982105c3e7dbb".to_string(),
            fixture_path: fixture,
            component_list: self.components.clone(),
            command_line: self
                .repro
                .clone()
                .unwrap_or_else(|| "see the parity test source".to_string()),
        };
        let mut bundle = parity_bundle::FailureBundle::new(
            parity_failures_root(),
            desc.cass_scenario_id,
            "sstabledump-parity-gate.yml",
            desc.tier,
            desc.evidence_type,
            repro,
        )
        .artifacts_compared(desc.artifacts_compared.iter().copied())
        .stdout(rendered.to_string())
        .stderr(rendered.to_string());
        // Attach the already-rendered diagnostic as the evidence diff for this
        // evidence type so the bundle's diffs[] resolves.
        bundle = match desc.evidence_type {
            "canonical_semantic" => {
                bundle.jsonl(rendered.to_string(), String::new(), String::new())
            }
            _ => bundle.byte_for_byte_component(
                self.components
                    .first()
                    .map(String::as_str)
                    .unwrap_or("component"),
                rendered.to_string(),
                rendered.to_string(),
                rendered.to_string(),
                rendered.to_string(),
            ),
        };
        match bundle.emit() {
            Ok(emitted) => eprintln!(
                "issue-1027: wrote failure bundle {}",
                emitted.bundle_dir.display()
            ),
            Err(e) => eprintln!(
                "issue-1027: ERROR writing failure bundle for {}: {e}",
                desc.cass_scenario_id
            ),
        }
    }
}

/// The deterministic root the shared failure bundle is written beneath, chosen so
/// the emitted `parity-failures/**` tree matches the workflow upload globs
/// (`parity-failures/**` and `cqlite-core/parity-failures/**`) in
/// `sstabledump-parity-gate.yml` / `compaction-parity.yml`.
///
/// Resolves to the workspace root (the `CARGO_MANIFEST_DIR` parent, exactly as
/// [`parity_artifact_dir`] resolves `target/`) so `<root>/parity-failures/` sits
/// at the repo root regardless of the test binary's cwd. Overridable via
/// `CQLITE_PARITY_FAILURES_ROOT` (the e2e test points it at a tempdir).
fn parity_failures_root() -> PathBuf {
    if let Ok(dir) = std::env::var("CQLITE_PARITY_FAILURES_ROOT") {
        return PathBuf::from(dir);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
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

/// Serialises summary.json writes **within a single process only**. There is no
/// cross-process synchronization: the five strict lanes run as separate test
/// binaries, and [`write_summary`] best-effort merges with the existing on-disk
/// file (read-modify-write), so two separate-process lane writes that interleave
/// can race and lose a row (last writer wins). This is artifact-only and never
/// affects pass/fail — the summary.json is a CI diagnostic, not a gate input —
/// so file locking is intentionally omitted to keep it simple.
static SUMMARY_LOCK: Mutex<()> = Mutex::new(());

/// Record (and rewrite) one lane's entry in `target/cassandra-parity/summary.json`.
///
/// The summary maps `lane -> { status, scenario_id, artifacts }`. Because the
/// strict lanes run as separate test binaries (separate processes), the existing
/// on-disk file is read and merged on every call so a row written by one lane is
/// preserved when another lane (another process) writes. The file is fully
/// rewritten each time so it is valid JSON at any point even if a lane panics.
///
/// NOTE: the merge is best-effort and **not cross-process safe** — the only
/// synchronization is the in-process [`SUMMARY_LOCK`], so two concurrent
/// separate-process lane writes can interleave their read-modify-write and lose
/// a row. That is acceptable here because summary.json is an artifact-only CI
/// diagnostic and never gates pass/fail.
///
/// LANE-ROW SEMANTICS (artifact-only, NON-GATING): a row is keyed by `lane`
/// stem, and several distinct tests can legitimately share one stem (e.g. both
/// `big_index_db_entry_byte_and_field_parity` and
/// `truncated_big_index_db_is_not_silently_full_or_empty` write `index_db_big`).
/// Because each call overwrites the existing row, a lane row reflects only the
/// LAST writer for that stem within a run — NOT the aggregate pass/fail of every
/// test mapped to it. A `pass` row therefore does NOT prove every test on that
/// lane passed; a sibling test on the same stem can have failed (and, being a
/// real `assert!`/`panic!`, that test still fails the build regardless of what
/// the summary row says). Read summary.json as a per-stem last-write snapshot,
/// never as the gate of record. The gate is the test outcomes themselves.
pub fn write_summary(
    lane: &str,
    status: LaneStatus,
    scenario_id: &str,
    artifacts: &[PathBuf],
) -> std::io::Result<()> {
    // Foot-gun guard: the read_existing_summary parser does NOT invert
    // json_escape (see its NOTE), so any escapable/separator character in a
    // lane/scenario/artifact string would silently corrupt the read-modify-write
    // merge. Lane and scenario_id are fixed identifiers and artifact paths are
    // controlled `target/…` paths, so this never trips on real values — but a
    // future change that feeds an escapable value through here fails loudly in
    // debug instead of corrupting summary.json.
    let has_escapable = |s: &str| s.contains(['"', '\\', '\n', ',']);
    debug_assert!(
        !has_escapable(lane),
        "lane contains escapable/separator char (summary.json reader does not unescape): {lane:?}"
    );
    debug_assert!(
        !has_escapable(scenario_id),
        "scenario_id contains escapable/separator char (summary.json reader does not unescape): {scenario_id:?}"
    );
    debug_assert!(
        artifacts
            .iter()
            .all(|p| !has_escapable(&p.display().to_string())),
        "artifact path contains escapable/separator char (summary.json reader does not unescape): {artifacts:?}"
    );

    let _guard = SUMMARY_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = parity_artifact_dir();
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("summary.json");

    // Merge with whatever a sibling test-binary process already wrote.
    let mut rows = read_existing_summary(&path);
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
    rows.sort_by(|a, b| a.lane.cmp(&b.lane));

    std::fs::write(&path, render_summary_json(&rows))?;
    Ok(())
}

/// Recover existing lane rows from a previously written `summary.json` so that a
/// row written by a sibling test-binary process is preserved on merge. This is a
/// deliberately small line-oriented parser for the exact one-line-per-lane shape
/// [`render_summary_json`] emits; on any parse trouble it returns what it could
/// recover (the file is always rewritten cleanly afterward).
///
/// NOTE: this parser (and its [`extract_json_str`]/[`extract_json_array`]
/// helpers) does NOT invert [`json_escape`]. It only handles the controlled
/// identifier/path shape `render_summary_json` emits — lane and scenario_id are
/// fixed identifiers and artifact paths are controlled `target/…` paths, none of
/// which contain escapable characters — so round-tripping the escapes is
/// unnecessary.
fn read_existing_summary(path: &Path) -> Vec<SummaryRow> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for line in text.lines() {
        let line = line.trim().trim_end_matches(',');
        // Each lane row looks like: "lane": {"status": "..", "scenario_id": "..", "artifacts": [..]}
        let Some(rest) = line.strip_prefix('"') else {
            continue;
        };
        let Some(end_key) = rest.find('"') else {
            continue;
        };
        let lane = rest[..end_key].to_string();
        if lane.is_empty() {
            continue;
        }
        let status = match extract_json_str(line, "status").as_deref() {
            Some("pass") => LaneStatus::Pass,
            Some("fail") => LaneStatus::Fail,
            Some("skip") => LaneStatus::Skip,
            _ => continue,
        };
        let scenario_id = extract_json_str(line, "scenario_id").unwrap_or_default();
        rows.push(SummaryRow {
            lane,
            status,
            scenario_id,
            artifacts: extract_json_array(line, "artifacts"),
        });
    }
    rows
}

/// Extract the string value of `"key": "value"` from a single rendered line.
fn extract_json_str(line: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\":");
    let after = &line[line.find(&needle)? + needle.len()..];
    let start = after.find('"')? + 1;
    let end = after[start..].find('"')? + start;
    Some(after[start..end].to_string())
}

/// Extract the string elements of `"key": ["a", "b"]` from a single line.
fn extract_json_array(line: &str, key: &str) -> Vec<String> {
    let needle = format!("\"{key}\":");
    let Some(after_idx) = line.find(&needle) else {
        return Vec::new();
    };
    let after = &line[after_idx + needle.len()..];
    let Some(open) = after.find('[') else {
        return Vec::new();
    };
    let Some(close) = after[open..].find(']') else {
        return Vec::new();
    };
    after[open + 1..open + close]
        .split(',')
        .filter_map(|tok| {
            let t = tok.trim();
            let t = t.strip_prefix('"')?.strip_suffix('"')?;
            Some(t.to_string())
        })
        .filter(|s| !s.is_empty())
        .collect()
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

// NOTE: because this module is pulled into each strict-parity test binary via
// `#[path = "parity_support/mod.rs"]`, these unit tests are compiled and run once
// per including binary (redundant runs). That redundancy is the accepted cost of
// the shared `#[path]` module pattern; we keep the tests here rather than
// restructuring the support module into its own crate/binary.
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
