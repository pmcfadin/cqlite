//! Scenario-id-keyed failure-bundle emitter for the Rust `required_parity`
//! byte/offset/checksum/JSONL parity checks (issue #1027, Wave 2a, tasks 2.1–2.4).
//!
//! This is a **subdirectory module** (not a top-level integration test file), so
//! Cargo does NOT compile it as its own test binary. Include it from a parity
//! test via:
//!
//! ```ignore
//! #[path = "parity_bundle/mod.rs"]
//! mod parity_bundle;
//! ```
//!
//! # What it does
//!
//! On a real parity mismatch, a strict lane calls [`FailureBundle`] to write a
//! forensic bundle keyed by the manifest **scenario id**:
//!
//! ```text
//! <root>/parity-failures/<tier>/<scenario_id>/
//!   failure-artifact.json   # the Wave 1 record (crate `cassandra_parity`)
//!   stdout.txt / stderr.txt
//!   diffs/                  # per evidence_type (byte_for_byte vs canonical_semantic)
//!   repro/
//!     command.sh
//!     INSTRUCTIONS.md
//!     inputs/               # fixture path(s) + dataset SHA256 (NO dataset copy)
//! ```
//!
//! The record itself is produced by the Wave 1 model + emitter
//! ([`cassandra_parity::failure_artifact`]); this helper only assembles the
//! bundle *contents* (diff files, repro dir) and fills in the `diffs[]` / repro
//! pointers so every Rust required_parity surface emits the identical shape as
//! the Java harness and the live lanes.
//!
//! # Fail-closed (owner decision 2)
//!
//! The bundle is emitted ON FAILURE only; a passing scenario writes nothing (see
//! the `passing_scenario_writes_no_bundle` evidence test). The write path returns
//! a typed error instead of panicking (no `unwrap`/`expect`) so a missed emitter
//! never introduces flakiness — the caller surfaces the write error alongside the
//! parity failure.
//!
//! Not every consumer uses every builder method; each including binary compiles
//! this module independently.
#![allow(dead_code)]

use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use cassandra_parity::enums;
use cassandra_parity::failure_artifact::{Diff, EmitError, FailureArtifact, Provenance};
use sha2::{Digest as _, Sha256};

/// Reproduction context a lane supplies once, mirrored into the record's
/// `provenance` and the `repro/` directory.
#[derive(Clone, Debug)]
pub struct ReproContext {
    pub cassandra_version: String,
    pub cassandra_git_sha: String,
    /// On-disk fixture the failure involves. Its SHA-256 is recorded (paths +
    /// SHA only — NO dataset copy, owner decision 4).
    pub fixture_path: PathBuf,
    pub component_list: Vec<String>,
    /// The exact comparison command line (goes into `repro/command.sh`).
    pub command_line: String,
}

/// The schema-required 64-hex `dataset_sha256` sentinel used when the fixture is
/// missing/unreadable/a directory (issue #1027 finding 2). All-zero clearly reads
/// as "not recorded" while satisfying the schema's `^[0-9a-f]{64}$` pattern. Kept
/// identical to the corpus-audit lane's sentinel so both surfaces agree.
pub const DATASET_SHA_UNAVAILABLE: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

impl ReproContext {
    /// Compute the dataset SHA-256 of the fixture, falling back to the all-zero
    /// [`DATASET_SHA_UNAVAILABLE`] sentinel when the fixture cannot be read (it is
    /// missing, unreadable, or a directory). `std::fs::read` returns an error for
    /// a directory too, so this is fixture-tolerant: a required-parity failure
    /// without a resolvable fixture still yields a schema-conforming record rather
    /// than aborting the bundle (issue #1027 finding 2).
    fn dataset_sha256_or_sentinel(&self) -> String {
        match std::fs::read(&self.fixture_path) {
            Ok(bytes) => {
                let mut hasher = Sha256::new();
                hasher.update(&bytes);
                format!("{:x}", hasher.finalize())
            }
            Err(_) => DATASET_SHA_UNAVAILABLE.to_string(),
        }
    }
}

/// A per-scenario failure bundle under `parity-failures/<tier>/<scenario_id>/`.
///
/// Build with [`FailureBundle::new`], attach diff artifacts (byte/offset/
/// checksum/component-inventory for `byte_for_byte`, or normalized+raw JSONL for
/// `canonical_semantic`), then [`FailureBundle::emit`].
pub struct FailureBundle {
    root: PathBuf,
    scenario_id: String,
    lane: String,
    tier: String,
    evidence_type: String,
    artifacts_compared: Vec<String>,
    repro: ReproContext,
    stdout: String,
    stderr: String,
    /// (kind, relative-under-`diffs/` file name, body) tuples.
    diff_files: Vec<(String, String, String)>,
    /// Raw side files written directly under the bundle's `diffs/` dir
    /// (e.g. `reference.jsonl`, `candidate.jsonl`) that are NOT record `diffs[]`
    /// pointers but are required bundle contents.
    raw_files: Vec<(String, String)>,
}

/// The failure-bundle emitter's own error surface (never panics in the helper).
#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("writing bundle file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Emit(#[from] EmitError),
}

/// The result of a successful emission: the bundle dir and the record path.
pub struct EmittedBundle {
    pub bundle_dir: PathBuf,
    pub record_path: PathBuf,
}

impl FailureBundle {
    /// Start a bundle. `root` is the retention root (`parity-failures/` is created
    /// beneath it); `tier` must be a [`enums::CI_TIER`] value and `evidence_type`
    /// an [`enums::EVIDENCE_TYPE`] value.
    pub fn new(
        root: impl Into<PathBuf>,
        scenario_id: impl Into<String>,
        lane: impl Into<String>,
        tier: impl Into<String>,
        evidence_type: impl Into<String>,
        repro: ReproContext,
    ) -> Self {
        FailureBundle {
            root: root.into(),
            scenario_id: scenario_id.into(),
            lane: lane.into(),
            tier: tier.into(),
            evidence_type: evidence_type.into(),
            artifacts_compared: Vec::new(),
            repro,
            stdout: String::new(),
            stderr: String::new(),
            diff_files: Vec::new(),
            raw_files: Vec::new(),
        }
    }

    /// Record what was compared (`["bytes","offsets","checksums","component_files"]`
    /// for byte_for_byte, `["jsonl"]` for canonical_semantic).
    pub fn artifacts_compared<I, S>(mut self, items: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.artifacts_compared = items.into_iter().map(Into::into).collect();
        self
    }

    /// Captured stdout of the failing comparison.
    pub fn stdout(mut self, s: impl Into<String>) -> Self {
        self.stdout = s.into();
        self
    }

    /// Captured stderr of the failing comparison.
    pub fn stderr(mut self, s: impl Into<String>) -> Self {
        self.stderr = s.into();
        self
    }

    // ---- byte_for_byte diff artifacts (task 2.2) --------------------------

    /// Add all four `byte_for_byte` diff artifacts for a compared component:
    /// `<component>.byte-diff.txt`, `<component>.offset-diff.txt`, the shared
    /// `checksums.txt`, and `component_inventory.txt`. Each becomes a record
    /// `diffs[]` entry (kinds `byte_diff`, `offset_diff`, `checksum_diff`,
    /// `component_inventory`).
    pub fn byte_for_byte_component(
        mut self,
        component: &str,
        byte_diff_body: String,
        offset_diff_body: String,
        checksums_body: String,
        component_inventory_body: String,
    ) -> Self {
        self.diff_files.push((
            enums::FAILURE_ARTIFACT_KIND[0].to_string(), // byte_diff
            format!("{component}.byte-diff.txt"),
            byte_diff_body,
        ));
        self.diff_files.push((
            "offset_diff".to_string(),
            format!("{component}.offset-diff.txt"),
            offset_diff_body,
        ));
        self.diff_files.push((
            "checksum_diff".to_string(),
            "checksums.txt".to_string(),
            checksums_body,
        ));
        self.diff_files.push((
            "component_inventory".to_string(),
            "component_inventory.txt".to_string(),
            component_inventory_body,
        ));
        self
    }

    /// Add a SINGLE `byte_diff` diff artifact (`<component>.byte-diff.txt`).
    /// Used when a site has real raw bytes but only wants the byte diff kind.
    pub fn byte_diff_only(mut self, component: &str, body: String) -> Self {
        self.diff_files.push((
            "byte_diff".to_string(),
            format!("{component}.byte-diff.txt"),
            body,
        ));
        self
    }

    /// Add a SINGLE `offset_diff` diff artifact (`<component>.offset-diff.txt`).
    /// Used by the Index.db offset-delta site, which has real offset pairs but no
    /// raw bytes to checksum — so ONLY this kind is emitted (issue #1027 finding 1).
    pub fn offset_diff_only(mut self, component: &str, body: String) -> Self {
        self.diff_files.push((
            "offset_diff".to_string(),
            format!("{component}.offset-diff.txt"),
            body,
        ));
        self
    }

    /// Add a SINGLE `checksum_diff` diff artifact (`checksums.txt`). Used by the
    /// Statistics.db CRC32 site, which has real scalar checksum fields but no raw
    /// component bytes — so ONLY this kind is emitted (issue #1027 finding 1).
    pub fn checksum_diff_only(mut self, body: String) -> Self {
        self.diff_files.push((
            "checksum_diff".to_string(),
            "checksums.txt".to_string(),
            body,
        ));
        self
    }

    /// Add a plain bundle file that is NOT a record `diffs[]` pointer (e.g.
    /// `diagnostic.txt` for a site that only has a rendered diagnostic and cannot
    /// supply a typed diff kind — issue #1027 finding 1: never fabricate a kind).
    pub fn raw_bundle_file(mut self, file_name: &str, body: String) -> Self {
        self.raw_files.push((file_name.to_string(), body));
        self
    }

    // ---- canonical_semantic diff artifacts (task 2.3) ---------------------

    /// Add the `canonical_semantic` artifacts: the normalized `jsonl.diff`
    /// (a record `diffs[]` entry, kind `jsonl_diff`) plus the raw
    /// `reference.jsonl` and `candidate.jsonl` bundle files.
    pub fn jsonl(
        mut self,
        normalized_diff: String,
        reference_jsonl: String,
        candidate_jsonl: String,
    ) -> Self {
        self.diff_files.push((
            "jsonl_diff".to_string(),
            "jsonl.diff".to_string(),
            normalized_diff,
        ));
        self.raw_files
            .push(("reference.jsonl".to_string(), reference_jsonl));
        self.raw_files
            .push(("candidate.jsonl".to_string(), candidate_jsonl));
        self
    }

    /// Write the whole bundle to disk and return the paths. Fail-closed: the
    /// `failure-artifact.json` record is always written and, per the schema, its
    /// `diffs[]` and `repro_bundle` pointers resolve inside the bundle.
    pub fn emit(self) -> Result<EmittedBundle, BundleError> {
        let bundle_dir = self
            .root
            .join("parity-failures")
            .join(&self.tier)
            .join(&self.scenario_id);
        create_dir(&bundle_dir)?;

        // stdout / stderr pointers.
        write_file(&bundle_dir.join("stdout.txt"), &self.stdout)?;
        write_file(&bundle_dir.join("stderr.txt"), &self.stderr)?;

        // diffs/
        let diffs_dir = bundle_dir.join("diffs");
        create_dir(&diffs_dir)?;
        let mut diffs: Vec<Diff> = Vec::with_capacity(self.diff_files.len());
        for (kind, file_name, body) in &self.diff_files {
            write_file(&diffs_dir.join(file_name), body)?;
            diffs.push(Diff::new(kind.clone(), format!("diffs/{file_name}")));
        }
        for (file_name, body) in &self.raw_files {
            write_file(&diffs_dir.join(file_name), body)?;
        }

        // repro/. Fixture-tolerant (issue #1027 finding 2): a required-parity
        // failure whose fixture is missing/unreadable/a directory still emits a
        // conforming record — the dataset SHA-256 falls back to the schema-valid
        // all-zero sentinel instead of aborting the whole bundle.
        let dataset_sha = self.repro.dataset_sha256_or_sentinel();
        write_repro(&bundle_dir, &self.repro, &dataset_sha)?;

        // The Wave 1 record (single source of the shape).
        let record = FailureArtifact {
            schema_version: cassandra_parity::failure_artifact::SCHEMA_VERSION,
            scenario_id: self.scenario_id.clone(),
            lane: self.lane.clone(),
            tier: self.tier.clone(),
            evidence_type: self.evidence_type.clone(),
            artifacts_compared: self.artifacts_compared.clone(),
            provenance: Provenance {
                cassandra_version: self.repro.cassandra_version.clone(),
                cassandra_git_sha: self.repro.cassandra_git_sha.clone(),
                dataset_sha256: dataset_sha,
                fixture_path: self.repro.fixture_path.display().to_string(),
                component_list: self.repro.component_list.clone(),
                command_line: self.repro.command_line.clone(),
                stdout: "stdout.txt".to_string(),
                stderr: "stderr.txt".to_string(),
            },
            diffs,
            repro_bundle: "repro/".to_string(),
        };
        let record_path = record.write_to_bundle(&bundle_dir)?;

        Ok(EmittedBundle {
            bundle_dir,
            record_path,
        })
    }
}

/// The repro directory: `command.sh`, `INSTRUCTIONS.md`, and `inputs/` (a
/// manifest of the fixture path + dataset SHA256 — NOT a dataset copy).
fn write_repro(
    bundle_dir: &Path,
    repro: &ReproContext,
    dataset_sha: &str,
) -> Result<(), BundleError> {
    let repro_dir = bundle_dir.join("repro");
    let inputs_dir = repro_dir.join("inputs");
    create_dir(&inputs_dir)?;

    let mut command = String::from("#!/usr/bin/env bash\nset -euo pipefail\n");
    let _ = writeln!(
        command,
        "# Reproduce the failing parity comparison locally."
    );
    let _ = writeln!(command, "{}", repro.command_line);
    write_file(&repro_dir.join("command.sh"), &command)?;

    let mut instructions = String::new();
    let _ = writeln!(instructions, "# Reproducing this parity failure\n");
    let _ = writeln!(
        instructions,
        "1. Fetch the referenced fixture(s) (see `inputs/fixtures.txt`); this \
         bundle records paths + SHA256 only, not the dataset.\n"
    );
    let _ = writeln!(
        instructions,
        "2. Verify the fixture SHA-256 matches `inputs/fixtures.txt`.\n"
    );
    let _ = writeln!(
        instructions,
        "3. Run `bash repro/command.sh` (see the exact command below).\n"
    );
    let _ = writeln!(instructions, "```\n{}\n```", repro.command_line);
    write_file(&repro_dir.join("INSTRUCTIONS.md"), &instructions)?;

    // inputs/: fixture path + dataset SHA256 (no copy).
    let mut fixtures = String::new();
    let _ = writeln!(fixtures, "# fixture_path\tdataset_sha256");
    let _ = writeln!(
        fixtures,
        "{}\t{}",
        repro.fixture_path.display(),
        dataset_sha
    );
    write_file(&inputs_dir.join("fixtures.txt"), &fixtures)?;

    Ok(())
}

fn create_dir(path: &Path) -> Result<(), BundleError> {
    std::fs::create_dir_all(path).map_err(|e| BundleError::Io {
        path: path.to_path_buf(),
        source: e,
    })
}

fn write_file(path: &Path, body: &str) -> Result<(), BundleError> {
    let mut f = std::fs::File::create(path).map_err(|e| BundleError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    f.write_all(body.as_bytes()).map_err(|e| BundleError::Io {
        path: path.to_path_buf(),
        source: e,
    })
}

// ----------------------------------------------------------------------------
// Bundle-body formatters (thin wrappers producing the diff/inventory bodies).
// These exist so a lane produces the exact per-component file bodies the design
// (design.md §"Per evidence_type, diffs/ MUST contain") calls for.
// ----------------------------------------------------------------------------

/// `<component>.byte-diff.txt`: first differing byte offset + a hex window.
pub fn byte_diff_body(component: &str, expected: &[u8], actual: &[u8]) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "byte diff [{component}]: expected ({} B) vs actual ({} B)",
        expected.len(),
        actual.len()
    );
    let max = expected.len().min(actual.len());
    let first = (0..max).find(|&i| expected[i] != actual[i]).unwrap_or(max);
    let _ = writeln!(s, "first difference at byte offset {first}");
    let lo = first.saturating_sub(8);
    let _ = writeln!(s, "  expected[{lo}..]: {}", hex_window(expected, lo, 16));
    let _ = writeln!(s, "  actual  [{lo}..]: {}", hex_window(actual, lo, 16));
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

/// `<component>.offset-diff.txt`: the first divergent offset + the byte lengths.
pub fn offset_diff_body(component: &str, expected: &[u8], actual: &[u8]) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "offset diff [{component}]");
    let max = expected.len().min(actual.len());
    let first = (0..max).find(|&i| expected[i] != actual[i]);
    match first {
        Some(off) => {
            let _ = writeln!(s, "  first divergent offset: {off}");
        }
        None => {
            let _ = writeln!(
                s,
                "  no in-range divergence; length differs: expected={} actual={}",
                expected.len(),
                actual.len()
            );
        }
    }
    let _ = writeln!(
        s,
        "  lengths: expected={} actual={}",
        expected.len(),
        actual.len()
    );
    s
}

/// `checksums.txt`: SHA-256 per component for both engines.
pub fn checksums_body(component: &str, expected: &[u8], actual: &[u8]) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "checksums (SHA-256): {component}");
    let _ = writeln!(s, "  cassandra: {}", sha256_hex(expected));
    let _ = writeln!(s, "  cqlite   : {}", sha256_hex(actual));
    s
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

/// `component_inventory.txt`: expected vs actual component set.
pub fn component_inventory_body(expected: &[String], actual: &[String]) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "component inventory (expected vs actual):");
    let _ = writeln!(s, "  expected: {}", expected.join(", "));
    let _ = writeln!(s, "  actual  : {}", actual.join(", "));
    let missing: Vec<&String> = expected.iter().filter(|c| !actual.contains(c)).collect();
    let extra: Vec<&String> = actual.iter().filter(|c| !expected.contains(c)).collect();
    if !missing.is_empty() {
        let _ = writeln!(
            s,
            "  missing from actual: {}",
            missing
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if !extra.is_empty() {
        let _ = writeln!(
            s,
            "  extra in actual: {}",
            extra
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    s
}

/// `jsonl.diff`: the first line whose normalized form differs, plus both lines.
pub fn jsonl_diff_body(expected: &[String], actual: &[String]) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "normalized-JSONL diff (expected {} line(s), actual {} line(s))",
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
