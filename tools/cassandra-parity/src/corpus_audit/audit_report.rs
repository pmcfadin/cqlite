//! Emit a conforming parity failure-artifact for the `exhaustive_regeneration`
//! lane when the corpus audit fails (issue #1027).
//!
//! The exhaustive-regeneration lane is the ONLY parity surface whose failure is
//! an *audit* over the regenerated corpus rather than a byte/offset/JSONL diff of
//! one fixture, so its failure-artifact's `diffs[]` points at the corpus-audit
//! REPORT (`kind = audit_report`) the lane already produces — never a duplicated
//! diff. This module owns the small glue that turns a failed [`AuditReport`] plus
//! the run's provenance into a [`FailureArtifact`] and writes the shared
//! `parity-failures/exhaustive_regeneration/<scenario_id>/` bundle via the Wave 1
//! [`crate::failure_artifact::write_to_bundle`] emitter (so the record shape is
//! identical to every other lane). Keeping it here — not inline in `main.rs` —
//! holds `main.rs` under the file-size ratchet and keeps the emit logic beside
//! the audit it reports on.

use std::path::{Path, PathBuf};

use super::provenance::Provenance;
use super::AuditReport;
use crate::failure_artifact::{
    Diff, EmitError, FailureArtifact, Provenance as ArtifactProvenance, SCHEMA_VERSION,
};
use crate::model::Manifest;

/// The emitting workflow file, recorded as the record's `lane`.
pub const LANE: &str = "exhaustive-regeneration.yml";

/// The manifest `exhaustive_regeneration` scenario the audit-level bundle is
/// keyed by. The corpus audit is a whole-corpus check (missing references,
/// unclassified files, component/provenance/corruption findings), not a
/// per-fixture diff, so there is no single failing scenario to key on; we key the
/// one audit-level bundle by the most representative REAL manifest
/// `exhaustive_regeneration` scenario id — `cass.delta_scan.wide_partition_corpus`
/// — so `scenario_id` still joins back to the manifest (never an invented id) and
/// matches the schema's `^cass\.…` pattern. Any of the lane's scenarios would
/// serve; this one is chosen because it names the regenerated corpus itself. This
/// id is asserted to exist in the manifest by
/// `tests/corpus_audit_failure_artifact_tests.rs`.
pub const AUDIT_SCENARIO_ID: &str = "cass.delta_scan.wide_partition_corpus";

/// Sentinel used for `provenance.dataset_sha256` when the run's provenance record
/// carries no valid 64-hex asset SHA (the field is schema-required as 64 hex, so
/// an unavailable value cannot be left empty). All-zero clearly reads as "not
/// recorded" while keeping the record schema-conforming.
const DATASET_SHA_UNAVAILABLE: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

/// Bundle-relative pointer to the corpus-audit report text, recorded as the
/// `audit_report` diff's `path`. The report the lane already produces is copied
/// into the bundle at this path (reused verbatim, never a re-run of the audit) so
/// the bundle is self-contained.
pub const AUDIT_REPORT_DIFF_PATH: &str = "diffs/corpus-audit-report.txt";

/// Bundle-relative pointer to the captured stdout, recorded as `provenance.stdout`.
/// The corpus audit has no separate console stream to capture, so the audit report
/// text is mirrored here (a real, resolvable file) rather than pointing at a path
/// that is never written (issue #1027 finding 2).
pub const STDOUT_FILE: &str = "stdout.txt";

/// Bundle-relative pointer to the captured stderr, recorded as `provenance.stderr`.
/// Same file body as [`STDOUT_FILE`]; both resolve inside the bundle.
pub const STDERR_FILE: &str = "stderr.txt";

/// Bundle-relative pointer to the reproduction directory, recorded as
/// `repro_bundle`. [`write_audit_bundle`] materializes this directory with a
/// `command.sh` and `INSTRUCTIONS.md`, consistent with the cqlite-core repro
/// bundle, so the pointer always resolves (issue #1027 finding 2).
pub const REPRO_DIR: &str = "repro/";

/// Build the failure-artifact record for a FAILED corpus audit. Callers only
/// invoke this on failure. The `diffs[]` carries the single `audit_report` entry
/// pointing at the lane's existing corpus-audit report text (reused, not
/// duplicated); the report body itself is written to the bundle by
/// [`write_audit_bundle`].
pub fn build_record(
    manifest: &Manifest,
    provenance: Option<&Provenance>,
    command_line: &str,
) -> FailureArtifact {
    let src = &manifest.cassandra_source;
    let cassandra_version = src
        .git_ref
        .strip_prefix("cassandra-")
        .unwrap_or(&src.git_ref)
        .to_string();
    let dataset_sha256 = provenance
        .map(|p| p.dataset_asset_sha256.as_str())
        .filter(|s| is_sha256(s))
        .unwrap_or(DATASET_SHA_UNAVAILABLE)
        .to_string();

    FailureArtifact {
        schema_version: SCHEMA_VERSION,
        scenario_id: AUDIT_SCENARIO_ID.to_string(),
        lane: LANE.to_string(),
        tier: "exhaustive_regeneration".to_string(),
        // The regeneration corpus is audited, not diffed byte-for-byte; the
        // audit is a canonical-semantic check over the regenerated inventory.
        evidence_type: "canonical_semantic".to_string(),
        artifacts_compared: vec![
            "component_inventory".to_string(),
            "checksums".to_string(),
            "provenance".to_string(),
        ],
        provenance: ArtifactProvenance {
            cassandra_version,
            cassandra_git_sha: src.sha.clone(),
            dataset_sha256,
            // The audit runs over the whole regenerated corpus tree, keyed via
            // the manifest; there is no single fixture path.
            fixture_path: "test-data/datasets (regenerated corpus)".to_string(),
            component_list: Vec::new(),
            command_line: command_line.to_string(),
            stdout: STDOUT_FILE.to_string(),
            stderr: STDERR_FILE.to_string(),
        },
        diffs: vec![Diff::new("audit_report", AUDIT_REPORT_DIFF_PATH)],
        repro_bundle: REPRO_DIR.to_string(),
    }
}

/// Write the exhaustive-regeneration audit failure bundle under
/// `<parity_failures_root>/exhaustive_regeneration/<scenario_id>/`, returning the
/// path to the written `failure-artifact.json`. The `audit_report_text` is the
/// corpus-audit report the lane already produced (`AuditReport::render()` output);
/// it is copied verbatim into the bundle at [`AUDIT_REPORT_DIFF_PATH`] so the
/// record's `audit_report` diff resolves and the bundle stands alone.
///
/// Every record pointer resolves to a real bundle file/dir (issue #1027
/// finding 2): the audit report at [`AUDIT_REPORT_DIFF_PATH`], the mirrored
/// `provenance.stdout`/`provenance.stderr` at [`STDOUT_FILE`]/[`STDERR_FILE`], and
/// the `repro_bundle` [`REPRO_DIR`] materialized with `command.sh` +
/// `INSTRUCTIONS.md` (consistent with the cqlite-core repro bundle).
pub fn write_audit_bundle(
    parity_failures_root: &Path,
    manifest: &Manifest,
    report: &AuditReport,
    provenance: Option<&Provenance>,
    audit_report_text: &str,
    command_line: &str,
) -> Result<PathBuf, EmitError> {
    let record = build_record(manifest, provenance, command_line);
    let bundle = parity_failures_root
        .join("exhaustive_regeneration")
        .join(&record.scenario_id);

    // Issue #1027 finding 2: write EVERY file the record points at BEFORE the record
    // itself, so `failure-artifact.json` exists only once all its pointers resolve.
    // The lane's fail-closed guard checks solely for the record file — writing it
    // last means a partial write can never leave the record with dangling pointers.
    create_dir(&bundle)?;

    // diffs/corpus-audit-report.txt — the reused audit report.
    let report_path = bundle.join(AUDIT_REPORT_DIFF_PATH);
    if let Some(parent) = report_path.parent() {
        create_dir(parent)?;
    }
    write_bundle_file(&report_path, audit_report_text)?;

    // provenance.stdout / provenance.stderr — the audit has no separate console
    // stream, so mirror the report text into both (resolvable files, not dangling
    // pointers).
    write_bundle_file(&bundle.join(STDOUT_FILE), audit_report_text)?;
    write_bundle_file(&bundle.join(STDERR_FILE), audit_report_text)?;

    // repro/ — command.sh + INSTRUCTIONS.md (consistent with the cqlite-core repro
    // bundle) so the repro_bundle pointer resolves to a real directory.
    write_repro(&bundle.join(REPRO_DIR), command_line, report.findings.len())?;

    // failure-artifact.json LAST — only now does the record (which the guard keys
    // on) come into existence, and every file/dir it references is already present.
    let written = record.write_to_bundle(&bundle)?;

    Ok(written)
}

/// Materialize the `repro/` directory: a `command.sh` that re-runs the audit and
/// an `INSTRUCTIONS.md` explaining how to reproduce, mirroring the cqlite-core
/// repro bundle shape.
fn write_repro(
    repro_dir: &Path,
    command_line: &str,
    finding_count: usize,
) -> Result<(), EmitError> {
    create_dir(repro_dir)?;
    let command = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\n# Reproduce the failing corpus audit locally.\n{command_line}\n"
    );
    write_bundle_file(&repro_dir.join("command.sh"), &command)?;
    let instructions = format!(
        "# Reproducing this corpus-audit failure\n\n\
         The exhaustive-regeneration corpus audit failed with {finding_count} finding(s); \
         see `../diffs/corpus-audit-report.txt` for the full report.\n\n\
         1. From the repo root, run `bash repro/command.sh` (the exact invocation is below).\n\
         2. Inspect the reported findings and compare against the regenerated corpus.\n\n\
         ```\n{command_line}\n```\n"
    );
    write_bundle_file(&repro_dir.join("INSTRUCTIONS.md"), &instructions)?;
    Ok(())
}

/// Create a directory (and parents), mapping the IO error to [`EmitError`].
fn create_dir(path: &Path) -> Result<(), EmitError> {
    std::fs::create_dir_all(path).map_err(|e| EmitError::Io {
        path: path.to_path_buf(),
        source: e,
    })
}

/// Write a bundle file, mapping the IO error to [`EmitError`].
fn write_bundle_file(path: &Path, body: &str) -> Result<(), EmitError> {
    std::fs::write(path, body).map_err(|e| EmitError::Io {
        path: path.to_path_buf(),
        source: e,
    })
}

/// CLI-facing orchestration for a FAILED corpus audit: write the shared
/// failure-artifact bundle under `parity-failures/` (relative to CWD — the lane
/// runs from the repo root) and log the outcome to stderr. Keeps `main.rs`'s
/// failure branch to one call (file-size ratchet). `command_line` is the full
/// invocation captured by the caller. A write error is surfaced to stderr; the
/// caller still returns the non-zero exit (fail-closed: the lane's guard step
/// also asserts the record exists).
pub fn emit_on_failure(
    manifest: &Manifest,
    report: &AuditReport,
    provenance: Option<&Provenance>,
    command_line: &str,
) {
    let rendered = report.render();
    let bundle_root = Path::new("parity-failures");
    match write_audit_bundle(
        bundle_root,
        manifest,
        report,
        provenance,
        &rendered,
        command_line,
    ) {
        Ok(path) => eprintln!(
            "corpus-audit: wrote failure-artifact bundle {}",
            path.display()
        ),
        Err(e) => eprintln!("corpus-audit: ERROR writing failure-artifact bundle: {e}"),
    }
}

/// True when `s` is exactly 64 lowercase-hex characters (a SHA-256), matching the
/// schema's `dataset_sha256` pattern.
fn is_sha256(s: &str) -> bool {
    s.len() == 64
        && s.chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_sha256_accepts_64_hex_only() {
        assert!(is_sha256(&"a".repeat(64)));
        assert!(is_sha256(&"0123456789abcdef".repeat(4)));
        assert!(!is_sha256("deadbeef"));
        assert!(!is_sha256(&"A".repeat(64))); // uppercase not allowed by pattern
        assert!(!is_sha256(&"g".repeat(64))); // non-hex
        assert!(!is_sha256(""));
    }
}
