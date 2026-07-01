//! The uniform parity failure-artifact record (issue #1027).
//!
//! Every strict and live parity surface (the Rust `required_parity` byte/offset/
//! checksum/JSONL checks, the Java compaction harness, the nightly Docker lane,
//! the exhaustive-regeneration audit) emits ONE `failure-artifact.json` per
//! failed scenario so a red gate maps mechanically back to its `cass.*` scenario.
//! This module owns the serde model + the emitter that writes the record, so all
//! surfaces produce the identical shape. The record schema lives at
//! `test-data/parity-failure-artifact.schema.json`; the round-trip test in
//! `tests/failure_artifact_tests.rs` emits a record and validates it against that
//! schema.
//!
//! The `tier` and `evidence_type` values are the SAME closed sets as the manifest
//! ([`crate::enums::CI_TIER`] / [`crate::enums::EVIDENCE_TYPE`]); the
//! `diffs[].kind` values are [`crate::enums::FAILURE_ARTIFACT_KIND`].

use std::path::Path;

use serde::{Deserialize, Serialize};

/// The current record schema version. Bump in lockstep with the `schema_version`
/// `const` in `parity-failure-artifact.schema.json`.
pub const SCHEMA_VERSION: u32 = 1;

/// The canonical file name for a written record.
pub const RECORD_FILE_NAME: &str = "failure-artifact.json";

/// A single `diffs[]` pointer into the bundle, typed by what was compared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diff {
    /// One of [`crate::enums::FAILURE_ARTIFACT_KIND`].
    pub kind: String,
    /// Pointer relative to the bundle dir, e.g. `diffs/Data.db.byte-diff.txt`.
    pub path: String,
}

impl Diff {
    /// Construct a diff pointer.
    pub fn new(kind: impl Into<String>, path: impl Into<String>) -> Self {
        Diff {
            kind: kind.into(),
            path: path.into(),
        }
    }
}

/// The full reproduction context captured on failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    pub cassandra_version: String,
    pub cassandra_git_sha: String,
    /// SHA-256 of the dataset asset the fixture came from (paths + SHA only; no
    /// dataset copy — owner decision 4).
    pub dataset_sha256: String,
    pub fixture_path: String,
    pub component_list: Vec<String>,
    pub command_line: String,
    /// Pointer relative to the bundle dir, e.g. `stdout.txt`.
    pub stdout: String,
    /// Pointer relative to the bundle dir, e.g. `stderr.txt`.
    pub stderr: String,
}

/// The uniform failure-artifact record. Serializes to `failure-artifact.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureArtifact {
    pub schema_version: u32,
    pub scenario_id: String,
    /// The emitting workflow file (e.g. `sstabledump-parity-gate.yml`).
    pub lane: String,
    /// The manifest `ci.tier` (closed enum).
    pub tier: String,
    /// The manifest `evidence.type` (closed enum).
    pub evidence_type: String,
    pub artifacts_compared: Vec<String>,
    pub provenance: Provenance,
    pub diffs: Vec<Diff>,
    /// Pointer to the `repro/` directory.
    pub repro_bundle: String,
}

impl FailureArtifact {
    /// Serialize the record to pretty JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Parse a record from JSON text.
    pub fn from_json(text: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(text)
    }

    /// Write the record as `failure-artifact.json` inside `bundle_dir`, creating
    /// the directory if needed. Returns the path written. Library code returns a
    /// typed error rather than panicking so a missed emitter never introduces
    /// flakiness (owner decision 2).
    pub fn write_to_bundle(&self, bundle_dir: &Path) -> Result<std::path::PathBuf, EmitError> {
        std::fs::create_dir_all(bundle_dir).map_err(|e| EmitError::Io {
            path: bundle_dir.to_path_buf(),
            source: e,
        })?;
        let record_path = bundle_dir.join(RECORD_FILE_NAME);
        let json = self.to_json().map_err(EmitError::Serialize)?;
        std::fs::write(&record_path, json).map_err(|e| EmitError::Io {
            path: record_path.clone(),
            source: e,
        })?;
        Ok(record_path)
    }
}

/// An error emitting a failure-artifact record.
#[derive(Debug, thiserror::Error)]
pub enum EmitError {
    #[error("serializing failure-artifact record: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("writing failure-artifact record to {path}: {source}")]
    Io {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
}
