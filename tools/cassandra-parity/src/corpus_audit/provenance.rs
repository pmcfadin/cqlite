//! Per-run dataset provenance record + its comparison against the manifest's
//! declared Cassandra pin (design D4 / spec "Each regeneration run records
//! dataset provenance").
//!
//! The regeneration lane writes this record (as JSON) into its report artifact;
//! it is NOT committed to any tracked repo path (owner decision: artifact-only).
//! The audit reads it back and fails if the corpus was produced from a Cassandra
//! version/ref/sha the manifest does not declare — catching a silent image bump.

use std::collections::BTreeSet;

use serde::Deserialize;

use crate::model::Manifest;

use super::{AuditFinding, FindingKind};

/// The structured provenance of one regeneration run.
#[derive(Debug, Clone, Deserialize)]
pub struct Provenance {
    /// Cassandra version string, e.g. `5.0.2`.
    pub cassandra_version: String,
    /// Cassandra source ref, e.g. `cassandra-5.0.2`.
    pub cassandra_ref: String,
    /// Cassandra source git sha.
    pub cassandra_git_sha: String,
    /// Docker image tag used to generate, e.g. `cassandra:5.0.2`.
    pub docker_image: String,
    /// The exact generator commands invoked, in order.
    #[serde(default)]
    pub generator_commands: Vec<String>,
    /// The `package_datasets.sh` asset name.
    pub dataset_asset_name: String,
    /// SHA256 of the produced dataset asset.
    pub dataset_asset_sha256: String,
}

impl Provenance {
    /// Parse a provenance record from its JSON document.
    pub fn from_json(text: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(text)
    }
}

/// Fail when the recorded provenance was produced from a Cassandra
/// version/ref/sha the manifest does not declare.
///
/// - `cassandra_ref` MUST equal `cassandra_source.ref`.
/// - `cassandra_git_sha` MUST be `cassandra_source.sha` or appear in some
///   `evidence.cassandra_git_sha`.
/// - `cassandra_version` MUST be the version implied by `cassandra_source.ref`
///   (e.g. `cassandra-5.0.2` → `5.0.2`) or appear in some
///   `evidence.cassandra_version`.
pub fn check_provenance(prov: &Provenance, manifest: &Manifest) -> Vec<AuditFinding> {
    let mut out = Vec::new();
    let src = &manifest.cassandra_source;

    if prov.cassandra_ref != src.git_ref {
        out.push(AuditFinding::new(
            FindingKind::ProvenanceMismatch,
            prov.cassandra_ref.clone(),
            format!(
                "recorded cassandra_ref does not match manifest cassandra_source.ref ({})",
                src.git_ref
            ),
        ));
    }

    let mut declared_shas: BTreeSet<&str> = BTreeSet::new();
    declared_shas.insert(src.sha.as_str());
    let mut declared_versions: BTreeSet<String> = BTreeSet::new();
    if let Some(v) = version_from_ref(&src.git_ref) {
        declared_versions.insert(v);
    }
    for s in &manifest.scenarios {
        if let Some(sha) = &s.evidence.cassandra_git_sha {
            declared_shas.insert(sha.as_str());
        }
        if let Some(v) = &s.evidence.cassandra_version {
            declared_versions.insert(v.clone());
        }
    }

    if !declared_shas.contains(prov.cassandra_git_sha.as_str()) {
        out.push(AuditFinding::new(
            FindingKind::ProvenanceMismatch,
            prov.cassandra_git_sha.clone(),
            "recorded cassandra_git_sha is not declared by cassandra_source.sha or any \
             evidence.cassandra_git_sha"
                .to_string(),
        ));
    }

    if !declared_versions.contains(&prov.cassandra_version) {
        out.push(AuditFinding::new(
            FindingKind::ProvenanceMismatch,
            prov.cassandra_version.clone(),
            "recorded cassandra_version is not declared by cassandra_source.ref or any \
             evidence.cassandra_version"
                .to_string(),
        ));
    }

    out
}

/// Derive a version (`5.0.2`) from a `cassandra-<version>` ref; `None` otherwise.
fn version_from_ref(git_ref: &str) -> Option<String> {
    git_ref.strip_prefix("cassandra-").map(str::to_string)
}
