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
/// - `docker_image` MUST carry a tag whose leading `MAJOR[.MINOR[.PATCH…]]`
///   version is one of the manifest's declared versions. This field is the ONE
///   provenance value sourced independently of the manifest in the lane (it is
///   grepped from `regenerate-datasets.sh`'s `CASSANDRA_IMAGE=`, while
///   version/ref/sha are parsed from the manifest itself), so it is the only
///   field that can catch a silent image bump (e.g. `cassandra:5.0.2` → `5.0.3`)
///   that was not mirrored into the manifest pin (issue #1026). A legitimately
///   pinned variant image (`cassandra:5.0.2-jdk11`, `cassandra:5.0.2-jammy`)
///   compares by its numeric lead `5.0.2`, so the build/variant tail never reds
///   the lane. A tag with no numeric lead (e.g. `latest`, empty) is itself a
///   mismatch: an unverifiable image cannot be trusted against the pin.
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

    // The independently-sourced docker_image is the only field that can catch a
    // silent image bump: version/ref/sha are all parsed from the manifest in the
    // lane, so validating them against the manifest is tautological, whereas the
    // image tag is grepped from regenerate-datasets.sh's CASSANDRA_IMAGE=.
    match version_from_image(&prov.docker_image) {
        Ok(img_version) if declared_versions.contains(&img_version) => {}
        Ok(img_version) => out.push(AuditFinding::new(
            FindingKind::ProvenanceMismatch,
            prov.docker_image.clone(),
            format!(
                "docker_image tag version {img_version} is not declared by cassandra_source.ref \
                 or any evidence.cassandra_version (silent image bump not mirrored into the \
                 manifest pin?)"
            ),
        )),
        Err(reason) => out.push(AuditFinding::new(
            FindingKind::ProvenanceMismatch,
            prov.docker_image.clone(),
            format!("docker_image cannot be validated against the manifest pin: {reason}"),
        )),
    }

    out
}

/// Derive a version (`5.0.2`) from a `cassandra-<version>` ref; `None` otherwise.
fn version_from_ref(git_ref: &str) -> Option<String> {
    git_ref.strip_prefix("cassandra-").map(str::to_string)
}

/// Extract the semver version a Docker image reference points at, so it can be
/// checked against the manifest's declared Cassandra version(s).
///
/// Handles a registry/namespace prefix and an optional `@sha256:<digest>` suffix:
/// `docker.io/library/cassandra:5.0.3@sha256:abc…` → `5.0.3`. The tag is the
/// portion after the last `:` of the final path segment (so a registry-host port
/// like `registry:5000/cassandra:5.0.3` is not mistaken for a tag). The returned
/// version is the tag's leading `MAJOR[.MINOR[.PATCH…]]` numeric component, with
/// an optional leading `v` and a `-<suffix>` build/variant tail ignored, so a
/// pinned variant image (`cassandra:5.0.2-jdk11`) compares as `5.0.2`. Returns
/// `Err(reason)` when there is no tag or the tag has no numeric lead (e.g.
/// `latest`) — an unverifiable image must be treated as a mismatch, never
/// silently trusted.
fn version_from_image(image: &str) -> Result<String, String> {
    // Drop an `@sha256:…` content digest if present.
    let no_digest = image.split('@').next().unwrap_or(image);
    // The repository[:tag] is the final `/`-separated segment; this discards any
    // registry host (which is the only place a `:`-port could otherwise appear).
    let last_seg = no_digest.rsplit('/').next().unwrap_or(no_digest);
    let tag = match last_seg.split_once(':') {
        Some((_, t)) if !t.is_empty() => t,
        _ => {
            return Err(format!(
                "image reference {image} has no tag to validate (implicit `latest` is not pinned)"
            ))
        }
    };
    semver_lead(tag).ok_or_else(|| {
        format!("image tag `{tag}` has no parseable MAJOR[.MINOR[.PATCH]] semver lead")
    })
}

/// Parse the leading `MAJOR[.MINOR[.PATCH…]]` numeric version from a Docker tag,
/// ignoring an optional leading `v` and a trailing `-<suffix>` build/variant tail.
/// So `5`, `5.0`, `5.0.2`, `v5.0.2`, `5.0.2-jdk11`, and `5.0.2-jammy` all yield
/// `5.0.2`-style numeric leads (the last three → `5.0.2`), while `latest`, ``,
/// `5.`, and `-jdk11` have no numeric lead and yield `None` — a genuinely
/// unverifiable tag stays a mismatch. The numeric lead must be a non-empty run of
/// digits and dots with at least one digit and no leading/trailing or doubled dot.
fn semver_lead(tag: &str) -> Option<String> {
    let t = tag.strip_prefix('v').unwrap_or(tag);
    // Ignore a build/variant suffix (`-jdk11`, `-jammy`, `-alpine`, …).
    let lead = t.split('-').next().unwrap_or(t);
    if is_dotted_numeric(lead) {
        Some(lead.to_string())
    } else {
        None
    }
}

/// True when `s` is a non-empty run of digits and dots with at least one digit
/// and no leading/trailing or doubled dot (so `5`, `5.0`, `5.0.2` qualify; ``,
/// `5.`, `.5`, `5..0`, `latest` do not).
fn is_dotted_numeric(s: &str) -> bool {
    !s.is_empty()
        && !s.starts_with('.')
        && !s.ends_with('.')
        && !s.contains("..")
        && s.chars().all(|c| c.is_ascii_digit() || c == '.')
        && s.chars().any(|c| c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_from_image_parses_bare_tag() {
        assert_eq!(
            version_from_image("cassandra:5.0.2").as_deref(),
            Ok("5.0.2")
        );
    }

    #[test]
    fn version_from_image_strips_registry_prefix_and_digest() {
        assert_eq!(
            version_from_image("docker.io/library/cassandra:5.0.3@sha256:abc123").as_deref(),
            Ok("5.0.3")
        );
    }

    #[test]
    fn version_from_image_ignores_registry_port() {
        // The host port (`:5000`) is before the last `/`, so it is not the tag.
        assert_eq!(
            version_from_image("registry.local:5000/cassandra:5.0.2").as_deref(),
            Ok("5.0.2")
        );
    }

    #[test]
    fn version_from_image_strips_leading_v() {
        assert_eq!(
            version_from_image("cassandra:v5.0.2").as_deref(),
            Ok("5.0.2")
        );
    }

    #[test]
    fn version_from_image_ignores_variant_build_suffix() {
        // A legitimately pinned variant image compares by its numeric lead, so a
        // `-jdk11` / `-jammy` build tail must not be mistaken for a non-semver tag.
        assert_eq!(
            version_from_image("cassandra:5.0.2-jdk11").as_deref(),
            Ok("5.0.2")
        );
        assert_eq!(
            version_from_image("cassandra:5.0.2-jammy").as_deref(),
            Ok("5.0.2")
        );
    }

    #[test]
    fn version_from_image_rejects_latest() {
        assert!(version_from_image("cassandra:latest").is_err());
    }

    #[test]
    fn version_from_image_rejects_missing_tag() {
        assert!(version_from_image("cassandra").is_err());
    }

    #[test]
    fn semver_lead_edge_cases() {
        assert_eq!(semver_lead("5").as_deref(), Some("5"));
        assert_eq!(semver_lead("5.0").as_deref(), Some("5.0"));
        assert_eq!(semver_lead("5.0.2").as_deref(), Some("5.0.2"));
        assert_eq!(semver_lead("v5.0.2").as_deref(), Some("5.0.2"));
        // Build/variant suffix is ignored, leaving the numeric lead.
        assert_eq!(semver_lead("5.0.2-jdk11").as_deref(), Some("5.0.2"));
        assert_eq!(semver_lead("5.0.2-jammy").as_deref(), Some("5.0.2"));
        assert_eq!(semver_lead("v5.0.2-alpine").as_deref(), Some("5.0.2"));
        // No numeric lead -> unverifiable -> None.
        assert!(semver_lead("").is_none());
        assert!(semver_lead("latest").is_none());
        assert!(semver_lead("-jdk11").is_none());
        assert!(semver_lead("5.").is_none());
        assert!(semver_lead(".5").is_none());
        assert!(semver_lead("5..0").is_none());
    }
}
