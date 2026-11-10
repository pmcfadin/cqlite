//! Per-run dataset provenance record + its comparison against the manifest's
//! declared Cassandra pin (design D4 / spec "Each regeneration run records
//! dataset provenance").
//!
//! The regeneration lane writes this record (as JSON) into its report artifact;
//! it is NOT committed to any tracked repo path (owner decision: artifact-only).
//! The audit reads it back and fails if the corpus was produced from a Cassandra
//! version/ref/sha the manifest does not declare — catching a silent image bump.

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

/// Fail when the recorded provenance was produced from a Cassandra release the
/// manifest does not declare.
///
/// The recorded `(cassandra_version, cassandra_ref, cassandra_git_sha)` is validated as
/// ONE TUPLE (roborev job 174): it must equal the primary `cassandra_source` pin or one
/// `cassandra_source.additional_pins` entry in every component. Checking the three as
/// independent allowlists would accept a mixed record such as the 5.0.8 ref and version
/// with the 5.0.2 sha. `docker_image` is held to the SAME tuple: its numeric tag lead
/// (a `-jdk11` style variant tail is ignored) must equal the matched pin's version.
/// That field is the ONE provenance value sourced independently of the manifest in the
/// lane (the lane sources it from `test-data/scripts/lib/cassandra-image.sh`, while
/// version/ref/sha are parsed from the manifest itself), so it is the only field that
/// can catch a silent image bump (e.g. `cassandra:5.0.2` → `5.0.3`) not mirrored into
/// the manifest pin (issue #1026). A tag with no numeric lead (`latest`, empty) cannot
/// be verified and is itself a mismatch.
///
/// Scenario `evidence.cassandra_*` values no longer widen this check: the linter requires
/// every scenario's version and sha to be one declared pin's (see `lint_versions.rs`),
/// so the pins are the single source of declared releases.
pub fn check_provenance(prov: &Provenance, manifest: &Manifest) -> Vec<AuditFinding> {
    let mut out = Vec::new();
    let pins = manifest.cassandra_source.pins();
    let finding = |subject: &str, detail: String| {
        AuditFinding::new(FindingKind::ProvenanceMismatch, subject.to_string(), detail)
    };

    let img_version = version_from_image(&prov.docker_image);

    match pins.iter().find(|(_, r, _)| *r == prov.cassandra_ref) {
        Some((pin_version, pin_ref, pin_sha)) => {
            if prov.cassandra_version != *pin_version {
                out.push(finding(
                    &prov.cassandra_version,
                    format!(
                        "recorded cassandra_version does not match the version of the declared pin \
                         {pin_ref} ({pin_version}); ref, sha and version must come from ONE pin"
                    ),
                ));
            }
            if prov.cassandra_git_sha != *pin_sha {
                out.push(finding(
                    &prov.cassandra_git_sha,
                    format!(
                        "recorded cassandra_git_sha does not match the sha of the declared pin \
                         {pin_ref} ({pin_sha}); ref, sha and version must come from ONE pin"
                    ),
                ));
            }
            match &img_version {
                Ok(v) if v == pin_version => {}
                Ok(v) => out.push(finding(
                    &prov.docker_image,
                    format!(
                        "docker_image tag version {v} is not the version of the declared pin \
                         {pin_ref} ({pin_version}) (silent image bump not mirrored into the \
                         manifest pin?)"
                    ),
                )),
                Err(reason) => out.push(finding(
                    &prov.docker_image,
                    format!("docker_image cannot be validated against the manifest pin: {reason}"),
                )),
            }
        }
        None => {
            let declared: Vec<&str> = pins.iter().map(|(_, r, _)| *r).collect();
            out.push(finding(
                &prov.cassandra_ref,
                format!(
                    "recorded cassandra_ref is not a declared pin (cassandra_source.ref or \
                     cassandra_source.additional_pins: {declared:?})"
                ),
            ));
            // With no pin to bind to, still name each value that no pin declares.
            if !pins.iter().any(|(v, _, _)| *v == prov.cassandra_version) {
                out.push(finding(
                    &prov.cassandra_version,
                    "recorded cassandra_version is not declared by any pin".to_string(),
                ));
            }
            if !pins.iter().any(|(_, _, s)| *s == prov.cassandra_git_sha) {
                out.push(finding(
                    &prov.cassandra_git_sha,
                    "recorded cassandra_git_sha is not declared by any pin".to_string(),
                ));
            }
            match &img_version {
                Ok(v) if pins.iter().any(|(pv, _, _)| pv == v) => {}
                Ok(v) => out.push(finding(
                    &prov.docker_image,
                    format!("docker_image tag version {v} is not declared by any pin"),
                )),
                Err(reason) => out.push(finding(
                    &prov.docker_image,
                    format!("docker_image cannot be validated against the manifest pin: {reason}"),
                )),
            }
        }
    }

    out
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
