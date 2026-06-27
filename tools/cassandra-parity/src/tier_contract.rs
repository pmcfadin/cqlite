//! Cross-check that keeps the parity CI tier enum honest across three sources:
//! the **documented** enum in `docs/development/parity-ci-tiers.md`, the
//! `cassandra-parity` code enum ([`crate::enums::CI_TIER`]), and the manifest
//! schema's `ci.tier` enum. It also validates that every `ci.tier` used in the
//! manifest YAML is one of the documented tiers.
//!
//! No Docker, datasets, or live Cassandra: this reads only text (doc, schema
//! JSON, manifest YAML) already present in the repository.

use serde::Deserialize;
use thiserror::Error;

/// The fence language tag marking the machine-parseable documented-enum block
/// in `parity-ci-tiers.md` (design D2).
const DOC_FENCE_TAG: &str = "parity-ci-tiers";

/// Errors that prevent the cross-check from running at all (as opposed to a
/// divergence the check is designed to *report*).
#[derive(Debug, Error)]
pub enum TierContractError {
    /// The documented-enum fenced block could not be located in the doc.
    #[error(
        "tier-contract doc is missing its ```{DOC_FENCE_TAG} fenced block \
         (the machine-parseable documented enum)"
    )]
    DocBlockMissing,

    /// The doc's fenced block is present but empty.
    #[error("tier-contract doc's ```{DOC_FENCE_TAG} block is empty")]
    DocBlockEmpty,

    /// The schema JSON could not be parsed or lacks the `ci.tier` enum.
    #[error("could not read ci.tier enum from manifest schema: {0}")]
    SchemaEnum(String),

    /// The manifest YAML could not be parsed for `id` + `ci.tier`.
    #[error("could not parse manifest tiers: {0}")]
    Manifest(String),
}

/// A single tier present in one source but missing from another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumDivergence {
    pub tier: String,
    pub present_in: &'static str,
    pub missing_from: &'static str,
}

/// A manifest scenario whose `ci.tier` is not a documented tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownManifestTier {
    pub scenario_id: String,
    pub tier: String,
}

/// Outcome of [`check`]. `ok()` is true only when there are no divergences and
/// no unknown manifest tiers.
#[derive(Debug, Default)]
pub struct Report {
    pub enum_divergences: Vec<EnumDivergence>,
    pub unknown_manifest_tiers: Vec<UnknownManifestTier>,
}

impl Report {
    /// True when documented, code, and schema enums agree AND every manifest
    /// `ci.tier` is documented.
    pub fn ok(&self) -> bool {
        self.enum_divergences.is_empty() && self.unknown_manifest_tiers.is_empty()
    }

    /// Human + machine readable summary of every problem found.
    pub fn render(&self) -> String {
        if self.ok() {
            return "tier-contract-check: OK".to_string();
        }
        let mut lines = Vec::new();
        for d in &self.enum_divergences {
            lines.push(format!(
                "ENUM-DRIFT tier '{}' present in {} but missing from {}",
                d.tier, d.present_in, d.missing_from
            ));
        }
        for u in &self.unknown_manifest_tiers {
            lines.push(format!(
                "UNKNOWN-MANIFEST-TIER scenario '{}' uses undocumented tier '{}'",
                u.scenario_id, u.tier
            ));
        }
        lines.join("\n")
    }
}

/// Parse the documented tier enum from the fenced ```parity-ci-tiers block.
///
/// The block must contain one tier name per non-blank line and nothing else.
pub fn parse_documented_enum(doc: &str) -> Result<Vec<String>, TierContractError> {
    let open = format!("```{DOC_FENCE_TAG}");
    let after = doc
        .find(&open)
        .map(|i| i + open.len())
        .ok_or(TierContractError::DocBlockMissing)?;
    let rest = &doc[after..];
    // Skip to the end of the opening fence line, then read until the closing ```.
    let body_start = rest.find('\n').map(|n| n + 1).unwrap_or(rest.len());
    let body = &rest[body_start..];
    let close = body.find("```").ok_or(TierContractError::DocBlockMissing)?;
    let tiers: Vec<String> = body[..close]
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();
    if tiers.is_empty() {
        return Err(TierContractError::DocBlockEmpty);
    }
    Ok(tiers)
}

/// Read the `ci.tier` enum from the manifest JSON schema.
pub fn schema_tier_enum(schema_json: &str) -> Result<Vec<String>, TierContractError> {
    let v: serde_json::Value = serde_json::from_str(schema_json)
        .map_err(|e| TierContractError::SchemaEnum(e.to_string()))?;
    let arr = v["$defs"]["scenario"]["properties"]["ci"]["properties"]["tier"]["enum"]
        .as_array()
        .ok_or_else(|| TierContractError::SchemaEnum("ci.tier.enum is not an array".to_string()))?;
    let tiers: Vec<String> = arr
        .iter()
        .filter_map(|x| x.as_str().map(str::to_string))
        .collect();
    if tiers.is_empty() {
        return Err(TierContractError::SchemaEnum("ci.tier enum is empty".to_string()));
    }
    Ok(tiers)
}

/// Minimal view of the manifest: just scenario id + declared `ci.tier`.
/// Permissive on purpose so test fixtures need not be full scenarios.
#[derive(Debug, Deserialize)]
struct TierManifest {
    #[serde(default)]
    scenarios: Vec<TierScenario>,
}

#[derive(Debug, Deserialize)]
struct TierScenario {
    id: String,
    ci: TierCi,
}

#[derive(Debug, Deserialize)]
struct TierCi {
    tier: String,
}

/// Extract `(scenario_id, ci.tier)` pairs from the manifest YAML.
pub fn manifest_tiers(manifest_yaml: &str) -> Result<Vec<(String, String)>, TierContractError> {
    let m: TierManifest = serde_yaml::from_str(manifest_yaml)
        .map_err(|e| TierContractError::Manifest(e.to_string()))?;
    Ok(m.scenarios.into_iter().map(|s| (s.id, s.ci.tier)).collect())
}

/// Run the full cross-check across the documented enum, the code enum, the
/// schema enum, and the manifest's declared tiers.
pub fn check(
    doc: &str,
    schema_json: &str,
    code_enum: &[&str],
    manifest_yaml: &str,
) -> Result<Report, TierContractError> {
    let documented = parse_documented_enum(doc)?;
    let schema = schema_tier_enum(schema_json)?;
    let code: Vec<String> = code_enum.iter().map(|s| s.to_string()).collect();

    let mut report = Report::default();

    // Pairwise set comparison across the three enum sources.
    compare("documented", &documented, "code", &code, &mut report);
    compare("code", &code, "documented", &documented, &mut report);
    compare("documented", &documented, "schema", &schema, &mut report);
    compare("schema", &schema, "documented", &documented, &mut report);
    compare("code", &code, "schema", &schema, &mut report);
    compare("schema", &schema, "code", &code, &mut report);

    // Every manifest ci.tier must be a documented tier.
    let documented_set: std::collections::HashSet<&str> =
        documented.iter().map(String::as_str).collect();
    for (id, tier) in manifest_tiers(manifest_yaml)? {
        if !documented_set.contains(tier.as_str()) {
            report.unknown_manifest_tiers.push(UnknownManifestTier {
                scenario_id: id,
                tier,
            });
        }
    }

    Ok(report)
}

/// Record every tier present in `a` (`a_name`) but missing from `b` (`b_name`).
fn compare(
    a_name: &'static str,
    a: &[String],
    b_name: &'static str,
    b: &[String],
    report: &mut Report,
) {
    let b_set: std::collections::HashSet<&str> = b.iter().map(String::as_str).collect();
    for tier in a {
        if !b_set.contains(tier.as_str()) {
            let div = EnumDivergence {
                tier: tier.clone(),
                present_in: a_name,
                missing_from: b_name,
            };
            if !report.enum_divergences.contains(&div) {
                report.enum_divergences.push(div);
            }
        }
    }
}
