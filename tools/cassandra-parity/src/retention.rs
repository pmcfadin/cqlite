//! Machine-enforced artifact-retention check (issue #1027, section 5).
//!
//! Every parity workflow that uploads failure artifacts MUST set the
//! `upload-artifact` step's `retention-days` at or above the minimum for the
//! tier(s) of the manifest scenarios it gates. The minimums are owner-confirmed
//! policy documented (single source) in the machine-parseable
//! ```parity-retention-minimums fenced block of
//! `docs/development/parity-ci-tiers.md`:
//!   required_parity=14, nightly_docker=30, exhaustive_regeneration=90.
//! `fast_pr` and `manual_debug` have no minimum (logs only / attach to issue).
//!
//! The check is pure text-in / findings-out (doc text, workflow YAML, manifest
//! tiers) so it is trivially unit-testable, mirroring [`crate::workflow_check`].

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::Deserialize;
use thiserror::Error;

use crate::model::Manifest;

/// The fence tag marking the machine-parseable retention-minimums block in
/// `parity-ci-tiers.md`.
const DOC_FENCE_TAG: &str = "parity-retention-minimums";

/// Errors that prevent the retention check from running (as opposed to a
/// below-minimum finding the check is designed to report).
#[derive(Debug, Error)]
pub enum RetentionError {
    #[error(
        "tier-contract doc is missing its ```{DOC_FENCE_TAG} fenced block \
         (the machine-parseable retention minimums)"
    )]
    DocBlockMissing,
    #[error("tier-contract doc's ```{DOC_FENCE_TAG} block is empty")]
    DocBlockEmpty,
    #[error("malformed retention-minimum line (expected `tier=days`): {0}")]
    MalformedMinimum(String),
}

/// A single below-minimum finding for one workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionFinding {
    /// The workflow path (for messages).
    pub workflow: String,
    /// The tier whose minimum was violated (the binding tier for this lane).
    pub tier: String,
    /// The minimum required for that tier.
    pub minimum: u32,
    /// The offending `retention-days` value found (or `None` when a required
    /// upload step set no `retention-days` at all).
    pub found: Option<u32>,
    /// Human-readable explanation.
    pub message: String,
}

/// Parse the documented retention minimums from the fenced block. Each non-blank
/// line is `tier=days`.
pub fn parse_documented_minimums(doc: &str) -> Result<BTreeMap<String, u32>, RetentionError> {
    let open = format!("```{DOC_FENCE_TAG}");
    let after = doc
        .find(&open)
        .map(|i| i + open.len())
        .ok_or(RetentionError::DocBlockMissing)?;
    let rest = &doc[after..];
    let body_start = rest.find('\n').map(|n| n + 1).unwrap_or(rest.len());
    let body = &rest[body_start..];
    let close = body.find("```").ok_or(RetentionError::DocBlockMissing)?;

    let mut out = BTreeMap::new();
    for raw in body[..close].lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let (tier, days) = line
            .split_once('=')
            .ok_or_else(|| RetentionError::MalformedMinimum(line.to_string()))?;
        let days: u32 = days
            .trim()
            .parse()
            .map_err(|_| RetentionError::MalformedMinimum(line.to_string()))?;
        out.insert(tier.trim().to_string(), days);
    }
    if out.is_empty() {
        return Err(RetentionError::DocBlockEmpty);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Workflow upload-artifact retention parsing.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct WorkflowYaml {
    #[serde(default)]
    jobs: BTreeMap<String, JobYaml>,
}

#[derive(Debug, Deserialize)]
struct JobYaml {
    #[serde(default)]
    steps: Vec<StepYaml>,
}

#[derive(Debug, Default, Deserialize)]
struct StepYaml {
    #[serde(default)]
    uses: Option<String>,
    #[serde(default)]
    with: BTreeMap<String, serde_yaml::Value>,
}

/// The `retention-days` values of every `actions/upload-artifact` step in a
/// workflow. A `None` entry marks an upload step that set no `retention-days`
/// (which, for a lane with a tier minimum, defaults to the org's 90d but cannot
/// be *relied* on — it is reported so the lane sets an explicit window).
pub fn upload_retention_days(workflow_text: &str) -> Vec<Option<u32>> {
    let Ok(wf) = serde_yaml::from_str::<WorkflowYaml>(workflow_text) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for job in wf.jobs.values() {
        for step in &job.steps {
            let is_upload = step
                .uses
                .as_deref()
                .map(|u| u.trim_start().starts_with("actions/upload-artifact"))
                .unwrap_or(false);
            if !is_upload {
                continue;
            }
            let days = step.with.get("retention-days").and_then(yaml_value_as_u32);
            out.push(days);
        }
    }
    out
}

/// Coerce a YAML `retention-days` value (integer, or a quoted-string integer) to
/// `u32`. Non-integer / negative values yield `None`.
fn yaml_value_as_u32(v: &serde_yaml::Value) -> Option<u32> {
    match v {
        serde_yaml::Value::Number(n) => n.as_u64().and_then(|x| u32::try_from(x).ok()),
        serde_yaml::Value::String(s) => s.trim().parse::<u32>().ok(),
        _ => None,
    }
}

/// The retention minimum a lane must satisfy = the LARGEST minimum among the
/// tiers of the scenarios it gates (a lane that gates both `required_parity` and
/// `nightly_docker` scenarios must satisfy the stricter, 30-day, window). Returns
/// `None` when the lane gates only tiers with no minimum (`fast_pr`/`manual_debug`).
pub fn binding_minimum(
    lane_tiers: &[String],
    minimums: &BTreeMap<String, u32>,
) -> Option<(String, u32)> {
    lane_tiers
        .iter()
        .filter_map(|t| minimums.get(t).map(|m| (t.clone(), *m)))
        .max_by_key(|(_, m)| *m)
}

/// Check one workflow's upload-artifact retention against the minimum for the
/// tiers it gates. `lane_tiers` is the set of `ci.tier` values of the manifest
/// scenarios whose `ci.workflow` is this workflow. Returns one finding per
/// offending upload step (empty == OK). A lane whose binding minimum is `None`
/// (no fixture-retaining tier) is never flagged.
pub fn check_workflow(
    workflow_path: &str,
    workflow_text: &str,
    lane_tiers: &[String],
    minimums: &BTreeMap<String, u32>,
) -> Vec<RetentionFinding> {
    let mut out = Vec::new();
    let Some((tier, minimum)) = binding_minimum(lane_tiers, minimums) else {
        return out;
    };
    for found in upload_retention_days(workflow_text) {
        let below = match found {
            Some(days) => days < minimum,
            // No explicit retention-days on a lane that must retain fixtures: the
            // implicit default is not a guaranteed floor, so require it be set.
            None => true,
        };
        if below {
            let message = match found {
                Some(days) => format!(
                    "{workflow_path}: upload-artifact retention-days {days} is below the \
                     minimum {minimum} for tier `{tier}`"
                ),
                None => format!(
                    "{workflow_path}: upload-artifact sets no retention-days; tier `{tier}` \
                     requires an explicit `retention-days: >= {minimum}`"
                ),
            };
            out.push(RetentionFinding {
                workflow: workflow_path.to_string(),
                tier: tier.clone(),
                minimum,
                found,
                message,
            });
        }
    }
    out
}

/// Outcome of a whole-repo retention check: how many fixture-retaining workflows
/// were checked and every below-minimum finding.
#[derive(Debug, Default)]
pub struct RepoCheck {
    pub checked: usize,
    pub findings: Vec<RetentionFinding>,
}

impl RepoCheck {
    /// True when no upload step is below its tier minimum.
    pub fn ok(&self) -> bool {
        self.findings.is_empty()
    }

    /// Human + machine readable summary (findings, then a final status line).
    pub fn render(&self) -> String {
        let mut lines: Vec<String> = self
            .findings
            .iter()
            .map(|f| format!("RETENTION {}", f.message))
            .collect();
        if self.ok() {
            lines.push(format!(
                "retention-check: OK — {} fixture-retaining parity workflow(s) meet their tier \
                 retention minimums",
                self.checked
            ));
        } else {
            lines.push(format!(
                "retention-check: FAILED — {} workflow upload step(s) below the tier minimum",
                self.findings.len()
            ));
        }
        lines.join("\n")
    }
}

/// Run the retention check across every workflow named by a manifest scenario.
/// Groups scenarios by `ci.workflow` → the set of `ci.tier` they gate, reads each
/// referenced workflow file relative to `repo_root`, and checks its upload steps
/// against the strictest minimum for the tiers it gates. A lane that gates only
/// no-minimum tiers, or whose workflow file is missing (already reported by
/// `lint`'s path-existence check), is skipped.
pub fn run_repo_check(
    manifest: &Manifest,
    repo_root: &Path,
    minimums: &BTreeMap<String, u32>,
) -> RepoCheck {
    let mut lane_tiers: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for s in &manifest.scenarios {
        if let Some(wf) = &s.ci.workflow {
            lane_tiers
                .entry(wf.clone())
                .or_default()
                .insert(s.ci.tier.clone());
        }
    }

    let mut result = RepoCheck::default();
    for (wf, tiers) in &lane_tiers {
        let tiers_vec: Vec<String> = tiers.iter().cloned().collect();
        if binding_minimum(&tiers_vec, minimums).is_none() {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(repo_root.join(wf)) else {
            continue;
        };
        result.checked += 1;
        result
            .findings
            .extend(check_workflow(wf, &text, &tiers_vec, minimums));
    }
    result
}
