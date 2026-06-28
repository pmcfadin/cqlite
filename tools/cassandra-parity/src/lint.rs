//! Cross-field linting for the parity manifest.
//!
//! JSON Schema (`cassandra-parity-manifest.schema.json`) covers structure and
//! enum membership; this module re-checks enums (to attach scenario ids to
//! errors) and enforces the cross-field parity rules from issues #976, #979,
//! and #980 that schema alone cannot express.

use std::path::Path;

use crate::enums;
use crate::model::{non_empty, Manifest, Scenario};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Error,
    Warn,
}

#[derive(Debug, Clone)]
pub struct Finding {
    pub level: Level,
    pub id: String,
    pub field: String,
    pub message: String,
}

impl Finding {
    fn error(id: &str, field: &str, message: impl Into<String>) -> Self {
        Finding {
            level: Level::Error,
            id: id.to_string(),
            field: field.to_string(),
            message: message.into(),
        }
    }
}

/// Validate the manifest. When `repo_root` is `Some`, referenced local files are
/// required to exist (unless the scenario is `planned`).
pub fn lint(m: &Manifest, repo_root: Option<&Path>) -> Vec<Finding> {
    let mut out = Vec::new();

    if m.manifest_version != 1 {
        out.push(Finding::error(
            "<manifest>",
            "manifest_version",
            format!("manifest_version must be 1, got {}", m.manifest_version),
        ));
    }

    // Unique, well-formed ids.
    let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for s in &m.scenarios {
        *seen.entry(s.id.as_str()).or_insert(0) += 1;
    }
    for s in &m.scenarios {
        if seen.get(s.id.as_str()).copied().unwrap_or(0) > 1 {
            out.push(Finding::error(&s.id, "id", "duplicate scenario id"));
        }
        if !valid_id(&s.id) {
            out.push(Finding::error(
                &s.id,
                "id",
                "id must match cass.<group>.<slug> (lowercase second segment, dotted)",
            ));
        }
    }

    for s in &m.scenarios {
        lint_scenario(s, repo_root, &mut out);
    }

    out
}

fn lint_scenario(s: &Scenario, repo_root: Option<&Path>, out: &mut Vec<Finding>) {
    let id = s.id.as_str();

    // --- enum membership (re-checked here so errors carry the scenario id) ---
    check_enum(out, id, "status", &s.status, enums::STATUS);
    check_enum(out, id, "capability", &s.capability, enums::CAPABILITY);
    check_enum(out, id, "priority", &s.priority, enums::PRIORITY);
    check_enum(out, id, "risk", &s.risk, enums::RISK);
    check_enum(
        out,
        id,
        "cassandra.category",
        &s.cassandra.category,
        enums::CATEGORY,
    );
    check_enum(
        out,
        id,
        "cassandra.relevance",
        &s.cassandra.relevance,
        enums::RELEVANCE,
    );
    check_enum(
        out,
        id,
        "evidence.type",
        &s.evidence.kind,
        enums::EVIDENCE_TYPE,
    );
    check_enum(out, id, "ci.tier", &s.ci.tier, enums::CI_TIER);
    if let Some(suite) = &s.cqlite.coverage.suite {
        check_enum(out, id, "cqlite.coverage.suite", suite, enums::SUITE);
    }
    if let Some(c) = &s.scope.out_of_scope_category {
        check_enum(
            out,
            id,
            "scope.out_of_scope_category",
            c,
            enums::OUT_OF_SCOPE_CATEGORY,
        );
    }
    if let Some(ts) = &s.scope.target_suite {
        check_enum(out, id, "scope.target_suite", ts, enums::SUITE);
    }
    for a in &s.evidence.artifacts {
        check_enum(out, id, "evidence.artifacts", a, enums::ARTIFACT);
    }
    for v in &s.evidence.storage_format_version {
        check_enum(
            out,
            id,
            "evidence.storage_format_version",
            v,
            enums::STORAGE_FORMAT,
        );
    }

    if s.cassandra.files.is_empty() {
        out.push(Finding::error(
            id,
            "cassandra.files",
            "at least one Cassandra file/category reference is required",
        ));
    }

    // --- status-conditional rules ---
    match s.status.as_str() {
        "mirrored" => {
            // delta_scan mirrored scenarios are held to a stricter bar than the
            // generic OR rule (issue #995, AC6): a delta-shape claim is only
            // trustworthy when it is backed by BOTH a real CQLite per-shape test
            // (whose file exists on disk) AND a JSONL fixture reference. The
            // generic OR rule below covers all other capabilities.
            if s.capability == "delta_scan" {
                if s.cqlite.coverage.tests.is_empty() {
                    out.push(Finding::error(
                        id,
                        "cqlite.coverage.tests",
                        "mirrored delta_scan scenarios must name a CQLite test target",
                    ));
                }
                if s.fixtures.references.is_empty() {
                    out.push(Finding::error(
                        id,
                        "fixtures.references",
                        "mirrored delta_scan scenarios must name a fixture reference",
                    ));
                }
                // The referenced test file(s) AND fixture(s) must actually exist
                // on disk: a dangling/typo'd path must not pass lint silently
                // (AC6 — backed by BOTH a real test AND a real fixture).
                if let Some(root) = repo_root {
                    for t in &s.cqlite.coverage.tests {
                        if !root.join(t).exists() {
                            out.push(Finding::error(
                                id,
                                "cqlite.coverage.tests",
                                format!("mirrored delta_scan test target does not exist: {t}"),
                            ));
                        }
                    }
                    for r in &s.fixtures.references {
                        if !root.join(r).exists() {
                            out.push(Finding::error(
                                id,
                                "fixtures.references",
                                format!(
                                    "mirrored delta_scan fixture reference does not exist: {r}"
                                ),
                            ));
                        }
                    }
                }
            } else if s.cqlite.coverage.tests.is_empty() && s.fixtures.references.is_empty() {
                out.push(Finding::error(
                    id,
                    "cqlite.coverage.tests|fixtures.references",
                    "mirrored scenarios must name a CQLite test target or a fixture reference",
                ));
            }
        }
        "partial" => {
            if !non_empty(&s.scope.gap) {
                out.push(Finding::error(
                    id,
                    "scope.gap",
                    "partial scenarios require scope.gap",
                ));
            }
            if !non_empty(&s.scope.next_step) {
                out.push(Finding::error(
                    id,
                    "scope.next_step",
                    "partial scenarios require scope.next_step",
                ));
            }
        }
        "planned" => {
            if s.scope.target_issue.is_none() && !non_empty(&s.scope.target_suite) {
                out.push(Finding::error(
                    id,
                    "scope.target_issue|scope.target_suite",
                    "planned scenarios require scope.target_issue or scope.target_suite",
                ));
            }
        }
        "out_of_scope" => {
            for (field, present) in [
                (
                    "scope.out_of_scope_category",
                    non_empty(&s.scope.out_of_scope_category),
                ),
                ("scope.rationale", non_empty(&s.scope.rationale)),
                ("scope.cqlite_boundary", non_empty(&s.scope.cqlite_boundary)),
                ("scope.safe_claim", non_empty(&s.scope.safe_claim)),
            ] {
                if !present {
                    out.push(Finding::error(
                        id,
                        field,
                        "out_of_scope scenarios require this field",
                    ));
                }
            }
            if s.scope.related_in_scope_scenarios.is_empty() {
                out.push(Finding::error(
                    id,
                    "scope.related_in_scope_scenarios",
                    "out_of_scope scenarios must list related in-scope scenarios",
                ));
            }
            if s.evidence.comparison_command.is_some() {
                out.push(Finding::error(
                    id,
                    "evidence.comparison_command",
                    "out_of_scope scenarios must not define a comparison_command",
                ));
            }
        }
        _ => {}
    }

    // --- evidence-type rules ---
    match s.evidence.kind.as_str() {
        "byte_for_byte" => {
            if s.evidence.strict != Some(true) {
                out.push(Finding::error(
                    id,
                    "evidence.strict",
                    "byte_for_byte requires strict: true",
                ));
            }
            if !s
                .evidence
                .artifacts
                .iter()
                .any(|a| enums::BYTE_LEVEL_ARTIFACTS.contains(&a.as_str()))
            {
                out.push(Finding::error(
                    id,
                    "evidence.artifacts",
                    "byte_for_byte requires a bytes/offsets/checksums/component_files artifact",
                ));
            }
            if !non_empty(&s.evidence.comparison_command) {
                out.push(Finding::error(
                    id,
                    "evidence.comparison_command",
                    "byte_for_byte requires a comparison_command",
                ));
            }
            if s.evidence.reference_paths.is_empty() {
                out.push(Finding::error(
                    id,
                    "evidence.reference_paths",
                    "byte_for_byte requires reference_paths",
                ));
            }
            if s.evidence.failure_artifacts.is_empty() {
                out.push(Finding::error(
                    id,
                    "evidence.failure_artifacts",
                    "byte_for_byte requires failure_artifacts for the diff",
                ));
            }
        }
        "canonical_semantic" => {
            if !non_empty(&s.evidence.normalization) {
                out.push(Finding::error(
                    id,
                    "evidence.normalization",
                    "canonical_semantic requires a normalization description",
                ));
            }
            let has_jsonl = s.evidence.artifacts.iter().any(|a| a == "jsonl")
                || s.evidence
                    .reference_paths
                    .iter()
                    .any(|p| p.ends_with(".jsonl"));
            if !has_jsonl {
                out.push(Finding::error(
                    id,
                    "evidence.artifacts",
                    "canonical_semantic requires a jsonl artifact or .jsonl reference path",
                ));
            }
        }
        "smoke" => {
            if !non_empty(&s.evidence.known_limitations) {
                out.push(Finding::error(
                    id,
                    "evidence.known_limitations",
                    "smoke requires known_limitations stating parse/load success is not byte parity",
                ));
            }
            if s.priority == "P0" && s.risk == "p0_data_loss" && !non_empty(&s.scope.gap) {
                out.push(Finding::error(
                    id,
                    "scope.gap",
                    "smoke cannot satisfy a P0 data-loss scenario without an explicit scope.gap",
                ));
            }
        }
        "partial" => {
            if !non_empty(&s.evidence.known_limitations) {
                out.push(Finding::error(
                    id,
                    "evidence.known_limitations",
                    "partial evidence requires known_limitations",
                ));
            }
        }
        _ => {}
    }

    // --- evidence metadata for fixture-backed scenarios (#980) ---
    let fixture_backed =
        matches!(s.status.as_str(), "mirrored" | "partial") && s.risk != "tooling_only";
    if fixture_backed {
        if !non_empty(&s.evidence.cassandra_version) {
            out.push(Finding::error(
                id,
                "evidence.cassandra_version",
                "fixture-backed scenarios require evidence.cassandra_version",
            ));
        }
        if !non_empty(&s.evidence.cassandra_git_sha) {
            out.push(Finding::error(
                id,
                "evidence.cassandra_git_sha",
                "fixture-backed scenarios require evidence.cassandra_git_sha",
            ));
        }
        if s.evidence.storage_format_version.is_empty() {
            out.push(Finding::error(
                id,
                "evidence.storage_format_version",
                "fixture-backed scenarios require evidence.storage_format_version",
            ));
        }
        if !non_empty(&s.evidence.fixture_generation_command) {
            out.push(Finding::error(
                id,
                "evidence.fixture_generation_command",
                "fixture-backed scenarios require evidence.fixture_generation_command",
            ));
        }
    }

    // --- CI rules ---
    if s.ci.tier == "required_parity" && !non_empty(&s.ci.workflow) {
        out.push(Finding::error(
            id,
            "ci.workflow",
            "required_parity scenarios must name a workflow path",
        ));
    }

    // --- referenced local files must exist (unless planned) ---
    if let Some(root) = repo_root {
        if s.status != "planned" {
            let mut paths: Vec<(&str, &String)> = Vec::new();
            for p in &s.fixtures.references {
                paths.push(("fixtures.references", p));
            }
            for p in &s.fixtures.datasets {
                paths.push(("fixtures.datasets", p));
            }
            for p in &s.evidence.reference_paths {
                paths.push(("evidence.reference_paths", p));
            }
            for p in &s.cqlite.coverage.tests {
                paths.push(("cqlite.coverage.tests", p));
            }
            if let Some(w) = &s.ci.workflow {
                paths.push(("ci.workflow", w));
            }
            for (field, p) in paths {
                if !root.join(p).exists() {
                    out.push(Finding::error(
                        id,
                        field,
                        format!("referenced local path does not exist: {p}"),
                    ));
                }
            }
        }
    }
}

fn check_enum(out: &mut Vec<Finding>, id: &str, field: &str, value: &str, allowed: &[&str]) {
    if !allowed.contains(&value) {
        out.push(Finding::error(
            id,
            field,
            format!("invalid value '{value}'; allowed: {}", allowed.join(", ")),
        ));
    }
}

fn valid_id(id: &str) -> bool {
    let parts: Vec<&str> = id.split('.').collect();
    if parts.len() < 3 || parts[0] != "cass" {
        return false;
    }
    let lower_ok = |p: &str| {
        !p.is_empty()
            && p.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    };
    let mixed_ok =
        |p: &str| !p.is_empty() && p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !lower_ok(parts[1]) {
        return false;
    }
    parts[2..].iter().all(|p| mixed_ok(p))
}
