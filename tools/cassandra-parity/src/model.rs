//! Serde model for `test-data/cassandra-parity-manifest.yml`.
//!
//! Enum-like fields are deserialized as plain `String` so the linter can attribute
//! invalid values to a scenario id + field path with a helpful message, rather
//! than failing opaquely during deserialization. The closed value sets live in
//! [`crate::enums`] and are enforced by [`crate::lint`].

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub manifest_version: u32,
    pub cassandra_source: CassandraSource,
    pub program: Program,
    pub scenarios: Vec<Scenario>,
}

#[derive(Debug, Deserialize)]
pub struct CassandraSource {
    pub repo: String,
    #[serde(rename = "ref")]
    pub git_ref: String,
    pub sha: String,
    pub index: String,
    pub assessment_report: String,
}

#[derive(Debug, Deserialize)]
pub struct Program {
    pub parent_epic: u64,
    pub reporting_epic: u64,
}

#[derive(Debug, Deserialize)]
pub struct Scenario {
    pub id: String,
    pub title: String,
    pub status: String,
    pub capability: String,
    pub priority: String,
    pub risk: String,
    pub cassandra: Cassandra,
    pub cqlite: Cqlite,
    #[serde(default)]
    pub fixtures: Fixtures,
    pub evidence: Evidence,
    pub ci: Ci,
    #[serde(default)]
    pub scope: Scope,
}

#[derive(Debug, Deserialize)]
pub struct Cassandra {
    pub category: String,
    pub relevance: String,
    #[serde(default)]
    pub files: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Cqlite {
    #[serde(default)]
    pub coverage: Coverage,
}

#[derive(Debug, Default, Deserialize)]
pub struct Coverage {
    #[serde(default)]
    pub suite: Option<String>,
    #[serde(default)]
    pub tests: Vec<String>,
    #[serde(default)]
    pub notes: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct Fixtures {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub references: Vec<String>,
    #[serde(default)]
    pub datasets: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Evidence {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub strict: Option<bool>,
    #[serde(default)]
    pub artifacts: Vec<String>,
    #[serde(default)]
    pub cassandra_version: Option<String>,
    #[serde(default)]
    pub cassandra_git_sha: Option<String>,
    #[serde(default)]
    pub storage_format_version: Vec<String>,
    #[serde(default)]
    pub fixture_generation_command: Option<String>,
    #[serde(default)]
    pub comparison_command: Option<String>,
    #[serde(default)]
    pub reference_paths: Vec<String>,
    #[serde(default)]
    pub failure_artifacts: Vec<String>,
    #[serde(default)]
    pub normalization: Option<String>,
    #[serde(default)]
    pub known_limitations: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Ci {
    pub tier: String,
    #[serde(default)]
    pub workflow: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct Scope {
    #[serde(default)]
    pub rationale: Option<String>,
    #[serde(default)]
    pub gap: Option<String>,
    #[serde(default)]
    pub next_step: Option<String>,
    #[serde(default)]
    pub target_issue: Option<u64>,
    #[serde(default)]
    pub target_suite: Option<String>,
    #[serde(default)]
    pub out_of_scope_category: Option<String>,
    #[serde(default)]
    pub cqlite_boundary: Option<String>,
    #[serde(default)]
    pub safe_claim: Option<String>,
    #[serde(default)]
    pub related_in_scope_scenarios: Vec<String>,
}

impl Manifest {
    /// Parse a manifest from YAML text.
    pub fn from_yaml(text: &str) -> anyhow::Result<Self> {
        Ok(serde_yaml::from_str(text)?)
    }
}

/// A non-empty optional string helper used throughout the linter.
pub fn non_empty(opt: &Option<String>) -> bool {
    opt.as_ref().map(|s| !s.trim().is_empty()).unwrap_or(false)
}
