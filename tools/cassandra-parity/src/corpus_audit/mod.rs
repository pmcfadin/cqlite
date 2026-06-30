//! Corpus audit (`cassandra-parity corpus-audit`, issue #1026): after the
//! exhaustive-regeneration lane rebuilds the Cassandra-generated parity corpus,
//! this audit diffs the regenerated component inventory + the run's provenance
//! against `test-data/cassandra-parity-manifest.yml`.
//!
//! Like [`crate::tier_contract`] and [`crate::workflow_check`], the audit is a
//! pure data-in / findings-out function so it is hermetically unit-testable
//! (clean + drifted in-memory fixtures, no Docker / live Cassandra / disk). The
//! `corpus-audit` subcommand in `main.rs` builds the inventories from the
//! regenerated tree on disk and calls [`audit`].
//!
//! Failure classes (design D3 of the OpenSpec change):
//!   1. **Missing reference** — a manifest reference under the regenerated corpus
//!      tree whose UUID-independent table+component identity NO fresh component
//!      matches (a genuine disappearance). A reference the corpus still produces
//!      under a *churned* `<table>-<uuid>` directory is NOT a finding: every
//!      regeneration mints fresh table UUIDs, so reference classification is by
//!      identity, never raw path ([`refs::component_identity`], issue #1026).
//!   3. **Unclassified high-relevance file** — a `docs/cassandra_test_index.md`
//!      high-relevance entry no manifest scenario classifies (reuses
//!      [`crate::coverage`], the `coverage --strict` failure verbatim).
//!   4. **Unexpected component change** — a regenerated component whose
//!      presence/SHA256 diverges from the expected manifest entry set.
//!   5. **Provenance mismatch** — the run's recorded Cassandra version/ref/sha is
//!      not declared by the manifest's `cassandra_source` / `evidence.*` pin.
//!   6. **Corruption coverage gap** — a required corrupted-component type
//!      (Data.db … Digest.crc32) has no corruption fixture.

use std::collections::{BTreeMap, BTreeSet};

use crate::coverage;
use crate::model::Manifest;

pub mod provenance;
pub mod refs;

pub use provenance::Provenance;

/// The seven SSTable component types that the corruption-fixture corpus MUST
/// each cover with at least one fixture (spec requirement "Corruption fixture
/// generation covers every required component type").
pub const REQUIRED_CORRUPTION_COMPONENTS: &[&str] = &[
    "Data.db",
    "Index.db",
    "Summary.db",
    "Statistics.db",
    "CompressionInfo.db",
    "TOC.txt",
    "Digest.crc32",
];

/// The kind of a single audit finding. Every kind is a hard-fail (non-zero exit)
/// per the owner-pinned strictness decision (always hard-fail; no report-but-pass).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FindingKind {
    MissingReference,
    /// Retained for the spec's "stale reference" vocabulary and the rendered
    /// tag. A genuinely vanished reference (no regenerated file shares its
    /// table+component identity) is reported as [`Self::MissingReference`]; a
    /// reference present under a churned `<table>-<uuid>` directory is clean, so
    /// reference checking never constructs this variant (issue #1026).
    StaleReference,
    UnclassifiedHighRelevance,
    UnexpectedComponentChange,
    ProvenanceMismatch,
    CorruptionCoverageGap,
}

impl FindingKind {
    /// Stable, greppable tag for rendered output.
    pub fn tag(self) -> &'static str {
        match self {
            FindingKind::MissingReference => "MISSING-REFERENCE",
            FindingKind::StaleReference => "STALE-REFERENCE",
            FindingKind::UnclassifiedHighRelevance => "UNCLASSIFIED-HIGH-RELEVANCE",
            FindingKind::UnexpectedComponentChange => "UNEXPECTED-COMPONENT-CHANGE",
            FindingKind::ProvenanceMismatch => "PROVENANCE-MISMATCH",
            FindingKind::CorruptionCoverageGap => "CORRUPTION-COVERAGE-GAP",
        }
    }
}

/// One audit failure naming the offending reference/component/value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditFinding {
    pub kind: FindingKind,
    /// The named offender (reference path, component path, version/sha, etc.).
    pub subject: String,
    /// Why it is a finding.
    pub detail: String,
}

impl AuditFinding {
    pub fn new(kind: FindingKind, subject: impl Into<String>, detail: impl Into<String>) -> Self {
        AuditFinding {
            kind,
            subject: subject.into(),
            detail: detail.into(),
        }
    }
}

/// The regenerated corpus as the audit sees it: the set of repo-relative file
/// paths present, plus an optional SHA256 per path (when a checksums file was
/// supplied to the CLI).
#[derive(Debug, Default)]
pub struct CorpusInventory {
    pub files: BTreeSet<String>,
    pub checksums: BTreeMap<String, String>,
}

/// The expected component entry set the corpus is diffed against: repo-relative
/// path -> expected SHA256 (e.g. committed reference-golden checksums computed at
/// checkout, before regeneration).
#[derive(Debug, Default)]
pub struct ExpectedInventory {
    pub components: BTreeMap<String, String>,
}

/// Outcome of [`audit`]. `ok()` is true only when there are zero findings.
#[derive(Debug, Default)]
pub struct AuditReport {
    pub findings: Vec<AuditFinding>,
}

impl AuditReport {
    pub fn ok(&self) -> bool {
        self.findings.is_empty()
    }

    /// Count of findings of a given kind (used by the CLI summary + tests).
    pub fn count(&self, kind: FindingKind) -> usize {
        self.findings.iter().filter(|f| f.kind == kind).count()
    }

    /// Human + machine readable rendering, one finding per line.
    pub fn render(&self) -> String {
        if self.ok() {
            return "corpus-audit: OK".to_string();
        }
        let mut lines = Vec::with_capacity(self.findings.len());
        for f in &self.findings {
            lines.push(format!("{} {} — {}", f.kind.tag(), f.subject, f.detail));
        }
        lines.join("\n")
    }
}

/// Run the full corpus audit. Pure: all inputs are owned by the caller, so the
/// same call is used by the CLI (disk-backed inputs) and the unit tests
/// (synthetic in-memory inputs).
///
/// `provenance` is `None` only when no provenance record was supplied; the
/// regeneration lane always supplies one, so the provenance comparison runs in CI.
pub fn audit(
    manifest: &Manifest,
    index_text: &str,
    inventory: &CorpusInventory,
    expected: &ExpectedInventory,
    provenance: Option<&Provenance>,
    corruption_components: &BTreeSet<String>,
) -> AuditReport {
    let mut findings = Vec::new();

    // 1 + 2: missing / stale references over the regenerated corpus tree.
    findings.extend(refs::check_references(manifest, inventory));

    // 3: unclassified high-relevance Cassandra files (reuse the coverage classifier).
    let cov = coverage::analyze(manifest, index_text);
    for f in &cov.unclassified_high {
        findings.push(AuditFinding::new(
            FindingKind::UnclassifiedHighRelevance,
            f.clone(),
            "high-relevance Cassandra test file is not referenced by any manifest scenario",
        ));
    }

    // 4: unexpected component change (presence/checksum vs expected entry set).
    findings.extend(check_component_changes(inventory, expected));

    // 5: provenance vs the manifest's declared Cassandra pin.
    if let Some(prov) = provenance {
        findings.extend(provenance::check_provenance(prov, manifest));
    }

    // 6: corruption-fixture coverage of every required component type.
    findings.extend(check_corruption_coverage(corruption_components));

    AuditReport { findings }
}

/// Diff the regenerated component inventory's presence/checksums against the
/// expected entry set: a recorded component that disappeared, whose checksum
/// changed, or that newly appeared inside an expected table — each without a
/// corresponding manifest update — is an unexpected component change.
///
/// Both sides are keyed by their UUID-independent table+component identity
/// ([`refs::component_identity`]), NOT by the raw repo-relative path. Every
/// regeneration `rm -rf`s the corpus and re-mints each table under a fresh
/// `<table>-<uuid>` directory, so the committed-expected golden and the
/// regenerated-actual golden never share a path; comparing identities is what
/// makes the check fire only on a *genuine* presence/checksum change of a stable
/// identity instead of false-positiving on the per-run UUID churn (issue #1026).
///
/// "Appeared" is scoped to tables that already carry an expected entry, so churn
/// in *unpinned* fixtures never reds the lane: only a table the manifest already
/// tracks can surface a surprise component.
pub fn check_component_changes(
    inventory: &CorpusInventory,
    expected: &ExpectedInventory,
) -> Vec<AuditFinding> {
    let mut out = Vec::new();
    if expected.components.is_empty() {
        return out;
    }

    // Regenerated checksummed components keyed by UUID-independent identity,
    // keeping a representative regenerated path for messaging.
    let mut actual_by_identity: BTreeMap<String, (String, String)> = BTreeMap::new();
    for (path, sha) in &inventory.checksums {
        actual_by_identity
            .entry(refs::component_identity(path))
            .or_insert_with(|| (sha.clone(), path.clone()));
    }

    // Identities + tables the expected set tracks (for the "appeared" half).
    let mut expected_identities: BTreeSet<String> = BTreeSet::new();
    let mut expected_tables: BTreeSet<String> = BTreeSet::new();

    // Disappeared / checksum-changed, by identity.
    for (path, expected_sha) in &expected.components {
        let identity = refs::component_identity(path);
        expected_tables.insert(parent_dir(&identity).to_string());
        expected_identities.insert(identity.clone());
        match actual_by_identity.get(&identity) {
            Some((actual_sha, _)) if actual_sha == expected_sha => {}
            Some((actual_sha, actual_path)) => out.push(AuditFinding::new(
                FindingKind::UnexpectedComponentChange,
                actual_path.clone(),
                format!("checksum changed: expected {expected_sha}, regenerated {actual_sha}"),
            )),
            None => out.push(AuditFinding::new(
                FindingKind::UnexpectedComponentChange,
                path.clone(),
                "expected component is absent from the regenerated corpus".to_string(),
            )),
        }
    }

    // Appeared: a regenerated component whose table identity the expected set
    // already tracks, but whose own component identity it does not.
    for (identity, (_, path)) in &actual_by_identity {
        if expected_identities.contains(identity) {
            continue;
        }
        if expected_tables.contains(parent_dir(identity)) {
            out.push(AuditFinding::new(
                FindingKind::UnexpectedComponentChange,
                path.clone(),
                "component appeared in a tracked table with no expected manifest entry".to_string(),
            ));
        }
    }

    out
}

/// Fail when any required corrupted-component type has no corruption fixture.
/// `produced` is the set of `expected_failing_component` values the corruption
/// corpus actually generated (parsed from `corruption-manifest.yml` by the CLI).
pub fn check_corruption_coverage(produced: &BTreeSet<String>) -> Vec<AuditFinding> {
    let mut out = Vec::new();
    for req in REQUIRED_CORRUPTION_COMPONENTS {
        if !produced.contains(*req) {
            out.push(AuditFinding::new(
                FindingKind::CorruptionCoverageGap,
                (*req).to_string(),
                "no corruption fixture targets this required component type".to_string(),
            ));
        }
    }
    out
}

/// Parent directory of a `/`-separated repo-relative path (`""` if top-level).
fn parent_dir(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[..i],
        None => "",
    }
}
