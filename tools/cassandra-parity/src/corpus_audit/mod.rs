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
//!   4. **Unexpected component change** — a COVERAGE/PRESENCE check (issue #2009):
//!      an expected non-`system*` component identity that the regeneration did NOT
//!      reproduce at all. This tier promises coverage/presence, NOT byte-drift
//!      detection: a present identity passes regardless of its SHA256 (byte-parity
//!      is owned by the sstabledump-parity-gate + nightly_docker tiers on the
//!      committed corpus), and `system*` keyspaces are excluded from the expected
//!      inventory because their contents are inherently run-dependent.
//!   5. **Provenance mismatch** — the run's recorded Cassandra version/ref/sha is
//!      not declared by the manifest's `cassandra_source` / `evidence.*` pin.
//!   6. **Corruption coverage gap** — a required corrupted-component type
//!      (Data.db … Digest.crc32) has no on-disk corruption fixture: either no
//!      manifest fixture declares it, or every fixture that declares it has no
//!      corrupted file present in the regenerated corpus (spec R4 is on-disk
//!      reality, not just a manifest declaration; issue #1026).

use std::collections::{BTreeMap, BTreeSet};

use crate::coverage;
use crate::model::Manifest;

pub mod audit_report;
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

/// One corruption fixture as declared in `corruption-manifest.yml`. The audit
/// cross-checks each fixture's [`corrupted_path`](Self::corrupted_path) against
/// the walked corpus inventory so a fixture that was DECLARED but never produced
/// on disk is caught (spec R4: an on-disk fixture per required component, not
/// merely a manifest declaration; issue #1026).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorruptionFixture {
    /// The targeted `expected_failing_component`, e.g. `Data.db`.
    pub component: String,
    /// Datasets-root-relative on-disk path of the corrupted fixture file, e.g.
    /// `corruption/test_comp_corrupt/data_db_bit_flip/nb-1-big-Data.db`. The
    /// walked inventory keys this with a `test-data/datasets/` prefix the
    /// manifest omits, so presence is matched on a path boundary
    /// ([`inventory_contains_fixture`]).
    pub corrupted_path: String,
    /// `active` (a real corrupted file was produced) or `planned` (declared but
    /// no clean source was available in the checkout, so no file exists).
    pub status: String,
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
    corruption_fixtures: &[CorruptionFixture],
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

    // 6: corruption-fixture coverage of every required component type, verified
    // against the on-disk inventory (not just the manifest declarations).
    findings.extend(check_corruption_coverage(
        corruption_fixtures,
        &inventory.files,
    ));

    AuditReport { findings }
}

/// COVERAGE/PRESENCE audit of the regenerated component inventory against the
/// expected entry set (issue #2009). The `exhaustive_regeneration` tier promises
/// that every manifest-referenced + committed-golden component identity is
/// PRODUCED by regeneration — it is NOT a byte-drift/checksum tier. So:
///   * an expected non-`system*` identity that NO regenerated file reproduces is
///     a finding (the presence check — detail carries the word `absent`);
///   * an expected identity that IS present passes REGARDLESS of its SHA256
///     (byte-parity is owned by the sstabledump-parity-gate + nightly_docker
///     tiers on the committed corpus, not here);
///   * extra produced components are never a finding (the newly-wired generators
///     may produce goldens not in the committed expected set), so there is no
///     "appeared" finding;
///   * `system*` keyspaces are excluded — their contents are inherently
///     run-dependent, so demanding their presence would false-positive.
///
/// Both sides are keyed by their UUID-independent table+component identity
/// ([`refs::component_identity`]), NOT the raw repo-relative path: every
/// regeneration `rm -rf`s the corpus and re-mints each table under a fresh
/// `<table>-<uuid>` directory, so the committed-expected golden and the
/// regenerated-actual golden never share a path. Comparing identities makes the
/// presence check tolerant of that per-run UUID churn (issue #1026).
pub fn check_component_changes(
    inventory: &CorpusInventory,
    expected: &ExpectedInventory,
) -> Vec<AuditFinding> {
    let mut out = Vec::new();
    if expected.components.is_empty() {
        return out;
    }

    // UUID-independent identities the regeneration actually produced. Source this
    // from the WALKED file set (`inventory.files`), which the CLI always
    // populates under `--corpus .` — NOT from `inventory.checksums`, which is
    // optional (`--checksums`). A presence contract must not depend on checksum
    // data: keying off checksums would false-fire every expected component as
    // "absent" whenever `--checksums` is omitted or empty (issue #2009).
    let produced_identities: BTreeSet<String> = inventory
        .files
        .iter()
        .map(|p| refs::component_identity(p))
        .collect();

    for path in expected.components.keys() {
        // `system*` keyspaces are inherently run-dependent; exclude them from the
        // expected inventory entirely (issue #2009).
        if refs::is_system_keyspace_path(path) {
            continue;
        }
        let identity = refs::component_identity(path);
        if !produced_identities.contains(&identity) {
            out.push(AuditFinding::new(
                FindingKind::UnexpectedComponentChange,
                path.clone(),
                "expected component identity is absent from the regenerated corpus".to_string(),
            ));
        }
    }

    out
}

/// Fail when any required corrupted-component type has no ON-DISK corruption
/// fixture. `fixtures` are the corruption fixtures declared in
/// `corruption-manifest.yml` (parsed by the CLI); `inventory` is the set of
/// repo-relative file paths walked from the regenerated corpus.
///
/// For each required component a fixture must both DECLARE it AND have produced a
/// corrupted file present in `inventory` — spec R4 is on-disk reality, not just a
/// manifest declaration, so a fixture that was declared but never produced (e.g.
/// `generate-corruption-corpus.sh` silently produced fewer files, or the fixture
/// is `planned` for lack of a clean source) is a coverage gap (issue #1026).
pub fn check_corruption_coverage(
    fixtures: &[CorruptionFixture],
    inventory: &BTreeSet<String>,
) -> Vec<AuditFinding> {
    let mut out = Vec::new();
    for req in REQUIRED_CORRUPTION_COMPONENTS {
        let declaring: Vec<&CorruptionFixture> =
            fixtures.iter().filter(|f| f.component == *req).collect();
        if declaring.is_empty() {
            out.push(AuditFinding::new(
                FindingKind::CorruptionCoverageGap,
                (*req).to_string(),
                "no corruption fixture declares this required component type".to_string(),
            ));
            continue;
        }
        let on_disk = declaring
            .iter()
            .any(|f| inventory_contains_fixture(inventory, &f.corrupted_path));
        if !on_disk {
            out.push(AuditFinding::new(
                FindingKind::CorruptionCoverageGap,
                (*req).to_string(),
                "corruption fixture(s) declare this required component type but no corrupted \
                 fixture file is present in the regenerated corpus"
                    .to_string(),
            ));
        }
    }
    out
}

/// True when `corrupted_path` (datasets-root-relative, as declared in the
/// corruption manifest, e.g. `corruption/test_comp_corrupt/<name>/<file>`) is
/// present in the walked corpus `inventory` (repo-root-relative, carrying a
/// leading `test-data/datasets/` the manifest path omits). The two are reconciled
/// by a path-boundary suffix match — an exact equality OR an inventory key whose
/// suffix is `/<corrupted_path>` — so the prefix is not hard-coded and a partial
/// path component (e.g. `…/x-Data.db` vs `…/Data.db`) cannot spuriously match.
fn inventory_contains_fixture(inventory: &BTreeSet<String>, corrupted_path: &str) -> bool {
    if corrupted_path.is_empty() {
        return false;
    }
    inventory.iter().any(|p| {
        p == corrupted_path
            || p.strip_suffix(corrupted_path)
                .is_some_and(|prefix| prefix.ends_with('/'))
    })
}
