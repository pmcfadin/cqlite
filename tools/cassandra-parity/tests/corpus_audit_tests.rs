//! Tests for the corpus audit (issue #1026). Mirrors `tier_contract_tests`:
//! every fixture is an inline string / in-memory struct — no Docker, datasets,
//! live Cassandra, or disk. One drifted fixture per failure class plus a
//! passing-clean fixture.

use std::collections::{BTreeMap, BTreeSet};

use cassandra_parity::corpus_audit::{
    self, CorpusInventory, ExpectedInventory, FindingKind, Provenance,
};
use cassandra_parity::model::Manifest;

const GOOD_SHA: &str = "f278f6774fc76465c182041e081982105c3e7dbb";

/// Repo-relative reference path the clean scenario pins (a committed JSONL golden).
const REF: &str = "test-data/datasets/sstables/test_basic/simple_table-aaaa0000000000000000000000000001/nb-1-big-Data.db.jsonl";

/// An index with one classified + one unclassified high-relevance Java file.
fn index_text(include_unclassified: bool) -> String {
    let mut s = String::from(
        "# Cassandra test index\n\n## High-relevance tests (quick list)\n\n\
         | Test | Notes |\n|------|-------|\n| `SortedTableWriterTest.java` | classified |\n",
    );
    if include_unclassified {
        s.push_str("| `RogueUnclassifiedTest.java` | not in manifest |\n");
    }
    s.push_str("\n## Other section\n");
    s
}

/// Build a manifest whose single scenario references `REF` and classifies
/// `SortedTableWriterTest.java`.
fn manifest() -> Manifest {
    let yaml = format!(
        r#"manifest_version: 1
cassandra_source:
  repo: https://github.com/apache/cassandra
  ref: cassandra-5.0.2
  sha: {GOOD_SHA}
  index: docs/cassandra_test_index.md
  assessment_report: docs/reports/x.md
program:
  parent_epic: 966
  reporting_epic: 967
scenarios:
  - id: cass.sstable_format.simple
    title: t
    status: mirrored
    capability: sstable_format
    priority: P0
    risk: p0_data_loss
    cassandra:
      category: sstable_format
      relevance: high
      files:
        - SortedTableWriterTest.java
    cqlite: {{}}
    evidence:
      type: byte_for_byte
      cassandra_version: "5.0.2"
      cassandra_git_sha: {GOOD_SHA}
      reference_paths:
        - {REF}
    ci:
      tier: exhaustive_regeneration
"#
    );
    Manifest::from_yaml(&yaml).expect("fixture manifest parses")
}

fn good_provenance() -> Provenance {
    Provenance {
        cassandra_version: "5.0.2".to_string(),
        cassandra_ref: "cassandra-5.0.2".to_string(),
        cassandra_git_sha: GOOD_SHA.to_string(),
        docker_image: "cassandra:5.0.2".to_string(),
        generator_commands: vec!["bash test-data/scripts/regenerate-datasets.sh".to_string()],
        dataset_asset_name: "cassandra5-small-full.tar.gz".to_string(),
        dataset_asset_sha256: "deadbeef".to_string(),
    }
}

fn all_corruption_components() -> BTreeSet<String> {
    corpus_audit::REQUIRED_CORRUPTION_COMPONENTS
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// Inventory containing exactly the referenced golden.
fn clean_inventory() -> CorpusInventory {
    let mut files = BTreeSet::new();
    files.insert(REF.to_string());
    CorpusInventory {
        files,
        checksums: BTreeMap::new(),
    }
}

#[test]
fn passing_clean_corpus_audit() {
    let report = corpus_audit::audit(
        &manifest(),
        &index_text(false),
        &clean_inventory(),
        &ExpectedInventory::default(),
        Some(&good_provenance()),
        &all_corruption_components(),
    );
    assert!(report.ok(), "expected clean pass, got: {}", report.render());
}

#[test]
fn missing_reference_fails_and_names_offender() {
    // Reference absent and no same-table component anywhere -> missing.
    let report = corpus_audit::audit(
        &manifest(),
        &index_text(false),
        &CorpusInventory::default(),
        &ExpectedInventory::default(),
        Some(&good_provenance()),
        &all_corruption_components(),
    );
    assert!(!report.ok());
    assert_eq!(report.count(FindingKind::MissingReference), 1);
    assert!(report.render().contains(REF), "got: {}", report.render());
}

#[test]
fn stale_reference_fails_and_names_offender() {
    // Same table + component, but under a NEW generation UUID dir -> stale.
    let regenerated = REF.replace(
        "simple_table-aaaa0000000000000000000000000001",
        "simple_table-bbbb0000000000000000000000000002",
    );
    let mut files = BTreeSet::new();
    files.insert(regenerated);
    let inv = CorpusInventory {
        files,
        checksums: BTreeMap::new(),
    };
    let report = corpus_audit::audit(
        &manifest(),
        &index_text(false),
        &inv,
        &ExpectedInventory::default(),
        Some(&good_provenance()),
        &all_corruption_components(),
    );
    assert!(!report.ok());
    assert_eq!(report.count(FindingKind::StaleReference), 1);
    assert_eq!(report.count(FindingKind::MissingReference), 0);
    assert!(report.render().contains(REF));
}

#[test]
fn unclassified_high_relevance_fails_and_names_offender() {
    let report = corpus_audit::audit(
        &manifest(),
        &index_text(true), // adds RogueUnclassifiedTest.java
        &clean_inventory(),
        &ExpectedInventory::default(),
        Some(&good_provenance()),
        &all_corruption_components(),
    );
    assert!(!report.ok());
    assert_eq!(report.count(FindingKind::UnclassifiedHighRelevance), 1);
    assert!(
        report.render().contains("RogueUnclassifiedTest.java"),
        "got: {}",
        report.render()
    );
}

/// Regression for issue #1026 (Finding 1): every regeneration mints a fresh
/// `<table>-<uuid>` directory, so the committed golden (expected, under uuidA)
/// and the regenerated golden (actual, under uuidB) NEVER share a repo-relative
/// path. The component-change check MUST normalize both sides by their
/// UUID-independent table+component identity, so identical bytes under a churned
/// UUID directory raise NO finding (otherwise the lane is perpetually red).
#[test]
fn uuid_churn_alone_does_not_fire_unexpected_component_change() {
    let regenerated = REF.replace(
        "simple_table-aaaa0000000000000000000000000001",
        "simple_table-bbbb0000000000000000000000000002",
    );
    let mut expected = BTreeMap::new();
    expected.insert(REF.to_string(), "same_sha".to_string());

    let mut inv = CorpusInventory::default();
    inv.files.insert(regenerated.clone());
    inv.checksums.insert(regenerated, "same_sha".to_string());

    let findings = corpus_audit::check_component_changes(
        &inv,
        &ExpectedInventory {
            components: expected,
        },
    );
    assert!(
        findings.is_empty(),
        "UUID churn alone must NOT fire UnexpectedComponentChange, got: {findings:?}"
    );
}

/// A genuine checksum drift of a STABLE identity (same table+component) is still
/// caught even though the regeneration churned the UUID directory.
#[test]
fn unexpected_component_change_fires_on_checksum_drift_across_uuid_churn() {
    let regenerated = REF.replace(
        "simple_table-aaaa0000000000000000000000000001",
        "simple_table-bbbb0000000000000000000000000002",
    );
    let mut expected = BTreeMap::new();
    expected.insert(REF.to_string(), "old_sha".to_string());

    let mut inv = CorpusInventory::default();
    inv.files.insert(regenerated.clone());
    inv.checksums.insert(regenerated, "new_sha".to_string());

    let findings = corpus_audit::check_component_changes(
        &inv,
        &ExpectedInventory {
            components: expected,
        },
    );
    assert_eq!(
        findings.len(),
        1,
        "drift under a stable identity must fire exactly one finding, got: {findings:?}"
    );
    assert_eq!(findings[0].kind, FindingKind::UnexpectedComponentChange);
}

/// A genuinely removed component (expected identity has no regenerated match at
/// all) is caught.
#[test]
fn unexpected_component_change_fires_on_removed_component() {
    let mut expected = BTreeMap::new();
    expected.insert(REF.to_string(), "some_sha".to_string());

    // Regenerated corpus has a DIFFERENT component (different basename) under the
    // same table — the expected component itself is gone.
    let other = REF.replace("Data.db.jsonl", "Index.db.jsonl");
    let mut inv = CorpusInventory::default();
    inv.checksums.insert(other, "x".to_string());

    let findings = corpus_audit::check_component_changes(
        &inv,
        &ExpectedInventory {
            components: expected,
        },
    );
    assert!(findings
        .iter()
        .any(|f| f.kind == FindingKind::UnexpectedComponentChange
            && f.detail.contains("absent")));
}

#[test]
fn unexpected_component_change_fails_on_checksum_drift() {
    let comp = REF.to_string();
    let mut expected = BTreeMap::new();
    expected.insert(comp.clone(), "expected_sha".to_string());
    let mut inv = clean_inventory();
    inv.checksums
        .insert(comp.clone(), "regenerated_sha".to_string());

    let report = corpus_audit::audit(
        &manifest(),
        &index_text(false),
        &inv,
        &ExpectedInventory {
            components: expected,
        },
        Some(&good_provenance()),
        &all_corruption_components(),
    );
    assert!(!report.ok());
    assert_eq!(report.count(FindingKind::UnexpectedComponentChange), 1);
    assert!(report.render().contains(&comp));
}

#[test]
fn provenance_mismatch_fails_on_undeclared_version() {
    let mut prov = good_provenance();
    prov.cassandra_git_sha = "0000000000000000000000000000000000000000".to_string();
    prov.cassandra_version = "6.6.6".to_string();
    let report = corpus_audit::audit(
        &manifest(),
        &index_text(false),
        &clean_inventory(),
        &ExpectedInventory::default(),
        Some(&prov),
        &all_corruption_components(),
    );
    assert!(!report.ok());
    assert!(report.count(FindingKind::ProvenanceMismatch) >= 1);
    assert!(
        report.render().contains("6.6.6"),
        "got: {}",
        report.render()
    );
}

#[test]
fn corruption_coverage_gap_fails_and_names_missing_component() {
    let mut components = all_corruption_components();
    components.remove("Summary.db");
    let report = corpus_audit::audit(
        &manifest(),
        &index_text(false),
        &clean_inventory(),
        &ExpectedInventory::default(),
        Some(&good_provenance()),
        &components,
    );
    assert!(!report.ok());
    assert_eq!(report.count(FindingKind::CorruptionCoverageGap), 1);
    assert!(report.render().contains("Summary.db"));
}

#[test]
fn corruption_coverage_clean_when_all_present() {
    let findings = corpus_audit::check_corruption_coverage(&all_corruption_components());
    assert!(findings.is_empty());
}

#[test]
fn provenance_round_trips_from_json() {
    let json = r#"{
      "cassandra_version": "5.0.2",
      "cassandra_ref": "cassandra-5.0.2",
      "cassandra_git_sha": "f278f6774fc76465c182041e081982105c3e7dbb",
      "docker_image": "cassandra:5.0.2",
      "generator_commands": ["bash test-data/scripts/regenerate-datasets.sh"],
      "dataset_asset_name": "cassandra5-small-full.tar.gz",
      "dataset_asset_sha256": "deadbeef"
    }"#;
    let prov = Provenance::from_json(json).expect("provenance parses");
    assert_eq!(prov.cassandra_ref, "cassandra-5.0.2");
    assert_eq!(prov.dataset_asset_name, "cassandra5-small-full.tar.gz");
    // Matches the fixture manifest pin -> no provenance findings.
    let findings = corpus_audit::provenance::check_provenance(&prov, &manifest());
    assert!(findings.is_empty(), "got: {findings:?}");
}
