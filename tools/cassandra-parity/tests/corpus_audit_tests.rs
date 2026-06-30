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

/// Regression for issue #1026 (HIGH, roborev): the regeneration lane `rm -rf`s
/// the corpus and re-mints every table under a FRESH `<table>-<uuid>` directory,
/// so the committed manifest reference (pinned to the OLD uuid) and the
/// regenerated golden NEVER share a repo-relative path. A reference whose
/// UUID-independent `(table_key, basename)` identity IS produced under the new
/// uuid dir is NOT stale/missing — the corpus still produces that exact
/// table+component — so the audit MUST be clean (zero reference findings).
/// Before the fix this fired a hard-fail STALE-REFERENCE on every run, leaving
/// the owner-pinned lane perpetually red.
#[test]
fn churned_reference_under_new_uuid_is_clean() {
    // Same table + component, but under a NEW generation UUID dir -> churn, not stale.
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
    assert!(
        report.ok(),
        "UUID churn (same table+component, new uuid dir) must be clean, got: {}",
        report.render()
    );
    assert_eq!(report.count(FindingKind::StaleReference), 0);
    assert_eq!(report.count(FindingKind::MissingReference), 0);
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
        .any(|f| f.kind == FindingKind::UnexpectedComponentChange && f.detail.contains("absent")));
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

/// Build a provenance record the way the CI heredoc does: version/ref/sha are
/// all derived from the manifest (`cassandra_source.ref`/`.sha`, version from the
/// ref), and ONLY `docker_image` is sourced independently (grepped from
/// `regenerate-datasets.sh`'s `CASSANDRA_IMAGE=`). This mirrors the real lane so
/// the docker_image check is exercised on the path that actually runs in CI.
fn manifest_derived_provenance(docker_image: &str) -> Provenance {
    // Manifest pins ref `cassandra-5.0.2` + GOOD_SHA; version is `ref` minus the
    // `cassandra-` prefix, exactly like the workflow's Python derivation.
    Provenance {
        cassandra_version: "5.0.2".to_string(),
        cassandra_ref: "cassandra-5.0.2".to_string(),
        cassandra_git_sha: GOOD_SHA.to_string(),
        docker_image: docker_image.to_string(),
        generator_commands: vec!["bash test-data/scripts/regenerate-datasets.sh".to_string()],
        dataset_asset_name: "dataset-asset.tar.gz".to_string(),
        dataset_asset_sha256: "deadbeef".to_string(),
    }
}

/// Regression for issue #1026 (roborev MEDIUM): in the lane, cassandra_version/
/// ref/sha are all parsed FROM the manifest, so validating them against the
/// manifest is tautological and can never fail. The one independently-sourced
/// field — `docker_image` (from `regenerate-datasets.sh`'s `CASSANDRA_IMAGE=`) —
/// is what catches a silent image bump. Bumping the image to `5.0.3` WITHOUT
/// updating the manifest pin (still `cassandra-5.0.2`) MUST now hard-fail with a
/// ProvenanceMismatch that names the image; before the fix this passed clean.
#[test]
fn provenance_mismatch_fails_on_divergent_docker_image() {
    let prov = manifest_derived_provenance("cassandra:5.0.3");
    let findings = corpus_audit::provenance::check_provenance(&prov, &manifest());
    assert_eq!(
        findings.len(),
        1,
        "only the divergent docker_image should fire, got: {findings:?}"
    );
    assert_eq!(findings[0].kind, FindingKind::ProvenanceMismatch);
    assert!(
        findings[0].subject.contains("cassandra:5.0.3"),
        "finding must name the offending image, got: {findings:?}"
    );
}

/// The matching-image clean case on the same manifest-derived CI path: when the
/// image tag agrees with the manifest pin, no provenance finding fires.
#[test]
fn provenance_clean_on_matching_docker_image() {
    let prov = manifest_derived_provenance("cassandra:5.0.2");
    let findings = corpus_audit::provenance::check_provenance(&prov, &manifest());
    assert!(findings.is_empty(), "got: {findings:?}");
}

/// An unverifiable image tag (`latest`, or any non-semver) is itself a mismatch:
/// it cannot be checked against the manifest pin, so it must not be trusted.
#[test]
fn provenance_mismatch_fails_on_unpinned_latest_image() {
    let prov = manifest_derived_provenance("cassandra:latest");
    let findings = corpus_audit::provenance::check_provenance(&prov, &manifest());
    assert_eq!(findings.len(), 1, "got: {findings:?}");
    assert_eq!(findings[0].kind, FindingKind::ProvenanceMismatch);
    assert!(findings[0].subject.contains("cassandra:latest"));
}

/// Regression for issue #1026 (roborev LOW 1): a legitimately pinned VARIANT
/// image carries a `-<suffix>` build/variant tail (`-jdk11`, `-jammy`). Its
/// numeric lead still matches the manifest pin (`5.0.2`), so the audit must be
/// clean — the variant tail must NOT be treated as a non-semver tag and red the
/// lane. Before the fix `is_semver_tag` required the WHOLE tag to be digits+dots,
/// so any future variant pin spuriously hard-failed PROVENANCE-MISMATCH.
#[test]
fn provenance_clean_on_variant_docker_image() {
    for image in ["cassandra:5.0.2-jdk11", "cassandra:5.0.2-jammy"] {
        let prov = manifest_derived_provenance(image);
        let findings = corpus_audit::provenance::check_provenance(&prov, &manifest());
        assert!(
            findings.is_empty(),
            "variant image {image} (numeric lead 5.0.2) must match the manifest pin, got: {findings:?}"
        );
    }
}

/// A divergent VARIANT image is still caught: a `5.0.3-jdk11` build whose numeric
/// lead (`5.0.3`) is not the manifest pin (`5.0.2`) must hard-fail just like a
/// bare `5.0.3` — the suffix does not launder a silent image bump.
#[test]
fn provenance_mismatch_fails_on_divergent_variant_docker_image() {
    let prov = manifest_derived_provenance("cassandra:5.0.3-jdk11");
    let findings = corpus_audit::provenance::check_provenance(&prov, &manifest());
    assert_eq!(findings.len(), 1, "got: {findings:?}");
    assert_eq!(findings[0].kind, FindingKind::ProvenanceMismatch);
    assert!(findings[0].subject.contains("cassandra:5.0.3-jdk11"));
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
