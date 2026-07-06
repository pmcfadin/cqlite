//! Tests for the corpus audit (issue #1026). Mirrors `tier_contract_tests`:
//! every fixture is an inline string / in-memory struct — no Docker, datasets,
//! live Cassandra, or disk. One drifted fixture per failure class plus a
//! passing-clean fixture.

use std::collections::{BTreeMap, BTreeSet};

use cassandra_parity::corpus_audit::{
    self, CorpusInventory, CorruptionFixture, ExpectedInventory, FindingKind, Provenance,
};
use cassandra_parity::model::Manifest;

const GOOD_SHA: &str = "f278f6774fc76465c182041e081982105c3e7dbb";

/// Repo-relative reference path the clean scenario pins (a committed JSONL golden).
const REF: &str = "test-data/datasets/sstables/test_basic/simple_table-aaaa0000000000000000000000000001/nb-1-big-Data.db.jsonl";

/// An index with one classified + one unclassified high-relevance Java file.
///
/// Emits the detailed per-file section format the coverage parser consumes
/// (issue #1199): each high file is a `#### 🔴 High · \`Name.java\`` header
/// followed by its `- **Path:** \`...\`` line. The basename-only "quick list"
/// table is no longer parsed, so fixtures must use this layout.
fn index_text(include_unclassified: bool) -> String {
    let mut s = String::from(
        "# Cassandra test index\n\n## Detailed high-relevance tests\n\n\
         #### 🔴 High · `SortedTableWriterTest.java`\n\
         - **Path:** `test/unit/org/apache/cassandra/io/sstable/format/SortedTableWriterTest.java`\n",
    );
    if include_unclassified {
        s.push_str(
            "#### 🔴 High · `RogueUnclassifiedTest.java`\n\
             - **Path:** `test/unit/org/apache/cassandra/db/RogueUnclassifiedTest.java`\n",
        );
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

/// One corruption fixture per required component, each with a distinct
/// datasets-relative `corrupted_path` whose on-disk (repo-relative) entry is
/// supplied by [`corruption_inventory_files`]. Mirrors the real
/// `corruption-manifest.yml` layout (`corruption/test_comp_corrupt/<name>/<file>`).
fn all_corruption_fixtures() -> Vec<CorruptionFixture> {
    corpus_audit::REQUIRED_CORRUPTION_COMPONENTS
        .iter()
        .map(|c| CorruptionFixture {
            component: c.to_string(),
            corrupted_path: format!("corruption/test_comp_corrupt/{c}_fixture/nb-1-big-{c}"),
            status: "active".to_string(),
        })
        .collect()
}

/// Repo-relative ON-DISK inventory entries for [`all_corruption_fixtures`] (the
/// walk keys carry the `test-data/datasets/` prefix the manifest path omits).
fn corruption_inventory_files() -> BTreeSet<String> {
    all_corruption_fixtures()
        .iter()
        .map(|f| format!("test-data/datasets/{}", f.corrupted_path))
        .collect()
}

/// Inventory that always carries the on-disk corruption fixtures (so corruption
/// coverage passes) plus any extra files supplied.
fn inventory_with(extra: &[&str]) -> CorpusInventory {
    let mut files = corruption_inventory_files();
    for e in extra {
        files.insert((*e).to_string());
    }
    CorpusInventory {
        files,
        checksums: BTreeMap::new(),
    }
}

/// Inventory containing the referenced golden plus the on-disk corruption corpus.
fn clean_inventory() -> CorpusInventory {
    inventory_with(&[REF])
}

#[test]
fn passing_clean_corpus_audit() {
    let report = corpus_audit::audit(
        &manifest(),
        &index_text(false),
        &clean_inventory(),
        &ExpectedInventory::default(),
        Some(&good_provenance()),
        &all_corruption_fixtures(),
    );
    assert!(report.ok(), "expected clean pass, got: {}", report.render());
}

#[test]
fn missing_reference_fails_and_names_offender() {
    // Reference absent and no same-table component anywhere -> missing. The
    // on-disk corruption corpus is present so ONLY the missing reference fires.
    let report = corpus_audit::audit(
        &manifest(),
        &index_text(false),
        &inventory_with(&[]),
        &ExpectedInventory::default(),
        Some(&good_provenance()),
        &all_corruption_fixtures(),
    );
    assert!(!report.ok());
    assert_eq!(report.count(FindingKind::MissingReference), 1);
    assert_eq!(report.count(FindingKind::CorruptionCoverageGap), 0);
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
    let inv = inventory_with(&[regenerated.as_str()]);
    let report = corpus_audit::audit(
        &manifest(),
        &index_text(false),
        &inv,
        &ExpectedInventory::default(),
        Some(&good_provenance()),
        &all_corruption_fixtures(),
    );
    assert!(
        report.ok(),
        "UUID churn (same table+component, new uuid dir) must be clean, got: {}",
        report.render()
    );
    assert_eq!(report.count(FindingKind::StaleReference), 0);
    assert_eq!(report.count(FindingKind::MissingReference), 0);
}

/// Issue #2009: a fresh regeneration flushes/compacts to a DIFFERENT SSTable
/// generation than the committed corpus (e.g. committed `nb-1-big`, regenerated
/// `nb-2-big`). Identity is generation-independent, so a reference whose only
/// difference is the generation number is NOT missing — the corpus still produces
/// that table+component. Before the fix this fired MISSING-REFERENCE on every
/// core keyspace (84+ findings), keeping the lane red.
#[test]
fn generation_churn_reference_is_clean() {
    // Same table + component, but a different generation number in the basename.
    let regenerated = REF.replace("nb-1-big-", "nb-2-big-");
    let inv = inventory_with(&[regenerated.as_str()]);
    let report = corpus_audit::audit(
        &manifest(),
        &index_text(false),
        &inv,
        &ExpectedInventory::default(),
        Some(&good_provenance()),
        &all_corruption_fixtures(),
    );
    assert!(
        report.ok(),
        "generation churn (same table+component, new generation) must be clean, got: {}",
        report.render()
    );
    assert_eq!(report.count(FindingKind::MissingReference), 0);
}

/// Issue #2009: a manifest reference into a `system*` keyspace is EXCLUDED from
/// the missing-reference check, consistently with the expected-inventory
/// exclusion — a system keyspace's tables/generations are inherently run- and
/// Cassandra-version-dependent (e.g. `system_schema.column_masks` only exists on
/// newer versions), so a reference pinned to one is not a coverage guarantee this
/// tier makes. The regeneration NOT producing it must be clean.
#[test]
fn system_keyspace_reference_is_excluded_from_missing_check() {
    let sys_ref = "test-data/datasets/sstables/system_schema/column_masks-738cc5ed01683268b9d1853d4bc278af/nb-45-big-Statistics.db.txt";
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
  - id: cass.repair.system_schema_ref
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
        - {sys_ref}
    ci:
      tier: exhaustive_regeneration
"#
    );
    let manifest = Manifest::from_yaml(&yaml).expect("fixture manifest parses");
    // Inventory does NOT contain the system_schema component at all.
    let findings = corpus_audit::refs::check_references(&manifest, &inventory_with(&[]));
    assert!(
        findings.is_empty(),
        "a system* keyspace reference must not fire MISSING-REFERENCE, got: {findings:?}"
    );
}

#[test]
fn unclassified_high_relevance_fails_and_names_offender() {
    let report = corpus_audit::audit(
        &manifest(),
        &index_text(true), // adds RogueUnclassifiedTest.java
        &clean_inventory(),
        &ExpectedInventory::default(),
        Some(&good_provenance()),
        &all_corruption_fixtures(),
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

/// Issue #2009: the `exhaustive_regeneration` tier is a COVERAGE/PRESENCE audit,
/// NOT a byte-drift/checksum tier. A checksum drift of a STABLE identity that is
/// still PRESENT in the regenerated corpus (even under a churned UUID directory)
/// must produce ZERO findings — presence alone passes. Byte-parity is owned by
/// the sstabledump-parity-gate + nightly_docker tiers on the committed corpus.
#[test]
fn unexpected_component_change_does_not_fire_on_checksum_drift_across_uuid_churn() {
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
    assert!(
        findings.is_empty(),
        "a PRESENT identity must pass regardless of SHA256 (coverage tier), got: {findings:?}"
    );
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
    // The regenerated corpus DOES produce a sibling component in the same table;
    // presence is by component identity (table+basename), so the sibling must NOT
    // satisfy REF's own identity — REF is genuinely absent.
    inv.files.insert(other.clone());
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

/// Issue #2009 (contract item: no "appeared" finding): a component the
/// regeneration produced that the expected inventory does NOT track — even inside
/// an already-tracked table — is NEVER a finding under the coverage contract
/// (the newly-wired generators may emit goldens absent from the committed set).
#[test]
fn extra_produced_component_never_fires() {
    let mut expected = BTreeMap::new();
    expected.insert(REF.to_string(), "some_sha".to_string());

    // Regenerated corpus has REF (present -> passes) PLUS an extra sibling the
    // expected set does not track.
    let extra = REF.replace("Data.db.jsonl", "Index.db.jsonl");
    let mut inv = CorpusInventory::default();
    inv.files.insert(REF.to_string());
    inv.files.insert(extra);

    let findings = corpus_audit::check_component_changes(
        &inv,
        &ExpectedInventory {
            components: expected,
        },
    );
    assert!(
        findings.is_empty(),
        "an extra produced component must never fire (no 'appeared' finding), got: {findings:?}"
    );
}

/// Issue #2009 (full-audit path): a present component identity whose SHA256
/// drifted must NOT fire under the COVERAGE/PRESENCE contract — the whole audit
/// stays clean (byte-drift is not this tier's job).
#[test]
fn unexpected_component_change_does_not_fire_on_checksum_drift() {
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
        &all_corruption_fixtures(),
    );
    assert!(
        report.ok(),
        "a present identity with a drifted SHA256 must pass the coverage tier, got: {}",
        report.render()
    );
    assert_eq!(report.count(FindingKind::UnexpectedComponentChange), 0);
}

/// Issue #2009: `system*` keyspaces are excluded from the expected inventory
/// (their on-disk contents are inherently run-dependent). An ABSENT expected
/// component under `system` or `system_schema` produces ZERO findings, while an
/// ABSENT non-system component still fires — proving the exclusion is scoped.
#[test]
fn system_keyspace_components_are_excluded_from_presence_check() {
    let system = "test-data/datasets/sstables/system/local-1234/nb-1-big-Statistics.db".to_string();
    let system_schema =
        "test-data/datasets/sstables/system_schema/tables-5678/nb-1-big-Statistics.db".to_string();
    let non_system =
        "test-data/datasets/sstables/test_basic/simple_table-abcd/nb-1-big-Statistics.db"
            .to_string();

    let mut expected = BTreeMap::new();
    expected.insert(system, "sha_a".to_string());
    expected.insert(system_schema, "sha_b".to_string());
    expected.insert(non_system.clone(), "sha_c".to_string());

    // Regenerated corpus reproduces NONE of them (empty checksums).
    let inv = CorpusInventory::default();

    let findings = corpus_audit::check_component_changes(
        &inv,
        &ExpectedInventory {
            components: expected,
        },
    );
    assert_eq!(
        findings.len(),
        1,
        "only the absent NON-system component should fire, got: {findings:?}"
    );
    assert_eq!(findings[0].kind, FindingKind::UnexpectedComponentChange);
    assert!(
        findings[0].subject.contains(&non_system) && findings[0].detail.contains("absent"),
        "the single finding must name the non-system absent component, got: {findings:?}"
    );
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
        &all_corruption_fixtures(),
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
    // Summary.db is declared by NO fixture -> an undeclared coverage gap, even
    // though its file happens to be on disk (clean_inventory carries the corpus).
    let fixtures: Vec<CorruptionFixture> = all_corruption_fixtures()
        .into_iter()
        .filter(|f| f.component != "Summary.db")
        .collect();
    let report = corpus_audit::audit(
        &manifest(),
        &index_text(false),
        &clean_inventory(),
        &ExpectedInventory::default(),
        Some(&good_provenance()),
        &fixtures,
    );
    assert!(!report.ok());
    assert_eq!(report.count(FindingKind::CorruptionCoverageGap), 1);
    assert!(report.render().contains("Summary.db"));
    assert!(
        report.render().contains("no corruption fixture declares"),
        "got: {}",
        report.render()
    );
}

/// Regression for issue #1026 (roborev LOW 2): spec R4 requires an ON-DISK
/// corruption fixture per required component, not merely a manifest declaration.
/// A fixture that DECLARES Summary.db but whose corrupted file is ABSENT from the
/// regenerated corpus must fail, naming Summary.db — so a generator that silently
/// produced fewer files than declared cannot pass the audit. Before the fix the
/// audit only checked the manifest declaration and would have passed.
#[test]
fn corruption_coverage_gap_fails_when_declared_fixture_absent_on_disk() {
    let fixtures = all_corruption_fixtures();
    // On-disk inventory has every corruption fixture EXCEPT Summary.db's file.
    let summary = all_corruption_fixtures()
        .into_iter()
        .find(|f| f.component == "Summary.db")
        .expect("Summary.db fixture");
    let mut inventory = corruption_inventory_files();
    inventory.remove(&format!("test-data/datasets/{}", summary.corrupted_path));

    let findings = corpus_audit::check_corruption_coverage(&fixtures, &inventory);
    assert_eq!(findings.len(), 1, "got: {findings:?}");
    assert_eq!(findings[0].kind, FindingKind::CorruptionCoverageGap);
    assert!(findings[0].subject.contains("Summary.db"));
    assert!(
        findings[0]
            .detail
            .contains("no corrupted fixture file is present"),
        "got: {}",
        findings[0].detail
    );
}

#[test]
fn corruption_coverage_clean_when_all_present_on_disk() {
    let findings = corpus_audit::check_corruption_coverage(
        &all_corruption_fixtures(),
        &corruption_inventory_files(),
    );
    assert!(findings.is_empty(), "got: {findings:?}");
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
