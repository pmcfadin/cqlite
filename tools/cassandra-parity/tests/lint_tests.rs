//! Linter tests: one valid manifest and several invalid manifests, plus an
//! end-to-end lint of the real checked-in manifest and a schema/enum sync check.

use std::path::PathBuf;

use cassandra_parity::lint::{lint, Level};
use cassandra_parity::model::Manifest;
use cassandra_parity::{coverage, enums, report};

fn wrap(scenarios: &str) -> String {
    format!(
        "manifest_version: 1
cassandra_source:
  repo: https://github.com/apache/cassandra
  ref: cassandra-5.0.2
  sha: f278f6774fc76465c182041e081982105c3e7dbb
  index: docs/cassandra_test_index.md
  assessment_report: docs/reports/cassandra-test-parity-assessment.md
program:
  parent_epic: 966
  reporting_epic: 967
scenarios:
{scenarios}"
    )
}

/// A single valid mirrored scenario (smoke evidence, no path checks needed).
const VALID_SCENARIO: &str = r#"  - id: cass.sstable_format.example_mirrored
    title: Example mirrored scenario
    status: mirrored
    capability: sstable_format
    priority: P0
    risk: p1_correctness
    cassandra:
      category: sstable-format
      relevance: high
      files: [DescriptorTest.java]
    cqlite:
      coverage:
        suite: sstable_parity_component_manifest
        tests: [cqlite-core/tests/foo.rs]
    fixtures: {}
    evidence:
      type: smoke
      known_limitations: parse/load only, not byte parity
      cassandra_version: "5.0.2"
      cassandra_git_sha: f278f6774fc76465c182041e081982105c3e7dbb
      storage_format_version: [nb]
      fixture_generation_command: bash regen.sh
    ci:
      tier: fast_pr
    scope: {}
"#;

fn errors(yaml: &str) -> Vec<String> {
    let m = Manifest::from_yaml(yaml).expect("manifest should parse");
    lint(&m, None)
        .into_iter()
        .filter(|f| f.level == Level::Error)
        .map(|f| format!("[{}] {}: {}", f.id, f.field, f.message))
        .collect()
}

#[test]
fn valid_manifest_passes() {
    let errs = errors(&wrap(VALID_SCENARIO));
    assert!(errs.is_empty(), "expected no errors, got: {errs:#?}");
}

#[test]
fn invalid_enum_value_is_rejected() {
    let yaml = wrap(&VALID_SCENARIO.replace(
        "capability: sstable_format",
        "capability: not_a_real_capability",
    ));
    let errs = errors(&yaml);
    assert!(
        errs.iter().any(|e| e.contains("capability")),
        "got: {errs:#?}"
    );
}

#[test]
fn duplicate_ids_are_rejected() {
    let yaml = wrap(&format!("{VALID_SCENARIO}{VALID_SCENARIO}"));
    let errs = errors(&yaml);
    assert!(
        errs.iter().any(|e| e.contains("duplicate scenario id")),
        "got: {errs:#?}"
    );
}

#[test]
fn mirrored_without_test_or_reference_is_rejected() {
    let scenario = VALID_SCENARIO.replace("        tests: [cqlite-core/tests/foo.rs]\n", "");
    let errs = errors(&wrap(&scenario));
    assert!(
        errs.iter()
            .any(|e| e.contains("must name a CQLite test target or a fixture reference")),
        "got: {errs:#?}"
    );
}

/// A mirrored `delta_scan` scenario whose only coverage is a real test target
/// (with an existing file path) AND a fixture reference. This is the strict bar
/// the per-shape delta_scan scenarios must clear (issue #995, AC6).
const VALID_DELTA_SCAN_SCENARIO: &str = r#"  - id: cass.delta_scan.cell_tombstones
    title: Delta-scan cell tombstones
    status: mirrored
    capability: delta_scan
    priority: P0
    risk: p1_correctness
    cassandra:
      category: tombstone-ttl
      relevance: high
      files: [SerializationMirrorTest.java]
    cqlite:
      coverage:
        suite: sstable_parity_delta_scan
        tests: [tools/cassandra-parity/src/lint.rs]
        notes: maps to test_delta_parity_cell_tombstones
    fixtures:
      references:
        - test-data/cassandra-parity-manifest.yml
    evidence:
      type: canonical_semantic
      artifacts: [jsonl]
      normalization: scan_delta records mapped to sstabledump JSONL deletion facts
      cassandra_version: "5.0.2"
      cassandra_git_sha: f278f6774fc76465c182041e081982105c3e7dbb
      storage_format_version: [nb]
      fixture_generation_command: bash test-data/scripts/generate-deltas.sh
    ci:
      tier: required_parity
      workflow: .github/workflows/delta-roundtrip.yml
    scope: {}
"#;

fn errors_checked(yaml: &str) -> Vec<String> {
    // Lint with repo_root so referenced paths are resolved against the repo.
    let m = Manifest::from_yaml(yaml).expect("manifest should parse");
    lint(&m, Some(&repo_root()))
        .into_iter()
        .filter(|f| f.level == Level::Error)
        .map(|f| format!("[{}] {}: {}", f.id, f.field, f.message))
        .collect()
}

#[test]
fn delta_scan_mirrored_with_test_and_fixture_passes() {
    let errs = errors_checked(&wrap(VALID_DELTA_SCAN_SCENARIO));
    assert!(errs.is_empty(), "expected no errors, got: {errs:#?}");
}

#[test]
fn delta_scan_mirrored_missing_fixture_reference_is_rejected() {
    // Strip the fixture reference: generic OR rule would still pass (test present),
    // but the delta_scan-specific rule requires BOTH.
    let scenario = VALID_DELTA_SCAN_SCENARIO.replace(
        "    fixtures:\n      references:\n        - test-data/cassandra-parity-manifest.yml\n",
        "    fixtures: {}\n",
    );
    let errs = errors_checked(&wrap(&scenario));
    assert!(
        errs.iter()
            .any(|e| e.contains("cass.delta_scan.cell_tombstones")
                && e.contains("fixtures.references")
                && e.contains("delta_scan")),
        "expected a delta_scan fixture-required error, got: {errs:#?}"
    );
}

#[test]
fn delta_scan_mirrored_missing_test_target_is_rejected() {
    // Strip the test target: generic OR rule would still pass (fixture present),
    // but the delta_scan-specific rule requires BOTH a test AND a fixture.
    let scenario = VALID_DELTA_SCAN_SCENARIO.replace(
        "        tests: [tools/cassandra-parity/src/lint.rs]\n        notes: maps to test_delta_parity_cell_tombstones\n",
        "",
    );
    let errs = errors_checked(&wrap(&scenario));
    assert!(
        errs.iter()
            .any(|e| e.contains("cass.delta_scan.cell_tombstones")
                && e.contains("cqlite.coverage.tests")
                && e.contains("delta_scan")),
        "expected a delta_scan test-required error, got: {errs:#?}"
    );
}

#[test]
fn delta_scan_mirrored_with_nonexistent_test_path_is_rejected() {
    // A test path that does not exist on disk must fail the delta_scan rule
    // (stricter than the generic "names a test" check).
    let scenario = VALID_DELTA_SCAN_SCENARIO.replace(
        "tests: [tools/cassandra-parity/src/lint.rs]",
        "tests: [cqlite-core/tests/does_not_exist_995.rs]",
    );
    let errs = errors_checked(&wrap(&scenario));
    assert!(
        errs.iter()
            .any(|e| e.contains("cass.delta_scan.cell_tombstones")
                && e.contains("delta_scan")
                && e.contains("does not exist")),
        "expected a delta_scan missing-test-file error, got: {errs:#?}"
    );
}

#[test]
fn out_of_scope_missing_required_fields_is_rejected() {
    let scenario = r#"  - id: cass.commitlog_replay.example_oos
    title: Out of scope example
    status: out_of_scope
    capability: sstable_format
    priority: P2
    risk: node_behavior
    cassandra:
      category: commitlog
      relevance: med
      files: [CommitLogTest.java]
    cqlite:
      coverage: {}
    fixtures: {}
    evidence:
      type: out_of_scope
    ci:
      tier: fast_pr
    scope:
      out_of_scope_category: commitlog_replay
"#;
    let errs = errors(&wrap(scenario));
    // rationale, cqlite_boundary, safe_claim, related_in_scope_scenarios all missing
    assert!(
        errs.iter().any(|e| e.contains("scope.rationale")),
        "got: {errs:#?}"
    );
    assert!(
        errs.iter().any(|e| e.contains("scope.cqlite_boundary")),
        "got: {errs:#?}"
    );
    assert!(
        errs.iter()
            .any(|e| e.contains("scope.related_in_scope_scenarios")),
        "got: {errs:#?}"
    );
}

#[test]
fn byte_for_byte_without_evidence_is_rejected() {
    let scenario = VALID_SCENARIO
        .replace("      type: smoke", "      type: byte_for_byte")
        .replace(
            "      known_limitations: parse/load only, not byte parity\n",
            "",
        );
    let errs = errors(&wrap(&scenario));
    assert!(
        errs.iter().any(|e| e.contains("strict: true")),
        "got: {errs:#?}"
    );
    assert!(
        errs.iter().any(|e| e.contains("reference_paths")),
        "got: {errs:#?}"
    );
    assert!(
        errs.iter().any(|e| e.contains("failure_artifacts")),
        "got: {errs:#?}"
    );
}

#[test]
fn missing_evidence_metadata_is_rejected() {
    let scenario = VALID_SCENARIO.replace("      cassandra_version: \"5.0.2\"\n", "");
    let errs = errors(&wrap(&scenario));
    assert!(
        errs.iter()
            .any(|e| e.contains("evidence.cassandra_version")),
        "got: {errs:#?}"
    );
}

#[test]
fn smoke_p0_data_loss_without_gap_is_rejected() {
    let scenario = VALID_SCENARIO.replace("risk: p1_correctness", "risk: p0_data_loss");
    let errs = errors(&wrap(&scenario));
    assert!(
        errs.iter()
            .any(|e| e.contains("P0 data-loss scenario without an explicit scope.gap")),
        "got: {errs:#?}"
    );
}

#[test]
fn partial_without_gap_and_next_step_is_rejected() {
    let scenario = VALID_SCENARIO
        .replace("      type: smoke", "      type: partial")
        .replace("status: mirrored", "status: partial");
    let errs = errors(&wrap(&scenario));
    assert!(
        errs.iter().any(|e| e.contains("scope.gap")),
        "got: {errs:#?}"
    );
    assert!(
        errs.iter().any(|e| e.contains("scope.next_step")),
        "got: {errs:#?}"
    );
}

#[test]
fn missing_local_reference_path_is_rejected_when_checked() {
    // With repo_root set, a non-existent reference path must be flagged.
    let scenario = VALID_SCENARIO.replace(
        "        tests: [cqlite-core/tests/foo.rs]",
        "        tests: [does/not/exist.rs]",
    );
    let m = Manifest::from_yaml(&wrap(&scenario)).unwrap();
    let tmp = std::env::temp_dir();
    let findings = lint(&m, Some(&tmp));
    assert!(
        findings
            .iter()
            .any(|f| f.message.contains("does not exist")),
        "expected missing-path error"
    );
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn real_manifest_lints_clean() {
    let root = repo_root();
    let path = root.join("test-data/cassandra-parity-manifest.yml");
    let text = std::fs::read_to_string(&path).expect("real manifest exists");
    let m = Manifest::from_yaml(&text).expect("real manifest parses");
    let findings = lint(&m, Some(&root));
    let errs: Vec<_> = findings
        .iter()
        .filter(|f| f.level == Level::Error)
        .map(|f| format!("[{}] {}: {}", f.id, f.field, f.message))
        .collect();
    assert!(errs.is_empty(), "real manifest has lint errors: {errs:#?}");
}

#[test]
fn real_report_is_deterministic_and_up_to_date() {
    let root = repo_root();
    let path = root.join("test-data/cassandra-parity-manifest.yml");
    let text = std::fs::read_to_string(&path).unwrap();
    let m = Manifest::from_yaml(&text).unwrap();
    let a = report::render(&m, "test-data/cassandra-parity-manifest.yml");
    let b = report::render(&m, "test-data/cassandra-parity-manifest.yml");
    assert_eq!(a, b, "report render must be deterministic");

    let checked_in = root.join("docs/reports/cassandra-test-parity.md");
    let existing = std::fs::read_to_string(&checked_in).unwrap_or_default();
    assert_eq!(
        existing, a,
        "checked-in report is stale; regenerate with the report subcommand"
    );
}

#[test]
fn schema_enums_match_lint_enums() {
    let root = repo_root();
    let schema_text =
        std::fs::read_to_string(root.join("test-data/cassandra-parity-manifest.schema.json"))
            .unwrap();
    let schema: serde_json::Value = serde_json::from_str(&schema_text).unwrap();
    let props = &schema["$defs"]["scenario"]["properties"];

    let schema_enum = |v: &serde_json::Value| -> Vec<String> {
        v["enum"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap().to_string())
            .collect()
    };
    let expect = |a: &[&str]| -> Vec<String> { a.iter().map(|s| s.to_string()).collect() };

    assert_eq!(schema_enum(&props["status"]), expect(enums::STATUS));
    assert_eq!(schema_enum(&props["capability"]), expect(enums::CAPABILITY));
    assert_eq!(schema_enum(&props["priority"]), expect(enums::PRIORITY));
    assert_eq!(schema_enum(&props["risk"]), expect(enums::RISK));
    assert_eq!(
        schema_enum(&props["cqlite"]["properties"]["coverage"]["properties"]["suite"]),
        expect(enums::SUITE)
    );
    assert_eq!(
        schema_enum(&props["scope"]["properties"]["out_of_scope_category"]),
        expect(enums::OUT_OF_SCOPE_CATEGORY)
    );
}

/// Extract the `test_deltas` table name from a fixture reference path of the
/// form `.../test_deltas/<table>-<uuid>/nb-1-big-Data.db.jsonl`.
fn delta_table_from_ref(path: &str) -> Option<String> {
    let after = path.split("test_deltas/").nth(1)?;
    let dir = after.split('/').next()?;
    let table = dir.rsplit_once('-').map(|(t, _)| t).unwrap_or(dir);
    Some(table.to_string())
}

/// Parse `CREATE TABLE [IF NOT EXISTS] <name>` table names from a CQL schema.
fn cql_table_names(cql: &str) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    let lower = cql.to_lowercase();
    let mut search = 0usize;
    while let Some(rel) = lower[search..].find("create table") {
        let start = search + rel + "create table".len();
        let rest = &cql[start..];
        // Skip "if not exists" and whitespace; the next token is the table name.
        let tokens: Vec<&str> = rest.split_whitespace().collect();
        let mut idx = 0;
        if tokens.first().map(|t| t.eq_ignore_ascii_case("if")) == Some(true) {
            idx = 3; // if not exists
        }
        if let Some(name) = tokens.get(idx) {
            let clean: String = name
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !clean.is_empty() {
                out.insert(clean.to_lowercase());
            }
        }
        search = start;
    }
    out
}

/// AC5 (issue #995): every `delta_scan` scenario's referenced fixture table must
/// correspond to a table that `test-data/schemas/deltas.cql` actually creates
/// (and therefore that `generate-deltas.sh` produces). A scenario cannot claim a
/// delta shape the generator does not emit. Deterministic — no Cassandra/Docker.
#[test]
fn delta_scan_fixtures_map_to_generated_tables() {
    let root = repo_root();
    let m = Manifest::from_yaml(
        &std::fs::read_to_string(root.join("test-data/cassandra-parity-manifest.yml")).unwrap(),
    )
    .unwrap();
    let cql = std::fs::read_to_string(root.join("test-data/schemas/deltas.cql"))
        .expect("deltas.cql exists");
    let known = cql_table_names(&cql);
    assert!(
        !known.is_empty(),
        "deltas.cql yielded no CREATE TABLE names"
    );

    let mut problems = Vec::new();
    for s in m.scenarios.iter().filter(|s| s.capability == "delta_scan") {
        // `planned` scenarios (e.g. the wide-partition corpus gap) intentionally
        // carry no test_deltas fixture and are exempt.
        if s.status == "planned" {
            continue;
        }
        let refs = s
            .fixtures
            .references
            .iter()
            .chain(s.evidence.reference_paths.iter());
        for r in refs {
            if let Some(table) = delta_table_from_ref(r) {
                if !known.contains(&table) {
                    problems.push(format!(
                        "[{}] references test_deltas table '{}' not created by deltas.cql (known: {:?})",
                        s.id, table, known
                    ));
                }
            }
        }
    }
    assert!(
        problems.is_empty(),
        "delta_scan fixture/generator drift:\n{}",
        problems.join("\n")
    );
}

#[test]
fn coverage_finds_high_relevance_files() {
    let root = repo_root();
    let path = root.join("test-data/cassandra-parity-manifest.yml");
    let text = std::fs::read_to_string(&path).unwrap();
    let m = Manifest::from_yaml(&text).unwrap();
    let index = std::fs::read_to_string(root.join(&m.cassandra_source.index)).unwrap();
    let cov = coverage::analyze(&m, &index);
    assert!(
        cov.high_total > 50,
        "expected many high-relevance files, got {}",
        cov.high_total
    );
    assert!(cov.high_classified > 0, "expected some classified files");
}
