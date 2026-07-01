//! Linter tests: one valid manifest and several invalid manifests, plus an
//! end-to-end lint of the real checked-in manifest and a schema/enum sync check.

use std::path::PathBuf;

use cassandra_parity::claim_scan::{scan_docs, ScanInput};
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
      tier: fast_pr
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
fn delta_scan_mirrored_with_nonexistent_fixture_path_is_rejected() {
    // A fixture reference that does not exist on disk must fail the delta_scan
    // rule (mirrors the test-target existence check; AC6 — backed by BOTH a
    // real test AND a real fixture).
    let scenario = VALID_DELTA_SCAN_SCENARIO.replace(
        "        - test-data/cassandra-parity-manifest.yml",
        "        - test-data/does_not_exist_995.jsonl",
    );
    let errs = errors_checked(&wrap(&scenario));
    assert!(
        errs.iter()
            .any(|e| e.contains("cass.delta_scan.cell_tombstones")
                && e.contains("fixtures.references")
                && e.contains("delta_scan")
                && e.contains("does not exist")),
        "expected a delta_scan missing-fixture-file error, got: {errs:#?}"
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

// ----------------------------------------------------------------------------
// Typed failure-artifact descriptors (issue #1027, section 4.2).
// ----------------------------------------------------------------------------

/// A byte_for_byte / required_parity scenario carrying typed failure-artifact
/// descriptors. `sstabledump-parity-gate.yml` is a real workflow that runs the
/// mapped test fail-closed, so the workflow-check passes and the only findings
/// under test are the descriptor rules.
const VALID_BYTE_SCENARIO: &str = r#"  - id: cass.data_db_decode.byte_artifact_example
    title: Byte artifact example scenario
    status: mirrored
    capability: data_db_decode
    priority: P0
    risk: p1_correctness
    cassandra:
      category: sstable-format
      relevance: high
      files: [UnfilteredSerializerTest.java]
    cqlite:
      coverage:
        suite: sstable_parity_index_db_big
        tests: [cqlite-core/tests/sstabledump_parity_index.rs]
    fixtures:
      references:
        - test-data/cassandra-parity-manifest.yml
    evidence:
      type: byte_for_byte
      strict: true
      artifacts: [bytes, offsets, checksums]
      comparison_command: cargo test --test sstabledump_parity_index
      reference_paths:
        - test-data/cassandra-parity-manifest.yml
      failure_artifacts:
        - artifact.required_parity.byte_diff
        - artifact.required_parity.checksum_diff
      cassandra_version: "5.0.2"
      cassandra_git_sha: f278f6774fc76465c182041e081982105c3e7dbb
      storage_format_version: [nb]
      fixture_generation_command: bash regen.sh
    ci:
      tier: required_parity
      workflow: .github/workflows/sstabledump-parity-gate.yml
    scope: {}
"#;

/// Descriptor findings from linting a wrapped scenario with repo_root set (so the
/// workflow-check resolves the real workflow file). Filters to the
/// `evidence.failure_artifacts` field so unrelated findings do not leak in.
fn descriptor_errors(scenario: &str) -> Vec<String> {
    errors_checked(&wrap(scenario))
        .into_iter()
        .filter(|e| e.contains("evidence.failure_artifacts"))
        .collect()
}

#[test]
fn valid_descriptors_pass_lint() {
    // Spec scenario "A valid descriptor passes lint": a byte_for_byte /
    // required_parity scenario declaring artifact.required_parity.byte_diff and
    // artifact.required_parity.checksum_diff is accepted.
    let errs = descriptor_errors(VALID_BYTE_SCENARIO);
    assert!(
        errs.is_empty(),
        "valid descriptors should pass, got: {errs:#?}"
    );
}

#[test]
fn descriptor_tier_mismatch_is_rejected() {
    // Spec scenario "Descriptor tier must match the scenario tier": a
    // required_parity scenario declaring a nightly_docker descriptor fails,
    // reporting the tier mismatch.
    let scenario = VALID_BYTE_SCENARIO.replace(
        "        - artifact.required_parity.byte_diff\n",
        "        - artifact.nightly_docker.live_logs\n",
    );
    let errs = descriptor_errors(&scenario);
    assert!(
        errs.iter().any(
            |e| e.contains("tier 'nightly_docker' must equal") && e.contains("required_parity")
        ),
        "tier mismatch must be reported, got: {errs:#?}"
    );
}

#[test]
fn descriptor_kind_wrong_for_evidence_is_rejected() {
    // Spec scenario "Descriptor kind must match the evidence type": a
    // canonical_semantic scenario declaring a byte_diff descriptor fails.
    let scenario = VALID_BYTE_SCENARIO
        .replace(
            "      type: byte_for_byte",
            "      type: canonical_semantic",
        )
        .replace("      strict: true\n", "")
        // canonical_semantic requires a normalization + jsonl; add them so the
        // ONLY remaining error is the byte_diff-on-canonical_semantic mismatch.
        .replace(
            "      artifacts: [bytes, offsets, checksums]",
            "      artifacts: [jsonl]\n      normalization: JSONL normalized to sstabledump facts",
        )
        .replace("        - artifact.required_parity.checksum_diff\n", "");
    let errs = descriptor_errors(&scenario);
    assert!(
        errs.iter()
            .any(|e| e.contains("byte_diff") && e.contains("canonical_semantic")),
        "byte_diff on canonical_semantic must be rejected, got: {errs:#?}"
    );
}

#[test]
fn malformed_descriptor_is_rejected() {
    // A free-text (non-descriptor) failure artifact must be rejected now that the
    // field is typed.
    let scenario = VALID_BYTE_SCENARIO.replace(
        "        - artifact.required_parity.byte_diff\n",
        "        - target/cassandra-parity/some-diff.log\n",
    );
    let errs = descriptor_errors(&scenario);
    assert!(
        errs.iter()
            .any(|e| e.contains("must be a typed descriptor")),
        "free-text failure artifact must be rejected, got: {errs:#?}"
    );
}

#[test]
fn descriptor_unknown_kind_is_rejected() {
    let scenario = VALID_BYTE_SCENARIO.replace(
        "        - artifact.required_parity.byte_diff\n",
        "        - artifact.required_parity.not_a_kind\n",
    );
    let errs = descriptor_errors(&scenario);
    assert!(
        errs.iter().any(|e| e.contains("unknown kind 'not_a_kind'")),
        "unknown descriptor kind must be rejected, got: {errs:#?}"
    );
}

#[test]
fn descriptor_kinds_match_schema_pattern() {
    // 1027: enums::ARTIFACT_DESCRIPTOR_KIND must equal the <kind> alternation in
    // the manifest schema's failure_artifacts item pattern, so drift between the
    // code enum and the schema cannot slip through (mirrors schema_enums_match).
    let root = repo_root();
    let schema_text =
        std::fs::read_to_string(root.join("test-data/cassandra-parity-manifest.schema.json"))
            .unwrap();
    let schema: serde_json::Value = serde_json::from_str(&schema_text).unwrap();
    let pattern = schema["$defs"]["scenario"]["properties"]["evidence"]["properties"]
        ["failure_artifacts"]["items"]["pattern"]
        .as_str()
        .expect("failure_artifacts item has a pattern");
    // The pattern is ^artifact\.(<tiers>)\.(<kinds>)$; extract the second group.
    let kinds_group = pattern
        .rsplit_once("\\.(")
        .and_then(|(_, tail)| tail.strip_suffix(")$"))
        .expect("pattern has a trailing (kind1|kind2|...)$ group");
    let schema_kinds: Vec<&str> = kinds_group.split('|').collect();
    assert_eq!(
        schema_kinds,
        enums::ARTIFACT_DESCRIPTOR_KIND,
        "enums::ARTIFACT_DESCRIPTOR_KIND must match the schema's failure_artifacts kind alternation"
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

    // Issue #1023 (roborev): the claim-kind enum lives under `$defs.claim`, not
    // `$defs.scenario`, so it was missed by the loop above. Assert it too, so
    // drift between `enums::CLAIM_KIND` and the schema's `claim.kind` enum can't
    // slip through.
    let claim_props = &schema["$defs"]["claim"]["properties"];
    assert_eq!(
        schema_enum(&claim_props["kind"]),
        expect(enums::CLAIM_KIND),
        "enums::CLAIM_KIND must match $defs.claim.properties.kind.enum"
    );
}

/// Extract the `test_deltas` table name from a fixture reference path of the
/// form `.../test_deltas/<table>-<uuid>/nb-1-big-Data.db.jsonl`.
fn delta_table_from_ref(path: &str) -> Option<String> {
    let after = path.split("test_deltas/").nth(1)?;
    let dir = after.split('/').next()?;
    // Assumes the trailing SSTable UUID is dashless 32-hex (true for the current
    // fixture dir names, e.g. `cell_tombstones-29733830701f11f1b5d1d98b0640ec05`),
    // so the last `-` separates table name from UUID. A dashed UUID would split
    // mid-UUID; revisit (e.g. a length-based strip) if fixture naming changes.
    let table = dir.rsplit_once('-').map(|(t, _)| t).unwrap_or(dir);
    Some(table.to_string())
}

/// Parse `CREATE TABLE [IF NOT EXISTS] <name>` table names from a CQL schema.
fn cql_table_names(cql: &str) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    let lower = cql.to_lowercase();
    let mut search = 0usize;
    // Index `lower` for both search and extraction: `to_lowercase()` is only
    // byte-length-preserving for ASCII, so cross-indexing the original `cql`
    // with offsets derived from `lower` could desync and panic on a non-char
    // boundary. The token is lowercased anyway, so working entirely in `lower`
    // is equivalent and safe.
    while let Some(rel) = lower[search..].find("create table") {
        let start = search + rel + "create table".len();
        let rest = &lower[start..];
        // Skip "if not exists" and whitespace; the next token is the table name.
        let tokens: Vec<&str> = rest.split_whitespace().collect();
        let mut idx = 0;
        if tokens.first().map(|t| t.eq_ignore_ascii_case("if")) == Some(true) {
            idx = 3; // if not exists
        }
        if let Some(name) = tokens.get(idx) {
            // Assumes unqualified table names (deltas.cql uses `USE test_deltas;`
            // + bare `CREATE TABLE [IF NOT EXISTS] <name>`). The alphanumeric/'_'
            // scan stops at the first '.', so a keyspace-qualified name
            // (`keyspace.table`) would yield the keyspace, not the table — strip a
            // leading `keyspace.` prefix here if deltas.cql ever switches to it.
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

// ----------------------------------------------------------------------------
// Public-claim scan + manifest claim validation (issue #1023).
// ----------------------------------------------------------------------------

/// A manifest with one mirrored scenario plus the claim entries the claim-scan
/// lint needs. `VALID_SCENARIO` supplies a real scenario id for `safe` claims to
/// cite, so manifest-level claim lint stays clean.
fn wrap_with_claims() -> String {
    format!(
        "{}claims:
  - id: claim.safe.selected_fixture_validation
    kind: safe
    phrase: validated against selected Apache Cassandra 5.0 SSTable fixtures
    rationale: scoped to the covered corpus, not exhaustive
    evidence_scenarios:
      - cass.sstable_format.example_mirrored
  - id: claim.blocked.same_tests_as_cassandra
    kind: blocked
    phrase: same tests as Cassandra
    rationale: CQLite does not run Cassandra's JVM test suite
    safe_alternative: claim.safe.selected_fixture_validation
  - id: claim.blocked.full_compaction_byte_parity
    kind: blocked
    phrase: full compaction byte parity
    rationale: byte parity only where the manifest records byte_for_byte
    safe_alternative: claim.safe.selected_fixture_validation
  - id: claim.blocked.zero_diff_sstabledump_all_datasets
    kind: blocked
    phrase: zero-diff sstabledump across every dataset
    rationale: only selected fixtures are validated
    safe_alternative: claim.safe.selected_fixture_validation
",
        wrap(VALID_SCENARIO)
    )
}

fn claim_findings(yaml: &str, files: &[(&str, &str)]) -> Vec<String> {
    let m = Manifest::from_yaml(yaml).expect("manifest parses");
    let inputs: Vec<ScanInput<'_>> = files
        .iter()
        .map(|(p, t)| ScanInput { path: p, text: t })
        .collect();
    scan_docs(&m, &inputs)
        .into_iter()
        .filter(|f| f.level == Level::Error)
        .map(|f| format!("[{}] {}: {}", f.id, f.field, f.message))
        .collect()
}

#[test]
fn manifest_supports_all_four_statuses() {
    // AC1: the closed status set is exactly these four.
    assert_eq!(
        enums::STATUS,
        ["mirrored", "partial", "planned", "out_of_scope"]
    );
}

#[test]
fn unqualified_same_tests_claim_fails() {
    let docs = [("README.md", "CQLite runs the same tests as Cassandra.")];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.blocked.same_tests_as_cassandra")),
        "got: {errs:#?}"
    );
}

#[test]
fn unqualified_full_compaction_byte_parity_fails() {
    let docs = [("README.md", "We ship full compaction byte parity today.")];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.blocked.full_compaction_byte_parity")),
        "got: {errs:#?}"
    );
}

#[test]
fn unqualified_zero_diff_all_datasets_fails() {
    let docs = [(
        "CHANGELOG.md",
        "Now with zero-diff sstabledump across every dataset.",
    )];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.blocked.zero_diff_sstabledump_all_datasets")),
        "got: {errs:#?}"
    );
}

#[test]
fn explicitly_scoped_blocked_phrase_passes() {
    // AC5: a blocked phrase framed as a counter-example is allowed.
    let docs = [(
        "docs/development/parity-release-checklist.md",
        "Do not claim the same tests as Cassandra; scope every parity claim.",
    )];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(errs.is_empty(), "scoped phrase should pass, got: {errs:#?}");
}

#[test]
fn unsafe_marked_blocked_phrase_passes() {
    let docs = [(
        "README.md",
        "Unsafe: \"full compaction byte parity\" — overclaims byte parity.",
    )];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.is_empty(),
        "unsafe-marked phrase should pass: {errs:#?}"
    );
}

#[test]
fn manifest_backed_safe_wording_passes() {
    // AC4: a claim that uses the manifest-backed safe wording passes even though
    // it mentions parity, because it is the recorded safe phrase.
    let docs = [(
        "README.md",
        "CQLite is validated against selected Apache Cassandra 5.0 SSTable fixtures.",
    )];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(errs.is_empty(), "safe wording should pass, got: {errs:#?}");
}

#[test]
fn same_line_safe_phrase_does_not_exempt_separate_blocked_phrase() {
    // Roborev finding 1: a line that contains a manifest-backed safe phrase AND a
    // separate unqualified over-claim must still FAIL — the safe wording only
    // exempts the span it covers, not the whole line.
    let docs = [(
        "README.md",
        "CQLite is validated against selected Apache Cassandra 5.0 SSTable fixtures \
         and runs the same tests as Cassandra.",
    )];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.blocked.same_tests_as_cassandra")),
        "separate blocked phrase on a safe-wording line must be caught, got: {errs:#?}"
    );
}

#[test]
fn unrelated_scope_marker_on_same_line_does_not_exempt_blocked_phrase() {
    // Roborev finding (issue #1023): a scope marker (`reject`) that is unrelated to
    // and far from the over-claim must NOT exempt it. Scope detection is tied to the
    // blocked-phrase occurrence, not the whole line.
    let docs = [(
        "README.md",
        "We reject stale fixtures and run the same tests as Cassandra.",
    )];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.blocked.same_tests_as_cassandra")),
        "unrelated scope marker must not exempt the over-claim, got: {errs:#?}"
    );
}

#[test]
fn scope_marker_adjacent_to_blocked_phrase_still_exempts() {
    // The positive counterpart: a scope marker in the bounded window immediately
    // preceding the blocked phrase still scopes (exempts) the occurrence.
    let docs = [(
        "docs/development/parity-release-checklist.md",
        "Reviewers must reject any \"same tests as Cassandra\" wording.",
    )];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.is_empty(),
        "adjacent scope marker should still exempt, got: {errs:#?}"
    );
}

#[test]
fn blocked_phrase_wrapped_across_lines_is_caught() {
    // Roborev finding 2: a blocked phrase split across a soft-wrap must still be
    // detected, with the finding reporting the line where the phrase starts.
    let docs = [(
        "README.md",
        "CQLite runs the same tests as\nCassandra in every build.",
    )];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.blocked.same_tests_as_cassandra")
                && e.contains("README.md:1")),
        "wrapped blocked phrase must be caught at its start line, got: {errs:#?}"
    );
}

#[test]
fn markdown_emphasis_inside_blocked_phrase_is_caught() {
    // Roborev finding (issue #1023): Markdown emphasis markers INSIDE a blocked
    // phrase (`same **tests** as Cassandra`) are semantically the same over-claim
    // and must FAIL lint at the phrase's start line.
    let docs = [("README.md", "CQLite runs the same **tests** as Cassandra.")];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.blocked.same_tests_as_cassandra")
                && e.contains("README.md:1")),
        "markdown emphasis inside a blocked phrase must be caught, got: {errs:#?}"
    );
}

#[test]
fn markdown_code_span_inside_blocked_phrase_is_caught() {
    // Roborev finding (issue #1023): a Markdown code span inside a blocked phrase
    // (`` same `tests` as Cassandra ``) must FAIL lint just like the plain phrase.
    let docs = [("README.md", "CQLite runs the same `tests` as Cassandra.")];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.blocked.same_tests_as_cassandra")
                && e.contains("README.md:1")),
        "markdown code span inside a blocked phrase must be caught, got: {errs:#?}"
    );
}

#[test]
fn markdown_underscore_emphasis_inside_blocked_phrase_is_caught() {
    // Roborev finding (issue #1023): underscore emphasis inside a blocked phrase
    // (`same _tests_ as Cassandra`) must FAIL lint.
    let docs = [("README.md", "CQLite runs the same _tests_ as Cassandra.")];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.blocked.same_tests_as_cassandra")),
        "markdown underscore emphasis inside a blocked phrase must be caught, got: {errs:#?}"
    );
}

#[test]
fn blocked_finding_names_safe_alternative() {
    let docs = [("README.md", "We have full compaction byte parity.")];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.safe.selected_fixture_validation")),
        "finding must point at the safe alternative, got: {errs:#?}"
    );
}

#[test]
fn safe_claim_with_unknown_scenario_is_rejected() {
    // Manifest-level claim lint: a safe claim citing a missing scenario fails.
    let yaml = wrap_with_claims().replace(
        "      - cass.sstable_format.example_mirrored",
        "      - cass.does.not_exist",
    );
    let errs = errors(&yaml);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.safe.selected_fixture_validation")
                && e.contains("unknown scenario id")),
        "got: {errs:#?}"
    );
}

#[test]
fn blocked_claim_without_safe_alternative_is_rejected() {
    let yaml = wrap_with_claims().replace(
        "    safe_alternative: claim.safe.selected_fixture_validation\n  - id: claim.blocked.full_compaction_byte_parity",
        "  - id: claim.blocked.full_compaction_byte_parity",
    );
    let errs = errors(&yaml);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.blocked.same_tests_as_cassandra")
                && e.contains("safe_alternative")),
        "got: {errs:#?}"
    );
}

#[test]
fn malformed_claim_id_empty_slug_is_rejected() {
    // Roborev finding (issue #1023): an id with the right prefix but an empty
    // slug (`claim.safe.`) must FAIL lint — prefix-only validation let it pass.
    let yaml = wrap_with_claims().replace("claim.safe.selected_fixture_validation", "claim.safe.");
    let errs = errors(&yaml);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.safe.") && e.contains("claims.id") && e.contains("slug")),
        "empty-slug claim id must be rejected, got: {errs:#?}"
    );
}

#[test]
fn malformed_claim_id_bad_chars_is_rejected() {
    // An id with the right prefix but uppercase/hyphen in the slug
    // (`claim.blocked.Bad-Slug`) violates `[a-z0-9_]+` and must FAIL lint.
    let yaml = wrap_with_claims().replace(
        "claim.blocked.same_tests_as_cassandra",
        "claim.blocked.Bad-Slug",
    );
    let errs = errors(&yaml);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.blocked.Bad-Slug") && e.contains("claims.id")),
        "bad-char claim id must be rejected, got: {errs:#?}"
    );
}

#[test]
fn well_formed_claim_ids_pass() {
    // The positive counterpart: the well-formed claim ids in `wrap_with_claims`
    // must NOT trigger any `claims.id` slug error.
    let errs = errors(&wrap_with_claims());
    assert!(
        !errs.iter().any(|e| e.contains("claims.id")),
        "well-formed claim ids must pass id validation, got: {errs:#?}"
    );
}

#[test]
fn real_manifest_has_the_six_claim_entries() {
    // The six claim entries from issue #1023 must exist and lint clean.
    let root = repo_root();
    let text =
        std::fs::read_to_string(root.join("test-data/cassandra-parity-manifest.yml")).unwrap();
    let m = Manifest::from_yaml(&text).expect("real manifest parses");
    for id in [
        "claim.safe.selected_fixture_validation",
        "claim.safe.rust_byte_level_coverage",
        "claim.safe.traceable_cassandra_parity_suite",
        "claim.blocked.same_tests_as_cassandra",
        "claim.blocked.full_compaction_byte_parity",
        "claim.blocked.zero_diff_sstabledump_all_datasets",
    ] {
        assert!(
            m.claims.iter().any(|c| c.id == id),
            "manifest missing claim entry {id}"
        );
    }
}

#[test]
fn assessment_report_is_in_release_files() {
    // Roborev finding 1 (issue #1023): the CI path filter for the cassandra-parity
    // lint job uses the glob `docs/reports/cassandra-test-parity*.md`, which matches
    // the hand-written, release-facing assessment report. `RELEASE_FILES` must list
    // that file so the lint actually scans it — otherwise an over-claim there would
    // trigger CI but escape the scanner (a guardrail hole).
    assert!(
        cassandra_parity::claim_scan::RELEASE_FILES
            .contains(&"docs/reports/cassandra-test-parity-assessment.md"),
        "assessment report must be in RELEASE_FILES (CI glob covers it), got: {:?}",
        cassandra_parity::claim_scan::RELEASE_FILES
    );
}

#[test]
fn blocked_claim_in_assessment_report_path_fails() {
    // Roborev finding 1 (issue #1023): a blocked over-claim placed in the assessment
    // report's path must be caught by the claim scan, proving that path is scanned.
    let docs = [(
        "docs/reports/cassandra-test-parity-assessment.md",
        "CQLite runs the same tests as Cassandra.",
    )];
    let errs = claim_findings(&wrap_with_claims(), &docs);
    assert!(
        errs.iter()
            .any(|e| e.contains("claim.blocked.same_tests_as_cassandra")
                && e.contains("docs/reports/cassandra-test-parity-assessment.md")),
        "over-claim in the assessment report path must be caught, got: {errs:#?}"
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
    // Issue #1199: every high-relevance Cassandra index file must be classified by
    // a manifest scenario (mirrored/partial/planned/out_of_scope). An unclassified
    // high-relevance file fails `coverage --strict` in CI; assert the same fully
    // here so the manifest can never silently regress below full coverage.
    assert!(
        cov.unclassified_high.is_empty(),
        "every high-relevance file must be classified; {} unclassified: {:#?}",
        cov.unclassified_high.len(),
        cov.unclassified_high
    );
    assert_eq!(
        cov.high_classified, cov.high_total,
        "all {} high-relevance files must be classified, only {} are",
        cov.high_total, cov.high_classified
    );
}
