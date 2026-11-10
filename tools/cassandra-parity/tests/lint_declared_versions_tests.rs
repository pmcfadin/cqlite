//! #4482: a scenario's `evidence.cassandra_version` is a per-fixture declaration and
//! must name the primary `cassandra_source.ref` release or an `additional_pins` one.

use cassandra_parity::lint::lint;
use cassandra_parity::model::Manifest;

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

fn manifest_with_scenario_version(version: &str, additional: bool) -> String {
    let mut y = wrap(VALID_SCENARIO).replace(
        "cassandra_version: \"5.0.2\"",
        &format!("cassandra_version: \"{version}\""),
    );
    if additional {
        y = y.replace(
            "  ref: cassandra-5.0.2\n",
            "  ref: cassandra-5.0.2\n  additional_pins:\n    - ref: cassandra-5.0.8\n      sha: 172227094626d50d4bc9a3d9de2e3659a7844386\n",
        );
    }
    y
}

fn version_findings(version: &str, additional: bool) -> Vec<String> {
    let y = manifest_with_scenario_version(version, additional);
    let m = Manifest::from_yaml(&y).expect("parses");
    lint(&m, None)
        .into_iter()
        .filter(|f| f.field == "evidence.cassandra_version")
        .map(|f| f.message)
        .collect()
}

#[test]
fn lint_accepts_a_per_fixture_version_declared_by_the_primary_or_an_additional_pin() {
    for (version, additional) in [("5.0.2", false), ("5.0.2", true), ("5.0.8", true)] {
        let found = version_findings(version, additional);
        assert!(
            found.is_empty(),
            "{version} (additional={additional}): {found:?}"
        );
    }
}

#[test]
fn lint_rejects_a_per_fixture_version_no_pin_declares() {
    for (version, additional) in [("5.0.8", false), ("5.0.9", true)] {
        let found = version_findings(version, additional);
        assert_eq!(
            found.len(),
            1,
            "{version} (additional={additional}): {found:?}"
        );
    }
}

const SHA_502: &str = "f278f6774fc76465c182041e081982105c3e7dbb";
const SHA_508: &str = "172227094626d50d4bc9a3d9de2e3659a7844386";

fn sha_findings(version: &str, sha: &str) -> Vec<String> {
    let y = manifest_with_scenario_version(version, true).replace(
        &format!("cassandra_git_sha: {SHA_502}"),
        &format!("cassandra_git_sha: {sha}"),
    );
    let m = Manifest::from_yaml(&y).expect("parses");
    lint(&m, None)
        .into_iter()
        .filter(|f| f.field == "evidence.cassandra_git_sha")
        .map(|f| f.message)
        .collect()
}

/// roborev job 174: version and sha must come from the SAME pin.
#[test]
fn lint_requires_version_and_sha_from_the_same_pin() {
    assert!(sha_findings("5.0.2", SHA_502).is_empty());
    assert!(sha_findings("5.0.8", SHA_508).is_empty());
    assert_eq!(
        sha_findings("5.0.8", SHA_502).len(),
        1,
        "5.0.8 version + 5.0.2 sha"
    );
    assert_eq!(
        sha_findings("5.0.2", SHA_508).len(),
        1,
        "5.0.2 version + 5.0.8 sha"
    );
    assert_eq!(
        sha_findings("5.0.2", "0000000000000000000000000000000000000000").len(),
        1
    );
}

/// Either field alone is rejected (roborev job 175): the same-pin check cannot run on one
/// field, so a lone value must not stand as a declaration.
#[test]
fn lint_requires_both_version_and_sha_when_either_is_present() {
    let base = manifest_with_scenario_version("5.0.2", true);
    let drop = |needle: &str| {
        let y: String = base
            .lines()
            .filter(|l| !l.contains(needle))
            .collect::<Vec<_>>()
            .join("\n");
        let m = Manifest::from_yaml(&y).expect("parses");
        lint(&m, None)
            .into_iter()
            .map(|f| f.field)
            .collect::<Vec<_>>()
    };
    let sha_only = drop("cassandra_version:");
    assert!(
        sha_only.iter().any(|f| f == "evidence.cassandra_version"),
        "sha-only evidence must be rejected: {sha_only:?}"
    );
    let version_only = drop("cassandra_git_sha:");
    assert!(
        version_only
            .iter()
            .any(|f| f == "evidence.cassandra_git_sha"),
        "version-only evidence must be rejected: {version_only:?}"
    );
}
