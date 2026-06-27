//! Tests for the doc <-> schema <-> code tier-enum cross-check.
//!
//! No Docker, datasets, or live Cassandra: every fixture is an inline string.

use cassandra_parity::enums;
use cassandra_parity::tier_contract::{self, TierContractError};

/// A documented-enum fenced block that agrees with the code enum.
const GOOD_DOC: &str = "\
# Parity CI Tier Contracts

intro prose

```parity-ci-tiers
fast_pr
required_parity
nightly_docker
exhaustive_regeneration
manual_debug
```

trailing prose
";

/// A schema fragment carrying the same five tiers.
const GOOD_SCHEMA: &str = r#"{
  "$defs": {
    "scenario": {
      "properties": {
        "ci": {
          "properties": {
            "tier": { "enum": ["fast_pr", "required_parity", "nightly_docker", "exhaustive_regeneration", "manual_debug"] }
          }
        }
      }
    }
  }
}"#;

fn manifest_with_tiers(tiers: &[&str]) -> String {
    let mut s = String::from("scenarios:\n");
    for (i, t) in tiers.iter().enumerate() {
        s.push_str(&format!(
            "  - id: cass.sstable_format.s{i}\n    ci:\n      tier: {t}\n"
        ));
    }
    s
}

#[test]
fn parses_documented_enum_from_fenced_block() {
    let parsed = tier_contract::parse_documented_enum(GOOD_DOC).expect("block parses");
    assert_eq!(
        parsed,
        vec![
            "fast_pr",
            "required_parity",
            "nightly_docker",
            "exhaustive_regeneration",
            "manual_debug"
        ]
    );
}

#[test]
fn missing_fenced_block_is_an_error() {
    let err = tier_contract::parse_documented_enum("# no block here\n").unwrap_err();
    assert!(matches!(err, TierContractError::DocBlockMissing));
}

#[test]
fn passing_fixture_all_three_agree() {
    let manifest = manifest_with_tiers(&["fast_pr", "required_parity"]);
    let report =
        tier_contract::check(GOOD_DOC, GOOD_SCHEMA, enums::CI_TIER, &manifest).expect("check runs");
    assert!(report.ok(), "expected pass, got: {report:#?}");
}

#[test]
fn doc_vs_code_drift_fails_with_specific_tier() {
    // Doc is missing `manual_debug` -> drift vs the code enum.
    let drifted_doc = GOOD_DOC.replace("manual_debug\n", "");
    let report = tier_contract::check(&drifted_doc, GOOD_SCHEMA, enums::CI_TIER, "scenarios: []")
        .expect("check runs");
    assert!(!report.ok());
    let rendered = report.render();
    assert!(
        rendered.contains("manual_debug"),
        "should name the divergent tier, got: {rendered}"
    );
}

#[test]
fn doc_vs_schema_drift_fails() {
    // Schema has an extra tier the doc does not document.
    let drifted_schema = GOOD_SCHEMA.replace(
        r#""manual_debug"]"#,
        r#""manual_debug", "rogue_tier"]"#,
    );
    let report = tier_contract::check(GOOD_DOC, &drifted_schema, enums::CI_TIER, "scenarios: []")
        .expect("check runs");
    assert!(!report.ok());
    assert!(report.render().contains("rogue_tier"));
}

#[test]
fn unknown_manifest_tier_fails_with_scenario_and_value() {
    let manifest = manifest_with_tiers(&["fast_pr", "bogus_tier"]);
    let report =
        tier_contract::check(GOOD_DOC, GOOD_SCHEMA, enums::CI_TIER, &manifest).expect("check runs");
    assert!(!report.ok());
    let rendered = report.render();
    assert!(rendered.contains("bogus_tier"), "got: {rendered}");
    assert!(
        rendered.contains("cass.sstable_format.s1"),
        "should name offending scenario id, got: {rendered}"
    );
}

#[test]
fn real_doc_schema_code_and_manifest_agree() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let doc = std::fs::read_to_string(root.join("docs/development/parity-ci-tiers.md")).unwrap();
    let schema =
        std::fs::read_to_string(root.join("test-data/cassandra-parity-manifest.schema.json"))
            .unwrap();
    let manifest =
        std::fs::read_to_string(root.join("test-data/cassandra-parity-manifest.yml")).unwrap();
    let report = tier_contract::check(&doc, &schema, enums::CI_TIER, &manifest)
        .expect("real cross-check runs");
    assert!(report.ok(), "real tier contract drift: {}", report.render());
}
