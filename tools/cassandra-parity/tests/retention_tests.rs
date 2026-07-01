//! Tests for the artifact-retention check (issue #1027, section 5.2): the check
//! parses a parity workflow's upload-artifact `retention-days` and fails if it is
//! below the tier minimum for the scenarios that lane gates. These are the
//! public-surface evidence for the "Retention windows are documented per tier and
//! enforced by tier minimum" requirement in `specs/parity-artifacts/spec.md`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use cassandra_parity::retention::{
    binding_minimum, check_workflow, parse_documented_minimums, upload_retention_days,
};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn minimums() -> BTreeMap<String, u32> {
    // Read from the documented single source so the tests exercise the real
    // doc-parse path used in CI.
    let doc = std::fs::read_to_string(repo_root().join("docs/development/parity-ci-tiers.md"))
        .expect("tier doc exists");
    parse_documented_minimums(&doc).expect("doc has a retention-minimums block")
}

/// A minimal upload-artifact workflow with a configurable retention-days.
fn upload_workflow(retention_days: &str) -> String {
    format!(
        "name: fixture\n\
         on: [push]\n\
         jobs:\n\
        \x20 parity:\n\
        \x20   runs-on: ubuntu-latest\n\
        \x20   steps:\n\
        \x20     - uses: actions/checkout@v4\n\
        \x20     - name: run\n\
        \x20       run: cargo test --test x\n\
        \x20     - name: upload\n\
        \x20       uses: actions/upload-artifact@v4\n\
        \x20       if: always()\n\
        \x20       with:\n\
        \x20         name: parity-failures-fixture\n\
        \x20         path: parity-failures/**\n\
        \x20         retention-days: {retention_days}\n"
    )
}

/// The owner-locked minimums are documented (single source) exactly as policy.
#[test]
fn documented_minimums_match_owner_policy() {
    let m = minimums();
    assert_eq!(m.get("required_parity"), Some(&14));
    assert_eq!(m.get("nightly_docker"), Some(&30));
    assert_eq!(m.get("exhaustive_regeneration"), Some(&90));
    // fast_pr / manual_debug have no minimum (logs only / attach to issue).
    assert_eq!(m.get("fast_pr"), None);
    assert_eq!(m.get("manual_debug"), None);
}

/// Spec scenario: "A workflow below its tier retention minimum fails the check" —
/// a required_parity lane setting retention-days: 7 fails, naming the workflow and
/// the >= 14 minimum.
#[test]
fn below_minimum_required_parity_fails() {
    let wf = upload_workflow("7");
    let findings = check_workflow(
        "sstabledump-parity-gate.yml",
        &wf,
        &["required_parity".to_string()],
        &minimums(),
    );
    assert_eq!(
        findings.len(),
        1,
        "expected one finding, got: {findings:#?}"
    );
    let f = &findings[0];
    assert_eq!(f.tier, "required_parity");
    assert_eq!(f.minimum, 14);
    assert_eq!(f.found, Some(7));
    assert!(
        f.message.contains("sstabledump-parity-gate.yml") && f.message.contains("14"),
        "message must name workflow + minimum, got: {}",
        f.message
    );
}

/// Spec scenario: "A workflow meeting its tier retention minimum passes" — an
/// exhaustive_regeneration lane at retention-days: 90 passes.
#[test]
fn at_minimum_exhaustive_regeneration_passes() {
    let wf = upload_workflow("90");
    let findings = check_workflow(
        "exhaustive-regeneration.yml",
        &wf,
        &["exhaustive_regeneration".to_string()],
        &minimums(),
    );
    assert!(
        findings.is_empty(),
        "at-minimum lane must pass, got: {findings:#?}"
    );
}

/// At exactly the required_parity minimum (14) the lane passes (boundary).
#[test]
fn exactly_at_required_parity_minimum_passes() {
    let wf = upload_workflow("14");
    let findings = check_workflow(
        "sstabledump-parity-gate.yml",
        &wf,
        &["required_parity".to_string()],
        &minimums(),
    );
    assert!(
        findings.is_empty(),
        "boundary value must pass, got: {findings:#?}"
    );
}

/// A lane gating multiple tiers must satisfy the STRICTER (largest) minimum.
#[test]
fn binding_minimum_is_the_strictest_tier() {
    let m = minimums();
    let binding = binding_minimum(
        &["required_parity".to_string(), "nightly_docker".to_string()],
        &m,
    );
    assert_eq!(binding, Some(("nightly_docker".to_string(), 30)));

    // A 14-day window is fine for required_parity alone but fails once the lane
    // also gates a nightly_docker scenario.
    let wf = upload_workflow("14");
    let findings = check_workflow(
        "combined.yml",
        &wf,
        &["required_parity".to_string(), "nightly_docker".to_string()],
        &m,
    );
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].minimum, 30);
}

/// A lane gating only no-minimum tiers (fast_pr) is never flagged.
#[test]
fn no_minimum_tier_lane_is_not_flagged() {
    let wf = upload_workflow("1");
    let findings = check_workflow("fast.yml", &wf, &["fast_pr".to_string()], &minimums());
    assert!(
        findings.is_empty(),
        "fast_pr lane has no minimum, got: {findings:#?}"
    );
}

/// A fixture-retaining lane that sets NO retention-days at all is flagged: the
/// implicit default is not a guaranteed floor.
#[test]
fn missing_retention_days_on_required_lane_is_flagged() {
    let wf = "name: x\n\
              on: [push]\n\
              jobs:\n\
             \x20 j:\n\
             \x20   runs-on: ubuntu-latest\n\
             \x20   steps:\n\
             \x20     - uses: actions/upload-artifact@v4\n\
             \x20       with:\n\
             \x20         name: parity-failures-x\n\
             \x20         path: parity-failures/**\n";
    let findings = check_workflow("x.yml", wf, &["required_parity".to_string()], &minimums());
    assert_eq!(findings.len(), 1, "got: {findings:#?}");
    assert_eq!(findings[0].found, None);
    assert!(findings[0].message.contains("no retention-days"));
}

/// A quoted-string retention-days (`retention-days: "7"`) is parsed as an integer.
#[test]
fn quoted_retention_days_is_parsed() {
    let wf = upload_workflow("\"7\"");
    let days = upload_retention_days(&wf);
    assert_eq!(days, vec![Some(7)]);
}

/// The upload-artifact parser ignores non-upload steps.
#[test]
fn parser_only_reads_upload_artifact_steps() {
    let wf = upload_workflow("30");
    let days = upload_retention_days(&wf);
    assert_eq!(days, vec![Some(30)], "only the one upload step counts");
}
