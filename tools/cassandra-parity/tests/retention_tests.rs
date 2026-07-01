//! Tests for the artifact-retention check (issue #1027, section 5.2): the check
//! parses a parity workflow's upload-artifact `retention-days` and fails if it is
//! below the tier minimum for the scenarios that lane gates. These are the
//! public-surface evidence for the "Retention windows are documented per tier and
//! enforced by tier minimum" requirement in `specs/parity-artifacts/spec.md`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use cassandra_parity::model::Manifest;
use cassandra_parity::retention::{
    binding_minimum, check_workflow, check_workflow_detailed, parse_documented_minimums,
    run_repo_check, upload_retention_days,
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

/// A workflow that gates parity scenarios (has a binding tier minimum) but has NO
/// `actions/upload-artifact` step at all.
fn no_upload_workflow() -> String {
    "name: x\n\
     on: [push]\n\
     jobs:\n\
    \x20 j:\n\
    \x20   runs-on: ubuntu-latest\n\
    \x20   steps:\n\
    \x20     - uses: actions/checkout@v4\n\
    \x20     - name: run parity suite\n\
    \x20       run: cargo test --test parity\n"
        .to_string()
}

/// Finding 1: a required/nightly parity-gating lane with NO upload-artifact step
/// that is NOT on the #1353 allowlist must FAIL retention-check (visible gap), not
/// pass vacuously. Names the "retains no failure artifacts" gap and the minimum.
#[test]
fn no_upload_non_allowlisted_lane_is_a_finding() {
    let wf = no_upload_workflow();
    let findings = check_workflow(
        ".github/workflows/some-new-parity-lane.yml",
        &wf,
        &["nightly_docker".to_string()],
        &minimums(),
    );
    assert_eq!(
        findings.len(),
        1,
        "a non-allowlisted no-upload parity lane must be a finding, got: {findings:#?}"
    );
    let f = &findings[0];
    assert_eq!(f.tier, "nightly_docker");
    assert_eq!(f.minimum, 30);
    assert_eq!(f.found, None);
    assert!(
        f.message.contains("retains no failure artifacts")
            && f.message.contains("some-new-parity-lane.yml"),
        "message must name the gap + the workflow, got: {}",
        f.message
    );

    // The detailed disposition is a finding with no note (it is a hard finding, not
    // an OK-with-a-note).
    let detailed = check_workflow_detailed(
        ".github/workflows/some-new-parity-lane.yml",
        &wf,
        &["nightly_docker".to_string()],
        &minimums(),
    );
    assert_eq!(detailed.findings.len(), 1);
    assert!(
        detailed.note.is_none(),
        "a non-allowlisted lane must not get an OK-with-a-note"
    );
}

/// Finding 1: an ALLOWLISTED lane (#1353 no-emitter) with no upload step is
/// OK-with-a-note — no finding — so retention-check stays green while the gap is
/// visibly tracked.
#[test]
fn no_upload_allowlisted_lane_is_ok_with_note() {
    let wf = no_upload_workflow();
    let detailed = check_workflow_detailed(
        ".github/workflows/cql-type-parity.yml",
        &wf,
        &["nightly_docker".to_string()],
        &minimums(),
    );
    assert!(
        detailed.findings.is_empty(),
        "allowlisted no-emitter lane must not be a finding, got: {:#?}",
        detailed.findings
    );
    let note = detailed
        .note
        .as_deref()
        .expect("allowlisted no-upload lane must carry an OK-with-a-note");
    assert!(
        note.contains("#1353") && note.contains("cql-type-parity.yml"),
        "note must reference #1353 + the workflow, got: {note}"
    );
}

/// Finding 1: the real repo retention-check stays GREEN — the 6+ known
/// non-emitting parity lanes are allowlisted (reported as notes) and the 3 emitting
/// lanes meet their tier minimums, so `run_repo_check` reports OK with zero
/// findings and at least one allowlist note.
#[test]
fn real_repo_retention_check_is_green_with_allowlist_notes() {
    let root = repo_root();
    let text = std::fs::read_to_string(root.join("test-data/cassandra-parity-manifest.yml"))
        .expect("manifest exists");
    let manifest = Manifest::from_yaml(&text).expect("manifest parses");
    let result = run_repo_check(&manifest, &root, &minimums());
    assert!(
        result.ok(),
        "real-repo retention-check must be green, findings: {:#?}",
        result.findings
    );
    assert!(
        !result.notes.is_empty(),
        "expected allowlisted no-emitter lanes to be reported as notes"
    );
    assert!(
        result.notes.iter().all(|n| n.contains("#1353")),
        "every allowlist note must reference #1353, got: {:#?}",
        result.notes
    );
    // The three emitting lanes are among the checked fixture-retaining workflows.
    assert!(
        result.checked >= 3,
        "expected at least the 3 emitting lanes checked, got {}",
        result.checked
    );
}
