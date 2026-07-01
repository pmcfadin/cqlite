//! Tests for the artifact-retention check (issue #1027, section 5.2): the check
//! parses a parity workflow's upload-artifact `retention-days` and fails if it is
//! below the tier minimum for the scenarios that lane gates. These are the
//! public-surface evidence for the "Retention windows are documented per tier and
//! enforced by tier minimum" requirement in `specs/parity-artifacts/spec.md`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use cassandra_parity::model::Manifest;
use cassandra_parity::retention::{
    binding_minimum, check_workflow, check_workflow_detailed, has_parity_failure_upload,
    no_emitter_allowlist, parse_documented_minimums, run_repo_check, upload_retention_days,
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

/// A workflow that gates parity scenarios but whose ONLY `actions/upload-artifact`
/// step uploads an UNRELATED artifact (not the shared `parity-failures/**` bundle),
/// e.g. `cassandra-validation.yml`'s `sstableloader-test-results` upload.
fn unrelated_upload_workflow() -> String {
    "name: x\n\
     on: [push]\n\
     jobs:\n\
    \x20 j:\n\
    \x20   runs-on: ubuntu-latest\n\
    \x20   steps:\n\
    \x20     - uses: actions/checkout@v4\n\
    \x20     - name: run parity suite\n\
    \x20       run: cargo test --test parity\n\
    \x20     - name: Upload Test Artifacts\n\
    \x20       uses: actions/upload-artifact@v6\n\
    \x20       with:\n\
    \x20         name: sstableloader-test-results\n\
    \x20         path: |\n\
    \x20           tier1_results.txt\n\
    \x20           test_summary.md\n\
    \x20         retention-days: 30\n"
        .to_string()
}

/// Finding 2 (issue #1027): an UNRELATED upload does NOT count as the shared
/// parity-failure bundle. `upload_retention_days` returns only shared-bundle
/// uploads, so an unrelated-only workflow yields an empty list.
#[test]
fn unrelated_upload_is_not_counted_as_parity_failure_bundle() {
    let wf = unrelated_upload_workflow();
    let days = upload_retention_days(&wf);
    assert!(
        days.is_empty(),
        "an unrelated upload must not count as a parity-failure upload, got: {days:#?}"
    );
}

/// Finding 2 (issue #1027): a parity-gating lane whose ONLY upload is unrelated,
/// and which is NOT on the #1353 allowlist, must FAIL retention-check — the
/// unrelated upload must not vacuously satisfy the lane.
#[test]
fn unrelated_upload_non_allowlisted_lane_is_a_finding() {
    let wf = unrelated_upload_workflow();
    let detailed = check_workflow_detailed(
        ".github/workflows/some-new-parity-lane.yml",
        &wf,
        &["nightly_docker".to_string()],
        &minimums(),
    );
    assert_eq!(
        detailed.findings.len(),
        1,
        "an unrelated-upload non-allowlisted parity lane must be a finding, got: {:#?}",
        detailed.findings
    );
    let f = &detailed.findings[0];
    assert_eq!(f.tier, "nightly_docker");
    assert_eq!(f.minimum, 30);
    assert_eq!(f.found, None);
    assert!(
        f.message.contains("retains no failure artifacts"),
        "message must name the gap, got: {}",
        f.message
    );
    assert!(
        detailed.note.is_none(),
        "a non-allowlisted lane must not get an OK-with-a-note"
    );
}

/// Finding 3 (issue #1027): the aspirational-descriptor lane list documented in
/// `parity-failure-artifacts.md` is tied to the retention no-emitter allowlist +
/// #1353 so triage never reads a descriptor on a non-emitting lane as a production
/// guarantee, and the doc + code cannot silently drift.
#[test]
fn aspirational_lanes_doc_matches_retention_allowlist() {
    let doc =
        std::fs::read_to_string(repo_root().join("docs/development/parity-failure-artifacts.md"))
            .expect("parity-failure-artifacts.md exists");

    // The doc must explicitly tie the aspirational descriptors to #1353.
    assert!(
        doc.contains("ASPIRATIONAL pending")
            && doc.contains("#1353")
            && doc.contains("no_emitter_allowlist"),
        "doc must mark descriptors aspirational + reference #1353 + the code source"
    );

    // Extract the machine-checked list between the markers.
    let begin = "<!-- aspirational-no-emitter-lanes:begin";
    let end = "<!-- aspirational-no-emitter-lanes:end -->";
    let start = doc.find(begin).expect("aspirational list begin marker");
    let after_begin = doc[start..]
        .find("-->")
        .map(|i| start + i + 3)
        .expect("begin close");
    let stop = doc.find(end).expect("aspirational list end marker");
    let block = &doc[after_begin..stop];
    let documented: Vec<String> = block
        .lines()
        .filter_map(|l| l.trim().strip_prefix("- "))
        .map(|s| s.trim().trim_matches('`').to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let mut expected: Vec<String> = no_emitter_allowlist()
        .iter()
        .map(|s| s.to_string())
        .collect();
    let mut actual = documented.clone();
    expected.sort();
    actual.sort();
    assert_eq!(
        actual, expected,
        "the aspirational-lane list in parity-failure-artifacts.md must equal \
         retention::no_emitter_allowlist() (add/remove in the same change as #1353 wiring)"
    );
}

/// Finding 2 (issue #1027): the SAME unrelated-upload lane, when ALLOWLISTED
/// (#1353), is OK-with-a-note — the unrelated upload does not mask the missing
/// parity-failure emitter. This is exactly `cassandra-validation.yml`'s shape.
#[test]
fn unrelated_upload_allowlisted_lane_is_ok_with_note() {
    let wf = unrelated_upload_workflow();
    let detailed = check_workflow_detailed(
        ".github/workflows/cassandra-validation.yml",
        &wf,
        &["nightly_docker".to_string()],
        &minimums(),
    );
    assert!(
        detailed.findings.is_empty(),
        "allowlisted unrelated-upload lane must not be a finding, got: {:#?}",
        detailed.findings
    );
    let note = detailed
        .note
        .as_deref()
        .expect("allowlisted unrelated-upload lane must carry an OK-with-a-note");
    assert!(
        note.contains("#1353") && note.contains("cassandra-validation.yml"),
        "note must reference #1353 + the workflow, got: {note}"
    );
}

/// Issue #1028's automation-summary upload: a SINGLE FILE `parity-failures.json`
/// under artifact `name: parity-failures` (NO trailing dash), step labelled
/// "Upload parity-failures.json". This is NOT the #1027 scenario-id forensic bundle.
fn issue_1028_summary_upload_workflow() -> String {
    "name: x\n\
     on: [push]\n\
     jobs:\n\
    \x20 j:\n\
    \x20   runs-on: ubuntu-latest\n\
    \x20   steps:\n\
    \x20     - uses: actions/checkout@v4\n\
    \x20     - name: run parity suite\n\
    \x20       run: cargo test --test parity\n\
    \x20     - name: Upload parity-failures.json\n\
    \x20       uses: actions/upload-artifact@v4\n\
    \x20       if: always()\n\
    \x20       with:\n\
    \x20         name: parity-failures\n\
    \x20         path: parity-failures.json\n\
    \x20         if-no-files-found: warn\n"
        .to_string()
}

/// Bug fix (issue #1027): the retention matcher must scope ONLY to the #1027 shared
/// forensic bundle and NOT to issue #1028's `parity-failures.json` summary upload.
/// A lane whose ONLY upload is the #1028 summary (bare `name: parity-failures`, file
/// `path: parity-failures.json`, step label "Upload parity-failures.json") must be
/// treated as having NO #1027 emitter — the label/name must not false-match.
#[test]
fn issue_1028_summary_upload_is_not_a_1027_bundle() {
    let wf = issue_1028_summary_upload_workflow();
    let days = upload_retention_days(&wf);
    assert!(
        days.is_empty(),
        "issue #1028's parity-failures.json summary must not count as a #1027 \
         forensic-bundle upload, got: {days:#?}"
    );
}

/// The #1028 summary upload, on an ALLOWLISTED lane (the three real deferred lanes:
/// compression-corruption / cql-type / tombstone-ttl), falls back to the no-emitter
/// allowlist disposition (OK-with-a-note), NOT a retention finding.
#[test]
fn issue_1028_summary_on_allowlisted_lane_is_ok_with_note() {
    let wf = issue_1028_summary_upload_workflow();
    let detailed = check_workflow_detailed(
        ".github/workflows/cql-type-parity.yml",
        &wf,
        &["nightly_docker".to_string()],
        &minimums(),
    );
    assert!(
        detailed.findings.is_empty(),
        "an allowlisted lane whose only upload is the #1028 summary must not be a \
         finding, got: {:#?}",
        detailed.findings
    );
    assert!(
        detailed
            .note
            .as_deref()
            .is_some_and(|n| n.contains("#1353")),
        "expected an OK-with-a-note referencing #1353, got: {:?}",
        detailed.note
    );
}

/// The #1028 summary upload on a NON-allowlisted parity lane is still a finding (it
/// does not vacuously satisfy the lane) — proving the scoping change did not turn the
/// summary into a silent pass.
#[test]
fn issue_1028_summary_on_non_allowlisted_lane_is_a_finding() {
    let wf = issue_1028_summary_upload_workflow();
    let findings = check_workflow(
        ".github/workflows/some-new-parity-lane.yml",
        &wf,
        &["nightly_docker".to_string()],
        &minimums(),
    );
    assert_eq!(
        findings.len(),
        1,
        "a #1028-summary-only non-allowlisted parity lane must be a finding, got: {findings:#?}"
    );
    assert!(findings[0].message.contains("retains no failure artifacts"));
}

/// The #1027 forensic bundle IS recognised: `name: parity-failures-foo` (trailing
/// dash) with `path: parity-failures/**` (directory glob) counts as a shared upload.
#[test]
fn issue_1027_forensic_bundle_is_recognised() {
    let wf = upload_workflow("90");
    let days = upload_retention_days(&wf);
    assert_eq!(
        days,
        vec![Some(90)],
        "a #1027 bundle (name parity-failures-*, path parity-failures/**) must count"
    );
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

// ---------------------------------------------------------------------------
// Finding 2 (round 6): retention-check must ALSO scan workflows directly for a
// shared parity-failure upload, even when no manifest `ci.workflow` references
// them (e.g. exhaustive-regeneration.yml's shared `parity-failures-*` upload).
// ---------------------------------------------------------------------------

/// A workflow whose ONLY parity-relevant upload is a SHARED parity-failure bundle
/// (`name: parity-failures-<slug>`, `path: parity-failures/**`), with a
/// configurable retention window.
fn shared_bundle_workflow(retention_days: &str) -> String {
    format!(
        "name: exhaustive fixture\n\
         on: [workflow_dispatch]\n\
         jobs:\n\
        \x20 j:\n\
        \x20   runs-on: ubuntu-latest\n\
        \x20   steps:\n\
        \x20     - uses: actions/checkout@v4\n\
        \x20     - name: run\n\
        \x20       run: cargo run -p cassandra-parity -- corpus-audit\n\
        \x20     - name: Upload parity failure bundle\n\
        \x20       uses: actions/upload-artifact@v4\n\
        \x20       if: failure()\n\
        \x20       with:\n\
        \x20         name: parity-failures-exhaustive-regeneration\n\
        \x20         path: parity-failures/**\n\
        \x20         retention-days: {retention_days}\n"
    )
}

/// `has_parity_failure_upload` recognises a shared bundle upload regardless of
/// whether the manifest references the workflow.
#[test]
fn shared_bundle_upload_is_detected_directly() {
    assert!(has_parity_failure_upload(&shared_bundle_workflow("90")));
    assert!(
        !has_parity_failure_upload(&unrelated_upload_workflow()),
        "an unrelated upload is not a shared parity-failure bundle"
    );
    assert!(
        !has_parity_failure_upload(&issue_1028_summary_upload_workflow()),
        "the #1028 summary upload is not a shared parity-failure bundle"
    );
}

/// A minimal but schema-complete manifest with NO scenarios (so nothing references
/// exhaustive-regeneration.yml) — the direct scan is the ONLY thing that can cover
/// a shared-bundle workflow here.
fn empty_manifest_yaml() -> String {
    "manifest_version: 1\n\
     cassandra_source:\n\
    \x20 repo: apache/cassandra\n\
    \x20 ref: cassandra-5.0.2\n\
    \x20 sha: f278f6774fc76465c182041e081982105c3e7dbb\n\
    \x20 index: docs/index.md\n\
    \x20 assessment_report: docs/assessment.md\n\
     program:\n\
    \x20 parent_epic: 974\n\
    \x20 reporting_epic: 974\n\
     scenarios: []\n"
        .to_string()
}

/// Build a tiny throwaway repo root: the tier-minimums doc + an empty manifest +
/// a `.github/workflows/exhaustive-regeneration.yml` carrying the given shared
/// bundle upload (NOT referenced by any manifest scenario).
fn temp_repo_with_exhaustive_workflow(retention_days: &str) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();

    // Mirror the real tier-minimums doc so parse_documented_minimums works, and so
    // the check's exhaustive_regeneration minimum is the real 90.
    let doc_dir = root.join("docs/development");
    std::fs::create_dir_all(&doc_dir).unwrap();
    let real_doc =
        std::fs::read_to_string(repo_root().join("docs/development/parity-ci-tiers.md")).unwrap();
    std::fs::write(doc_dir.join("parity-ci-tiers.md"), real_doc).unwrap();

    // A manifest with NO scenario referencing exhaustive-regeneration.yml.
    std::fs::write(root.join("manifest.yml"), empty_manifest_yaml()).unwrap();

    let wf_dir = root.join(".github/workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("exhaustive-regeneration.yml"),
        shared_bundle_workflow(retention_days),
    )
    .unwrap();

    tmp
}

/// Finding 2: a directly-scanned shared-bundle workflow NOT in the manifest, with
/// retention BELOW its (exhaustive_regeneration) tier minimum, is a FINDING.
#[test]
fn unmanifested_shared_bundle_below_tier_min_is_a_finding() {
    let tmp = temp_repo_with_exhaustive_workflow("30");
    let manifest =
        Manifest::from_yaml(&std::fs::read_to_string(tmp.path().join("manifest.yml")).unwrap())
            .expect("manifest parses");
    let result = run_repo_check(&manifest, tmp.path(), &minimums());
    assert!(
        !result.ok(),
        "a shared bundle at 30 days for exhaustive_regeneration (min 90) must fail, \
         findings: {:#?}",
        result.findings
    );
    let f = result
        .findings
        .iter()
        .find(|f| f.workflow.ends_with("exhaustive-regeneration.yml"))
        .expect("finding names the directly-scanned workflow");
    assert_eq!(f.tier, "exhaustive_regeneration");
    assert_eq!(f.minimum, 90);
    assert_eq!(f.found, Some(30));
}

/// Finding 2: the SAME directly-scanned shared-bundle workflow, AT/ABOVE its tier
/// minimum (90), is OK — and it WAS checked (proving the direct scan ran).
#[test]
fn unmanifested_shared_bundle_at_tier_min_is_ok() {
    let tmp = temp_repo_with_exhaustive_workflow("90");
    let manifest =
        Manifest::from_yaml(&std::fs::read_to_string(tmp.path().join("manifest.yml")).unwrap())
            .expect("manifest parses");
    let result = run_repo_check(&manifest, tmp.path(), &minimums());
    assert!(
        result.ok(),
        "a shared bundle at 90 days meets the exhaustive_regeneration min, findings: {:#?}",
        result.findings
    );
    assert!(
        result.checked >= 1,
        "the directly-scanned exhaustive-regeneration.yml must have been checked"
    );
}

/// Finding 2 (real repo): the REAL exhaustive-regeneration.yml — not referenced by
/// any manifest `ci.workflow` — is now covered by run_repo_check and passes at its
/// 90-day exhaustive_regeneration minimum. This is the direct-scan coverage over
/// the real repository.
#[test]
fn real_exhaustive_regeneration_yml_is_covered_and_green() {
    let root = repo_root();
    let wf = std::fs::read_to_string(root.join(".github/workflows/exhaustive-regeneration.yml"))
        .expect("exhaustive-regeneration.yml exists");
    // Sanity: the real workflow does upload a shared bundle and is NOT referenced
    // by any manifest ci.workflow (so ONLY the direct scan can cover it).
    assert!(
        has_parity_failure_upload(&wf),
        "the real exhaustive-regeneration.yml must upload a shared parity-failure bundle"
    );
    let manifest_text =
        std::fs::read_to_string(root.join("test-data/cassandra-parity-manifest.yml")).unwrap();
    assert!(
        !manifest_text.contains("workflow: .github/workflows/exhaustive-regeneration.yml"),
        "precondition: no manifest scenario references exhaustive-regeneration.yml"
    );

    let manifest = Manifest::from_yaml(&manifest_text).expect("manifest parses");
    let result = run_repo_check(&manifest, &root, &minimums());
    assert!(
        result.ok(),
        "real-repo retention-check must be green (incl. exhaustive-regeneration.yml), \
         findings: {:#?}",
        result.findings
    );
    // The exhaustive-regeneration.yml lane must NOT be flagged.
    assert!(
        !result
            .findings
            .iter()
            .any(|f| f.workflow.ends_with("exhaustive-regeneration.yml")),
        "exhaustive-regeneration.yml must pass at its 90-day minimum"
    );
}
