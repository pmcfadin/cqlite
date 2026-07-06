//! End-to-end tests for the `cassandra-parity corpus-audit` subcommand (issue
//! #1026, Finding 5). These exercise the disk->`audit()` glue in `main.rs`
//! (`walk_relative`, `read_sha256_file`, `read_corruption_fixtures`,
//! `cmd_corpus_audit`, and the exit-code mapping) that the pure `audit()` unit
//! tests cannot reach: they build a temp-dir corpus + manifest + index +
//! provenance + corruption manifest on disk, invoke the built binary, and assert
//! the process exit code and the named finding in its output.

use std::fs;
use std::path::Path;
use std::process::Command;

const GOOD_SHA: &str = "f278f6774fc76465c182041e081982105c3e7dbb";
const UUID: &str = "aaaa0000000000000000000000000001";

/// The seven required corruption component types (mirrors
/// `corpus_audit::REQUIRED_CORRUPTION_COMPONENTS`).
const REQUIRED_CORRUPTION: &[&str] = &[
    "Data.db",
    "Index.db",
    "Summary.db",
    "Statistics.db",
    "CompressionInfo.db",
    "TOC.txt",
    "Digest.crc32",
];

/// A repo-relative corpus reference the fixture scenario pins.
fn reference_path() -> String {
    format!("test-data/datasets/sstables/test_basic/simple_table-{UUID}/nb-1-big-Data.db.jsonl")
}

/// Write the manifest, index, and corruption manifest the audit needs. Returns
/// nothing; callers create/omit the referenced corpus file to drive pass/fail.
fn write_common(root: &Path) {
    let reference = reference_path();
    let manifest = format!(
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
        - {reference}
    ci:
      tier: exhaustive_regeneration
"#
    );
    fs::write(root.join("manifest.yml"), manifest).expect("write manifest");

    let index = "# Cassandra test index\n\n## High-relevance tests (quick list)\n\n\
         | Test | Notes |\n|------|-------|\n| `SortedTableWriterTest.java` | classified |\n\n\
         ## Other section\n";
    fs::write(root.join("index.md"), index).expect("write index");

    // Corruption manifest covering every required component type, with each
    // declared fixture's corrupted file present on disk so coverage is clean.
    write_corruption(root, REQUIRED_CORRUPTION);
}

/// Datasets-relative on-disk path of a component's corruption fixture, mirroring
/// the real `corruption-manifest.yml` layout.
fn corruption_rel(comp: &str) -> String {
    format!("corruption/test_comp_corrupt/{comp}_fixture/nb-1-big-{comp}")
}

/// Write a corruption manifest DECLARING every required component (with on-disk
/// `corrupted_path` + `status`), and create the corrupted file under the corpus
/// only for components listed in `on_disk`. A component declared but omitted from
/// `on_disk` drives the on-disk coverage gap (spec R4): a declared-but-absent
/// fixture must fail the audit, not merely a manifest declaration.
fn write_corruption(root: &Path, on_disk: &[&str]) {
    // Start from a clean corruption tree so a re-declaration with a narrower
    // `on_disk` set genuinely removes a previously-written fixture file.
    let corruption_root = root.join("test-data/datasets/corruption");
    let _ = fs::remove_dir_all(&corruption_root);
    let mut corr = String::from("schema_version: 1\nfixtures:\n");
    for comp in REQUIRED_CORRUPTION {
        let rel = corruption_rel(comp);
        corr.push_str(&format!(
            "  - name: {comp}_fixture\n    status: active\n    corrupted_path: \"{rel}\"\n    \
             expected_failing_component: {comp}\n"
        ));
        if on_disk.contains(comp) {
            let path = root.join("test-data/datasets").join(&rel);
            fs::create_dir_all(path.parent().expect("parent")).expect("mkdir corruption fixture");
            fs::write(&path, b"corrupt\n").expect("write corruption fixture");
        }
    }
    fs::write(root.join("corruption-manifest.yml"), corr).expect("write corruption manifest");
}

/// Create the referenced corpus file under the temp corpus root.
fn write_reference_file(root: &Path) {
    let path = root.join(reference_path());
    fs::create_dir_all(path.parent().expect("parent")).expect("mkdir corpus");
    fs::write(&path, b"{}\n").expect("write golden");
}

/// Provenance JSON that matches the fixture manifest pin.
fn write_provenance(root: &Path) {
    let prov = format!(
        r#"{{
  "cassandra_version": "5.0.2",
  "cassandra_ref": "cassandra-5.0.2",
  "cassandra_git_sha": "{GOOD_SHA}",
  "docker_image": "cassandra:5.0.2",
  "generator_commands": ["bash test-data/scripts/regenerate-datasets.sh"],
  "dataset_asset_name": "cassandra5-small-full.tar.gz",
  "dataset_asset_sha256": "deadbeef"
}}"#
    );
    fs::write(root.join("provenance.json"), prov).expect("write provenance");
}

fn run_audit(root: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_cassandra-parity"))
        .args([
            "corpus-audit",
            "--manifest",
            root.join("manifest.yml").to_str().expect("utf8"),
            "--index",
            root.join("index.md").to_str().expect("utf8"),
            "--corpus",
            root.to_str().expect("utf8"),
            "--provenance",
            root.join("provenance.json").to_str().expect("utf8"),
            "--corruption-manifest",
            root.join("corruption-manifest.yml").to_str().expect("utf8"),
        ])
        .output()
        .expect("run cassandra-parity corpus-audit")
}

#[test]
fn corpus_audit_clean_exits_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    write_common(root);
    write_reference_file(root);
    write_provenance(root);

    let out = run_audit(root);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "expected exit 0 for a clean corpus.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("corpus-audit: OK"),
        "expected OK summary, got stdout: {stdout}"
    );
}

/// Issue #1026 (HIGH/LOW A, roborev): the lane audits a FRESHLY regenerated
/// corpus — every run `rm -rf`s the tree and re-creates each table, so Cassandra
/// mints a NEW random table UUID and the regenerated golden lands under a
/// DIFFERENT `<table>-<uuid>` directory than the committed manifest reference
/// pins. The prior exact-path-match clean test never exercised this and passed
/// green while the real regenerated input hard-failed STALE-REFERENCE. This
/// drives the CLI with the golden under a churned UUID dir and asserts exit 0.
#[test]
fn corpus_audit_clean_under_uuid_churn_exits_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    write_common(root);
    write_provenance(root);

    // Same table_key + basename, but under a fresh (churned) UUID directory the
    // manifest does not pin.
    let churned = reference_path().replace(
        "simple_table-aaaa0000000000000000000000000001",
        "simple_table-bbbb0000000000000000000000000002",
    );
    let path = root.join(&churned);
    fs::create_dir_all(path.parent().expect("parent")).expect("mkdir churned corpus");
    fs::write(&path, b"{}\n").expect("write churned golden");

    let out = run_audit(root);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "UUID churn (golden under a new uuid dir) must exit 0.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("corpus-audit: OK"),
        "expected OK summary under churn, got stdout: {stdout}"
    );
}

#[test]
fn corpus_audit_missing_reference_exits_nonzero_and_names_finding() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    write_common(root);
    // Deliberately do NOT create the referenced corpus file -> missing reference.
    write_provenance(root);

    let out = run_audit(root);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let combined = format!("{stdout}{stderr}");
    assert!(
        !out.status.success(),
        "expected non-zero exit for a missing reference.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        combined.contains("MISSING-REFERENCE"),
        "expected a named MISSING-REFERENCE finding, got: {combined}"
    );
    assert!(
        combined.contains(&reference_path()),
        "finding must name the offending reference, got: {combined}"
    );
}

/// Issue #2009 (CLI E2E for the component-change path): the
/// `exhaustive_regeneration` tier is a COVERAGE/PRESENCE audit, NOT a byte-drift
/// tier. Drive `--checksums` + `--expected-inventory` through the binary with a
/// deliberate checksum DRIFT for a stable, PRESENT table+component identity and
/// assert the audit stays clean (exit 0, no UNEXPECTED-COMPONENT-CHANGE). This is
/// the only test exercising the disk -> `read_sha256_file` ->
/// `check_component_changes` glue end-to-end with a drift, proving presence alone
/// passes regardless of SHA256 (byte-parity lives in the sstabledump-parity-gate
/// + nightly_docker tiers).
#[test]
fn corpus_audit_component_checksum_drift_stays_clean_under_presence_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    write_common(root);
    write_reference_file(root);
    write_provenance(root);

    // A stable, UUID-independent component identity (table_key/basename) present
    // in both the committed-expected golden and the regenerated-actual checksums,
    // but with drifted SHA256s -> under the coverage tier this must NOT fire.
    let component =
        format!("test-data/datasets/sstables/test_basic/simple_table-{UUID}/nb-1-big-Data.db");
    // Presence is sourced from the WALKED corpus (issue #2009), so the component
    // must exist ON DISK under --corpus for it to count as present — this is the
    // realistic "regeneration produced it, bytes drifted" case.
    let component_path = root.join(&component);
    fs::create_dir_all(component_path.parent().expect("parent"))
        .expect("create component dir");
    fs::write(&component_path, b"regenerated bytes").expect("write on-disk component");
    let expected_sha = "1".repeat(64);
    let actual_sha = "2".repeat(64);
    fs::write(
        root.join("expected.sha256"),
        format!("{expected_sha}  {component}\n"),
    )
    .expect("write expected inventory");
    fs::write(
        root.join("actual.sha256"),
        format!("{actual_sha}  {component}\n"),
    )
    .expect("write actual checksums");

    let out = Command::new(env!("CARGO_BIN_EXE_cassandra-parity"))
        .args([
            "corpus-audit",
            "--manifest",
            root.join("manifest.yml").to_str().expect("utf8"),
            "--index",
            root.join("index.md").to_str().expect("utf8"),
            "--corpus",
            root.to_str().expect("utf8"),
            "--provenance",
            root.join("provenance.json").to_str().expect("utf8"),
            "--corruption-manifest",
            root.join("corruption-manifest.yml").to_str().expect("utf8"),
            "--checksums",
            root.join("actual.sha256").to_str().expect("utf8"),
            "--expected-inventory",
            root.join("expected.sha256").to_str().expect("utf8"),
        ])
        .output()
        .expect("run cassandra-parity corpus-audit");

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "a present identity with a drifted SHA256 must exit 0 (coverage tier), got: {combined}"
    );
    assert!(
        !combined.contains("UNEXPECTED-COMPONENT-CHANGE"),
        "no UNEXPECTED-COMPONENT-CHANGE finding may fire for a present-but-drifted identity, got: {combined}"
    );
}

#[test]
fn corpus_audit_provenance_mismatch_exits_nonzero_and_names_finding() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    write_common(root);
    write_reference_file(root);
    // Provenance from an undeclared Cassandra version/sha.
    let bad = r#"{
  "cassandra_version": "6.6.6",
  "cassandra_ref": "cassandra-6.6.6",
  "cassandra_git_sha": "0000000000000000000000000000000000000000",
  "docker_image": "cassandra:6.6.6",
  "generator_commands": [],
  "dataset_asset_name": "x.tar.gz",
  "dataset_asset_sha256": "dead"
}"#;
    fs::write(root.join("provenance.json"), bad).expect("write provenance");

    let out = run_audit(root);
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !out.status.success(),
        "expected non-zero exit for provenance mismatch, got: {combined}"
    );
    assert!(
        combined.contains("PROVENANCE-MISMATCH"),
        "expected a named PROVENANCE-MISMATCH finding, got: {combined}"
    );
}

/// Issue #1026 (roborev LOW 2), CLI E2E for the on-disk corruption check: spec R4
/// requires an on-disk corruption FIXTURE per required component, not just a
/// manifest declaration. Here every component is DECLARED but `Summary.db`'s
/// corrupted file is not written under the corpus, so the disk-backed audit must
/// exit non-zero naming `Summary.db` — proving the check validates on-disk
/// reality, not the manifest alone (a generator that silently produced fewer
/// files than declared cannot slip through).
#[test]
fn corpus_audit_corruption_fixture_absent_on_disk_exits_nonzero_and_names_component() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    write_common(root);
    write_reference_file(root);
    write_provenance(root);

    // Re-declare all seven but withhold Summary.db's on-disk fixture file.
    let on_disk: Vec<&str> = REQUIRED_CORRUPTION
        .iter()
        .copied()
        .filter(|c| *c != "Summary.db")
        .collect();
    write_corruption(root, &on_disk);

    let out = run_audit(root);
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !out.status.success(),
        "expected non-zero exit when a declared corruption fixture is absent on disk, got: {combined}"
    );
    assert!(
        combined.contains("CORRUPTION-COVERAGE-GAP") && combined.contains("Summary.db"),
        "expected a named CORRUPTION-COVERAGE-GAP for Summary.db, got: {combined}"
    );
}
