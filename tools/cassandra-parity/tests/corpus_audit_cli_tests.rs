//! End-to-end tests for the `cassandra-parity corpus-audit` subcommand (issue
//! #1026, Finding 5). These exercise the disk->`audit()` glue in `main.rs`
//! (`walk_relative`, `read_sha256_file`, `read_corruption_components`,
//! `cmd_corpus_audit`, and the exit-code mapping) that the pure `audit()` unit
//! tests cannot reach: they build a temp-dir corpus + manifest + index +
//! provenance + corruption manifest on disk, invoke the built binary, and assert
//! the process exit code and the named finding in its output.

use std::fs;
use std::path::Path;
use std::process::Command;

const GOOD_SHA: &str = "f278f6774fc76465c182041e081982105c3e7dbb";
const UUID: &str = "aaaa0000000000000000000000000001";

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

    // Corruption manifest covering every required component type.
    let mut corr = String::new();
    for comp in [
        "Data.db",
        "Index.db",
        "Summary.db",
        "Statistics.db",
        "CompressionInfo.db",
        "TOC.txt",
        "Digest.crc32",
    ] {
        corr.push_str(&format!(
            "- fixture: x\n  expected_failing_component: {comp}\n"
        ));
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
