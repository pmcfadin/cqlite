//! Evidence for the spec scenario "Exhaustive regeneration failure includes the
//! audit report" (issue #1027, `specs/parity-artifacts/spec.md`): when the
//! `exhaustive_regeneration` lane's corpus audit FAILS, `cmd_corpus_audit` emits a
//! conforming `failure-artifact.json` with `tier=exhaustive_regeneration` and a
//! `diffs[]` entry of `kind=audit_report`, validated against
//! `test-data/parity-failure-artifact.schema.json`.
//!
//! This drives the REAL `cassandra-parity corpus-audit` binary end-to-end (the
//! same `cmd_corpus_audit` emit path CI runs), from a temp CWD so the bundle lands
//! under `parity-failures/exhaustive_regeneration/<scenario_id>/` — it does not
//! call a stand-alone helper.

use std::fs;
use std::path::Path;
use std::process::Command;

use cassandra_parity::corpus_audit::audit_report;

const GOOD_SHA: &str = "f278f6774fc76465c182041e081982105c3e7dbb";
const UUID: &str = "aaaa0000000000000000000000000001";

/// Repo root (two levels up from this crate's manifest dir), for the schema file.
fn repo_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tools/")
        .parent()
        .expect("repo root")
        .to_path_buf()
}

fn schema() -> serde_json::Value {
    let text =
        fs::read_to_string(repo_root().join("test-data/parity-failure-artifact.schema.json"))
            .expect("failure-artifact schema exists");
    serde_json::from_str(&text).expect("schema is valid JSON")
}

/// Schema-DRIVEN validator (mirrors `failure_artifact_tests::validate`): reads the
/// required-field list + enum members FROM the schema and checks the record.
fn validate(record: &serde_json::Value, schema: &serde_json::Value) -> Vec<String> {
    let mut errs = Vec::new();
    let required =
        |obj: &serde_json::Value, node: &serde_json::Value, prefix: &str, out: &mut Vec<String>| {
            if let Some(reqs) = node["required"].as_array() {
                for r in reqs {
                    let key = r.as_str().expect("required entry is a string");
                    if obj.get(key).is_none() {
                        out.push(format!("missing required field: {prefix}{key}"));
                    }
                }
            }
        };
    required(record, schema, "", &mut errs);
    if let Some(c) = schema["properties"]["schema_version"]["const"].as_u64() {
        if record["schema_version"].as_u64() != Some(c) {
            errs.push(format!("schema_version must be {c}"));
        }
    }
    let enum_of = |node: &serde_json::Value| -> Vec<String> {
        node["enum"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    };
    let check_enum = |val: Option<&str>, allowed: &[String], field: &str, out: &mut Vec<String>| {
        if let Some(v) = val {
            if !allowed.iter().any(|a| a == v) {
                out.push(format!("{field} '{v}' not in enum {allowed:?}"));
            }
        }
    };
    check_enum(
        record["tier"].as_str(),
        &enum_of(&schema["properties"]["tier"]),
        "tier",
        &mut errs,
    );
    check_enum(
        record["evidence_type"].as_str(),
        &enum_of(&schema["properties"]["evidence_type"]),
        "evidence_type",
        &mut errs,
    );
    // provenance patterns: cassandra_git_sha + dataset_sha256 must match the
    // schema's hex patterns (this is what would reject a `deadbeef` fallback).
    if let Some(prov) = record.get("provenance") {
        required(
            prov,
            &schema["properties"]["provenance"],
            "provenance.",
            &mut errs,
        );
        let sha = prov["dataset_sha256"].as_str().unwrap_or("");
        if sha.len() != 64 || !sha.chars().all(|c| c.is_ascii_hexdigit()) {
            errs.push(format!("provenance.dataset_sha256 not 64-hex: {sha:?}"));
        }
        let git = prov["cassandra_git_sha"].as_str().unwrap_or("");
        if git.len() < 7 || !git.chars().all(|c| c.is_ascii_hexdigit()) {
            errs.push(format!("provenance.cassandra_git_sha not hex: {git:?}"));
        }
    }
    if let Some(diffs) = record["diffs"].as_array() {
        let kind_enum = enum_of(&schema["properties"]["diffs"]["items"]["properties"]["kind"]);
        for (i, d) in diffs.iter().enumerate() {
            required(
                d,
                &schema["properties"]["diffs"]["items"],
                &format!("diffs[{i}]."),
                &mut errs,
            );
            check_enum(
                d["kind"].as_str(),
                &kind_enum,
                &format!("diffs[{i}].kind"),
                &mut errs,
            );
        }
    }
    errs
}

/// A repo-relative corpus reference the fixture scenario pins.
fn reference_path() -> String {
    format!("test-data/datasets/sstables/test_basic/simple_table-{UUID}/nb-1-big-Data.db.jsonl")
}

/// Write a manifest whose ONE scenario pins a reference path, plus the index the
/// audit needs. The referenced corpus file is intentionally NOT created, so the
/// audit fails MISSING-REFERENCE — the failure that must produce forensics.
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
}

/// Run the real `corpus-audit` subcommand FROM `root` as CWD so the emitted
/// `parity-failures/` bundle lands under the temp dir.
fn run_audit_in(root: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_cassandra-parity"))
        .current_dir(root)
        .args([
            "corpus-audit",
            "--manifest",
            "manifest.yml",
            "--index",
            "index.md",
            "--corpus",
            ".",
        ])
        .output()
        .expect("run cassandra-parity corpus-audit")
}

/// The spec-scenario evidence: a FAILED corpus audit emits a conforming
/// `failure-artifact.json` with `tier=exhaustive_regeneration` and a
/// `kind=audit_report` diff, validating against the schema, and the referenced
/// audit-report text is present in the bundle.
#[test]
fn failed_corpus_audit_emits_audit_report_failure_artifact() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    write_common(root);
    // Deliberately omit the referenced corpus file -> MISSING-REFERENCE finding.

    let out = run_audit_in(root);
    assert!(
        !out.status.success(),
        "corpus audit must fail (missing reference).\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let bundle = root
        .join("parity-failures")
        .join("exhaustive_regeneration")
        .join(audit_report::AUDIT_SCENARIO_ID);
    let record_path = bundle.join("failure-artifact.json");
    assert!(
        record_path.exists(),
        "failure-artifact record must exist at {}.\nstderr: {}",
        record_path.display(),
        String::from_utf8_lossy(&out.stderr)
    );

    let text = fs::read_to_string(&record_path).expect("read record");
    let value: serde_json::Value = serde_json::from_str(&text).expect("record is JSON");

    // Conforms to the schema (including the provenance hex patterns the deadbeef
    // fallback would violate).
    let errs = validate(&value, &schema());
    assert!(errs.is_empty(), "record must validate, got: {errs:#?}");

    // tier=exhaustive_regeneration.
    assert_eq!(
        value["tier"].as_str(),
        Some("exhaustive_regeneration"),
        "tier must be exhaustive_regeneration"
    );

    // diffs[] includes an audit_report entry.
    let diffs = value["diffs"].as_array().expect("diffs[] array");
    let audit_diff = diffs
        .iter()
        .find(|d| d["kind"].as_str() == Some("audit_report"))
        .expect("diffs[] must include a kind=audit_report entry");

    // The audit-report text it points at is present in the bundle (reused report).
    let pointer = audit_diff["path"].as_str().expect("audit_report path");
    let report_file = bundle.join(pointer);
    assert!(
        report_file.exists(),
        "audit_report diff must resolve to a file in the bundle: {}",
        report_file.display()
    );
    let report_text = fs::read_to_string(&report_file).expect("read audit report");
    assert!(
        report_text.contains("MISSING-REFERENCE"),
        "audit report must carry the finding, got: {report_text}"
    );

    // scenario_id is a REAL manifest exhaustive_regeneration scenario id.
    assert_eq!(
        value["scenario_id"].as_str(),
        Some(audit_report::AUDIT_SCENARIO_ID)
    );
}

/// The `scenario_id` the audit-level bundle is keyed by MUST be a real
/// `tier: exhaustive_regeneration` scenario in the shipped manifest (never an
/// invented id), and MUST match the schema's `scenario_id` pattern.
#[test]
fn audit_scenario_id_is_a_real_exhaustive_regeneration_manifest_id() {
    let manifest = fs::read_to_string(repo_root().join("test-data/cassandra-parity-manifest.yml"))
        .expect("manifest exists");
    let needle = format!("- id: {}", audit_report::AUDIT_SCENARIO_ID);
    assert!(
        manifest.contains(&needle),
        "AUDIT_SCENARIO_ID must be a real manifest id: {}",
        audit_report::AUDIT_SCENARIO_ID
    );

    // Its scenario block declares tier: exhaustive_regeneration.
    let after = manifest
        .split(&needle)
        .nth(1)
        .expect("scenario block present");
    // The tier line appears in the scenario's ci: block below the id; find the
    // first `tier:` after the id and assert it is exhaustive_regeneration.
    let tier_line = after
        .lines()
        .find(|l| l.trim_start().starts_with("tier:"))
        .expect("scenario has a tier");
    assert!(
        tier_line.contains("exhaustive_regeneration"),
        "AUDIT_SCENARIO_ID's tier must be exhaustive_regeneration, got: {tier_line}"
    );
}
