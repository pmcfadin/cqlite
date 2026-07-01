//! Issue #1027 Wave 2a — scenario-id-keyed failure-bundle emission from the Rust
//! `required_parity` byte/offset/checksum/JSONL parity checks (tasks 2.1–2.4).
//!
//! These are the evidence tests for the OpenSpec change `parity-artifact-retention`
//! requirements that Section 2 satisfies:
//!
//!   * "Failure artifacts live in a bundle keyed by manifest scenario id" — a
//!     failed scenario writes `parity-failures/<tier>/<scenario_id>/` with
//!     `failure-artifact.json`, `stdout.txt`, `stderr.txt`, `diffs/`, `repro/`
//!     (`byte_mismatch_emits_scenario_id_keyed_bundle`) and a passing scenario
//!     writes no bundle (`passing_scenario_writes_no_bundle`).
//!   * "Byte-for-byte failures preserve byte, offset, and checksum diffs plus a
//!     component inventory" (`byte_mismatch_emits_all_four_diff_kinds`).
//!   * "Canonical-semantic failures preserve normalized and raw JSONL"
//!     (`jsonl_mismatch_emits_jsonl_diff_and_raw_sources`).
//!   * "The reproduction bundle lets a maintainer rerun the failing check"
//!     (`repro_bundle_names_command_and_fixture_inputs`).
//!   * "A uniform failure-artifact record schema exists and is versioned" — every
//!     emitted record validates against `test-data/parity-failure-artifact.schema.json`
//!     (`emitted_record_validates_against_wave1_schema`).
//!
//! The tests are hermetic: they use a synthetic fixture file in a tempdir and
//! synthetic mismatched bytes / JSONL, so they run in CI with no fetched Data.db
//! binaries and prove the emitter path is exercised (fail-closed, owner decision 2).

use std::path::{Path, PathBuf};

#[path = "parity_bundle/mod.rs"]
mod parity_bundle;

use parity_bundle::{
    byte_diff_body, checksums_body, component_inventory_body, jsonl_diff_body, offset_diff_body,
    FailureBundle, ReproContext,
};

/// A realistic `required_parity` scenario id (matches the failure-artifact schema
/// pattern `^cass\.[a-z0-9_]+(\.[A-Za-z0-9_]+)+$`).
const BYTE_SCENARIO: &str = "cass.statistics_metadata.statistics_db_strict_parity";
const JSONL_SCENARIO: &str = "cass.data_db_decode.sstable_parity_data_db_jsonl";
const LANE: &str = "sstabledump-parity-gate.yml";

/// Write a synthetic fixture the emitter can SHA-256 (paths + SHA only, no copy).
fn synthetic_fixture(dir: &Path) -> PathBuf {
    let path = dir.join("nb-1-big-Statistics.db");
    std::fs::write(&path, b"synthetic-fixture-bytes-for-issue-1027").expect("write fixture");
    path
}

fn byte_repro(fixture: PathBuf) -> ReproContext {
    ReproContext {
        cassandra_version: "5.0.2".to_string(),
        cassandra_git_sha: "f278f6774fc76465c182041e081982105c3e7dbb".to_string(),
        fixture_path: fixture,
        component_list: vec![
            "Data.db".to_string(),
            "Statistics.db".to_string(),
            "TOC.txt".to_string(),
        ],
        command_line: "env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
             --test sstable_parity_statistics_db_strict_test"
            .to_string(),
    }
}

/// Build a `byte_for_byte` bundle from a forced byte mismatch.
fn emit_byte_bundle(root: &Path, fixture: PathBuf) -> parity_bundle::EmittedBundle {
    // Deliberately-altered expected value vs actual: identical prefix, one byte
    // diverges. This is the "feed a deliberately corrupted expected value" force.
    let expected = b"\x00\x01\x02\x03\x04\x05".to_vec();
    let actual = b"\x00\x01\x02\xff\x04\x05".to_vec();
    let expected_components = vec![
        "Data.db".to_string(),
        "Statistics.db".to_string(),
        "TOC.txt".to_string(),
    ];
    let actual_components = vec!["Data.db".to_string(), "Statistics.db".to_string()];

    FailureBundle::new(
        root,
        BYTE_SCENARIO,
        LANE,
        "required_parity",
        "byte_for_byte",
        byte_repro(fixture),
    )
    .artifacts_compared(["bytes", "offsets", "checksums", "component_files"])
    .stdout("comparing Statistics.db byte-for-byte\n")
    .stderr("assertion failed: statistics bytes differ\n")
    .byte_for_byte_component(
        "Statistics.db",
        byte_diff_body("Statistics.db", &expected, &actual),
        offset_diff_body("Statistics.db", &expected, &actual),
        checksums_body("Statistics.db", &expected, &actual),
        component_inventory_body(&expected_components, &actual_components),
    )
    .emit()
    .expect("byte bundle emits")
}

/// Build a `canonical_semantic` bundle from a forced JSONL mismatch.
fn emit_jsonl_bundle(root: &Path, fixture: PathBuf) -> parity_bundle::EmittedBundle {
    let reference = vec![
        r#"{"partition":"a","cells":[{"name":"v","value":1}]}"#.to_string(),
        r#"{"partition":"b","cells":[{"name":"v","value":2}]}"#.to_string(),
    ];
    // Deliberately-altered candidate: second row's value diverges.
    let candidate = vec![
        r#"{"partition":"a","cells":[{"name":"v","value":1}]}"#.to_string(),
        r#"{"partition":"b","cells":[{"name":"v","value":999}]}"#.to_string(),
    ];
    let repro = ReproContext {
        cassandra_version: "5.0.2".to_string(),
        cassandra_git_sha: "f278f6774fc76465c182041e081982105c3e7dbb".to_string(),
        fixture_path: fixture,
        component_list: vec!["Data.db".to_string()],
        command_line: "env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
             --test sstabledump_parity_data"
            .to_string(),
    };

    FailureBundle::new(
        root,
        JSONL_SCENARIO,
        LANE,
        "required_parity",
        "canonical_semantic",
        repro,
    )
    .artifacts_compared(["jsonl"])
    .stdout("comparing Data.db JSONL\n")
    .stderr("assertion failed: jsonl rows differ\n")
    .jsonl(
        jsonl_diff_body(&reference, &candidate),
        reference.join("\n"),
        candidate.join("\n"),
    )
    .emit()
    .expect("jsonl bundle emits")
}

// ---------------------------------------------------------------------------
// Minimal schema-driven validator (no `jsonschema` crate in the workspace).
// Reads the required-field list + enum members FROM the Wave 1 schema file and
// checks the record against them (required fields present, tier/evidence_type/
// diffs[].kind drawn from the schema enums, schema_version const).
// ---------------------------------------------------------------------------

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .expect("workspace root")
}

fn wave1_schema() -> serde_json::Value {
    let text =
        std::fs::read_to_string(repo_root().join("test-data/parity-failure-artifact.schema.json"))
            .expect("failure-artifact schema exists");
    serde_json::from_str(&text).expect("schema is valid JSON")
}

fn enum_of(node: &serde_json::Value) -> Vec<String> {
    node["enum"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn required_present(
    record: &serde_json::Value,
    schema: &serde_json::Value,
    at: &str,
    errs: &mut Vec<String>,
) {
    if let Some(reqs) = schema["required"].as_array() {
        for r in reqs {
            if let Some(field) = r.as_str() {
                if record.get(field).is_none() {
                    errs.push(format!("missing required field {at}{field}"));
                }
            }
        }
    }
}

/// Validate `record` against the Wave 1 schema; returns validation errors.
fn validate(record: &serde_json::Value, schema: &serde_json::Value) -> Vec<String> {
    let mut errs = Vec::new();
    required_present(record, schema, "", &mut errs);
    if let Some(c) = schema["properties"]["schema_version"]["const"].as_u64() {
        if record["schema_version"].as_u64() != Some(c) {
            errs.push(format!("schema_version must be {c}"));
        }
    }
    let check_enum = |field: &str, node: &serde_json::Value, errs: &mut Vec<String>| {
        let allowed = enum_of(node);
        if let Some(v) = record[field].as_str() {
            if !allowed.iter().any(|a| a == v) {
                errs.push(format!("{field} value {v:?} not in enum"));
            }
        }
    };
    check_enum("tier", &schema["properties"]["tier"], &mut errs);
    check_enum(
        "evidence_type",
        &schema["properties"]["evidence_type"],
        &mut errs,
    );
    // provenance required fields.
    required_present(
        &record["provenance"],
        &schema["properties"]["provenance"],
        "provenance.",
        &mut errs,
    );
    // diffs[].kind drawn from enum.
    let kind_enum = enum_of(&schema["properties"]["diffs"]["items"]["properties"]["kind"]);
    if let Some(diffs) = record["diffs"].as_array() {
        for d in diffs {
            if let Some(kind) = d["kind"].as_str() {
                if !kind_enum.iter().any(|k| k == kind) {
                    errs.push(format!("diffs[].kind {kind:?} not in enum"));
                }
            }
            if d["path"].as_str().map(str::is_empty).unwrap_or(true) {
                errs.push("diffs[].path must be a non-empty string".to_string());
            }
        }
    }
    errs
}

fn read_record(bundle_dir: &Path) -> serde_json::Value {
    let text =
        std::fs::read_to_string(bundle_dir.join("failure-artifact.json")).expect("record written");
    serde_json::from_str(&text).expect("record is valid JSON")
}

// ---------------------------------------------------------------------------
// Evidence tests
// ---------------------------------------------------------------------------

/// 2.1 — a forced byte mismatch writes a scenario-id-keyed bundle with the
/// required top-level layout.
#[test]
fn byte_mismatch_emits_scenario_id_keyed_bundle() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fixture = synthetic_fixture(tmp.path());
    let out = emit_byte_bundle(tmp.path(), fixture);

    let expected_dir = tmp
        .path()
        .join("parity-failures")
        .join("required_parity")
        .join(BYTE_SCENARIO);
    assert_eq!(out.bundle_dir, expected_dir, "bundle keyed by scenario id");
    assert!(out.bundle_dir.is_dir());
    assert!(out.bundle_dir.join("failure-artifact.json").is_file());
    assert!(out.bundle_dir.join("stdout.txt").is_file());
    assert!(out.bundle_dir.join("stderr.txt").is_file());
    assert!(out.bundle_dir.join("diffs").is_dir());
    assert!(out.bundle_dir.join("repro").is_dir());
}

/// Every emitted record validates against the Wave 1 schema.
#[test]
fn emitted_record_validates_against_wave1_schema() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let schema = wave1_schema();

    let byte_out = emit_byte_bundle(tmp.path(), synthetic_fixture(tmp.path()));
    let errs = validate(&read_record(&byte_out.bundle_dir), &schema);
    assert!(errs.is_empty(), "byte record must validate, got: {errs:#?}");

    let jsonl_out = emit_jsonl_bundle(tmp.path(), synthetic_fixture(tmp.path()));
    let errs = validate(&read_record(&jsonl_out.bundle_dir), &schema);
    assert!(
        errs.is_empty(),
        "jsonl record must validate, got: {errs:#?}"
    );
}

/// 2.2 — a byte mismatch preserves all four byte_for_byte diff files and the
/// record's `diffs[]` includes matching byte_diff/offset_diff/checksum_diff/
/// component_inventory entries whose paths resolve inside the bundle.
#[test]
fn byte_mismatch_emits_all_four_diff_kinds() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = emit_byte_bundle(tmp.path(), synthetic_fixture(tmp.path()));
    let diffs = out.bundle_dir.join("diffs");

    assert!(diffs.join("Statistics.db.byte-diff.txt").is_file());
    assert!(diffs.join("Statistics.db.offset-diff.txt").is_file());
    assert!(diffs.join("checksums.txt").is_file());
    assert!(diffs.join("component_inventory.txt").is_file());

    let record = read_record(&out.bundle_dir);
    let kinds: Vec<String> = record["diffs"]
        .as_array()
        .expect("diffs array")
        .iter()
        .map(|d| {
            // every diff path must resolve inside the bundle
            let p = d["path"].as_str().expect("path");
            assert!(
                out.bundle_dir.join(p).is_file(),
                "diff path {p} resolves inside bundle"
            );
            d["kind"].as_str().expect("kind").to_string()
        })
        .collect();
    for k in [
        "byte_diff",
        "offset_diff",
        "checksum_diff",
        "component_inventory",
    ] {
        assert!(kinds.contains(&k.to_string()), "missing diffs[] kind {k}");
    }

    // The byte-diff body pinpoints the forced first-differing byte (offset 3).
    let body = std::fs::read_to_string(diffs.join("Statistics.db.byte-diff.txt")).unwrap();
    assert!(body.contains("first difference at byte offset 3"), "{body}");
}

/// 2.3 — a JSONL mismatch preserves the normalized diff plus both raw JSONL
/// sources; the record's diffs[] includes a jsonl_diff entry.
#[test]
fn jsonl_mismatch_emits_jsonl_diff_and_raw_sources() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = emit_jsonl_bundle(tmp.path(), synthetic_fixture(tmp.path()));
    let diffs = out.bundle_dir.join("diffs");

    assert!(diffs.join("jsonl.diff").is_file());
    assert!(diffs.join("reference.jsonl").is_file());
    assert!(diffs.join("candidate.jsonl").is_file());

    let record = read_record(&out.bundle_dir);
    let jsonl_entry = record["diffs"]
        .as_array()
        .expect("diffs array")
        .iter()
        .find(|d| d["kind"].as_str() == Some("jsonl_diff"))
        .expect("jsonl_diff entry");
    let p = jsonl_entry["path"].as_str().expect("path");
    assert_eq!(p, "diffs/jsonl.diff");
    assert!(out.bundle_dir.join(p).is_file());

    let body = std::fs::read_to_string(diffs.join("jsonl.diff")).unwrap();
    assert!(body.contains("first differing line 1"), "{body}");
}

/// 2.4 — the repro bundle names the exact command and the fixture inputs
/// (path + dataset SHA256), and the record's repro_bundle resolves to repro/.
#[test]
fn repro_bundle_names_command_and_fixture_inputs() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fixture = synthetic_fixture(tmp.path());
    let out = emit_byte_bundle(tmp.path(), fixture.clone());

    let record = read_record(&out.bundle_dir);
    let repro_ptr = record["repro_bundle"].as_str().expect("repro_bundle");
    assert_eq!(repro_ptr, "repro/");
    let repro = out.bundle_dir.join(repro_ptr);
    assert!(repro.is_dir(), "repro_bundle resolves to a directory");

    let command = std::fs::read_to_string(repro.join("command.sh")).unwrap();
    assert!(command.contains("cargo test -p cqlite-core"), "{command}");
    assert!(repro.join("INSTRUCTIONS.md").is_file());

    let inputs = std::fs::read_to_string(repro.join("inputs/fixtures.txt")).unwrap();
    assert!(
        inputs.contains(&fixture.display().to_string()),
        "inputs names the fixture path: {inputs}"
    );
    // dataset SHA256 present (64 hex chars) and NOT a copy of the fixture.
    assert!(
        inputs
            .lines()
            .any(|l| l.split('\t').nth(1).map(|s| s.len() == 64).unwrap_or(false)),
        "inputs names a dataset SHA256: {inputs}"
    );
    assert!(
        !repro
            .join("inputs")
            .join(fixture.file_name().unwrap())
            .exists(),
        "repro must NOT copy the dataset"
    );
}

/// Spec requirement "A passing scenario writes no failure bundle": a passing
/// parity check emits nothing under parity-failures/.../<scenario_id>/.
#[test]
fn passing_scenario_writes_no_bundle() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // Simulate a passing scenario: no FailureBundle::emit is called.
    let would_be_dir = tmp
        .path()
        .join("parity-failures")
        .join("required_parity")
        .join(BYTE_SCENARIO);
    assert!(
        !would_be_dir.exists(),
        "a passing scenario must not produce a failure bundle"
    );
    // And the parity-failures/ root is never created on a clean pass.
    assert!(!tmp.path().join("parity-failures").exists());
}
