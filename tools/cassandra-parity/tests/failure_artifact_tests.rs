//! Tests for the uniform failure-artifact record (issue #1027, section 1):
//! the record model + emitter, its schema, and the `diffs[].kind` enum
//! cross-check. These are the public-surface evidence for the "uniform
//! failure-artifact record schema", "provenance block", and "record validates /
//! missing field rejected" requirements in `specs/parity-artifacts/spec.md`.

use std::path::PathBuf;

use cassandra_parity::enums;
use cassandra_parity::failure_artifact::{Diff, FailureArtifact, Provenance, SCHEMA_VERSION};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn schema() -> serde_json::Value {
    let text =
        std::fs::read_to_string(repo_root().join("test-data/parity-failure-artifact.schema.json"))
            .expect("failure-artifact schema exists");
    serde_json::from_str(&text).expect("schema is valid JSON")
}

/// A minimal, schema-DRIVEN validator: it reads the required-field list and the
/// enum members FROM the schema file (not hardcoded here) and checks the record
/// against them. No `jsonschema` crate is available in the workspace, so this
/// covers exactly the properties the spec scenarios assert: all required fields
/// present, and `tier`/`evidence_type`/`diffs[].kind` drawn from the schema enums.
/// Returns the list of validation errors (empty == valid).
fn validate(record: &serde_json::Value, schema: &serde_json::Value) -> Vec<String> {
    let mut errs = Vec::new();

    let required =
        |obj: &serde_json::Value, node: &serde_json::Value, prefix: &str, out: &mut Vec<String>| {
            if let Some(reqs) = node["required"].as_array() {
                for r in reqs {
                    let key = r.as_str().unwrap();
                    if obj.get(key).is_none() {
                        out.push(format!("missing required field: {prefix}{key}"));
                    }
                }
            }
        };

    // Top-level required fields.
    required(record, schema, "", &mut errs);

    // schema_version const.
    if let Some(c) = schema["properties"]["schema_version"]["const"].as_u64() {
        if record["schema_version"].as_u64() != Some(c) {
            errs.push(format!("schema_version must be {c}"));
        }
    }

    // Enum membership drawn from the schema.
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

    // provenance required fields.
    if let Some(prov) = record.get("provenance") {
        required(
            prov,
            &schema["properties"]["provenance"],
            "provenance.",
            &mut errs,
        );
    }

    // diffs[].kind enum + required fields.
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

fn sample_record() -> FailureArtifact {
    FailureArtifact {
        schema_version: SCHEMA_VERSION,
        scenario_id: "cass.compression_checksum.digest_crc32_byte_for_byte_parity".to_string(),
        lane: "sstabledump-parity-gate.yml".to_string(),
        tier: "required_parity".to_string(),
        evidence_type: "byte_for_byte".to_string(),
        artifacts_compared: vec![
            "bytes".to_string(),
            "offsets".to_string(),
            "checksums".to_string(),
            "component_files".to_string(),
        ],
        provenance: Provenance {
            cassandra_version: "5.0.2".to_string(),
            cassandra_git_sha: "f278f6774fc76465c182041e081982105c3e7dbb".to_string(),
            dataset_sha256: "a".repeat(64),
            fixture_path: "test-data/datasets/sstables/test_basic/simple-uuid/nb-1-big-Data.db"
                .to_string(),
            component_list: vec![
                "Data.db".to_string(),
                "Index.db".to_string(),
                "Statistics.db".to_string(),
                "TOC.txt".to_string(),
                "Digest.crc32".to_string(),
            ],
            command_line: "CQLITE_REQUIRE_FIXTURES=1 cargo test -p cqlite-core --test x name"
                .to_string(),
            stdout: "stdout.txt".to_string(),
            stderr: "stderr.txt".to_string(),
        },
        diffs: vec![
            Diff::new("byte_diff", "diffs/Data.db.byte-diff.txt"),
            Diff::new("offset_diff", "diffs/Data.db.offset-diff.txt"),
            Diff::new("checksum_diff", "diffs/checksums.txt"),
            Diff::new("component_inventory", "diffs/component_inventory.txt"),
        ],
        repro_bundle: "repro/".to_string(),
    }
}

/// Requirement: "A uniform failure-artifact record schema exists and is
/// versioned" — a record written by a parity surface validates against the
/// schema (round-trip: emit -> validate).
#[test]
fn emitted_record_validates_against_schema() {
    let record = sample_record();
    let json = record.to_json().expect("serialize");
    let value: serde_json::Value = serde_json::from_str(&json).expect("parse emitted json");
    let errs = validate(&value, &schema());
    assert!(
        errs.is_empty(),
        "emitted record must validate, got: {errs:#?}"
    );

    // Round-trips back to an identical struct.
    let back = FailureArtifact::from_json(&json).expect("deserialize");
    assert_eq!(record, back);
}

/// Requirement scenario: "A record missing a required field is rejected" — a
/// record with `scenario_id` omitted fails validation and names the field.
#[test]
fn record_missing_required_field_is_rejected() {
    let record = sample_record();
    let mut value: serde_json::Value = serde_json::to_value(&record).unwrap();
    value.as_object_mut().unwrap().remove("scenario_id");
    let errs = validate(&value, &schema());
    assert!(
        errs.iter().any(|e| e.contains("scenario_id")),
        "must name the missing scenario_id field, got: {errs:#?}"
    );
}

/// Requirement scenario: `tier`/`evidence_type` must be drawn from the manifest
/// enums — an out-of-enum tier fails validation.
#[test]
fn record_with_out_of_enum_tier_is_rejected() {
    let mut value: serde_json::Value = serde_json::to_value(sample_record()).unwrap();
    value["tier"] = serde_json::json!("not_a_tier");
    let errs = validate(&value, &schema());
    assert!(
        errs.iter()
            .any(|e| e.contains("tier") && e.contains("not_a_tier")),
        "out-of-enum tier must be rejected, got: {errs:#?}"
    );
}

/// Requirement: "The provenance block records the full reproduction context" —
/// dropping a provenance field (fixture_path) fails validation.
#[test]
fn record_missing_provenance_field_is_rejected() {
    let mut value: serde_json::Value = serde_json::to_value(sample_record()).unwrap();
    value["provenance"]
        .as_object_mut()
        .unwrap()
        .remove("fixture_path");
    let errs = validate(&value, &schema());
    assert!(
        errs.iter().any(|e| e.contains("provenance.fixture_path")),
        "missing provenance.fixture_path must be named, got: {errs:#?}"
    );
}

/// The emitter writes `failure-artifact.json` inside a scenario-id-keyed bundle
/// and the written file validates against the schema.
#[test]
fn write_to_bundle_emits_conforming_record() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let bundle = tmp
        .path()
        .join("parity-failures")
        .join("required_parity")
        .join("cass.compression_checksum.digest_crc32_byte_for_byte_parity");
    let record = sample_record();
    let written = record.write_to_bundle(&bundle).expect("write");
    assert_eq!(written.file_name().unwrap(), "failure-artifact.json");

    let text = std::fs::read_to_string(&written).expect("read back");
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    let errs = validate(&value, &schema());
    assert!(
        errs.is_empty(),
        "written record must validate, got: {errs:#?}"
    );
}

/// 1.3: the `diffs[].kind` enum in `enums.rs` must match the schema's enum,
/// mirroring the existing `schema_enums_match_lint_enums` / tier-enum
/// cross-check pattern so drift between code and schema cannot slip through.
#[test]
fn failure_artifact_kind_enum_matches_schema() {
    let schema = schema();
    let schema_kinds: Vec<String> = schema["properties"]["diffs"]["items"]["properties"]["kind"]
        ["enum"]
        .as_array()
        .expect("diffs.items.kind.enum is an array")
        .iter()
        .map(|x| x.as_str().unwrap().to_string())
        .collect();
    let code_kinds: Vec<String> = enums::FAILURE_ARTIFACT_KIND
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        schema_kinds, code_kinds,
        "enums::FAILURE_ARTIFACT_KIND must match the schema's diffs[].kind enum"
    );
}

/// The record's tier/evidence_type enums are the SAME closed sets as the manifest
/// schema (spec: values MUST be drawn from the manifest enums). Cross-check the
/// failure-artifact schema's enums against `enums::CI_TIER` / `EVIDENCE_TYPE`.
#[test]
fn record_tier_and_evidence_enums_match_manifest_enums() {
    let schema = schema();
    let tiers: Vec<String> = schema["properties"]["tier"]["enum"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_string())
        .collect();
    let evidence: Vec<String> = schema["properties"]["evidence_type"]["enum"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        tiers,
        enums::CI_TIER
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        "failure-artifact tier enum must equal the manifest CI_TIER enum"
    );
    assert_eq!(
        evidence,
        enums::EVIDENCE_TYPE
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        "failure-artifact evidence_type enum must equal the manifest EVIDENCE_TYPE enum"
    );
}
