//! Issue #1027 finding 1 — END-TO-END proof that the REAL required-parity failure
//! terminal emits the shared, scenario-id-keyed failure bundle.
//!
//! The other issue-1027 test (`issue_1027_parity_failure_bundle.rs`) drives the
//! `FailureBundle` helper directly; this one drives the PRODUCTION failure path:
//! it builds a [`ParityFailure`] for a real wired suite with a real file fixture
//! and calls the public [`ParityFailure::panic`] terminal (the single site every
//! concrete required-parity mismatch routes through), then asserts that
//! `parity-failures/<tier>/<scenario_id>/failure-artifact.json` was written to the
//! deterministic root. This proves the emitter is reachable from the production
//! failure surface, not only from a bespoke test helper.
//!
//! Isolated in its own test binary (single `#[test]`) because it sets the
//! `CQLITE_PARITY_FAILURES_ROOT` process env var to redirect the bundle into a
//! tempdir; a single-test binary has no intra-binary parallelism to race it.

use std::path::Path;
use std::sync::Mutex;

#[path = "parity_support/mod.rs"]
mod parity_support;

use parity_support::{bundle_descriptor_for_suite, scenario, ParityFailure};

/// Serialises the two tests that mutate the process-global
/// `CQLITE_PARITY_FAILURES_ROOT` env var so they can never interleave (each sets
/// it to its own tempdir, runs, then clears it).
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Driving the real `ParityFailure::panic` terminal for a wired suite with a real
/// file fixture writes the shared bundle at
/// `<root>/parity-failures/<tier>/<cass_scenario_id>/failure-artifact.json`.
#[test]
fn real_parity_failure_panic_emits_scenario_id_keyed_bundle() {
    let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let tmp = tempfile::tempdir().expect("tempdir");
    // Redirect the deterministic emit root into the tempdir.
    std::env::set_var("CQLITE_PARITY_FAILURES_ROOT", tmp.path());

    // A real, readable file fixture (the emitter SHA-256s it; paths + SHA only).
    let fixture = tmp.path().join("nb-1-big-Statistics.db");
    std::fs::write(&fixture, b"e2e-fixture-bytes-issue-1027").expect("write fixture");

    // The representative cass.* id the STATISTICS_DB suite binds to.
    let desc =
        bundle_descriptor_for_suite(scenario::STATISTICS_DB).expect("statistics suite is wired");
    let expected_bundle = tmp
        .path()
        .join("parity-failures")
        .join(desc.tier)
        .join(desc.cass_scenario_id);
    let record_path = expected_bundle.join("failure-artifact.json");

    // Drive the REAL production failure terminal. It panics (fail-closed), so catch
    // the unwind — the bundle is emitted just before the panic. Attach REAL raw
    // byte_for_byte evidence (issue #1027 finding 1) so the emitted diff bodies
    // are genuine, not the same rendered string four times.
    let fixture_for_panic = fixture.clone();
    let expected_bytes = b"CASSANDRA-Statistics.db-bytes".to_vec();
    let actual_bytes = b"CQLITE-XX-Statistics.db-bytes".to_vec();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ParityFailure::new(scenario::STATISTICS_DB)
            .lane("statistics_db")
            .cassandra_source("MetadataSerializer TOC CRC32 (Statistics.db)")
            .fixture(fixture_for_panic)
            .components(["Statistics.db"])
            .byte_evidence(
                "Statistics.db",
                expected_bytes.clone(),
                actual_bytes.clone(),
                Some((
                    vec!["Statistics.db".to_string(), "Data.db".to_string()],
                    vec!["Statistics.db".to_string()],
                )),
            )
            .detail("e2e forced mismatch: accumulated TOC CRC32 differs")
            .panic();
    }));
    assert!(result.is_err(), "panic() must abort the failing lane");

    // The production path wrote the bundle at the deterministic root.
    assert!(
        record_path.is_file(),
        "real ParityFailure::panic must emit {}",
        record_path.display()
    );

    // The record joins back to the manifest via the cass.* scenario id + tier.
    let text = std::fs::read_to_string(&record_path).expect("read record");
    let value: serde_json::Value = serde_json::from_str(&text).expect("record is JSON");
    assert_eq!(
        value["scenario_id"].as_str(),
        Some(desc.cass_scenario_id),
        "record keyed by the manifest cass.* scenario id"
    );
    assert_eq!(value["tier"].as_str(), Some(desc.tier));
    assert_eq!(value["evidence_type"].as_str(), Some(desc.evidence_type));

    // Bundle layout: the diffs/ and repro/ pointers resolve inside the bundle.
    let diffs_dir = expected_bundle.join("diffs");
    assert!(expected_bundle.join("stdout.txt").is_file());
    assert!(expected_bundle.join("stderr.txt").is_file());
    assert!(diffs_dir.is_dir());
    let repro = value["repro_bundle"].as_str().expect("repro_bundle");
    assert!(expected_bundle.join(repro).is_dir());

    // Issue #1027 finding 1: the byte_for_byte diff bodies are REAL and DISTINCT.
    // Every promised diffs[] kind resolves to a file that contains its evidence.
    let kinds: Vec<&str> = value["diffs"]
        .as_array()
        .expect("diffs[]")
        .iter()
        .map(|d| d["kind"].as_str().expect("kind"))
        .collect();
    for k in ["byte_diff", "offset_diff", "checksum_diff", "component_inventory"] {
        assert!(kinds.contains(&k), "byte_for_byte must promise {k}");
    }
    let byte_diff = std::fs::read_to_string(diffs_dir.join("Statistics.db.byte-diff.txt"))
        .expect("byte-diff file");
    // The first differing byte offset and a hex window from each side.
    assert!(
        byte_diff.contains("first difference at byte offset"),
        "byte-diff must name the first differing byte offset"
    );
    assert!(
        byte_diff.contains("expected[") && byte_diff.contains("actual  ["),
        "byte-diff must show a hex window from each side"
    );
    let checksums = std::fs::read_to_string(diffs_dir.join("checksums.txt")).expect("checksums");
    // Two DISTINCT SHA-256s — not the same rendered string.
    let hexes: Vec<&str> = checksums
        .lines()
        .filter_map(|l| l.rsplit(':').next().map(str::trim))
        .filter(|t| t.len() == 64 && t.chars().all(|c| c.is_ascii_hexdigit()))
        .collect();
    assert_eq!(hexes.len(), 2, "checksums.txt must carry two SHA-256s");
    assert_ne!(hexes[0], hexes[1], "the two SHA-256s must differ");
    let inventory = std::fs::read_to_string(diffs_dir.join("component_inventory.txt"))
        .expect("component_inventory");
    assert!(
        inventory.contains("Statistics.db") && inventory.contains("Data.db"),
        "component_inventory must list the real components"
    );
    // The four byte_for_byte bodies are NOT the same string (the round-4 finding).
    let offset_diff = std::fs::read_to_string(diffs_dir.join("Statistics.db.offset-diff.txt"))
        .expect("offset-diff");
    assert_ne!(byte_diff, checksums);
    assert_ne!(byte_diff, offset_diff);
    assert_ne!(checksums, inventory);

    std::env::remove_var("CQLITE_PARITY_FAILURES_ROOT");
}

/// Issue #1027 finding 1 — a canonical_semantic failure preserves BOTH raw source
/// JSONL files (non-empty and differing) plus a real normalized diff.
#[test]
fn canonical_semantic_failure_preserves_real_jsonl_sides() {
    let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let tmp = tempfile::tempdir().expect("tempdir");
    std::env::set_var("CQLITE_PARITY_FAILURES_ROOT", tmp.path());

    let desc = bundle_descriptor_for_suite(scenario::DATA_DB_JSONL).expect("data_db suite is wired");
    let bundle = tmp
        .path()
        .join("parity-failures")
        .join(desc.tier)
        .join(desc.cass_scenario_id);
    let record_path = bundle.join("failure-artifact.json");

    let reference = vec![
        "{\"key\":\"a\",\"v\":1}".to_string(),
        "{\"key\":\"b\",\"v\":2}".to_string(),
    ];
    let candidate = vec![
        "{\"key\":\"a\",\"v\":1}".to_string(),
        "{\"key\":\"b\",\"v\":999}".to_string(),
    ];

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ParityFailure::new(scenario::DATA_DB_JSONL)
            .lane("data_db_jsonl")
            .cassandra_source("sstabledump JSONL (Data.db row/cell decode)")
            .components(["Data.db", "Data.db.jsonl"])
            .jsonl_evidence(reference.clone(), candidate.clone())
            .detail("e2e forced JSONL parity mismatch")
            .panic();
    }));
    assert!(result.is_err(), "panic() must abort the failing lane");
    assert!(record_path.is_file(), "canonical_semantic must emit a bundle");

    let value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&record_path).expect("record"))
            .expect("json");
    assert_eq!(value["evidence_type"].as_str(), Some("canonical_semantic"));

    let diffs_dir = bundle.join("diffs");
    // BOTH raw source JSONL files are preserved, non-empty, and differ.
    let ref_body = std::fs::read_to_string(diffs_dir.join("reference.jsonl")).expect("reference");
    let cand_body = std::fs::read_to_string(diffs_dir.join("candidate.jsonl")).expect("candidate");
    assert!(!ref_body.is_empty(), "reference.jsonl must be non-empty");
    assert!(!cand_body.is_empty(), "candidate.jsonl must be non-empty");
    assert_ne!(ref_body, cand_body, "the two JSONL sides must differ");
    assert!(ref_body.contains("\"v\":2") && cand_body.contains("\"v\":999"));
    // A real normalized jsonl.diff that pinpoints the first differing line.
    let jsonl_diff = std::fs::read_to_string(diffs_dir.join("jsonl.diff")).expect("jsonl.diff");
    assert!(
        jsonl_diff.contains("first differing line 1"),
        "jsonl.diff must pinpoint the first differing line"
    );
    // The only promised diffs[] kind is the real jsonl_diff.
    let kinds: Vec<&str> = value["diffs"]
        .as_array()
        .expect("diffs[]")
        .iter()
        .map(|d| d["kind"].as_str().expect("kind"))
        .collect();
    assert_eq!(kinds, vec!["jsonl_diff"]);

    std::env::remove_var("CQLITE_PARITY_FAILURES_ROOT");
}

/// Issue #1027 finding 1 — a site with NO typed evidence (only a rendered
/// diagnostic, e.g. the dataset-absent fail-closed panics) writes a single
/// `diagnostic.txt` and an EMPTY `diffs[]`. It never fabricates a diff kind.
#[test]
fn diagnostic_only_failure_writes_no_fabricated_diff_kinds() {
    let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let tmp = tempfile::tempdir().expect("tempdir");
    std::env::set_var("CQLITE_PARITY_FAILURES_ROOT", tmp.path());

    let desc =
        bundle_descriptor_for_suite(scenario::SUMMARY_DB_BIG).expect("summary suite is wired");
    let bundle = tmp
        .path()
        .join("parity-failures")
        .join(desc.tier)
        .join(desc.cass_scenario_id);
    let record_path = bundle.join("failure-artifact.json");

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ParityFailure::new(scenario::SUMMARY_DB_BIG)
            .lane("summary_db_big")
            .cassandra_source("IndexSummaryTest (Summary.db byte/offset parity)")
            .components(["Summary.db"])
            .detail("dataset-absent fail-closed: no *-Summary.db images present")
            .panic();
    }));
    assert!(result.is_err(), "panic() must abort the failing lane");
    assert!(record_path.is_file(), "diagnostic-only must emit a bundle");

    let value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&record_path).expect("record"))
            .expect("json");
    // No fabricated kinds: diffs[] is empty and a real diagnostic.txt is written.
    assert!(
        value["diffs"].as_array().expect("diffs[]").is_empty(),
        "diagnostic-only failure must NOT promise any diff kind"
    );
    let diagnostic =
        std::fs::read_to_string(bundle.join("diffs/diagnostic.txt")).expect("diagnostic.txt");
    assert!(
        diagnostic.contains("dataset-absent fail-closed"),
        "diagnostic.txt must carry the rendered diagnostic"
    );

    std::env::remove_var("CQLITE_PARITY_FAILURES_ROOT");
}

/// Issue #1027 finding 2 — a `ParityFailure::panic` with NO `.fixture()` still
/// emits a conforming bundle. Several production call sites lack a fixture; the
/// emitter is fixture-tolerant, falling back to the `<unknown>` fixture-path
/// sentinel and the all-zero `dataset_sha256` sentinel rather than skipping.
#[test]
fn parity_failure_without_fixture_still_emits_conforming_bundle() {
    let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let tmp = tempfile::tempdir().expect("tempdir");
    std::env::set_var("CQLITE_PARITY_FAILURES_ROOT", tmp.path());

    let desc =
        bundle_descriptor_for_suite(scenario::STATISTICS_DB).expect("statistics suite is wired");
    let record_path = tmp
        .path()
        .join("parity-failures")
        .join(desc.tier)
        .join(desc.cass_scenario_id)
        .join("failure-artifact.json");

    // Drive the real terminal WITHOUT ever calling .fixture(...).
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ParityFailure::new(scenario::STATISTICS_DB)
            .lane("statistics_db")
            .cassandra_source("MetadataSerializer TOC CRC32 (Statistics.db)")
            .components(["Statistics.db"])
            .detail("forced mismatch with no fixture attached")
            .panic();
    }));
    assert!(result.is_err(), "panic() must abort the failing lane");

    assert!(
        record_path.is_file(),
        "a no-fixture ParityFailure::panic must still emit {}",
        record_path.display()
    );

    let text = std::fs::read_to_string(&record_path).expect("read record");
    let value: serde_json::Value = serde_json::from_str(&text).expect("record is JSON");

    // The record still joins back to the manifest and uses the two sentinels.
    assert_eq!(value["scenario_id"].as_str(), Some(desc.cass_scenario_id));
    let prov = &value["provenance"];
    assert_eq!(
        prov["fixture_path"].as_str(),
        Some("<unknown>"),
        "no-fixture record uses the <unknown> fixture_path sentinel (schema minLength: 1)"
    );
    let sha = prov["dataset_sha256"].as_str().expect("dataset_sha256");
    assert_eq!(
        sha,
        "0".repeat(64),
        "no-fixture record uses the all-zero dataset_sha256 sentinel"
    );
    // Sentinels must satisfy the schema patterns (fixture_path minLength 1;
    // dataset_sha256 ^[0-9a-f]{64}$).
    assert!(!prov["fixture_path"].as_str().unwrap_or("").is_empty());
    assert!(sha.len() == 64 && sha.chars().all(|c| c.is_ascii_hexdigit()));

    std::env::remove_var("CQLITE_PARITY_FAILURES_ROOT");
}

/// The `cass.*` ids every wired suite binds to MUST be real manifest scenario ids
/// (never invented) so a bundle joins straight back to the manifest.
#[test]
fn bundle_descriptors_are_real_manifest_ids() {
    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("test-data/cassandra-parity-manifest.yml");
    let manifest = std::fs::read_to_string(&manifest_path).expect("manifest exists");

    for suite in [
        scenario::INDEX_DB_BIG,
        scenario::SUMMARY_DB_BIG,
        scenario::STATISTICS_DB,
        scenario::DATA_DB_JSONL,
        scenario::COMPONENT_MANIFEST,
        scenario::COMPRESSION_INFO_CHUNKS,
    ] {
        let desc = bundle_descriptor_for_suite(suite)
            .unwrap_or_else(|| panic!("suite {suite} must be wired to a bundle descriptor"));
        let needle = format!("- id: {}", desc.cass_scenario_id);
        assert!(
            manifest.contains(&needle),
            "bundle descriptor for suite {suite} must name a REAL manifest id: {}",
            desc.cass_scenario_id
        );
    }
}
