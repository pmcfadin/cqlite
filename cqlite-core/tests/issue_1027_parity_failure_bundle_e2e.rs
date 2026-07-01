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
    // the unwind — the bundle is emitted just before the panic.
    let fixture_for_panic = fixture.clone();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ParityFailure::new(scenario::STATISTICS_DB)
            .lane("statistics_db")
            .cassandra_source("MetadataSerializer TOC CRC32 (Statistics.db)")
            .fixture(fixture_for_panic)
            .components(["Statistics.db"])
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
    assert!(expected_bundle.join("stdout.txt").is_file());
    assert!(expected_bundle.join("stderr.txt").is_file());
    assert!(expected_bundle.join("diffs").is_dir());
    let repro = value["repro_bundle"].as_str().expect("repro_bundle");
    assert!(expected_bundle.join(repro).is_dir());

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
