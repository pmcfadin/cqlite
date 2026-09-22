//! Issue #4194 (corruption-locator, spec verify-location L4.2) — `cqlite
//! verify --mode full` renders `location` in both `--out text` and `--out
//! json`, and a finding with no location (e.g. a companion `DigestMismatch`)
//! renders exactly as it did before this change.
//!
//! Drives the REAL compiled `cqlite` binary (`CARGO_BIN_EXE_cqlite`) — the
//! existing `execute_verify_command` exit-code contract (non-zero on any
//! finding) is only observable through a subprocess.
//!
//! Dataset doctrine (issue #1094): SKIP when the corruption corpus binaries
//! are absent; `CQLITE_REQUIRE_FIXTURES=1` turns that into a hard failure.

use std::path::PathBuf;
use std::process::Command;

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.is_dir())
}

/// The `data_db_bit_flip` corruption fixture directory, gated per #1094
/// doctrine: `None` (after an eprintln SKIP, or a panic under
/// `CQLITE_REQUIRE_FIXTURES=1`) when unusable.
fn data_db_bit_flip_dir() -> Option<PathBuf> {
    let Some(root) = datasets_root() else {
        assert!(
            !require_fixtures_strict(),
            "CQLITE_REQUIRE_FIXTURES=1 but CQLITE_DATASETS_ROOT is unset/not a directory"
        );
        eprintln!("SKIP: CQLITE_DATASETS_ROOT unset/not a directory");
        return None;
    };
    let dir = root.join("corruption/test_comp_corrupt/data_db_bit_flip");
    let has_data_db = std::fs::read_dir(&dir)
        .map(|rd| {
            rd.flatten().any(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    if !has_data_db {
        assert!(
            !require_fixtures_strict(),
            "CQLITE_REQUIRE_FIXTURES=1 but data_db_bit_flip is unusable at {}",
            dir.display()
        );
        eprintln!("SKIP: data_db_bit_flip unusable at {}", dir.display());
        return None;
    }
    Some(dir)
}

fn run_verify(dir: &std::path::Path, out: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(["verify", &dir.display().to_string(), "--mode", "full", "--out", out])
        .output()
        .expect("spawn cqlite verify");
    // verify_sstable's contract (unchanged by this issue): exit 2 on any
    // finding, never a usage-error exit. Assert it here so a future
    // regression in exit-code plumbing fails this suite too, not just
    // silently produces empty stdout.
    assert_eq!(
        output.status.code(),
        Some(2),
        "expected exit 2 (verification failed) for a corrupt fixture; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("utf8 stdout")
}

#[test]
fn cli_text_output_names_the_chunk_index_and_a_partition_key() {
    let Some(dir) = data_db_bit_flip_dir() else {
        return;
    };
    let stdout = run_verify(&dir, "text");
    assert!(
        stdout.contains("ChunkDecompressionError"),
        "stdout did not name the finding class: {stdout}"
    );
    assert!(
        stdout.contains("location:") && stdout.contains("chunk 0"),
        "stdout did not render a location naming chunk 0: {stdout}"
    );
    // The oracle-pinned partition key for this fixture (issue_4194_verify_location.rs
    // asserts the full set independently) — this CLI-level check only needs
    // AT LEAST one partition key rendered, per spec L4.2's "at least one".
    assert!(
        stdout.contains("partition(s):"),
        "stdout did not render any partition key: {stdout}"
    );
}

#[test]
fn cli_json_output_carries_a_location_object_with_chunk_index_and_partitions() {
    let Some(dir) = data_db_bit_flip_dir() else {
        return;
    };
    let stdout = run_verify(&dir, "json");
    let value: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("cqlite verify --out json did not emit valid JSON: {e}\n{stdout}"));
    let findings = value["findings"].as_array().expect("findings array");

    let chunk_finding = findings
        .iter()
        .find(|f| f["class"] == "ChunkDecompressionError")
        .unwrap_or_else(|| panic!("no ChunkDecompressionError finding in {value}"));
    let location = &chunk_finding["location"];
    assert!(!location.is_null(), "ChunkDecompressionError finding's location was null: {value}");
    assert_eq!(location["chunk_index"], 0);
    assert_eq!(location["component"], "Data.db");
    let resolved = location["partitions"]["resolved"]
        .as_array()
        .unwrap_or_else(|| panic!("location.partitions was not {{\"resolved\": [...]}}: {location}"));
    assert!(!resolved.is_empty(), "resolved partitions array was empty: {location}");
    assert!(
        resolved[0]["key_hex"].is_string(),
        "resolved partition entry missing key_hex: {location}"
    );

    // A companion finding with no natural byte range (DigestMismatch, a
    // whole-file check) renders EXACTLY as it did before this change — its
    // `location` is JSON `null`, additive-only (spec L4.2).
    let digest_finding = findings
        .iter()
        .find(|f| f["class"] == "DigestMismatch")
        .unwrap_or_else(|| panic!("no DigestMismatch finding in {value}"));
    assert!(
        digest_finding["location"].is_null(),
        "DigestMismatch (no natural byte range) unexpectedly carried a location: {digest_finding}"
    );
}
