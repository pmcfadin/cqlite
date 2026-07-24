//! End-to-end wiring evidence for the `read-commitlog` subcommand (issue #2389).
//!
//! Proves the full call chain CLI entry point → `CommitLogReader` → decoded
//! mutations by invoking the built `cqlite` binary against a real
//! Cassandra-5.0.2-produced segment fixture committed under
//! `test-data/datasets/commitlog/` (see `generate-commitlog-fixtures.sh`). The
//! fixtures are small committed reference binaries, so this test never silently
//! passes on missing data.

use assert_cmd::Command;
use std::path::PathBuf;

fn commitlog_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("test-data/datasets/commitlog")
}

fn clean_fixture() -> PathBuf {
    find_fixture("clean-")
}

fn corrupt_crc_fixture() -> PathBuf {
    find_fixture("corrupt-crc-")
}

fn find_fixture(prefix: &str) -> PathBuf {
    let dir = commitlog_dir();
    for entry in std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read commitlog dir {}: {e}", dir.display()))
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(prefix) && name.ends_with(".log") {
            return entry.path();
        }
    }
    panic!("no {prefix}*.log fixture under {}", dir.display());
}

/// Ground-truth table id recorded alongside the fixture.
fn ground_truth_table_id() -> String {
    let path = commitlog_dir().join("commitlog-ground-truth.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read ground truth {}: {e}", path.display()));
    let v: serde_json::Value = serde_json::from_str(&text).expect("parse ground truth");
    v["table_id"]
        .as_str()
        .expect("ground-truth table_id")
        .to_string()
}

/// Requirement: CLI subcommand reads a real segment end to end.
#[test]
fn read_commitlog_json_reports_descriptor_and_mutations() {
    let fixture = clean_fixture();
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary should be built for integration tests")
        .arg("--quiet")
        .arg("read-commitlog")
        .arg(&fixture)
        .arg("--format")
        .arg("json")
        .output()
        .expect("read-commitlog command should execute");

    assert!(
        output.status.success(),
        "read-commitlog exited non-zero: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout must be valid JSON");

    // Descriptor parsed authoritatively from the real segment.
    assert_eq!(parsed["descriptor"]["version"], 7, "descriptor version");
    assert!(
        parsed["descriptor"]["compression"].is_null(),
        "clean fixture is uncompressed"
    );
    assert_eq!(parsed["truncated"], false, "clean fixture not truncated");
    assert_eq!(parsed["decode_error"], false, "no decode error");

    let count = parsed["mutation_count"].as_u64().expect("mutation_count");
    assert!(count > 0, "segment must decode at least one mutation");

    // The full call chain reached our inserted table's partition updates.
    let table_id = ground_truth_table_id();
    let updates = parsed["updates"].as_array().expect("updates array");
    let saw_our_table = updates
        .iter()
        .any(|u| u["table_id"].as_str() == Some(table_id.as_str()));
    assert!(
        saw_our_table,
        "decoded stream must include the inserted table id {table_id}"
    );
}

/// `--limit 1` stops before the segment's true tail, so `limited` is reported
/// true and `truncated`/`decode_error` are JSON `null` (never a false-looking
/// `false`) since the tail state was never reached (roborev finding).
#[test]
fn read_commitlog_limit_one_reports_limited_and_null_truncated() {
    let fixture = clean_fixture();
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary should be built for integration tests")
        .arg("--quiet")
        .arg("read-commitlog")
        .arg(&fixture)
        .arg("--format")
        .arg("json")
        .arg("--limit")
        .arg("1")
        .output()
        .expect("read-commitlog command should execute");

    assert!(
        output.status.success(),
        "read-commitlog --limit 1 exited non-zero: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout must be valid JSON");
    assert_eq!(parsed["limited"], true, "--limit 1 must report limited");
    assert!(
        parsed["truncated"].is_null(),
        "truncated must be null when limited, not a false-looking false"
    );
    assert!(
        parsed["decode_error"].is_null(),
        "decode_error must be null when limited"
    );
}

/// `--limit 0` yields zero mutations (its own short-circuit) and still reports
/// `limited` true.
#[test]
fn read_commitlog_limit_zero_reports_zero_mutations() {
    let fixture = clean_fixture();
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary should be built for integration tests")
        .arg("--quiet")
        .arg("read-commitlog")
        .arg(&fixture)
        .arg("--format")
        .arg("json")
        .arg("--limit")
        .arg("0")
        .output()
        .expect("read-commitlog command should execute");

    assert!(
        output.status.success(),
        "read-commitlog --limit 0 exited non-zero: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout must be valid JSON");
    assert_eq!(
        parsed["mutation_count"], 0,
        "--limit 0 must decode zero mutations"
    );
    assert_eq!(parsed["limited"], true, "--limit 0 must report limited");
}

/// The text (default) format also succeeds end to end and reports the segment.
#[test]
fn read_commitlog_text_reports_segment_header() {
    let fixture = clean_fixture();
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary should be built")
        .arg("--quiet")
        .arg("read-commitlog")
        .arg(&fixture)
        .output()
        .expect("read-commitlog command should execute");

    assert!(output.status.success(), "read-commitlog exited non-zero");
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    assert!(
        stdout.contains("CommitLog segment"),
        "text output must report the segment header, got: {stdout}"
    );
    assert!(stdout.contains("version:     7"), "reports version 7");
}

/// Requirement: a mid-stream corrupt record must exit non-zero, not silently
/// succeed with a `decode_error` field buried in the output (roborev finding,
/// review-first pass) — a script piping this command must be able to detect
/// the failure from the exit code alone.
#[test]
fn read_commitlog_exits_non_zero_on_corrupt_stream() {
    let fixture = corrupt_crc_fixture();
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary should be built")
        .arg("--quiet")
        .arg("read-commitlog")
        .arg(&fixture)
        .arg("--format")
        .arg("json")
        .output()
        .expect("read-commitlog command should execute");

    assert!(
        !output.status.success(),
        "a corrupt-CRC segment must exit non-zero, not silently succeed"
    );
    // The already-decoded prefix is still valid JSON on stdout.
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout must still be valid JSON");
    assert_eq!(
        parsed["decode_error"], true,
        "decode_error must be reported"
    );
}
