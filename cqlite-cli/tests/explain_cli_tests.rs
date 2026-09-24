//! Public-surface checks for `cqlite explain` (issue #4193).
//!
//! These tests intentionally spawn the binary. They prove the early dispatch,
//! exit codes, fixture resolution, and output contracts rather than testing a
//! helper in isolation.

use assert_cmd::Command;
use serde_json::Value;
use std::path::PathBuf;

fn schema_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test-data/schemas/explain-trace.cql")
}

fn datasets_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test-data/datasets")
}

fn explain_command() -> Command {
    let mut command = Command::cargo_bin("cqlite").expect("cqlite binary is built");
    command.env("CQLITE_DATASETS_ROOT", datasets_root()).args([
        "--schema",
        schema_path().to_str().expect("schema path is UTF-8"),
        "--dataset",
        "test_explain",
        "explain",
        "test_explain.trace_decisions",
        "1",
        "--now",
        "1789963136",
    ]);
    command
}

#[test]
fn explain_json_reports_all_generations_and_stable_keys() {
    let output = explain_command()
        .args(["--clustering", "1", "--out", "json"])
        .output()
        .expect("explain process starts");
    assert!(
        output.status.success(),
        "explain failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let lines = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let mut lines = lines.lines();
    let metadata = lines.next().expect("metadata line");
    assert!(metadata.contains("now=1789963136"));
    assert!(metadata.contains("generations=2"));
    let report: Value =
        serde_json::from_str(lines.next().expect("JSON report")).expect("second line is JSON");
    let object = report.as_object().expect("report object");
    for key in ["now", "generations", "cells", "tombstones"] {
        assert!(object.contains_key(key), "missing report key {key}");
    }
    assert_eq!(object["now"], 1_789_963_136_i64);
    assert_eq!(object["generations"].as_array().unwrap().len(), 2);
    assert!(object["cells"].as_array().unwrap().iter().all(|cell| {
        [
            "column",
            "clustering",
            "generation",
            "writetime",
            "ttl",
            "expires_at",
            "value",
            "verdict",
            "decided_by",
        ]
        .iter()
        .all(|key| cell.get(key).is_some())
    }));
}

/// Partition 3 (id=3) carries a range tombstone `[1,2]` shadowing `ck=1` AND a
/// live winner at `ck=3` outside that range (test-data/datasets/sstables/
/// test_explain/README.md). Filtering `--clustering 3` renders only the ck=3
/// cell, so this is the only partition in the fixture that can prove a range
/// tombstone stays visible in `tombstones` while the cell list is filtered
/// (R7.3) — id=1 (used by `explain_command()` for the other cases) has no
/// tombstone at all.
#[test]
fn explain_csv_keeps_tombstones_in_the_render_only_filter() {
    let mut command = Command::cargo_bin("cqlite").expect("cqlite binary is built");
    let output = command
        .env("CQLITE_DATASETS_ROOT", datasets_root())
        .args([
            "--schema",
            schema_path().to_str().expect("schema path is UTF-8"),
            "--dataset",
            "test_explain",
            "explain",
            "test_explain.trace_decisions",
            "3",
            "--now",
            "1789963136",
            "--clustering",
            "3",
            "--out",
            "csv",
        ])
        .output()
        .expect("explain process starts");
    assert!(
        output.status.success(),
        "explain failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(stdout.lines().next().unwrap().contains("generations=2"));
    assert!(stdout.contains("column,clustering,generation,writetime"));
    assert!(
        stdout.contains("tombstone:range"),
        "the range tombstone covering ck=1 must remain visible when \
         --clustering filters the cell list to ck=3 only; stdout: {stdout}"
    );
    assert!(
        !stdout.contains("range-shadowed"),
        "the ck=1 cell must be filtered out of the cell list by --clustering 3"
    );
}

#[test]
fn explain_missing_schema_is_usage_exit_one() {
    let mut command = Command::cargo_bin("cqlite").expect("cqlite binary is built");
    let output = command
        .env("CQLITE_DATASETS_ROOT", datasets_root())
        .args([
            "--dataset",
            "test_explain",
            "explain",
            "test_explain.trace_decisions",
            "1",
        ])
        .output()
        .expect("explain process starts");
    assert_eq!(output.status.code(), Some(1));
    assert!(
        output.stdout.is_empty(),
        "usage errors must not write stdout"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("--schema is required"));
}

#[test]
fn explain_without_now_marks_wall_clock_in_metadata() {
    let mut command = Command::cargo_bin("cqlite").expect("cqlite binary is built");
    let output = command
        .env("CQLITE_DATASETS_ROOT", datasets_root())
        .args([
            "--schema",
            schema_path().to_str().expect("schema path is UTF-8"),
            "--dataset",
            "test_explain",
            "explain",
            "test_explain.trace_decisions",
            "1",
            "--clustering",
            "1",
            "--out",
            "json",
        ])
        .output()
        .expect("explain process starts");
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout)
        .lines()
        .next()
        .expect("metadata line")
        .contains("(wall clock)"));
}

#[test]
fn explain_budget_failure_is_exit_two_without_partial_stdout() {
    let mut command = explain_command();
    let output = command
        .env("CQLITE_MAX_RESULT_BYTES", "1")
        .args(["--clustering", "1", "--out", "json"])
        .output()
        .expect("explain process starts");
    assert_eq!(output.status.code(), Some(2));
    assert!(
        output.stdout.is_empty(),
        "budget errors must not write partial output"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("materialization budget"));
}

#[test]
fn explain_help_is_available_without_database_initialization() {
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary is built")
        .args(["explain", "--help"])
        .output()
        .expect("help process starts");
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("reconciliation decisions"));
}
