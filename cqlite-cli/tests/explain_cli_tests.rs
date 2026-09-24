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

/// The committed generation directory backing every `test_explain.trace_decisions`
/// case (both `nb-1-big` and `nb-2-big` live directly under it, matching the
/// `--data-dir` shape `resolve_table_dir` accepts without a `--dataset` prefix).
fn fixture_generation_dir() -> PathBuf {
    datasets_root()
        .join("sstables")
        .join("test_explain")
        .join("trace_decisions-c2f35e90b57011f183ec5947a78bc662")
}

/// Recursively copy `src` into `dst` (created if absent). Used to stage a
/// mutable, disposable copy of the committed fixture for the corruption (R8.1)
/// and read-only (R8.3) cases, which must never touch the git-tracked original.
fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).expect("create dest dir");
    for entry in std::fs::read_dir(src).expect("read src dir") {
        let entry = entry.expect("dir entry");
        let dest_path = dst.join(entry.file_name());
        let file_type = entry.file_type().expect("file type");
        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &dest_path);
        } else {
            std::fs::copy(entry.path(), &dest_path).expect("copy file");
        }
    }
}

/// A sorted `(relative path, contents)` snapshot of every regular file under
/// `dir`, for asserting a directory is byte-identical before and after a run
/// (R8.3: `explain` must never write to the SSTable directory it reads).
fn snapshot_dir(dir: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &std::path::Path, dir: &std::path::Path, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in std::fs::read_dir(dir).expect("read dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if entry.file_type().expect("file type").is_dir() {
                walk(root, &path, out);
            } else {
                let relative = path
                    .strip_prefix(root)
                    .expect("path under root")
                    .to_string_lossy()
                    .into_owned();
                let contents = std::fs::read(&path).expect("read file");
                out.push((relative, contents));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
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
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains("reconciliation decisions"));
    // D7/R5.3: the naming collision with the pre-existing `query --explain`
    // query-plan flag must be documented in the binary itself.
    assert!(help.contains("for a query plan use `query --explain`"));
}

/// R5.1: the exact first-line format
/// `now=<epoch-secs> (<RFC3339>)  generations=<n> [<basename>, …]  gc_grace_seconds=<g>`,
/// with both `nb-*-big` basenames from the two-generation fixture and the
/// table's `gc_grace_seconds = 864000` (test-data/schemas/explain-trace.cql).
#[test]
fn explain_first_line_matches_the_documented_format_exactly() {
    let output = explain_command()
        .args(["--clustering", "1", "--out", "table"])
        .output()
        .expect("explain process starts");
    assert!(
        output.status.success(),
        "explain failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let first_line = stdout.lines().next().expect("first line");
    assert!(
        first_line.starts_with("now=1789963136 ("),
        "first line: {first_line}"
    );
    assert!(
        first_line.contains(") ") && !first_line.contains("(wall clock)"),
        "an explicit --now must render an RFC3339 timestamp, not the wall-clock \
         suffix; first line: {first_line}"
    );
    assert!(
        first_line.contains("generations=2 ["),
        "first line: {first_line}"
    );
    assert!(first_line.contains("nb-1-big"), "first line: {first_line}");
    assert!(first_line.contains("nb-2-big"), "first line: {first_line}");
    assert!(
        first_line.contains("gc_grace_seconds=864000"),
        "first line: {first_line}"
    );
}

/// R7.2: the default `table` output renders one line per cell as
/// `<column>  <sstable>  writetime=<ts>  <verdict>  <decided_by>`.
#[test]
fn explain_table_output_renders_the_documented_per_cell_line_shape() {
    let output = explain_command()
        .args(["--clustering", "1", "--out", "table"])
        .output()
        .expect("explain process starts");
    assert!(
        output.status.success(),
        "explain failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    // Partition 1 (id=1): A's `v='older'` is shadowed-by-timestamp by B's
    // `v='newer'` winner (test_explain/README.md).
    let cell_line = stdout
        .lines()
        .find(|line| line.starts_with("v  "))
        .unwrap_or_else(|| panic!("no `v` cell line in table output:\n{stdout}"));
    assert!(
        cell_line.contains("writetime="),
        "cell line: {cell_line}"
    );
    assert!(
        cell_line.contains("winner") || cell_line.contains("shadowed-by-timestamp"),
        "cell line: {cell_line}"
    );
}

/// R8.1: a generation whose `Statistics.db` cannot be decoded is a fail-closed
/// error naming the generation, never a partial/shorter trail (the #4159
/// class).
#[test]
fn explain_unreadable_generation_is_exit_two_naming_the_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let staged = temp.path().join("trace_decisions-c2f35e90b57011f183ec5947a78bc662");
    copy_dir_recursive(&fixture_generation_dir(), &staged);
    let truncated = staged.join("nb-1-big-Statistics.db");
    assert!(truncated.exists(), "expected {truncated:?} to exist");
    std::fs::write(&truncated, b"not a statistics file").expect("truncate Statistics.db");

    let mut command = Command::cargo_bin("cqlite").expect("cqlite binary is built");
    let output = command
        .args([
            "--schema",
            schema_path().to_str().expect("schema path is UTF-8"),
            "--data-dir",
            staged.to_str().expect("staged path is UTF-8"),
            "explain",
            "test_explain.trace_decisions",
            "1",
            "--now",
            "1789963136",
        ])
        .output()
        .expect("explain process starts");
    assert_eq!(
        output.status.code(),
        Some(2),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.is_empty(),
        "an unreadable generation must never emit a shorter trail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("nb-1-big"),
        "stderr must name the unreadable generation: {stderr}"
    );
}

/// R8.2: a partition key held by no generation renders `generations=<n>` and
/// says so explicitly, exit code 0 (a resolved, empty answer is not a
/// failure).
#[test]
fn explain_key_absent_from_every_generation_is_exit_zero() {
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
            "999999",
            "--now",
            "1789963136",
        ])
        .output()
        .expect("explain process starts");
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(stdout.lines().next().unwrap().contains("generations=2"));
    assert!(
        stdout.contains("0 generations hold this key"),
        "stdout: {stdout}"
    );
}

/// R8.3: `explain` must never write to the SSTable directory it reads, in any
/// output format.
#[test]
fn explain_never_mutates_the_input_directory() {
    let temp = tempfile::tempdir().expect("tempdir");
    let staged = temp.path().join("trace_decisions-c2f35e90b57011f183ec5947a78bc662");
    copy_dir_recursive(&fixture_generation_dir(), &staged);
    let before = snapshot_dir(&staged);
    assert!(!before.is_empty(), "staged fixture copy must be non-empty");

    for out_format in ["table", "json", "csv"] {
        let mut command = Command::cargo_bin("cqlite").expect("cqlite binary is built");
        let output = command
            .args([
                "--schema",
                schema_path().to_str().expect("schema path is UTF-8"),
                "--data-dir",
                staged.to_str().expect("staged path is UTF-8"),
                "explain",
                "test_explain.trace_decisions",
                "1",
                "--now",
                "1789963136",
                "--out",
                out_format,
            ])
            .output()
            .expect("explain process starts");
        assert!(
            output.status.success(),
            "explain ({out_format}) failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let after = snapshot_dir(&staged);
    assert_eq!(
        before, after,
        "explain must not modify the SSTable directory it reads, in any output format"
    );
}
