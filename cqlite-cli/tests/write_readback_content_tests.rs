//! Content-asserting write→read round-trips through the public CLI (Issue #1231).
//!
//! Unlike `cli_dml_integration_tests.rs` (which asserts only exit-code + "OK"),
//! every test here drives the FULL public chain a user hits:
//!
//!   CQL string → parser → Mutation → WriteEngine → WAL → flush → SSTable
//!     → independent reopen via `--data-dir` → SELECT → assert decoded VALUES
//!
//! A write-format/encoding regression that emits a structurally-present but
//! semantically-WRONG Data.db will turn these red. The earlier shape-only tests
//! could not (the "CI blind to the write path" hazard, epic #1227).
//!
//! Invocation model (proven against the real binary): the one-shot CLI handles
//! `--flush` BEFORE `--execute`, and `--execute INSERT` returns early, so a
//! single `--execute INSERT --flush` invocation does NOT persist (see the
//! discovered-bug note in the issue report). The durable, user-reachable path is
//! WAL-backed across invocations:
//!
//! 1. `--execute "INSERT/UPDATE/DELETE"` appends to the WAL,
//! 2. a later `--writable --write-dir <wd> --flush` replays the WAL into the
//!    memtable and flushes a real SSTable under `<wd>/data`,
//! 3. a read-only `--data-dir <wd>/data --execute "SELECT ..."` reopens and
//!    decodes it.
//!
//! These tests use that path and assert the decoded column values.

#![cfg(feature = "write-support")]

use serde_json::Value as Json;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

/// The pre-built `cqlite` binary for this test crate. Using `CARGO_BIN_EXE_*`
/// runs the exact binary the harness built with `--features write-support`,
/// without spawning a nested `cargo run` rebuild.
fn cqlite_bin() -> &'static str {
    env!("CARGO_BIN_EXE_cqlite")
}

/// Write the single-table schema used by the round-trip tests.
fn write_schema(dir: &Path) -> PathBuf {
    let path = dir.join("schema.cql");
    std::fs::write(
        &path,
        r#"
CREATE KEYSPACE IF NOT EXISTS test_write WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE test_write;

CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name TEXT,
    age INT,
    active BOOLEAN
);
"#,
    )
    .expect("write schema file");
    path
}

/// Run the binary with the given args and return the captured output.
fn run(args: &[&str]) -> Output {
    Command::new(cqlite_bin())
        .args(args)
        .output()
        .expect("spawn cqlite binary")
}

/// Run a write-side invocation (`--writable --write-dir <wd> --schema <s> ...`).
fn write_cmd(wd: &Path, schema: &Path, extra: &[&str]) -> Output {
    let mut args = vec![
        "--writable",
        "--write-dir",
        wd.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
    ];
    args.extend_from_slice(extra);
    run(&args)
}

/// Execute a single DML statement against the WAL (`--execute`), asserting
/// success + the "OK" acknowledgement.
fn dml(wd: &Path, schema: &Path, stmt: &str) {
    let out = write_cmd(wd, schema, &["--execute", stmt]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "DML failed: `{stmt}`\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("OK"),
        "expected OK for `{stmt}`, got: {stdout}"
    );
}

/// Flush the WAL-backed memtable to a real SSTable under `<wd>/data`.
fn flush(wd: &Path, schema: &Path) {
    let out = write_cmd(wd, schema, &["--flush"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "flush failed\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("Flushed:"),
        "expected a real flush (memtable non-empty after WAL replay), got: {stdout}"
    );
}

/// Reopen an SSTable directory read-only and SELECT, returning the rows as a
/// JSON array (the `--out json` payload on stdout; tracing logs go to stderr).
fn select_rows(data_dir: &Path, schema: &Path, query: &str) -> Vec<Json> {
    let out = run(&[
        "--data-dir",
        data_dir.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
        "--execute",
        query,
        "--out",
        "json",
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "SELECT failed: `{query}`\nstdout: {stdout}\nstderr: {stderr}"
    );
    let parsed: Json = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("SELECT did not emit JSON: {e}\nstdout: {stdout}"));
    match parsed {
        Json::Array(rows) => rows,
        other => panic!("expected a JSON array of rows, got: {other}"),
    }
}

/// Find the row whose `id` column equals `id`.
fn row_with_id(rows: &[Json], id: i64) -> Option<&Json> {
    rows.iter()
        .find(|r| r.get("id").and_then(|v| v.as_i64()) == Some(id))
}

// ===========================================================================
// INSERT: values survive the round-trip unchanged
// ===========================================================================

#[test]
fn cli_insert_flush_reopen_select_asserts_values() {
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");

    dml(
        &wd,
        &schema,
        "INSERT INTO test_write.users (id, name, age, active) VALUES (1, 'Alice', 30, true)",
    );
    flush(&wd, &schema);

    let rows = select_rows(&wd.join("data"), &schema, "SELECT * FROM test_write.users");
    assert_eq!(rows.len(), 1, "exactly one row expected, got: {rows:?}");
    let row = &rows[0];
    assert_eq!(row["id"].as_i64(), Some(1), "id value mismatch in {row}");
    assert_eq!(
        row["name"].as_str(),
        Some("Alice"),
        "name value mismatch in {row}"
    );
    assert_eq!(row["age"].as_i64(), Some(30), "age value mismatch in {row}");
    assert_eq!(
        row["active"].as_bool(),
        Some(true),
        "active value mismatch in {row}"
    );
}

// ===========================================================================
// UPDATE: a later write overwrites the earlier value (last-write-wins)
// ===========================================================================

#[test]
fn cli_update_overwrite_wins_on_readback() {
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");

    dml(
        &wd,
        &schema,
        "INSERT INTO test_write.users (id, name, age, active) VALUES (1, 'Alice', 30, true)",
    );
    dml(
        &wd,
        &schema,
        "UPDATE test_write.users SET name = 'Alicia', age = 31 WHERE id = 1",
    );
    flush(&wd, &schema);

    let rows = select_rows(&wd.join("data"), &schema, "SELECT * FROM test_write.users");
    assert_eq!(rows.len(), 1, "exactly one row expected, got: {rows:?}");
    let row = &rows[0];
    // Overwritten columns reflect the UPDATE, not the INSERT.
    assert_eq!(
        row["name"].as_str(),
        Some("Alicia"),
        "UPDATE did not win for name: {row}"
    );
    assert_eq!(
        row["age"].as_i64(),
        Some(31),
        "UPDATE did not win for age: {row}"
    );
    // A column the UPDATE did not touch keeps the INSERT value.
    assert_eq!(
        row["active"].as_bool(),
        Some(true),
        "untouched column changed: {row}"
    );
}

// ===========================================================================
// DELETE: the tombstone makes the row absent on read-back
// ===========================================================================

#[test]
fn cli_delete_tombstone_absent_on_readback() {
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");

    dml(
        &wd,
        &schema,
        "INSERT INTO test_write.users (id, name, age, active) VALUES (1, 'Alice', 30, true)",
    );
    dml(
        &wd,
        &schema,
        "INSERT INTO test_write.users (id, name, age, active) VALUES (2, 'Bob', 25, false)",
    );
    dml(&wd, &schema, "DELETE FROM test_write.users WHERE id = 2");
    flush(&wd, &schema);

    let rows = select_rows(&wd.join("data"), &schema, "SELECT * FROM test_write.users");
    assert!(
        row_with_id(&rows, 2).is_none(),
        "deleted row id=2 must be absent, got: {rows:?}"
    );
    let surviving = row_with_id(&rows, 1).expect("surviving row id=1 must be present");
    assert_eq!(
        surviving["name"].as_str(),
        Some("Alice"),
        "survivor corrupted: {surviving}"
    );
    assert_eq!(
        surviving["age"].as_i64(),
        Some(30),
        "survivor corrupted: {surviving}"
    );
}

// ===========================================================================
// AC #2: binary-level `export-sstable` — write → export → reopen the EXPORTED
// SSTable → assert content.
// ===========================================================================

#[test]
fn cli_export_sstable_reopen_exported_asserts_content() {
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");
    let export_dir = tmp.path().join("exported");

    dml(
        &wd,
        &schema,
        "INSERT INTO test_write.users (id, name, age, active) VALUES (7, 'Zara', 42, true)",
    );

    // export-sstable replays the WAL into the memtable and writes a fresh
    // SSTable under <export_dir>/<keyspace>/<table>/.
    let out = write_cmd(
        &wd,
        &schema,
        &[
            "export-sstable",
            export_dir.to_str().unwrap(),
            "--keyspace",
            "test_write",
            "--table",
            "users",
        ],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "export-sstable failed\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("Export complete:"),
        "export shape: {stdout}"
    );

    // Reopen the EXPORTED SSTable (not the write-dir) and assert content.
    let rows = select_rows(&export_dir, &schema, "SELECT * FROM test_write.users");
    assert_eq!(
        rows.len(),
        1,
        "exported SSTable should hold one row, got: {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(row["id"].as_i64(), Some(7), "exported id mismatch: {row}");
    assert_eq!(
        row["name"].as_str(),
        Some("Zara"),
        "exported name mismatch: {row}"
    );
    assert_eq!(
        row["age"].as_i64(),
        Some(42),
        "exported age mismatch: {row}"
    );
    assert_eq!(
        row["active"].as_bool(),
        Some(true),
        "exported active mismatch: {row}"
    );
}

// ===========================================================================
// AC #2: subcommand glue smoke — flags parse, `--writable` gating, output shape
// for write-stats / maintenance / compact.
//
// NOTE: the exact stdout substrings these tests match ("Write Engine
// Statistics:", "Maintenance complete:", "OK: compacted", "Export complete:",
// "Flushed:", "OK") are treated as part of the CLI's *output contract*. A
// deliberate wording change in the CLI must update these strings here so the
// behavioral coupling stays visible (roborev judged exact-substring matching
// acceptable for CLI glue smoke). The `--writable` gating checks additionally
// assert the real gate error phrase on stderr ("requires --writable mode"), so
// a command that fails for an unrelated reason cannot satisfy the gating tests.
// ===========================================================================

/// The substring the CLI emits on stderr when a write subcommand is invoked
/// without `--writable`. Asserting this (not just a non-zero exit) proves the
/// failure is the gate, not e.g. an empty/invalid data dir. Confirmed against
/// the real binary: `Error: Write stats requires --writable mode` /
/// `Error: Maintenance requires --writable mode`.
const WRITABLE_GATE_MSG: &str = "requires --writable mode";

#[test]
fn cli_write_stats_subcommand_glue() {
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");

    // With --writable the subcommand runs and prints the stats banner.
    let out = write_cmd(&wd, &schema, &["write-stats"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "write-stats should succeed under --writable"
    );
    assert!(
        stdout.contains("Write Engine Statistics:"),
        "write-stats output shape: {stdout}"
    );

    // Without --writable the subcommand must be gated (no write engine).
    let gated = run(&[
        "--data-dir",
        wd.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
        "write-stats",
    ]);
    let gated_stderr = String::from_utf8_lossy(&gated.stderr);
    assert!(
        !gated.status.success(),
        "write-stats without --writable must fail (gating)"
    );
    assert!(
        gated_stderr.contains(WRITABLE_GATE_MSG),
        "write-stats must fail *because of* the --writable gate; expected stderr to contain {WRITABLE_GATE_MSG:?}, got: {gated_stderr}"
    );
}

#[test]
fn cli_maintenance_subcommand_glue() {
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");

    let out = write_cmd(&wd, &schema, &["maintenance", "--budget-ms", "50"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "maintenance should succeed under --writable"
    );
    assert!(
        stdout.contains("Maintenance complete:"),
        "maintenance output shape: {stdout}"
    );

    // Gating: maintenance without --writable must fail.
    let gated = run(&[
        "--data-dir",
        wd.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
        "maintenance",
        "--budget-ms",
        "50",
    ]);
    let gated_stderr = String::from_utf8_lossy(&gated.stderr);
    assert!(
        !gated.status.success(),
        "maintenance without --writable must fail (gating)"
    );
    assert!(
        gated_stderr.contains(WRITABLE_GATE_MSG),
        "maintenance must fail *because of* the --writable gate; expected stderr to contain {WRITABLE_GATE_MSG:?}, got: {gated_stderr}"
    );
}

#[test]
fn cli_compact_subcommand_glue() {
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");
    let out_dir = tmp.path().join("compacted");

    // Produce one published input SSTable via the write path.
    dml(
        &wd,
        &schema,
        "INSERT INTO test_write.users (id, name, age, active) VALUES (5, 'Cara', 19, true)",
    );
    flush(&wd, &schema);
    let input_dir = wd.join("data").join("test_write").join("users");

    // `compact` is policy-free and operates directly on the input files.
    let out = run(&[
        "compact",
        input_dir.to_str().unwrap(),
        "--output",
        out_dir.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
        "--generation",
        "9",
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "compact failed\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("OK: compacted"),
        "compact output shape: {stdout}"
    );

    // The compacted SSTable must reopen and still decode to the written values.
    let rows = select_rows(&out_dir, &schema, "SELECT * FROM test_write.users");
    assert_eq!(
        rows.len(),
        1,
        "compacted output should hold one row, got: {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(row["id"].as_i64(), Some(5), "compacted id mismatch: {row}");
    assert_eq!(
        row["name"].as_str(),
        Some("Cara"),
        "compacted name mismatch: {row}"
    );
}
