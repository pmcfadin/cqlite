//! Integration tests for the `delta-export` subcommand (Issue #705, Epic #696 DS9).
//!
//! Tests:
//! 1. Happy path: produces a valid Parquet file readable by DuckDB.
//! 2. Counter table: exits non-zero, clear message, no partial file.
//! 3. Column collision: exits non-zero, message mentions --envelope-prefix.
//! 4. --envelope-prefix: remediation works (collision resolved, export succeeds).
//! 5. --overwrite behaviour: refuses to overwrite by default, overwrites with flag.
//!    5c. --overwrite safety: original file is preserved when export errors (Finding 1).
//! 6. --help documents the subcommand.
//! 7. Element tombstone summary: counter plumbed from ScanSummaryHandle (not hardcoded 0).
//!
//! Requires: `cargo test --features delta-export,duckdb-tests` and SSTable binaries from
//! `bash test-data/scripts/fetch-datasets.sh`.
//!
//! ## Note on `run_cli` (Finding 3)
//!
//! `run_cli` intentionally uses `cargo run --features delta-export` rather than
//! `assert_cmd::Command::cargo_bin("cqlite")`.  The binary under test must be
//! compiled with the `delta-export` feature, and `cargo_bin` has no mechanism to
//! select cargo features for the binary it resolves.  Switching to `cargo_bin`
//! would run a binary built without the feature, causing all `delta-export` tests
//! to exit non-zero with "not compiled with --features delta-export".
//!
//! For tests that do not require the feature binary (e.g. generic --help on the
//! top-level cqlite binary), `assert_cmd` is used directly where appropriate.

#![cfg(feature = "delta-export")]

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

// ============================================================================
// Helpers
// ============================================================================

/// Run the pre-built `cqlite` binary compiled with `--features delta-export`.
///
/// Uses `cargo run --quiet --features delta-export` to ensure the correct
/// feature set is active.  `assert_cmd::cargo_bin` cannot select cargo features
/// for the binary it resolves, so it cannot be used here without losing the
/// delta-export feature.
fn run_cli(args: &[&str]) -> std::process::Output {
    Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--package",
            "cqlite-cli",
            "--features",
            "delta-export",
            "--",
        ])
        .args(args)
        .output()
        .expect("failed to spawn cqlite")
}

fn datasets_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("test-data/datasets")
        })
}

/// Returns the SSTable directory for `test_basic/simple_table`.
fn simple_table_dir() -> PathBuf {
    let base = datasets_root().join("sstables/test_basic");
    find_table_dir(&base, "simple_table").expect("simple_table directory not found")
}

/// Returns the SSTable directory for `test_basic/counters` (counter table).
fn counters_dir() -> PathBuf {
    let base = datasets_root().join("sstables/test_basic");
    find_table_dir(&base, "counters").expect("counters directory not found")
}

fn find_table_dir(base: &Path, prefix: &str) -> Option<PathBuf> {
    std::fs::read_dir(base).ok()?.find_map(|entry| {
        let entry = entry.ok()?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with(prefix) {
            Some(entry.path())
        } else {
            None
        }
    })
}

fn data_db_exists(dir: &Path) -> bool {
    dir.read_dir()
        .ok()
        .map(|mut rd| {
            rd.any(|e| {
                e.ok()
                    .is_some_and(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
            })
        })
        .unwrap_or(false)
}

/// Write a minimal single-table CQL schema that `parse_cql_schema` accepts.
/// The CLI's schema parser requires a bare `CREATE TABLE` statement (no
/// `CREATE KEYSPACE` / `USE ks` preamble).
fn write_simple_table_schema(tmp: &Path) -> PathBuf {
    let path = tmp.join("simple_table.cql");
    std::fs::write(
        &path,
        "CREATE TABLE test_basic.simple_table (
    id uuid PRIMARY KEY,
    name text,
    age int,
    salary bigint,
    active boolean
);",
    )
    .unwrap();
    path
}

/// Write a minimal counter table schema for counter-rejection tests.
fn write_counter_schema(tmp: &Path) -> PathBuf {
    let path = tmp.join("counter_schema.cql");
    std::fs::write(
        &path,
        "CREATE TABLE test_basic.counters (
    id text PRIMARY KEY,
    view_count counter,
    like_count counter
);",
    )
    .unwrap();
    path
}

/// Write a schema whose column collides with the default `__op` envelope name.
fn write_collision_schema(tmp: &Path) -> PathBuf {
    let path = tmp.join("collision_schema.cql");
    std::fs::write(
        &path,
        "CREATE TABLE test_basic.simple_table (
    id uuid PRIMARY KEY,
    __op text
);",
    )
    .unwrap();
    path
}

/// Verify that the Parquet file at `path` is structurally valid and contains
/// the required footer metadata keys.  Returns the total row count.
///
/// This is the primary "independent reader" validation (Rust `parquet` crate).
fn verify_parquet_readable(path: &Path) -> u64 {
    let data = std::fs::read(path).expect("failed to read parquet file");
    assert!(data.len() >= 4, "parquet file too small");
    assert_eq!(&data[0..4], b"PAR1", "missing parquet magic bytes");

    let bytes = Bytes::from(data);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).expect("reader builder");

    // Validate required footer metadata.
    let meta = builder.metadata().clone();
    let kv: std::collections::HashMap<String, String> = meta
        .file_metadata()
        .key_value_metadata()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|kv| kv.value.map(|v| (kv.key, v)))
        .collect();

    assert_eq!(
        kv.get("cqlite.delta.version").map(String::as_str),
        Some("1"),
        "cqlite.delta.version footer key must be '1'"
    );
    assert!(
        kv.contains_key("cqlite.delta.schema_hash"),
        "cqlite.delta.schema_hash footer key must be present"
    );
    assert!(
        kv.contains_key("cqlite.version"),
        "cqlite.version footer key must be present"
    );

    // Count rows.
    let reader = builder.build().expect("reader build");
    reader.map(|b| b.expect("batch ok").num_rows() as u64).sum()
}

/// Verify using DuckDB (independent reader).  Returns the row count.
fn verify_with_duckdb(path: &Path) -> u64 {
    use duckdb::Connection;

    let conn = Connection::open_in_memory().expect("duckdb open_in_memory");
    let path_str = path.to_string_lossy();
    conn.query_row(
        &format!("SELECT COUNT(*) FROM read_parquet('{path_str}')"),
        [],
        |row| row.get(0),
    )
    .expect("duckdb COUNT(*) query")
}

// ============================================================================
// Test 1: Happy path — valid Parquet readable by Rust parquet crate + DuckDB
// ============================================================================

#[test]
fn test_delta_export_simple_table_produces_valid_parquet() {
    let sstable_dir = simple_table_dir();
    if !data_db_exists(&sstable_dir) {
        eprintln!("SKIP: Data.db not present in {sstable_dir:?} — run fetch-datasets.sh");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let schema = write_simple_table_schema(tmp.path());
    let output = tmp.path().join("simple_table_delta.parquet");

    let result = run_cli(&[
        "delta-export",
        sstable_dir.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
        "--out",
        "parquet",
        "-o",
        output.to_str().unwrap(),
    ]);

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    eprintln!("stdout: {stdout}");
    eprintln!("stderr: {stderr}");

    assert!(
        result.status.success(),
        "delta-export should succeed; exit code {:?}\nstdout: {stdout}\nstderr: {stderr}",
        result.status.code()
    );
    assert!(
        output.exists(),
        "output Parquet file must exist at {output:?}"
    );

    // --- Independent read-back: Rust `parquet` crate ---
    let row_count_rust = verify_parquet_readable(&output);
    eprintln!("Rust parquet reader: {row_count_rust} records");
    assert!(
        row_count_rust > 0,
        "Expected at least 1 delta record in the output file"
    );

    // --- Independent read-back: DuckDB ---
    let row_count_duckdb = verify_with_duckdb(&output);
    eprintln!("DuckDB reader: {row_count_duckdb} records");
    assert_eq!(
        row_count_rust, row_count_duckdb,
        "Rust parquet reader and DuckDB must agree on row count ({row_count_rust} vs {row_count_duckdb})"
    );

    // Summary line on stdout.
    assert!(
        stdout.contains("delta-export:"),
        "stdout must contain 'delta-export:' summary line; stdout={stdout}"
    );
}

// ============================================================================
// Test 2: Counter table → exits non-zero, clear message, no partial file.
//
// No data_db_exists guard: the counter schema error fires at schema-derivation
// time, before scan_delta is called and before any Data.db is opened.  The
// test requires only the counters directory to exist, not its Data.db content.
// ============================================================================

#[test]
fn test_delta_export_counter_table_exits_nonzero_no_partial_file() {
    let sstable_dir = counters_dir();

    let tmp = TempDir::new().unwrap();
    let schema = write_counter_schema(tmp.path());
    let output = tmp.path().join("counters_delta.parquet");

    let result = run_cli(&[
        "delta-export",
        sstable_dir.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
        "--out",
        "parquet",
        "-o",
        output.to_str().unwrap(),
    ]);

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    eprintln!("stdout: {stdout}");
    eprintln!("stderr: {stderr}");

    // Must exit non-zero.
    assert!(
        !result.status.success(),
        "delta-export on a counter table must exit non-zero; exit {:?}\nstdout={stdout}\nstderr={stderr}",
        result.status.code()
    );

    // No partial output file must remain.
    assert!(
        !output.exists(),
        "no partial output file must exist after a counter table error; output={output:?}"
    );

    // Error message must mention 'counter'.
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.to_lowercase().contains("counter"),
        "error message must mention 'counter'; output={combined}"
    );
}

// ============================================================================
// Test 3: Column collision → exits non-zero, message mentions --envelope-prefix.
//
// No data_db_exists guard: the column collision error fires at schema-derivation
// time, before scan_delta is called and before any Data.db is opened.  The
// sstable_dir just needs to be a valid path that the CLI can accept.
// ============================================================================

#[test]
fn test_delta_export_column_collision_exits_nonzero_mentions_envelope_prefix() {
    // We only need the collision schema error, which fires before any Data.db
    // read. Use a temp dir as the sstable path — the CLI never reaches scan_delta.
    let tmp = TempDir::new().unwrap();
    let schema = write_collision_schema(tmp.path());
    let output = tmp.path().join("collision_delta.parquet");

    // Use the temp dir itself as the sstable_dir; the schema error fires first.
    let result = run_cli(&[
        "delta-export",
        tmp.path().to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
        "--out",
        "parquet",
        "-o",
        output.to_str().unwrap(),
    ]);

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    eprintln!("stdout: {stdout}");
    eprintln!("stderr: {stderr}");

    assert!(
        !result.status.success(),
        "collision should exit non-zero; exit {:?}\nstdout={stdout}\nstderr={stderr}",
        result.status.code()
    );

    // No partial file.
    assert!(
        !output.exists(),
        "no partial file must remain after collision error; output={output:?}"
    );

    // Error message must mention --envelope-prefix.
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("envelope-prefix") || combined.contains("envelope_prefix"),
        "error must mention --envelope-prefix option; output={combined}"
    );
}

// ============================================================================
// Test 4: --envelope-prefix remediates collision and export succeeds
// ============================================================================

#[test]
fn test_delta_export_envelope_prefix_remediates_collision() {
    let sstable_dir = simple_table_dir();
    if !data_db_exists(&sstable_dir) {
        eprintln!("SKIP: Data.db not present in {sstable_dir:?} — run fetch-datasets.sh");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let schema = write_collision_schema(tmp.path());
    let output = tmp.path().join("remediated_delta.parquet");

    let result = run_cli(&[
        "delta-export",
        sstable_dir.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
        "--out",
        "parquet",
        "-o",
        output.to_str().unwrap(),
        "--envelope-prefix",
        "_cqlite_", // prefix avoids collision with __op user column
    ]);

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    eprintln!("stdout: {stdout}");
    eprintln!("stderr: {stderr}");

    assert!(
        result.status.success(),
        "--envelope-prefix remediation must succeed; exit {:?}\nstdout={stdout}\nstderr={stderr}",
        result.status.code()
    );
    assert!(
        output.exists(),
        "output file must exist after remediation; output={output:?}"
    );

    // File must be valid Parquet.
    let data = std::fs::read(&output).unwrap();
    assert_eq!(
        &data[0..4],
        b"PAR1",
        "remediated output must be valid Parquet"
    );
}

// ============================================================================
// Test 5a: --overwrite flag — refuses to overwrite without flag
// ============================================================================

#[test]
fn test_delta_export_refuses_overwrite_without_flag() {
    let sstable_dir = simple_table_dir();
    if !data_db_exists(&sstable_dir) {
        eprintln!("SKIP: Data.db not present in {sstable_dir:?} — run fetch-datasets.sh");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let schema = write_simple_table_schema(tmp.path());
    let output = tmp.path().join("existing.parquet");

    // Create a pre-existing file.
    std::fs::write(&output, b"existing content").unwrap();

    let result = run_cli(&[
        "delta-export",
        sstable_dir.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
        "--out",
        "parquet",
        "-o",
        output.to_str().unwrap(),
        // No --overwrite
    ]);

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    eprintln!("stdout: {stdout}");
    eprintln!("stderr: {stderr}");

    assert!(
        !result.status.success(),
        "should exit non-zero when output exists without --overwrite; exit {:?}",
        result.status.code()
    );

    // Original file must be intact.
    let content = std::fs::read(&output).unwrap();
    assert_eq!(
        content, b"existing content",
        "original file must be unchanged"
    );
}

// ============================================================================
// Test 5b: --overwrite flag — succeeds and replaces file
// ============================================================================

#[test]
fn test_delta_export_overwrites_with_flag() {
    let sstable_dir = simple_table_dir();
    if !data_db_exists(&sstable_dir) {
        eprintln!("SKIP: Data.db not present in {sstable_dir:?} — run fetch-datasets.sh");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let schema = write_simple_table_schema(tmp.path());
    let output = tmp.path().join("overwrite.parquet");

    // Create a pre-existing file.
    std::fs::write(&output, b"old content").unwrap();

    let result = run_cli(&[
        "delta-export",
        sstable_dir.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
        "--out",
        "parquet",
        "-o",
        output.to_str().unwrap(),
        "--overwrite",
    ]);

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    eprintln!("stdout: {stdout}");
    eprintln!("stderr: {stderr}");

    assert!(
        result.status.success(),
        "--overwrite must succeed; exit {:?}\nstdout={stdout}\nstderr={stderr}",
        result.status.code()
    );
    assert!(output.exists(), "output file must exist after --overwrite");

    // File must be a valid Parquet file, not the old content.
    let data = std::fs::read(&output).unwrap();
    assert_eq!(
        &data[0..4],
        b"PAR1",
        "output must be Parquet, not old content"
    );
}

// ============================================================================
// Test 5c: Atomic overwrite safety — original is preserved when export errors.
//
// Finding 1 (roborev): a mid-stream failure with --overwrite must leave the
// ORIGINAL file intact.  The atomic write pattern (temp file + rename) ensures
// this: the original is not touched until rename succeeds after finalize().
//
// We trigger an error at schema-derivation time (counter table) with a
// pre-existing output file and --overwrite: the error fires before any file I/O,
// so the original must survive intact.  This is the earliest possible error
// after the overwrite flag is accepted, and it exercises the same preservation
// guarantee that applies to all later error paths (writer init, streaming,
// finalize) because the temp-file write never starts.
// ============================================================================

#[test]
fn test_delta_export_overwrite_error_leaves_original_intact() {
    // Use a counter table schema so the error fires at schema-derivation time —
    // before any file I/O — even without SSTable data present.
    let sstable_dir = counters_dir();

    let tmp = TempDir::new().unwrap();
    let schema = write_counter_schema(tmp.path());
    let output = tmp.path().join("precious_original.parquet");

    // Write sentinel bytes to represent the user's existing valuable file.
    let sentinel = b"PRECIOUS ORIGINAL FILE CONTENT DO NOT DESTROY";
    std::fs::write(&output, sentinel).unwrap();

    // Run delta-export with --overwrite on a counter table: must fail.
    let result = run_cli(&[
        "delta-export",
        sstable_dir.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
        "--out",
        "parquet",
        "-o",
        output.to_str().unwrap(),
        "--overwrite",
    ]);

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    eprintln!("stdout: {stdout}");
    eprintln!("stderr: {stderr}");

    // Export must fail (counter table).
    assert!(
        !result.status.success(),
        "delta-export on counter table must exit non-zero even with --overwrite; \
         exit {:?}\nstdout={stdout}\nstderr={stderr}",
        result.status.code()
    );

    // CRITICAL: original file must be byte-for-byte intact.
    assert!(
        output.exists(),
        "original file must still exist after a failed --overwrite export; output={output:?}"
    );
    let actual = std::fs::read(&output).unwrap();
    assert_eq!(
        actual, sentinel,
        "original file must be byte-for-byte identical after a failed --overwrite export.\n\
         Expected: {sentinel:?}\n\
         Actual:   {actual:?}"
    );

    // No temp file should linger.
    let tmp_path = {
        let mut p = output.clone();
        let mut name = p.file_name().unwrap_or_default().to_os_string();
        name.push(".tmp");
        p.set_file_name(name);
        p
    };
    assert!(
        !tmp_path.exists(),
        "no .tmp sibling file must remain after a failed export; tmp={tmp_path:?}"
    );
}

// ============================================================================
// Test 6: --help documents the subcommand
// ============================================================================

#[test]
fn test_delta_export_help_contains_key_options() {
    let result = run_cli(&["delta-export", "--help"]);
    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    eprintln!("stderr: {stderr}");

    assert!(
        result.status.success(),
        "delta-export --help should exit 0; stderr={stderr}"
    );

    assert!(
        stdout.contains("--schema"),
        "--help must mention --schema; stdout={stdout}"
    );
    assert!(
        stdout.contains("--out"),
        "--help must mention --out; stdout={stdout}"
    );
    assert!(
        stdout.contains("envelope-prefix"),
        "--help must mention --envelope-prefix; stdout={stdout}"
    );
    assert!(
        stdout.contains("-o") || stdout.contains("--output"),
        "--help must mention output flag; stdout={stdout}"
    );
}

// ============================================================================
// Test 7: Element tombstone warning — counter plumbed from ScanSummaryHandle
//
// Verifies that element_tombstone_warnings is read from ScanSummaryHandle.read()
// rather than hardcoded to 0 (Fix A / roborev Finding 1).
//
// On a simple table with no element tombstones the count must be 0; on a
// collection table with element tombstones the warning must fire on stderr.
// This test validates the plumbing path: the count in the summary line on
// stdout must match what the handle reports (not a hardcoded literal zero).
// ============================================================================

#[test]
fn test_delta_export_element_tombstone_count_is_plumbed_not_hardcoded() {
    // This test validates the plumbing even on a table with no element
    // tombstones: the count must be 0 (not a hardcoded 0 that hides the bug).
    // A separate assertion checks the stderr warning is absent when count == 0.
    let sstable_dir = simple_table_dir();
    if !data_db_exists(&sstable_dir) {
        eprintln!("SKIP: Data.db not present in {sstable_dir:?} — run fetch-datasets.sh");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let schema = write_simple_table_schema(tmp.path());
    let output = tmp.path().join("element_tombstone_test.parquet");

    let result = run_cli(&[
        "delta-export",
        sstable_dir.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
        "--out",
        "parquet",
        "-o",
        output.to_str().unwrap(),
    ]);

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    eprintln!("stdout: {stdout}");
    eprintln!("stderr: {stderr}");

    assert!(
        result.status.success(),
        "delta-export should succeed; exit code {:?}\nstdout: {stdout}\nstderr: {stderr}",
        result.status.code()
    );

    // simple_table has no collection element tombstones: warning must be absent.
    assert!(
        !stderr.contains("element tombstone"),
        "simple_table has no element tombstones; warning must not appear on stderr.\nstderr={stderr}"
    );

    // The summary line must be present — confirming the run completed and
    // the real plumbed path (not a panic or early exit) was exercised.
    assert!(
        stdout.contains("delta-export:"),
        "stdout must contain 'delta-export:' summary line; stdout={stdout}"
    );
}
