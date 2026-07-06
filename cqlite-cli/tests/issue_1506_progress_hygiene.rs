//! Issue #1506 (epic #1471 hygiene, finding AF5): legacy subcommands must
//! honor the #284 quiet/tty contract for progress output and must not call
//! `.unwrap()`/`.expect()` on `ProgressStyle::template(...)`.
//!
//! Two guarantees are covered here:
//!   1. A source grep-guard: none of the three legacy command modules build a
//!      `ProgressStyle` template with `.unwrap()`/`.expect()` (they must use the
//!      `if let Ok(style) = ...` fallback pattern from `commands/export.rs`).
//!   2. Runtime: `--quiet` suppresses the progress/status preamble that the
//!      `read-sstable` and `import` subcommands otherwise emit.
//!
//! Both fail on `main` (which panics-on-error and ignores `--quiet`) and pass
//! after the fix.

#![cfg(feature = "state_machine")]

mod common;

use common::{crate_root, datasets_root, find_simple_table_data_db, schemas_dir};
use std::fs;
use std::process::{Command, Output};

/// Run the pre-built CLI binary, capturing stdout/stderr.
fn run_cli_command(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(args)
        .output()
        .expect("Failed to execute CLI command")
}

// ============================================================================
// (1) Source grep-guard: no unwrap()/expect() on ProgressStyle::template(...)
// ============================================================================

/// For each `.template(` occurrence in `path`, assert the enclosing statement
/// (up to the next `;`) does not call `.unwrap()`/`.expect(` on the builder.
fn assert_no_template_unwrap(rel: &str) {
    let path = crate_root().join(rel);
    let src = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    // Collapse whitespace so multi-line builder chains scan as one line.
    let collapsed = src.split_whitespace().collect::<Vec<_>>().join(" ");

    let mut from = 0usize;
    while let Some(rel_idx) = collapsed[from..].find(".template(") {
        let start = from + rel_idx;
        let end = collapsed[start..]
            .find(';')
            .map(|i| start + i)
            .unwrap_or(collapsed.len());
        let stmt = &collapsed[start..end];
        assert!(
            !stmt.contains(".unwrap()") && !stmt.contains(".expect("),
            "{rel}: ProgressStyle template must not use unwrap()/expect(); \
             use the `if let Ok(style) = ...` fallback from commands/export.rs. \
             Offending statement: {stmt}"
        );
        from = end;
    }
}

#[test]
fn test_no_progressstyle_unwrap_in_legacy_commands() {
    assert_no_template_unwrap("src/commands/read_sstable.rs");
    assert_no_template_unwrap("src/commands/export_sstable.rs");
    assert_no_template_unwrap("src/commands/import.rs");
}

// ============================================================================
// (2) Runtime: --quiet suppresses the progress/status preamble
// ============================================================================

#[test]
fn test_read_sstable_quiet_suppresses_progress_preamble() {
    let Some(data_db) = find_simple_table_data_db() else {
        eprintln!("SKIP: no simple_table Data.db under datasets root");
        return;
    };

    let output = run_cli_command(&[
        "--quiet",
        "read-sstable",
        data_db.to_str().unwrap(),
        "--format",
        "json",
        "--limit",
        "1",
    ]);

    assert!(
        output.status.success(),
        "read-sstable --quiet should succeed. stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("Reading SSTable")
            && !stderr.contains("Displaying")
            && !stderr.contains("Displayed"),
        "read-sstable --quiet must emit no progress/status preamble on stderr. Got:\n{stderr}"
    );
}

/// FINDING 2 (roborev, Low): `read-sstable --quiet --verbose` must NOT write the
/// verbose SSTable statistics block to stderr — quiet wins over verbose for status
/// output. Fails before the fix (the block is gated only on `verbose`) and passes
/// after (`verbose && show_status`).
#[test]
fn test_read_sstable_quiet_verbose_suppresses_stats_block() {
    let Some(data_db) = find_simple_table_data_db() else {
        eprintln!("SKIP: no simple_table Data.db under datasets root");
        return;
    };

    let output = run_cli_command(&[
        "--quiet",
        "read-sstable",
        data_db.to_str().unwrap(),
        "--verbose",
        "--format",
        "json",
        "--limit",
        "1",
    ]);

    assert!(
        output.status.success(),
        "read-sstable --quiet --verbose should succeed. stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("SSTable Statistics")
            && !stderr.contains("Total entries:")
            && !stderr.contains("Compression ratio:"),
        "read-sstable --quiet --verbose must suppress the verbose statistics block \
         on stderr (quiet wins over verbose). Got:\n{stderr}"
    );
}

#[test]
fn test_import_quiet_suppresses_progress_preamble() {
    // Requires a valid data-dir + schema so the Database opens; the import
    // itself may warn on individual inserts (read-only db) — we only assert the
    // progress/status preamble is gone under --quiet.
    let data_dir = datasets_root().join("sstables");
    let schema_file = schemas_dir().join("basic-types.cql");
    if !data_dir.exists() || !schema_file.exists() {
        eprintln!("SKIP: dataset/schema not present");
        return;
    }

    let tmp = std::env::temp_dir().join("cqlite_issue_1506_import.csv");
    fs::write(&tmp, "id,name\n1,alice\n").expect("write temp csv");

    let output = run_cli_command(&[
        "--quiet",
        "--schema",
        schema_file.to_str().unwrap(),
        "--data-dir",
        data_dir.to_str().unwrap(),
        "import",
        tmp.to_str().unwrap(),
        "--format",
        "csv",
        "--table",
        "test_basic.simple_table",
    ]);

    let _ = fs::remove_file(&tmp);

    // Assert the command actually reached (and completed) the import path before
    // checking for absence of the preamble. `import_data` returns Ok even when
    // individual inserts warn (read-only db), so a success exit means we ran the
    // import — without this, an early command failure (which also emits no
    // preamble) would pass the suppression checks vacuously (#1506 roborev Low).
    assert!(
        output.status.success(),
        "import --quiet should reach the import path and exit successfully. stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("Importing data from:")
            && !stdout.contains("Import Summary")
            && !stdout.contains("CSV columns")
            && !stdout.contains("Import completed"),
        "import --quiet must emit no progress/status preamble on stdout. Got:\n{stdout}"
    );
}

// ============================================================================
// (3) FINDING 1: export_sstable(quiet=true) suppresses schema-loading status
// ============================================================================
//
// `export_sstable` is an internal library API with no CLI subcommand, so it
// cannot be driven as a subprocess; and its schema-loading status is emitted via
// `println!`, which Rust's test harness captures on a per-test thread-local
// buffer (never on the process fd), so an in-process stdout-fd capture cannot
// observe it. This regression is therefore locked down with a source-guard in
// the same idiom as the `.template(` guard above: the handler must route schema
// loading through the quiet-aware `load_schema_file_with_status(...)` gated on
// its `show_progress` flag, and must NOT call the unconditional
// `load_schema_file(...)` (which prints "Loading schema…" regardless of quiet).
// Fails on `main` (unconditional call) and passes after the fix.

#[test]
fn test_export_sstable_schema_load_is_quiet_aware() {
    let path = crate_root().join("src/commands/export_sstable.rs");
    let src = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    let collapsed = src.split_whitespace().collect::<Vec<_>>().join(" ");

    // Must use the quiet-aware loader gated on the handler's show_progress flag.
    assert!(
        collapsed.contains("load_schema_file_with_status(schema_path, false, None, show_progress)"),
        "export_sstable must load its schema via \
         load_schema_file_with_status(schema_path, false, None, show_progress) so schema \
         status is suppressed under --quiet (#1506/#284)."
    );

    // Must NOT call the unconditional loader (which prints schema status regardless).
    assert!(
        !collapsed.contains("load_schema_file(schema_path"),
        "export_sstable must not call the unconditional load_schema_file(...) \
         (it prints schema-loading status even under --quiet); use \
         load_schema_file_with_status(...) gated on show_progress instead."
    );
}

/// The quiet-aware loader must actually gate every status `println!` behind its
/// `show_status` flag (guards against a future edit that reintroduces an
/// unconditional print inside the helper). Fails on `main` (no such helper /
/// unconditional prints) and passes after the fix.
#[test]
fn test_schema_loader_gates_status_behind_show_status() {
    let path = crate_root().join("src/commands/schema_load.rs");
    let src = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    let collapsed = src.split_whitespace().collect::<Vec<_>>().join(" ");

    assert!(
        src.contains("fn load_schema_file_with_status(") && src.contains("show_status: bool"),
        "schema_load must expose a load_schema_file_with_status(..., show_status: bool) helper (#1506)."
    );

    // Every schema-status println! must sit directly under an `if show_status {`
    // guard so it is suppressed when the caller passes show_status=false.
    for status in [
        "if show_status { println!(\"📋 Loading schema",
        "if show_status { println!(\"📝 Parsing JSON schema",
        "if show_status { println!(\"📝 Parsing CQL schema",
    ] {
        assert!(
            collapsed.contains(status),
            "schema-loading status must be gated behind `if show_status` in \
             schema_load.rs (#1506); missing guarded form for: {status}"
        );
    }
}
