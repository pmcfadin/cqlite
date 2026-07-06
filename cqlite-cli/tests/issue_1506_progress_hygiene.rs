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

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};

/// Run the pre-built CLI binary, capturing stdout/stderr.
fn run_cli_command(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(args)
        .output()
        .expect("Failed to execute CLI command")
}

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn datasets_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| crate_root().parent().unwrap().join("test-data/datasets"))
}

fn schemas_dir() -> PathBuf {
    crate_root().parent().unwrap().join("test-data/schemas")
}

/// Locate a concrete `*-Data.db` file under `test_basic/simple_table-*`, or
/// return `None` when the binary dataset is not present (test then skips).
fn find_simple_table_data_db() -> Option<PathBuf> {
    let dir = datasets_root().join("sstables/test_basic");
    let entries = fs::read_dir(&dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("simple_table-") {
            continue;
        }
        let sub = fs::read_dir(entry.path()).ok()?;
        for f in sub.flatten() {
            let fname = f.file_name();
            let fname = fname.to_string_lossy();
            if fname.ends_with("Data.db") {
                return Some(f.path());
            }
        }
    }
    None
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

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("Importing data from:")
            && !stdout.contains("Import Summary")
            && !stdout.contains("CSV columns")
            && !stdout.contains("Import completed"),
        "import --quiet must emit no progress/status preamble on stdout. Got:\n{stdout}"
    );
}
