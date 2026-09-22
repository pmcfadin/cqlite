//! Issue #4222 — raw SSTable view, `cqlite query` CLI surface.
//!
//! End-to-end wiring evidence (this repo's doctrine: a feature is done only
//! when its PUBLIC surface exercises it, never a helper-only unit test):
//! invokes the BUILT `cqlite` binary — the same one an operator runs — against
//! a real Cassandra 5.0 fixture, through the standard `--query`/`--out` flags,
//! with no new CLI subcommand or flag (spec's "works through the existing CLI
//! surface unmodified" requirement).
//!
//! Fixture: `test_tomb.resurrection_gc_positive` (git-committed binaries,
//! `test-data/schemas/tombstone-parity.cql`) — the same 2-generation,
//! 3-tombstone-kind fixture the core-level point-read test validates
//! byte-exact against the sstabledump golden; this file only proves the CLI
//! renders the raw view's columns, not a duplicate of that parity check.

use std::path::PathBuf;
use std::process::Command;

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

fn schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-data/schemas")
}

/// `true` when the resolved corpus actually carries the committed fixture's
/// `Data.db` binaries (gitignored; fetched via
/// `test-data/scripts/fetch-datasets.sh`). Since the binaries are
/// git-COMMITTED for this table, an absent corpus here is a harness/env
/// problem (unset `CQLITE_DATASETS_ROOT`), not a legitimate skip on CI — but
/// this file still SKIPs cleanly on a bare `cargo test` with no dataset root
/// configured, matching this repo's other CLI dataset lanes.
fn fixture_present() -> bool {
    datasets_root()
        .join("sstables/test_tomb")
        .read_dir()
        .map(|mut entries| {
            entries.any(|e| {
                e.ok()
                    .map(|e| {
                        e.file_name()
                            .to_string_lossy()
                            .starts_with("resurrection_gc_positive-")
                    })
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

struct CliRun {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn run_query(query: &str, format: &str) -> CliRun {
    let data_dir = datasets_root().join("sstables");
    let schema_path = schemas_dir().join("tombstone-parity.cql");
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "--schema",
            schema_path.to_str().unwrap(),
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--query",
            query,
            "--out",
            format,
        ])
        .output()
        .expect("failed to execute the built cqlite binary");
    CliRun {
        exit_code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    }
}

/// Spec: "All three output formats render the raw view's wide, table-specific
/// column set" — `table`/`json`/`csv` each succeed and carry the raw view's
/// distinctive columns (never present on the base table), with no CLI code
/// change required (schema-agnostic writers driven by `metadata.columns`).
#[test]
fn all_three_output_formats_render_raw_view_columns() {
    if !fixture_present() {
        eprintln!(
            "SKIP: test_tomb.resurrection_gc_positive fixture binaries absent — \
             export CQLITE_DATASETS_ROOT per test-data/scripts/fetch-datasets.sh"
        );
        return;
    }
    let query =
        "SELECT pk, ck, row_kind, row_tombstone, val_tombstone, generation, sstable, format \
         FROM test_tomb.resurrection_gc_positive_raw_sstable_data WHERE pk = 1";

    for format in ["json", "table", "csv"] {
        let run = run_query(query, format);
        assert_eq!(
            run.exit_code,
            Some(0),
            "format={format} must succeed; stderr={}",
            run.stderr
        );
        assert!(
            !run.stdout.trim().is_empty(),
            "format={format} must produce non-empty output for a resolvable table"
        );
        // Every format must name the raw-view-only columns somewhere in its
        // rendering (header row for table/csv, keys for json) — proving the
        // writer surfaced the wide column set, not just the base columns.
        for needle in ["row_kind", "generation", "sstable"] {
            assert!(
                run.stdout.contains(needle),
                "format={format} output must mention '{needle}' — got:\n{}",
                run.stdout
            );
        }
    }
}

/// Spec: "A nonexistent base table produces a typed error, not an empty
/// result" — `CliExitCode::SchemaError` (3), never 0-with-zero-rows
/// (design.md D8's deliberate divergence from the base SELECT path).
#[test]
fn nonexistent_base_table_exits_with_schema_error_not_empty_success() {
    let run = run_query(
        "SELECT * FROM test_tomb.nonexistent_table_raw_sstable_data",
        "json",
    );
    assert_eq!(
        run.exit_code,
        Some(3),
        "a raw-view query over a nonexistent base table must exit 3 (SchemaError), \
         not succeed with zero rows — stdout={}, stderr={}",
        run.stdout,
        run.stderr
    );
}
