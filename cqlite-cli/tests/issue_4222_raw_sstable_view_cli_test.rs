//! Issue #4222 — raw SSTable view, `cqlite query` CLI surface.
//!
//! End-to-end wiring evidence (this repo's doctrine: a feature is done only
//! when its PUBLIC surface exercises it, never a helper-only unit test):
//! invokes the BUILT `cqlite` binary — the same one an operator runs — against
//! a real Cassandra 5.0 fixture, through the standard `--query`/`--out` flags,
//! with no new CLI subcommand or flag (spec's "works through the existing CLI
//! surface unmodified" requirement).
//!
//! Fixture: `test_tomb.resurrection_gc_positive`
//! (`test-data/schemas/tombstone-parity.cql`) — the same 2-generation,
//! 3-tombstone-kind fixture the core-level point-read test validates
//! byte-exact against the sstabledump golden; this file only proves the CLI
//! renders the raw view's columns, not a duplicate of that parity check.
//!
//! ## Fixture-root discipline (issue #3220/#3121, roborev finding #4222)
//!
//! The fixture is resolved by walking EVERY candidate root — a
//! `CQLITE_DATASETS_ROOT` corpus (if set) first, then the checkout's own
//! `test-data/datasets` — and picking whichever ACTUALLY carries the table's
//! `*-Data.db` bytes, never committing to a single env-first root.
//! `resurrection_gc_positive`'s `Data.db` is NOT git-committed — only its
//! JSONL/`.txt`/`.crc32` sidecars are — so this lane SKIPs cleanly whenever
//! no candidate root carries the table's real bytes. It does NOT replicate
//! issue #3121's two-level SKIP/PANIC rule: `scripts/agent-gate.sh`
//! UNCONDITIONALLY exports `CQLITE_DATASETS_ROOT` for every test run it
//! drives, so "is the env var set" can never signal "a real fetch
//! happened" here (unlike #3121's OWN fixture, `static_with_tombstones`,
//! whose `Data.db` genuinely IS git-tracked, so its directory-presence
//! check holds regardless of fetch state). `CQLITE_REQUIRE_FIXTURES=1`
//! turns even the clean-skip case into a hard failure. Mirrors
//! `issue_4222_raw_view_point_read_test.rs`'s core-side helper (not
//! directly shareable across crates).

use std::path::{Path, PathBuf};
use std::process::Command;

fn checkout_datasets_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-data/datasets")
}

fn schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-data/schemas")
}

/// Every `sstables/` root to search, in preference order — `CQLITE_DATASETS_ROOT`
/// first (when set), then the checkout's own corpus. Mirrors
/// `cqlite-core/tests/support/datasets_root.rs::sstables_root_candidates` (a
/// different crate, so not directly shareable, but the SAME walk-every-root
/// rule — #3220's fix, generalized rather than re-broken here).
fn sstables_root_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(env_root) = std::env::var("CQLITE_DATASETS_ROOT") {
        if !env_root.is_empty() {
            candidates.push(PathBuf::from(env_root).join("sstables"));
        }
    }
    let checkout = checkout_datasets_root().join("sstables");
    if !candidates.contains(&checkout) {
        candidates.push(checkout);
    }
    candidates
}

/// `true` when `<root>/<keyspace>/<table>-*` holds at least one directory
/// carrying a real `*-Data.db` — not just the committed JSONL/`.txt` sidecars.
fn table_has_data(root: &Path, keyspace: &str, table: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(root.join(keyspace)) else {
        return false;
    };
    let prefix = format!("{table}-");
    entries.filter_map(|e| e.ok()).any(|e| {
        let path = e.path();
        path.is_dir()
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with(&prefix))
                .unwrap_or(false)
            && std::fs::read_dir(&path)
                .map(|rd| {
                    rd.filter_map(|e| e.ok()).any(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.ends_with("-Data.db"))
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false)
    })
}

/// The first candidate `sstables/` root that actually carries the table's
/// bytes — table-granular by contract (#3220): a root holding the KEYSPACE
/// but not this table must not be selected.
fn sstables_root_for_table(keyspace: &str, table: &str) -> Option<PathBuf> {
    sstables_root_candidates()
        .into_iter()
        .find(|root| table_has_data(root, keyspace, table))
}

fn describe_search(keyspace: &str, table: &str) -> String {
    format!(
        "searched {:?} for {keyspace}/{table}-*/*-Data.db",
        sstables_root_candidates()
    )
}

/// `true` when `CQLITE_REQUIRE_FIXTURES` is truthy (issue #972 strict mode):
/// every would-be SKIP becomes a PANIC so a CI lane cannot false-pass on
/// missing data.
fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// The resolved `sstables/` root for the fixture, or `None` — the ONLY
/// sanctioned skip — whenever no candidate root carries the table's real
/// bytes.
fn committed_data_dir() -> Option<PathBuf> {
    if let Some(root) = sstables_root_for_table("test_tomb", "resurrection_gc_positive") {
        return Some(root);
    }
    if require_fixtures_strict() {
        panic!(
            "CQLITE_REQUIRE_FIXTURES=1 but 'test_tomb.resurrection_gc_positive' was not found \
             under any candidate root — fetch the corpus first \
             (bash test-data/scripts/fetch-datasets.sh): {}",
            describe_search("test_tomb", "resurrection_gc_positive")
        );
    }
    eprintln!(
        "SKIP: 'test_tomb.resurrection_gc_positive' (fetch-only fixture) was not found under \
         any candidate root — {}",
        describe_search("test_tomb", "resurrection_gc_positive")
    );
    None
}

struct CliRun {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn run_query_at(data_dir: &Path, query: &str, format: &str) -> CliRun {
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
    let Some(data_dir) = committed_data_dir() else {
        return;
    };
    let query = "SELECT pk, ck, row_kind, row_tombstone, val_tombstone, generation, sstable, \
                 format FROM test_tomb.resurrection_gc_positive_raw_sstable_data WHERE pk = 1";

    for format in ["json", "table", "csv"] {
        let run = run_query_at(&data_dir, query, format);
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
///
/// Roborev finding (issue #4222): queried against the RESOLVED, KNOWN-GOOD
/// committed data dir (not a possibly-nonexistent path), so the `exit == 3`
/// assertion can only mean "schema/table not found", never "data dir not
/// found" masquerading as the same exit code.
#[test]
fn nonexistent_base_table_exits_with_schema_error_not_empty_success() {
    let Some(data_dir) = committed_data_dir() else {
        return;
    };
    let run = run_query_at(
        &data_dir,
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
