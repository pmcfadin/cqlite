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

/// The raw view's FULL column contract for this fixture, in the positional
/// order `metadata.columns` declares and every writer renders from. The
/// projection test above names three columns; this list is what a
/// `SELECT *` must put in front of each writer.
const FULL_COLUMN_CONTRACT: &[&str] = &[
    "pk",
    "ck",
    "val",
    "val_timestamp",
    "val_ttl",
    "val_local_deletion_time",
    "val_tombstone",
    "extra",
    "extra_timestamp",
    "extra_ttl",
    "extra_local_deletion_time",
    "extra_tombstone",
    "row_timestamp",
    "row_ttl",
    "row_liveness_expires_at",
    "row_local_deletion_time",
    "row_tombstone",
    "row_deletion_timestamp",
    "partition_deletion_time",
    "partition_deletion_timestamp",
    "row_kind",
    "bound_inclusive",
    "range_deletion_time",
    "range_deletion_timestamp",
    "sstable",
    "generation",
    "format",
    "position",
];

/// Spec R10, the half the 8-column projection test could not reach: the
/// scenario asks that each writer emit **every column `metadata.columns`
/// names** and render NULLs per its own existing contract (empty CSV cell,
/// absent-or-null JSON key, empty table cell), with no writer code change.
///
/// C intent-audit R10: the existing test projects 8 of the 27 contract
/// columns, so 19 never reached a writer in any test, and the
/// NULL-rendering clause was asserted for no writer at all.
///
/// The NULL subject is `row_tombstone` on a **gen-1** row: every gen-1 row
/// of this fixture is live, so the column is genuinely absent there, while
/// gen-2's ck=2 row carries `row` — so the same column is non-empty
/// elsewhere in the SAME output. That rules out the degenerate pass where a
/// writer emits an empty cell because it never learned the column at all.
#[test]
fn select_star_renders_every_contract_column_and_nulls_per_writer_contract() {
    let Some(data_dir) = committed_data_dir() else {
        return;
    };
    let query = "SELECT * FROM test_tomb.resurrection_gc_positive_raw_sstable_data WHERE pk = 1";

    // --- CSV: header names every column; a NULL is an EMPTY field --------
    let csv = run_query_at(&data_dir, query, "csv");
    assert_eq!(
        csv.exit_code,
        Some(0),
        "csv must succeed; stderr={}",
        csv.stderr
    );
    let mut csv_lines = csv.stdout.lines();
    let header: Vec<&str> = csv_lines
        .next()
        .expect("csv must emit a header row")
        .split(',')
        .collect();
    assert_eq!(
        header, FULL_COLUMN_CONTRACT,
        "the csv writer must name EVERY column metadata.columns declares, in order"
    );
    let tombstone_idx = header
        .iter()
        .position(|c| *c == "row_tombstone")
        .expect("row_tombstone is in the contract");
    let sstable_idx = header
        .iter()
        .position(|c| *c == "sstable")
        .expect("sstable is in the contract");

    let data_rows: Vec<Vec<&str>> = csv_lines
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.split(',').collect())
        .collect();
    assert_eq!(
        data_rows.len(),
        7,
        "pk=1 yields 7 physical rows across both generations"
    );
    let gen1: Vec<&Vec<&str>> = data_rows
        .iter()
        .filter(|r| r[sstable_idx].starts_with("nb-1-"))
        .collect();
    assert_eq!(gen1.len(), 5, "gen-1 contributes 5 live rows");
    for row in &gen1 {
        assert_eq!(
            row[tombstone_idx], "",
            "the csv writer's NULL contract is an EMPTY field — a live gen-1 row has no \
             row_tombstone"
        );
    }
    // Non-vacuity: the same column IS populated on a gen-2 row, so the
    // empties above are real NULLs, not a column the writer dropped.
    assert!(
        data_rows
            .iter()
            .any(|r| r[sstable_idx].starts_with("nb-2-") && r[tombstone_idx] == "row"),
        "gen-2's ck=2 row must render row_tombstone='row' in the SAME csv output"
    );

    // --- JSON: every key present; a NULL is an absent-or-null key --------
    let json = run_query_at(&data_dir, query, "json");
    assert_eq!(
        json.exit_code,
        Some(0),
        "json must succeed; stderr={}",
        json.stderr
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&json.stdout).expect("the json writer must emit parseable JSON");
    let objects = parsed.as_array().expect("json output is an array of rows");
    assert_eq!(objects.len(), 7, "pk=1 yields 7 physical rows");
    let mut null_tombstones = 0usize;
    let mut populated_tombstones = 0usize;
    for obj in objects {
        for column in FULL_COLUMN_CONTRACT {
            assert!(
                obj.get(*column).is_some(),
                "the json writer must name every contract column — '{column}' is missing \
                 from {obj}"
            );
        }
        match &obj["row_tombstone"] {
            serde_json::Value::Null => null_tombstones += 1,
            other => {
                // The ONLY row-level tombstone in this fixture is gen-2's
                // ck=2 whole-row delete. gen-2's ck=3 is a CELL tombstone,
                // so its row_tombstone is null like every live gen-1 row —
                // "null" here is not a per-generation property.
                assert_eq!(other.as_str(), Some("row"));
                assert_eq!(obj["ck"].as_i64(), Some(2));
                assert!(obj["sstable"]
                    .as_str()
                    .unwrap_or_default()
                    .starts_with("nb-2-"));
                populated_tombstones += 1;
            }
        }
    }
    assert_eq!(
        (null_tombstones, populated_tombstones),
        (6, 1),
        "the json writer's NULL contract is an absent-or-null key: 6 of pk=1's 7 rows carry \
         no row-level tombstone and must render null, and exactly one (gen-2 ck=2) must \
         render 'row' — a populated value in the SAME output, so the null assertion cannot \
         pass on a column the writer simply dropped"
    );

    // --- table: header names every column; a NULL is an EMPTY cell -------
    let table = run_query_at(&data_dir, query, "table");
    assert_eq!(
        table.exit_code,
        Some(0),
        "table must succeed; stderr={}",
        table.stderr
    );
    let mut table_lines = table.stdout.lines();
    let table_header: Vec<String> = table_lines
        .next()
        .expect("table must emit a header row")
        .split('|')
        .map(|c| c.trim().to_string())
        .collect();
    assert_eq!(
        table_header, FULL_COLUMN_CONTRACT,
        "the table writer must name EVERY column metadata.columns declares, in order"
    );
    let table_rows: Vec<Vec<String>> = table_lines
        // skip the `---+---` rule and the trailing "(N rows)" footer
        .filter(|l| l.contains('|'))
        .map(|l| l.split('|').map(|c| c.trim().to_string()).collect())
        .collect();
    assert_eq!(table_rows.len(), 7, "pk=1 yields 7 physical rows");
    let table_gen1: Vec<&Vec<String>> = table_rows
        .iter()
        .filter(|r| r[sstable_idx].starts_with("nb-1-"))
        .collect();
    assert_eq!(table_gen1.len(), 5, "gen-1 contributes 5 live rows");
    for row in &table_gen1 {
        assert_eq!(
            row[tombstone_idx], "",
            "the table writer's NULL contract is an EMPTY cell — a live gen-1 row has no \
             row_tombstone"
        );
    }
    assert!(
        table_rows
            .iter()
            .any(|r| r[sstable_idx].starts_with("nb-2-") && r[tombstone_idx] == "row"),
        "gen-2's ck=2 row must render row_tombstone='row' in the SAME table output"
    );
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
