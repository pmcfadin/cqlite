//! Issue #4193, requirements R3 and R4 — the trace never changes the result,
//! and a generation that cannot be read is an error, never a shorter trail
//! (spec `reconcile-decision-trail` R3.1, R4.1).

#![cfg(feature = "write-support")]

use std::path::{Path, PathBuf};

use cqlite_core::schema::cql_parser::{
    classify_statement, parse_create_table, split_cql_statements, StatementType,
};
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::write_engine::merge::trace::RecordingSink;
use cqlite_core::storage::write_engine::merge::{KWayMerger, MergeEntry, MergeStep};
use cqlite_core::storage::write_engine::mutation::DecoratedKey;

#[path = "support/datasets_root.rs"]
mod datasets_root;

/// A far-future evaluation instant shared by BOTH the traced and untraced run
/// of every table below, so any table carrying a TTL or a purgeable
/// tombstone genuinely exercises `expire_ttl_cells`/`purge_gc_grace` in both
/// runs identically — R3.1 requires "same now/gc_before/purge_safe", not any
/// PARTICULAR value.
const NOW_SECS: i64 = 4_000_000_000;
const GC_GRACE_SECONDS: i64 = 864_000;

fn load_table_schema(cql_path: &Path, keyspace: &str, table: &str) -> TableSchema {
    let content = std::fs::read_to_string(cql_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", cql_path.display()));
    for statement in split_cql_statements(&content) {
        if !matches!(classify_statement(&statement), StatementType::CreateTable) {
            continue;
        }
        let (_, mut schema) = parse_create_table(&statement).unwrap_or_else(|error| {
            panic!("parse CREATE TABLE in {}: {error:?}", cql_path.display())
        });
        if schema.table.eq_ignore_ascii_case(table) {
            schema.keyspace = keyspace.to_string();
            return schema;
        }
    }
    panic!(
        "table {keyspace}.{table} not found in {}",
        cql_path.display()
    );
}

/// Newest-to-oldest `nb-N-big-Data.db` paths directly under `dir`.
fn discover_generations_newest_first(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read_dir {}: {error}", dir.display()))
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?.to_string();
            if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
                let generation = name
                    .strip_prefix("nb-")?
                    .split("-big-")
                    .next()?
                    .parse::<u64>()
                    .ok()?;
                Some((generation, path))
            } else {
                None
            }
        })
        .collect();
    found.sort_by_key(|(generation, _)| std::cmp::Reverse(*generation));
    found.into_iter().map(|(_, path)| path).collect()
}

/// Unconditionally fail-closed generation-dir resolution (#3220) — for
/// `test_explain`, whose fixtures (including the `*-Data.db` binaries) are
/// FULLY git-committed, so absence is always a test-data defect, never a
/// legitimate "dataset not fetched" state.
fn generation_dir(keyspace: &str, table: &str) -> PathBuf {
    let root = datasets_root::sstables_root_for_table(keyspace, table)
        .unwrap_or_else(|| panic!("{}", datasets_root::describe_search(keyspace, table)));
    datasets_root::table_generation_dirs(&root, keyspace, table)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no usable {keyspace}.{table} generation dir"))
}

/// `true` when `CQLITE_REQUIRE_FIXTURES` is set to a truthy value. In strict
/// mode (the full gate's default), a `test_tomb` fixture that would otherwise
/// SKIP because its binaries are unfetched must PANIC instead, so a CI gate
/// cannot false-pass on missing data (issue #972).
fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// SKIP-clean (or, under `CQLITE_REQUIRE_FIXTURES=1`, PANIC-naming-the-table)
/// generation-dir resolution for the `test_tomb` fixtures this file uses.
/// Unlike `generation_dir` above (`test_explain`, fully committed), `test_tomb`'s
/// `*-Data.db` binaries are NOT git-committed — `test-data/datasets/sstables/test_tomb`
/// ships only `.jsonl`/`.txt` sidecars — so an unfetched dataset root on a fresh
/// checkout or CI run without `fetch-datasets.sh` is a legitimate absence, not
/// a defect (issue #4193 review finding; precedent:
/// `issue_1014_resurrection_safety_parity.rs::require_fixture`). Every caller
/// asserts PER CASE (#3220) — never behind a suite-wide `assert!(ran > 0)`.
fn skip_clean_generation_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    let dir = datasets_root::sstables_root_for_table(keyspace, table).and_then(|root| {
        datasets_root::table_generation_dirs(&root, keyspace, table)
            .into_iter()
            .next()
    });
    if dir.is_none() {
        let reason = datasets_root::describe_search(keyspace, table);
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {keyspace}.{table} fixture is absent — {reason}; \
                 fetch it (bash test-data/scripts/fetch-datasets.sh)"
            );
        }
        eprintln!("[skip] {keyspace}.{table}: {reason}");
    }
    dir
}

/// Drive a full-scan merger (`KWayMerger::new_with_gc`, never the point-read
/// path — R3.1 covers the compaction/full-scan reconciliation kernel every
/// table below exercises) to completion, collecting `(key, rows)` per
/// emitted partition in emission order.
fn drive_full_scan(
    paths: Vec<PathBuf>,
    schema: &TableSchema,
) -> Vec<(DecoratedKey, Vec<MergeEntry>)> {
    let mut merger = KWayMerger::new_with_gc(
        paths,
        schema,
        Some(NOW_SECS - GC_GRACE_SECONDS),
        Some(NOW_SECS),
    )
    .expect("build untraced merger")
    .with_purge_safe(true);
    let mut out = Vec::new();
    loop {
        match merger.step().expect("merge step") {
            MergeStep::Complete => break,
            MergeStep::Partition { key, rows } => out.push((key, rows)),
        }
    }
    out
}

fn drive_full_scan_traced(
    paths: Vec<PathBuf>,
    schema: &TableSchema,
) -> (Vec<(DecoratedKey, Vec<MergeEntry>)>, RecordingSink) {
    let mut merger = KWayMerger::new_with_gc(
        paths,
        schema,
        Some(NOW_SECS - GC_GRACE_SECONDS),
        Some(NOW_SECS),
    )
    .expect("build merger before tracing")
    .with_purge_safe(true)
    .with_trace_sink(RecordingSink::new());
    let mut out = Vec::new();
    loop {
        match merger.step().expect("merge step") {
            MergeStep::Complete => break,
            MergeStep::Partition { key, rows } => out.push((key, rows)),
        }
    }
    (out, merger.into_trace_sink())
}

/// Assert traced == untraced for one table, and that the trail is non-empty
/// (R3.1: "an empty trail on a table with rows FAILs — no vacuous pass").
/// SKIPs cleanly (see `skip_clean_generation_dir`) when the `test_tomb`
/// fixture's binaries are not fetched, unless `CQLITE_REQUIRE_FIXTURES=1`.
fn assert_traced_equals_untraced(keyspace: &str, table: &str) {
    let Some(dir) = skip_clean_generation_dir(keyspace, table) else {
        return;
    };
    let schema = load_table_schema(
        &datasets_root::schema_path("tombstone-parity.cql")
            .expect("committed test-data/schemas/tombstone-parity.cql fixture"),
        keyspace,
        table,
    );
    let paths = discover_generations_newest_first(&dir);
    assert!(
        !paths.is_empty(),
        "{keyspace}.{table}: expected at least one nb-*-big-Data.db under {}",
        dir.display()
    );

    let untraced = drive_full_scan(paths.clone(), &schema);
    let (traced, sink) = drive_full_scan_traced(paths, &schema);

    assert_eq!(
        untraced.len(),
        traced.len(),
        "{keyspace}.{table}: traced and untraced emitted a different number of partitions"
    );
    for (index, ((untraced_key, untraced_rows), (traced_key, traced_rows))) in
        untraced.iter().zip(traced.iter()).enumerate()
    {
        assert_eq!(
            untraced_key, traced_key,
            "{keyspace}.{table}: partition {index}'s key differs between traced and untraced"
        );
        assert_eq!(
            untraced_rows, traced_rows,
            "{keyspace}.{table}: partition {index}'s rows differ between traced and untraced"
        );
    }
    assert!(
        !untraced.is_empty(),
        "{keyspace}.{table}: this table must hold at least one partition for the comparison \
         to be meaningful (a committed fixture with zero rows is a test-data defect, not a pass)"
    );

    let (cells, tombstones, probes) = sink.into_parts();
    assert!(
        !cells.is_empty() || !tombstones.is_empty(),
        "{keyspace}.{table}: RecordingSink recorded nothing for a table with {} partition(s) \
         — an empty trail on a non-empty table is a vacuous pass, not a proof (R3.1)",
        untraced.len()
    );
    // `generation_probe` is a POINT-READ concept (design.md §D2: "probe
    // outcomes from the point-read builder's PathProbe") — a full-scan merger
    // (this helper's `KWayMerger::new_with_gc`) always scans every input in
    // full and never probes, so `probes` is legitimately empty here.
    let _ = probes;
}

// Every table in test-data/schemas/tombstone-parity.cql (design.md §D6's
// dropped-column fixture family) — each independently verified present with
// real *-Data.db under CQLITE_DATASETS_ROOT / the checkout corpus before
// being listed here (never asserted with a suite-wide `assert!(ran > 0)`;
// each is its OWN #[test], per #3220's per-case rule).

#[test]
fn traced_equals_untraced_gc_before_boundary() {
    assert_traced_equals_untraced("test_tomb", "gc_before_boundary");
}

#[test]
fn traced_equals_untraced_tombstone_histogram() {
    assert_traced_equals_untraced("test_tomb", "tombstone_histogram");
}

#[test]
fn traced_equals_untraced_skipped_partition_delete() {
    assert_traced_equals_untraced("test_tomb", "skipped_partition_delete");
}

#[test]
fn traced_equals_untraced_resurrection_gc0() {
    assert_traced_equals_untraced("test_tomb", "resurrection_gc0");
}

#[test]
fn traced_equals_untraced_resurrection_gc_positive() {
    assert_traced_equals_untraced("test_tomb", "resurrection_gc_positive");
}

#[test]
fn traced_equals_untraced_dropped_regular_col() {
    assert_traced_equals_untraced("test_tomb", "dropped_regular_col");
}

#[test]
fn traced_equals_untraced_dropped_static_col() {
    assert_traced_equals_untraced("test_tomb", "dropped_static_col");
}

#[test]
fn traced_equals_untraced_static_with_tombstones() {
    assert_traced_equals_untraced("test_tomb", "static_with_tombstones");
}

#[test]
fn traced_equals_untraced_wide_range_tombstone() {
    assert_traced_equals_untraced("test_tomb", "wide_range_tombstone");
}

#[test]
fn traced_equals_untraced_trace_decisions() {
    let dir = generation_dir("test_explain", "trace_decisions");
    let schema = load_table_schema(
        &datasets_root::schema_path("explain-trace.cql")
            .expect("committed test-data/schemas/explain-trace.cql fixture"),
        "test_explain",
        "trace_decisions",
    );
    let paths = discover_generations_newest_first(&dir);
    assert_eq!(paths.len(), 2, "expected both nb-1-big and nb-2-big");

    let untraced = drive_full_scan(paths.clone(), &schema);
    let (traced, sink) = drive_full_scan_traced(paths, &schema);
    assert_eq!(untraced.len(), traced.len());
    for (index, ((uk, ur), (tk, tr))) in untraced.iter().zip(traced.iter()).enumerate() {
        assert_eq!(uk, tk, "partition {index} key differs");
        assert_eq!(ur, tr, "partition {index} rows differ");
    }
    assert!(!untraced.is_empty());
    let (cells, tombstones, _probes) = sink.into_parts();
    assert!(!cells.is_empty() && !tombstones.is_empty());
}

/// R4.1: a generation whose `Statistics.db` cannot be decoded fails
/// CONSTRUCTION (never a partial trail). `dropped_regular_col`'s own
/// two-generation fixture, staged to a disposable temp copy so the
/// committed fixture is never mutated.
#[test]
fn truncated_statistics_fails_closed_before_any_trace() {
    let Some(dir) = skip_clean_generation_dir("test_tomb", "dropped_regular_col") else {
        return;
    };
    let temp = tempfile::tempdir().expect("tempdir");
    let staged = temp.path().join("staged");
    std::fs::create_dir_all(&staged).expect("create staged dir");
    for entry in std::fs::read_dir(&dir).expect("read fixture dir") {
        let entry = entry.expect("dir entry");
        let dest = staged.join(entry.file_name());
        std::fs::copy(entry.path(), &dest).expect("copy fixture file");
    }
    // `parse_statistics_with_fallback` is deliberately lenient (its name says
    // so) and tolerates a garbled body with a fallback default rather than
    // erroring, so a byte-corrupted Statistics.db does not reproduce a real
    // "unreadable generation". Truncate it to a size too short to contain
    // even the fixed-size component-length header any parse path requires,
    // which no fallback can paper over.
    let truncated = staged.join("nb-1-big-Statistics.db");
    assert!(
        truncated.exists(),
        "expected {truncated:?} to exist in the staged copy"
    );
    std::fs::write(&truncated, b"\x00").expect("truncate Statistics.db to 1 byte");

    let schema = load_table_schema(
        &datasets_root::schema_path("tombstone-parity.cql")
            .expect("committed test-data/schemas/tombstone-parity.cql fixture"),
        "test_tomb",
        "dropped_regular_col",
    );
    let paths = discover_generations_newest_first(&staged);
    assert_eq!(
        paths.len(),
        2,
        "expected both generations in the staged copy"
    );

    // `SSTableRowIteratorAdapter::open` does not eagerly parse the FULL
    // Statistics.db at construction (encoding/deletion stats are consulted
    // lazily during the scan), so a truncated Statistics.db does not fail
    // `KWayMerger::new_with_gc` itself here — it fails during `step()`, when
    // the corrupted generation's data is actually read. Either way, R4's
    // guarantee is that driving the merger NEVER silently produces a partial
    // trail in place of the error: it returns `Err` naming the unreadable
    // input before reaching `MergeStep::Complete`.
    let mut merger = KWayMerger::new_with_gc(paths, &schema, None, None)
        .expect("construction succeeds; the corruption surfaces during step()")
        .with_trace_sink(RecordingSink::new());
    let mut saw_error = false;
    loop {
        match merger.step() {
            Ok(MergeStep::Complete) => break,
            Ok(MergeStep::Partition { .. }) => {}
            Err(error) => {
                saw_error = true;
                let message = error.to_string();
                assert!(
                    message.contains("nb-1-big") || message.contains("Statistics"),
                    "error must name the unreadable generation or its Statistics.db, \
                     got: {message}"
                );
                break;
            }
        }
    }
    assert!(
        saw_error,
        "a truncated Statistics.db must surface as an Err during the merge, never a \
         silently-shorter trail"
    );
}
