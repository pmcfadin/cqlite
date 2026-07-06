//! Issue #1642 (Epic K / K3): positional row emit — the row decoder builds each
//! row's cells directly in serialization-header (schema) column order, so the
//! shared display-row builder no longer allocates a per-row `HashMap` and no
//! longer runs a per-row alphabetical `sort_by` to hide `HashMap` iteration
//! nondeterminism. Determinism now comes from CONSTRUCTION.
//!
//! Wiring-evidence against real fixtures across the four column shapes
//! (simple / static / collections / wide). The headline assertion is the H5
//! `ROW_SORT_INVOCATIONS == 0` counter (Issue #1618): on `main`/pre-K3 the
//! shared `build_display_row` bumped it once per returned live row, so these
//! assertions FAIL before K3 lands and PASS after.
//!
//! Observable output is BYTE-IDENTICAL across K3: the public query result is a
//! name-keyed `HashMap` (`QueryRow.values`), so the INTERNAL emit order is not
//! user-visible — the 33-table parity/goldens harness is the end-to-end truth.
//! This file additionally proves determinism-by-construction directly: two scans
//! of the same fixture yield the identical column set with ZERO sort calls.
//!
//! Compiled only with `--features work-counters` (the getters/`reset` and the
//! counter bodies live behind that feature; see `read_work_counters`). Requires
//! `CQLITE_DATASETS_ROOT` + fetched binaries; skips (never fails) when a fixture
//! is absent. Excluded under `tombstones` (that build serves reads via a
//! full-scan filter path). Serialized on the shared counter mutex.

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "work-counters",
    not(feature = "tombstones")
))]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::storage::sstable::read_work_counters as rwc;
use cqlite_core::Database;
use serial_test::serial;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        if let Some(dir) = root.parent().and_then(|p| {
            let d = p.join("schemas");
            d.exists().then_some(d)
        }) {
            return Some(dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

/// True if `<datasets>/sstables/<keyspace>/<table>-*/` holds a `*-Data.db` file.
/// Skip keys off fixture presence (not a 0-row result), so a present fixture that
/// yields 0 rows stays a hard failure.
fn fixture_data_present(keyspace: &str, table: &str) -> bool {
    let Some(root) = datasets_root() else {
        return false;
    };
    let Ok(entries) = std::fs::read_dir(root.join("sstables").join(keyspace)) else {
        return false;
    };
    let prefix = format!("{table}-");
    for e in entries.flatten() {
        if !e.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        if let Ok(files) = std::fs::read_dir(e.path()) {
            for f in files.flatten() {
                if f.file_name().to_string_lossy().ends_with("-Data.db") {
                    return true;
                }
            }
        }
    }
    false
}

/// Ingest a single fixture table and return a fresh `Database`. Returns `None`
/// when the fixture / schema is absent.
async fn setup(keyspace: &str, schema_file: &str) -> Option<Database> {
    let root = datasets_root()?;
    let schema_path = schemas_dir()?.join(schema_file);
    if !schema_path.exists() {
        return None;
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return None;
    }
    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(config).await.ok()?;
    if result.schema_load_result.schemas_loaded == 0 {
        return None;
    }
    Some(result.database)
}

/// Scan one table and return `(returned_row_count, row_sort_invocations)` measured
/// around the scan. Resets the shared counter first so a parallel test's value can
/// never satisfy the assertion (serialized by `#[serial]`).
async fn scan_and_count_sorts(db: &Database, sql: &str) -> (u64, u64) {
    rwc::reset();
    assert_eq!(
        rwc::row_sort_invocations(),
        0,
        "reset must zero ROW_SORT_INVOCATIONS"
    );
    let scan = db.execute(sql).await.expect("scan must succeed");
    let rows = scan.rows.len() as u64;
    let sorts = rwc::row_sort_invocations();
    (rows, sorts)
}

/// K3 headline (simple table): a full scan returns live rows but performs ZERO
/// per-row cell sorts — the decoder emits cells pre-ordered by construction.
/// FAILS on `main`/pre-K3 (the shared builder sorts once per returned row).
#[tokio::test]
#[serial]
async fn full_scan_does_no_per_row_sort_simple() {
    if !fixture_data_present("test_basic", "simple_table") {
        eprintln!("Skipping (K3 wiring): test_basic/simple_table Data.db not present");
        return;
    }
    let Some(db) = setup("test_basic", "basic-types.cql").await else {
        eprintln!("Skipping (K3 wiring): could not ingest test_basic");
        return;
    };

    let (rows, sorts) = scan_and_count_sorts(&db, "SELECT * FROM test_basic.simple_table").await;
    eprintln!("K3: rows={rows} ROW_SORT_INVOCATIONS={sorts}");
    assert!(rows > 0, "present fixture must return rows");
    assert_eq!(
        sorts, 0,
        "K3: positional emit must perform ZERO per-row cell sorts on a full scan; \
         got {sorts} for {rows} rows"
    );
}

/// K3 across column shapes: static, collection, and wide-partition tables all
/// emit rows with zero per-row sorts. Each shape is skipped independently when its
/// fixture is absent (never fails on absence). A shape that IS present and returns
/// rows must record zero sorts.
#[tokio::test]
#[serial]
async fn positional_emit_across_column_shapes() {
    // (keyspace, schema, table, sql) — one per column shape.
    let cases = [
        (
            "test_basic",
            "basic-types.cql",
            "static_columns_table",
            "SELECT * FROM test_basic.static_columns_table",
        ),
        (
            "test_collections",
            "collections.cql",
            "collection_table",
            "SELECT * FROM test_collections.collection_table",
        ),
        (
            "test_wide_rows",
            "wide-rows.cql",
            "wide_partition_table",
            "SELECT * FROM test_wide_rows.wide_partition_table",
        ),
    ];

    let mut exercised = 0usize;
    for (ks, schema, table, sql) in cases {
        if !fixture_data_present(ks, table) {
            eprintln!("Skipping shape {ks}.{table} (Data.db not present)");
            continue;
        }
        let Some(db) = setup(ks, schema).await else {
            eprintln!("Skipping shape {ks}.{table} (could not ingest)");
            continue;
        };
        let (rows, sorts) = scan_and_count_sorts(&db, sql).await;
        eprintln!("K3 shape {ks}.{table}: rows={rows} ROW_SORT_INVOCATIONS={sorts}");
        assert!(rows > 0, "present fixture {ks}.{table} must return rows");
        assert_eq!(
            sorts, 0,
            "K3: shape {ks}.{table} must perform ZERO per-row sorts; got {sorts} for {rows} rows"
        );
        exercised += 1;
    }

    if exercised == 0 {
        eprintln!("Skipping (K3 wiring): no multi-shape fixtures present");
    }
}

/// Determinism-by-construction: two independent scans of the same fixture yield
/// the identical per-row column SET, and BOTH perform zero per-row sorts (the
/// per-row alphabetical sort that this change removed). This asserts stability of
/// the surfaced column set plus the zero-sort guarantee; it does NOT observe emit
/// ORDER, because the public result is a name-keyed map (issue #1334) whose
/// key iteration order is not the positional emit order — cross-scan positional
/// order is unobservable at this layer.
#[tokio::test]
#[serial]
async fn two_scans_identical_columns_without_sort() {
    if !fixture_data_present("test_basic", "simple_table") {
        eprintln!("Skipping (K3 wiring): test_basic/simple_table Data.db not present");
        return;
    }
    let Some(db) = setup("test_basic", "basic-types.cql").await else {
        eprintln!("Skipping (K3 wiring): could not ingest test_basic");
        return;
    };

    let sql = "SELECT * FROM test_basic.simple_table";

    rwc::reset();
    let scan_a = db.execute(sql).await.expect("first scan");
    let sorts_a = rwc::row_sort_invocations();

    rwc::reset();
    let scan_b = db.execute(sql).await.expect("second scan");
    let sorts_b = rwc::row_sort_invocations();

    assert!(!scan_a.rows.is_empty(), "present fixture must return rows");
    assert_eq!(
        scan_a.rows.len(),
        scan_b.rows.len(),
        "two scans must return the same row count"
    );
    assert_eq!(sorts_a, 0, "first scan must perform zero per-row sorts");
    assert_eq!(sorts_b, 0, "second scan must perform zero per-row sorts");

    // The column SET per row is identical across scans (order in the public
    // name-keyed result is not observable, so compare the canonical set).
    let columns = |scan: &cqlite_core::QueryResult| -> Vec<BTreeSet<String>> {
        scan.rows
            .iter()
            .map(|r| r.values.keys().map(|k| k.to_string()).collect())
            .collect()
    };
    assert_eq!(
        columns(&scan_a),
        columns(&scan_b),
        "two scans must surface the identical per-row column set"
    );
}
