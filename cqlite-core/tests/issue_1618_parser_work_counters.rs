//! Issue #1618 (Epic H / H5): wiring-evidence for the parser work counters.
//! Proves each parser counter increments on the REAL public read path (a real
//! scan / point read through the `Database` API) and pins TODAY's wasteful count
//! against a real fixture, so the J1/K2/K3/L1/L3 children can flip the assertions
//! to the fixed values as they land.
//!
//! Compiled only with `--features work-counters` (the getters/`reset` and the
//! counter bodies live behind that feature; see `read_work_counters`). Requires
//! `CQLITE_DATASETS_ROOT` + fetched binaries; skips (never fails) when the fixture
//! is absent. Excluded under `tombstones` (that build serves point reads by a
//! full-scan filter rather than the targeted seek this evidences).
//!
//! The counters are a shared process-global, so every test here serializes on the
//! `serial_test` mutex (the existing counter-test convention) — a stale value from
//! a parallel test can never satisfy an assertion after a `reset`.

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "work-counters",
    not(feature = "tombstones")
))]

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

/// Format a 16-byte UUID as the canonical unquoted 8-4-4-4-12 literal.
fn uuid_to_literal(bytes: &[u8; 16]) -> String {
    let h = |range: std::ops::Range<usize>| -> String {
        bytes[range].iter().map(|b| format!("{b:02x}")).collect()
    };
    format!(
        "{}-{}-{}-{}-{}",
        h(0..4),
        h(4..6),
        h(6..8),
        h(8..10),
        h(10..16)
    )
}

/// Scan for one present `id` UUID and build the projected point-read SQL for it.
/// Projected (>8 tokens) so it routes through the modern SelectExecutor and the
/// partition-targeted BTI fast path.
async fn learn_point_sql(db: &Database, table: &str) -> Option<String> {
    let scan = db.execute(&format!("SELECT id FROM {table}")).await.ok()?;
    let first = scan.rows.first()?;
    let id = match first.values.get("id") {
        Some(cqlite_core::Value::Uuid(b)) => *b,
        _ => return None,
    };
    Some(format!(
        "SELECT id, name FROM {table} WHERE id = {}",
        uuid_to_literal(&id)
    ))
}

/// Scenario (J1 currency): a full scan normalizes the type name per cell, so
/// `TYPE_NORMALIZE_CALLS` scales with the rows scanned. Two normalization sites
/// fire per non-key cell today (the value-parse `to_lowercase` and the
/// per-column `is_complex_column`), so the count is at least one per returned row.
/// J1 (zero `to_lowercase` per cell) flips this assertion to `== 0`.
#[tokio::test]
#[serial]
async fn scan_normalizes_type_per_cell() {
    if !fixture_data_present("test_basic", "simple_table") {
        eprintln!("Skipping (H5 wiring): test_basic/simple_table Data.db not present");
        return;
    }
    let Some(db) = setup("test_basic", "basic-types.cql").await else {
        eprintln!("Skipping (H5 wiring): could not ingest test_basic");
        return;
    };

    rwc::reset();
    assert_eq!(
        rwc::type_normalize_calls(),
        0,
        "reset must zero TYPE_NORMALIZE_CALLS"
    );

    let scan = db
        .execute("SELECT * FROM test_basic.simple_table")
        .await
        .expect("scan of test_basic.simple_table");
    let rows = scan.rows.len() as u64;
    assert!(
        rows > 0,
        "present fixture must return rows (0 rows = read regression, not a skip)"
    );

    let normalizes = rwc::type_normalize_calls();
    eprintln!("H5: rows={rows} TYPE_NORMALIZE_CALLS={normalizes}");
    // Today's (wasteful) cost: at least one type normalization per returned row —
    // the per-cell `to_lowercase` J1 will eliminate.
    assert!(
        normalizes >= rows,
        "H5: type normalization must scale with cells scanned (>= rows); got \
         {normalizes} for {rows} rows"
    );
}

/// Scenario (issue #1618 roborev undercount fix): the empty-value cell path must
/// count its per-cell type normalization too. `test_types/nb_null_empty_text_blob`
/// row `ck=4` writes an empty `text` (`''`) and an empty `blob` (`0x`) — both take
/// the `HAS_EMPTY_VALUE` early-return in `parse_cell_value_schema_order`, which
/// still `to_lowercase()`-normalizes the declared type. Before the fix that
/// normalization allocation ran UNCOUNTED (the counter only fired on the live-cell
/// path AFTER the empty early-return), so a future J1 `== 0` assertion could pass
/// while the hot-path allocation still happened. The fix normalizes once before
/// the empty/live split and records the counter at that single site, so both empty
/// cells are now counted.
///
/// Pinned counts for this 4-row fixture (deterministic parse):
///   - post-fix `TYPE_NORMALIZE_CALLS` = **29**
///   - pre-fix it was **27** — exactly `empty_simple` (=2) lower, because the two
///     empty simple cells in `ck=4` went uncounted. This assertion therefore fails
///     on the pre-fix undercount and passes after.
#[tokio::test]
#[serial]
async fn scan_counts_empty_cell_type_normalization() {
    if !fixture_data_present("test_types", "nb_null_empty_text_blob") {
        eprintln!("Skipping (H5 wiring): test_types/nb_null_empty_text_blob not present");
        return;
    }
    let Some(db) = setup("test_types", "cql-type-parity.cql").await else {
        eprintln!("Skipping (H5 wiring): could not ingest test_types");
        return;
    };

    rwc::reset();
    assert_eq!(
        rwc::type_normalize_calls(),
        0,
        "reset must zero TYPE_NORMALIZE_CALLS"
    );

    let scan = db
        .execute("SELECT * FROM test_types.nb_null_empty_text_blob")
        .await
        .expect("scan of nb_null_empty_text_blob");
    let rows = scan.rows.len();
    assert!(
        rows > 0,
        "present fixture must return rows (0 rows = read regression, not a skip)"
    );

    // Count the empty simple cells the scan surfaced — these are the cells that
    // take the HAS_EMPTY_VALUE early-return in the value parser.
    let mut empty_simple = 0usize;
    for row in &scan.rows {
        for v in row.values.values() {
            match v {
                cqlite_core::Value::Text(s) if s.is_empty() => empty_simple += 1,
                cqlite_core::Value::Blob(b) if b.is_empty() => empty_simple += 1,
                _ => {}
            }
        }
    }

    let normalizes = rwc::type_normalize_calls();
    eprintln!("H5: rows={rows} empty_simple={empty_simple} TYPE_NORMALIZE_CALLS={normalizes}");

    // Fixture-shape guards: fail loudly (pointing at the fixture) if the pinned
    // dataset ever regenerates into a different shape than these counts assume.
    assert_eq!(rows, 4, "fixture shape changed: expected 4 rows");
    assert_eq!(
        empty_simple, 2,
        "fixture shape changed: expected 2 empty simple cells (empty text + empty blob in ck=4)"
    );

    // The empty-cell type normalization is now counted (pre-fix undercount was 27).
    assert_eq!(
        normalizes, 29,
        "empty-cell type normalization must be counted: expected 29 \
         (pre-fix undercount was 27, missing the {empty_simple} empty simple cells); got {normalizes}"
    );
}

/// Scenario (K2/K3 currency): a full scan speculatively try-parses a partition
/// header at every partition boundary, so `PARTITION_HEADER_TRY_PARSES` is at
/// least the partition count. For `test_basic/simple_table` each row is its own
/// partition, so a scan bumps it at least once per returned row. K2/K3 (one
/// try-parse per partition) flip this to an exact per-partition bound.
#[tokio::test]
#[serial]
async fn scan_try_parses_partition_headers() {
    if !fixture_data_present("test_basic", "simple_table") {
        eprintln!("Skipping (H5 wiring): test_basic/simple_table Data.db not present");
        return;
    }
    let Some(db) = setup("test_basic", "basic-types.cql").await else {
        eprintln!("Skipping (H5 wiring): could not ingest test_basic");
        return;
    };

    rwc::reset();
    assert_eq!(
        rwc::partition_header_try_parses(),
        0,
        "reset must zero PARTITION_HEADER_TRY_PARSES"
    );

    let scan = db
        .execute("SELECT * FROM test_basic.simple_table")
        .await
        .expect("scan of test_basic.simple_table");
    let rows = scan.rows.len() as u64;
    assert!(rows > 0, "present fixture must return rows");

    let tries = rwc::partition_header_try_parses();
    eprintln!("H5: rows={rows} PARTITION_HEADER_TRY_PARSES={tries}");
    assert!(
        tries >= 1,
        "H5: a scan must speculatively try-parse at least one partition header; got {tries}"
    );
}

/// Scenario (K2/L currency): the shared display-row builder sorts each row's cells
/// once, so `ROW_SORT_INVOCATIONS` is at least the number of returned live rows.
/// K2/L (cells arrive pre-sorted) flip this to prove no per-row sort.
#[tokio::test]
#[serial]
async fn scan_sorts_cells_per_row() {
    if !fixture_data_present("test_basic", "simple_table") {
        eprintln!("Skipping (H5 wiring): test_basic/simple_table Data.db not present");
        return;
    }
    let Some(db) = setup("test_basic", "basic-types.cql").await else {
        eprintln!("Skipping (H5 wiring): could not ingest test_basic");
        return;
    };

    rwc::reset();
    assert_eq!(
        rwc::row_sort_invocations(),
        0,
        "reset must zero ROW_SORT_INVOCATIONS"
    );

    let scan = db
        .execute("SELECT * FROM test_basic.simple_table")
        .await
        .expect("scan of test_basic.simple_table");
    let rows = scan.rows.len() as u64;
    assert!(rows > 0, "present fixture must return rows");

    let sorts = rwc::row_sort_invocations();
    eprintln!("H5: rows={rows} ROW_SORT_INVOCATIONS={sorts}");
    // Every returned live row required exactly one per-row cell sort today; markers
    // (filtered out of the result) require none, so sorts is at least the returned
    // live-row count.
    assert!(
        sorts >= rows,
        "H5: the display-row builder sorts each live row's cells (>= rows); got \
         {sorts} for {rows} rows"
    );
}

/// Scenario (L1/L3 currency): a BTI (`da`) point read descends the
/// `Partitions.db` trie. The targeted descent decodes each internal node it
/// follows (`BTI_POINTER_DECODES`), and the first point read builds the cached
/// successor index by enumerating the whole trie in-order (`BTI_NODES_VISITED`),
/// so both counters are positive after a real point read through the `Database`
/// API. Counters are reset AFTER learning the key (the learning scan does not
/// walk the trie) so only the point read's trie work is measured. Uses the
/// optional `test_da` BTI corpus; skips (never fails) when it is absent. L1/L3
/// (<40 BTI nodes visited) flip these to bounded counts.
#[tokio::test]
#[serial]
async fn bti_point_read_visits_and_decodes_nodes() {
    if !fixture_data_present("test_da", "simple_table") {
        eprintln!("Skipping (H5 wiring): optional BTI test_da/simple_table not present");
        return;
    }
    let Some(db) = setup("test_da", "da-test.cql").await else {
        eprintln!("Skipping (H5 wiring): could not ingest test_da");
        return;
    };
    let Some(point_sql) = learn_point_sql(&db, "test_da.simple_table").await else {
        panic!("H5: could not learn a present UUID key from test_da.simple_table");
    };

    rwc::reset();
    assert_eq!(
        rwc::bti_nodes_visited(),
        0,
        "reset must zero BTI_NODES_VISITED"
    );
    assert_eq!(
        rwc::bti_pointer_decodes(),
        0,
        "reset must zero BTI_POINTER_DECODES"
    );

    let res = db.execute(&point_sql).await.expect("BTI point read");
    assert!(
        !res.rows.is_empty(),
        "H5: BTI point read on a known-present key returned zero rows"
    );

    let nodes = rwc::bti_nodes_visited();
    let decodes = rwc::bti_pointer_decodes();
    eprintln!("H5: BTI_NODES_VISITED={nodes} BTI_POINTER_DECODES={decodes}");
    assert!(
        nodes >= 1,
        "H5: a BTI point read must visit at least one trie node (successor-index \
         enumeration); got {nodes}"
    );
    assert!(
        decodes >= 1,
        "H5: a BTI point read must decode at least one trie node; got {decodes}"
    );
}

/// Scenario: `reset` zeroes every parser counter — a pure-API check that does not
/// require a fixture.
#[tokio::test]
#[serial]
async fn reset_zeroes_parser_counters() {
    if let Some(db) = setup("test_basic", "basic-types.cql").await {
        if fixture_data_present("test_basic", "simple_table") {
            let _ = db.execute("SELECT * FROM test_basic.simple_table").await;
        }
    }
    rwc::reset();
    assert_eq!(rwc::type_normalize_calls(), 0);
    assert_eq!(rwc::partition_header_try_parses(), 0);
    assert_eq!(rwc::bti_nodes_visited(), 0);
    assert_eq!(rwc::bti_pointer_decodes(), 0);
    assert_eq!(rwc::row_sort_invocations(), 0);
}
