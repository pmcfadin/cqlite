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

/// Scenario (J1 delivered): a full scan resolves each column's decode dispatch
/// ONCE per block (`RowColumnResolution::build`), so NO `to_lowercase` runs in the
/// per-cell decode path. `TYPE_NORMALIZE_CALLS` — the per-cell-loop normalization
/// gauge — therefore reads exactly `0` after a scan. On `main` (pre-J1) two
/// normalization sites fired per non-key cell (the value-parse `to_lowercase` and
/// the per-row `is_complex_column`), so the count was at least one per returned row;
/// this `== 0` assertion FAILS there and PASSES after J1. The parallel `rows > 0`
/// assertion keeps the check non-vacuous: the per-column dispatch must still decode
/// real rows, not skip them.
#[tokio::test]
#[serial]
async fn scan_does_not_normalize_type_per_cell() {
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
    eprintln!("H5 (J1): rows={rows} TYPE_NORMALIZE_CALLS={normalizes}");
    // J1: dispatch is resolved once per column at block bind time, so the per-cell
    // decode path performs ZERO type normalizations. (Pre-J1 this was >= rows.)
    assert_eq!(
        normalizes, 0,
        "J1: the per-cell decode path must perform zero type normalizations \
         (dispatch resolved once per column); got {normalizes} for {rows} rows"
    );
}

/// Scenario (J1 delivered, empty-value path): the empty (`HAS_EMPTY_VALUE`)
/// early-return in `parse_cell_value_schema_order` used to `to_lowercase()`-normalize
/// the declared type per cell too (issue #1618 counted it at 29 for this 4-row
/// fixture). J1 dispatches the empty-value early-return on the precomputed per-column
/// `CellKind`, so it performs ZERO per-cell normalizations. `test_types/
/// nb_null_empty_text_blob` row `ck=4` writes an empty `text` (`''`) and an empty
/// `blob` (`0x`); this scan must still surface them (parity) while
/// `TYPE_NORMALIZE_CALLS` reads `0`. On `main` this fixture read 29 — the `== 0`
/// assertion FAILS there and PASSES after J1.
#[tokio::test]
#[serial]
async fn scan_empty_cells_do_not_normalize_type() {
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
    eprintln!("H5 (J1): rows={rows} empty_simple={empty_simple} TYPE_NORMALIZE_CALLS={normalizes}");

    // Fixture-shape guards: fail loudly (pointing at the fixture) if the pinned
    // dataset ever regenerates into a different shape than these counts assume.
    assert_eq!(rows, 4, "fixture shape changed: expected 4 rows");
    assert_eq!(
        empty_simple, 2,
        "fixture shape changed: expected 2 empty simple cells (empty text + empty blob in ck=4)"
    );

    // J1: the empty-value early-return dispatches on the precomputed per-column
    // CellKind, so it performs zero per-cell normalizations (pre-J1 this fixture
    // read 29). The empty text/blob cells above prove the path is still exercised.
    assert_eq!(
        normalizes, 0,
        "J1: the empty-value cell path must perform zero per-cell type \
         normalizations (dispatch resolved once per column); got {normalizes}"
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

/// Scenario (K2 DELIVERED): the per-row partition-BOUNDARY peek is now
/// non-allocating (`peek_partition_boundary`) and does NOT run the full
/// `parse_partition_header_full` (no key `to_vec`, no `PARTITION_HEADER_TRY_PARSES`
/// increment). So on a genuinely WIDE-partition fixture — `test_timeseries/
/// sensor_data` has ~200 rows per `sensor_id` partition — a full scan try-parses a
/// partition header once per PARTITION (at a confirmed start), not once per ROW.
///
/// The assertion brackets the count as a per-partition bound:
/// `distinct_partitions <= PARTITION_HEADER_TRY_PARSES < rows`. On `main` the
/// per-row peek made the count `>= rows` (each of the ~200 rows/partition
/// try-parsed), so `tries < rows` FAILS there and PASSES after K2. The lower bound
/// (`>= distinct_partitions`) keeps it non-vacuous: every partition's header is
/// still really parsed at least once.
#[tokio::test]
#[serial]
async fn scan_boundary_peek_does_not_try_parse_per_row() {
    if !fixture_data_present("test_timeseries", "sensor_data") {
        eprintln!("Skipping (K2 wiring): test_timeseries/sensor_data Data.db not present");
        return;
    }
    let Some(db) = setup("test_timeseries", "time-series.cql").await else {
        eprintln!("Skipping (K2 wiring): could not ingest test_timeseries");
        return;
    };

    rwc::reset();
    assert_eq!(
        rwc::partition_header_try_parses(),
        0,
        "reset must zero PARTITION_HEADER_TRY_PARSES"
    );

    let scan = db
        .execute("SELECT * FROM test_timeseries.sensor_data")
        .await
        .expect("scan of test_timeseries.sensor_data");
    let rows = scan.rows.len() as u64;
    assert!(
        rows > 0,
        "present fixture must return rows (0 rows = read regression, not a skip)"
    );

    // Distinct partitions = distinct `sensor_id` (the partition key) values.
    let mut partitions = std::collections::HashSet::new();
    for row in &scan.rows {
        if let Some(cqlite_core::Value::Uuid(b)) = row.values.get("sensor_id") {
            partitions.insert(*b);
        }
    }
    let distinct_partitions = partitions.len() as u64;
    assert!(
        distinct_partitions >= 1,
        "fixture shape: sensor_data must expose at least one sensor_id partition"
    );
    // Genuinely wide: many more rows than partitions (otherwise `tries < rows`
    // could not distinguish per-row from per-partition). Guards the fixture shape.
    assert!(
        rows >= distinct_partitions * 2,
        "fixture shape changed: expected a WIDE fixture (rows {rows} >= 2 * partitions \
         {distinct_partitions}); K2's per-row-vs-per-partition claim is only testable when wide"
    );

    let tries = rwc::partition_header_try_parses();
    eprintln!(
        "K2: rows={rows} distinct_partitions={distinct_partitions} \
         PARTITION_HEADER_TRY_PARSES={tries}"
    );

    // K2 headline: the boundary peek stopped try-parsing per row, so the full
    // parse now runs per PARTITION, not per row. FAILS on `main` (tries >= rows).
    assert!(
        tries < rows,
        "K2: a scan must try-parse partition headers fewer times than it returns \
         rows (per-partition, not per-row); got tries={tries} rows={rows}"
    );
    // Non-vacuous lower bound: each partition's header is really parsed at least
    // once, so the count is at least the distinct-partition count.
    assert!(
        tries >= distinct_partitions,
        "K2: every partition's header must be parsed at least once; got tries={tries} \
         distinct_partitions={distinct_partitions}"
    );
}

/// Scenario (K3 delivered, issue #1642): the decoder emits each row's cells
/// positionally in serialization-header column order, so the shared display-row
/// builder performs NO per-row sort — `ROW_SORT_INVOCATIONS == 0` on a full scan.
/// This flipped the former `>= rows` currency assertion when K3 landed; the
/// dedicated wiring-evidence across column shapes lives in
/// `issue_1642_positional_row_emit.rs`.
#[tokio::test]
#[serial]
async fn scan_does_not_sort_cells_per_row() {
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
    // K3 (issue #1642): positional emit means zero per-row cell sorts. Any
    // reintroduced per-row sort must call `record_row_sort` and would flip this red.
    assert_eq!(
        sorts, 0,
        "K3: positional emit performs zero per-row cell sorts; got {sorts} for {rows} rows"
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
