//! Issue #1573 (Epic C / C2): the `ReadAt` positional point-read migration,
//! validated through the public `Database` API.
//!
//! Covers the observable spec scenarios that do not need internal injection:
//! - **fd high-water:** BTI point lookups no longer `open(2)` per call — 64
//!   concurrent lookups + 8 scans keep `FILE_OPENS` bounded (fails on `main`,
//!   where each BTI lookup minted a fresh scan-cursor fd).
//! - **value parity:** rows via the migrated point-read path byte-match the scan
//!   path (the correctness oracle) over the corpus.
//! - **scans unchanged:** a full scan is deterministic and non-empty after the
//!   point-read migration (the windowed pipeline is untouched).
//!
//! Compiled only with `--features work-counters` (the `FILE_OPENS` getter lives
//! behind it, mirroring `issue_1566_read_work_counters`). Requires
//! `CQLITE_DATASETS_ROOT` + fetched binaries; every test skips (never fails) when
//! its fixture is absent, and never treats 0 rows as a skip. Excluded under
//! `tombstones` (that build serves point reads via a full-scan filter rather than
//! the targeted seek this evidences).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "work-counters",
    not(feature = "tombstones")
))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::storage::sstable::read_work_counters as rwc;
use cqlite_core::{Database, Value};
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

/// True if `<datasets>/sstables/<keyspace>/<table>-*/` holds a `*-Data.db`. Skip
/// keys off fixture presence (not a 0-row result), so a present fixture that
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

/// Canonical unquoted 8-4-4-4-12 UUID literal the SELECT parser accepts (#956).
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

/// Every `id` UUID present in `table`, learned from a full scan (the oracle).
async fn learn_ids(db: &Database, table: &str) -> Vec<[u8; 16]> {
    let Ok(scan) = db.execute(&format!("SELECT id FROM {table}")).await else {
        return Vec::new();
    };
    scan.rows
        .iter()
        .filter_map(|r| match r.values.get("id") {
            Some(Value::Uuid(b)) => Some(*b),
            _ => None,
        })
        .collect()
}

/// Scenario (fd high-water): 64 concurrent BTI point lookups + 8 concurrent scans
/// keep `FILE_OPENS` bounded. On `main` each BTI lookup minted a fresh scan-cursor
/// fd (`new_scan_cursor` → `open(2)`), so 64 concurrent lookups recorded ~64
/// opens; the migrated positional path opens the fd once at reader-open and issues
/// positioned reads thereafter, so the lookups record none — only the (unchanged)
/// per-scan cursor opens remain.
#[tokio::test]
#[serial]
async fn bti_lookups_do_not_open_per_lookup() {
    if !fixture_data_present("test_da", "simple_table") {
        eprintln!("SKIP (#1573 fd): optional BTI test_da/simple_table not present");
        return;
    }
    let Some(db) = setup("test_da", "da-test.cql").await else {
        eprintln!("SKIP (#1573 fd): could not ingest test_da");
        return;
    };
    let table = "test_da.simple_table";
    let ids = learn_ids(&db, table).await;
    assert!(
        !ids.is_empty(),
        "present BTI fixture must yield >= 1 id (0 = read regression, not a skip)"
    );
    let db = std::sync::Arc::new(db);

    // Reader is now open (from the learning scan); reset so we measure only the
    // opens the 64 lookups + 8 scans trigger.
    rwc::reset();
    assert_eq!(rwc::file_opens(), 0, "reset must zero FILE_OPENS");
    let fd_before = rwc::fd_high_water();

    let point_sql = format!(
        "SELECT id, name FROM {table} WHERE id = {}",
        uuid_to_literal(&ids[0])
    );
    let mut handles = Vec::new();
    for _ in 0..64u32 {
        let (d, sql) = (db.clone(), point_sql.clone());
        handles.push(tokio::spawn(async move { d.execute(&sql).await }));
    }
    for _ in 0..8u32 {
        let d = db.clone();
        let sql = format!("SELECT id FROM {table}");
        handles.push(tokio::spawn(async move { d.execute(&sql).await }));
    }
    for h in handles {
        let _ = h.await.expect("task").expect("query");
    }

    // The 64 point lookups must contribute ZERO opens; only the 8 scans mint
    // cursors (unchanged). A generous cap distinguishes the fixed path (~8) from
    // main's per-lookup opens (~64 + 8). If a lookup ever opened, this trips.
    let opens = rwc::file_opens();
    assert!(
        opens <= 16,
        "64 BTI lookups + 8 scans recorded {opens} opens; the migrated point path \
         must not open(2) per lookup (main records ~72 here). Expected <= 16 \
         (the per-scan cursor opens only)."
    );

    // Secondary sanity via the fd sampler where supported: the process fd count
    // after the ops drains back near the open-time baseline (scan cursors closed).
    if let (Some(before), Some(after)) = (fd_before, rwc::fd_high_water()) {
        assert!(
            after <= before + 24,
            "fd count grew unexpectedly: before={before}, after={after}"
        );
    }
}

/// Scenario (value parity): every row returned by the migrated point-read path
/// byte-matches the scan path (the correctness oracle) for the corpus table.
///
/// Issue #3890 (AC2): the comparison is CELL-LEVEL ACROSS EVERY COLUMN the scan
/// returns (`SELECT *`), in both directions — every scan column must be PRESENT in
/// the point row with an equal value, and the point row must carry no column the
/// scan row lacks. This test previously projected `id` plus ONE named column, which
/// is exactly why a point read whose later cells failed to decode stayed green for
/// years: the two compared columns decoded fine and everything after the failure
/// was simply absent from the row. `value_col` is retained as the historically
/// projected non-key column and is asserted to be among the columns compared, so
/// the strengthening can never silently degrade to comparing key columns alone.
async fn assert_point_equals_scan(
    keyspace: &str,
    schema: &str,
    table_short: &str,
    table: &str,
    value_col: &str,
) {
    if !fixture_data_present(keyspace, table_short) {
        eprintln!("SKIP (#1573 parity): {keyspace}/{table_short} Data.db absent");
        return;
    }
    let Some(db) = setup(keyspace, schema).await else {
        eprintln!("SKIP (#1573 parity): could not ingest {keyspace}");
        return;
    };
    let ids = learn_ids(&db, table).await;
    assert!(
        !ids.is_empty(),
        "present fixture must yield >= 1 id (0 = read regression, not a skip)"
    );

    // Oracle: the full scan's WHOLE row for each id (every column).
    let scan = db
        .execute(&format!("SELECT * FROM {table}"))
        .await
        .expect("scan");
    assert!(
        !scan.rows.is_empty(),
        "present fixture must yield >= 1 scanned row (0 = read regression, not a skip)"
    );
    for id in &ids {
        let point = db
            .execute(&format!(
                "SELECT * FROM {table} WHERE id = {}",
                uuid_to_literal(id)
            ))
            .await
            .expect("point read");
        assert_eq!(
            point.rows.len(),
            1,
            "point read of a present key must return exactly one row"
        );
        let scanned = scan
            .rows
            .iter()
            .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(b)) if b == id))
            .expect("scanned row for id");
        let got = &point.rows[0].values;
        assert!(
            scanned.values.contains_key(value_col),
            "oracle row for id={} must carry the projected column '{value_col}' — without it \
             this comparison would degrade to key columns only",
            uuid_to_literal(id)
        );
        // Direction 1: no column the SCAN saw may be MISSING from the point row,
        // and every shared column must byte-match. A truncated point-read row
        // (issue #3890) shows up here as an absent column, not a wrong value.
        for (col, want) in scanned.values.iter() {
            match got.get(col) {
                Some(have) => assert_eq!(
                    have,
                    want,
                    "point-read column '{col}' for id={} must byte-match the scan path",
                    uuid_to_literal(id)
                ),
                None => panic!(
                    "point-read row for id={} is MISSING column '{col}' that the scan path \
                     returned — a truncated point-read row (issue #3890). Point row has {:?}",
                    uuid_to_literal(id),
                    {
                        let mut names: Vec<&str> = got.keys().map(|k| &**k).collect();
                        names.sort_unstable();
                        names
                    }
                ),
            }
        }
        // Direction 2: and no column the scan did NOT return.
        for col in got.keys() {
            assert!(
                scanned.values.contains_key(&**col),
                "point-read row for id={} carries column '{col}' the scan path did not return",
                uuid_to_literal(id)
            );
        }
    }
}

#[tokio::test]
#[serial]
async fn big_point_read_matches_scan() {
    assert_point_equals_scan(
        "test_basic",
        "basic-types.cql",
        "simple_table",
        "test_basic.simple_table",
        "name",
    )
    .await;
}

/// Exercises the whole-section point-read fallback (`point_read_whole_section`)
/// AND its CRC.db verification: `uncompressed_table` has no CompressionInfo, so
/// the point path reads the whole data section positionally and CRC-checks it.
#[tokio::test]
#[serial]
async fn big_uncompressed_point_read_matches_scan() {
    assert_point_equals_scan(
        "test_basic",
        "basic-types.cql",
        "uncompressed_table",
        "test_basic.uncompressed_table",
        "data",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn bti_point_read_matches_scan() {
    assert_point_equals_scan(
        "test_da",
        "da-test.cql",
        "simple_table",
        "test_da.simple_table",
        "name",
    )
    .await;
}

/// Scenario (scans unchanged): a full scan is non-empty and deterministic after
/// the point-read migration — two consecutive scans return identical rows in
/// identical order (the windowed pipeline is untouched by this change).
#[tokio::test]
#[serial]
async fn scan_output_is_unchanged_and_deterministic() {
    if !fixture_data_present("test_basic", "simple_table") {
        eprintln!("SKIP (#1573 scan): test_basic/simple_table Data.db absent");
        return;
    }
    let Some(db) = setup("test_basic", "basic-types.cql").await else {
        eprintln!("SKIP (#1573 scan): could not ingest test_basic");
        return;
    };
    let sql = "SELECT id, name FROM test_basic.simple_table";
    let first = db.execute(sql).await.expect("first scan");
    assert!(
        !first.rows.is_empty(),
        "present fixture must return rows (0 rows = read regression, not a skip)"
    );
    let second = db.execute(sql).await.expect("second scan");
    assert_eq!(
        first.rows.len(),
        second.rows.len(),
        "scan row count must be stable across runs"
    );
    for (a, b) in first.rows.iter().zip(second.rows.iter()) {
        assert_eq!(
            a.values.get("id"),
            b.values.get("id"),
            "scan order/content must be deterministic (id)"
        );
        assert_eq!(
            a.values.get("name"),
            b.values.get("name"),
            "scan order/content must be deterministic (name)"
        );
    }
}
