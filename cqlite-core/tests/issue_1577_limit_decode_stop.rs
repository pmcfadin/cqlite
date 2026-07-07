//! Issue #1577 (Epic D / D1): wiring-evidence that a `LIMIT N` scan STOPS
//! DECODING rows past the limit, using the issue #1618 parser work counters.
//!
//! `PARTITION_HEADER_TRY_PARSES` is bumped once per partition header parsed
//! during a scan; for `test_basic/simple_table` each of its ~999 rows is its own
//! partition, so an UNBOUNDED scan parses ~999 headers. This test proves that a
//! `LIMIT 10` scan parses only a small `O(limit + buffer)` number of headers —
//! i.e. the reader really stops decoding the tail — while returning exactly the
//! same first 10 rows as the unbounded scan.
//!
//! Compiled only with `--features work-counters` (the counter getters/`reset`
//! live behind that feature). Requires `CQLITE_DATASETS_ROOT` + fetched binaries;
//! skips (never fails) when the fixture is absent, and treats a present-but-0-rows
//! result as a hard failure. Excluded under `tombstones` (that build's scan path
//! differs). The counters are process-global, so this serializes on `serial_test`.

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

fn row_ids(rows: &[cqlite_core::QueryRow]) -> Vec<[u8; 16]> {
    rows.iter()
        .filter_map(|r| match r.values.get("id") {
            Some(Value::Uuid(b)) => Some(*b),
            _ => None,
        })
        .collect()
}

/// `LIMIT 10` on the ~999-row `simple_table` must decode only `O(limit + buffer)`
/// partition headers, not all ~999 — AND return the identical first 10 rows as
/// the unbounded scan. Before this change the executor passed `None` and decoded
/// every row, so `PARTITION_HEADER_TRY_PARSES` was ~999 (the full-scan count this
/// test also measures as the oracle).
#[tokio::test]
#[serial]
async fn limit_stops_decoding_past_the_limit() {
    if !fixture_data_present("test_basic", "simple_table") {
        eprintln!("Skipping (#1577): test_basic/simple_table Data.db not present");
        return;
    }
    let Some(db) = setup("test_basic", "basic-types.cql").await else {
        eprintln!("Skipping (#1577): could not ingest test_basic");
        return;
    };

    // Oracle: the unbounded scan decodes one header per row.
    rwc::reset();
    let full = db
        .execute("SELECT * FROM test_basic.simple_table")
        .await
        .expect("full scan");
    let full_headers = rwc::partition_header_try_parses();
    let full_ids = row_ids(&full.rows);
    assert!(
        full_ids.len() > 100,
        "present fixture must return its full row set (got {}) — 0/low rows = read \
         regression, not a skip",
        full_ids.len()
    );
    assert!(
        full_headers >= full_ids.len() as u64,
        "sanity: an unbounded scan parses >= one header per returned row; got \
         {full_headers} headers for {} rows",
        full_ids.len()
    );

    // The bounded scan must decode DRAMATICALLY fewer headers.
    const LIMIT: usize = 10;
    rwc::reset();
    assert_eq!(
        rwc::partition_header_try_parses(),
        0,
        "reset must zero PARTITION_HEADER_TRY_PARSES"
    );
    let limited = db
        .execute(&format!(
            "SELECT * FROM test_basic.simple_table LIMIT {LIMIT}"
        ))
        .await
        .expect("limited scan");
    let limited_headers = rwc::partition_header_try_parses();
    eprintln!(
        "#1577: full_rows={} full_headers={full_headers} LIMIT {LIMIT} headers={limited_headers}",
        full_ids.len()
    );

    // Result parity: the bounded scan returns exactly the first LIMIT rows.
    assert_eq!(
        limited.rows.len(),
        LIMIT,
        "LIMIT {LIMIT} must return {LIMIT}"
    );
    assert_eq!(
        row_ids(&limited.rows),
        full_ids[..LIMIT].to_vec(),
        "LIMIT {LIMIT} rows must equal the first {LIMIT} full-scan rows, in order"
    );

    // Decode-stop evidence: headers parsed for LIMIT 10 must be a small
    // O(limit + buffer) count, far below the full-scan header count. The capped
    // stream buffer is `limit + 1`, so ~limit+buffer+a-few headers get decoded;
    // 128 is a generous ceiling that is still an order of magnitude below the
    // full ~999-header scan (this assertion FAILS on the pre-#1577 `None`-limit
    // code, which decoded every row).
    assert!(
        limited_headers <= 128,
        "LIMIT {LIMIT} must stop decoding: parsed {limited_headers} partition headers \
         (expected O(limit+buffer) ~ a few dozen), vs {full_headers} for the full scan"
    );
    assert!(
        limited_headers < full_headers,
        "LIMIT {LIMIT} ({limited_headers} headers) must decode strictly fewer headers \
         than the full scan ({full_headers})"
    );
}
