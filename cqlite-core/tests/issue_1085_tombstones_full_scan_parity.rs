//! Issue #1085: a full-table `SELECT *` over a compressed, *clustered* table must
//! return every clustering row — in EVERY feature build, including the `tombstones`
//! feature (which `--all-features` enables).
//!
//! ROOT CAUSE (not what the issue title says): the bug was never in `experimental`
//! / legacy compression heuristics. It was the `#[cfg(feature = "tombstones")]`
//! variant of `SSTableManager::scan`, which grouped per-row scan results into a
//! `HashMap` keyed on `RowKey`. `RowKey` carries only the *partition-key* bytes
//! (no clustering), so all clustering rows of a partition collided into one bucket
//! and `TombstoneMerger::merge_generations` collapsed each bucket to a single row.
//! A `SELECT *` over `test_timeseries.sensor_data` (10 partitions × 200 clustering
//! rows = 2000 rows, LZ4-compressed `nb`) therefore returned only ~one partition's
//! worth of rows, stripped down to just the clustering column — instead of 2000
//! fully-populated rows. The fix makes the (now single) `scan` implementation
//! concatenate every row and reconcile only ACROSS generations, exactly like the
//! default build.
//!
//! This is an END-TO-END regression test driving the same `Database::execute`
//! query path the CLI uses. It asserts the full-scan row count for a clustered
//! compressed table equals the committed sstabledump JSONL golden, AND that the
//! rows carry their partition-key + regular columns (not just the clustering key).
//!
//! Fixtures resolve via `CQLITE_DATASETS_ROOT`; the table-dir UUID is never
//! hardcoded (globbed by `<table>-` prefix). When the dataset (or its gitignored
//! `*.db` binaries) is absent, the test SKIPs cleanly. When the fixture IS present,
//! a zero-row or mismatched result FAILS loudly.
//!
//! Requires `cli-helpers` (the `ingestion` module that builds a queryable
//! `Database`) and `state_machine` (the query engine); without them the file
//! compiles out, matching the other end-to-end SELECT integration tests.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

const KEYSPACE_FILTER: &str = "/test_timeseries/";

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schema_path() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        let p = root.parent()?.join("schemas").join("time-series.cql");
        if p.exists() {
            return Some(p);
        }
    }
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let p = manifest
        .parent()?
        .join("test-data")
        .join("schemas")
        .join("time-series.cql");
    p.exists().then_some(p)
}

/// Resolve `<root>/sstables/test_timeseries/<table>-<uuid>/`, globbing by prefix.
fn fixture_dir(table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let ks_dir = root.join("sstables").join("test_timeseries");
    if !ks_dir.is_dir() {
        return None;
    }
    let prefix = format!("{table}-");
    std::fs::read_dir(&ks_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&prefix))
                    .unwrap_or(false)
        })
}

/// Count the total CQL rows in the committed sstabledump JSONL golden (sum of the
/// `rows` arrays across every partition line, counting only `type == "row"`).
fn golden_row_count(table: &str) -> Option<usize> {
    let dir = fixture_dir(table)?;
    let jsonl = dir.join("nb-1-big-Data.db.jsonl");
    let text = std::fs::read_to_string(&jsonl).ok()?;
    let mut total = 0usize;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("{table}: golden JSONL parse failed: {e}\nline: {line}"));
        if let Some(rows) = v.get("rows").and_then(|r| r.as_array()) {
            total += rows
                .iter()
                .filter(|r| r.get("type").and_then(|t| t.as_str()) == Some("row"))
                .count();
        }
    }
    Some(total)
}

/// Ingest the `test_timeseries` keyspace and return a queryable `Database`, or a
/// skip reason. The Data.db binaries are gitignored, so a missing fixture SKIPs.
async fn setup_db() -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT unset or path missing")?;
    let schema = schema_path().ok_or("time-series.cql schema not found")?;
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }
    let sensor = fixture_dir("sensor_data").ok_or("test_timeseries/sensor_data-* dir absent")?;
    if !sensor.join("nb-1-big-Data.db").exists() {
        return Err(format!(
            "sensor_data Data.db missing (binary not fetched) at {sensor:?}"
        ));
    }

    let cfg = IngestionConfig {
        schema_paths: vec![schema],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(KEYSPACE_FILTER.to_string()),
    };
    let result = ingest(cfg).await.map_err(|e| format!("ingestion: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".into());
    }
    Ok(result.database)
}

/// The load-bearing regression: a clustered, LZ4-compressed `nb` table whose
/// partitions each hold many clustering rows. The `tombstones`-feature scan used
/// to collapse each partition to one row (≈2000 → ≈200, columns reduced to the
/// clustering key). This asserts BOTH the full row count and that the rows carry
/// the partition key + regular columns.
#[tokio::test]
async fn sensor_data_full_scan_returns_all_clustering_rows() {
    let db = match setup_db().await {
        Ok(db) => db,
        Err(skip) => {
            eprintln!("SKIP sensor_data_full_scan_returns_all_clustering_rows: {skip}");
            return;
        }
    };

    let golden = golden_row_count("sensor_data")
        .expect("sensor_data fixture present but golden JSONL missing/unreadable");
    assert!(
        golden > 0,
        "golden row count must be > 0 (fixture sanity); got {golden}"
    );

    let result = db
        .execute("SELECT * FROM test_timeseries.sensor_data")
        .await
        .expect("SELECT * over sensor_data must succeed");

    assert_eq!(
        result.rows.len(),
        golden,
        "full-scan row count {} != sstabledump golden {} — the tombstones scan path \
         collapsed multi-row partitions (issue #1085)",
        result.rows.len(),
        golden
    );

    // The collapse symptom also stripped every column except the clustering key.
    // Assert the partition key and a regular column survive on every row.
    for (i, row) in result.rows.iter().enumerate() {
        assert!(
            row.values.contains_key("sensor_id"),
            "row {i} is missing the partition-key column `sensor_id` — \
             rows were collapsed to the clustering key only (issue #1085)"
        );
        assert!(
            row.values.contains_key("temperature") || row.values.contains_key("location"),
            "row {i} is missing all regular columns — rows were collapsed (issue #1085)"
        );
    }
}
