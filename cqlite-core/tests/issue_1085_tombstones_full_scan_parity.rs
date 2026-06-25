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
//! A `SELECT *` over a clustered table therefore returned roughly one row per
//! partition, stripped down to just the clustering column — instead of every
//! fully-populated clustering row. The fix makes the (now single) `scan`
//! implementation concatenate every row and reconcile only ACROSS generations,
//! exactly like the default build.
//!
//! These are END-TO-END regression tests driving the same `Database::execute`
//! query path the CLI uses. They assert the full-scan row count for a clustered
//! compressed table equals the committed sstabledump JSONL golden, AND that the
//! rows carry their partition-key + regular columns (not just the clustering key).
//!
//! Two complementary fixtures:
//!   * `test_timeseries.sensor_data` — 10 partitions × 200 clustering rows
//!     (LZ4 `nb`). The collapse symptom here is 2000 → ~200.
//!   * `test_tomb.wide_range_tombstone` — a SINGLE partition × 2987 clustering
//!     rows spanning many compression chunks (LZ4 `nb`). The collapse symptom
//!     here is the dramatic 2987 → ~1, and it also exercises a partition that
//!     spans multiple compressed chunks.
//!
//! Fixtures resolve via `CQLITE_DATASETS_ROOT`; the table-dir UUID is never
//! hardcoded (globbed by `<table>-` prefix). When the dataset (or its gitignored
//! `*.db` binaries) is absent, the tests SKIP cleanly. When the fixture IS
//! present, a zero-row or mismatched result FAILS loudly.
//!
//! Requires `cli-helpers` (the `ingestion` module that builds a queryable
//! `Database`) and `state_machine` (the query engine); without them the file
//! compiles out, matching the other end-to-end SELECT integration tests.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Resolve `<repo>/test-data/schemas/<file>`, preferring the location derived from
/// `CQLITE_DATASETS_ROOT` and falling back to the crate-relative path.
fn schema_path(file: &str) -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        let p = root.parent()?.join("schemas").join(file);
        if p.exists() {
            return Some(p);
        }
    }
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let p = manifest
        .parent()?
        .join("test-data")
        .join("schemas")
        .join(file);
    p.exists().then_some(p)
}

/// Resolve `<root>/sstables/<keyspace>/<table>-<uuid>/`, globbing by prefix.
fn fixture_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let ks_dir = root.join("sstables").join(keyspace);
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
/// `rows` arrays across every partition line, counting only `type == "row"` so
/// range-tombstone bound markers are excluded).
fn golden_row_count(keyspace: &str, table: &str) -> Option<usize> {
    let dir = fixture_dir(keyspace, table)?;
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

/// Ingest a single keyspace and return a queryable `Database`, or a skip reason.
/// The Data.db binaries are gitignored, so a missing fixture SKIPs.
async fn setup_db(
    keyspace: &str,
    schema_file: &str,
    sentinel_table: &str,
) -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT unset or path missing")?;
    let schema =
        schema_path(schema_file).ok_or_else(|| format!("{schema_file} schema not found"))?;
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }
    let sentinel = fixture_dir(keyspace, sentinel_table)
        .ok_or_else(|| format!("{keyspace}/{sentinel_table}-* dir absent"))?;
    if !sentinel.join("nb-1-big-Data.db").exists() {
        return Err(format!(
            "{sentinel_table} Data.db missing (binary not fetched) at {sentinel:?}"
        ));
    }

    let cfg = IngestionConfig {
        schema_paths: vec![schema],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(cfg).await.map_err(|e| format!("ingestion: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".into());
    }
    Ok(result.database)
}

/// Run a full `SELECT *` over `keyspace.table` and assert the row count equals the
/// sstabledump JSONL golden AND that every row carries the named partition-key and
/// regular columns (the collapse symptom stripped rows down to the clustering key).
async fn assert_full_scan_parity(
    keyspace: &str,
    schema_file: &str,
    table: &str,
    pk_col: &str,
    regular_cols: &[&str],
) {
    let test_name = format!("{keyspace}.{table}");
    let db = match setup_db(keyspace, schema_file, table).await {
        Ok(db) => db,
        Err(skip) => {
            eprintln!("SKIP full_scan_parity {test_name}: {skip}");
            return;
        }
    };

    let golden = golden_row_count(keyspace, table).unwrap_or_else(|| {
        panic!("{test_name} fixture present but golden JSONL missing/unreadable")
    });
    assert!(
        golden > 0,
        "{test_name}: golden row count must be > 0 (fixture sanity); got {golden}"
    );

    let query = format!("SELECT * FROM {keyspace}.{table}");
    let result = db
        .execute(&query)
        .await
        .unwrap_or_else(|e| panic!("{test_name}: SELECT * must succeed: {e}"));

    assert_eq!(
        result.rows.len(),
        golden,
        "{test_name}: full-scan row count {} != sstabledump golden {} — the tombstones \
         scan path collapsed multi-row partitions (issue #1085)",
        result.rows.len(),
        golden
    );

    // The collapse symptom also stripped every column except the clustering key.
    // Assert the partition key and at least one regular column survive on every row.
    for (i, row) in result.rows.iter().enumerate() {
        assert!(
            row.values.contains_key(pk_col),
            "{test_name}: row {i} is missing the partition-key column `{pk_col}` — \
             rows were collapsed to the clustering key only (issue #1085)"
        );
        assert!(
            regular_cols.iter().any(|c| row.values.contains_key(*c)),
            "{test_name}: row {i} is missing all regular columns {regular_cols:?} — \
             rows were collapsed (issue #1085)"
        );
    }
}

/// Multi-partition clustered table (10 partitions × 200 clustering rows). The
/// `tombstones` scan used to collapse each partition to one row (2000 → ~200,
/// columns reduced to the clustering key).
#[tokio::test]
async fn sensor_data_full_scan_returns_all_clustering_rows() {
    assert_full_scan_parity(
        "test_timeseries",
        "time-series.cql",
        "sensor_data",
        "sensor_id",
        &["temperature", "location"],
    )
    .await;
}

/// SINGLE-partition clustered table (1 partition × 2987 clustering rows spanning
/// many compression chunks). The collapse symptom here is the dramatic 2987 → ~1,
/// and it also exercises a partition spanning multiple compressed chunks. The
/// golden counter excludes the 6 `range_tombstone_bound` markers (type != "row").
#[tokio::test]
async fn wide_range_tombstone_single_partition_full_scan() {
    assert_full_scan_parity(
        "test_tomb",
        "tombstone-parity.cql",
        "wide_range_tombstone",
        "pk",
        &["val"],
    )
    .await;
}
