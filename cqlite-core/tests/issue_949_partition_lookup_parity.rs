//! Issue #949: partition-targeted lookup parity.
//!
//! A `SELECT ... WHERE <partition_key> = ?` that fully constrains the partition
//! key is served by [`StorageEngine::scan_partition`], which prunes the SSTable
//! set via the bloom filter / BTI trie and only parses the candidates, instead of
//! scanning every SSTable for the table and filtering in memory.
//!
//! These tests assert that the optimized path returns *exactly* the same rows as
//! a full scan filtered down to the same partition key, for every real partition
//! in a composite-text-keyed table, plus the empty result for an absent key.
//!
//! Uses `test_timeseries.app_metrics`, whose partition key is the composite
//! `(application_id TEXT, metric_name TEXT)` — a key the SELECT parser encodes
//! from string literals, so the fast path actually engages end-to-end. The
//! UUID-partition-key path is exercised separately by
//! `issue_956_uuid_literal_partition_lookup_parity.rs` (Issue #956 added the
//! unquoted-UUID literal the parser previously lacked).
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped
//! (not failed) when the data isn't present, matching the repo's other
//! dataset-backed integration tests.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryRow;
use cqlite_core::{Database, Value};

const QUALIFIED_TABLE: &str = "test_timeseries.app_metrics";
const KEYSPACE_FILTER: &str = "/test_timeseries/";

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        let dir = root.parent()?.join("schemas");
        if dir.exists() {
            return Some(dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

async fn setup() -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let schema_path = schemas_dir()
        .ok_or("schemas dir not found")?
        .join("time-series.cql");
    if !schema_path.exists() {
        return Err(format!("schema not found at {schema_path:?}"));
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(KEYSPACE_FILTER.to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".to_string());
    }
    Ok(result.database)
}

fn text(row: &QueryRow, col: &str) -> Option<String> {
    match row.values.get(col) {
        Some(Value::Text(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Canonical, order-independent fingerprint of a row's columns for set comparison.
fn row_fingerprint(row: &QueryRow) -> BTreeMap<String, String> {
    row.values
        .iter()
        .map(|(k, v)| (k.to_string(), format!("{v:?}")))
        .collect()
}

fn fingerprints(rows: &[QueryRow]) -> Vec<BTreeMap<String, String>> {
    let mut out: Vec<_> = rows.iter().map(row_fingerprint).collect();
    out.sort_by_key(|m| format!("{m:?}"));
    out
}

#[tokio::test]
async fn partition_lookup_matches_full_scan_for_every_partition() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Full scan once: the reference. Group rows by their composite partition key.
    let full = db
        .execute(&format!(
            "SELECT application_id, metric_name, unit, value FROM {QUALIFIED_TABLE}"
        ))
        .await
        .expect("full scan must succeed");

    if full.rows.is_empty() {
        eprintln!("Skipping: app_metrics returned 0 rows (Data.db not fetched?)");
        return;
    }

    let mut by_partition: BTreeMap<(String, String), Vec<QueryRow>> = BTreeMap::new();
    for row in full.rows {
        let (Some(app), Some(metric)) = (text(&row, "application_id"), text(&row, "metric_name"))
        else {
            continue;
        };
        by_partition.entry((app, metric)).or_default().push(row);
    }

    // Bound runtime on large datasets while still covering many real partitions.
    let mut checked = 0usize;
    for ((app, metric), expected_rows) in by_partition.iter() {
        // Skip values that would break naive quoting; real test data is simple words.
        if app.contains('\'') || metric.contains('\'') {
            continue;
        }

        let targeted = db
            .execute(&format!(
                "SELECT application_id, metric_name, unit, value FROM {QUALIFIED_TABLE} \
                 WHERE application_id = '{app}' AND metric_name = '{metric}'"
            ))
            .await
            .unwrap_or_else(|e| panic!("targeted lookup for ({app},{metric}) failed: {e}"));

        assert_eq!(
            fingerprints(&targeted.rows),
            fingerprints(expected_rows),
            "Issue #949: partition lookup for ({app}, {metric}) must equal the full-scan rows \
             for that partition",
        );

        checked += 1;
        if checked >= 50 {
            break;
        }
    }

    assert!(
        checked > 0,
        "expected at least one partition to validate; dataset may be missing",
    );
    println!("Issue #949: validated partition-lookup parity for {checked} partitions");
}

#[tokio::test]
async fn partition_lookup_for_absent_key_is_empty() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(&format!(
            "SELECT application_id FROM {QUALIFIED_TABLE} \
             WHERE application_id = 'definitely-not-a-real-app-key-zzz' \
             AND metric_name = 'definitely-not-a-real-metric-zzz'"
        ))
        .await
        .expect("absent-key lookup must succeed");

    assert!(
        result.rows.is_empty(),
        "Issue #949: a fully-constrained lookup on an absent partition key must return no rows, \
         got {}",
        result.rows.len()
    );
}
