//! Integration test for Issue #757: PER PARTITION LIMIT
//!
//! **Feature**: `SELECT ... PER PARTITION LIMIT n` caps the number of rows
//! returned *per partition key*, applied before the query-wide `LIMIT`
//! (Cassandra semantics). Before this change the clause was parsed-around
//! (`per_partition: false` hardcoded) and had no effect.
//!
//! **Coverage**:
//! - per-partition cap on a genuinely wide-partitioned table
//! - combined `PER PARTITION LIMIT k LIMIT m` (global cap applies after)
//!
//! `test_wide_rows` is unusable here — every partition has exactly one row.
//! `test_timeseries.sensor_data` has ~10 partitions of 170+ rows each, so the
//! cap is meaningful. Each test asserts a non-vacuous precondition (uncapped
//! partitions exceed the cap) so a regression that silently drops enforcement
//! is caught.
//!
//! **Requirements**:
//! - `CQLITE_DATASETS_ROOT` pointing to `test-data/datasets`
//! - Real SSTable Data.db files (run `bash test-data/scripts/fetch-datasets.sh`)

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::collections::HashMap;
use std::path::PathBuf;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::Database;

const QUALIFIED_TABLE: &str = "test_timeseries.sensor_data";
const PARTITION_COLUMN: &str = "sensor_id";

fn get_datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn get_schemas_dir() -> Option<PathBuf> {
    if let Some(datasets_root) = get_datasets_root() {
        let schemas_dir = datasets_root.parent()?.join("schemas");
        if schemas_dir.exists() {
            return Some(schemas_dir);
        }
    }
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
    if schemas_dir.exists() {
        return Some(schemas_dir);
    }
    None
}

/// Returns true if at least one Data.db file exists for test_timeseries.
fn data_files_present() -> bool {
    let Some(root) = get_datasets_root() else {
        return false;
    };
    let dir = root.join("sstables").join("test_timeseries");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return false;
    };
    for entry in entries.flatten() {
        if let Ok(files) = std::fs::read_dir(entry.path()) {
            for file in files.flatten() {
                if file
                    .file_name()
                    .to_str()
                    .is_some_and(|n| n.ends_with("-Data.db"))
                {
                    return true;
                }
            }
        }
    }
    false
}

async fn setup_timeseries_db() -> Result<Database, String> {
    let datasets_root =
        get_datasets_root().ok_or_else(|| "CQLITE_DATASETS_ROOT not set".to_string())?;
    let schemas_dir = get_schemas_dir().ok_or_else(|| "schemas dir not found".to_string())?;

    let schema_path = schemas_dir.join("time-series.cql");
    if !schema_path.exists() {
        return Err(format!("schema not found at {:?}", schema_path));
    }

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: datasets_root.join("sstables"),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some("/test_timeseries/".to_string()),
    };

    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {}", e))?;
    Ok(result.database)
}

/// Stream a query and return the per-partition row counts keyed by the
/// partition column value (rendered as a string).
async fn counts_by_partition(db: &Database, sql: &str) -> HashMap<String, usize> {
    let mut iter = db
        .execute_streaming(sql, StreamingConfig::default())
        .await
        .expect("execute_streaming should succeed");

    let mut counts: HashMap<String, usize> = HashMap::new();
    while let Some(row) = iter.next_async().await {
        let row = row.expect("streamed row should be Ok");
        let key = row
            .get(PARTITION_COLUMN)
            .map(|v| format!("{:?}", v))
            .unwrap_or_else(|| "<missing>".to_string());
        *counts.entry(key).or_insert(0) += 1;
    }
    counts
}

/// Core: `PER PARTITION LIMIT k` must cap each partition at k rows.
#[tokio::test]
async fn test_per_partition_limit_caps_each_partition() {
    if !data_files_present() {
        eprintln!("Skipping: no Data.db files (run fetch-datasets.sh)");
        return;
    }
    let db = match setup_timeseries_db().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: setup failed: {}", e);
            return;
        }
    };

    // Precondition: uncapped, at least one partition has more than the cap so
    // the assertion below is non-vacuous.
    let cap = 3usize;
    let uncapped = counts_by_partition(&db, &format!("SELECT * FROM {}", QUALIFIED_TABLE)).await;
    assert!(
        !uncapped.is_empty(),
        "precondition: scan should return partitions"
    );
    assert!(
        uncapped.values().any(|&n| n > cap),
        "precondition: at least one partition must exceed the cap of {} to make \
         the test meaningful; got {:?}",
        cap,
        uncapped
    );
    let partitions_over_cap = uncapped.values().filter(|&&n| n > cap).count();

    let capped = counts_by_partition(
        &db,
        &format!(
            "SELECT * FROM {} PER PARTITION LIMIT {}",
            QUALIFIED_TABLE, cap
        ),
    )
    .await;

    // Same set of partitions appears (none dropped), each capped at `cap`.
    assert_eq!(
        capped.len(),
        uncapped.len(),
        "PER PARTITION LIMIT must not drop partitions"
    );
    for (partition, &n) in &capped {
        assert!(
            n <= cap,
            "partition {} returned {} rows, exceeds PER PARTITION LIMIT {}",
            partition,
            n,
            cap
        );
    }
    // Partitions that had > cap rows must be capped to exactly `cap`.
    let exactly_capped = capped.values().filter(|&&n| n == cap).count();
    assert!(
        exactly_capped >= partitions_over_cap,
        "expected at least {} partitions capped to exactly {}, got {} (counts {:?})",
        partitions_over_cap,
        cap,
        exactly_capped,
        capped
    );
}

/// Combined `PER PARTITION LIMIT k LIMIT m`: the global LIMIT applies after the
/// per-partition cap, so the total is min(m, sum of capped partitions).
#[tokio::test]
async fn test_per_partition_limit_with_global_limit() {
    if !data_files_present() {
        eprintln!("Skipping: no Data.db files (run fetch-datasets.sh)");
        return;
    }
    let db = match setup_timeseries_db().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: setup failed: {}", e);
            return;
        }
    };

    let cap = 2usize;
    let global = 5usize;
    let counts = counts_by_partition(
        &db,
        &format!(
            "SELECT * FROM {} PER PARTITION LIMIT {} LIMIT {}",
            QUALIFIED_TABLE, cap, global
        ),
    )
    .await;

    let total: usize = counts.values().sum();
    assert_eq!(
        total, global,
        "PER PARTITION LIMIT {} LIMIT {} should yield exactly {} rows, got {} ({:?})",
        cap, global, global, total, counts
    );
    for (partition, &n) in &counts {
        assert!(
            n <= cap,
            "partition {} returned {} rows under PER PARTITION LIMIT {}",
            partition,
            n,
            cap
        );
    }
}
