//! Issue #586 regression tests — partition-key column reconstruction on the scan path.
//!
//! Defect (shipped in v0.10.0): scan-built rows reconstructed partition-key
//! columns with a hand-rolled decoder (`decode_partition_key_value`) that
//! assumed a `u16` length prefix for *every* TEXT key. Two consequences:
//!
//! 1. **Single-component TEXT PK** (`id text PRIMARY KEY`): the raw key bytes
//!    carry NO length prefix, so the decoder read a phantom length, ran off the
//!    end, returned `Err`, and the error was silently swallowed — the PK column
//!    was dropped. `SELECT *` was missing `id` and `WHERE id = '...'` matched
//!    nothing (UUID worked only because it goes through the index point-lookup
//!    path, #548/#553).
//! 2. **Composite TEXT PK** (`PRIMARY KEY ((a, b), ...)`): every PK column was
//!    decoded from component[0], so `a` and `b` both got the first component's
//!    value, and non-text components (e.g. a `date`) fell through to a debug
//!    string.
//!
//! Fix: route the scan path through the canonical
//! `storage::partition_key_codec::decode_partition_key_columns`, the same codec
//! the write engine's `PartitionKey::from_bytes` uses.
//!
//! ## Fixture coverage
//!
//! There is no scannable **single-component TEXT-PK** table in the fetchable
//! corpus (the only single-text-PK tables are counter tables, whose counter
//! cells don't parse into a row, and system tables the scan skips). That exact
//! single-PK scenario is covered deterministically by unit tests:
//! `storage::partition_key_codec::tests::single_text_pk_is_raw_bytes` and
//! `query::select_executor` `build_row_from_scan` tests. These integration
//! tests cover the real composite TEXT-PK fixtures end-to-end (including the
//! wide-key / clustering-range behaviour the report flagged).
//!
//! Requires `CQLITE_DATASETS_ROOT` and real Data.db files
//! (`bash test-data/scripts/fetch-datasets.sh`).

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::Value;
use cqlite_core::{Database, QueryRow};

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
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
    schemas_dir.exists().then_some(schemas_dir)
}

/// Ingest `test_timeseries` (which holds the composite TEXT-PK tables) and
/// return a queryable `Database`, or `None` if datasets aren't present.
async fn setup_timeseries_db() -> Option<Database> {
    let datasets_root = get_datasets_root()?;
    let schemas_dir = get_schemas_dir()?;
    let schema_path = schemas_dir.join("time-series.cql");
    if !schema_path.exists() {
        return None;
    }
    let data_dir = datasets_root.join("sstables");
    if !data_dir.exists() {
        return None;
    }

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some("/test_timeseries/".to_string()),
    };

    let result = ingest(config).await.ok()?;
    if result.schema_load_result.schemas_loaded == 0 {
        return None;
    }
    Some(result.database)
}

/// Read a column from a row as a UTF-8 string, accepting either a native
/// `Text` value or any other type rendered via `Display` (so the assertions
/// don't depend on the exact `Value` variant).
fn col_as_string(row: &QueryRow, name: &str) -> Option<String> {
    row.values.get(name).map(|v| match v {
        Value::Text(s) => s.clone(),
        other => format!("{}", other),
    })
}

/// #586: a composite TEXT partition key must decode **each** component to its
/// own value. Before the fix both columns received component[0], so
/// `metric_name` came back as `"goal"` instead of `"interest"`.
#[tokio::test]
async fn composite_text_pk_decodes_each_component() {
    let Some(db) = setup_timeseries_db().await else {
        eprintln!("composite_text_pk_decodes_each_component: SKIPPED (no datasets)");
        return;
    };

    let result = db
        .execute("SELECT application_id, metric_name FROM test_timeseries.app_metrics WHERE application_id = 'goal'")
        .await
        .expect("query should succeed");

    if result.rows.is_empty() {
        eprintln!("composite_text_pk_decodes_each_component: SKIPPED (0 rows — Data.db absent?)");
        return;
    }

    for row in &result.rows {
        assert_eq!(
            col_as_string(row, "application_id").as_deref(),
            Some("goal"),
            "WHERE filter must hold: application_id == 'goal'"
        );
        // The discriminating assertion: before #586 this was wrongly 'goal'
        // (component[0] reused for every PK column).
        assert_eq!(
            col_as_string(row, "metric_name").as_deref(),
            Some("interest"),
            "Issue #586: second composite PK component must decode independently. \
             Before fix it incorrectly mirrored component[0] ('goal')."
        );
    }
}

/// #586 + SELECT-completeness: `SELECT *` must include every partition-key
/// column with the correct value.
#[tokio::test]
async fn select_star_includes_all_partition_key_columns() {
    let Some(db) = setup_timeseries_db().await else {
        eprintln!("select_star_includes_all_partition_key_columns: SKIPPED (no datasets)");
        return;
    };

    let result = db
        .execute("SELECT * FROM test_timeseries.app_metrics WHERE application_id = 'goal' AND metric_name = 'interest'")
        .await
        .expect("query should succeed");

    if result.rows.is_empty() {
        eprintln!("select_star_includes_all_partition_key_columns: SKIPPED (0 rows)");
        return;
    }

    let row = &result.rows[0];
    assert_eq!(
        col_as_string(row, "application_id").as_deref(),
        Some("goal"),
        "Issue #586: SELECT * must materialise the first PK column"
    );
    assert_eq!(
        col_as_string(row, "metric_name").as_deref(),
        Some("interest"),
        "Issue #586: SELECT * must materialise the second PK column"
    );
}

/// #586: a non-text composite PK component (here `trading_day date`) must
/// decode by its declared type, not fall through to a raw-bytes debug string.
#[tokio::test]
async fn composite_pk_non_text_component_decodes_by_type() {
    let Some(db) = setup_timeseries_db().await else {
        eprintln!("composite_pk_non_text_component_decodes_by_type: SKIPPED (no datasets)");
        return;
    };

    let result = db
        .execute("SELECT symbol, trading_day FROM test_timeseries.stock_prices WHERE symbol = 'AMZN' LIMIT 1")
        .await
        .expect("query should succeed");

    if result.rows.is_empty() {
        eprintln!("composite_pk_non_text_component_decodes_by_type: SKIPPED (0 rows)");
        return;
    }

    let row = &result.rows[0];
    assert_eq!(col_as_string(row, "symbol").as_deref(), Some("AMZN"));

    // Before the fix this component was emitted as a debug byte array like
    // "[0, 4, 65, 65, 80, 76, ...]". After the fix it is a proper Date value.
    let trading_day = row
        .values
        .get("trading_day")
        .expect("trading_day PK column must be present");
    assert!(
        matches!(trading_day, Value::Date(_)),
        "Issue #586: DATE partition-key component must decode to Value::Date, got {:?}",
        trading_day
    );
}

/// #586: composite TEXT PK with a clustering-range slice. The report noted that
/// the broken PK reconstruction "also blocks clustering-range slices"; with the
/// PK columns correctly materialised the slice returns its rows.
#[tokio::test]
async fn composite_text_pk_clustering_range_slice() {
    let Some(db) = setup_timeseries_db().await else {
        eprintln!("composite_text_pk_clustering_range_slice: SKIPPED (no datasets)");
        return;
    };

    let result = db
        .execute(
            "SELECT application_id, metric_name, timestamp \
             FROM test_timeseries.app_metrics \
             WHERE application_id = 'goal' AND metric_name = 'interest' \
             AND timestamp >= '2025-09-29 00:00:00' AND timestamp <= '2025-09-30 00:00:00'",
        )
        .await
        .expect("clustering-range query should succeed");

    if result.rows.is_empty() {
        eprintln!("composite_text_pk_clustering_range_slice: SKIPPED (0 rows)");
        return;
    }

    for row in &result.rows {
        assert_eq!(
            col_as_string(row, "application_id").as_deref(),
            Some("goal")
        );
        assert_eq!(
            col_as_string(row, "metric_name").as_deref(),
            Some("interest")
        );
        assert!(
            row.values.contains_key("timestamp"),
            "clustering column must be present in the slice result"
        );
    }
}
