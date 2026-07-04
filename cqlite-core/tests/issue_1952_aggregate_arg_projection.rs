//! Issue #1952 (P1, value-correctness): the SSTable scan projection must
//! include aggregate ARGUMENT source columns.
//!
//! Before the fix, `extract_projection_columns` emitted only the projected +
//! grouped DIMENSION columns. For a grouped query that also projects a group
//! dimension — `SELECT category, SUM(value) FROM t GROUP BY category` — the scan
//! projection became `["category"]`, so `value` was filtered out of every scanned
//! row before `update_aggregate` could read it. Non-star aggregates then silently
//! computed from missing inputs (SUM/AVG → 0/null, COUNT(col) → 0, MIN/MAX →
//! null). `COUNT(*)` (no argument column) was unaffected.
//!
//! These tests run through the public `Database::execute` path on a real
//! Cassandra 5.0 fixture and assert EXACT per-group aggregate values derived
//! from the committed JSONL golden.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::Value;
use cqlite_core::{Database, QueryRow};

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

async fn setup(schema_file: &str, keyspace_filter: &str) -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let schema_path = schemas_dir()
        .ok_or("schemas dir not found")?
        .join(schema_file);
    if !schema_path.exists() {
        return Err(format!("{schema_file} not found at {schema_path:?}"));
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
        table_directory_filter: Some(keyspace_filter.to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".to_string());
    }
    Ok(result.database)
}

/// Ground truth for `test_basic.multi_partition_table`, grouped by the `category`
/// clustering column over the BIGINT `value` column. Derived directly from the
/// committed JSONL golden (`nb-1-big-Data.db.jsonl`, 100 single-row partitions):
///   A: count=36 sum=18785439 min=42438  max=990685
///   B: count=36 sum=17144227 min=34344  max=941836
///   C: count=28 sum=15711813 min=73596  max=979921
const GROUPS: &[(&str, i64, i64, i64, i64)] = &[
    ("A", 36, 18_785_439, 42_438, 990_685),
    ("B", 36, 17_144_227, 34_344, 941_836),
    ("C", 28, 15_711_813, 73_596, 979_921),
];

/// Find the row whose `category` equals `cat`, then return the value keyed by
/// `agg_key` (the aggregate output name, e.g. `Sum_value`).
fn agg_value<'a>(rows: &'a [QueryRow], cat: &str, agg_key: &str) -> Option<&'a Value> {
    rows.iter().find_map(|row| {
        match row.values.get("category") {
            Some(Value::Text(t)) if t == cat => {}
            _ => return None,
        }
        row.values.get(agg_key)
    })
}

/// `SELECT category, SUM(value) ... GROUP BY category` must compute EXACT
/// per-group sums. On origin/main `value` is filtered out of the scan projection
/// so every SUM is `0.0` — this asserts the real totals.
#[tokio::test]
async fn grouped_sum_with_selected_dimension_is_exact() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT category, SUM(value) \
             FROM test_basic.multi_partition_table GROUP BY category",
        )
        .await
        .expect("grouped SUM query must execute");

    for &(cat, _count, sum, _min, _max) in GROUPS {
        let got = agg_value(&result.rows, cat, "Sum_value");
        assert_eq!(
            got,
            Some(&Value::Float(sum as f64)),
            "SUM(value) for category {cat}; scan projection must include the \
             aggregate argument column `value`",
        );
    }
}

/// `COUNT(value)` counts non-null `value` cells per group; every row has a
/// `value`, so it equals the group size. On origin/main it is `0` (column
/// filtered out).
#[tokio::test]
async fn grouped_count_column_with_selected_dimension_is_exact() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT category, COUNT(value) \
             FROM test_basic.multi_partition_table GROUP BY category",
        )
        .await
        .expect("grouped COUNT(col) query must execute");

    for &(cat, count, _sum, _min, _max) in GROUPS {
        let got = agg_value(&result.rows, cat, "Count_value");
        assert_eq!(
            got,
            Some(&Value::BigInt(count)),
            "COUNT(value) for category {cat} must equal the non-null group size",
        );
    }
}

/// `MIN(value)` / `MAX(value)` per group. On origin/main both are `null` (column
/// filtered out of the scan).
#[tokio::test]
async fn grouped_min_max_with_selected_dimension_is_exact() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT category, MIN(value), MAX(value) \
             FROM test_basic.multi_partition_table GROUP BY category",
        )
        .await
        .expect("grouped MIN/MAX query must execute");

    for &(cat, _count, _sum, min, max) in GROUPS {
        assert_eq!(
            agg_value(&result.rows, cat, "Min_value"),
            Some(&Value::BigInt(min)),
            "MIN(value) for category {cat}",
        );
        assert_eq!(
            agg_value(&result.rows, cat, "Max_value"),
            Some(&Value::BigInt(max)),
            "MAX(value) for category {cat}",
        );
    }
}

/// Regression guard: `COUNT(*)` grouped needs no argument column and must stay
/// correct after the projection change (the group size per category).
#[tokio::test]
async fn grouped_count_star_still_correct() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT category, COUNT(*) \
             FROM test_basic.multi_partition_table GROUP BY category",
        )
        .await
        .expect("grouped COUNT(*) query must execute");

    for &(cat, count, _sum, _min, _max) in GROUPS {
        let got = agg_value(&result.rows, cat, "Count(*)");
        assert_eq!(
            got,
            Some(&Value::BigInt(count)),
            "COUNT(*) for category {cat} must equal the group size",
        );
    }
}
