//! Issue #1587 (E5) fix round: public-surface wiring evidence for the
//! hash-indexed GROUP BY group lookup (optimization #5).
//!
//! The unit tests (`find_or_init_group_*`) call the `pub(super)` helper directly,
//! and the only end-to-end GROUP BY tests live behind `#[cfg(feature =
//! "experimental")]` (NOT run by the gate's `cli-helpers` core-tests). This test
//! exercises GROUP BY THROUGH the public `Database::execute` path on a real
//! Cassandra 5.0 fixture, under the default gate feature set (`cli-helpers`),
//! asserting correct aggregate results across multiple groups. That establishes
//! that the accelerated group lookup is wired into the public query path.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::Value;
use cqlite_core::Database;

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

/// GROUP BY through the public query path produces correct per-group aggregates
/// across MULTIPLE groups.
///
/// `test_basic.multi_partition_table` has a `category` clustering column with
/// three distinct values across 100 single-row partitions:
///   A → 36 rows, B → 36 rows, C → 28 rows.
/// (Ground truth derived from the committed JSONL golden.)
///
/// The aggregate is `COUNT(*)`, which needs no projected column and so exercises
/// grouping + per-group accumulation independently of a PRE-EXISTING scan-
/// projection bug (aggregate-argument columns like `SUM(value)`'s `value` are
/// excluded from `SSTableScan.projection` on origin/main, so `SUM` reads null →
/// 0; out of scope for #1587, reported separately). `COUNT(*)` is unaffected and
/// is exact per group, so it is a faithful multi-group aggregate result.
#[tokio::test]
async fn group_by_via_execute_aggregates_multiple_groups() {
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
        .expect("GROUP BY query must execute through the public path");

    // Read the per-group COUNT from the public `QueryRow.values` map. (`to_json`
    // mislabels aggregate output columns — a PRE-EXISTING bug where
    // `get_result_columns` names an unaliased aggregate `col_N` while the value
    // map keys it by the derived alias; out of scope for #1587. Reading the value
    // map directly proves grouping is wired into the public query path.)
    let count_of = |cat: &str| -> Option<i64> {
        result.rows.iter().find_map(|row| {
            match row.values.get("category") {
                Some(Value::Text(t)) if t == cat => {}
                _ => return None,
            }
            // COUNT(*) is the sole BigInt aggregate in the group row.
            row.values.iter().find_map(|(k, v)| match v {
                Value::BigInt(n) if k.as_ref() != "category" => Some(*n),
                _ => None,
            })
        })
    };

    let mut got: HashMap<String, i64> = HashMap::new();
    for cat in ["A", "B", "C"] {
        if let Some(count) = count_of(cat) {
            got.insert(cat.to_string(), count);
        }
    }

    let expected: HashMap<&str, i64> = HashMap::from([("A", 36), ("B", 36), ("C", 28)]);

    assert_eq!(
        got.len(),
        expected.len(),
        "GROUP BY must produce exactly one row per distinct category; got {got:?}"
    );
    for (cat, count) in expected {
        assert_eq!(
            got.get(cat),
            Some(&count),
            "COUNT(*) for category {cat} across the grouped scan; got {got:?}"
        );
    }
}
