//! Issue #1871: aggregate-ARGUMENT columns must be included in the SSTable scan
//! projection, or `SUM`/`AVG`/`MIN`/`MAX` read a filtered-away column and compute
//! from missing inputs (0/null).
//!
//! Root cause (pre-existing on main before the #1952 fix,
//! `select_optimizer::extract_projection_columns` /
//! `select_naming::aggregate_arg_source_columns`): a non-star aggregate's
//! argument column (`value` in `SUM(value)`) was NOT added to
//! `SSTableScan.projection`, so `build_row_from_scan` filtered the cell out and
//! `update_aggregate` saw a missing column — `SUM`/`AVG` → 0/null, `MIN`/`MAX` →
//! null, `COUNT(col)` → 0. `COUNT(*)` (no argument column) was unaffected.
//!
//! These tests exercise the fix THROUGH the public `Database::execute` surface on
//! a real Cassandra 5.0 fixture and validate the aggregate VALUES against a
//! hand-computed reference derived from the committed JSONL golden (parity is
//! truth — not merely "non-null"), both GROUPED and UNGROUPED.
//!
//! Ground truth — `test_basic.multi_partition_table`, BIGINT `value` per
//! `category` clustering value (100 single-row partitions), computed from
//! `nb-1-big-Data.db.jsonl`:
//!   category A: count 36, sum 18_785_439, min 42_438, max 990_685, avg 521_817.75
//!   category B: count 36, sum 17_144_227, min 34_344, max 941_836, avg 476_228.527_777…
//!   category C: count 28, sum 15_711_813, min 73_596, max 979_921, avg 561_136.178_571…
//!   ALL (global): count 100, sum 51_641_479, min 34_344, max 990_685, avg 516_414.79

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryResult;
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

/// Numeric view of an aggregate result `Value`, normalizing the BIGINT-typed
/// `MIN`/`MAX`/`SUM`/`AVG` (issue #2202: SUM/AVG over a BIGINT column stay
/// `bigint`, using integer division for AVG) into one comparable type.
fn as_num(v: &Value) -> f64 {
    v.as_f64()
        .unwrap_or_else(|| panic!("aggregate result must be numeric, got {v:?}"))
}

/// Read one aggregate result `Value` from a row by its derived output key (the
/// SAME key `finalize_group` emits, e.g. `Sum_value`). Fails loudly on a missing
/// or null value — the whole point of #1871 is that these must NOT be null.
fn agg(result: &QueryResult, row_idx: usize, key: &str) -> f64 {
    let row = result.rows.get(row_idx).unwrap_or_else(|| {
        panic!(
            "expected result row {row_idx}; got {} rows",
            result.rows.len()
        )
    });
    let value = row.values.get(key).unwrap_or_else(|| {
        panic!(
            "issue #1871: aggregate `{key}` MUST be a row value key; keys were {:?}",
            row.values.keys().collect::<Vec<_>>()
        )
    });
    assert!(
        !value.is_null(),
        "issue #1871: aggregate `{key}` read null — its argument column was dropped \
         from the scan projection; row keys were {:?}",
        row.values.keys().collect::<Vec<_>>()
    );
    as_num(value)
}

const EPS: f64 = 1e-3;

/// UNGROUPED (global) `SUM`/`AVG`/`MIN`/`MAX`/`COUNT(col)` over the BIGINT `value`
/// column must equal the hand-computed reference, proving the aggregate argument
/// column reaches the scan even with no GROUP BY.
#[tokio::test]
async fn ungrouped_numeric_aggregates_match_reference() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT SUM(value), AVG(value), MIN(value), MAX(value), COUNT(value) \
             FROM test_basic.multi_partition_table",
        )
        .await
        .expect("ungrouped numeric-aggregate query must execute");

    assert_eq!(
        result.rows.len(),
        1,
        "a global (no GROUP BY) aggregate yields exactly one row"
    );

    assert!(
        (agg(&result, 0, "Sum_value") - 51_641_479.0).abs() < EPS,
        "global SUM(value)"
    );
    // Issue #2202: AVG over a BIGINT column uses integer division (truncated):
    // 51_641_479 / 100 = 516_414 (not 516_414.79).
    assert!(
        (agg(&result, 0, "Avg_value") - 516_414.0).abs() < EPS,
        "global AVG(value)"
    );
    assert!(
        (agg(&result, 0, "Min_value") - 34_344.0).abs() < EPS,
        "global MIN(value)"
    );
    assert!(
        (agg(&result, 0, "Max_value") - 990_685.0).abs() < EPS,
        "global MAX(value)"
    );
    assert!(
        (agg(&result, 0, "Count_value") - 100.0).abs() < EPS,
        "global COUNT(value)"
    );
}

/// GROUPED `SUM`/`AVG`/`MIN`/`MAX` over `value`, grouped by `category`, must equal
/// the per-group hand-computed reference for EVERY group — the exact scenario in
/// the #1871 report (`SELECT category, SUM(value) ... GROUP BY category`).
#[tokio::test]
async fn grouped_numeric_aggregates_match_reference_per_group() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT category, SUM(value), AVG(value), MIN(value), MAX(value) \
             FROM test_basic.multi_partition_table GROUP BY category",
        )
        .await
        .expect("grouped numeric-aggregate query must execute");

    // (category, sum, avg, min, max). Issue #2202: value is BIGINT, so SUM/AVG
    // stay bigint and AVG is integer division (truncated toward zero):
    //   A: 18_785_439 / 36 = 521_817   B: 17_144_227 / 36 = 476_228
    //   C: 15_711_813 / 28 = 561_136
    let expected: [(&str, f64, f64, f64, f64); 3] = [
        ("A", 18_785_439.0, 521_817.0, 42_438.0, 990_685.0),
        ("B", 17_144_227.0, 476_228.0, 34_344.0, 941_836.0),
        ("C", 15_711_813.0, 561_136.0, 73_596.0, 979_921.0),
    ];

    assert_eq!(
        result.rows.len(),
        expected.len(),
        "GROUP BY category must yield exactly one row per distinct category"
    );

    for (cat, sum, avg, min, max) in expected {
        let row_idx = result
            .rows
            .iter()
            .position(|r| matches!(r.values.get("category"), Some(Value::Text(t)) if t == cat))
            .unwrap_or_else(|| panic!("issue #1871: missing group row for category {cat}"));

        assert!(
            (agg(&result, row_idx, "Sum_value") - sum).abs() < EPS,
            "SUM(value) for category {cat}"
        );
        assert!(
            (agg(&result, row_idx, "Avg_value") - avg).abs() < EPS,
            "AVG(value) for category {cat}"
        );
        assert!(
            (agg(&result, row_idx, "Min_value") - min).abs() < EPS,
            "MIN(value) for category {cat}"
        );
        assert!(
            (agg(&result, row_idx, "Max_value") - max).abs() < EPS,
            "MAX(value) for category {cat}"
        );
    }
}
