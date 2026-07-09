//! Issue #2202: SUM/AVG must preserve Cassandra's integral result TYPE (not
//! collapse every numeric input to `double`), and the result metadata type must
//! match the value variant CQLite actually emits (the #1941 invariant).
//!
//! Cassandra 5.0 promotion this pins:
//!   * SUM/AVG over `int`      → `int`     (`Value::Integer`)
//!   * SUM/AVG over `bigint`   → `bigint`  (`Value::BigInt`)
//!   * SUM/AVG over `double`   → `double`  (`Value::Float`, no regression)
//!   * SUM/AVG over `float`    → `double`  (`Value::Float`, no regression)
//!
//! AVG over an integral column uses integer division (truncated toward zero).
//!
//! Exercised THROUGH the public `Database::execute` surface on real Cassandra 5.0
//! fixtures (`test_basic`): BIGINT `value` (`multi_partition_table`), INT
//! `row_value` (`static_columns_table`), and DOUBLE `weight` / FLOAT `height`
//! (`simple_table`). Requires `CQLITE_DATASETS_ROOT` + fetched Data.db binaries;
//! skips loudly (never a false 0-row pass) when a fixture is absent.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryResult;
use cqlite_core::schema::CqlType;
use cqlite_core::types::{DataType, Value};
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

async fn setup() -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let schema_path = schemas_dir()
        .ok_or("schemas dir not found")?
        .join("basic-types.cql");
    if !schema_path.exists() {
        return Err(format!("basic-types.cql not found at {schema_path:?}"));
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
        table_directory_filter: Some("/test_basic/".to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".to_string());
    }
    Ok(result.database)
}

/// The single (global-aggregate) result row plus its one aggregate column's
/// metadata `(cql_type, data_type)` and the emitted `Value`.
async fn run_scalar_aggregate(db: &Database, sql: &str) -> (Option<CqlType>, DataType, Value) {
    let result: QueryResult = db
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query `{sql}` must execute: {e}"));
    assert_eq!(
        result.rows.len(),
        1,
        "global aggregate `{sql}` yields exactly one row"
    );
    assert_eq!(
        result.metadata.columns.len(),
        1,
        "single-aggregate SELECT `{sql}` has one result column"
    );
    let col = &result.metadata.columns[0];
    let value = result.rows[0]
        .values
        .get(col.name.as_str())
        .unwrap_or_else(|| {
            panic!(
                "`{sql}`: metadata column {:?} MUST be a row value key",
                col.name
            )
        })
        .clone();
    (col.cql_type.clone(), col.data_type.clone(), value)
}

/// The #1941 invariant: the metadata `(cql_type, data_type)` must describe the
/// SAME variant as the emitted value. Assert the exact expected variant AND that
/// metadata agrees with it.
fn assert_type_matches_value(
    label: &str,
    cql_type: Option<CqlType>,
    data_type: DataType,
    value: &Value,
    expect_cql: CqlType,
    expect_data: DataType,
) {
    assert_eq!(cql_type, Some(expect_cql), "{label}: metadata cql_type");
    assert_eq!(data_type, expect_data, "{label}: metadata data_type");
    let variant_ok = matches!(
        (&data_type, value),
        (DataType::Integer, Value::Integer(_))
            | (DataType::BigInt, Value::BigInt(_))
            | (DataType::Float, Value::Float(_))
    );
    assert!(
        variant_ok,
        "{label}: metadata type {data_type:?} must match emitted value variant, got {value:?}"
    );
}

/// SUM/AVG over a BIGINT column stay `bigint` (value + metadata), and AVG uses
/// integer division. Values pinned to the committed golden reference
/// (`multi_partition_table`: sum 51_641_479, count 100 → avg 516_414).
#[tokio::test]
async fn sum_avg_bigint_column_is_bigint() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping sum_avg_bigint_column_is_bigint: {e}");
            return;
        }
    };
    let tbl = "test_basic.multi_partition_table";

    let (cql, dt, v) = run_scalar_aggregate(&db, &format!("SELECT SUM(value) FROM {tbl}")).await;
    assert_type_matches_value(
        "SUM(bigint)",
        cql,
        dt,
        &v,
        CqlType::BigInt,
        DataType::BigInt,
    );
    assert_eq!(v, Value::BigInt(51_641_479), "SUM(bigint) value");

    let (cql, dt, v) = run_scalar_aggregate(&db, &format!("SELECT AVG(value) FROM {tbl}")).await;
    assert_type_matches_value(
        "AVG(bigint)",
        cql,
        dt,
        &v,
        CqlType::BigInt,
        DataType::BigInt,
    );
    assert_eq!(
        v,
        Value::BigInt(516_414),
        "AVG(bigint) is integer division: 51_641_479 / 100 = 516_414 (truncated)"
    );
}

/// SUM/AVG over an INT column are `int` (value + metadata) — never `double`.
#[tokio::test]
async fn sum_avg_int_column_is_int() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping sum_avg_int_column_is_int: {e}");
            return;
        }
    };
    let tbl = "test_basic.static_columns_table";

    let (cql, dt, v) =
        run_scalar_aggregate(&db, &format!("SELECT SUM(row_value) FROM {tbl}")).await;
    assert_type_matches_value("SUM(int)", cql, dt, &v, CqlType::Int, DataType::Integer);

    let (cql, dt, v) =
        run_scalar_aggregate(&db, &format!("SELECT AVG(row_value) FROM {tbl}")).await;
    assert_type_matches_value("AVG(int)", cql, dt, &v, CqlType::Int, DataType::Integer);
}

/// SUM/AVG over FLOAT/DOUBLE columns still return `double` (`Value::Float`) — the
/// pre-#2202 behaviour is preserved with no regression.
#[tokio::test]
async fn sum_avg_float_double_columns_stay_double() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping sum_avg_float_double_columns_stay_double: {e}");
            return;
        }
    };
    let tbl = "test_basic.simple_table";

    for sql in [
        format!("SELECT SUM(weight) FROM {tbl}"), // DOUBLE
        format!("SELECT AVG(weight) FROM {tbl}"),
        format!("SELECT SUM(height) FROM {tbl}"), // FLOAT → double
        format!("SELECT AVG(height) FROM {tbl}"),
    ] {
        let (cql, dt, v) = run_scalar_aggregate(&db, &sql).await;
        assert_type_matches_value(&sql, cql, dt, &v, CqlType::Double, DataType::Float);
    }
}
