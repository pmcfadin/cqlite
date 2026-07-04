//! Issue #1763: aggregate result METADATA column names must equal the emitted
//! ROW value key.
//!
//! Before the fix, an aggregate query's `metadata.columns[i].name` used a
//! synthetic `col_0` fallback while the row value map keyed the same column by
//! the derived aggregate alias (e.g. `Count(*)`) — so `metadata.columns`
//! disagreed with the row value keys for aggregates. This regressed BOTH the
//! `execute()` and `executeNative()` binding paths because the divergence lives
//! in cqlite-core result-metadata construction (surfaced during #1446).
//!
//! These tests assert the invariant at the public `Database::execute` surface:
//! for an aggregate column, `metadata.columns[i].name == row.values` key.
//! - WITHOUT an explicit alias → both equal the expression text (never `col_0`).
//! - WITH an explicit alias (`... AS total`) → both equal `total`.

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

/// Assert that every metadata column name is present as a row value key, and
/// return the single aggregate column name (the non-empty metadata name).
fn assert_metadata_matches_row_keys(db_result: &cqlite_core::query::result::QueryResult) -> String {
    let row = db_result
        .rows
        .first()
        .expect("aggregate query must return exactly one result row");
    assert_eq!(
        db_result.metadata.columns.len(),
        1,
        "single-aggregate query has exactly one result column; got {:?}",
        db_result
            .metadata
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
    );
    let meta_name = db_result.metadata.columns[0].name.clone();
    assert!(
        !meta_name.starts_with("col_"),
        "issue #1763: aggregate metadata column must NOT be a synthetic `col_N` \
         fallback; got {meta_name:?}"
    );
    assert!(
        row.values.contains_key(meta_name.as_str()),
        "issue #1763: metadata column name {meta_name:?} MUST be a row value key; \
         row keys were {:?}",
        row.values.keys().collect::<Vec<_>>()
    );
    meta_name
}

/// WITHOUT an explicit alias: metadata name == row value key == expression text
/// (the derived aggregate name), never `col_0`.
#[tokio::test]
async fn aggregate_without_alias_metadata_equals_row_key() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute("SELECT COUNT(*) FROM test_basic.multi_partition_table")
        .await
        .expect("unaliased COUNT(*) must execute");

    let name = assert_metadata_matches_row_keys(&result);
    // The derived expression text for COUNT(*) — must be identical for metadata
    // AND the row value key (single source), and must NOT be `col_0`.
    assert_eq!(
        name, "Count(*)",
        "unaliased aggregate name is the expression text, shared by metadata and row key"
    );
}

/// WITH an explicit alias: metadata name == row value key == the alias.
#[tokio::test]
async fn aggregate_with_alias_metadata_equals_row_key() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute("SELECT COUNT(*) AS total FROM test_basic.multi_partition_table")
        .await
        .expect("aliased COUNT(*) AS total must execute");

    let name = assert_metadata_matches_row_keys(&result);
    assert_eq!(
        name, "total",
        "aliased aggregate name is the explicit alias, shared by metadata and row key"
    );
}

/// Multi-aggregate: `metadata.columns` names AND order must match the emitted row
/// value keys, in SELECT-clause order (NOT the alphabetical order an unordered
/// row-value map would otherwise expose). `SUM(value), COUNT(*)` picks names
/// whose SELECT order (`Sum_value`, `Count(*)`) differs from their alphabetical
/// order (`Count(*)`, `Sum_value`), so the assertion pins the contract.
#[tokio::test]
async fn multi_aggregate_metadata_names_and_order_match_row_keys() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute("SELECT SUM(value), COUNT(*) FROM test_basic.multi_partition_table")
        .await
        .expect("multi-aggregate SELECT must execute");

    let row = result
        .rows
        .first()
        .expect("aggregate query must return exactly one result row");

    let meta_names: Vec<String> = result
        .metadata
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();

    // Contract: SELECT-clause order, derived expression text, never `col_N`.
    assert_eq!(
        meta_names,
        vec!["Sum_value".to_string(), "Count(*)".to_string()],
        "issue #1763: multi-aggregate metadata columns must be the derived names in \
         SELECT order (not alphabetical, not synthetic col_N)"
    );

    // Every metadata column name must be an actual row value key (name parity).
    for name in &meta_names {
        assert!(
            row.values.contains_key(name.as_str()),
            "issue #1763: metadata column {name:?} must be a row value key; row keys \
             were {:?}",
            row.values.keys().collect::<Vec<_>>()
        );
    }
}

/// CLI-surface parity: `QueryResult::to_json` (the exact renderer behind the CLI
/// `--out json` path) keys each row field by `metadata.columns[i].name` and looks
/// the value up in `row.values`. Before the fix, an aggregate's metadata name
/// (`col_0`) did not match its row value key (`Count(*)`), so the CLI JSON emitted
/// `"col_0": null` — silently dropping the aggregate result. This asserts the JSON
/// carries the aggregate value under its real name (aliased and unaliased),
/// covering the CLI leg of the 3-way parity. (Python/Node bindings consume the
/// same cqlite-core metadata + row values and were already correct after #1446;
/// the core name-parity guard above is what protects those surfaces.)
#[tokio::test]
async fn aggregate_to_json_uses_aggregate_name_not_null() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    for (query, name) in [
        (
            "SELECT COUNT(*) FROM test_basic.multi_partition_table",
            "Count(*)",
        ),
        (
            "SELECT COUNT(*) AS total FROM test_basic.multi_partition_table",
            "total",
        ),
    ] {
        let result = db
            .execute(query)
            .await
            .expect("aggregate query must execute");
        let json = result.to_json();
        let first_row = json
            .get("rows")
            .and_then(|r| r.as_array())
            .and_then(|a| a.first())
            .expect("to_json must emit at least one row");
        let value = first_row.get(name).unwrap_or_else(|| {
            panic!("issue #1763: CLI JSON must key the aggregate by {name:?}; row was {first_row}")
        });
        assert!(
            value.is_number() && value.as_i64() == Some(100),
            "issue #1763: CLI JSON field {name:?} must carry the COUNT value (100 rows), \
             not null/synthetic; got {value}"
        );
        // The synthetic fallback must never appear as a JSON field.
        assert!(
            first_row.get("col_0").is_none(),
            "issue #1763: CLI JSON must not contain a synthetic `col_0` field; row was {first_row}"
        );
    }
}
