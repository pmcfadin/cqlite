//! Issue #1587 (E5) fix round: public-surface parity for the bare-column
//! `SELECT` projection-elision optimization.
//!
//! The optimizer now skips the redundant `Project` step for bare-column SELECTs
//! (every item a plain `Column`), projecting each row once in the SSTable scan.
//! The old `Project` path rebuilt rows with an EMPTY `RowKey` and *errored*
//! ("Column not found") when a projected column was NULL/absent in a row. The
//! scan path preserves the storage key and simply omits absent columns.
//!
//! These tests pin the resulting public behavior against real Cassandra 5.0
//! fixtures through the public `Database::execute` path (default gate feature
//! set: `cli-helpers`):
//!
//!   1. A bare-column `SELECT a, b` returns the SAME `_key` per row as
//!      `SELECT *` (bare-column projection is now consistent with the canonical
//!      scan path), and the projected column VALUES match `SELECT *` exactly.
//!   2. Selecting a schema column that is NULL/absent in a given row returns
//!      null (not an error) — the intended, Cassandra-aligned behavior.

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

/// Ingest one keyspace (`schema_file` scoped to `keyspace_filter`) into a
/// queryable database, or return a skip reason.
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

/// The `_key` string rendered by `QueryResult::to_json` (`format!("{:?}", key)`).
fn key_str(row: &serde_json::Value) -> String {
    row.get("_key")
        .and_then(|k| k.as_str())
        .unwrap_or("<missing>")
        .to_string()
}

/// Build a map from the partition-key column value (`id`, a UUID string) to the
/// row's JSON object, so rows can be correlated across two queries independent
/// of return order.
fn index_by_id(result: &serde_json::Value) -> std::collections::HashMap<String, serde_json::Value> {
    let mut map = std::collections::HashMap::new();
    if let Some(rows) = result.get("rows").and_then(|r| r.as_array()) {
        for row in rows {
            if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                map.insert(id.to_string(), row.clone());
            }
        }
    }
    map
}

/// STEP 1a: a bare-column projection now emits the SAME `_key` per row as
/// `SELECT *`, and identical VALUES for the projected columns. The only
/// observable difference from `SELECT *` is the projected column subset.
#[tokio::test]
async fn bare_column_select_key_and_values_match_select_star() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let star = db
        .execute("SELECT * FROM test_basic.simple_table LIMIT 50")
        .await
        .expect("SELECT * must succeed")
        .to_json();
    let bare = db
        .execute("SELECT id, name, age FROM test_basic.simple_table LIMIT 50")
        .await
        .expect("bare-column SELECT must succeed")
        .to_json();

    let star_by_id = index_by_id(&star);
    let bare_by_id = index_by_id(&bare);

    assert!(
        !star_by_id.is_empty() && !bare_by_id.is_empty(),
        "both queries must return correlatable rows (id present); \
         star={} bare={}",
        star_by_id.len(),
        bare_by_id.len()
    );
    assert_eq!(
        star_by_id.len(),
        bare_by_id.len(),
        "both queries must cover the same set of partitions"
    );

    for (id, bare_row) in &bare_by_id {
        let star_row = star_by_id
            .get(id)
            .unwrap_or_else(|| panic!("id {id} present in bare but not in SELECT *"));

        // (1) _key must be IDENTICAL — the elided bare-column path preserves the
        // storage key, matching the canonical SELECT * scan path (the old
        // Project path emitted an EMPTY key here).
        assert_eq!(
            key_str(bare_row),
            key_str(star_row),
            "issue #1587: bare-column _key must equal SELECT * _key for id {id}"
        );

        // (2) For every projected column, the VALUE must be byte-identical to
        // SELECT *. The only difference is the projected subset.
        for col in ["id", "name", "age"] {
            assert_eq!(
                bare_row.get(col),
                star_row.get(col),
                "issue #1587: projected column `{col}` must equal SELECT * for id {id}"
            );
        }

        // The bare projection must NOT leak non-projected columns.
        if let Some(obj) = bare_row.as_object() {
            for k in obj.keys() {
                assert!(
                    matches!(k.as_str(), "id" | "name" | "age" | "_key" | "_metadata"),
                    "bare projection leaked unexpected field `{k}`"
                );
            }
        }
    }
}

/// STEP 1b: selecting a schema column that is NULL/absent in a row returns null
/// (not an error). The old `Project` path errored with "Column not found" on
/// such rows; the elided scan path omits the absent column and `to_json`
/// renders it as null.
///
/// `many_columns_table` (test_wide_rows) declares `col_001..col_100` but the
/// fixture only writes a handful (`col_001`, `col_011`, ...). `col_002` is a
/// declared schema column that is NULL in EVERY row — a deterministic null that
/// the old bare-column error path could not handle.
#[tokio::test]
async fn select_column_null_in_row_returns_null_not_error() {
    let db = match setup("wide-rows.cql", "/test_wide_rows/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute("SELECT id, col_001, col_002 FROM test_wide_rows.many_columns_table LIMIT 25")
        .await
        // The KEY assertion: this must NOT error. The old Project path returned
        // Err("Column not found: col_002") for rows where the column is absent.
        .expect("issue #1587: selecting a column that is null in a row must NOT error");

    let json = result.to_json();
    let rows = json
        .get("rows")
        .and_then(|r| r.as_array())
        .expect("rows array");
    assert!(!rows.is_empty(), "fixture must return rows");

    // `col_002` is declared but never written → it must render as JSON null in
    // EVERY row (proving null-not-error), while a populated column is non-null.
    for row in rows {
        assert_eq!(
            row.get("col_002"),
            Some(&serde_json::Value::Null),
            "issue #1587: never-written schema column col_002 must be null, got: {}",
            serde_json::to_string(row).unwrap_or_default()
        );
    }
    assert!(
        rows.iter()
            .any(|row| !matches!(row.get("col_001"), Some(serde_json::Value::Null) | None)),
        "populated column col_001 must be non-null in at least one row"
    );
}
