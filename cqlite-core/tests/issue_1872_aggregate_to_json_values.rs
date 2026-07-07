//! Issue #1872: `to_json` must render aggregate output columns under a stable
//! name carrying the real VALUE — not null.
//!
//! Root cause (pre-existing on main before the #1763 fix): `get_result_columns`
//! named an unaliased aggregate `col_N`, while `finalize_group` keyed the row
//! value map by the DERIVED alias (e.g. `Sum_value`, `Count(*)`). `to_json`
//! (`QueryResult::row_to_json_deterministic`) looks each field up by
//! `metadata.columns[i].name`, so the lookup missed and the aggregate rendered as
//! `"col_N": null` even though the value WAS present under the derived-alias key.
//! The fix routes BOTH the metadata name (`result_column_name`) and the row value
//! key (`finalize_group` via the aggregation plan alias) through ONE
//! `select_naming::aggregate_output_name` source, so they can never disagree.
//!
//! The #1763 suite already pins the invariant + `to_json` parity for `COUNT(*)`.
//! This suite extends it to a NON-COUNT aggregate (`SUM`) and asserts the JSON
//! carries the correct numeric VALUE under a stable non-`col_N` name, plus the
//! `metadata.columns` keys == row value keys invariant restated at the JSON edge.
//!
//! Ground truth (`test_basic.multi_partition_table`, BIGINT `value`, from the
//! committed JSONL golden): global SUM(value) = 51_641_479, COUNT(*) = 100.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

/// Extract the `name` field of every entry in `to_json()`'s serialized
/// `"columns"` metadata array — i.e. what `metadata.columns` actually
/// serializes to, NOT the row payload.
///
/// This is the guard against a false-positive hole (roborev finding on this
/// PR): `row_to_json_deterministic` falls back to serializing the row's raw
/// value map (sorted keys) whenever `metadata.columns` is EMPTY, so asserting
/// only on `to_json()["rows"]` can pass even if the metadata/naming contract
/// regressed to empty metadata — the fallback path would still produce
/// `Sum_value`/`total_value` fields from the row map, masking the regression.
/// Asserting the serialized `"columns"` array explicitly proves the name came
/// from the metadata path, not the fallback.
fn json_column_names(json: &serde_json::Value) -> Vec<String> {
    json.get("columns")
        .and_then(|c| c.as_array())
        .unwrap_or_else(|| panic!("to_json must emit a `columns` array; json was {json}"))
        .iter()
        .map(|c| {
            c.get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_else(|| panic!("each columns[] entry must have a `name`; got {c}"))
                .to_string()
        })
        .collect()
}

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

/// `execute()` + `to_json()`: a non-COUNT aggregate (`SUM(value)`) renders under
/// its derived name carrying the correct numeric value — never `col_N: null`.
#[tokio::test]
async fn sum_aggregate_to_json_carries_value_under_stable_name() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute("SELECT SUM(value) FROM test_basic.multi_partition_table")
        .await
        .expect("SUM(value) must execute");

    // Invariant restated at the JSON edge: every metadata column name is a row key.
    let row = result.rows.first().expect("SUM query yields one row");
    for col in &result.metadata.columns {
        assert!(
            !col.name.starts_with("col_"),
            "issue #1872: aggregate metadata name must not be synthetic col_N; got {:?}",
            col.name
        );
        assert!(
            row.values.contains_key(col.name.as_str()),
            "issue #1872: metadata column {:?} MUST be a row value key; keys were {:?}",
            col.name,
            row.values.keys().collect::<Vec<_>>()
        );
    }

    let json = result.to_json();

    // Guard against the false-positive hole: assert the serialized `columns`
    // metadata section itself carries the aggregate name — proving the JSON
    // field name below comes from the metadata path, not the row-map fallback
    // that `row_to_json_deterministic` uses when `metadata.columns` is empty.
    let meta_names = json_column_names(&json);
    assert_eq!(
        meta_names,
        vec!["Sum_value".to_string()],
        "issue #1872: to_json's serialized `columns` metadata must name the \
         aggregate `Sum_value` (not empty/col_N); got {meta_names:?}"
    );

    let first_row = json
        .get("rows")
        .and_then(|r| r.as_array())
        .and_then(|a| a.first())
        .expect("to_json must emit one row");

    // The derived name for an unaliased SUM over `value`.
    let value = first_row.get("Sum_value").unwrap_or_else(|| {
        panic!("issue #1872: to_json must key SUM by `Sum_value`; row was {first_row}")
    });
    // BIGINT summed as f64 → JSON number equal to the reference sum.
    let got = value
        .as_f64()
        .unwrap_or_else(|| panic!("issue #1872: `Sum_value` must be numeric, not {value}"));
    assert!(
        (got - 51_641_479.0).abs() < 1e-3,
        "issue #1872: to_json `Sum_value` must carry the real SUM (51_641_479), got {got}"
    );
    assert!(
        first_row.get("col_0").is_none(),
        "issue #1872: to_json must not contain a synthetic col_0 field; row was {first_row}"
    );
}

/// Aliased non-COUNT aggregate: `SUM(value) AS total_value` renders under the
/// alias in `to_json`, still carrying the value.
#[tokio::test]
async fn aliased_sum_aggregate_to_json_uses_alias() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute("SELECT SUM(value) AS total_value FROM test_basic.multi_partition_table")
        .await
        .expect("aliased SUM must execute");

    let json = result.to_json();

    // Guard against the false-positive hole: the serialized `columns` metadata
    // section must name the aggregate `total_value` (the alias), not fall back
    // to an empty/synthetic name that the row-map fallback would mask.
    let meta_names = json_column_names(&json);
    assert_eq!(
        meta_names,
        vec!["total_value".to_string()],
        "issue #1872: to_json's serialized `columns` metadata must name the \
         aliased aggregate `total_value`; got {meta_names:?}"
    );

    let first_row = json
        .get("rows")
        .and_then(|r| r.as_array())
        .and_then(|a| a.first())
        .expect("to_json must emit one row");

    let value = first_row.get("total_value").unwrap_or_else(|| {
        panic!(
            "issue #1872: to_json must key the aliased SUM by `total_value`; row was {first_row}"
        )
    });
    let got = value
        .as_f64()
        .unwrap_or_else(|| panic!("issue #1872: `total_value` must be numeric, not {value}"));
    assert!(
        (got - 51_641_479.0).abs() < 1e-3,
        "issue #1872: aliased SUM to_json must carry the real SUM, got {got}"
    );
}

/// GROUP BY through `to_json`: each group's per-group SUM renders under the
/// aggregate name AND the grouped dimension under its own name — no null field,
/// no synthetic col_N (the full row is JSON-visible, not just the value map).
#[tokio::test]
async fn grouped_sum_to_json_all_fields_named_and_valued() {
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
        .expect("grouped SUM must execute");

    // (category, per-group sum) — from the golden.
    let expected = [
        ("A", 18_785_439.0),
        ("B", 17_144_227.0),
        ("C", 15_711_813.0),
    ];

    let json = result.to_json();

    // Guard against the false-positive hole: the serialized `columns` metadata
    // section must name BOTH the grouped dimension and the aggregate, in
    // SELECT order, proving the JSON field names below come from the metadata
    // path rather than the row-map fallback that would mask empty metadata.
    let meta_names = json_column_names(&json);
    assert_eq!(
        meta_names,
        vec!["category".to_string(), "Sum_value".to_string()],
        "issue #1872: to_json's serialized `columns` metadata must name the \
         grouped dimension and aggregate; got {meta_names:?}"
    );

    let rows = json
        .get("rows")
        .and_then(|r| r.as_array())
        .expect("to_json rows array");
    assert_eq!(rows.len(), expected.len(), "one JSON row per category");

    for (cat, sum) in expected {
        let obj = rows
            .iter()
            .find(|r| r.get("category").and_then(|c| c.as_str()) == Some(cat))
            .unwrap_or_else(|| panic!("issue #1872: missing JSON row for category {cat}"));
        let got = obj
            .get("Sum_value")
            .and_then(|v| v.as_f64())
            .unwrap_or_else(|| panic!("issue #1872: category {cat} missing numeric `Sum_value`"));
        assert!(
            (got - sum).abs() < 1e-3,
            "issue #1872: category {cat} to_json SUM must be {sum}, got {got}"
        );
        assert!(
            obj.get("col_0").is_none() && obj.get("col_1").is_none(),
            "issue #1872: no synthetic col_N field in {obj}"
        );
    }
}
