//! Integration test for Issue #660: BTI (da) end-to-end read support.
//!
//! VG5 (#657) delivered the `da` foundation (recognition + reader-open), and
//! #831/#755 wired the partition-key point lookup via the `Partitions.db` trie.
//! This test covers the remaining read deliverable: a full `SELECT *` over a
//! real `da`/BTI SSTable must return every live partition with correct values.
//!
//! **Problem (pre-#660)**: BTI tables have no `Index.db`/`Summary.db`, so the
//! index-based scan path found no entries and the reader returned 0 rows for a
//! `SELECT *` — the data was unreachable except by exact-key point lookup.
//!
//! **Fix**: `SSTableReader::scan` / `scan_with_cell_metadata` / `get_all_entries`
//! route BTI tables through `bti_scan_with_metadata`, which decompresses the
//! whole `Data.db` section and decodes every partition via the same
//! V5CompressedLegacy partition parser the point-lookup path proves correct.
//!
//! **Requirements**:
//! - CQLITE_DATASETS_ROOT pointing to test-data/datasets
//! - test_da dataset (da/BTI fixtures from #654): simple_table, collection_table, ttl_table
//! - da-test.cql schema file
//!
//! Verifies against the `da-2-bti-Data.db.jsonl` goldens checked into the repo.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

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

/// Ingest the `da-test.cql` schema against the full sstables directory.
/// Returns Err(reason) when the test should be skipped (missing data/schema).
async fn setup_test_database() -> Result<Database, String> {
    let datasets_root = get_datasets_root()
        .ok_or_else(|| "CQLITE_DATASETS_ROOT not set or path doesn't exist".to_string())?;
    let schemas_dir = get_schemas_dir().ok_or_else(|| "schemas directory not found".to_string())?;

    let schema_path = schemas_dir.join("da-test.cql");
    if !schema_path.exists() {
        return Err(format!("Schema not found at {:?}", schema_path));
    }

    let data_dir = datasets_root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables directory not found at {:?}", data_dir));
    }

    let ingestion_config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: None,
    };

    let ingestion_result = ingest(ingestion_config)
        .await
        .map_err(|e| format!("ingestion failed: {}", e))?;

    if ingestion_result.schema_load_result.schemas_loaded == 0 {
        return Err("No schemas loaded during ingestion".to_string());
    }

    Ok(ingestion_result.database)
}

/// A `SELECT *` over the da/BTI `simple_table` must return all three partitions
/// with the exact values from the JSONL golden (Alice / Bob / Carol).
#[tokio::test]
async fn bti_simple_table_select_star_returns_all_rows() {
    let db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    let result = db
        .execute("SELECT * FROM test_da.simple_table")
        .await
        .expect("SELECT * on da/BTI simple_table must succeed");

    assert_eq!(
        result.rows.len(),
        3,
        "Issue #660: da/BTI simple_table has 3 partitions in the golden"
    );

    // Index rows by the UUID partition key so the assertions are order-independent
    // (scan returns token order, not insertion order).
    let by_name: std::collections::HashMap<String, &_> = result
        .rows
        .iter()
        .filter_map(|row| row.values.get("name").map(|v| (format!("{:?}", v), row)))
        .collect();

    // The three names must all be present (proves every partition decoded).
    let names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|r| r.values.get("name").map(|v| format!("{:?}", v)))
        .collect();
    let joined = names.join(",");
    assert!(joined.contains("Alice Smith"), "missing Alice: {}", joined);
    assert!(joined.contains("Bob Johnson"), "missing Bob: {}", joined);
    assert!(
        joined.contains("Carol Williams"),
        "missing Carol: {}",
        joined
    );
    assert_eq!(by_name.len(), 3, "all three rows must carry a name column");

    // Every row must carry the full column set (id + 5 regular columns).
    for row in &result.rows {
        assert!(row.values.contains_key("id"), "row missing id: {:?}", row);
        assert!(row.values.contains_key("age"), "row missing age: {:?}", row);
        assert!(
            row.values.contains_key("salary"),
            "row missing salary: {:?}",
            row
        );
        assert!(
            row.values.contains_key("active"),
            "row missing active: {:?}",
            row
        );
    }
}

/// A `SELECT *` over the da/BTI `collection_table` must return both partitions
/// with their set / list / map collection columns populated.
#[tokio::test]
async fn bti_collection_table_select_star_returns_all_rows() {
    let db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    let result = db
        .execute("SELECT * FROM test_da.collection_table")
        .await
        .expect("SELECT * on da/BTI collection_table must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "Issue #660: da/BTI collection_table has 2 partitions in the golden"
    );

    for row in &result.rows {
        assert!(
            row.values.contains_key("tags"),
            "row missing tags: {:?}",
            row
        );
        assert!(
            row.values.contains_key("scores"),
            "row missing scores: {:?}",
            row
        );
        assert!(
            row.values.contains_key("properties"),
            "row missing properties: {:?}",
            row
        );
    }
}

/// A `SELECT *` over the da/BTI `ttl_table` returns ZERO rows because every row's
/// TTL (`ttl=86400`, `expires_at = 2026-06-11T16:17:37Z`, baked into the fixture)
/// has elapsed — a Cassandra `SELECT` hides expired rows.
///
/// This ORIGINALLY asserted "returns all rows": before issue #1741 the read path
/// ignored TTL and served the expired rows as live (the P0 bug). The fix now applies
/// read-time TTL expiry on the BTI read path too, so the correct result is 0 rows.
/// The expiry is a FIXED past timestamp in the SSTable, so this is stable going
/// forward in wall-clock time (the rows only get "more expired"). The BTI read-path's
/// ability to decode live rows is covered by the sibling `simple_table` /
/// `collection_table` cases above; this case now pins TTL shadowing on that path.
#[tokio::test]
async fn bti_ttl_table_select_star_hides_expired_rows() {
    let db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    let result = db
        .execute("SELECT * FROM test_da.ttl_table")
        .await
        .expect("SELECT * on da/BTI ttl_table must succeed");

    assert_eq!(
        result.rows.len(),
        0,
        "Issue #1741: da/BTI ttl_table rows all expired (TTL 86400, expires 2026-06-11) \
         — a Cassandra SELECT hides them, but the read path returned {} live rows",
        result.rows.len()
    );
}
