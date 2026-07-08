//! Issue #1750: a short literal `WHERE id = <value>` point read must return
//! fully-populated `metadata.columns`, honor projection, include the partition-key
//! column, and take the partition-targeted fast path — NOT the legacy
//! column-less path.
//!
//! ## The bug this pins
//!
//! `engine.rs` used to route any SELECT literally containing `WHERE id =` with
//! ≤ 8 whitespace tokens to the legacy `QueryExecutor` point-lookup path, which
//! builds its result via `QueryResult::with_rows` → `QueryMetadata::default()`
//! with an **empty `columns` vec**, never applies projection, and never
//! reconstructs proper metadata. Net effect: every CLI/binding output writer,
//! which keys rows off `result.metadata.columns`, rendered empty/column-less
//! output for these queries even though `row.values` was populated. This was
//! also a no-heuristics-mandate violation (#28): routing was decided by a
//! substring + token-count guess on the raw CQL text.
//!
//! These tests exercise the PUBLIC read surface only (`Database::execute`) and
//! assert, for the exact ≤ 8-token `WHERE id =` shape:
//!   1. `metadata.columns` is non-empty and names the projected columns in order
//!      (the projected form `SELECT id, name, age`),
//!   2. `SELECT *` populates `metadata.columns` from the schema (incl. the PK),
//!   3. the returned row's cells match the projection and include the PK column,
//!   4. the query takes `AccessPath::PartitionLookup` (the #949/#956 fast path),
//!      proving heuristic removal did NOT regress point reads to a full scan.
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped (not
//! failed) when the data isn't present, matching the repo's other dataset-backed
//! integration tests. A present-but-empty result is a FAILURE, never a skip.

// Epic #951 (honest access paths): the `tombstones` build compiles out the
// partition-targeted prune, so a fully-constrained `WHERE id = ?` honestly
// reports `FallbackFullScan { TombstonesBuildNoPrune }` there. Gate the
// access-path assertion `not(tombstones)`; the columns/projection assertions
// (the actual #1750 bug) hold on every build, so they live in a separate,
// unconditional test below.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryRow;
use cqlite_core::{Database, Value};

/// Serializes tests that read the process-global access-path probe. Only the
/// `not(tombstones)` access-path test consults the probe, so gate it likewise to
/// avoid a dead-code warning on the `tombstones` build.
#[cfg(not(feature = "tombstones"))]
static PROBE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const QUALIFIED_TABLE: &str = "test_basic.simple_table";

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
    let schemas = schemas_dir().ok_or("schemas dir not found")?;
    let schema_path = schemas.join("basic-types.cql");
    if !schema_path.exists() {
        return Err(format!("schema not found at {schema_path:?}"));
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

/// Open the SAME fixture with NO schema loaded (issue #1750 regression). A
/// schema-less open exercises the point-read path that CANNOT reconstruct the
/// partition-key column from the row bytes.
async fn setup_schemaless() -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }
    let config = IngestionConfig {
        schema_paths: vec![],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some("/test_basic/simple_table".to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    Ok(result.database)
}

/// Table with an `int` single-component partition key (roborev 3784 FINDING 2).
/// `test_compactionparity.live_no_clustering` is `id INT PRIMARY KEY, v TEXT` —
/// the closest real scalar-column `int`-pk fixture (a true `int` pk; the pk NAME
/// is not serialised so the header synthesises `partition_key`). Opened SCHEMA-LESS
/// so the point read must derive the pk width from the SerializationHeader `keyType`.
const INT_PK_TABLE: &str = "test_compactionparity.live_no_clustering";

async fn setup_schemaless_int_pk() -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }
    let config = IngestionConfig {
        schema_paths: vec![],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some("/test_compactionparity/live_no_clustering".to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    Ok(result.database)
}

fn uuid_to_literal(bytes: &[u8; 16]) -> String {
    let h = |range: std::ops::Range<usize>| -> String {
        bytes[range].iter().map(|b| format!("{b:02x}")).collect()
    };
    format!(
        "{}-{}-{}-{}-{}",
        h(0..4),
        h(4..6),
        h(6..8),
        h(8..10),
        h(10..16)
    )
}

fn first_uuid(rows: &[QueryRow], col: &str) -> Option<[u8; 16]> {
    match rows.first().and_then(|r| r.values.get(col)) {
        Some(Value::Uuid(id)) => Some(*id),
        _ => None,
    }
}

/// A real UUID partition key from the fixture, or `None` to skip.
async fn a_real_id(db: &Database) -> Option<[u8; 16]> {
    let probe = db
        .execute(&format!("SELECT id FROM {QUALIFIED_TABLE} LIMIT 1"))
        .await
        .ok()?;
    first_uuid(&probe.rows, "id")
}

/// The exact ≤ 8-token literal `WHERE id =` projected point read must return
/// fully-populated, correctly-ordered `metadata.columns` matching the projection,
/// and a row whose cells match (incl. the PK column). This is the #1750 bug: on
/// the old heuristic route the columns vec was EMPTY.
#[tokio::test]
async fn projected_where_id_eq_populates_metadata_columns() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let Some(id) = a_real_id(&db).await else {
        eprintln!("Skipping: simple_table returned 0 rows or id not uuid");
        return;
    };

    // Exactly 8 whitespace tokens — the old heuristic's trigger shape. The
    // comma-less projection (`id,name,age`) is one token, matching the issue's
    // exact repro `SELECT id,name,age FROM ks.t WHERE id = <k>`.
    let query = format!(
        "SELECT id,name,age FROM {QUALIFIED_TABLE} WHERE id = {}",
        uuid_to_literal(&id)
    );
    assert_eq!(
        query.split_whitespace().count(),
        8,
        "regression guard: this must be the exact ≤8-token shape the heuristic caught",
    );

    let result = db.execute(&query).await.expect("point lookup must succeed");

    assert!(
        !result.rows.is_empty(),
        "Issue #1750: the point read must return the row (present-but-empty is a failure)",
    );
    // The core bug: projected columns must be present, in projection order.
    let column_names: Vec<String> = result
        .metadata
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert_eq!(
        column_names,
        vec!["id".to_string(), "name".to_string(), "age".to_string()],
        "Issue #1750: metadata.columns must name the projected columns in order, got {column_names:?}",
    );

    // The row cells must match the projection AND include the PK column.
    let row = &result.rows[0];
    for col in ["id", "name", "age"] {
        assert!(
            row.values.contains_key(col),
            "Issue #1750: projected column '{col}' must be present in the row cells",
        );
    }
    assert_eq!(
        row.values.get("id"),
        Some(&Value::Uuid(id)),
        "Issue #1750: the PK column must be reconstructed with the looked-up value",
    );
}

/// `SELECT *` in the ≤ 8-token `WHERE id =` shape must populate `metadata.columns`
/// from the schema (all columns, incl. the PK) — not an empty vec.
#[tokio::test]
async fn select_star_where_id_eq_populates_metadata_columns() {
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let Some(id) = a_real_id(&db).await else {
        eprintln!("Skipping: simple_table returned 0 rows or id not uuid");
        return;
    };

    let query = format!(
        "SELECT * FROM {QUALIFIED_TABLE} WHERE id = {}",
        uuid_to_literal(&id)
    );
    // `SELECT * FROM ks.t WHERE id = <uuid>` is 7 tokens — inside the old ≤8 gate.
    assert!(
        query.split_whitespace().count() <= 8,
        "regression guard: SELECT * shape must be inside the old ≤8-token gate",
    );

    let result = db.execute(&query).await.expect("point lookup must succeed");

    assert!(
        !result.rows.is_empty(),
        "Issue #1750: SELECT * point read must return the row",
    );
    assert!(
        !result.metadata.columns.is_empty(),
        "Issue #1750: SELECT * must populate metadata.columns from the schema, got empty",
    );
    let names: Vec<String> = result
        .metadata
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert!(
        names.iter().any(|n| n == "id"),
        "Issue #1750: SELECT * columns must include the PK column 'id', got {names:?}",
    );
    // The row must carry the PK value.
    assert_eq!(
        result.rows[0].values.get("id"),
        Some(&Value::Uuid(id)),
        "Issue #1750: SELECT * row must include the reconstructed PK value",
    );
}

/// Heuristic removal must NOT regress the point read to a full scan: the exact
/// ≤ 8-token `WHERE id = <uuid>` must still take the partition-targeted fast
/// path (#949/#956). Gated `not(tombstones)` because that build compiles out the
/// prune and honestly reports a fallback full scan.
#[cfg(not(feature = "tombstones"))]
#[tokio::test]
async fn where_id_eq_still_takes_partition_lookup_fast_path() {
    use cqlite_core::query::access_path::{self, AccessPath};

    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let Some(id) = a_real_id(&db).await else {
        eprintln!("Skipping: simple_table returned 0 rows or id not uuid");
        return;
    };

    // Comma-less ≤8-token shape — the exact route the old heuristic diverted.
    let query = format!(
        "SELECT id,name,age FROM {QUALIFIED_TABLE} WHERE id = {}",
        uuid_to_literal(&id)
    );

    access_path::reset();
    let result = db.execute(&query).await.expect("point lookup must succeed");

    assert_eq!(
        result.metadata.access_path,
        Some(AccessPath::PartitionLookup),
        "Issue #1750: a literal WHERE id = <uuid> must still take the partition-targeted \
         fast path after heuristic removal, got {:?}",
        result.metadata.access_path,
    );
    assert_eq!(
        access_path::last(),
        Some(AccessPath::PartitionLookup),
        "Issue #1750: the access-path probe must record PartitionLookup for WHERE id = <uuid>",
    );
}

/// Issue #1750 (round-C regression): a SCHEMA-LESS `WHERE id = <uuid>` point
/// read must still return the row. With no schema the full-scan path cannot
/// reconstruct the partition-key column, so the shared per-row predicate backstop
/// would reject every row on the `id` equality and return 0 rows — the regression
/// introduced by rerouting this read off the legacy `QueryExecutor` (which looked
/// up by key bytes and never re-evaluated the predicate). The structural
/// schema-less point-lookup path restores the row WITHOUT reintroducing any text
/// heuristic. A present-but-empty result is a FAILURE.
///
/// The valid UUID literal is learned from a SCHEMA-FULL open (the schema-less scan
/// omits the pk column), then the point read is issued against a SCHEMA-LESS open.
#[tokio::test]
async fn schemaless_where_id_eq_returns_the_row() {
    // Learn a real id via a schema-full open.
    let schema_db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let Some(id) = a_real_id(&schema_db).await else {
        eprintln!("Skipping: simple_table returned 0 rows or id not uuid");
        return;
    };
    drop(schema_db);

    let db = match setup_schemaless().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let query = format!(
        "SELECT * FROM {QUALIFIED_TABLE} WHERE id = {}",
        uuid_to_literal(&id)
    );
    let result = db
        .execute(&query)
        .await
        .expect("schema-less point lookup must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "Issue #1750: a schema-less WHERE id = <uuid> point read must return exactly the one \
         matching row (regression returned 0 rows because the schema-less scan can't \
         reconstruct the pk column and the predicate backstop then rejected every row)",
    );
    // The row carries real values (never an all-empty/column-less row).
    let row = &result.rows[0];
    assert!(
        !row.values.is_empty(),
        "Issue #1750: the schema-less point-read row must surface its cell values",
    );
    assert!(
        row.values.values().any(|v| !matches!(v, Value::Null)),
        "Issue #1750: the schema-less point-read row must surface at least one non-null value",
    );
}

/// Issue #1750 (CONFIRMED regression): a SCHEMA-LESS `WHERE <non_pk_col> = <literal>`
/// must return the CORRECT matching row(s) via the honest full scan — NOT 0 rows.
///
/// The prior fix routed ANY schema-less single `col = <literal>` equality into a
/// by-partition-key seek, which treated the literal as raw pk bytes, found no such
/// partition, and returned 0 rows. On `simple_table` (`id UUID` pk, `name TEXT`
/// regular), `SELECT * WHERE name = '<real value>'` regressed 1→0. This fix confirms
/// the equality column is the sole partition key from AUTHORITATIVE metadata (the
/// Statistics.db SerializationHeader shape) BY ELIMINATION before seeking; a
/// non-pk column keeps the full-scan path and matches the regular-column cell
/// (regular cells ARE decodable schema-less). A present-but-0-rows result is a
/// FAILURE. The `name` value is learned from a schema-full open.
#[tokio::test]
async fn schemaless_where_non_pk_col_eq_returns_the_matching_row() {
    // Learn a real (id, name) pair via a schema-full open.
    let schema_db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let probe = schema_db
        .execute(&format!("SELECT id, name FROM {QUALIFIED_TABLE} LIMIT 1"))
        .await
        .expect("probe select must succeed");
    let Some(row) = probe.rows.first() else {
        eprintln!("Skipping: simple_table returned 0 rows");
        return;
    };
    let name = match row.values.get("name") {
        Some(Value::Text(s)) if !s.is_empty() => s.clone(),
        _ => {
            eprintln!("Skipping: first row has no non-empty text `name`");
            return;
        }
    };
    // How many rows carry this name in the full table (the correct answer)?
    let expected = schema_db
        .execute(&format!(
            "SELECT id FROM {QUALIFIED_TABLE} WHERE name = '{}' ALLOW FILTERING",
            name.replace('\'', "''")
        ))
        .await
        .map(|r| r.rows.len())
        .unwrap_or(0);
    drop(schema_db);

    let db = match setup_schemaless().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let query = format!(
        "SELECT * FROM {QUALIFIED_TABLE} WHERE name = '{}'",
        name.replace('\'', "''")
    );
    let result = db
        .execute(&query)
        .await
        .expect("schema-less non-pk equality must succeed");

    // The confirmed regression: this returned 0. It must return the matching row(s),
    // matching origin/main's full-scan behaviour. Present-but-0-rows is a FAILURE.
    assert!(
        !result.rows.is_empty(),
        "Issue #1750 regression: a schema-less WHERE name = <value> must return the matching \
         row(s) via full scan, not 0 rows (the by-pk-key seek over-fired on a non-pk column)",
    );
    // At least one returned row must actually carry the requested name value.
    assert!(
        result
            .rows
            .iter()
            .any(|r| matches!(r.values.get("name"), Some(Value::Text(v)) if *v == name)),
        "Issue #1750: a returned row must carry the requested `name` value",
    );
    if expected > 0 {
        assert_eq!(
            result.rows.len(),
            expected,
            "Issue #1750: the schema-less non-pk equality must return the SAME rows the \
             schema-full full scan does",
        );
    }
}

/// roborev 3784 FINDING 2 (real correctness bug): a SCHEMA-LESS `WHERE <int_pk> = <n>`
/// point read through the REAL parser must return the matching row. A SELECT integer
/// literal parses as `Value::BigInt`; before the fix it encoded to an 8-byte key while
/// the `int` partition key on disk is 4 bytes, so the seek missed and returned 0 rows
/// (silent wrong result). The fix carries the pk TYPE from the Statistics.db
/// SerializationHeader and encodes through the typed single-component codec (4 bytes
/// for `int`). This test runs an actual query STRING through `Database::execute` — NOT
/// a hand-built `Value` — so it exercises the parsed-query path the unit tests miss.
/// It FAILS on a2d72f96 (8-byte encoding → 0 rows) and passes after. Present-but-0-rows
/// is a FAILURE.
#[tokio::test]
async fn schemaless_where_int_pk_eq_returns_the_row() {
    let db = match setup_schemaless_int_pk().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Discover a real int partition-key value via a schema-less full scan of the
    // pk column. The header synthesises the pk name `partition_key`; the pk column
    // is reconstructed by the seek path but a bare scan omits it — so learn a valid
    // key by counting rows and probing candidate small ints present in the fixture.
    // The fixture (`live_no_clustering`) has partition keys 1..=4; assert `= 1`
    // returns exactly its one row, and `= 999` (absent) returns 0.
    let all = db
        .execute(&format!("SELECT * FROM {INT_PK_TABLE}"))
        .await
        .expect("schema-less full scan must succeed");
    if all.rows.is_empty() {
        eprintln!("Skipping: {INT_PK_TABLE} present but returned 0 rows");
        return;
    }

    let hit = db
        .execute(&format!(
            "SELECT * FROM {INT_PK_TABLE} WHERE partition_key = 1"
        ))
        .await
        .expect("schema-less int-pk point lookup must succeed");
    assert_eq!(
        hit.rows.len(),
        1,
        "roborev 3784 FINDING 2: a schema-less `WHERE int_pk = 1` must return exactly the one \
         matching row (before the fix the 8-byte BigInt encoding missed the 4-byte int key → 0 rows)",
    );
    // The matching row must carry its regular cell value AND the reconstructed pk.
    let row = &hit.rows[0];
    assert_eq!(
        row.values.get("partition_key"),
        Some(&Value::Integer(1)),
        "the reconstructed int pk column must equal the looked-up value",
    );
    assert!(
        row.values.contains_key("v"),
        "the matching row must surface its regular `v` cell",
    );

    // A partition key that is NOT present returns 0 rows (the seek is self-verifying).
    let miss = db
        .execute(&format!(
            "SELECT * FROM {INT_PK_TABLE} WHERE partition_key = 999"
        ))
        .await
        .expect("absent-key point lookup must succeed");
    assert_eq!(
        miss.rows.len(),
        0,
        "an absent int partition key must return 0 rows",
    );
}

/// roborev 3784 FINDING 1 (hardening): a SCHEMA-LESS `WHERE <nonexistent_col> = <n>`
/// whose literal COLLIDES with a real partition key's bytes must return 0 rows — NOT
/// the collided partition's row. The classifier admits the predicate column by
/// ELIMINATION (absent from non-key names), which also admits a misspelled/nonexistent
/// column; the post-seek guard reconstructs the pk under its authoritative name and
/// re-evaluates the predicate, so a predicate on a nonexistent column is Unknown and
/// rejects the row (correct 0 rows). On `live_no_clustering` (`partition_key` int pk),
/// `WHERE not_a_column = 1` collides with the real key `1` but must yield 0 rows.
#[tokio::test]
async fn schemaless_where_nonexistent_col_eq_returns_zero_rows() {
    let db = match setup_schemaless_int_pk().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    // Confirm the fixture is present and that `partition_key = 1` really is a hit,
    // so a 0 result for the bogus column is meaningfully the guard rejecting it.
    let baseline = db
        .execute(&format!(
            "SELECT * FROM {INT_PK_TABLE} WHERE partition_key = 1"
        ))
        .await
        .expect("baseline pk lookup must succeed");
    if baseline.rows.is_empty() {
        eprintln!("Skipping: {INT_PK_TABLE} present but `partition_key = 1` returned 0 rows");
        return;
    }

    let result = db
        .execute(&format!(
            "SELECT * FROM {INT_PK_TABLE} WHERE not_a_column = 1"
        ))
        .await
        .expect("schema-less nonexistent-column equality must succeed");
    assert_eq!(
        result.rows.len(),
        0,
        "roborev 3784 FINDING 1: a schema-less `WHERE not_a_column = 1` must return 0 rows — the \
         post-seek predicate guard rejects a nonexistent column even when its literal collides \
         with a real partition key's bytes",
    );
}
