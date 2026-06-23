//! Issue #961 (Epic #951): positional `?` binding for parameterized and prepared
//! SELECTs, routed through the partition-targeted fast path.
//!
//! Before #961 the parameter slice handed to `execute_with_params` and
//! `PreparedQuery::execute` was validated for arity but otherwise *ignored* — the
//! `?` placeholders were never bound, so the underlying query ran with no
//! constraint and the #949/#956 partition-targeted fast path could not engage
//! through the parameterized/prepared API.
//!
//! These tests exercise the PUBLIC API only (`Database::execute_with_params`,
//! `Database::prepare` + `PreparedQuery::execute`) and assert, per partition-key
//! type (int / text / uuid / composite):
//!   1. parameterized lookup returns the SAME rows as the equivalent literal,
//!   2. prepared execution with DIFFERENT params returns DIFFERENT partitions,
//!   3. the bound `WHERE pk = ?` uses `AccessPath::PartitionLookup` (the #960
//!      signal), NOT a full scan,
//!   4. binding the WRONG value returns wrong/empty rows (proves binding actually
//!      happens — a test that fails if params are accepted but ignored), and
//!   5. arity validation stays strict (too few / too many params -> error).
//!
//! Fixtures (single-column partition key unless noted):
//!   - int  : `test_da.wide_table`            (pk int)               — BTI `da`
//!   - text : `test_basic.counters`           (id text)
//!   - uuid : `test_basic.simple_table`       (id uuid)
//!   - composite text : `test_timeseries.app_metrics`,
//!     PRIMARY KEY ((application_id, metric_name), timestamp)
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped (not
//! failed) when the data isn't present, matching the repo's other dataset-backed
//! integration tests.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::{self, AccessPath};
use cqlite_core::query::result::QueryRow;
use cqlite_core::query::{ExecutionHints, PreparedContext};
use cqlite_core::{Database, Value};
use std::collections::HashMap;

/// Serializes tests that read the process-global access-path probe
/// (`access_path::last()`), mirroring the #960 test harness.
static PROBE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

/// Open a database over the full sstables tree with every schema this test needs
/// loaded (basic-types, time-series, and the BTI wide_table schema).
async fn setup() -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let schemas = schemas_dir().ok_or("schemas dir not found")?;
    let schema_paths: Vec<PathBuf> = ["basic-types.cql", "time-series.cql", "wide-table-bti.cql"]
        .iter()
        .map(|f| schemas.join(f))
        .collect();
    for p in &schema_paths {
        if !p.exists() {
            return Err(format!("schema not found at {p:?}"));
        }
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }

    let config = IngestionConfig {
        schema_paths,
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: None,
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".to_string());
    }
    Ok(result.database)
}

/// Canonical, order-independent fingerprint of a row's columns.
fn row_fingerprint(row: &QueryRow) -> BTreeMap<String, String> {
    row.values
        .iter()
        .map(|(k, v)| (k.clone(), format!("{v:?}")))
        .collect()
}

fn fingerprints(rows: &[QueryRow]) -> Vec<BTreeMap<String, String>> {
    let mut out: Vec<_> = rows.iter().map(row_fingerprint).collect();
    out.sort_by_key(|m| format!("{m:?}"));
    out
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

/// Read a column value from the first row, if any.
fn first_value(rows: &[QueryRow], col: &str) -> Option<Value> {
    rows.first().and_then(|r| r.values.get(col).cloned())
}

// ===========================================================================
// 1. Parameterized SELECT == literal SELECT, per partition-key type.
// ===========================================================================

#[tokio::test]
async fn param_int_pk_matches_literal() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    // wide_table has int partition keys pk = 1, 2, 3.
    let probe = db
        .execute("SELECT pk, ck, payload FROM test_da.wide_table LIMIT 1")
        .await
        .expect("scan must succeed");
    if probe.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows");
        return;
    }
    let Some(Value::Integer(pk_i32)) = first_value(&probe.rows, "pk") else {
        // pk may decode as a wider integer variant; fall back to any int-like.
        match first_value(&probe.rows, "pk") {
            Some(other) => {
                eprintln!("Skipping: pk decoded as unexpected variant {other:?}");
                return;
            }
            None => panic!("pk column missing"),
        }
    };

    let literal = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM test_da.wide_table WHERE pk = {pk_i32}"
        ))
        .await
        .expect("literal lookup must succeed");
    let param = db
        .execute_with_params(
            "SELECT pk, ck, payload FROM test_da.wide_table WHERE pk = ?",
            &[Value::Integer(pk_i32)],
        )
        .await
        .expect("parameterized lookup must succeed");

    assert!(!literal.rows.is_empty(), "literal lookup must return rows");
    assert_eq!(
        fingerprints(&param.rows),
        fingerprints(&literal.rows),
        "Issue #961: parameterized int-pk lookup must equal the literal lookup",
    );
}

#[tokio::test]
async fn param_text_pk_matches_literal() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let probe = db
        .execute("SELECT id FROM test_basic.counters LIMIT 1")
        .await
        .expect("scan must succeed");
    let Some(Value::Text(id)) = first_value(&probe.rows, "id") else {
        eprintln!("Skipping: counters returned 0 rows or id not text");
        return;
    };

    let literal = db
        .execute(&format!(
            "SELECT id, view_count FROM test_basic.counters WHERE id = '{id}'"
        ))
        .await
        .expect("literal lookup must succeed");
    let param = db
        .execute_with_params(
            "SELECT id, view_count FROM test_basic.counters WHERE id = ?",
            &[Value::Text(id.clone())],
        )
        .await
        .expect("parameterized lookup must succeed");

    assert!(!literal.rows.is_empty(), "literal lookup must return rows");
    assert_eq!(
        fingerprints(&param.rows),
        fingerprints(&literal.rows),
        "Issue #961: parameterized text-pk lookup must equal the literal lookup",
    );
}

#[tokio::test]
async fn param_uuid_pk_matches_literal() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let probe = db
        .execute("SELECT id, name, age FROM test_basic.simple_table LIMIT 1")
        .await
        .expect("scan must succeed");
    let Some(Value::Uuid(id)) = first_value(&probe.rows, "id") else {
        eprintln!("Skipping: simple_table returned 0 rows or id not uuid");
        return;
    };
    let literal = db
        .execute(&format!(
            "SELECT id, name, age FROM test_basic.simple_table WHERE id = {}",
            uuid_to_literal(&id)
        ))
        .await
        .expect("literal lookup must succeed");
    let param = db
        .execute_with_params(
            "SELECT id, name, age FROM test_basic.simple_table WHERE id = ?",
            &[Value::Uuid(id)],
        )
        .await
        .expect("parameterized lookup must succeed");

    assert!(!literal.rows.is_empty(), "literal lookup must return rows");
    assert_eq!(
        fingerprints(&param.rows),
        fingerprints(&literal.rows),
        "Issue #961: parameterized uuid-pk lookup must equal the literal lookup",
    );
}

#[tokio::test]
async fn param_composite_pk_matches_literal() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    // app_metrics: PRIMARY KEY ((application_id, metric_name), timestamp).
    let probe = db
        .execute("SELECT application_id, metric_name FROM test_timeseries.app_metrics LIMIT 1")
        .await
        .expect("scan must succeed");
    let (Some(Value::Text(app)), Some(Value::Text(metric))) = (
        first_value(&probe.rows, "application_id"),
        first_value(&probe.rows, "metric_name"),
    ) else {
        eprintln!("Skipping: app_metrics returned 0 rows or composite key not text");
        return;
    };

    let literal = db
        .execute(&format!(
            "SELECT application_id, metric_name, value FROM test_timeseries.app_metrics \
             WHERE application_id = '{app}' AND metric_name = '{metric}'"
        ))
        .await
        .expect("literal lookup must succeed");
    let param = db
        .execute_with_params(
            "SELECT application_id, metric_name, value FROM test_timeseries.app_metrics \
             WHERE application_id = ? AND metric_name = ?",
            &[Value::Text(app.clone()), Value::Text(metric.clone())],
        )
        .await
        .expect("parameterized composite lookup must succeed");

    assert!(!literal.rows.is_empty(), "literal lookup must return rows");
    assert_eq!(
        fingerprints(&param.rows),
        fingerprints(&literal.rows),
        "Issue #961: parameterized composite-pk lookup must equal the literal lookup",
    );
}

// ===========================================================================
// 2. The bound `WHERE pk = ?` reports AccessPath::PartitionLookup (#960),
//    proving the targeted fast path engages through the parameterized API.
// ===========================================================================

#[tokio::test]
async fn param_where_pk_eq_reports_partition_lookup() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let probe = db
        .execute("SELECT id FROM test_basic.simple_table LIMIT 1")
        .await
        .expect("scan must succeed");
    let Some(Value::Uuid(id)) = first_value(&probe.rows, "id") else {
        eprintln!("Skipping: simple_table returned 0 rows or id not uuid");
        return;
    };

    access_path::reset();
    let result = db
        .execute_with_params(
            "SELECT id, name FROM test_basic.simple_table WHERE id = ?",
            &[Value::Uuid(id)],
        )
        .await
        .expect("parameterized targeted lookup must succeed");

    assert_eq!(
        result.metadata.access_path,
        Some(AccessPath::PartitionLookup),
        "Issue #961: a bound WHERE pk = ? must take the partition-targeted path, got {:?}",
        result.metadata.access_path,
    );
    assert_eq!(
        access_path::last(),
        Some(AccessPath::PartitionLookup),
        "Issue #961: the access-path probe must record PartitionLookup for a bound WHERE pk = ?",
    );
    assert!(
        !result
            .metadata
            .access_path
            .as_ref()
            .expect("access path present")
            .is_full_scan(),
        "a bound partition lookup must NOT be classified as a full scan",
    );
}

// ===========================================================================
// 3. Prepared execution: DIFFERENT params -> DIFFERENT partitions; and the
//    prepared path also reaches PartitionLookup. (uuid pk).
// ===========================================================================

#[tokio::test]
async fn prepared_different_params_return_different_partitions() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Learn two distinct UUID partition keys from a full scan.
    let full = db
        .execute("SELECT id, name, age FROM test_basic.simple_table")
        .await
        .expect("full scan must succeed");
    if full.rows.len() < 2 {
        eprintln!("Skipping: need >= 2 rows in simple_table");
        return;
    }
    let mut ids: Vec<[u8; 16]> = Vec::new();
    for row in &full.rows {
        if let Some(Value::Uuid(b)) = row.values.get("id") {
            if !ids.contains(b) {
                ids.push(*b);
            }
        }
        if ids.len() == 2 {
            break;
        }
    }
    if ids.len() < 2 {
        eprintln!("Skipping: need >= 2 distinct UUID partitions");
        return;
    }

    let prepared = db
        .prepare("SELECT id, name, age FROM test_basic.simple_table WHERE id = ?")
        .await
        .expect("prepare must succeed");

    access_path::reset();
    let first = prepared
        .execute(&[Value::Uuid(ids[0])])
        .await
        .expect("prepared exec #1 must succeed");
    // Prepared SELECT must reach the partition-targeted path (#961 unification).
    assert_eq!(
        access_path::last(),
        Some(AccessPath::PartitionLookup),
        "Issue #961: prepared WHERE pk = ? must take the partition-targeted path",
    );

    let second = prepared
        .execute(&[Value::Uuid(ids[1])])
        .await
        .expect("prepared exec #2 must succeed");

    // Each result must contain ONLY its requested partition, and the two must differ.
    assert!(
        first
            .rows
            .iter()
            .all(|r| matches!(r.values.get("id"), Some(Value::Uuid(b)) if *b == ids[0])),
        "first prepared result must contain only partition ids[0]",
    );
    assert!(
        second
            .rows
            .iter()
            .all(|r| matches!(r.values.get("id"), Some(Value::Uuid(b)) if *b == ids[1])),
        "second prepared result must contain only partition ids[1]",
    );
    assert_ne!(
        fingerprints(&first.rows),
        fingerprints(&second.rows),
        "Issue #961: re-executing a prepared query with DIFFERENT params must return DIFFERENT \
         partitions (proves params are bound, not ignored)",
    );
}

// ===========================================================================
// 4. Binding the WRONG value returns the WRONG/empty rows. This test FAILS if
//    params are accepted but ignored (the pre-#961 bug).
// ===========================================================================

#[tokio::test]
async fn binding_wrong_value_returns_no_matching_rows() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let probe = db
        .execute("SELECT id FROM test_basic.simple_table LIMIT 1")
        .await
        .expect("scan must succeed");
    let Some(Value::Uuid(present)) = first_value(&probe.rows, "id") else {
        eprintln!("Skipping: simple_table returned 0 rows or id not uuid");
        return;
    };

    // Bind a UUID that is overwhelmingly unlikely to exist.
    let absent = [0xffu8; 16];
    assert_ne!(present, absent, "sanity: present != absent key");

    let result = db
        .execute_with_params(
            "SELECT id FROM test_basic.simple_table WHERE id = ?",
            &[Value::Uuid(absent)],
        )
        .await
        .expect("absent-key lookup must succeed");

    // If params were ignored, this would full-scan and return ALL rows. With
    // binding, an absent key returns nothing.
    assert!(
        result.rows.is_empty(),
        "Issue #961: binding an absent partition key must return no rows (got {}); a non-empty \
         result means the parameter was IGNORED and the query degenerated to a full scan",
        result.rows.len(),
    );
}

// ===========================================================================
// 5. Strict arity validation (negative tests).
// ===========================================================================

#[tokio::test]
async fn param_count_mismatch_is_rejected() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // One `?` placeholder, zero params supplied. `execute_with_params` always
    // parses + binds SELECTs, so the arity check fires even with an empty slice.
    let too_few = db
        .execute_with_params("SELECT id FROM test_basic.simple_table WHERE id = ?", &[])
        .await;
    assert!(
        too_few.is_err(),
        "Issue #961: a `?` query with no parameters must be rejected, not silently full-scanned",
    );

    // One `?` placeholder, two params supplied -> arity error.
    let too_many = db
        .execute_with_params(
            "SELECT id FROM test_basic.simple_table WHERE id = ?",
            &[Value::Integer(1), Value::Integer(2)],
        )
        .await;
    assert!(
        too_many.is_err(),
        "Issue #961: supplying more parameters than `?` placeholders must error",
    );

    // Prepared arity: prepared query has one `?`, execute with zero params.
    let prepared = db
        .prepare("SELECT id FROM test_basic.simple_table WHERE id = ?")
        .await
        .expect("prepare must succeed");
    let prep_too_few = prepared.execute(&[]).await;
    assert!(
        prep_too_few.is_err(),
        "Issue #961: prepared execute with too few params must error",
    );
}

// ===========================================================================
// 6. Finding 1: a zero-marker `execute_with_params(sql, &[])` routes EXACTLY
//    like a literal `execute(sql)` — same rows AND same access path — including
//    the simple `WHERE id = <literal>` case that `execute` keeps on the legacy /
//    simple-id point-lookup path. This proves the two APIs cannot diverge for
//    markerless statements.
// ===========================================================================

#[tokio::test]
async fn zero_marker_params_route_like_literal_execute_scan() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let sql = "SELECT id, name, age FROM test_basic.simple_table";

    access_path::reset();
    let via_execute = db.execute(sql).await.expect("execute scan must succeed");
    let execute_path = access_path::last();

    access_path::reset();
    let via_params = db
        .execute_with_params(sql, &[])
        .await
        .expect("zero-marker execute_with_params must succeed");
    let params_path = access_path::last();

    assert_eq!(
        fingerprints(&via_params.rows),
        fingerprints(&via_execute.rows),
        "Finding 1: zero-marker execute_with_params must return the SAME rows as execute()",
    );
    assert_eq!(
        params_path, execute_path,
        "Finding 1: zero-marker execute_with_params must take the SAME access path as execute()",
    );
    assert_eq!(
        via_params.metadata.access_path, via_execute.metadata.access_path,
        "Finding 1: reported access_path must match between the two APIs",
    );
}

#[tokio::test]
async fn zero_marker_params_route_like_literal_execute_simple_id_lookup() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let probe = db
        .execute("SELECT id FROM test_basic.simple_table LIMIT 1")
        .await
        .expect("scan must succeed");
    let Some(Value::Uuid(id)) = first_value(&probe.rows, "id") else {
        eprintln!("Skipping: simple_table returned 0 rows or id not uuid");
        return;
    };

    // `SELECT * FROM <table> WHERE id = <uuid>` is <= 8 whitespace tokens, which
    // is exactly the simple-id point lookup that `execute` keeps on the legacy
    // executor for INSERT/SELECT key compatibility. The zero-marker
    // `execute_with_params` MUST delegate to `execute` and hit the same path.
    let sql = format!(
        "SELECT * FROM test_basic.simple_table WHERE id = {}",
        uuid_to_literal(&id)
    );
    assert!(
        sql.split_whitespace().count() <= 8,
        "test setup: query must hit the simple-id legacy path (<= 8 tokens)",
    );

    access_path::reset();
    let via_execute = db
        .execute(&sql)
        .await
        .expect("literal execute must succeed");
    let execute_path = access_path::last();

    access_path::reset();
    let via_params = db
        .execute_with_params(&sql, &[])
        .await
        .expect("zero-marker execute_with_params must succeed");
    let params_path = access_path::last();

    assert_eq!(
        fingerprints(&via_params.rows),
        fingerprints(&via_execute.rows),
        "Finding 1: zero-marker simple-id execute_with_params must return the SAME rows as execute()",
    );
    assert_eq!(
        params_path, execute_path,
        "Finding 1: zero-marker simple-id execute_with_params must take the SAME (legacy) access \
         path as execute(); divergence here means execute_with_params(&[]) bypassed the \
         simple-id routing",
    );
    assert_eq!(
        via_params.metadata.access_path, via_execute.metadata.access_path,
        "Finding 1: reported access_path must match between the two APIs for the simple-id case",
    );
}

#[tokio::test]
async fn zero_marker_query_with_stray_param_is_rejected() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // No `?` placeholder, but a parameter is supplied. This is a caller bug and
    // must be rejected (strict arity), not silently delegated.
    let result = db
        .execute_with_params(
            "SELECT id FROM test_basic.simple_table",
            &[Value::Integer(1)],
        )
        .await;
    assert!(
        result.is_err(),
        "Finding 1: supplying a parameter for a query with 0 bind markers must error",
    );
}

// ===========================================================================
// 7. Finding 2: a prepared SELECT executed through the CONTEXT API
//    (`execute_with_context` with `positional_params`) binds correctly:
//    different params -> different partitions, and `WHERE pk = ?` reports
//    AccessPath::PartitionLookup (not a full scan, not an unbound legacy plan).
//    Supplying legacy hints with a prepared SELECT returns a clear error.
// ===========================================================================

#[tokio::test]
async fn prepared_context_binds_params_and_reaches_partition_lookup() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let full = db
        .execute("SELECT id, name, age FROM test_basic.simple_table")
        .await
        .expect("full scan must succeed");
    let mut ids: Vec<[u8; 16]> = Vec::new();
    for row in &full.rows {
        if let Some(Value::Uuid(b)) = row.values.get("id") {
            if !ids.contains(b) {
                ids.push(*b);
            }
        }
        if ids.len() == 2 {
            break;
        }
    }
    if ids.len() < 2 {
        eprintln!("Skipping: need >= 2 distinct UUID partitions");
        return;
    }

    let prepared = db
        .prepare("SELECT id, name, age FROM test_basic.simple_table WHERE id = ?")
        .await
        .expect("prepare must succeed");

    let ctx0 = PreparedContext {
        parameters: HashMap::new(),
        positional_params: vec![Value::Uuid(ids[0])],
        hints: ExecutionHints::default(),
    };
    let ctx1 = PreparedContext {
        parameters: HashMap::new(),
        positional_params: vec![Value::Uuid(ids[1])],
        hints: ExecutionHints::default(),
    };

    access_path::reset();
    let first = prepared
        .execute_with_context(&ctx0)
        .await
        .expect("prepared context exec #1 must succeed");
    assert_eq!(
        access_path::last(),
        Some(AccessPath::PartitionLookup),
        "Finding 2: prepared WHERE pk = ? via context must take the partition-targeted path \
         (not the unbound legacy plan / full scan)",
    );

    let second = prepared
        .execute_with_context(&ctx1)
        .await
        .expect("prepared context exec #2 must succeed");

    assert!(
        first
            .rows
            .iter()
            .all(|r| matches!(r.values.get("id"), Some(Value::Uuid(b)) if *b == ids[0])),
        "first context result must contain only partition ids[0]",
    );
    assert!(
        second
            .rows
            .iter()
            .all(|r| matches!(r.values.get("id"), Some(Value::Uuid(b)) if *b == ids[1])),
        "second context result must contain only partition ids[1]",
    );
    assert_ne!(
        fingerprints(&first.rows),
        fingerprints(&second.rows),
        "Finding 2: different positional_params through the context API must return DIFFERENT \
         partitions (proves the params are bound, not dropped)",
    );
}

#[tokio::test]
async fn prepared_context_rejects_hints_for_select() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let probe = db
        .execute("SELECT id FROM test_basic.simple_table LIMIT 1")
        .await
        .expect("scan must succeed");
    let Some(Value::Uuid(id)) = first_value(&probe.rows, "id") else {
        eprintln!("Skipping: simple_table returned 0 rows or id not uuid");
        return;
    };

    let prepared = db
        .prepare("SELECT id, name FROM test_basic.simple_table WHERE id = ?")
        .await
        .expect("prepare must succeed");

    // A legacy hint cannot be mapped onto the SELECT pipeline: rather than
    // silently ignore it (and risk running the unbound legacy plan), the context
    // API returns a clear error for a prepared SELECT carrying any hint.
    let ctx = PreparedContext {
        parameters: HashMap::new(),
        positional_params: vec![Value::Uuid(id)],
        hints: ExecutionHints {
            timeout_ms: Some(5000),
            ..ExecutionHints::default()
        },
    };
    let result = prepared.execute_with_context(&ctx).await;
    assert!(
        result.is_err(),
        "Finding 2: supplying a legacy ExecutionHint to a prepared SELECT via context must error",
    );
}

#[tokio::test]
async fn non_select_with_params_is_rejected() {
    let _guard = PROBE_LOCK.lock().await;
    let db = match setup().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };
    let result = db
        .execute_with_params(
            "INSERT INTO test_basic.simple_table (id) VALUES (?)",
            &[Value::Integer(1)],
        )
        .await;
    assert!(
        result.is_err(),
        "Issue #961: parameterized execution of a non-SELECT must return a clear error",
    );
}
