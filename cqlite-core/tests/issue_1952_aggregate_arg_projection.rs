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
use cqlite_core::query::result::StreamingConfig;
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
            Some(&Value::BigInt(sum)),
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

/// #1952 REGRESSION (roborev HIGH): `SELECT SUM(value) ... GROUP BY category`
/// with `category` NOT in the SELECT clause. #1952 derived the scan projection
/// from the SELECT clause, so the projection became `["value"]` and OMITTED the
/// GROUP BY column `category`. `build_group_key` then read `category` as `Null`,
/// collapsing ALL groups into one and returning a single wrong sum. The GROUP BY
/// columns must ALWAYS be scanned. Asserts multiple distinct groups with EXACT
/// per-group sums.
#[tokio::test]
async fn grouped_sum_with_unselected_dimension_is_per_group() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT SUM(value) \
             FROM test_basic.multi_partition_table GROUP BY category",
        )
        .await
        .expect("grouped SUM (unselected dimension) query must execute");

    // Multiple distinct groups must be returned, not one collapsed group.
    assert_eq!(
        result.rows.len(),
        GROUPS.len(),
        "GROUP BY category must return one row per group ({} groups), not a \
         single collapsed group; scan projection must include the GROUP BY \
         column `category`",
        GROUPS.len(),
    );

    for &(cat, _count, sum, _min, _max) in GROUPS {
        let got = agg_value(&result.rows, cat, "Sum_value");
        assert_eq!(
            got,
            Some(&Value::BigInt(sum)),
            "SUM(value) for category {cat}; GROUP BY column `category` must be \
             scanned even though it is not in the SELECT clause",
        );
    }
}

/// #1952 REGRESSION companion: `SELECT COUNT(value) ... GROUP BY category` with
/// `category` unselected must produce correct PER-GROUP counts, not one collapsed
/// group.
#[tokio::test]
async fn grouped_count_column_with_unselected_dimension_is_per_group() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT COUNT(value) \
             FROM test_basic.multi_partition_table GROUP BY category",
        )
        .await
        .expect("grouped COUNT(col) (unselected dimension) query must execute");

    assert_eq!(
        result.rows.len(),
        GROUPS.len(),
        "GROUP BY category must return one row per group, not a collapsed group",
    );

    for &(cat, count, _sum, _min, _max) in GROUPS {
        let got = agg_value(&result.rows, cat, "Count_value");
        assert_eq!(
            got,
            Some(&Value::BigInt(count)),
            "COUNT(value) for category {cat} must equal the non-null group size \
             even though `category` is not in the SELECT clause",
        );
    }
}

/// #1952 REGRESSION (roborev HIGH, second round): a non-empty scan projection
/// must ALSO include WHERE predicate columns. `SELECT SUM(value) ... WHERE
/// category = 'A'` has projection `["value"]` (the aggregate argument); before
/// this fix `category` was filtered out of every scanned row, so the per-row
/// backstop (`evaluate_predicates`) saw a missing column (SQL UNKNOWN) and
/// REJECTED every row — an empty result / zero-or-null SUM. `category` is neither
/// selected nor an aggregate argument, so only unioning WHERE columns fixes it.
/// Asserts the exact single-group total from the committed JSONL golden.
#[tokio::test]
async fn single_sum_filtered_by_unselected_where_column_is_exact() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT SUM(value) \
             FROM test_basic.multi_partition_table WHERE category = 'A'",
        )
        .await
        .expect("filtered SUM query must execute");

    // Ground truth: category A sum = 18_785_439 (from GROUPS / the golden).
    assert_eq!(
        result.rows.len(),
        1,
        "an ungrouped SUM returns exactly one row; got {}",
        result.rows.len()
    );
    let sum = result.rows[0].values.get("Sum_value");
    assert_eq!(
        sum,
        Some(&Value::BigInt(18_785_439)),
        "SUM(value) WHERE category = 'A' must equal the category-A total; the WHERE \
         column `category` must be in the scan projection so the predicate backstop \
         can evaluate it (pre-fix: `category` filtered out → every row rejected → \
         empty / zero SUM)",
    );
}

/// #1952 REGRESSION companion: a compound WHERE that filters on BOTH a
/// non-projected dimension (`category`, dropped pre-fix) AND the aggregate
/// argument column (`value`, "the predicate on the aggregate arg column too")
/// must compute the exact filtered sum. Pre-fix `category` was filtered out so
/// the backstop rejected every row (empty / zero SUM).
#[tokio::test]
async fn single_sum_filtered_by_unselected_and_agg_arg_columns_is_exact() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT SUM(value) \
             FROM test_basic.multi_partition_table \
             WHERE category = 'A' AND value > 500000",
        )
        .await
        .expect("compound-filtered SUM query must execute");

    // Ground truth: category A with value > 500000 → count 17, sum 12_520_796.
    assert_eq!(result.rows.len(), 1, "an ungrouped SUM returns one row");
    assert_eq!(
        result.rows[0].values.get("Sum_value"),
        Some(&Value::BigInt(12_520_796)),
        "SUM(value) WHERE category = 'A' AND value > 500000 must equal the exact \
         filtered subtotal; `category` (WHERE-only) must be scanned alongside \
         `value` (the aggregate argument)",
    );
}

/// #1952 correctness: `SELECT category, SUM(value) ... WHERE value > n GROUP BY
/// category` — a predicate on the aggregate-argument column combined with GROUP
/// BY must compute correct PER-GROUP sums over only the filtered rows. This
/// exercises the combined WHERE + GROUP BY + aggregate-argument projection path.
#[tokio::test]
async fn grouped_sum_filtered_by_agg_arg_column_is_per_group() {
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
             FROM test_basic.multi_partition_table \
             WHERE value > 500000 GROUP BY category",
        )
        .await
        .expect("grouped filtered SUM query must execute");

    // Ground truth (value > 500000, from the golden):
    //   A: sum 12_520_796   B: sum 11_760_014   C: sum 12_302_991
    let filtered: &[(&str, i64)] = &[("A", 12_520_796), ("B", 11_760_014), ("C", 12_302_991)];
    assert_eq!(
        result.rows.len(),
        filtered.len(),
        "one row per group; got {}",
        result.rows.len()
    );
    for &(cat, sum) in filtered {
        assert_eq!(
            agg_value(&result.rows, cat, "Sum_value"),
            Some(&Value::BigInt(sum)),
            "SUM(value) WHERE value > 500000 for category {cat} must be the exact \
             per-group filtered subtotal",
        );
    }
}

/// Collect all rows from a streaming query into a Vec.
async fn stream_all(db: &Database, sql: &str) -> Vec<QueryRow> {
    let mut iter = db
        .execute_streaming(sql, StreamingConfig::default())
        .await
        .expect("streaming query must start");
    let mut rows = Vec::new();
    while let Some(item) = iter.next_async().await {
        rows.push(item.expect("streamed row must decode"));
    }
    rows
}

/// #1952 STREAMING follow-up (roborev HIGH): the broadened scan projection now
/// includes UNSELECTED WHERE helper columns, relying on the `Project` step to
/// trim them. The streaming producer ignores a plain-column `Project`, so before
/// the `requires_materialization` fix, `SELECT value ... WHERE category = 'A'`
/// scanned `[value, category]` and STREAMED both — leaking the WHERE helper
/// column `category` into every streamed row. After the fix the plan routes
/// through the materialized-then-stream path and each streamed row carries ONLY
/// the selected column `value`.
#[tokio::test]
async fn streaming_trims_unselected_where_helper_column() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let rows = stream_all(
        &db,
        "SELECT value FROM test_basic.multi_partition_table WHERE category = 'A'",
    )
    .await;

    // Ground truth: category A has 36 rows.
    assert_eq!(
        rows.len(),
        36,
        "streaming SELECT value WHERE category='A' must yield the 36 category-A rows"
    );
    for row in &rows {
        let keys: Vec<&str> = row.values.keys().map(|k| k.as_ref()).collect();
        assert_eq!(
            keys,
            vec!["value"],
            "streamed row must contain ONLY the selected column `value`; the WHERE \
             helper column `category` must be trimmed, not leaked (pre-fix the \
             streaming path ignored the Project step and leaked `category`)"
        );
        assert!(
            !row.values.contains_key("category"),
            "the unselected WHERE helper column `category` must NOT appear in a \
             streamed row"
        );
    }
}

/// #1952 STREAMING follow-up: a query with BOTH an ORDER BY helper and a WHERE
/// helper (neither selected) must stream only the selected column. `SELECT value
/// ... WHERE category = 'A' ORDER BY item_id` scans `[value, category, item_id]`
/// and must trim `category` and `item_id` from every streamed row.
#[tokio::test]
async fn streaming_trims_unselected_where_and_order_by_helpers() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let rows = stream_all(
        &db,
        "SELECT value FROM test_basic.multi_partition_table \
         WHERE category = 'A' ORDER BY item_id",
    )
    .await;

    assert_eq!(rows.len(), 36, "category A has 36 rows");
    for row in &rows {
        let keys: Vec<&str> = row.values.keys().map(|k| k.as_ref()).collect();
        assert_eq!(
            keys,
            vec!["value"],
            "streamed row must contain ONLY `value`; WHERE helper `category` and \
             ORDER BY helper `item_id` must be trimmed"
        );
    }
}

/// #1952 STREAMING regression guard: when the selected columns EXACTLY equal the
/// scan projection (no helper columns to trim), the query must still stream
/// directly and return correct rows — the fix must not force materialization for
/// the common case. `SELECT value ... WHERE value > 500000` scans exactly
/// `[value]` (the WHERE column is already selected), so no `Project`-trim is
/// needed.
#[tokio::test]
async fn streaming_exact_match_projection_still_streams_correctly() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let rows = stream_all(
        &db,
        "SELECT value FROM test_basic.multi_partition_table WHERE value > 500000",
    )
    .await;

    assert!(
        !rows.is_empty(),
        "there are rows with value > 500000; streaming must return them"
    );
    for row in &rows {
        let keys: Vec<&str> = row.values.keys().map(|k| k.as_ref()).collect();
        assert_eq!(
            keys,
            vec!["value"],
            "each streamed row carries only `value`"
        );
        match row.values.get("value") {
            Some(Value::BigInt(v)) => assert!(
                *v > 500_000,
                "streamed value {v} must satisfy the WHERE predicate value > 500000"
            ),
            other => panic!("expected BigInt value, got {other:?}"),
        }
    }
}

/// Parse a canonical UUID string (`xxxxxxxx-xxxx-...`) into its 16 raw bytes,
/// matching `Value::Uuid([u8; 16])`.
fn uuid_bytes(s: &str) -> [u8; 16] {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .unwrap_or_else(|_| panic!("bad uuid hex in {s}"));
    }
    out
}

/// #1952 (round-6 HIGH): a NON-aggregate, plain-column SELECT whose WHERE
/// references an UNSELECTED column must PRESERVE each returned row's real
/// partition/clustering `RowKey`. Pre-fix the kept `Project` step routed these
/// bare-column selects through `SelectExecutor::execute_projection`, which
/// rebuilt every row with an EMPTY `RowKey` (`vec![]`) — a #1587-class regression
/// destroying the key downstream consumers (per-partition-limit boundary
/// detection, dedup, ordering) rely on. This FAILS pre-fix (empty key).
#[tokio::test]
async fn nonaggregate_select_preserves_row_key() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT tenant_id, name \
             FROM test_basic.multi_partition_table WHERE category = 'A'",
        )
        .await
        .expect("non-aggregate filtered SELECT must execute");

    assert_eq!(
        result.rows.len(),
        36,
        "category A has 36 rows; got {}",
        result.rows.len()
    );
    for row in &result.rows {
        assert!(
            !row.key.0.is_empty(),
            "each returned row must carry its real (non-empty) partition key; \
             pre-fix `execute_projection` destroyed it to vec![] (#1587-class \
             regression)"
        );
    }
}

/// #1952 (round-6) audit guard: the UNSELECTED WHERE helper column (`category`)
/// must NOT leak into a plain-column SELECT's output rows after the trim.
#[tokio::test]
async fn nonaggregate_select_does_not_leak_where_helper() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT tenant_id, name \
             FROM test_basic.multi_partition_table WHERE category = 'A'",
        )
        .await
        .expect("non-aggregate filtered SELECT must execute");

    assert_eq!(result.rows.len(), 36, "category A has 36 rows");
    for row in &result.rows {
        assert!(
            !row.values.contains_key("category"),
            "the unselected WHERE helper column `category` must be trimmed, not \
             leaked into the output row"
        );
        let mut keys: Vec<&str> = row.values.keys().map(|k| k.as_ref()).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["name", "tenant_id"],
            "output row must contain exactly the selected columns"
        );
    }
}

/// #1952 (round-6) value parity: selected column values for the filtered rows
/// match the committed JSONL golden. Three known category-A rows (keyed by
/// partition `tenant_id`) must carry their exact golden `name`, and the row's
/// preserved key must contain the partition-key tenant_id bytes.
#[tokio::test]
async fn nonaggregate_select_value_parity_with_golden() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT tenant_id, name \
             FROM test_basic.multi_partition_table WHERE category = 'A'",
        )
        .await
        .expect("non-aggregate filtered SELECT must execute");

    // (tenant_id, expected name) triples taken directly from nb-1-big-Data.db.jsonl
    // for category A.
    let expected: &[(&str, &str)] = &[
        ("98e05820-982d-411c-961f-26d1057474e4", "Mrs"),
        ("a75b0a43-21c4-4561-8331-472a710f2e37", "position"),
        ("17213719-bb70-453b-8398-7cd097ca9d11", "return"),
    ];

    for &(tid, expected_name) in expected {
        let tid_bytes = uuid_bytes(tid);
        let row = result
            .rows
            .iter()
            .find(|r| matches!(r.values.get("tenant_id"), Some(Value::Uuid(b)) if *b == tid_bytes))
            .unwrap_or_else(|| panic!("row for tenant_id {tid} not found in result"));

        assert_eq!(
            row.values.get("name"),
            Some(&Value::Text(expected_name.to_string())),
            "name for tenant_id {tid} must match the golden value"
        );
        // The preserved partition key must embed the tenant_id bytes (composite
        // PK `(tenant_id, user_id)`), proving the key is real, not vec![].
        assert!(
            row.key.0.windows(16).any(|w| w == tid_bytes),
            "preserved partition key must contain the tenant_id bytes for {tid}"
        );
    }
}

/// #1952 (roborev LOW): `SELECT DISTINCT` also produces a bare-column `Project`
/// step. Distinct is deliberately scoped OUT of the #1587 "skip redundant
/// Project" optimization (`is_bare_columns` requires `SelectClause::Columns`, so
/// a `SelectClause::Distinct` ALWAYS pushes a `Project`) AND DISTINCT forces
/// materialization (`requires_materialization`, mod.rs:596). So a DISTINCT query
/// whose WHERE filters on an UNSELECTED column now flows through the new
/// key-preserving `trim_projection` (the plain-column arm of the `Project` step
/// in `execute.rs`): the scan projection is WIDENED with the WHERE helper
/// (`category`), and the DISTINCT `Project` must TRIM that helper back out while
/// preserving each row's real partition key.
///
/// `SELECT DISTINCT tenant_id, user_id ... WHERE category = 'A'` selects the two
/// partition-key columns and filters on the clustering column `category` (NOT in
/// the SELECT list). This asserts: (a) `category` is trimmed, never leaked; (b)
/// the selected DISTINCT columns are present; (c) the result equals the exact
/// distinct set from the golden — the 36 category-A partitions, all unique; (d)
/// every returned row keeps a real (non-empty) composite partition key.
#[tokio::test]
async fn distinct_partition_key_trims_unselected_where_helper() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let result = db
        .execute(
            "SELECT DISTINCT tenant_id, user_id \
             FROM test_basic.multi_partition_table WHERE category = 'A'",
        )
        .await
        .expect("DISTINCT filtered query must execute");

    // (c) Ground truth: category A has 36 rows, each a distinct single-row
    // partition, so DISTINCT over the partition key returns 36 rows.
    assert_eq!(
        result.rows.len(),
        36,
        "DISTINCT tenant_id, user_id WHERE category='A' must return the 36 \
         category-A partitions; got {}",
        result.rows.len()
    );

    let mut seen_pairs: std::collections::HashSet<([u8; 16], [u8; 16])> =
        std::collections::HashSet::new();
    for row in &result.rows {
        // (a) The unselected WHERE helper `category` must be TRIMMED, not leaked.
        assert!(
            !row.values.contains_key("category"),
            "the unselected WHERE helper column `category` must be trimmed from a \
             DISTINCT row, not leaked (DISTINCT routes through trim_projection)"
        );
        // (b) Exactly the two selected DISTINCT columns are present.
        let mut keys: Vec<&str> = row.values.keys().map(|k| k.as_ref()).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["tenant_id", "user_id"],
            "a DISTINCT row must contain exactly the selected partition-key columns"
        );
        // (d) The trim path must PRESERVE the real composite partition key (it is
        // NOT rebuilt to an empty vec![] the way `execute_projection` would).
        assert!(
            !row.key.0.is_empty(),
            "each DISTINCT row must carry its real (non-empty) composite partition \
             key; the key-preserving trim must not destroy it to vec![]"
        );

        let tid = match row.values.get("tenant_id") {
            Some(Value::Uuid(b)) => *b,
            other => panic!("tenant_id must be a Uuid, got {other:?}"),
        };
        let uid = match row.values.get("user_id") {
            Some(Value::Uuid(b)) => *b,
            other => panic!("user_id must be a Uuid, got {other:?}"),
        };
        // (c) DISTINCT contract: no (tenant_id, user_id) pair repeats.
        assert!(
            seen_pairs.insert((tid, uid)),
            "DISTINCT must not return a duplicate (tenant_id, user_id) pair"
        );
        // The preserved key must embed the partition-key bytes, proving it is the
        // real on-disk key rather than a fabricated empty one.
        assert!(
            row.key.0.windows(16).any(|w| w == tid),
            "the preserved partition key must contain the tenant_id bytes"
        );
    }

    // (c) Three known category-A partitions (tenant_ids drawn from
    // nb-1-big-Data.db.jsonl) must appear in the distinct set.
    for tid in [
        "98e05820-982d-411c-961f-26d1057474e4",
        "a75b0a43-21c4-4561-8331-472a710f2e37",
        "17213719-bb70-453b-8398-7cd097ca9d11",
    ] {
        let bytes = uuid_bytes(tid);
        assert!(
            result
                .rows
                .iter()
                .any(|r| matches!(r.values.get("tenant_id"), Some(Value::Uuid(b)) if *b == bytes)),
            "distinct set must contain the known category-A tenant_id {tid}"
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
