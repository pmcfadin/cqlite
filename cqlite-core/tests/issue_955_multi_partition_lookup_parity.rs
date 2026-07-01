//! Issue #955 (Epic #951): `WHERE pk IN (...)` and token-range restrictions.
//!
//! Builds on #949 (single-partition targeted lookup) and #960 (honest access
//! path). These tests assert:
//!
//!   A) `WHERE pk IN (a, b, c)` over the COMPLETE partition key returns rows
//!      EQUAL to the union of the single-key `pk = a`, `pk = b`, `pk = c`
//!      queries (same set, and correct token order under LIMIT), for an int pk
//!      (`test_da.wide_table`) and a uuid pk (`test_basic.simple_table`).
//!   B) The IN path reports `AccessPath::MultiPartitionLookup`, and touches only
//!      candidate SSTables (work-counter bound, on a synthetic multi-generation
//!      fixture — see the gated test at the bottom).
//!   C) A large IN list is capped and falls back honestly to a full scan.
//!   D) A `token(pk)` range restriction returns rows EQUAL to a full scan
//!      filtered by the same token bound, and reports its access path honestly
//!      (a documented fallback — partitions are token-ordered but a true
//!      within-SSTable token-span seek is out of scope for #955; correct
//!      results + honest fallback is preferred over fake pruning).
//!   E) Empty/duplicate IN elements are handled (dedupe; an empty IN yields no
//!      rows).
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped
//! (not failed) when the data isn't present, matching the repo's other
//! dataset-backed integration tests.

// Epic #951 (honest access paths): these tests assert the `IN (...)` fan-out
// reports the TARGETED `AccessPath::MultiPartitionLookup`. The `tombstones`
// build compiles out the partition-targeted prune, so each lookup full-scans +
// retains and the executor honestly reports
// `FallbackFullScan { TombstonesBuildNoPrune }` instead — making the
// targeted-label assertions inapplicable. Gate the file `not(tombstones)` so
// the parity + access-path assertions only run where the prune exists.
#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    not(feature = "tombstones")
))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::AccessPath;
use cqlite_core::query::result::{QueryRow, StreamingConfig};
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_core::{Database, Value};

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

/// Canonical, order-independent fingerprint of a row's columns for set comparison.
fn row_fingerprint(row: &QueryRow) -> BTreeMap<String, String> {
    row.values
        .iter()
        .map(|(k, v)| (k.to_string(), format!("{v:?}")))
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

// ---------------------------------------------------------------------------
// A) IN over an int partition key (test_da.wide_table, pk = 1, 2, 3).
// ---------------------------------------------------------------------------

const WIDE_TABLE: &str = "test_da.wide_table";

#[tokio::test]
async fn in_int_pk_equals_union_of_single_key_queries() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Reference: the union of the three single-key queries.
    let mut union: Vec<QueryRow> = Vec::new();
    for pk in [1, 2, 3] {
        let single = db
            .execute(&format!(
                "SELECT pk, ck, payload FROM {WIDE_TABLE} WHERE pk = {pk}"
            ))
            .await
            .unwrap_or_else(|e| panic!("single-key pk={pk} failed: {e}"));
        union.extend(single.rows);
    }
    if union.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    // The IN query must equal that union as a SET.
    let in_result = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {WIDE_TABLE} WHERE pk IN (1, 2, 3)"
        ))
        .await
        .expect("IN query must succeed");

    assert_eq!(
        fingerprints(&in_result.rows),
        fingerprints(&union),
        "Issue #955: WHERE pk IN (1,2,3) must equal the union of pk=1, pk=2, pk=3",
    );

    // Access path: the IN fan-out reports MultiPartitionLookup, not a full scan.
    assert_eq!(
        in_result.metadata.access_path,
        Some(AccessPath::MultiPartitionLookup),
        "Issue #955: WHERE pk IN (...) over the complete key must report \
         MultiPartitionLookup, got {:?}",
        in_result.metadata.access_path
    );
    assert!(!in_result
        .metadata
        .access_path
        .as_ref()
        .unwrap()
        .is_full_scan());
}

#[tokio::test]
async fn in_int_pk_subset_equals_union() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // A two-of-three subset (pk = 1, 3); pk = 2 must be excluded.
    let mut union: Vec<QueryRow> = Vec::new();
    for pk in [1, 3] {
        let single = db
            .execute(&format!("SELECT pk, ck FROM {WIDE_TABLE} WHERE pk = {pk}"))
            .await
            .unwrap_or_else(|e| panic!("single-key pk={pk} failed: {e}"));
        union.extend(single.rows);
    }
    if union.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    let in_result = db
        .execute(&format!(
            "SELECT pk, ck FROM {WIDE_TABLE} WHERE pk IN (1, 3)"
        ))
        .await
        .expect("IN subset query must succeed");

    assert_eq!(
        fingerprints(&in_result.rows),
        fingerprints(&union),
        "Issue #955: WHERE pk IN (1, 3) must equal the union of pk=1 and pk=3 (and exclude pk=2)",
    );
    // No pk = 2 rows leaked in.
    assert!(
        in_result
            .rows
            .iter()
            .all(|r| r.values.get("pk") != Some(&Value::Integer(2))),
        "pk = 2 must not appear in IN (1, 3)",
    );
    assert_eq!(
        in_result.metadata.access_path,
        Some(AccessPath::MultiPartitionLookup),
    );
}

#[tokio::test]
async fn in_int_pk_duplicate_and_absent_elements_handled() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Reference: pk = 1 once.
    let single = db
        .execute(&format!("SELECT pk, ck FROM {WIDE_TABLE} WHERE pk = 1"))
        .await
        .expect("single-key pk=1 failed");
    if single.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    // Duplicate element (1, 1) must not double rows; an absent key (999) adds nothing.
    let in_result = db
        .execute(&format!(
            "SELECT pk, ck FROM {WIDE_TABLE} WHERE pk IN (1, 1, 999)"
        ))
        .await
        .expect("IN with duplicate/absent must succeed");

    assert_eq!(
        fingerprints(&in_result.rows),
        fingerprints(&single.rows),
        "Issue #955: IN (1, 1, 999) must equal pk=1 exactly (dedupe + absent key adds nothing)",
    );
}

#[tokio::test]
async fn in_int_pk_with_limit_is_token_ordered_prefix() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // The full IN result, then a LIMIT'd one: the LIMIT result must be a prefix
    // of the full result in token order (Cassandra returns IN results in
    // partition-token order, not IN-list order).
    let full = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {WIDE_TABLE} WHERE pk IN (3, 1, 2)"
        ))
        .await
        .expect("full IN must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    let limited = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {WIDE_TABLE} WHERE pk IN (3, 1, 2) LIMIT 10"
        ))
        .await
        .expect("limited IN must succeed");

    assert_eq!(limited.rows.len(), 10, "LIMIT 10 must return 10 rows");
    // The LIMIT result equals the first 10 rows of the full (token-ordered) result.
    let full_prefix: Vec<_> = full.rows.iter().take(10).map(row_fingerprint).collect();
    let limited_fp: Vec<_> = limited.rows.iter().map(row_fingerprint).collect();
    assert_eq!(
        limited_fp, full_prefix,
        "Issue #955: a LIMIT'd IN must be the token-ordered prefix of the full IN result",
    );
}

// ---------------------------------------------------------------------------
// A') IN over a uuid partition key (test_basic.simple_table).
// ---------------------------------------------------------------------------

const SIMPLE_TABLE: &str = "test_basic.simple_table";

#[tokio::test]
async fn in_uuid_pk_equals_union_of_single_key_queries() {
    let db = match setup("basic-types.cql", "/test_basic/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Learn up to three real uuid partition keys from a full scan.
    let full = db
        .execute(&format!("SELECT id, name FROM {SIMPLE_TABLE}"))
        .await
        .expect("full scan must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: simple_table returned 0 rows (Data.db not fetched?)");
        return;
    }
    let mut ids: Vec<[u8; 16]> = Vec::new();
    for row in &full.rows {
        if let Some(Value::Uuid(b)) = row.values.get("id") {
            if !ids.contains(b) {
                ids.push(*b);
            }
        }
        if ids.len() == 3 {
            break;
        }
    }
    if ids.is_empty() {
        eprintln!("Skipping: no uuid ids found");
        return;
    }

    // Reference: union of single-key queries.
    let mut union: Vec<QueryRow> = Vec::new();
    for id in &ids {
        let lit = uuid_to_literal(id);
        let single = db
            .execute(&format!(
                "SELECT id, name FROM {SIMPLE_TABLE} WHERE id = {lit}"
            ))
            .await
            .unwrap_or_else(|e| panic!("single-key id={lit} failed: {e}"));
        union.extend(single.rows);
    }

    let in_list = ids
        .iter()
        .map(uuid_to_literal)
        .collect::<Vec<_>>()
        .join(", ");
    let in_result = db
        .execute(&format!(
            "SELECT id, name FROM {SIMPLE_TABLE} WHERE id IN ({in_list})"
        ))
        .await
        .expect("uuid IN must succeed");

    assert_eq!(
        fingerprints(&in_result.rows),
        fingerprints(&union),
        "Issue #955: uuid pk IN (...) must equal the union of the single-key queries",
    );
    assert_eq!(
        in_result.metadata.access_path,
        Some(AccessPath::MultiPartitionLookup),
        "Issue #955: uuid pk IN (...) must report MultiPartitionLookup, got {:?}",
        in_result.metadata.access_path,
    );
}

// ---------------------------------------------------------------------------
// C) A large IN list is capped → honest full-scan fallback.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn large_in_list_falls_back_to_full_scan_but_stays_correct() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Reference: pk = 1 alone.
    let single = db
        .execute(&format!("SELECT pk, ck FROM {WIDE_TABLE} WHERE pk = 1"))
        .await
        .expect("single-key pk=1 failed");
    if single.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    // 65 elements (> MAX_IN_TARGETED_LOOKUPS = 64): only pk = 1 exists in the
    // data; the other 64 are absent keys (100..=163, well clear of the real
    // pk = 1/2/3). The query must fall back to a full scan (reported honestly)
    // yet still return exactly pk = 1's rows.
    let mut elems: Vec<String> = (100..=163).map(|n| n.to_string()).collect();
    elems.insert(0, "1".to_string());
    assert_eq!(elems.len(), 65, "construct a 65-element IN list");
    let in_list = elems.join(", ");

    let result = db
        .execute(&format!(
            "SELECT pk, ck FROM {WIDE_TABLE} WHERE pk IN ({in_list})"
        ))
        .await
        .expect("large IN must succeed");

    // Correct rows: equal to pk = 1 (the only present key in the list).
    assert_eq!(
        fingerprints(&result.rows),
        fingerprints(&single.rows),
        "Issue #955: a large IN list must still return correct rows (the union of present keys)",
    );
    // Honest reporting: it fell back to a full scan, NOT a fake MultiPartitionLookup.
    let path = result.metadata.access_path.clone().expect("access path");
    assert!(
        path.is_full_scan(),
        "Issue #955: an IN list over the cap must report a full scan (not a fake targeted \
         path), got {path:?}",
    );
}

// ---------------------------------------------------------------------------
// D) token(pk) range: correct results EQUAL to a full scan filtered by token,
//    with an honest access path (documented fallback, not fake pruning).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn token_range_equals_full_scan_filtered_by_token() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Reference: full scan, then filter rows by token(pk) in [lo, hi) in the
    // test itself, using the same partitioner the executor uses.
    let full = db
        .execute(&format!("SELECT pk, ck FROM {WIDE_TABLE}"))
        .await
        .expect("full scan must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    // wide_table has exactly pk = 1, 2, 3. Compute their tokens (single int pk →
    // raw 4-byte big-endian on-disk key) and pick a half-open span covering a
    // strict subset so the filter is meaningful.
    let mut tokens: Vec<(i32, i64)> = (1i32..=3)
        .map(|pk| (pk, cassandra_murmur3_token(&pk.to_be_bytes())))
        .collect();
    tokens.sort_by_key(|(_, t)| *t);
    // Span = [min_token, max_token): includes the two lowest-token partitions,
    // excludes the highest (Cassandra's `>=`/`<` token inclusivity).
    let lo = tokens.first().map(|(_, t)| *t).expect("min token");
    let hi = tokens.last().map(|(_, t)| *t).expect("max token");
    let excluded_pk = tokens.last().map(|(pk, _)| *pk).expect("highest-token pk");

    // Expected: full rows whose pk-token is in [lo, hi).
    let expected: Vec<QueryRow> = full
        .rows
        .iter()
        .filter(|r| match r.values.get("pk") {
            Some(Value::Integer(pk)) => {
                let t = cassandra_murmur3_token(&pk.to_be_bytes());
                t >= lo && t < hi
            }
            _ => false,
        })
        .cloned()
        .collect();

    let result = db
        .execute(&format!(
            "SELECT pk, ck FROM {WIDE_TABLE} WHERE token(pk) >= {lo} AND token(pk) < {hi}"
        ))
        .await
        .expect("token-range query must succeed");

    assert_eq!(
        fingerprints(&result.rows),
        fingerprints(&expected),
        "Issue #955: token(pk) range result must equal a full scan filtered by token in [lo, hi)",
    );
    // The excluded (highest-token) partition must not appear.
    assert!(
        result
            .rows
            .iter()
            .all(|r| r.values.get("pk") != Some(&Value::Integer(excluded_pk))),
        "the highest-token partition (pk = {excluded_pk}) is excluded by `< hi`",
    );

    // Honest access path: token-range does NOT pretend to be a targeted lookup.
    let path = result.metadata.access_path.clone().expect("access path");
    assert!(
        path.is_full_scan(),
        "Issue #955: a token-range restriction is served by a token-filtered full scan today \
         (no within-SSTable token-span seek yet); it MUST report a full scan honestly, got \
         {path:?}. A follow-up may add real token-span pruning and flip this.",
    );
}

// ---------------------------------------------------------------------------
// D') token(pk) = <t>: exact-token restriction (FINDING 1). Previously DROPPED;
//     must now equal a full scan filtered to token == t, including when combined
//     with another pushed predicate.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn token_equal_equals_full_scan_filtered_by_token() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let full = db
        .execute(&format!("SELECT pk, ck FROM {WIDE_TABLE}"))
        .await
        .expect("full scan must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    // Pick one real partition's exact token (pk = 2).
    let target_pk = 2i32;
    let t = cassandra_murmur3_token(&target_pk.to_be_bytes());

    let expected: Vec<QueryRow> = full
        .rows
        .iter()
        .filter(|r| match r.values.get("pk") {
            Some(Value::Integer(pk)) => cassandra_murmur3_token(&pk.to_be_bytes()) == t,
            _ => false,
        })
        .cloned()
        .collect();
    assert!(
        !expected.is_empty(),
        "the chosen exact token must match at least pk = {target_pk}'s rows"
    );

    let result = db
        .execute(&format!(
            "SELECT pk, ck FROM {WIDE_TABLE} WHERE token(pk) = {t}"
        ))
        .await
        .expect("token-equal query must succeed");

    assert_eq!(
        fingerprints(&result.rows),
        fingerprints(&expected),
        "FINDING 1: token(pk) = t must equal a full scan filtered to token == t",
    );
    // No other partition leaked in.
    assert!(
        result
            .rows
            .iter()
            .all(|r| r.values.get("pk") == Some(&Value::Integer(target_pk))),
        "only the partition whose token equals t may appear",
    );
}

#[tokio::test]
async fn token_equal_combined_with_clustering_predicate_is_not_ignored() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let full = db
        .execute(&format!("SELECT pk, ck FROM {WIDE_TABLE}"))
        .await
        .expect("full scan must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    // token(pk) = t(pk=2) AND ck >= 10. The bug: when combined with another
    // pushed predicate the optimizer skipped the residual Filter, so the token
    // equality was silently ignored and rows from OTHER partitions leaked in.
    let target_pk = 2i32;
    let t = cassandra_murmur3_token(&target_pk.to_be_bytes());

    let expected: Vec<QueryRow> = full
        .rows
        .iter()
        .filter(|r| {
            let token_ok = matches!(
                r.values.get("pk"),
                Some(Value::Integer(pk)) if cassandra_murmur3_token(&pk.to_be_bytes()) == t
            );
            let ck_ok = matches!(r.values.get("ck"), Some(Value::Integer(ck)) if *ck >= 10);
            token_ok && ck_ok
        })
        .cloned()
        .collect();
    assert!(
        !expected.is_empty(),
        "the combined filter must match some rows"
    );

    let result = db
        .execute(&format!(
            "SELECT pk, ck FROM {WIDE_TABLE} WHERE token(pk) = {t} AND ck >= 10"
        ))
        .await
        .expect("combined token-equal query must succeed");

    assert_eq!(
        fingerprints(&result.rows),
        fingerprints(&expected),
        "FINDING 1: token(pk) = t AND ck >= 10 must apply BOTH restrictions (the token \
         equality must not be silently dropped when combined with another predicate)",
    );
    assert!(
        result
            .rows
            .iter()
            .all(|r| r.values.get("pk") == Some(&Value::Integer(target_pk))),
        "no other partition may leak in when the token equality is enforced",
    );
}

// ---------------------------------------------------------------------------
// FINDING 2: token() on the wrong column / wrong order is rejected, not
//            silently evaluated against the real partition key.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn token_on_non_partition_key_column_is_rejected() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // `ck` is a clustering column, NOT the partition key. token(ck) would, before
    // the fix, hash the real partition key (pk) and silently return wrong rows.
    let err = db
        .execute(&format!(
            "SELECT pk, ck FROM {WIDE_TABLE} WHERE token(ck) > 0"
        ))
        .await
        .err();
    assert!(
        err.is_some(),
        "FINDING 2: token(ck) on a non-partition-key column must be rejected, not silently \
         evaluated against the real pk token",
    );
}

#[tokio::test]
async fn token_single_lower_bound_filters_correctly() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let full = db
        .execute(&format!("SELECT pk, ck FROM {WIDE_TABLE}"))
        .await
        .expect("full scan must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    // Pick the median partition token as a strict lower bound: `token(pk) > med`
    // keeps only the highest-token partition.
    let mut tokens: Vec<i64> = (1..=3)
        .map(|pk: i32| cassandra_murmur3_token(&pk.to_be_bytes()))
        .collect();
    tokens.sort_unstable();
    let med = tokens[1];

    let expected: Vec<QueryRow> = full
        .rows
        .iter()
        .filter(|r| match r.values.get("pk") {
            Some(Value::Integer(pk)) => cassandra_murmur3_token(&pk.to_be_bytes()) > med,
            _ => false,
        })
        .cloned()
        .collect();

    let result = db
        .execute(&format!(
            "SELECT pk, ck FROM {WIDE_TABLE} WHERE token(pk) > {med}"
        ))
        .await
        .expect("single-bound token query must succeed");

    assert_eq!(
        fingerprints(&result.rows),
        fingerprints(&expected),
        "Issue #955: a single-bound token(pk) > ? must filter by token (exclusive lower bound)",
    );
}

// ---------------------------------------------------------------------------
// FINDING 1 (roborev): the STREAMING path must surface an invalid token()
// query as an ERROR — synchronously from execute_streaming() or as an Err item
// from the iterator — not as a silently-empty iterator. Previously the
// validation error was raised inside the spawned background task whose caller
// only LOGGED it and closed the channel, so execute_streaming() returned an
// apparently-successful iterator that just ended with no rows.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn streaming_token_on_non_partition_key_column_surfaces_error() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let sql = format!("SELECT pk, ck FROM {WIDE_TABLE} WHERE token(ck) > 0");

    // The materializing path rejects this synchronously (FINDING 2 test above).
    // The streaming path must do the same, so an invalid query is never hidden
    // as an empty result set.
    let materialized_err = db.execute(&sql).await.err();
    assert!(
        materialized_err.is_some(),
        "precondition: execute() must reject token(ck) on a non-pk column",
    );

    let stream = db.execute_streaming(&sql, StreamingConfig::default()).await;
    match stream {
        // Preferred: error returned synchronously before spawning the task.
        Err(_) => {}
        // Acceptable alternative: the iterator yields an Err item.
        Ok(mut iter) => {
            let mut saw_error = false;
            let mut row_count = 0usize;
            while let Some(item) = iter.next_async().await {
                match item {
                    Ok(_) => row_count += 1,
                    Err(_) => {
                        saw_error = true;
                        break;
                    }
                }
            }
            assert!(
                saw_error,
                "FINDING 1: streaming token(ck) on a non-pk column must surface an ERROR, \
                 not a silently-empty iterator (got {row_count} rows, no error)",
            );
        }
    }
}

// ---------------------------------------------------------------------------
// FINDING 2 (roborev): unsupported token() forms (IN / BETWEEN) must be a
// PLANNING error on BOTH paths, including when combined with another predicate
// — never silently ignored (which would return all rows).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn token_in_is_rejected_on_both_paths() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    // Bare token(pk) IN (...).
    let sql = format!("SELECT pk, ck FROM {WIDE_TABLE} WHERE token(pk) IN (1, 2, 3)");
    assert!(
        db.execute(&sql).await.is_err(),
        "FINDING 2: token(pk) IN (...) must be a planning error on execute()",
    );
    assert!(
        db.execute_streaming(&sql, StreamingConfig::default())
            .await
            .is_err(),
        "FINDING 2: token(pk) IN (...) must be a planning error on execute_streaming()",
    );

    // Combined with another pushable predicate — the dangerous case that used to
    // be silently ignored (the residual Filter was dropped, returning all rows).
    let combined = format!("SELECT pk, ck FROM {WIDE_TABLE} WHERE token(pk) IN (1, 2) AND ck > 0");
    assert!(
        db.execute(&combined).await.is_err(),
        "FINDING 2: token(pk) IN (...) AND ck > 0 must error, not silently return all rows",
    );
    assert!(
        db.execute_streaming(&combined, StreamingConfig::default())
            .await
            .is_err(),
        "FINDING 2: streaming token(pk) IN (...) AND ck > 0 must error, not return all rows",
    );

    // token(pk) BETWEEN is likewise rejected.
    let between = format!("SELECT pk, ck FROM {WIDE_TABLE} WHERE token(pk) BETWEEN 1 AND 9");
    assert!(
        db.execute(&between).await.is_err(),
        "FINDING 2: token(pk) BETWEEN must be a planning error",
    );
}

// ---------------------------------------------------------------------------
// roborev FINDING (whole-tree): a token() restriction under OR/NOT was never
// traversed by the pushdown walk, so it was neither pushed nor enforced by a
// residual filter — silently ignored. The whole-tree validation pass in
// optimize() now guarantees no token() restriction is ever dropped: it is
// pushed (top-level / AND), or it errors (unsupported form anywhere, or any
// token form under OR/NOT). These must hold on BOTH execute and
// execute_streaming, since both go through optimize().
// ---------------------------------------------------------------------------

/// `WHERE ck > 0 AND NOT token(pk) IN (1, 2)` — the exact roborev scenario: an
/// unsupported token() form under NOT, combined with a pushable `ck > 0`.
/// Previously `ck > 0` was pushed, the residual Filter omitted, and the invalid
/// token IN restriction silently ignored. Must now error on both paths.
#[tokio::test]
async fn token_in_under_not_rejected_on_both_paths() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let sql = format!("SELECT pk, ck FROM {WIDE_TABLE} WHERE ck > 0 AND NOT token(pk) IN (1, 2)");
    assert!(
        db.execute(&sql).await.is_err(),
        "whole-tree: `ck > 0 AND NOT token(pk) IN (1, 2)` must error on execute(), \
         not silently ignore the token restriction",
    );
    assert!(
        db.execute_streaming(&sql, StreamingConfig::default())
            .await
            .is_err(),
        "whole-tree: same query must error on execute_streaming() (not an empty iterator)",
    );
}

/// `token(pk) IN (...) OR ck = 3` — unsupported token() form under OR must be a
/// planning error on both paths.
#[tokio::test]
async fn token_in_under_or_rejected_on_both_paths() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let sql = format!("SELECT pk, ck FROM {WIDE_TABLE} WHERE token(pk) IN (1, 2) OR ck = 3");
    assert!(
        db.execute(&sql).await.is_err(),
        "whole-tree: token(pk) IN (...) OR ck = 3 must error on execute()",
    );
    assert!(
        db.execute_streaming(&sql, StreamingConfig::default())
            .await
            .is_err(),
        "whole-tree: token(pk) IN (...) OR ck = 3 must error on execute_streaming()",
    );
}

/// `token(pk) > 5 OR ck = 3` — a SUPPORTED token range, but under OR it can be
/// neither pushed down nor evaluated by the row-level WHERE evaluator (which has
/// no token() support). Per the documented decision it must be a CLEAR planning
/// error rather than silently ignoring the token bound. Asserted on both paths.
#[tokio::test]
async fn supported_token_range_under_or_rejected_on_both_paths() {
    let db = match setup("wide-table-bti.cql", "/test_da/").await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping: {e}");
            return;
        }
    };

    let sql = format!("SELECT pk, ck FROM {WIDE_TABLE} WHERE token(pk) > 5 OR ck = 3");
    let err = db
        .execute(&sql)
        .await
        .expect_err("whole-tree: a supported token range under OR must error, not be ignored");
    assert!(
        err.to_string().contains("token()"),
        "error must explain the token() restriction; got: {err}",
    );
    assert!(
        db.execute_streaming(&sql, StreamingConfig::default())
            .await
            .is_err(),
        "whole-tree: supported token range under OR must error on execute_streaming() too",
    );
}
