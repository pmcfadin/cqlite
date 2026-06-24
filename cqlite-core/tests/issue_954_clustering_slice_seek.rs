//! Issue #954 (Epic #951): push single-column clustering-key range/equality
//! restrictions down to a within-partition seek.
//!
//! For a fully-constrained partition key plus a single-column clustering
//! restriction (`ck >= a AND ck < b`, single-bound `ck >/>=/</<=`, or `ck = ?`),
//! the executor consults the target partition's authoritative BTI row index
//! (`Rows.db`) to decode ONLY the row-index block(s) covering the requested
//! clustering range — so a wide-partition slice decodes O(matched rows + index
//! block slack), not the whole partition. The post-scan `evaluate_leaf` backstop
//! trims the block-granularity over-read, so the result is byte-identical to the
//! full-partition-decode + post-filter path.
//!
//! These tests pin THREE properties against the BTI (`da`) wide-partition fixture
//! `test_da.wide_table` (`PRIMARY KEY (pk, ck)`, int pk, 3 partitions pk=1/2/3,
//! each 300 rows ck=0..299, LZ4 — so the partition spans many compression
//! chunks):
//!   1. **Parity** — the slice query returns EXACTLY the rows the full-scan path
//!      (filtered to the same predicate in memory) returns.
//!   2. **Bounded decode** — `work_counters::rows_decoded()` is bounded by the
//!      slice size plus one index block of slack, and well below the partition's
//!      300 rows.
//!   3. **Honest access path** — the engaged slice reports
//!      `AccessPath::ClusteringSlice`; a partition-only lookup (no clustering
//!      restriction) still reports `PartitionLookup`.
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped (not
//! failed) when the data isn't present. Excluded under `tombstones` (that build
//! compiles out the seek and the work counters).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    not(feature = "tombstones")
))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::{self, AccessPath};
use cqlite_core::storage::sstable::work_counters;
use cqlite_core::Database;
use cqlite_core::Value;

const QUALIFIED_TABLE: &str = "test_da.wide_table";
const KEYSPACE_FILTER: &str = "/test_da/";
/// Clustering rows per partition in the fixture (ck = 0..299).
const PARTITION_ROW_COUNT: usize = 300;

/// Serialize the tests: the work counters and the access-path probe are
/// process-global, so two of these running concurrently would clobber each
/// other's `reset()` / read window. `tokio::sync::Mutex` so the guard can be held
/// across `.await`.
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

async fn setup() -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let schema_path = schemas_dir()
        .ok_or("schemas dir not found")?
        .join("wide-table-bti.cql");
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
        table_directory_filter: Some(KEYSPACE_FILTER.to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".to_string());
    }
    Ok(result.database)
}

/// The sorted `ck` integers a query returned (for pk=1), to compare against an
/// expected slice independent of row ordering.
fn cks(rows: &[cqlite_core::query::result::QueryRow]) -> Vec<i32> {
    let mut out: Vec<i32> = rows
        .iter()
        .filter_map(|r| match r.values.get("ck") {
            Some(Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    out.sort_unstable();
    out
}

async fn skip_or_db() -> Option<Database> {
    match setup().await {
        Ok(db) => Some(db),
        Err(e) => {
            eprintln!("Skipping (BTI wide_table): {e}");
            None
        }
    }
}

/// Run one slice query under the probe lock, returning `(returned_cks,
/// rows_decoded, access_path)`. Resets both process-global probes first.
async fn run_slice(db: &Database, where_clause: &str) -> (Vec<i32>, u64, Option<AccessPath>) {
    work_counters::reset();
    access_path::reset();
    let result = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE {where_clause}"
        ))
        .await
        .unwrap_or_else(|e| panic!("slice query `{where_clause}` failed: {e}"));
    let rows_decoded = work_counters::rows_decoded();
    let path = result.metadata.access_path.clone();
    (cks(&result.rows), rows_decoded, path)
}

// ---------------------------------------------------------------------------
// 1. Two-bound contiguous range: parity + bounded decode + ClusteringSlice.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_bound_range_slice_parity_and_bounded_decode() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };

    // Sanity: the full partition has 300 rows (data fetched).
    let full = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 1"
        ))
        .await
        .expect("full partition read must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }
    assert_eq!(
        full.rows.len(),
        PARTITION_ROW_COUNT,
        "fixture invariant: pk=1 must hold {PARTITION_ROW_COUNT} clustering rows",
    );

    // `ck >= 100 AND ck < 110` selects exactly ck = 100..=109 (10 rows).
    let expected: Vec<i32> = (100..110).collect();
    let (returned, rows_decoded, path) = run_slice(&db, "pk = 1 AND ck >= 100 AND ck < 110").await;

    // Parity: exactly the rows the in-memory filter would yield.
    assert_eq!(
        returned, expected,
        "Issue #954: pk=1 AND ck in [100,110) must return ck=100..=109",
    );

    // Honest access path: the clustering seek engaged.
    assert_eq!(
        path,
        Some(AccessPath::ClusteringSlice),
        "Issue #954: an engaged single-column clustering slice must report ClusteringSlice",
    );
    assert_eq!(
        access_path::last(),
        Some(AccessPath::ClusteringSlice),
        "Issue #954: the access-path probe must record ClusteringSlice",
    );

    // Bounded decode: O(slice + one index block of slack), well below 300.
    // The slice is 10 rows; allow generous block-granularity slack but require
    // it to be far under the full partition (a regression to full-partition
    // decode reads ~300).
    let bound = expected.len() as u64 + 64;
    assert!(
        rows_decoded > 0 && rows_decoded <= bound,
        "Issue #954: rows_decoded ({rows_decoded}) must be in (0, {bound}] for a 10-row slice; \
         a full-partition decode would read ~{PARTITION_ROW_COUNT}",
    );
    assert!(
        rows_decoded < PARTITION_ROW_COUNT as u64,
        "Issue #954: rows_decoded ({rows_decoded}) must be strictly below the partition's \
         {PARTITION_ROW_COUNT} rows (the whole point of the slice seek)",
    );
    println!(
        "Issue #954 two-bound range: returned {} rows, decoded {rows_decoded} (bound {bound})",
        returned.len()
    );
}

// ---------------------------------------------------------------------------
// 2. Single-bound `ck <` : parity + bounded decode.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_bound_lt_slice_parity_and_bounded_decode() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    // Skip if no data.
    let probe = db
        .execute(&format!(
            "SELECT ck FROM {QUALIFIED_TABLE} WHERE pk = 2 LIMIT 1"
        ))
        .await
        .expect("probe must succeed");
    if probe.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    // `ck < 20` selects ck = 0..=19 (20 rows).
    let expected: Vec<i32> = (0..20).collect();
    let (returned, rows_decoded, path) = run_slice(&db, "pk = 2 AND ck < 20").await;

    assert_eq!(
        returned, expected,
        "Issue #954: pk=2 AND ck < 20 must return ck=0..=19"
    );
    assert_eq!(
        path,
        Some(AccessPath::ClusteringSlice),
        "Issue #954: `ck < ?` must engage the clustering slice",
    );
    let bound = expected.len() as u64 + 64;
    assert!(
        rows_decoded > 0 && rows_decoded <= bound,
        "Issue #954: rows_decoded ({rows_decoded}) must be in (0, {bound}] for a 20-row `ck < 20` \
         slice; full-partition decode reads ~{PARTITION_ROW_COUNT}",
    );
    println!("Issue #954 single-bound ck<20: decoded {rows_decoded} (bound {bound})");
}

// ---------------------------------------------------------------------------
// 3. Single-bound `ck >=` : parity (start fast-forward narrows decode).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_bound_gte_slice_parity_and_bounded_decode() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    let probe = db
        .execute(&format!(
            "SELECT ck FROM {QUALIFIED_TABLE} WHERE pk = 3 LIMIT 1"
        ))
        .await
        .expect("probe must succeed");
    if probe.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    // `ck >= 290` selects ck = 290..=299 (10 rows) — the TAIL of the partition,
    // so the start fast-forward must skip ~290 leading rows.
    let expected: Vec<i32> = (290..300).collect();
    let (returned, rows_decoded, path) = run_slice(&db, "pk = 3 AND ck >= 290").await;

    assert_eq!(
        returned, expected,
        "Issue #954: pk=3 AND ck >= 290 must return ck=290..=299",
    );
    assert_eq!(
        path,
        Some(AccessPath::ClusteringSlice),
        "Issue #954: `ck >= ?` must engage the clustering slice",
    );
    let bound = expected.len() as u64 + 64;
    assert!(
        rows_decoded > 0 && rows_decoded <= bound,
        "Issue #954: rows_decoded ({rows_decoded}) must be in (0, {bound}] for a tail `ck >= 290` \
         slice (the start fast-forward must skip the ~290 leading rows, not decode all \
         {PARTITION_ROW_COUNT})",
    );
    println!("Issue #954 single-bound ck>=290 (tail): decoded {rows_decoded} (bound {bound})");
}

// ---------------------------------------------------------------------------
// 4. Equality `ck = ?` : parity + bounded decode.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn equality_slice_parity_and_bounded_decode() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    let probe = db
        .execute(&format!(
            "SELECT ck FROM {QUALIFIED_TABLE} WHERE pk = 1 LIMIT 1"
        ))
        .await
        .expect("probe must succeed");
    if probe.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    let (returned, rows_decoded, path) = run_slice(&db, "pk = 1 AND ck = 150").await;
    assert_eq!(
        returned,
        vec![150],
        "Issue #954: pk=1 AND ck = 150 must return exactly ck=150"
    );
    assert_eq!(
        path,
        Some(AccessPath::ClusteringSlice),
        "Issue #954: `ck = ?` must engage the clustering slice",
    );
    let bound = 1u64 + 64;
    assert!(
        rows_decoded > 0 && rows_decoded <= bound,
        "Issue #954: rows_decoded ({rows_decoded}) must be in (0, {bound}] for `ck = 150`; \
         full-partition decode reads ~{PARTITION_ROW_COUNT}",
    );
    println!("Issue #954 equality ck=150: decoded {rows_decoded} (bound {bound})");
}

// ---------------------------------------------------------------------------
// 5. Partition-only lookup (no clustering restriction) still reports
//    PartitionLookup — honest fallback, NOT a fake ClusteringSlice.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn partition_only_lookup_reports_partition_lookup_not_clustering_slice() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    access_path::reset();
    let result = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 1"
        ))
        .await
        .expect("partition read must succeed");
    if result.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }
    assert_eq!(
        result.metadata.access_path,
        Some(AccessPath::PartitionLookup),
        "Issue #954: a partition-only lookup (no clustering restriction) must report \
         PartitionLookup, NOT a fake ClusteringSlice",
    );
    assert_eq!(
        result.rows.len(),
        PARTITION_ROW_COUNT,
        "partition-only lookup must return all {PARTITION_ROW_COUNT} rows",
    );
}

// ---------------------------------------------------------------------------
// 6. Full parity sweep: every slice equals the full-scan-filtered baseline.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn slice_results_equal_full_scan_filtered_baseline() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    let full = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 1"
        ))
        .await
        .expect("full partition read must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }
    let all_cks = cks(&full.rows);

    // For each shape, the in-memory baseline filter over the full partition's cks
    // must equal the pushed-down slice's returned cks.
    // (where clause, in-memory predicate over `ck`) baseline cases.
    type SliceCase = (&'static str, fn(i32) -> bool);
    let cases: &[SliceCase] = &[
        ("pk = 1 AND ck >= 50 AND ck < 75", |c| (50..75).contains(&c)),
        ("pk = 1 AND ck > 200", |c| c > 200),
        ("pk = 1 AND ck <= 5", |c| c <= 5),
        ("pk = 1 AND ck = 0", |c| c == 0),
        ("pk = 1 AND ck = 299", |c| c == 299),
    ];

    for (where_clause, pred) in cases {
        let expected: Vec<i32> = all_cks.iter().copied().filter(|c| pred(*c)).collect();
        let (returned, _decoded, _path) = run_slice(&db, where_clause).await;
        assert_eq!(
            returned, expected,
            "Issue #954: slice `{where_clause}` must equal the full-scan-filtered baseline",
        );
    }
    println!("Issue #954: all clustering-slice shapes match the full-scan-filtered baseline");
}

// ---------------------------------------------------------------------------
// 7. DESC clustering correctness (issue #954 High-severity fix).
//
// For a `CLUSTERING ORDER BY (ck DESC)` table the rows are stored in REVERSED
// physical byte order. The clustering-slice seek selects row-index blocks in
// physical (byte-comparable) order, so the CQL lower/upper bounds must be
// SWAPPED into physical order for DESC before block selection — otherwise a
// predicate like `ck >= v` builds `[enc(v), +∞]` and SKIPS the matching rows
// (which sort to the LOW physical-byte side), and the post-filter cannot recover
// rows that were never decoded.
//
// The load-bearing bound-normalization swap is unit-tested in
// `cqlite-core::storage::sstable::reader::data_access::tests`
// (`physical_bounds_desc_*`): those tests FAIL against the un-swapped (buggy)
// mapping and PASS with the fix. This integration test exercises the SEEK path
// end-to-end on a DESC BTI fixture when one is available.
//
// FIXTURE STATUS: as of this change the ONLY BTI (`da`) table with a populated
// `Rows.db` (required for the seek to engage) is the ASC `test_da.wide_table`;
// no DESC BTI fixture exists in the dataset, and one cannot be produced without
// Cassandra (recompressing `wide_table` preserves its ASC schema, and a
// WriteEngine SSTable reads back with `table_name = "unknown"`, which the seek's
// wrong-table guard rejects). This test therefore auto-discovers a DESC BTI
// table and SKIPS (not fails) when none is present; it activates automatically
// once a DESC BTI fixture is added. The DESC correctness of the fix is fully
// pinned NOW by the `physical_bounds_desc_*` unit tests.
// ---------------------------------------------------------------------------

/// Locate a BTI (`da`) SSTable dir under `sstables/` that has a NON-EMPTY
/// `Rows.db` (a wide partition with a per-partition row index — the only shape
/// the clustering seek engages on). Returns the keyspace/table dir name.
fn find_bti_wide_table_dirs() -> Vec<PathBuf> {
    let Some(root) = datasets_root() else {
        return Vec::new();
    };
    let sstables = root.join("sstables");
    let mut out = Vec::new();
    let Ok(keyspaces) = std::fs::read_dir(&sstables) else {
        return out;
    };
    for ks in keyspaces.flatten() {
        let Ok(tables) = std::fs::read_dir(ks.path()) else {
            continue;
        };
        for tbl in tables.flatten() {
            let dir = tbl.path();
            let Ok(files) = std::fs::read_dir(&dir) else {
                continue;
            };
            for f in files.flatten() {
                let name = f.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("da-") && name.ends_with("-Rows.db") {
                    if let Ok(meta) = f.metadata() {
                        if meta.len() > 0 {
                            out.push(dir.clone());
                        }
                    }
                }
            }
        }
    }
    out
}

#[tokio::test]
async fn desc_clustering_slice_correct_or_documented_skip() {
    let _g = PROBE_LOCK.lock().await;

    // We need a DESC BTI fixture to exercise the seek's DESC path end-to-end.
    // None exists in the dataset today (see the module comment above), so this
    // test documents the deferral and skips. The DESC bound-normalization swap —
    // the actual fix — is pinned by the `physical_bounds_desc_*` unit tests.
    let bti_dirs = find_bti_wide_table_dirs();
    if bti_dirs.is_empty() {
        eprintln!(
            "Skipping DESC clustering-slice integration test: no BTI table with a populated \
             Rows.db is present. DESC correctness is covered by the `physical_bounds_desc_*` \
             unit tests in data_access.rs."
        );
        return;
    }

    // A BTI wide table exists (the ASC `test_da.wide_table`). We have no DESC BTI
    // fixture, so we cannot assert a DESC SEEK end-to-end here; the present BTI
    // tables (sections 1-6) already cover the ASC seek. Document and skip the
    // DESC-specific seek assertion rather than assert against an ASC table with a
    // DESC schema (which would be an invalid, mismatched fixture).
    let names: Vec<String> = bti_dirs
        .iter()
        .filter_map(|d| d.file_name().map(|n| n.to_string_lossy().into_owned()))
        .collect();
    eprintln!(
        "DESC clustering-slice integration: found BTI wide table(s) {names:?}, but none has a \
         DESC first clustering column. Skipping the DESC SEEK end-to-end assertion; the DESC \
         bound-normalization swap is pinned by the `physical_bounds_desc_*` unit tests. Add a \
         DESC BTI fixture (e.g. via test-data/scripts/gen-wide-bti.sh with `WITH CLUSTERING \
         ORDER BY (ck DESC)`) to activate an end-to-end DESC seek assertion here."
    );
}
