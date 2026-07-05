//! Issue #1968 (P1, oracle/value-correctness): a BTI clustering slice with an
//! OPEN lower bound (`ck < N` / `ck <= N`, no `>=`) must NOT drop the partition's
//! FIRST row-index block.
//!
//! ## The bug
//!
//! A `Rows.db` row-index trie stores a separator per block EXCEPT the first: the
//! block covering keys BELOW the first separator lives at the partition body start
//! and has no trie entry. For an OPEN lower bound the physical-lower sentinel is
//! `-∞` (`b""`), so `select_row_index_blocks_for_range` returns the STORED blocks
//! overlapping the range but never that implicit first block. The pre-fix decode
//! started at the earliest STORED block, silently skipping the earliest clustering
//! rows: `SELECT pk, ck, payload ... WHERE pk = 2 AND ck < 20` returned `ck=8..19`
//! instead of `ck=0..19`.
//!
//! The fix (in `resolve_bti_clustering_seek_window`) begins the decode at the
//! partition body start whenever the range's physical-lower bound sorts below the
//! first separator (open lower, or a closed lower below the first separator such as
//! `ck >= 2 AND ck < 20` or `ck = 0`), so the implicit first block's rows are kept.
//!
//! These tests use a projection that INCLUDES `pk` — the shape that reproduces the
//! bug on plain origin/main WITHOUT depending on issue #1952 (a bare `SELECT ck`
//! seek probe is unrelated). The two-bound / open-upper / equality shapes that
//! already worked are re-asserted here as regression guards.
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

/// Serialize the tests: the work counters and access-path probe are process-global,
/// so two of these running concurrently would clobber each other's `reset()` / read.
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

async fn skip_or_db() -> Option<Database> {
    match setup().await {
        Ok(db) => Some(db),
        Err(e) => {
            eprintln!("Skipping (BTI wide_table): {e}");
            None
        }
    }
}

/// The sorted `ck` integers a query returned, to compare against an expected slice
/// independent of row ordering.
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

/// Run one slice query (projection INCLUDES `pk` — the #1968 reproduction shape)
/// under the probe lock, returning `(returned_cks, rows_decoded, access_path)`.
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

/// Skip helper: confirm pk=2 is populated (Data.db fetched) via a pk-INCLUDING
/// projection (a bare `SELECT ck` probe would need issue #1952). Returns the db and
/// full pk=2 partition when present.
async fn db_and_pk2(db: &Database) -> Option<Vec<i32>> {
    let full = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 2"
        ))
        .await
        .expect("pk=2 partition read must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return None;
    }
    assert_eq!(
        full.rows.len(),
        PARTITION_ROW_COUNT,
        "fixture invariant: pk=2 must hold {PARTITION_ROW_COUNT} clustering rows",
    );
    Some(cks(&full.rows))
}

// ---------------------------------------------------------------------------
// 1. THE BUG: open lower bound `ck < 20` must return ck=0..=19, NOT 8..=19.
//    Also asserts the slice ENGAGES (ClusteringSlice) with a BOUNDED decode — so
//    the fix is not a silent regression to a full-partition scan.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_lower_bound_lt_keeps_first_block() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    if db_and_pk2(&db).await.is_none() {
        return;
    }

    // `ck < 20` selects ck = 0..=19 (20 rows). The pre-fix bug dropped the implicit
    // first block and returned ck = 8..=19.
    let expected: Vec<i32> = (0..20).collect();
    let (returned, rows_decoded, path) = run_slice(&db, "pk = 2 AND ck < 20").await;

    assert_eq!(
        returned, expected,
        "Issue #1968: pk=2 AND ck < 20 must return ck=0..=19 (pre-fix bug returned ck=8..=19, \
         dropping the partition's first row-index block)",
    );

    // Honest, bounded access path: the seek engaged (not a full-partition fallback).
    assert_eq!(
        path,
        Some(AccessPath::ClusteringSlice),
        "Issue #1968: `ck < ?` must engage the clustering slice, not fall back to a full scan",
    );
    // A BTI clustering slice decodes whole row-index blocks, so `rows_decoded`
    // overshoots the exact 20-row predicate match by up to (roughly) one row-index
    // block. Rather than bake in a magic per-block row count tied to this fixture's
    // exact block sizing (a benign regeneration could change it and flip the test to
    // a false failure), derive the slack from the fixture's known partition size:
    // half the partition stays well clear of a full-partition regression
    // (~PARTITION_ROW_COUNT) yet is robust to block-layout changes. The strict
    // `< PARTITION_ROW_COUNT` assertion below is the primary bounded-decode guard.
    let bound = expected.len() as u64 + (PARTITION_ROW_COUNT as u64 / 2);
    assert!(
        rows_decoded > 0 && rows_decoded <= bound,
        "Issue #1968: rows_decoded ({rows_decoded}) must be in (0, {bound}] for a 20-row `ck < 20` \
         slice; a regression to full-partition decode reads ~{PARTITION_ROW_COUNT}",
    );
    assert!(
        rows_decoded < PARTITION_ROW_COUNT as u64,
        "Issue #1968: rows_decoded ({rows_decoded}) must stay strictly below the partition's \
         {PARTITION_ROW_COUNT} rows (the fix must keep the first block WITHOUT a full scan)",
    );
}

// ---------------------------------------------------------------------------
// 2. Inclusive open lower bound `ck <= N` also keeps the first block, across an
//    N below the first separator (`ck <= 3`, entirely inside the implicit block)
//    and an N above it (`ck <= 12`, implicit block + first stored block).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_lower_bound_lte_keeps_first_block() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    let Some(all_cks) = db_and_pk2(&db).await else {
        return;
    };

    for n in [3i32, 12] {
        let expected: Vec<i32> = all_cks.iter().copied().filter(|c| *c <= n).collect();
        let (returned, _decoded, path) = run_slice(&db, &format!("pk = 2 AND ck <= {n}")).await;
        assert_eq!(
            returned, expected,
            "Issue #1968: pk=2 AND ck <= {n} must return ck=0..={n} (implicit first block kept)",
        );
        // `ck <= 3` lies ENTIRELY within the implicit first block; the narrowed
        // seek engages there too (no full-scan fallback).
        assert_eq!(
            path,
            Some(AccessPath::ClusteringSlice),
            "Issue #1968: `ck <= {n}` must engage the clustering slice",
        );
    }
}

// ---------------------------------------------------------------------------
// 3. Closed lower bound BELOW the first separator (`ck >= 2 AND ck < 20`,
//    `ck = 0`) — same implicit-first-block hazard, must be correct.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn closed_lower_bound_below_first_separator_keeps_first_block() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    let Some(all_cks) = db_and_pk2(&db).await else {
        return;
    };

    // Two-bound whose lower bound (2) is below the first stored separator (ck=8):
    // ck 2..7 live in the implicit first block and must NOT be dropped.
    let expected: Vec<i32> = all_cks
        .iter()
        .copied()
        .filter(|c| (2..20).contains(c))
        .collect();
    let (returned, _d, path) = run_slice(&db, "pk = 2 AND ck >= 2 AND ck < 20").await;
    assert_eq!(
        returned, expected,
        "Issue #1968: pk=2 AND ck in [2,20) must return ck=2..=19 (ck 2..7 are in the implicit \
         first block)",
    );
    assert_eq!(path, Some(AccessPath::ClusteringSlice));

    // Equality on the very first clustering value.
    let (returned0, _d0, path0) = run_slice(&db, "pk = 2 AND ck = 0").await;
    assert_eq!(
        returned0,
        vec![0],
        "Issue #1968: pk=2 AND ck = 0 must return exactly ck=0 (implicit first block)",
    );
    assert_eq!(path0, Some(AccessPath::ClusteringSlice));
}

// ---------------------------------------------------------------------------
// 4. Regression guards: the shapes that already worked must STAY correct — a
//    two-bound range above the first block, an open UPPER bound, and equality in
//    the interior. Byte-parity vs the full-scan-filtered in-memory baseline.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn previously_working_shapes_still_correct() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    let Some(all_cks) = db_and_pk2(&db).await else {
        return;
    };

    type SliceCase = (&'static str, fn(i32) -> bool);
    let cases: &[SliceCase] = &[
        // Two-bound range entirely ABOVE the first block (start > first separator).
        ("pk = 2 AND ck >= 100 AND ck < 110", |c| {
            (100..110).contains(&c)
        }),
        // Open UPPER bound (`ck >= a`, tail of the partition).
        ("pk = 2 AND ck >= 290", |c| c >= 290),
        // Equality in the interior.
        ("pk = 2 AND ck = 150", |c| c == 150),
    ];

    for (where_clause, pred) in cases {
        let expected: Vec<i32> = all_cks.iter().copied().filter(|c| pred(*c)).collect();
        let (returned, _decoded, path) = run_slice(&db, where_clause).await;
        assert_eq!(
            returned, expected,
            "Issue #1968: `{where_clause}` must equal the full-scan-filtered baseline",
        );
        assert_eq!(
            path,
            Some(AccessPath::ClusteringSlice),
            "Issue #1968: `{where_clause}` must still engage the clustering slice",
        );
    }
}
