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
//! ## WHICH BRANCH THESE TESTS COVER NOW (issue #3002)
//!
//! Read from the CORRECTED row-index root, the `test_da/wide_table` trie stores a
//! separator for block 0 after all: `RowIndexWriter.add` indexes it under
//! `ByteComparable.EMPTY`, parked as the ROOT node's own payload, and nothing sorts
//! below the empty key. So EVERY query below — including the open-lower ones — floors
//! to a genuine STORED block-0 entry (`data_offset = 7`, the partition body start);
//! `rows_floor_block` returns `None` for NONE of them. The rows these tests assert
//! are unchanged (block 0's rows are kept either way), but the mechanism is the
//! stored-floor path, so the names and messages here say `block 0` / `stored empty
//! separator`, not "implicit first block" — a test whose name lies about its branch is
//! worse than no test.
//!
//! The `None` (implicit-first) branch is still live for a trie whose FIRST separator
//! is NON-empty — which is exactly what CQLite's own row-index writer emits (issue
//! #3045). Its end-to-end coverage lives in
//! `issue_1968_cqlite_written_bti_implicit_first.rs`, which writes a BTI wide
//! partition with CQLite, ASSERTS that the floor walk really returns `None`, and then
//! drives the same open/below-first-separator bounds through `Database::execute`.
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
/// The `ck` of the first NON-EMPTY row-index separator for pk=2 — the inclusive start
/// of block 1. Block 0 covers ck 0..(this-1) and is indexed under the EMPTY separator
/// carried by the trie root's own payload (issue #3002), so it is a STORED floor, not
/// an unindexed implicit block. Used to pin the equality-boundary case where a closed
/// lower bound lands EXACTLY on this separator and must therefore floor PAST block 0.
const FIRST_SEPARATOR_CK: i32 = 8;

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

/// Probe that the BTI wide-partition fixture really is on disk.
///
/// This lane's hard asserts (a PRESENT fixture decoding 0 rows is a FAILURE, issue
/// #3002 review) are only meaningful once the fixture exists: an ABSENT
/// `test_da/wide_table-*/da-2-bti-Data.db` (partial fetch, or a datasets root other
/// than the in-repo corpus) SKIPs rather than panicking on a missing file — while a
/// PRESENT fixture that returns 0 rows still hard-FAILS.
///
/// `CQLITE_REQUIRE_FIXTURES=1` makes even the absent case fail closed.
fn wide_table_fixture(sstables: &Path) -> Option<PathBuf> {
    let found = std::fs::read_dir(sstables.join("test_da"))
        .ok()?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|dir| {
            dir.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("wide_table-"))
                && dir.join("da-2-bti-Data.db").exists()
        });
    if found.is_none() {
        assert!(
            !std::env::var("CQLITE_REQUIRE_FIXTURES")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            "CQLITE_REQUIRE_FIXTURES=1 but test_da/wide_table-*/da-2-bti-Data.db is absent \
             under {} — fail-closed",
            sstables.display()
        );
    }
    found
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
    if wide_table_fixture(&data_dir).is_none() {
        return Err(format!(
            "BTI fixture test_da/wide_table-*/da-2-bti-Data.db not present under {data_dir:?}"
        ));
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

/// Fixture-invariant helper: read the full pk=2 partition via a pk-INCLUDING
/// projection (a bare `SELECT ck` probe would need issue #1952) and return its cks.
///
/// The `test_da/wide_table` binaries are COMMITTED, so a 0-row read here is a
/// read-path FAILURE, never a "not fetched" skip — a silent skip would make every
/// open-lower-bound assertion below vacuous.
async fn db_and_pk2(db: &Database) -> Option<Vec<i32>> {
    let full = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 2"
        ))
        .await
        .expect("pk=2 partition read must succeed");
    assert!(
        !full.rows.is_empty(),
        "fixture invariant: pk=2 must decode at least one row (the committed \
         test_da/wide_table binaries are present; 0 rows is a read-path FAILURE)"
    );
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
//
//    BRANCH COVERED (#3002): the open-lower sentinel floors to the STORED block-0
//    entry (the root payload's EMPTY separator), so this exercises the stored-floor
//    path. The `None` implicit-first branch is covered end-to-end in
//    `issue_1968_cqlite_written_bti_implicit_first.rs`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_lower_bound_lt_keeps_block_zero_via_stored_empty_separator_floor() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    if db_and_pk2(&db).await.is_none() {
        return;
    }

    // `ck < 20` selects ck = 0..=19 (20 rows). The pre-fix bug dropped block 0 and
    // returned ck = 8..=19.
    let expected: Vec<i32> = (0..20).collect();
    let (returned, rows_decoded, path) = run_slice(&db, "pk = 2 AND ck < 20").await;

    assert_eq!(
        returned, expected,
        "Issue #1968: pk=2 AND ck < 20 must return ck=0..=19 (pre-fix bug returned ck=8..=19, \
         dropping the partition's first row-index block; the floor is block 0's STORED \
         empty-separator entry — see #3002)",
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
// 2. Inclusive open lower bound `ck <= N` also keeps block 0, across an N inside
//    block 0 (`ck <= 3`) and an N above it (`ck <= 12`, block 0 + block 1).
//    BRANCH COVERED (#3002): the stored empty-separator floor, as in test 1.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_lower_bound_lte_keeps_block_zero_via_stored_empty_separator_floor() {
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
            "Issue #1968: pk=2 AND ck <= {n} must return ck=0..={n} (block 0's rows kept)",
        );
        // `ck <= 3` lies ENTIRELY within block 0; the narrowed seek engages there too
        // (no full-scan fallback).
        assert_eq!(
            path,
            Some(AccessPath::ClusteringSlice),
            "Issue #1968: `ck <= {n}` must engage the clustering slice",
        );
    }
}

// ---------------------------------------------------------------------------
// 3. Closed lower bound INSIDE block 0 (`ck >= 2 AND ck < 20`, `ck = 0`) — below the
//    first NON-EMPTY separator (ck=8), so the same block-0 hazard, must be correct.
//    BRANCH COVERED (#3002): the stored empty-separator floor (`ck >= 2` sorts ABOVE
//    the empty separator, so the walk returns block 0, not `None`).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn closed_lower_bound_inside_block_zero_keeps_its_rows() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    let Some(all_cks) = db_and_pk2(&db).await else {
        return;
    };

    // Two-bound whose lower bound (2) is below the first NON-EMPTY separator (ck=8):
    // ck 2..7 live in block 0 and must NOT be dropped.
    let expected: Vec<i32> = all_cks
        .iter()
        .copied()
        .filter(|c| (2..20).contains(c))
        .collect();
    let (returned, _d, path) = run_slice(&db, "pk = 2 AND ck >= 2 AND ck < 20").await;
    assert_eq!(
        returned, expected,
        "Issue #1968: pk=2 AND ck in [2,20) must return ck=2..=19 (ck 2..7 live in block 0)",
    );
    assert_eq!(path, Some(AccessPath::ClusteringSlice));

    // Equality on the very first clustering value.
    let (returned0, _d0, path0) = run_slice(&db, "pk = 2 AND ck = 0").await;
    assert_eq!(
        returned0,
        vec![0],
        "Issue #1968: pk=2 AND ck = 0 must return exactly ck=0 (block 0)",
    );
    assert_eq!(path0, Some(AccessPath::ClusteringSlice));
}

// ---------------------------------------------------------------------------
// 3b. EQUALITY BOUNDARY: a closed lower bound EQUAL to the first NON-EMPTY separator
//     (`ck >= FIRST_SEPARATOR_CK`) must floor to BLOCK 1 (that separator's own
//     block), NOT back to block 0. This pins Cassandra
//     `RowIndexReader.separatorFloor` semantics and guards against an off-by-one
//     that would re-include block 0.
//
//     WHY: a separator is its block's INCLUSIVE start (Cassandra floors a lookup key
//     to the GREATEST separator <= key). Block 0 — indexed under the root payload's
//     EMPTY separator (#3002) — holds only keys STRICTLY BELOW ck=8, so a lower bound
//     of exactly 8 floors to the ck=8 separator and block 0 is correctly EXCLUDED
//     from the decode window. A bound below it (`ck >= 2`) floors to block 0 instead.
//
//     The returned rows stay ck=8..=19 either way (the `ck >= 8` row predicate filters
//     ck 0..7 post-decode), so this test asserts the DECODE WORK: `ck >= 8` (floors to
//     block 1) must decode strictly FEWER rows than `ck >= 2` (floors to block 0) over
//     the same upper bound. A floor that slid back to block 0 for `ck >= 8` would
//     flatten the two counts — which this assertion catches.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn closed_lower_bound_equal_to_first_nonempty_separator_floors_past_block_zero() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    let Some(all_cks) = db_and_pk2(&db).await else {
        return;
    };

    let sep = FIRST_SEPARATOR_CK;
    let upper = 20;

    // Primary correctness: `ck >= sep AND ck < 20` returns exactly ck=sep..=19.
    let expected: Vec<i32> = all_cks
        .iter()
        .copied()
        .filter(|c| (sep..upper).contains(c))
        .collect();
    let (returned, decoded_at_sep, path) =
        run_slice(&db, &format!("pk = 2 AND ck >= {sep} AND ck < {upper}")).await;
    assert_eq!(
        returned, expected,
        "Issue #1968: pk=2 AND ck in [{sep},{upper}) must return ck={sep}..=19 (a lower bound EQUAL \
         to the first non-empty separator floors to ITS block; block 0, ck 0..{}, is correctly \
         EXCLUDED from the window)",
        sep - 1,
    );
    assert_eq!(
        path,
        Some(AccessPath::ClusteringSlice),
        "Issue #1968: `ck >= {sep} AND ck < {upper}` must engage the clustering slice",
    );

    // Regression guard on DECODE WORK: a lower bound BELOW the first non-empty
    // separator (`ck >= 2`) floors to block 0, so it must decode STRICTLY MORE rows
    // than the equality-boundary slice (`ck >= sep`), which floors past it. A floor
    // that slid back to block 0 for `ck >= sep` would flatten this inequality.
    let (_below, decoded_below_sep, _p) =
        run_slice(&db, &format!("pk = 2 AND ck >= 2 AND ck < {upper}")).await;
    assert!(
        decoded_at_sep > 0 && decoded_below_sep > 0,
        "Issue #1968: both equality-boundary and below-separator slices must decode some rows \
         (at_sep={decoded_at_sep}, below_sep={decoded_below_sep})",
    );
    assert!(
        decoded_at_sep < decoded_below_sep,
        "Issue #1968: `ck >= {sep}` (equal to the first non-empty separator, floors to ITS block, \
         block 0 EXCLUDED) must decode strictly fewer rows ({decoded_at_sep}) than `ck >= 2` \
         (floors to block 0, {decoded_below_sep}); equal counts would mean the floor slid back to \
         block 0 at the equality boundary",
    );
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
