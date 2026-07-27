//! Issue #1647 (Epic L / L1): a BTI (`da`) clustering read locates its row-index
//! block(s) with an O(key-length) `Rows.db` separator-floor walk instead of
//! materializing EVERY row-index block and linearly filtering.
//!
//! Wiring evidence, driven through the public `Database` API against the BTI
//! wide-partition fixture `test_da.wide_table` (`PRIMARY KEY (pk, ck)`, int pk, 3
//! partitions pk=1/2/3, each 300 rows ck=0..299). Each partition's `Rows.db` trie
//! indexes **38** row-index blocks, so the pre-L1 enumerate-then-filter path
//! visited ~42 BTI nodes per clustering read (a full DFS) and deserialized the
//! per-partition `TrieIndexEntry` TWICE. This test pins the two work-counter
//! invariants L1 delivers:
//!
//!   1. **`BTI_NODES_VISITED < 40`** on a point clustering read — the floor/ceiling
//!      walks visit O(len(ck)) nodes, not O(blocks). (Pre-L1: ~42, a full DFS.)
//!   2. **`ROWS_DB_ENTRY_RESOLVES == 1`** per clustering read — the window path
//!      resolves the per-partition entry ONCE (pre-L1: 2 — once directly, once
//!      inside `iterate_rows_for_partition`).
//!
//! Both counters are measured AFTER a warm-up read. The next-partition seek END
//! bound is now resolved by an O(depth) local strict-ceiling trie walk (issue #2058,
//! replacing the pre-#2058 whole-trie DFS + `OnceLock` offset cache): it visits only
//! O(depth) Partitions.db nodes per read and resolves a wide successor's `Rows.db`
//! entry UNCOUNTED (it is seek-bound work, not the clustering-window per-partition
//! resolve this test accounts for). So the measured read reflects the clustering
//! window work plus that short successor descent — still well under the bounds below.
//!
//! Since issue #3002 the lane also pins the resolved WINDOW itself (not just the
//! walk) via `DECOMPRESS_CALLS`: an over-inclusive row-index window still returns
//! the correct rows, which is precisely how the two compensating #3002 defects hid
//! for two releases.
//!
//! Compiled only with `--features work-counters` (the counter getters/`reset` live
//! behind it). Requires `CQLITE_DATASETS_ROOT`; skips (never fails) when the
//! datasets root or the schema is absent, but a PRESENT `test_da` fixture that
//! decodes 0 rows is a hard FAILURE (the binaries are committed). Excluded under
//! `tombstones` (that build serves reads by a full-scan filter, compiling out the
//! clustering seek).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "work-counters",
    not(feature = "tombstones")
))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::storage::sstable::read_work_counters as rwc;
use cqlite_core::storage::sstable::work_counters as wc;
use cqlite_core::Database;
use serial_test::serial;

const QUALIFIED_TABLE: &str = "test_da.wide_table";
const KEYSPACE_FILTER: &str = "/test_da/";
/// Clustering rows per fixture partition (ck = 0..299).
const PARTITION_ROW_COUNT: usize = 300;
/// `ROWS_DECODED` bound for a narrow (1-block) clustering slice, i.e. the observable
/// size of the resolved `[body_start_rel, body_end_rel)` window (issue #3002). One
/// row-index block of this fixture holds 8 rows; the bound allows a few blocks of
/// slack while staying an order of magnitude below the partition's 300 rows, so an
/// over-inclusive window (which still returns the CORRECT rows) trips it.
const WINDOW_ROWS_BOUND: u64 = 32;
/// Node-visit bound for ONE clustering read.
///
/// The pre-L1 full DFS over a 38-block row-index trie visited ~42 nodes. The
/// floor + strict-ceiling walks are O(len(key)) instead, and since issue #3002 they
/// descend ONE extra trie level: the corrected root is the node carrying the shared
/// `0x40` NEXT_COMPONENT transition (separators are now 5 bytes, `40 80 00 00 xx`,
/// not 4), so every walk visits ~1 more node per bound — ~2 more per read. The
/// bound is re-baselined against the measured post-#3002 counts (printed by each
/// test below) with headroom, and stays well under the pre-L1 DFS count so a
/// regression to enumerate-then-filter still trips it.
const NODES_BOUND: u64 = 40;

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

async fn skip_or_db() -> Option<Database> {
    let root = datasets_root()?;
    let schema_path = schemas_dir()?.join("wide-table-bti.cql");
    if !schema_path.exists() {
        eprintln!("Skipping (L1): wide-table-bti.cql schema not found");
        return None;
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        eprintln!("Skipping (L1): sstables dir not found");
        return None;
    }
    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(KEYSPACE_FILTER.to_string()),
    };
    let result = ingest(config).await.ok()?;
    if result.schema_load_result.schemas_loaded == 0 {
        eprintln!("Skipping (L1): no schemas loaded");
        return None;
    }
    Some(result.database)
}

/// Read the full pk=1 partition to (a) prove the committed fixture really decodes
/// and (b) warm the reader's next-partition successor cache, so the measured
/// clustering reads carry only the clustering-window work.
///
/// A PRESENT fixture returning 0 rows is a hard FAILURE (issue #3002 review): the
/// `test_da/wide_table` binaries are committed, and the old
/// "Data.db not fetched?" skip turned a collapsed clustering window — exactly this
/// bug's half-fix failure mode — into a silent pass.
///
/// Also returns the full partition read's `ROWS_DECODED`, the reference every
/// narrowed slice's window must come in far under. (`DECOMPRESS_CALLS` cannot serve
/// as that proxy here: the warm-up populates the decompressed-chunk cache, so a
/// later read of the same partition decompresses 0 chunks whatever its window.
/// `ROWS_DECODED` counts rows the decoder actually parsed out of the narrowed byte
/// window, so it measures the WINDOW and is immune to caching.)
async fn warm_up(db: &Database) -> u64 {
    wc::reset();
    let full = db
        .execute(&format!(
            "SELECT pk, ck FROM {QUALIFIED_TABLE} WHERE pk = 1"
        ))
        .await
        .expect("warm-up full partition read must succeed");
    let full_rows_decoded = wc::rows_decoded();
    assert_eq!(
        full.rows.len(),
        PARTITION_ROW_COUNT,
        "fixture invariant: pk=1 must hold {PARTITION_ROW_COUNT} clustering rows (0 rows \
         means the COMMITTED fixture did not decode — a FAILURE, never a skip)",
    );
    assert!(
        full_rows_decoded >= PARTITION_ROW_COUNT as u64,
        "a full-partition read must decode all {PARTITION_ROW_COUNT} rows; got \
         {full_rows_decoded}"
    );
    full_rows_decoded
}

/// A point clustering read (`ck = 150`) visits fewer than 40 BTI nodes, resolves
/// the per-partition `Rows.db` entry exactly once, and — issue #3002 — decodes a
/// window strictly TIGHTER than the whole partition (`ROWS_DECODED`, the observable
/// proxy for `[body_start_rel, body_end_rel)`).
#[tokio::test]
#[serial]
async fn point_clustering_read_walks_floor_not_all_blocks() {
    let Some(db) = skip_or_db().await else {
        return;
    };
    let full_rows_decoded = warm_up(&db).await;

    rwc::reset();
    wc::reset();
    let res = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 1 AND ck = 150"
        ))
        .await
        .expect("point clustering read must succeed");

    // Correctness first: the exact row is returned.
    assert_eq!(
        res.rows.len(),
        1,
        "L1: `pk = 1 AND ck = 150` must return exactly the one matching row",
    );

    let nodes = rwc::bti_nodes_visited();
    let resolves = rwc::rows_db_entry_resolves();
    let rows_decoded = wc::rows_decoded();
    println!(
        "L1 point read: bti_nodes_visited={nodes} rows_db_entry_resolves={resolves} \
         rows_decoded={rows_decoded} (full partition: {full_rows_decoded})"
    );

    assert!(
        nodes > 0 && nodes < NODES_BOUND,
        "L1: a point clustering read must visit (0, {NODES_BOUND}) BTI nodes via the \
         separator-floor walk; got {nodes} (pre-L1 full DFS over 38 blocks visited ~42; \
         since #3002 each walk descends one extra level for the shared 0x40 \
         NEXT_COMPONENT root transition)",
    );
    assert_eq!(
        resolves, 1,
        "L1: the clustering-window path must resolve the per-partition Rows.db entry \
         EXACTLY once; got {resolves} (pre-L1 resolved it twice)",
    );
    // Issue #3002: pin the resolved WINDOW, not just the walk. An over-inclusive
    // window still returns the right ROW, which is exactly how the two compensating
    // defects hid for two releases — so bound the rows the window makes the decoder
    // parse. One row-index block of this fixture holds 8 rows; `WINDOW_ROWS_BOUND`
    // allows a couple of blocks of slack while staying far below 300.
    assert!(
        rows_decoded > 0 && rows_decoded <= WINDOW_ROWS_BOUND,
        "#3002: a `ck = 150` point read must decode (0, {WINDOW_ROWS_BOUND}] rows from its \
         row-index window; got {rows_decoded} of the partition's {full_rows_decoded} — an \
         over-inclusive window returns the right row while decoding the whole partition",
    );
}

/// A bounded range clustering read (`ck >= 100 AND ck < 110`) also floors in
/// O(key-length) — the forward window end is a strict-ceiling walk, not a scan of
/// every block.
#[tokio::test]
#[serial]
async fn range_clustering_read_walks_floor_not_all_blocks() {
    let Some(db) = skip_or_db().await else {
        return;
    };
    let full_rows_decoded = warm_up(&db).await;

    rwc::reset();
    wc::reset();
    let res = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 1 AND ck >= 100 AND ck < 110"
        ))
        .await
        .expect("range clustering read must succeed");

    assert_eq!(
        res.rows.len(),
        10,
        "L1: `ck in [100, 110)` must return exactly ck=100..=109",
    );

    let nodes = rwc::bti_nodes_visited();
    let resolves = rwc::rows_db_entry_resolves();
    let rows_decoded = wc::rows_decoded();
    println!(
        "L1 range read: bti_nodes_visited={nodes} rows_db_entry_resolves={resolves} \
         rows_decoded={rows_decoded} (full partition: {full_rows_decoded})"
    );

    assert!(
        nodes > 0 && nodes < NODES_BOUND,
        "L1: a range clustering read must visit (0, {NODES_BOUND}) BTI nodes via the \
         floor + strict-ceiling walks; got {nodes} (pre-L1 full DFS visited ~42)",
    );
    assert_eq!(
        resolves, 1,
        "L1: the clustering-window path must resolve the per-partition Rows.db entry \
         EXACTLY once; got {resolves} (pre-L1 resolved it twice)",
    );
    // Issue #3002: bound the resolved WINDOW here too, not only the walk — an
    // over-inclusive window returns the correct 10 rows while making the decoder parse
    // the whole partition, which no row-level assertion can see.
    assert!(
        rows_decoded > 0 && rows_decoded <= WINDOW_ROWS_BOUND,
        "#3002: a `ck >= 100 AND ck < 110` range read must decode (0, {WINDOW_ROWS_BOUND}] rows \
         from its row-index window; got {rows_decoded} of the partition's {full_rows_decoded}",
    );
}

/// Issue #3002 — the BLOCK-0 slice (`ck < 8`), whose floor is the
/// `ByteComparable.EMPTY` separator stored on the corrected root node. Pins the
/// resolved WINDOW (via `rows_decoded` — the count of rows the window makes the
/// decoder parse; `decompress_calls` would be useless here, since `warm_up` has
/// already cached the blocks and it stays 0) as well as the walk, because an
/// over-inclusive window returns the CORRECT rows: before #3002 the compensating
/// defects made this slice decode from rel 0 through the whole partition, which no
/// row-level assertion can see.
#[tokio::test]
#[serial]
async fn block_zero_slice_window_is_bounded() {
    let Some(db) = skip_or_db().await else {
        return;
    };
    let full_rows_decoded = warm_up(&db).await;

    rwc::reset();
    wc::reset();
    let res = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 1 AND ck < 8"
        ))
        .await
        .expect("block-0 clustering read must succeed");

    assert_eq!(
        res.rows.len(),
        8,
        "#3002: `ck < 8` must return exactly ck=0..=7 (the first row-index block)",
    );

    let nodes = rwc::bti_nodes_visited();
    let resolves = rwc::rows_db_entry_resolves();
    let rows_decoded = wc::rows_decoded();
    println!(
        "#3002 block-0 slice: bti_nodes_visited={nodes} rows_db_entry_resolves={resolves} \
         rows_decoded={rows_decoded} (full partition: {full_rows_decoded})"
    );

    assert!(
        nodes > 0 && nodes < NODES_BOUND,
        "#3002: the block-0 slice must floor in (0, {NODES_BOUND}) BTI nodes; got {nodes}",
    );
    assert_eq!(
        resolves, 1,
        "#3002: the per-partition Rows.db entry must resolve exactly once; got {resolves}",
    );
    assert!(
        rows_decoded > 0 && rows_decoded <= WINDOW_ROWS_BOUND,
        "#3002: the `ck < 8` window must stop at the SECOND block's start instead of \
         running to the partition end — it must decode (0, {WINDOW_ROWS_BOUND}] rows; got \
         {rows_decoded} of the partition's {full_rows_decoded}. Block 0's floor is now the \
         root's STORED empty separator, so the start narrows too",
    );
}
