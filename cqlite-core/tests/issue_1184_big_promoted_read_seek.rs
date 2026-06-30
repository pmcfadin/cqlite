//! Issue #1184 — BIG ("nb") promoted-index forward clustering-range seek + reverse
//! partition iteration, exercised end-to-end through the production query path.
//!
//! These tests build a REAL multi-block wide BIG partition with the production
//! write engine (a single `int`-clustered partition large enough — > 128 KiB — that
//! the writer emits a multi-block promoted `IndexInfo` array), then drive the
//! production query stack (`Database::execute` → `SelectExecutor` →
//! `scan_partition_clustering` → the BIG promoted-index selector / reverse
//! iterator). This is real wiring evidence (named surface + production call chain +
//! e2e assertions), not a helper unit test, and it runs in CI without any fetched
//! external binaries.
//!
//! It complements the byte-parity scenarios pinned against the real
//! `test_big.wide_partition` Cassandra fixture in
//! `issue_993_wide_partition_promoted_index_parity.rs` (skip-on-absence: the
//! binaries are local-only until a dataset re-pin).
//!
//! Coverage:
//!   1. Forward seek — `WHERE pk=1 AND ck>100 AND ck<140` reports
//!      `AccessPath::ClusteringSlice` (the promoted-index seek engaged) and returns
//!      exactly the live `ck` in `(100,140)`, decoding O(slice + block slack) rows
//!      (well below the partition total).
//!   2. Boundary across a clustering gap — a ranged read over `ck 25..45`, where
//!      `ck 30..39` are absent, returns `ck 29` and `ck 40` and omits `30..39`.
//!   3. Forward vs reverse — `ORDER BY ck DESC` returns the identical clustering set
//!      as ASC, in exact reverse order, driven by a back-to-front block walk
//!      (`reverse_blocks_decoded > 1`) with per-iteration memory bounded to one
//!      block (`reverse_peak_block_rows` far below the partition total).
//!   4. Fallback regression — `ORDER BY ck DESC` on a SMALL (single-block, no
//!      promoted index) partition is still served by the in-memory sort
//!      (`reverse_blocks_decoded == 0`), unchanged.

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    not(feature = "tombstones")
))]

use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::AccessPath;
use cqlite_core::query::result::QueryRow;
use cqlite_core::storage::sstable::work_counters;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use tempfile::TempDir;

const KS: &str = "test_1184";
const TBL: &str = "wide";

/// Clustering values written for the wide partition: ck 0..999 EXCEPT the gap
/// 30..39 (so a ranged read across the gap can prove no live row adjacent to it is
/// lost — the CI-runnable analogue of the real fixture's range tombstone that
/// straddles a promoted-index block boundary).
const N_CK: i32 = 1000;
const GAP_LO: i32 = 30;
const GAP_HI: i32 = 40; // exclusive

/// Payload size per row so the partition crosses several 64 KiB IndexInfo blocks.
/// 1000 rows × ~512 B ≈ 500 KB → ~8 promoted-index blocks.
const PAYLOAD_LEN: usize = 512;

/// Process-global probes (work counters + access-path) — serialize the tests.
static PROBE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn schema_cql() -> String {
    format!(
        "CREATE TABLE {KS}.{TBL} (\n  pk int,\n  ck int,\n  payload text,\n  \
         PRIMARY KEY (pk, ck)\n);\n"
    )
}

fn live_cks() -> Vec<i32> {
    (0..N_CK).filter(|c| !(GAP_LO..GAP_HI).contains(c)).collect()
}

fn write_row(pk: i32, ck: i32, payload: &str, ts: i64) -> Mutation {
    let table_id = TableId::new(KS, TBL);
    let partition_key = PartitionKey::single("pk", Value::Integer(pk));
    let clustering_key = Some(ClusteringKey::single("ck", Value::Integer(ck)));
    let ops = vec![CellOperation::Write {
        column: "payload".to_string(),
        value: Value::Text(payload.to_string()),
    }];
    Mutation::new(table_id, partition_key, clustering_key, ops, ts, None)
}

/// Build a single wide partition (pk=1) with a multi-block promoted index, plus a
/// SMALL partition (pk=2, one row) for the fallback regression. Returns the data
/// dir holding the flushed SSTable.
fn build_fixture(data_dir: &std::path::Path, wal_dir: &std::path::Path) {
    use cqlite_core::schema::parse_cql_schema;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let schema = parse_cql_schema(&schema_cql()).expect("parse fixture schema");
    let config = WriteEngineConfig::new(data_dir.to_path_buf(), wal_dir.to_path_buf(), schema);
    let mut engine = WriteEngine::new(config).expect("engine creation");

    let payload = "p".repeat(PAYLOAD_LEN);
    let mut ts = 1_000_000i64;
    for ck in live_cks() {
        engine
            .write(write_row(1, ck, &payload, ts))
            .expect("write wide row");
        ts += 1;
    }
    // Small partition: a single clustering row → no promoted index.
    engine
        .write(write_row(2, 0, "small", ts))
        .expect("write small row");

    rt.block_on(engine.flush())
        .expect("flush")
        .expect("flush must produce an SSTable");
    rt.block_on(engine.close()).expect("close engine");
}

async fn open_db() -> (TempDir, Arc<Database>) {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let schema_path = temp.path().join("schema.cql");
    std::fs::write(&schema_path, schema_cql()).expect("write schema file");

    {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        tokio::task::spawn_blocking(move || build_fixture(&data_dir, &wal_dir))
            .await
            .expect("fixture build task");
    }

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest wide-partition fixture");
    assert!(
        result.schema_load_result.schemas_loaded >= 1,
        "schema must load"
    );
    (temp, Arc::new(result.database))
}

/// Sorted-ascending `ck` ints a query returned.
fn cks(rows: &[QueryRow]) -> Vec<i32> {
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

/// `ck` ints in the exact order the query returned them (no re-sort).
fn cks_in_order(rows: &[QueryRow]) -> Vec<i32> {
    rows.iter()
        .filter_map(|r| match r.values.get("ck") {
            Some(Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect()
}

// ───────────────────────── 1. Forward seek ─────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forward_clustering_slice_seeks_via_promoted_index() {
    let _g = PROBE_LOCK.lock().await;
    let (temp, db) = open_db().await;

    // Sanity: the wide partition read back in full.
    let full = db
        .execute(&format!("SELECT pk, ck, payload FROM {KS}.{TBL} WHERE pk = 1"))
        .await
        .expect("full partition read");
    assert_eq!(
        full.rows.len(),
        live_cks().len(),
        "fixture invariant: pk=1 must hold {} live rows",
        live_cks().len()
    );

    work_counters::reset();
    cqlite_core::query::access_path::reset();
    let res = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {KS}.{TBL} WHERE pk = 1 AND ck > 100 AND ck < 140"
        ))
        .await
        .expect("forward slice query");
    let rows_decoded = work_counters::rows_decoded();

    let expected: Vec<i32> = (101..140).collect();
    assert_eq!(
        cks(&res.rows),
        expected,
        "Issue #1184: ck in (100,140) must return ck=101..=139"
    );
    assert_eq!(
        res.metadata.access_path,
        Some(AccessPath::ClusteringSlice),
        "Issue #1184: an engaged BIG promoted-index slice must report ClusteringSlice, got {:?}",
        res.metadata.access_path
    );
    // Bounded decode: a multi-block partition (~8 blocks) must NOT decode all rows.
    assert!(
        rows_decoded > 0 && rows_decoded < live_cks().len() as u64,
        "Issue #1184: rows_decoded ({rows_decoded}) must be > 0 and strictly below the \
         partition's {} rows — a regression to full-partition decode reads them all",
        live_cks().len()
    );
    drop(temp);
}

// ───────────────────── 2. Boundary across a gap ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ranged_read_across_clustering_gap_keeps_adjacent_rows() {
    let _g = PROBE_LOCK.lock().await;
    let (temp, db) = open_db().await;

    let res = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {KS}.{TBL} WHERE pk = 1 AND ck >= 25 AND ck <= 45"
        ))
        .await
        .expect("boundary slice query");

    let returned = cks(&res.rows);
    let expected: Vec<i32> = (25..=45).filter(|c| !(GAP_LO..GAP_HI).contains(c)).collect();
    assert_eq!(
        returned, expected,
        "Issue #1184: ck in [25,45] must return 25..29 and 40..45 (gap 30..39 absent), \
         keeping the rows adjacent to the gap (ck 29 and ck 40)"
    );
    assert!(returned.contains(&29) && returned.contains(&40));
    assert!(!returned.iter().any(|c| (GAP_LO..GAP_HI).contains(c)));
    drop(temp);
}

// ───────────────────── 3. Forward vs reverse ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reverse_order_by_desc_matches_forward_via_block_walk() {
    let _g = PROBE_LOCK.lock().await;
    let (temp, db) = open_db().await;

    let asc = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {KS}.{TBL} WHERE pk = 1 ORDER BY ck ASC"
        ))
        .await
        .expect("asc query");
    let asc_order = cks_in_order(&asc.rows);

    work_counters::reset();
    let desc = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {KS}.{TBL} WHERE pk = 1 ORDER BY ck DESC"
        ))
        .await
        .expect("desc query");
    let blocks = work_counters::reverse_blocks_decoded();
    let peak = work_counters::reverse_peak_block_rows();
    let desc_order = cks_in_order(&desc.rows);

    // Identical set, exact reverse ordering.
    let mut asc_sorted = asc_order.clone();
    asc_sorted.sort_unstable();
    assert_eq!(asc_sorted, live_cks(), "ASC must be the full live ck set");
    assert_eq!(
        desc_order,
        asc_order.iter().rev().copied().collect::<Vec<_>>(),
        "Issue #1184: DESC must be the exact reverse of the ASC ordering"
    );

    // Back-to-front block walk drove it (not a post-fetch in-memory sort over a
    // single forward full read), bounded to one block per iteration.
    assert!(
        blocks > 1,
        "Issue #1184: reverse must decode multiple promoted-index blocks back-to-front, \
         got reverse_blocks_decoded={blocks}"
    );
    assert!(
        peak > 0 && peak < live_cks().len() as u64,
        "Issue #1184: per-iteration block buffer ({peak}) must be bounded to one block, \
         far below the partition's {} rows",
        live_cks().len()
    );
    drop(temp);
}

// ─────────────── 5. Real Cassandra fixture (skip-on-absence) ───────────────
//
// Byte-level forward==reverse 290-row equality on the REAL `test_big.wide_partition`
// Cassandra 5.0 fixture (pk=1 = 290 live rows, ck 0..299 minus the range tombstone
// deleting ck 30..39 that straddles a promoted-index block boundary). The binaries
// are local-only until a dataset re-pin, so this skips (does not fail) when absent;
// the CI-runnable proof is the write-engine fixture tests above.

const REAL_FIXTURE_REL: &str =
    "sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294";
const REAL_PK1_LIVE_ROWS: usize = 290;

fn real_fixture_data_dir() -> Option<std::path::PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let dir = std::path::PathBuf::from(&root).join(REAL_FIXTURE_REL);
    dir.join("nb-2-big-Data.db").exists().then(|| {
        std::path::PathBuf::from(root).join("sstables")
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_fixture_forward_equals_reverse_290_rows() {
    let _g = PROBE_LOCK.lock().await;
    let Some(data_dir) = real_fixture_data_dir() else {
        eprintln!("Skipping real test_big.wide_partition forward==reverse (binaries local-only)");
        return;
    };
    let temp = TempDir::new().unwrap();
    let schema_path = temp.path().join("wide_partition.cql");
    std::fs::write(
        &schema_path,
        "CREATE TABLE test_big.wide_partition (pk int, ck int, payload text, PRIMARY KEY (pk, ck));\n",
    )
    .unwrap();

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some("/test_big/".to_string()),
    })
    .await
    .expect("ingest real test_big.wide_partition");
    let db = result.database;

    let asc = db
        .execute("SELECT pk, ck, payload FROM test_big.wide_partition WHERE pk = 1 ORDER BY ck ASC")
        .await
        .expect("asc query");
    let desc = db
        .execute("SELECT pk, ck, payload FROM test_big.wide_partition WHERE pk = 1 ORDER BY ck DESC")
        .await
        .expect("desc query");

    let asc_order = cks_in_order(&asc.rows);
    let desc_order = cks_in_order(&desc.rows);
    assert_eq!(
        asc_order.len(),
        REAL_PK1_LIVE_ROWS,
        "real fixture pk=1 must hold {REAL_PK1_LIVE_ROWS} live rows"
    );
    assert_eq!(
        desc_order.len(),
        REAL_PK1_LIVE_ROWS,
        "real fixture pk=1 DESC must hold {REAL_PK1_LIVE_ROWS} live rows"
    );
    assert_eq!(
        desc_order,
        asc_order.iter().rev().copied().collect::<Vec<_>>(),
        "real fixture: DESC must be the exact reverse of ASC (forward==reverse), \
         with no row lost adjacent to the deleted ck 30..39 block boundary"
    );
    // The deleted range is absent on both sides.
    assert!(!asc_order.iter().any(|c| (30..40).contains(c)));
}

// ───────────────────── 4. Fallback regression ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_partition_desc_uses_in_memory_sort_fallback() {
    let _g = PROBE_LOCK.lock().await;
    let (temp, db) = open_db().await;

    work_counters::reset();
    let res = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {KS}.{TBL} WHERE pk = 2 ORDER BY ck DESC"
        ))
        .await
        .expect("small desc query");
    assert_eq!(cks(&res.rows), vec![0], "pk=2 holds exactly ck=0");
    assert_eq!(
        work_counters::reverse_blocks_decoded(),
        0,
        "Issue #1184: a SMALL (no-promoted-index) partition must NOT engage the reverse \
         block iterator — the in-memory sort fallback serves it unchanged"
    );
    drop(temp);
}
