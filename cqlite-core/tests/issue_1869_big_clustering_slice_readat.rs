//! Issue #1869 — the BIG ("nb") clustering-slice seek
//! (`big_decode_clustering_window`) must fetch its partition window via the shared
//! positional `point_source` (issue #1573 C2), NOT a fresh per-query
//! `new_scan_cursor()` → `open(2)`.
//!
//! #1573 migrated the primary point-read path (BIG chunk fetch, BTI lookups) off the
//! per-lookup `open(2)`, but the BIG `WHERE pk=? AND ck </>/=` slice-seek entry
//! (`scan_single_partition_clustering` → `big_decode_clustering_window` →
//! `decompress_partition_window`) still minted a scan cursor per query on non-mmap
//! backends. This test proves that residual fd-per-query is gone.
//!
//! Wiring evidence (named surface + production call chain + e2e assertions): a REAL
//! multi-block wide BIG partition is built with the production write engine (> 128
//! KiB, so the writer emits a multi-block promoted `IndexInfo` array), then the
//! production query stack (`Database::execute` → `SelectExecutor` →
//! `scan_partition_clustering` → the BIG promoted-index selector →
//! `big_decode_clustering_window`) is driven with `WHERE pk=? AND ck<…>` slices.
//!
//! Scenarios:
//!   1. **routing** — a slice query reports `AccessPath::ClusteringSlice` (the BIG
//!      promoted-index slice seek engaged) and returns exactly the live `ck` in the
//!      requested range (value parity vs the full-partition read).
//!   2. **fd high-water** — the reader is forced onto the BUFFERED backend (where
//!      the pre-#1869 path minted one `open(2)` per query), and N concurrent slice
//!      queries record ZERO `FILE_OPENS`; a control full scan on the SAME reader
//!      DOES record opens, so the zero is a real positional-read result, never a
//!      vacuous mmap free ride.
//!
//! Compiled only with `--features work-counters,write-support` (the `FILE_OPENS`
//! getter is `work-counters`-gated; the write engine builds the fixture). Excluded
//! under `tombstones` (that build serves reads via a full-scan filter, not the
//! targeted BIG seek this evidences).

#![cfg(all(
    feature = "work-counters",
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    not(feature = "tombstones")
))]

use std::sync::Arc;

use cqlite_core::config::DiskAccessMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::AccessPath;
use cqlite_core::query::result::QueryRow;
use cqlite_core::storage::sstable::read_work_counters as rwc;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use tempfile::TempDir;

const KS: &str = "test_1869";
const TBL: &str = "wide";

/// ck 0..999 EXCEPT the gap 30..39, so a ranged read across the gap is exercised.
const N_CK: i32 = 1000;
const GAP_LO: i32 = 30;
const GAP_HI: i32 = 40; // exclusive

/// Payload size per row so the partition crosses several 64 KiB IndexInfo blocks
/// (1000 rows × ~512 B ≈ 500 KB → multiple promoted-index blocks).
const PAYLOAD_LEN: usize = 512;

/// Upper bound for the slice queries (`ck < SLICE_HI`).
const SLICE_HI: i32 = 500;

/// Number of concurrent slice queries in the fd high-water scenario. On the
/// buffered backend the pre-#1869 path would mint one `open(2)` per query, so a
/// regression records ~`CONCURRENCY` opens; the fixed path records zero.
const CONCURRENCY: u32 = 32;

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

/// Live `ck` strictly below `SLICE_HI` — the expected result of `ck < SLICE_HI`.
fn expected_slice() -> Vec<i32> {
    (0..SLICE_HI)
        .filter(|c| !(GAP_LO..GAP_HI).contains(c))
        .collect()
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

/// Build a single wide partition (pk=1) with a multi-block promoted index.
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

    rt.block_on(engine.flush())
        .expect("flush")
        .expect("flush must produce an SSTable");
    rt.block_on(engine.close()).expect("close engine");
}

/// Open the fixture through the production ingestion path, FORCING the buffered
/// disk-access backend so the pre-#1869 per-query `open(2)` is observable (the mmap
/// backend never opens per cursor, which would make the fd assertion vacuous).
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

    let mut core_config = Config::default();
    core_config.storage.disk_access_mode = DiskAccessMode::Buffered;

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config,
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

// ───────────────── 1. routing: the BIG slice seek engages ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn big_clustering_slice_engages_and_matches() {
    let _g = PROBE_LOCK.lock().await;
    let (_temp, db) = open_db().await;

    // Sanity: the wide partition reads back in full.
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

    cqlite_core::query::access_path::reset();
    let res = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {KS}.{TBL} WHERE pk = 1 AND ck < {SLICE_HI}"
        ))
        .await
        .expect("slice query");
    assert_eq!(
        cks(&res.rows),
        expected_slice(),
        "Issue #1869: `ck < {SLICE_HI}` must return the live ck below {SLICE_HI}"
    );
    assert_eq!(
        res.metadata.access_path,
        Some(AccessPath::ClusteringSlice),
        "Issue #1869: an engaged BIG promoted-index slice must report ClusteringSlice \
         (proving the query routed through big_decode_clustering_window), got {:?}",
        res.metadata.access_path
    );
}

// ───────────────── 2. fd high-water: no `open(2)` per slice query ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_big_clustering_slice_queries_open_no_fd() {
    let _g = PROBE_LOCK.lock().await;
    let (_temp, db) = open_db().await;

    let slice_sql = format!("SELECT pk, ck, payload FROM {KS}.{TBL} WHERE pk = 1 AND ck < {SLICE_HI}");

    // Warm the reader (cold-open fd + lazy index materialization happen here, BEFORE
    // the measured window) so the measured section observes only per-query I/O.
    let warm = db.execute(&slice_sql).await.expect("warmup slice query");
    assert_eq!(
        cks(&warm.rows),
        expected_slice(),
        "warmup slice must already return the correct rows (0 rows = read regression)"
    );

    // Measured: N concurrent slice queries must record ZERO opens on the buffered
    // backend — the migrated positional path issues `read_at` calls on the shared
    // point source, never a per-query `new_scan_cursor()` → `open(2)`.
    rwc::reset();
    assert_eq!(rwc::file_opens(), 0, "reset must zero FILE_OPENS");
    let fd_before = rwc::fd_high_water();

    let mut handles = Vec::new();
    for _ in 0..CONCURRENCY {
        let (d, sql) = (db.clone(), slice_sql.clone());
        handles.push(tokio::spawn(async move { d.execute(&sql).await }));
    }
    for h in handles {
        let res = h.await.expect("task").expect("slice query");
        assert_eq!(
            cks(&res.rows),
            expected_slice(),
            "each concurrent slice query must return the correct rows (0/partial = regression)"
        );
    }

    let opens = rwc::file_opens();
    assert_eq!(
        opens, 0,
        "Issue #1869: {CONCURRENCY} concurrent BIG clustering-slice queries recorded \
         {opens} FILE_OPENS; the migrated positional path must open NONE per query \
         (a regression to new_scan_cursor() records ~{CONCURRENCY} here)."
    );

    // Non-vacuous guard: prove THIS reader/backend actually opens fds on a genuine
    // scan-cursor path, so the zero above is a real positional-read result rather
    // than an mmap backend that never opens per cursor. A full table scan mints a
    // per-scan buffered cursor (`new_scan_cursor` → `open(2)`).
    rwc::reset();
    let scan = db
        .execute(&format!("SELECT pk, ck FROM {KS}.{TBL}"))
        .await
        .expect("control full scan");
    assert!(
        !scan.rows.is_empty(),
        "control scan must return rows (0 = read regression)"
    );
    assert!(
        rwc::file_opens() > 0,
        "control: the buffered backend must mint at least one open(2) on a full scan; \
         if this is 0 the reader is mmap-backed and the fd assertion above was vacuous"
    );

    // Secondary sanity via the fd sampler where supported: the process fd count
    // after the slice ops did not balloon (scan cursors, if any, are closed).
    if let (Some(before), Some(after)) = (fd_before, rwc::fd_high_water()) {
        assert!(
            after <= before + 24,
            "fd count grew unexpectedly: before={before}, after={after}"
        );
    }
}
