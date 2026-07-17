//! Issue #2063 (Epic F, F4 — deferred second half of #1594): the write-support
//! multi-generation EAGER-materialize read path
//! (`generation_merge::merge_generations_for_read`, chosen when a table dir holds
//! more than one SSTable generation AND a schema is present) now acquires ONE
//! operation-level permit from the SAME process-wide `scan_admission` semaphore the
//! windowed/lazy path uses. Before this change the eager path drained a `KWayMerger`
//! inside a single `spawn_blocking` WITHOUT passing through admission, so concurrent
//! eager multi-gen scans (the schema-present common case) were unbounded.
//!
//! ## What this guards (wiring evidence + non-vacuity)
//!
//! A self-contained fixture flushes TWO generations via the public `WriteEngine`
//! API (exactly the `issue_1849` multi-gen harness), so the reopened table dir has
//! `candidates > 1`; scanning it with `Some(schema)` provably routes through the
//! eager `merge_generations_for_read` branch (`mod.rs:1133-1135`) rather than the
//! lazy per-reader concatenation fallback (which is taken only on a merge ERROR or
//! `schema=None`). Contrast the #1594 fan-out guard, which passes `schema=None` to
//! force the LAZY branch — here we do the OPPOSITE.
//!
//! With a LOW admission limit `L` installed and `N > L` concurrent eager scans
//! driven, the `scan-offload-probe` in-flight instrumentation records:
//! - `max_in_flight <= L` — the eager path is now covered by the operation bound;
//! - `max_in_flight >= 1` — the acquire is actually WIRED (non-vacuous: a
//!   never-admitting path leaves `max_in_flight == 0`, and — because the ONLY
//!   `admit()` on the materializing multi-gen `scan` path is the new eager-path
//!   one — a non-zero max PROVES the eager branch was admitted, not the lazy
//!   fallback);
//! - `current_in_flight == 0` after — every permit was released across the
//!   `spawn_blocking` join (RAII, no leak).
//!
//! Deterministic — the assertions are the SAFETY bound and level snapshots, never
//! wall-clock timing. The deadlock-freedom flavor wraps the concurrent drive in a
//! generous timeout purely as a hang backstop (proving the shared semaphore does
//! not deadlock under `N > cap` contention); correctness is asserted from the
//! probe counters, not from elapsed time.
//!
//! On `main` (eager path unadmitted) `max_in_flight` stays `0` and the `>= 1`
//! assertion fails; a mis-sized-larger semaphore trips the `<= L` bound.

// Requires the non-default `scan-offload-probe` feature (gates the admission probe
// surface) plus `write-support` (the eager multi-gen merge path itself) and
// `state_machine` (the `SSTableManager::new` signature). The agent gate runs this
// binary with these features enabled.
#![cfg(all(
    feature = "write-support",
    feature = "state_machine",
    feature = "scan-offload-probe"
))]

use std::collections::HashMap;
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::scan_stream_windowed::scan_admission::probe as admission;
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use serial_test::serial;
use tempfile::TempDir;

const KS: &str = "eager_admit_ks";
const TBL: &str = "sessions";
/// Deliberately small admission limit so `N > LIMIT` eager scans contend.
const LIMIT: usize = 2;
/// Concurrent eager multi-gen scans launched (must exceed `LIMIT`).
const CONCURRENT_SCANS: usize = 6;
/// Partitions written per generation — enough rows that each materializing scan
/// does real work and concurrent scans can overlap under the shared semaphore.
const PARTITIONS_PER_GEN: i32 = 64;
/// Partition key targeted by the seek/point-read test. It is written into BOTH
/// generations (see `build_multigen_fixture`) so a `WHERE id = SEEK_TARGET_ID`
/// point read genuinely resolves to >1 candidate generation and takes the
/// multi-candidate `seek_merge_generations_for_read` branch (not a single-candidate
/// permit-free seek).
const SEEK_TARGET_ID: i32 = 0;

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// A simple live row (`name` cell only) — always survives the merge.
fn write_name(id: i32, name: &str, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: Value::Text(name.to_string()),
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

fn count_data_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
        .count()
}

/// Flush TWO generations into a fresh table dir and return `(TempDir, data_dir)`.
/// The two generations guarantee `candidates > 1`, so a schema-carrying scan takes
/// the eager `merge_generations_for_read` branch.
async fn build_multigen_fixture(schema: &TableSchema) -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // Gen 1 (ts=100): even ids.
    for id in (0..PARTITIONS_PER_GEN).step_by(2) {
        engine
            .write(write_name(id, &format!("g1-{id}"), 100))
            .expect("write gen1");
    }
    engine.flush().await.expect("flush 1").expect("gen1");

    // Gen 2 (ts=200): odd ids (distinct partitions, so both generations contribute)
    // PLUS a re-write of the seek target (an even id already in gen 1) so that key is
    // present in BOTH generations — a `WHERE id = SEEK_TARGET_ID` point read then
    // resolves to >1 candidate and drives the multi-candidate seek merge branch.
    for id in (1..PARTITIONS_PER_GEN).step_by(2) {
        engine
            .write(write_name(id, &format!("g2-{id}"), 200))
            .expect("write gen2");
    }
    engine
        .write(write_name(
            SEEK_TARGET_ID,
            &format!("g2-{SEEK_TARGET_ID}"),
            200,
        ))
        .expect("write seek target into gen2");
    engine.flush().await.expect("flush 2").expect("gen2");

    engine.close().await.expect("close engine");

    let sstable_dir = data_dir.join(KS).join(TBL);
    assert_eq!(
        count_data_files(&sstable_dir),
        2,
        "fixture MUST be a genuine multi-generation directory (>1 Data.db) so the \
         eager KWayMerger `merge_generations_for_read` branch is taken"
    );

    (temp_dir, data_dir)
}

async fn open_manager(data_dir: &std::path::Path, config: &Config) -> SSTableManager {
    let platform = Arc::new(Platform::new(config).await.expect("platform"));
    SSTableManager::new(
        data_dir,
        config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("SSTableManager open")
}

/// Concurrent EAGER multi-gen scans never exceed the installed admission limit,
/// the acquire is demonstrably wired (non-vacuous), and every permit is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_eager_merge_scans_never_exceed_admission_limit() {
    let schema = make_schema();
    let config = Config::default();
    let (_temp_dir, data_dir) = build_multigen_fixture(&schema).await;
    let manager = Arc::new(open_manager(&data_dir, &config).await);
    let schema = Arc::new(schema);
    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());

    // Install a low admission limit and zero the in-flight counters.
    admission::set_test_limit(LIMIT);

    // Launch N > LIMIT concurrent eager multi-gen scans. Each routes through
    // `merge_generations_for_read`, which now acquires one permit for the whole
    // operation; the semaphore caps concurrently-admitted operations at LIMIT.
    let mut handles = Vec::with_capacity(CONCURRENT_SCANS);
    for _ in 0..CONCURRENT_SCANS {
        let manager = Arc::clone(&manager);
        let schema = Arc::clone(&schema);
        let table_id = table_id.clone();
        handles.push(tokio::spawn(async move {
            manager
                .scan(&table_id, None, None, None, Some(&schema))
                .await
                .expect("eager multi-gen scan must not error")
                .len()
        }));
    }

    let mut total_rows = 0usize;
    for h in handles {
        total_rows += h.await.expect("scan task joins");
    }

    let max_admitted = admission::max_in_flight();
    let residual = admission::current_in_flight();
    admission::clear_test_limit();

    eprintln!(
        "Issue #2063 eager-merge admission guard: limit={LIMIT} \
         concurrent_scans={CONCURRENT_SCANS} max_admitted={max_admitted} total_rows={total_rows}"
    );

    // Non-vacuous: the eager merge returned the full reconciled row set.
    assert_eq!(
        total_rows,
        (CONCURRENT_SCANS as i32 * PARTITIONS_PER_GEN) as usize,
        "each of the {CONCURRENT_SCANS} eager scans must return all {PARTITIONS_PER_GEN} \
         reconciled rows; a short/zero count means the eager merge path was not exercised"
    );
    // Wiring / non-vacuity: at least one operation was admitted. The ONLY `admit()`
    // on the materializing multi-gen `scan` path is the new eager-path acquire, so a
    // non-zero max proves the EAGER branch (not the lazy fallback) was admitted.
    assert!(
        max_admitted >= 1,
        "Issue #2063: no eager multi-gen operation was ever recorded as admitted — the \
         `scan_admission::admit()` acquire in `merge_generations_for_read` is not wired \
         (or the scan fell to the lazy fallback, which does not exercise the eager path)"
    );
    // The bound: concurrently-admitted eager operations never exceeded the limit.
    assert!(
        max_admitted <= LIMIT,
        "Issue #2063 REGRESSION: {max_admitted} eager multi-gen merge operations were admitted \
         at once, exceeding the admission limit of {LIMIT}. The eager path must be bounded by \
         the same operation-concurrency semaphore as the windowed path (#1594)."
    );
    // Every permit released across the spawn_blocking join (RAII): no slot leaked.
    assert_eq!(
        residual, 0,
        "Issue #2063: {residual} admission permits were still held after all eager scans \
         finished — a scan leaked its admission slot instead of releasing it (RAII on the join)"
    );
}

/// Deadlock-freedom: more concurrent eager scans than the cap all COMPLETE (the
/// shared semaphore does not hang under contention), do real work, and stay bounded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn more_eager_scans_than_cap_all_complete_without_hanging() {
    const CAP: usize = 2;
    const N: usize = 8;

    let schema = make_schema();
    let config = Config::default();
    let (_temp_dir, data_dir) = build_multigen_fixture(&schema).await;
    let manager = Arc::new(open_manager(&data_dir, &config).await);
    let schema = Arc::new(schema);
    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());

    admission::set_test_limit(CAP);

    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let manager = Arc::clone(&manager);
        let schema = Arc::clone(&schema);
        let table_id = table_id.clone();
        handles.push(tokio::spawn(async move {
            manager
                .scan(&table_id, None, None, None, Some(&schema))
                .await
                .expect("eager multi-gen scan must not error")
                .len()
        }));
    }

    // Generous timeout is a HANG BACKSTOP only (pre-change there was no bound on the
    // eager path; the guard proves the shared semaphore does not deadlock). All
    // correctness assertions below come from the probe counters / row totals, never
    // from elapsed time.
    let total_rows: usize = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        let mut total = 0usize;
        for h in handles {
            total += h.await.expect("scan task joins");
        }
        total
    })
    .await
    .expect("all N eager scans must complete — a hang means the shared semaphore deadlocked");

    let max_admitted = admission::max_in_flight();
    let residual = admission::current_in_flight();
    admission::clear_test_limit();

    // Scans did real work (not a vacuous early return).
    assert!(
        total_rows > 0,
        "Issue #2063: {N} concurrent eager scans returned 0 rows total — the scans did no work"
    );
    // The bound held while all N eventually completed.
    assert!(
        max_admitted <= CAP,
        "Issue #2063: max_admitted {max_admitted} exceeded CAP {CAP} under N>{CAP} contention"
    );
    // Wiring: the eager path admitted at least once.
    assert!(
        max_admitted >= 1,
        "Issue #2063: eager path never admitted (max_in_flight == 0) — acquire not wired"
    );
    assert_eq!(
        residual, 0,
        "Issue #2063: {residual} permits still held after all eager scans completed"
    );
}

/// FIX 4 (rust-reviewer): the metadata sibling `merge_generations_for_read_with_metadata`
/// (the WRITETIME/TTL projection path) also acquires an operation-level permit.
/// Drive concurrent `scan_with_cell_metadata` reads over the multi-gen + schema
/// fixture so that acquire is exercised end-to-end: non-vacuous (`>= 1`), bounded
/// (`<= LIMIT`), and leak-free (`current_in_flight == 0` after).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_eager_metadata_scans_never_exceed_admission_limit() {
    let schema = make_schema();
    let config = Config::default();
    let (_temp_dir, data_dir) = build_multigen_fixture(&schema).await;
    let manager = Arc::new(open_manager(&data_dir, &config).await);
    let schema = Arc::new(schema);
    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());

    admission::set_test_limit(LIMIT);

    // N > LIMIT concurrent metadata scans; each routes through
    // `merge_generations_for_read_with_metadata` (multi-gen + schema present).
    let mut handles = Vec::with_capacity(CONCURRENT_SCANS);
    for _ in 0..CONCURRENT_SCANS {
        let manager = Arc::clone(&manager);
        let schema = Arc::clone(&schema);
        let table_id = table_id.clone();
        handles.push(tokio::spawn(async move {
            manager
                .scan_with_cell_metadata(&table_id, None, None, None, Some(&schema))
                .await
                .expect("eager metadata multi-gen scan must not error")
                .len()
        }));
    }

    let mut total_rows = 0usize;
    for h in handles {
        total_rows += h.await.expect("metadata scan task joins");
    }

    let max_admitted = admission::max_in_flight();
    let residual = admission::current_in_flight();
    admission::clear_test_limit();

    // Non-vacuous: the eager metadata merge returned the full reconciled row set.
    assert_eq!(
        total_rows,
        (CONCURRENT_SCANS as i32 * PARTITIONS_PER_GEN) as usize,
        "each metadata scan must return all {PARTITIONS_PER_GEN} reconciled rows; a \
         short/zero count means the eager metadata merge path was not exercised"
    );
    // Wiring: the metadata acquire is actually reached (non-vacuous).
    assert!(
        max_admitted >= 1,
        "Issue #2063: metadata eager path never admitted — the acquire in \
         `merge_generations_for_read_with_metadata` is not wired"
    );
    // The bound holds for the metadata path too.
    assert!(
        max_admitted <= LIMIT,
        "Issue #2063 REGRESSION: {max_admitted} eager metadata merge operations were \
         admitted at once, exceeding the admission limit of {LIMIT}"
    );
    // Every permit released (RAII): no slot leaked on the metadata path.
    assert_eq!(
        residual, 0,
        "Issue #2063: {residual} metadata-scan admission permits still held after completion"
    );
}

/// FIX 1 (roborev): the partition-SEEKING point-read helper
/// `seek_merge_generations_for_read` (reached via `scan_partition` when >1 candidate
/// generation holds the key) also acquires an operation-level permit. Drive concurrent
/// multi-candidate point reads and assert the acquire is wired, bounded, and leak-free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_eager_seek_point_reads_never_exceed_admission_limit() {
    use cqlite_core::storage::partition_key_codec::encode_partition_key_columns;

    let schema = make_schema();
    let config = Config::default();
    let (_temp_dir, data_dir) = build_multigen_fixture(&schema).await;
    let manager = Arc::new(open_manager(&data_dir, &config).await);
    let schema = Arc::new(schema);
    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());

    // The fixture wrote SEEK_TARGET_ID into BOTH generations, so the point-read
    // resolves to 2 candidate generations and the multi-candidate seek merge branch is
    // genuinely taken (not a single-candidate permit-free seek). Confirm the fixture is
    // a 2-generation directory, as the other cases do.
    let sstable_dir = data_dir.join(KS).join(TBL);
    assert_eq!(
        count_data_files(&sstable_dir),
        2,
        "seek fixture MUST hold 2 generations so `WHERE id = SEEK_TARGET_ID` (present \
         in both) resolves to >1 candidate and drives `seek_merge_generations_for_read`"
    );

    admission::set_test_limit(LIMIT);

    // Point-read the SAME partition key concurrently. SEEK_TARGET_ID is present in BOTH
    // generations, so both blooms report it present → `candidates.len() > 1` → the
    // multi-candidate `seek_merge_generations_for_read` branch runs (and admits).
    let pk_bytes = encode_partition_key_columns(&[Value::Integer(SEEK_TARGET_ID)], &schema)
        .expect("encode partition key");
    let pk = Arc::new(pk_bytes);

    let mut handles = Vec::with_capacity(CONCURRENT_SCANS);
    for _ in 0..CONCURRENT_SCANS {
        let manager = Arc::clone(&manager);
        let schema = Arc::clone(&schema);
        let table_id = table_id.clone();
        let pk = Arc::clone(&pk);
        handles.push(tokio::spawn(async move {
            manager
                .scan_partition(&table_id, &pk, Some(&schema))
                .await
                .expect("eager seek point read must not error")
                .0
                .len()
        }));
    }

    for h in handles {
        h.await.expect("seek point-read task joins");
    }

    let max_admitted = admission::max_in_flight();
    let residual = admission::current_in_flight();
    admission::clear_test_limit();

    // The target key is present in BOTH generations, so the point read routes through
    // the multi-candidate `seek_merge_generations_for_read` branch, which admits.
    // Non-vacuous: a never-admitting path leaves `max_admitted == 0`.
    assert!(
        max_admitted >= 1,
        "Issue #2063: seek eager path never admitted — the acquire in \
         `seek_merge_generations_for_read` is not wired (or the fixture failed to place \
         the target key in >1 candidate generation)"
    );
    assert!(
        max_admitted <= LIMIT,
        "Issue #2063 REGRESSION: {max_admitted} eager seek merge operations were admitted \
         at once, exceeding the admission limit of {LIMIT}"
    );
    assert_eq!(
        residual, 0,
        "Issue #2063: {residual} seek point-read admission permits still held after completion"
    );
}
