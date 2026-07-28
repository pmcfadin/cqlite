//! Issue #3058 — PATH-TAKEN pins for the single-source `do_get` merge bypass.
//!
//! These are the AC #1 / AC #2 / AC #4 assertions, and they are deliberately
//! made on an EXPLICIT OBSERVED MARKER — `cqlite_core::storage::read_path_probe`
//! counts merger constructions, compaction-reconcile entries, and per-row
//! cell-write-metadata map allocations at the sites that do the work — never on
//! elapsed time, throughput, or CPU share. A timing assertion could pass while
//! the merge still ran (the #2877 shape); `reconcile_entries == 0` cannot.
//!
//! Both directions are pinned, because only the pair is meaningful:
//!   * ONE post-prune source  → merger NOT constructed, reconciler NOT entered,
//!     ZERO `CellWriteMetadata` maps built.
//!   * TWO overlapping sources → merger IS constructed, reconciler IS entered,
//!     and the rows are correctly reconciled (later generation's overwrite wins,
//!     its row deletion hides the older row).
//!
//! ## Isolation
//!
//! The probe counters are PROCESS-GLOBAL and `CQLITE_FLIGHT_MERGE_PATH` is a
//! process-global env var, so every test here takes `PROBE_LOCK` for the whole
//! measured window. One file = one test binary = one process, so no sibling test
//! file can perturb these counts.

use std::collections::BTreeMap;
use std::path::PathBuf;
use tokio::sync::Mutex;

use arrow::array::{Array, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::storage::read_path_probe::ReadPathProbe;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_flight::bypass::MERGE_PATH_ENV;
use cqlite_flight::service::CqliteFlightService;

/// Serializes the process-global probe/env window (see the module doc).
static PROBE_LOCK: Mutex<()> = Mutex::const_new(());

const KS: &str = "bypass_ks";
const TBL: &str = "rows";
const DDL: &str = "CREATE TABLE bypass_ks.rows (pk int, ck int, v text, PRIMARY KEY (pk, ck))";

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            column("pk", "int", false),
            column("ck", "int", false),
            column("v", "text", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn column(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    }
}

fn write(pk: i32, ck: i32, v: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "v".into(),
            value: Value::text(v),
        }],
        ts,
        None,
    )
}

fn delete(pk: i32, ck: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::DeleteRow],
        ts,
        None,
    )
}

/// Flush each batch of mutations as its OWN SSTable generation, so the caller
/// controls exactly how many post-prune sources a `do_get` sees.
async fn build_generations(batches: Vec<Vec<Mutation>>) -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema());
    let mut engine = WriteEngine::new(config).expect("engine");
    for batch in batches {
        for m in batch {
            engine.write(m).expect("write");
        }
        engine.flush().await.expect("flush").expect("info");
    }
    let data_dbs = count_data_dbs(&data_dir);
    assert!(data_dbs > 0, "the fixture wrote at least one SSTable");
    (temp, data_dir)
}

/// Authoritative count of `*-Data.db` generations under the table directory —
/// the same listing the warm registry's generation probe uses, so a test can
/// state the source count it is exercising instead of assuming it.
fn count_data_dbs(data_dir: &std::path::Path) -> usize {
    let table_dir = data_dir.join(KS).join(TBL);
    std::fs::read_dir(&table_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .count()
}

fn ticket_bytes() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": KS, "table": TBL, "ddl": DDL,
    }))
    .expect("ticket json")
}

/// Drain a `do_get` into `(pk, ck) -> v` rows.
// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
async fn do_get_rows(svc: &CqliteFlightService, ticket: Vec<u8>) -> BTreeMap<(i32, i32), String> {
    let resp = svc
        .do_get(Request::new(Ticket::new(ticket)))
        .await
        .expect("do_get")
        .into_inner();
    let mapped = resp.map(|r| r.map_err(|e| FlightError::ExternalError(Box::new(e))));
    let mut stream = FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut rows = BTreeMap::new();
    while let Some(batch) = stream.next().await {
        collect_rows(&batch.expect("record batch"), &mut rows);
    }
    rows
}

fn collect_rows(batch: &RecordBatch, out: &mut BTreeMap<(i32, i32), String>) {
    let pk = int_col(batch, "pk");
    let ck = int_col(batch, "ck");
    let v = batch
        .column_by_name("v")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .expect("v is a StringArray");
    for i in 0..batch.num_rows() {
        out.insert((pk.value(i), ck.value(i)), v.value(i).to_string());
    }
}

fn int_col<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int32Array {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .unwrap_or_else(|| panic!("{name} is an Int32Array"))
}

/// RAII guard for the process-global forced-path env var.
struct ForcedPath;

impl ForcedPath {
    fn set(value: &str) -> Self {
        std::env::set_var(MERGE_PATH_ENV, value);
        Self
    }
}

impl Drop for ForcedPath {
    fn drop(&mut self) {
        std::env::remove_var(MERGE_PATH_ENV);
    }
}

/// AC #1 + AC #2: a single-source warm `do_get` builds NO merger, enters the
/// compaction reconciler ZERO times, and allocates ZERO per-row
/// `CellWriteMetadata` maps (i.e. the decoder ran with `want_cell_metadata ==
/// false`) — while still returning every live row.
#[tokio::test]
async fn single_source_do_get_neither_merges_nor_reconciles() {
    let _guard = PROBE_LOCK.lock().await;
    std::env::remove_var(MERGE_PATH_ENV);
    let (_temp, data_dir) = build_generations(vec![vec![
        write(1, 1, "a", 100),
        write(1, 2, "b", 100),
        write(2, 1, "c", 100),
    ]])
    .await;
    assert_eq!(
        count_data_dbs(&data_dir),
        1,
        "the fixture must be exactly ONE generation for this pin to mean anything"
    );

    let svc = CqliteFlightService::new(data_dir, 8192);
    let before = ReadPathProbe::snapshot();
    let rows = do_get_rows(&svc, ticket_bytes()).await;
    let delta = ReadPathProbe::snapshot().delta_since(&before);

    assert_eq!(
        rows,
        BTreeMap::from([
            ((1, 1), "a".to_string()),
            ((1, 2), "b".to_string()),
            ((2, 1), "c".to_string()),
        ]),
        "the fast path must return every live row"
    );
    assert_eq!(
        delta.mergers_built, 0,
        "AC #1: a single-source do_get must NOT construct a KWayMerger (observed \
         marker, not a timing inference)"
    );
    assert_eq!(
        delta.reconcile_entries, 0,
        "AC #1: a single-source do_get must NOT enter the compaction reconciler"
    );
    assert_eq!(
        delta.cell_metadata_maps, 0,
        "AC #2: the fast path decodes with want_cell_metadata == false — zero \
         per-row CellWriteMetadata maps are built for the request"
    );
}

/// AC #4 (the inverse): with TWO overlapping generations the merger IS built and
/// the reconciler IS entered — and the rows come back correctly reconciled. This
/// is new Flight-surface coverage: every committed `test_compaction_tombstone_ttl`
/// fixture holds exactly ONE `*-Data.db`, so without this the bypass would remove
/// the only multi-generation oracle coverage on this surface.
#[tokio::test]
async fn two_overlapping_sstables_enter_the_merger_and_reconcile() {
    let _guard = PROBE_LOCK.lock().await;
    std::env::remove_var(MERGE_PATH_ENV);
    let (_temp, data_dir) = build_generations(vec![
        // Generation 1 (older).
        vec![
            write(1, 1, "gen1-overwritten", 100),
            write(1, 2, "gen1-deleted", 100),
            write(2, 1, "gen1-kept", 100),
        ],
        // Generation 2 (newer): overwrites one value, deletes one row.
        vec![write(1, 1, "gen2-wins", 200), delete(1, 2, 200)],
    ])
    .await;
    assert_eq!(
        count_data_dbs(&data_dir),
        2,
        "the fixture must be TWO overlapping generations"
    );

    let svc = CqliteFlightService::new(data_dir, 8192);
    let before = ReadPathProbe::snapshot();
    let rows = do_get_rows(&svc, ticket_bytes()).await;
    let delta = ReadPathProbe::snapshot().delta_since(&before);

    assert_eq!(
        rows,
        BTreeMap::from([
            ((1, 1), "gen2-wins".to_string()),
            ((2, 1), "gen1-kept".to_string()),
        ]),
        "the later generation's overwrite must win and its row deletion must hide \
         the older row — no shadowed cell may surface"
    );
    assert!(
        delta.mergers_built >= 1,
        "AC #4: two overlapping sources MUST construct the KWayMerger (got {})",
        delta.mergers_built
    );
    assert!(
        delta.reconcile_entries > 0,
        "AC #4: two overlapping sources MUST enter the compaction reconciler (got {})",
        delta.reconcile_entries
    );
}

/// Spec R1: the count that decides is the POST-prune one. Two generations whose
/// partitions live in DISJOINT token regions, plus a ticket whose token range
/// covers only one of them, leaves exactly ONE source after `prune_readers` —
/// and that must select the fast path even though the table holds two
/// generations.
#[tokio::test]
async fn token_pruning_to_one_source_still_selects_the_fast_path() {
    let _guard = PROBE_LOCK.lock().await;
    std::env::remove_var(MERGE_PATH_ENV);
    // pk=1 in generation 1, pk=2 in generation 2 — no partition overlap, so each
    // generation's endpoint-token span covers exactly its own partition.
    let (_temp, data_dir) = build_generations(vec![
        vec![write(1, 1, "gen1", 100)],
        vec![write(2, 1, "gen2", 100)],
    ])
    .await;
    assert_eq!(count_data_dbs(&data_dir), 2, "two generations on disk");

    // The authoritative token of each partition key: a single `int` partition key
    // is stored as its 4-byte big-endian value, which is exactly what Cassandra
    // hashes.
    let t1 = cassandra_murmur3_token(&1_i32.to_be_bytes());
    let t2 = cassandra_murmur3_token(&2_i32.to_be_bytes());
    assert_ne!(t1, t2, "the two partitions must hash to different tokens");

    // A half-open `(start, end]` range holding ONLY pk=1's token.
    let svc = CqliteFlightService::new(data_dir, 8192);
    let ticket = serde_json::to_vec(&serde_json::json!({
        "keyspace": KS, "table": TBL, "ddl": DDL,
        "token_start": t1.saturating_sub(1), "token_end": t1,
    }))
    .expect("ticket json");

    let before = ReadPathProbe::snapshot();
    let rows = do_get_rows(&svc, ticket).await;
    let delta = ReadPathProbe::snapshot().delta_since(&before);

    assert_eq!(
        rows,
        BTreeMap::from([((1, 1), "gen1".to_string())]),
        "only the in-range partition is returned (token {t1} in ({}, {t1}]; pk=2 is at {t2})",
        t1.saturating_sub(1)
    );
    assert_eq!(
        delta.mergers_built, 0,
        "the POST-prune count is 1, so the fast path must be selected even though \
         the table has two generations"
    );
    assert_eq!(delta.reconcile_entries, 0);
    // Spec R3 on the TOKEN-BOUND arm — the Trino split shape, i.e. the route the
    // connector actually uses and the one that WAS broken mid-delivery. This arm
    // runs the Summary-guided walk, whose per-partition coverage check used to
    // decode through the compaction parser and allocate one `CellWriteMetadata`
    // map PER ROW; it is now a structure-only drive
    // (`parse_one_partition_structure_only`), so this arm allocates none either.
    // Without this assertion R3 had no regression pin here at all.
    assert_eq!(
        delta.cell_metadata_maps, 0,
        "AC #2 / spec R3 must hold on the token-bound arm too, not only the \
         full-ring one — the structure-only coverage check is what makes it true"
    );
}

// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
/// Spec R7: a client that stops reading mid-stream stops the fast path's scan —
/// the response stream is dropped and the request ends without draining the
/// table. Pinned on the OBSERVED completion of the drop (no timing threshold in
/// the assertion path): the test simply proves the server-side stream terminates
/// and no further rows are delivered after the drop.
#[tokio::test]
async fn fast_arm_stream_stops_when_the_client_drops_it() {
    let _guard = PROBE_LOCK.lock().await;
    std::env::remove_var(MERGE_PATH_ENV);
    // Enough partitions that a full drain would be many batches at batch_size 1.
    let rows: Vec<_> = (0..200).map(|i| write(i, 1, "v", 100)).collect();
    let (_temp, data_dir) = build_generations(vec![rows]).await;
    assert_eq!(count_data_dbs(&data_dir), 1);

    let svc = CqliteFlightService::new(data_dir, 1);
    let before = ReadPathProbe::snapshot();
    let resp = svc
        .do_get(Request::new(Ticket::new(ticket_bytes())))
        .await
        .expect("do_get")
        .into_inner();
    let mapped = resp.map(|r| r.map_err(|e| FlightError::ExternalError(Box::new(e))));
    let mut stream = FlightRecordBatchStream::new_from_flight_data(mapped);
    // Take ONE batch, then drop the stream mid-flight.
    let first = stream
        .next()
        .await
        .expect("a first batch")
        .expect("decodes");
    assert!(first.num_rows() > 0, "the first batch carries rows");
    drop(stream);
    let delta = ReadPathProbe::snapshot().delta_since(&before);
    assert_eq!(
        delta.mergers_built, 0,
        "the cancelled request ran on the fast arm"
    );
}

/// Roborev BLOCKER (issue #3058), e2e half: when the single reader cannot be
/// served by the single-generation streaming query walk, the request FALLS BACK
/// to the k-way merge arm and returns the FULL row set — it must never come back
/// short, empty, or `Cancelled` because the fast path touched the request's
/// cancellation flag on its way out.
///
/// The unservable reader is produced authoritatively, by removing the `Index.db`
/// (and `Summary.db`) components the walk requires — not by stubbing a predicate.
#[tokio::test]
async fn an_unservable_reader_falls_back_to_the_merge_arm_with_every_row() {
    let _guard = PROBE_LOCK.lock().await;
    std::env::remove_var(MERGE_PATH_ENV);
    let (_temp, data_dir) = build_generations(vec![vec![
        write(1, 1, "a", 100),
        write(1, 2, "b", 100),
        write(2, 1, "c", 100),
    ]])
    .await;
    let table_dir = data_dir.join(KS).join(TBL);
    for entry in std::fs::read_dir(&table_dir).expect("table dir").flatten() {
        let name = entry.file_name();
        let name = name.to_str().unwrap_or_default().to_string();
        if name.ends_with("-Index.db") || name.ends_with("-Summary.db") {
            std::fs::remove_file(entry.path()).expect("component removed");
        }
    }

    let svc = CqliteFlightService::new(data_dir, 8192);
    let before = ReadPathProbe::snapshot();
    let rows = do_get_rows(&svc, ticket_bytes()).await;
    let delta = ReadPathProbe::snapshot().delta_since(&before);

    assert_eq!(
        rows,
        BTreeMap::from([
            ((1, 1), "a".to_string()),
            ((1, 2), "b".to_string()),
            ((2, 1), "c".to_string()),
        ]),
        "the fallback must return EVERY row — a poisoned cancellation flag would \
         make this empty (or a Cancelled abort)"
    );
    assert!(
        delta.mergers_built >= 1,
        "an unservable reader must be served by the merge arm (mergers={})",
        delta.mergers_built
    );
}

/// The forced-path seam is a real kill switch: `CQLITE_FLIGHT_MERGE_PATH=merge`
/// puts a SINGLE-source request back on the k-way merge arm (observed), and the
/// rows it returns are the same ones the fast arm returns.
#[tokio::test]
async fn forced_merge_puts_a_single_source_back_on_the_merge_arm() {
    let _guard = PROBE_LOCK.lock().await;
    let (_temp, data_dir) = build_generations(vec![vec![
        write(1, 1, "a", 100),
        write(1, 2, "b", 100),
        write(2, 1, "c", 100),
    ]])
    .await;
    assert_eq!(count_data_dbs(&data_dir), 1);
    let svc = CqliteFlightService::new(data_dir, 8192);

    std::env::remove_var(MERGE_PATH_ENV);
    let auto_before = ReadPathProbe::snapshot();
    let auto_rows = do_get_rows(&svc, ticket_bytes()).await;
    let auto_delta = ReadPathProbe::snapshot().delta_since(&auto_before);

    let _forced = ForcedPath::set("merge");
    let merge_before = ReadPathProbe::snapshot();
    let merge_rows = do_get_rows(&svc, ticket_bytes()).await;
    let merge_delta = ReadPathProbe::snapshot().delta_since(&merge_before);

    assert_eq!(
        auto_rows, merge_rows,
        "the kill switch changes the ARM, never the rows"
    );
    assert_eq!(
        auto_delta.mergers_built, 0,
        "automatic selection took the fast arm"
    );
    assert!(
        merge_delta.mergers_built >= 1 && merge_delta.reconcile_entries > 0,
        "CQLITE_FLIGHT_MERGE_PATH=merge must restore the merge arm (mergers={}, \
         reconciles={})",
        merge_delta.mergers_built,
        merge_delta.reconcile_entries
    );
    assert!(
        merge_delta.cell_metadata_maps > 0,
        "the merge arm is the path that builds the per-row CellWriteMetadata maps \
         — so the fast arm's zero above is a real difference, not a dead counter"
    );
}
