//! Issue #3106 — WIRING evidence: a query-row-stream producer thread that dies
//! mid-scan must FAIL the `do_get` RPC, not complete it with a silently truncated
//! result set.
//!
//! # Why this test exists at the RPC surface
//!
//! The core-side pin (`cqlite-core`'s `query_rows_panic_tests`) proves the STREAM
//! reports an error. That is necessary but not sufficient: the defect that shipped
//! was a *consumer-side interpretation* — `cqlite-flight`'s single-source
//! [`ScanRowSource`] mapped the stream's `None` onto `SourceStep::Complete`, i.e.
//! "the scan finished", so a dead producer became a successful RPC with fewer rows
//! than the table holds. The call chain under test is therefore the whole thing:
//!
//! ```text
//! FlightService::do_get
//!   → MergeProducer::produce_streaming_from_readers   (cqlite-flight/src/producer_warm.rs)
//!     → bypass::ScanRowSource::open / next_step       (cqlite-flight/src/bypass.rs)
//!       → QueryRowStream::next_batch                  (cqlite-core query_rows.rs)
//!     → producer_stream::drive_row_source             (maps Err → ProducerError::Merge)
//!   → streaming::spawn_streaming                      (ProducerError → tonic::Status)
//! ```
//!
//! With the fix, `SourceStep::Complete` is reachable ONLY after the producer's
//! explicit `Done` sentinel, so the client either sees every row or sees an error
//! — never a short success.
//!
//! # Determinism
//!
//! The producer death is injected, not raced: `arm_query_row_producer_panic`
//! (cqlite-core, test-only feature `producer-fault-injection`) makes the producer
//! thread panic at a fixed batch boundary. The fixture is a SINGLE generation and
//! the merge path is FORCED to `bypass`, so the single-source arm is definitely
//! the one running; a control `do_get` with no fault armed establishes the
//! complete row count first, so "the faulted RPC is short" cannot pass vacuously.
//! No timing, sleeps or wall-clock thresholds are involved.

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::array::{Array, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tokio::sync::Mutex;
use tonic::Request;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::producer_fault::{
    arm_inner_scan_task_panic, arm_query_row_producer_panic, silence_injected_panics,
    INJECTED_PANIC_MESSAGE,
};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_flight::bypass::MERGE_PATH_ENV;
use cqlite_flight::service::CqliteFlightService;

/// Partitions in the fixture. Must be large enough that the single-source walk
/// hands the consumer SEVERAL batches, so a fault armed after the first one kills
/// the producer genuinely mid-scan (rows already streamed to the client, rows
/// still to come) — the case that used to be served as a complete scan.
const PARTITIONS: i32 = 800;

/// Batches the row source receives before its producer dies.
const BATCHES_BEFORE_THE_PANIC: u64 = 1;

/// Serializes the process-global fault arm + forced-path env var window.
static FAULT_LOCK: Mutex<()> = Mutex::const_new(());

const KS: &str = "panic_ks";
const TBL: &str = "rows";
const DDL: &str = "CREATE TABLE panic_ks.rows (pk int, v text, PRIMARY KEY (pk))";

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![column("pk", "int", false), column("v", "text", true)],
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

fn write(pk: i32, v: &str) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(pk)),
        None,
        vec![CellOperation::Write {
            column: "v".into(),
            value: Value::text(v),
        }],
        1_000,
        None,
    )
}

/// Flush `PARTITIONS` single-row partitions as ONE SSTable generation.
async fn build_one_generation() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal"), schema());
    let mut engine = WriteEngine::new(config).expect("engine");
    for pk in 0..PARTITIONS {
        engine
            .write(write(pk, &format!("row-{pk}")))
            .expect("write");
    }
    engine.flush().await.expect("flush").expect("flush info");
    assert_eq!(
        count_data_dbs(&data_dir),
        1,
        "the fixture must be exactly ONE generation, or the single-source arm \
         under test is not the arm that runs"
    );
    (temp, data_dir)
}

fn count_data_dbs(data_dir: &std::path::Path) -> usize {
    std::fs::read_dir(data_dir.join(KS).join(TBL))
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

/// How a `do_get` ended, plus the rows it actually delivered.
struct Served {
    rows: BTreeMap<i32, String>,
    /// `Some(message)` when the RPC stream terminated with an ERROR; `None` when
    /// it completed successfully.
    error: Option<String>,
}

/// Drain a `do_get` WITHOUT panicking on a stream error — the whole point is to
/// observe whether the RPC ends in success or failure.
// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
async fn do_get_served(svc: &CqliteFlightService, ticket: Vec<u8>) -> Served {
    let resp = svc
        .do_get(Request::new(Ticket::new(ticket)))
        .await
        .expect("do_get setup succeeds")
        .into_inner();
    let mapped = resp.map(|r| r.map_err(|e| FlightError::ExternalError(Box::new(e))));
    let mut stream = FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut rows = BTreeMap::new();
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(batch) => collect_rows(&batch, &mut rows),
            Err(e) => {
                return Served {
                    rows,
                    error: Some(e.to_string()),
                }
            }
        }
    }
    Served { rows, error: None }
}

fn collect_rows(batch: &RecordBatch, out: &mut BTreeMap<i32, String>) {
    let pk = batch
        .column_by_name("pk")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .expect("pk is an Int32Array");
    let v = batch
        .column_by_name("v")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .expect("v is a StringArray");
    for i in 0..batch.num_rows() {
        out.insert(pk.value(i), v.value(i).to_string());
    }
}

/// RAII guard for the process-global forced-path env var.
struct ForcedPath;

impl ForcedPath {
    fn bypass() -> Self {
        std::env::set_var(MERGE_PATH_ENV, "bypass");
        Self
    }
}

impl Drop for ForcedPath {
    fn drop(&mut self) {
        std::env::remove_var(MERGE_PATH_ENV);
    }
}

/// The pin (issue #3106): with the single-source query row stream's producer
/// thread killed mid-scan, `do_get` must terminate the client stream with an
/// ERROR and a SHORT row set — never the successful completion that made a
/// truncated result set indistinguishable from a complete one.
#[tokio::test]
async fn a_dead_query_row_producer_fails_the_do_get_instead_of_truncating_it_silently() {
    let _serialized = FAULT_LOCK.lock().await;
    let _forced = ForcedPath::bypass();
    let (_temp, data_dir) = build_one_generation().await;
    let svc = CqliteFlightService::new(data_dir, 8192);

    // Control: no fault armed. Establishes the COMPLETE result set, and that the
    // single-source arm serves this fixture successfully.
    let complete = do_get_served(&svc, ticket_bytes()).await;
    assert_eq!(
        complete.error, None,
        "the control do_get must succeed, or this fixture proves nothing"
    );
    assert_eq!(
        complete.rows.len(),
        PARTITIONS as usize,
        "test precondition: the control do_get must return EVERY written row"
    );

    // Fault: the producer thread panics just before its second batch handoff.
    let served = {
        // Silence ONLY the injected panic's console noise; the previous hook is
        // restored before any assertion below runs, so a real failure message is
        // never masked.
        let _silence = silence_injected_panics();
        let _fault = arm_query_row_producer_panic(BATCHES_BEFORE_THE_PANIC);
        do_get_served(&svc, ticket_bytes()).await
    };

    let message = served.error.expect(
        "a dead query-row producer must FAIL the do_get — completing it is issue \
         #3106: a silently truncated result set served to the client as a \
         successful scan (SourceStep::Complete must be unreachable on a dead \
         producer)",
    );
    assert!(
        message.contains("PANICKED") && message.contains(INJECTED_PANIC_MESSAGE),
        "the client-visible error must carry the producer's panic message, so the \
         failure is diagnosable rather than a generic internal error, got: {message}"
    );
    assert!(
        served.rows.len() < complete.rows.len(),
        "the faulted RPC must be SHORT of the complete {} rows — otherwise the \
         fault never fired and this test proves nothing (got {})",
        complete.rows.len(),
        served.rows.len()
    );
}

/// The SECOND boundary (issue #3106, rust-reviewer blocker): the full-ring arm's
/// rows come from an INNER `tokio` task over its own channel
/// (`scan_stream_batched_admitted`), and that task's death was likewise read as
/// "the scan finished".
///
/// This is the arm the reported defect actually takes — this ticket carries NO
/// token filter, so `producer_warm` passes `token_bound = None` and
/// `drive_full_scan_rows` runs. The task unwinds, drops its sender, and the outer
/// query-row thread sees a plain channel close. Before the fix that meant `Ok(())`
/// → the `Done` sentinel → a SUCCESSFUL truncated `do_get`; the outer channel's
/// own protocol cannot see it, which is why the first test above passes either
/// way.
///
/// # Where the fault lands, and why that is enough
///
/// `arm_inner_scan_task_panic()` panics at the scan task's ONE checkpoint: its
/// cursor-open PRELUDE (`batched_scan_stream::open_batched_scan_cursor`), NOT a
/// block decode. That is chosen for FORMAT-BRANCH INDEPENDENCE. The task body
/// branches on `requires_chunk_stitching()`, and this fixture is a CQLite-written
/// `nb-*-big-Data.db`, which resolves to `CassandraVersion::V5_0NewBig` →
/// `is_nb_format()` → **stitching branch** — so a checkpoint placed in the
/// non-stitching block decode would never fire here, and an earlier draft of this
/// very test passed VACUOUSLY for exactly that reason.
///
/// The property proven is nevertheless branch-agnostic: the join lives in
/// `BatchedScanStream::recv`, wrapping the single `tokio::spawn` in
/// `scan_stream_batched_admitted` — strictly ABOVE the `requires_chunk_stitching()`
/// branch inside the task body — so a panic in EITHER branch surfaces as the
/// identical `JoinError` on the identical code path.
///
/// What is therefore NOT covered end-to-end: a panic in the non-stitching branch's
/// own decode, because no fixture in the tree takes that branch (it needs a reader
/// whose format is not `nb`). Nothing about the fix is branch-specific, but do not
/// read this test as evidence that the non-stitching decode has been exercised.
///
/// Same control-arm-first shape: the complete result set is established with no
/// fault armed, so "the faulted RPC is short" is never vacuous.
#[tokio::test]
async fn a_dead_inner_scan_task_fails_the_do_get_instead_of_truncating_it_silently() {
    let _serialized = FAULT_LOCK.lock().await;
    let _forced = ForcedPath::bypass();
    let (_temp, data_dir) = build_one_generation().await;
    let svc = CqliteFlightService::new(data_dir, 8192);

    // Control: no fault armed. NOTE this also proves the fixture is served by the
    // full-ring arm successfully, i.e. the inner batched-scan task is genuinely on
    // the path being killed below.
    let complete = do_get_served(&svc, ticket_bytes()).await;
    assert_eq!(
        complete.error, None,
        "the control do_get must succeed, or this fixture proves nothing"
    );
    assert_eq!(
        complete.rows.len(),
        PARTITIONS as usize,
        "test precondition: the control do_get must return EVERY written row"
    );

    // Fault: the inner scan task panics in its cursor-open prelude, so it dies
    // before it can report anything — the purest form of "the sender just went
    // away", and reached whatever format branch this reader takes (see the header:
    // a checkpoint inside one branch can silently not fire).
    let served = {
        let _silence = silence_injected_panics();
        let _fault = arm_inner_scan_task_panic();
        do_get_served(&svc, ticket_bytes()).await
    };

    let message = served.error.expect(
        "a dead INNER batched-scan task must FAIL the do_get — completing it is \
         issue #3106 one layer down: the query-row thread read the inner channel's \
         close as 'the scan finished', sent its Done sentinel, and served a \
         truncated result set as a successful full-table scan",
    );
    assert!(
        message.contains("DIED without reporting") && message.contains("TRUNCATED"),
        "the client-visible error must name the dead task and the truncation, so \
         the failure is diagnosable, got: {message}"
    );
    assert!(
        served.rows.len() < complete.rows.len(),
        "the faulted RPC must be SHORT of the complete {} rows — otherwise the \
         fault never fired and this test proves nothing (got {})",
        complete.rows.len(),
        served.rows.len()
    );
}
