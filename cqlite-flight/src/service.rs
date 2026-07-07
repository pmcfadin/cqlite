//! Arrow Flight gRPC service.
//!
//! Phase 1 implements the read path: `get_flight_info` / `get_schema` (Arrow
//! schema from the ticket DDL) and `do_get` (compaction-merge → Arrow stream).
//! The remaining Flight RPCs are intentionally unimplemented.
//!
//! Clients address a table by sending a [`FlightTicket`] JSON as the
//! `FlightDescriptor.cmd` (for `get_flight_info`/`get_schema`) or as the
//! `Ticket.ticket` bytes (for `do_get`).

// The Flight gRPC trait mandates `tonic::Status` as the error type, which is
// large; helpers returning `Result<_, Status>` mirror it so they compose with the
// trait methods. Boxing would only add churn at every call site.
#![allow(clippy::result_large_err)]

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::Stream;
use tonic::{Request, Response, Status, Streaming};

use cqlite_core::schema::{parse_cql_schema, TableSchema};
use tracing::Instrument;

use crate::cancel::CancelFlag;
use crate::filter::{FilterError, ScanSpec};
use crate::obs::{rpc_span, RpcMetrics};
use crate::producer::{DirSource, MergeProducer, ProducerError};
use crate::stats::{gather_table_stats, StatsError, TableStatsRequest, TABLE_STATS_ACTION};
use crate::ticket::{FlightTicket, TicketError};

/// Boxed server response stream alias.
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Map a ticket-decode failure to a client (`invalid_argument`) gRPC error so the
/// Java connector can distinguish bad input from server faults.
impl From<TicketError> for Status {
    fn from(e: TicketError) -> Self {
        Status::invalid_argument(e.to_string())
    }
}

/// Map a producer failure to the appropriate gRPC status code, preserving the
/// error's message (and its source chain via `thiserror` `Display`).
impl From<ProducerError> for Status {
    fn from(e: ProducerError) -> Self {
        let msg = e.to_string();
        match e {
            ProducerError::InvalidColumnType { .. } | ProducerError::Aggregation(_) => {
                Status::invalid_argument(msg)
            }
            ProducerError::Discovery { source, .. }
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Status::not_found(msg)
            }
            // A canonicalization escape (issue #1430) is treated as a missing
            // resource so the server never confirms a path outside the data dir.
            ProducerError::UnsafePath { .. } => Status::not_found(msg),
            // Cooperative cancellation (issue #1473): a clean, expected abort
            // (client disconnected mid-stream), not a server fault.
            ProducerError::Cancelled => Status::aborted(msg),
            ProducerError::Discovery { .. }
            | ProducerError::Merge(_)
            | ProducerError::Convert(_)
            | ProducerError::Predicate(_)
            // A panic on the blocking pool (issue #1476, roborev B1) is a server
            // fault surfaced mid-stream, same class as any other internal error.
            | ProducerError::Panicked { .. } => Status::internal(msg),
        }
    }
}

/// Bad filter input (unknown column, type mismatch, malformed operand) is a
/// client error.
impl From<FilterError> for Status {
    fn from(e: FilterError) -> Self {
        Status::invalid_argument(e.to_string())
    }
}

/// Map a `table_stats` gather failure (issue #944) to a gRPC status: a bad
/// request body is a client error; a missing table directory is `not_found`; a
/// platform-init failure is internal.
impl From<StatsError> for Status {
    fn from(e: StatsError) -> Self {
        let msg = e.to_string();
        match e {
            StatsError::Decode(_) => Status::invalid_argument(msg),
            StatsError::Discovery { source, .. }
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Status::not_found(msg)
            }
            StatsError::Discovery { .. } => Status::internal(msg),
        }
    }
}

/// Resolved, ready-to-serve `do_get` inputs produced by the eager setup step
/// (issue #1476): the built producer, its Arrow schema, the token-pruned SSTable
/// paths, and whether the ticket aggregates. Kept together so the row and
/// aggregate response builders share one setup path.
struct DoGetSetup {
    producer: MergeProducer,
    schema_ref: Arc<ArrowSchema>,
    paths: Vec<PathBuf>,
    aggregating: bool,
}

/// Flight service over a node-local SSTable data directory.
#[derive(Clone)]
pub struct CqliteFlightService {
    /// Root holding `<keyspace>/<table>[-<uuid>]/` SSTable directories.
    data_dir: PathBuf,
    /// Max rows per emitted Arrow record batch.
    batch_size: usize,
}

impl CqliteFlightService {
    /// Create a service serving SSTables under `data_dir`.
    pub fn new(data_dir: impl Into<PathBuf>, batch_size: usize) -> Self {
        Self {
            data_dir: data_dir.into(),
            batch_size: batch_size.max(1),
        }
    }

    /// Parse the table schema from the ticket's CQL DDL.
    fn parse_schema(ticket: &FlightTicket) -> Result<TableSchema, Status> {
        parse_cql_schema(&ticket.ddl)
            .map_err(|e| Status::invalid_argument(format!("invalid ddl: {e}")))
    }

    /// Build a producer for a ticket, applying its token-range/predicate/projection
    /// filters. Used by every RPC so the Arrow schema reflects the projection.
    fn build_producer(&self, ticket: &FlightTicket) -> Result<MergeProducer, Status> {
        let schema = Self::parse_schema(ticket)?;
        let spec = ScanSpec::from_ticket(ticket, &schema)?;
        let producer = MergeProducer::with_spec(schema, self.batch_size, spec)?;
        // Aggregation pushdown (issue #841): when the ticket carries an
        // aggregation spec, the producer emits PARTIAL aggregate rows under the
        // partial schema instead of full rows.
        match &ticket.aggregation {
            Some(aggregation) => Ok(producer.with_aggregation(aggregation)?),
            None => Ok(producer),
        }
    }

    /// Arrow schema for a ticket (no SSTable access required).
    fn arrow_schema_for(&self, ticket: &FlightTicket) -> Result<ArrowSchema, Status> {
        Ok(self.build_producer(ticket)?.arrow_schema()?)
    }
}

#[tonic::async_trait]
impl FlightService for CqliteFlightService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let span = rpc_span("get_flight_info", &request);
        let mut metrics = RpcMetrics::start("get_flight_info");
        // Run the body AND finish() inside the RPC span so error/status recording
        // (record_status_error → Span::current()) tags THIS span, not whatever
        // span happens to be current after `.instrument` has completed.
        async {
            let result = self.get_flight_info_inner(request).await;
            finish(&mut metrics, result)
        }
        .instrument(span)
        .await
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let span = rpc_span("get_schema", &request);
        let mut metrics = RpcMetrics::start("get_schema");
        async {
            let result = self.get_schema_inner(request).await;
            finish(&mut metrics, result)
        }
        .instrument(span)
        .await
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let span = rpc_span("do_get", &request);
        let metrics = RpcMetrics::start("do_get");
        // The merge now STREAMS through a bounded channel (issue #1476): batches
        // reach the wire while it is still running, so the row/byte totals are not
        // known here. `metrics` moves INTO the response stream, which accumulates
        // per batch and records the terminal RPC counters when the stream ends
        // (including the emitted prefix for a cancelled stream). On a setup error
        // (before the stream exists) the metrics come back so we record the error
        // and close the RPC within this span.
        async move {
            match self.do_get_inner(request, metrics).await {
                Ok(response) => Ok(response),
                Err((status, metrics)) => {
                    crate::obs::record_status_error(&status);
                    drop(metrics);
                    Err(status)
                }
            }
        }
        .instrument(span)
        .await
    }

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let span = rpc_span("handshake", &request);
        let mut metrics = RpcMetrics::start("handshake");
        // Enter the span so finish()'s error recording tags THIS RPC span.
        span.in_scope(|| {
            finish(
                &mut metrics,
                Err(Status::unimplemented("handshake is not supported")),
            )
        })
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let span = rpc_span("list_flights", &request);
        let mut metrics = RpcMetrics::start("list_flights");
        span.in_scope(|| {
            finish(
                &mut metrics,
                Err(Status::unimplemented("list_flights is not yet supported")),
            )
        })
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        let span = rpc_span("poll_flight_info", &request);
        let mut metrics = RpcMetrics::start("poll_flight_info");
        span.in_scope(|| {
            finish(
                &mut metrics,
                Err(Status::unimplemented("poll_flight_info is not supported")),
            )
        })
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let span = rpc_span("do_put", &request);
        let mut metrics = RpcMetrics::start("do_put");
        span.in_scope(|| {
            finish(
                &mut metrics,
                Err(Status::unimplemented(
                    "do_put is not supported (read-only server)",
                )),
            )
        })
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let span = rpc_span("do_exchange", &request);
        let mut metrics = RpcMetrics::start("do_exchange");
        span.in_scope(|| {
            finish(
                &mut metrics,
                Err(Status::unimplemented("do_exchange is not supported")),
            )
        })
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let span = rpc_span("do_action", &request);
        let mut metrics = RpcMetrics::start("do_action");
        async {
            let result = self.do_action_inner(request).await;
            finish(&mut metrics, result)
        }
        .instrument(span)
        .await
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let span = rpc_span("list_actions", &request);
        let mut metrics = RpcMetrics::start("list_actions");
        span.in_scope(|| {
            // Advertise the one action this server supports (issue #944).
            let action = ActionType {
                r#type: TABLE_STATS_ACTION.to_string(),
                description: "Per-table aggregate statistics (Σ live_rows, Σ partition_count, \
                              SSTable count) for aggregation-pushdown planning."
                    .to_string(),
            };
            let stream: BoxStream<ActionType> =
                Box::pin(futures::stream::once(async move { Ok(action) }));
            finish(&mut metrics, Ok(Response::new(stream)))
        })
    }
}

/// Finalise a handler: on success, mark the metrics OK; on error, record the
/// error-rate signal (subsystem `flight`). `RpcMetrics` emits the request
/// counter, latency histogram, and in-flight gauge on drop, so returning the
/// result here (which drops `metrics` at the call site) closes the RPC.
fn finish<T>(metrics: &mut RpcMetrics, result: Result<T, Status>) -> Result<T, Status> {
    match &result {
        Ok(_) => metrics.ok(),
        Err(status) => crate::obs::record_status_error(status),
    }
    result
}

impl CqliteFlightService {
    /// Body of [`FlightService::get_flight_info`], run inside the RPC span.
    async fn get_flight_info_inner(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let ticket = FlightTicket::from_bytes(&descriptor.cmd)?;
        let arrow_schema = self.arrow_schema_for(&ticket)?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(descriptor.cmd.clone()));
        let info = FlightInfo::new()
            .try_with_schema(&arrow_schema)
            .map_err(|e| Status::internal(format!("schema encode: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);
        Ok(Response::new(info))
    }

    /// Body of [`FlightService::get_schema`], run inside the RPC span.
    async fn get_schema_inner(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let ticket = FlightTicket::from_bytes(&descriptor.cmd)?;
        let arrow_schema = self.arrow_schema_for(&ticket)?;

        let options = IpcWriteOptions::default();
        let schema_result: SchemaResult = SchemaAsIpc::new(&arrow_schema, &options)
            .try_into()
            .map_err(|e: arrow::error::ArrowError| Status::internal(e.to_string()))?;
        Ok(Response::new(schema_result))
    }

    /// Body of [`FlightService::do_get`], run inside the RPC span. Streams the
    /// merge through a bounded channel (issue #1476). On a setup error (before the
    /// response stream exists) `metrics` is returned so the caller can record the
    /// error; on success `metrics` moves into the stream (recorded at stream end).
    async fn do_get_inner(
        &self,
        request: Request<Ticket>,
        metrics: RpcMetrics,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, (Status, RpcMetrics)> {
        // Cancellation (issue #1476, roborev F1): pre-change, the single merge
        // `spawn_blocking` was covered by a `CancelGuard` held across its whole
        // `.await`, so a client disconnect during that call stopped the work. The
        // streaming rewrite splits eager setup (path discovery/token-prune, which
        // reads a `Summary.db` per SSTable and can be slow over many of them) from
        // the merge stage; ONE `CancelFlag` now spans both so a disconnect during
        // EITHER phase stops it — `setup_guard` covers the setup await (disarmed
        // once setup succeeds; a still-armed guard cancels on this future's drop),
        // and the SAME flag hands off to the merge stage under a fresh guard
        // (`spawn_streaming`/`build_aggregate_response`) covering the stream's
        // lifetime, exactly as before this fix.
        let cancel = CancelFlag::new();
        let mut setup_guard = cancel.drop_guard();
        // Fallible eager setup: parse the ticket, build the producer + schema, and
        // resolve/token-prune the SSTable paths off the reactor. A missing table
        // surfaces here as a clean `not_found` BEFORE the stream opens.
        let setup = match self.do_get_setup(request, &cancel).await {
            Ok(setup) => setup,
            Err(status) => return Err((status, metrics)),
        };
        setup_guard.disarm();
        let DoGetSetup {
            producer,
            schema_ref,
            paths,
            aggregating,
        } = setup;

        if aggregating {
            // Aggregate output is bounded (one row per group): keep materializing
            // and serve it as a stream, unchanged in content (issue #1476).
            return Ok(Response::new(
                crate::streaming::build_aggregate_response(
                    producer, paths, schema_ref, metrics, cancel,
                )
                .await?,
            ));
        }

        // Row path: the merge runs on the blocking pool and sends each batch into a
        // bounded channel; the response wraps the receiver. Peak resident payload
        // is O(channel capacity · batch_size), not O(result). The merge task handle
        // is detached (dropping it does not cancel `spawn_blocking`; a dropped
        // response stream cancels the merge cooperatively).
        let (stream, _merge_handle) = crate::streaming::spawn_streaming(
            producer,
            paths,
            schema_ref,
            metrics,
            crate::streaming::DO_GET_CHANNEL_CAPACITY,
            crate::streaming::StreamProbe::default(),
            cancel,
        );
        Ok(Response::new(stream))
    }

    /// Eager, fallible `do_get` setup shared by the row and aggregate paths: parse
    /// the ticket, build the producer + Arrow schema, and resolve + token-prune the
    /// SSTable paths off the async reactor (the prune reads `Summary.db` files).
    /// A missing table surfaces as `not_found` here, before any stream opens.
    ///
    /// `cancel` is polled inside the blocking discovery/prune task (issue #1476,
    /// roborev F1) so a client disconnect during this (potentially slow, many-
    /// SSTable) phase stops it instead of running to completion.
    async fn do_get_setup(
        &self,
        request: Request<Ticket>,
        cancel: &CancelFlag,
    ) -> Result<DoGetSetup, Status> {
        let ticket = FlightTicket::from_bytes(&request.into_inner().ticket)?;
        let producer = self.build_producer(&ticket)?;
        let schema_ref = Arc::new(producer.arrow_schema()?);
        let source = DirSource::resolve(
            &self.data_dir,
            &ticket.keyspace,
            &ticket.table,
            ticket.snapshot.as_deref(),
        )?;
        let aggregating = producer.is_aggregating();

        // Return the producer back out of the blocking task (it is moved in to run
        // the blocking discovery/prune, then reused for the merge) — avoids an Arc
        // and its Sync bound.
        let resolve_cancel = cancel.clone();
        let (producer, paths) = tokio::task::spawn_blocking(move || {
            let paths = producer.resolve_paths_cancellable(&source, &resolve_cancel);
            (producer, paths)
        })
        .await
        .map_err(|e| Status::internal(format!("path resolution panicked: {e}")))?;
        // A cancellation here surfaces as `ProducerError::Cancelled`, mapped by
        // `From<ProducerError> for Status` to `Status::aborted` — the same clean,
        // expected-abort class as a merge-stage cancellation.
        let paths = paths?;

        Ok(DoGetSetup {
            producer,
            schema_ref,
            paths,
            aggregating,
        })
    }

    /// Body of [`FlightService::do_action`], run inside the RPC span (issue #944).
    ///
    /// The only supported action is [`TABLE_STATS_ACTION`]: its body is a
    /// [`TableStatsRequest`] JSON, and the single emitted result is a
    /// [`TableStatsResponse`] JSON carrying the AUTHORITATIVE per-table sums
    /// (Σ live_rows, Σ partition_count, SSTable count) the Java connector uses to
    /// drive its AUTOMATIC aggregation-pushdown gate. Any other action type is
    /// `unimplemented`.
    async fn do_action_inner(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let action = request.into_inner();
        if action.r#type != TABLE_STATS_ACTION {
            return Err(Status::unimplemented(format!(
                "unsupported action type: {}",
                action.r#type
            )));
        }

        let req = TableStatsRequest::from_bytes(&action.body)
            .map_err(|e| Status::invalid_argument(format!("invalid table_stats request: {e}")))?;
        // Validate the path-bearing fields (issue #1430): a `table_stats` body is
        // the same class of attacker-controlled input as a Flight ticket.
        crate::pathsafe::validate_identifier("keyspace", &req.keyspace)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        crate::pathsafe::validate_identifier("table", &req.table)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        if let Some(snapshot) = &req.snapshot {
            crate::pathsafe::validate_snapshot(snapshot)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }
        let dir = DirSource::resolve(
            &self.data_dir,
            &req.keyspace,
            &req.table,
            req.snapshot.as_deref(),
        )?
        .into_dir();

        // `gather_table_stats` is synchronous blocking fs I/O (read_dir + read of
        // every Statistics.db). Run it off the async runtime so a table with many
        // SSTables / slow storage cannot stall unrelated Flight RPCs — mirroring the
        // `do_get` merge offload above. Outer `?` maps a task panic; inner `?` keeps
        // the `StatsError` -> `Status` mapping (`From<StatsError> for Status`).
        let stats = tokio::task::spawn_blocking(move || gather_table_stats(&dir))
            .await
            .map_err(|e| Status::internal(format!("table_stats task panicked: {e}")))??;
        let body = stats
            .to_bytes()
            .map_err(|e| Status::internal(format!("encode table_stats response: {e}")))?;

        let result = arrow_flight::Result { body: body.into() };
        let stream: BoxStream<arrow_flight::Result> =
            Box::pin(futures::stream::once(async move { Ok(result) }));
        Ok(Response::new(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{
        build_sstables, simple_schema, total_rows, write_row, KS, SIMPLE_DDL, TBL,
    };
    use arrow::array::Array;
    use arrow::record_batch::RecordBatch;
    use arrow_flight::decode::FlightRecordBatchStream;
    use arrow_flight::error::FlightError;
    use futures::StreamExt;

    fn ticket(keyspace: &str, table: &str) -> FlightTicket {
        FlightTicket {
            keyspace: keyspace.into(),
            table: table.into(),
            ddl: SIMPLE_DDL.into(),
            ..Default::default()
        }
    }

    fn cmd_descriptor(ticket: &FlightTicket) -> FlightDescriptor {
        FlightDescriptor::new_cmd(ticket.to_bytes().unwrap())
    }

    async fn decode(
        stream: <CqliteFlightService as FlightService>::DoGetStream,
    ) -> Vec<RecordBatch> {
        let mapped = stream.map(|r| r.map_err(|s| FlightError::ExternalError(Box::new(s))));
        let mut rb = FlightRecordBatchStream::new_from_flight_data(mapped);
        let mut out = Vec::new();
        while let Some(batch) = rb.next().await {
            out.push(batch.expect("decode batch"));
        }
        out
    }

    // Plain `#[test]`: `build_sstables` drives its own runtime to flush, so we
    // construct the data set first, then enter a fresh runtime to drive `do_get`.
    // Using `#[tokio::test]` here would nest runtimes and panic.
    #[test]
    fn do_get_streams_merged_rows() {
        let schema = simple_schema();
        // SSTable A: id=1 (old). SSTable B: id=1 (new, wins) + id=2.
        let (_temp, data_dir, _dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "old", 1, 100)],
                vec![write_row(1, "new", 2, 200), write_row(2, "b", 3, 200)],
            ],
        );
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let batches = rt.block_on(async {
            let bytes = ticket(KS, TBL).to_bytes().unwrap();
            let resp = svc
                .do_get(Request::new(Ticket::new(bytes)))
                .await
                .expect("do_get");
            decode(resp.into_inner()).await
        });

        assert_eq!(total_rows(&batches), 2, "two partitions after LWW merge");
        let names = batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let values: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
        assert!(values.contains(&"new"), "newer write wins, got {values:?}");
        assert!(!values.contains(&"old"));
    }

    #[tokio::test]
    async fn get_schema_returns_declared_columns() {
        // No SSTables needed — schema comes from the ticket DDL.
        let svc = CqliteFlightService::new(std::env::temp_dir(), 1024);
        let resp = svc
            .get_schema(Request::new(cmd_descriptor(&ticket(KS, TBL))))
            .await
            .expect("get_schema");
        let schema: ArrowSchema = (&resp.into_inner())
            .try_into()
            .expect("decode schema result");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name", "score"]);
    }

    #[tokio::test]
    async fn get_flight_info_carries_schema_and_endpoint() {
        let svc = CqliteFlightService::new(std::env::temp_dir(), 1024);
        let resp = svc
            .get_flight_info(Request::new(cmd_descriptor(&ticket(KS, TBL))))
            .await
            .expect("get_flight_info");
        let info = resp.into_inner();
        assert_eq!(info.endpoint.len(), 1, "one endpoint with the ticket");
        assert!(
            !info.endpoint[0].ticket.as_ref().unwrap().ticket.is_empty(),
            "endpoint carries a non-empty ticket"
        );
    }

    #[tokio::test]
    async fn invalid_ddl_is_invalid_argument() {
        let svc = CqliteFlightService::new(std::env::temp_dir(), 1024);
        let bad = FlightTicket {
            keyspace: KS.into(),
            table: TBL.into(),
            ddl: "this is not valid CQL".into(),
            ..Default::default()
        };
        let err = svc
            .get_schema(Request::new(cmd_descriptor(&bad)))
            .await
            .expect_err("invalid ddl must error");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn do_get_missing_table_is_not_found() {
        let schema = simple_schema();
        let (_temp, data_dir, _dir) =
            build_sstables(&schema, vec![vec![write_row(1, "x", 1, 100)]]);
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let result = rt.block_on(async {
            let bytes = ticket(KS, "missing_table").to_bytes().unwrap();
            svc.do_get(Request::new(Ticket::new(bytes))).await
        });
        let err = match result {
            Ok(_) => panic!("missing table must error"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::NotFound, "got: {err:?}");
    }

    // ---- Issue #1476 roborev F1: setup-phase cancellation ----------------------

    /// Pre-change, the single merge `spawn_blocking` was covered by a
    /// `CancelGuard` across its whole `.await`, so a disconnect during that call
    /// stopped the work. `do_get_setup`'s eager path discovery/token-prune phase
    /// (its OWN, separate `spawn_blocking`) must honor the SAME cancellation
    /// contract: a flag already cancelled when setup runs (the worst case of a
    /// disconnect firing the setup-phase `CancelGuard` at or before the very
    /// start of the blocking task) must stop resolution rather than run the
    /// discovery/prune to completion, surfacing as a clean `Aborted` — never a
    /// server fault and never a resolved path list.
    ///
    /// Uses a token-range ticket over multiple SSTables so an UNCANCELLED run
    /// would genuinely read every sibling `Summary.db` during the prune (see
    /// `prune_paths_cancellable`), proving this isn't a vacuous check against an
    /// empty/no-op path.
    #[test]
    fn do_get_setup_honors_cancellation_before_resolution() {
        let schema = simple_schema();
        let (_temp, data_dir, _dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "a", 1, 100), write_row(2, "b", 2, 100)],
                vec![write_row(3, "c", 3, 100), write_row(4, "d", 4, 100)],
            ],
        );
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let mut t = ticket(KS, TBL);
        // A concrete (full-ring) token range so `ScanSpec.token` is `Some(..)` and
        // `prune_paths_cancellable`'s per-SSTable loop actually runs.
        t.token_start = Some(i64::MIN);
        t.token_end = Some(i64::MAX);

        let result = rt.block_on(async {
            // Simulates the setup-phase `CancelGuard` having already fired (the
            // client disconnected) by the time the blocking resolution task runs.
            let cancel = CancelFlag::new();
            cancel.cancel();
            let bytes = t.to_bytes().unwrap();
            svc.do_get_setup(Request::new(Ticket::new(bytes)), &cancel)
                .await
        });
        let err = match result {
            Ok(_) => panic!("a cancelled setup must not resolve/return paths"),
            Err(e) => e,
        };
        assert_eq!(
            err.code(),
            tonic::Code::Aborted,
            "cancellation must surface as Aborted (ProducerError::Cancelled → Status::aborted), \
             got: {err:?}"
        );
    }

    /// Baseline for the test above: the SAME token-range ticket, WITHOUT
    /// cancellation, resolves normally — proving the cancelled case above is a
    /// genuine early stop, not just "this ticket always errors."
    #[test]
    fn do_get_setup_resolves_normally_without_cancellation() {
        let schema = simple_schema();
        let (_temp, data_dir, _dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "a", 1, 100), write_row(2, "b", 2, 100)],
                vec![write_row(3, "c", 3, 100), write_row(4, "d", 4, 100)],
            ],
        );
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let mut t = ticket(KS, TBL);
        t.token_start = Some(i64::MIN);
        t.token_end = Some(i64::MAX);

        let setup = rt.block_on(async {
            let cancel = CancelFlag::new();
            let bytes = t.to_bytes().unwrap();
            svc.do_get_setup(Request::new(Ticket::new(bytes)), &cancel)
                .await
        });
        let setup = setup.expect("uncancelled setup resolves");
        assert_eq!(
            setup.paths.len(),
            2,
            "a full-ring token filter keeps both SSTables"
        );
    }

    // ---- Issue #1430: end-to-end path-traversal rejection (wiring evidence) ----

    #[test]
    fn do_get_rejects_absolute_snapshot_traversal() {
        // A legit data dir that a valid `do_get` would serve from.
        let schema = simple_schema();
        let (_temp, data_dir, _dir) =
            build_sstables(&schema, vec![vec![write_row(1, "x", 1, 100)]]);

        // A "secret" directory OUTSIDE data_dir holding a Data.db an attacker
        // wants to disclose.
        let secret = tempfile::TempDir::new().unwrap();
        std::fs::write(secret.path().join("nb-1-big-Data.db"), b"top secret").unwrap();

        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        // snapshot = absolute path to the secret dir. Without the guard,
        // `Path::join` with an absolute component would escape data_dir entirely.
        let mut t = ticket(KS, TBL);
        t.snapshot = Some(secret.path().to_string_lossy().into_owned());

        let result = rt.block_on(async {
            let bytes = t.to_bytes().unwrap();
            svc.do_get(Request::new(Ticket::new(bytes))).await
        });
        let err = match result {
            Ok(_) => panic!("path-traversal ticket must be rejected, not served"),
            Err(e) => e,
        };
        // Parse-time charset validation rejects the absolute snapshot path.
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "absolute snapshot must be rejected as invalid argument, got: {err:?}"
        );
    }

    #[test]
    fn do_get_rejects_parent_traversal_keyspace() {
        let schema = simple_schema();
        let (_temp, data_dir, _dir) =
            build_sstables(&schema, vec![vec![write_row(1, "x", 1, 100)]]);
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        // keyspace with `../` must be rejected before any filesystem access.
        let t = ticket("../secret", TBL);
        let result = rt.block_on(async {
            let bytes = t.to_bytes().unwrap();
            svc.do_get(Request::new(Ticket::new(bytes))).await
        });
        let err = match result {
            Ok(_) => panic!("`../` keyspace must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "got: {err:?}");
    }

    #[test]
    fn do_get_applies_predicate_pushdown() {
        use crate::ticket::{Predicate, PredicateOp};
        let schema = simple_schema();
        let rows = (1..=5)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100)) // scores 10..50
            .collect::<Vec<_>>();
        let (_temp, data_dir, _dir) = build_sstables(&schema, vec![rows]);
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let mut t = ticket(KS, TBL);
        t.predicates = vec![Predicate {
            column: "score".into(),
            op: PredicateOp::Gte,
            value: serde_json::json!(40),
        }];

        let batches = rt.block_on(async {
            let resp = svc
                .do_get(Request::new(Ticket::new(t.to_bytes().unwrap())))
                .await
                .expect("do_get");
            decode(resp.into_inner()).await
        });
        // score >= 40 → 40, 50.
        assert_eq!(total_rows(&batches), 2);
    }

    #[tokio::test]
    async fn do_get_unknown_predicate_column_is_invalid_argument() {
        use crate::ticket::{Predicate, PredicateOp};
        let svc = CqliteFlightService::new(std::env::temp_dir(), 1024);
        let mut t = ticket(KS, TBL);
        t.predicates = vec![Predicate {
            column: "nonexistent".into(),
            op: PredicateOp::Equal,
            value: serde_json::json!(1),
        }];
        // get_schema also runs the ticket through build_producer → ScanSpec.
        let err = svc
            .get_schema(Request::new(cmd_descriptor(&t)))
            .await
            .expect_err("unknown predicate column must error");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn do_action_table_stats_returns_one_result_per_table() {
        use crate::stats::{TableStatsRequest, TableStatsResponse, TABLE_STATS_ACTION};
        let schema = simple_schema();
        // Two write-engine SSTables. Their StatisticsWriter emits empty estimated
        // histograms, so the authoritative counts assertion lives in the
        // dataset-backed stats unit test; here we assert the action plumbing:
        // one Result, decodable, with the SSTable count.
        let (_temp, data_dir, _dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "a", 1, 100), write_row(2, "b", 2, 100)],
                vec![write_row(1, "a2", 3, 200), write_row(3, "c", 4, 200)],
            ],
        );
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let resp = rt.block_on(async {
            let req = TableStatsRequest {
                keyspace: KS.into(),
                table: TBL.into(),
                snapshot: None,
            };
            let action = Action {
                r#type: TABLE_STATS_ACTION.into(),
                body: req.to_bytes().unwrap().into(),
            };
            let mut stream = svc
                .do_action(Request::new(action))
                .await
                .expect("do_action")
                .into_inner();
            let first = stream.next().await.expect("one result").expect("ok");
            assert!(stream.next().await.is_none(), "exactly one Result");
            TableStatsResponse::from_bytes(&first.body).expect("decode")
        });

        assert_eq!(resp.sstable_count, 2);
    }

    #[tokio::test]
    async fn do_action_unknown_type_is_unimplemented() {
        let svc = CqliteFlightService::new(std::env::temp_dir(), 1024);
        let action = Action {
            r#type: "not_a_real_action".into(),
            body: Vec::new().into(),
        };
        let err = match svc.do_action(Request::new(action)).await {
            Ok(_) => panic!("unknown action must error"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn do_action_table_stats_missing_table_is_not_found() {
        use crate::stats::{TableStatsRequest, TABLE_STATS_ACTION};
        let svc = CqliteFlightService::new(std::env::temp_dir(), 1024);
        let req = TableStatsRequest {
            keyspace: "no_such_ks".into(),
            table: "no_such_table".into(),
            snapshot: None,
        };
        let action = Action {
            r#type: TABLE_STATS_ACTION.into(),
            body: req.to_bytes().unwrap().into(),
        };
        let err = match svc.do_action(Request::new(action)).await {
            Ok(_) => panic!("missing table must error"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::NotFound, "got: {err:?}");
    }

    #[tokio::test]
    async fn list_actions_advertises_table_stats() {
        use crate::stats::TABLE_STATS_ACTION;
        let svc = CqliteFlightService::new(std::env::temp_dir(), 1024);
        let mut stream = svc
            .list_actions(Request::new(Empty {}))
            .await
            .expect("list_actions")
            .into_inner();
        let action = stream.next().await.expect("one action").expect("ok");
        assert_eq!(action.r#type, TABLE_STATS_ACTION);
        assert!(stream.next().await.is_none(), "only one action advertised");
    }

    #[test]
    fn do_get_empty_table_emits_schema_only() {
        // An existing table directory with no SSTables → valid empty result that
        // still carries the Arrow schema (via FlightDataEncoder `with_schema`).
        let temp = tempfile::TempDir::new().unwrap();
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(data_dir.join(KS).join(TBL)).unwrap();
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let (schema, batches) = rt.block_on(async {
            let bytes = ticket(KS, TBL).to_bytes().unwrap();
            let resp = svc
                .do_get(Request::new(Ticket::new(bytes)))
                .await
                .expect("do_get");
            let mapped = resp
                .into_inner()
                .map(|r| r.map_err(|s| FlightError::ExternalError(Box::new(s))));
            let mut rb = FlightRecordBatchStream::new_from_flight_data(mapped);
            let mut out = Vec::new();
            while let Some(b) = rb.next().await {
                out.push(b.expect("decode"));
            }
            let schema = rb.schema().cloned();
            (schema, out)
        });

        assert_eq!(total_rows(&batches), 0, "no rows for an empty table");
        let schema = schema.expect("schema must be present even when empty");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name", "score"]);
    }
}
