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

use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

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

use cqlite_core::storage::sstable::reader::SSTableReader;

use crate::admission::{Admission, AdmissionConfig};
use crate::cancel::CancelFlag;
use crate::filter::{FilterError, ScanSpec};
use crate::obs::{rpc_span, RpcMetrics};
use crate::producer::{DirSource, MergeProducer, ProducerError};
use crate::stats::{gather_table_stats, StatsError, TableStatsRequest, TABLE_STATS_ACTION};
use crate::ticket::{FlightTicket, TicketError};
use crate::warm::{ddl_hash, TableKey, WarmError, WarmMetricsSnapshot, WarmTableRegistry};

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

/// Map a warm-handle failure (issue #2310) to a gRPC status, mirroring the
/// producer error mapping: a cancellation is a clean `aborted`; a probe that
/// hit a missing directory is `not_found` (the same class as a missing table on
/// the cold path); an open/parse failure during a fail-closed rebuild (e.g. a
/// corrupt `Statistics.db`, #1626) or a runtime failure is an `internal` fault.
fn warm_error_to_status(e: WarmError) -> Status {
    let msg = e.to_string();
    match e {
        WarmError::Cancelled => Status::aborted(msg),
        WarmError::Probe { source, .. } if source.kind() == std::io::ErrorKind::NotFound => {
            Status::not_found(msg)
        }
        WarmError::Probe { .. }
        | WarmError::ProbeEntry { .. }
        | WarmError::Open { .. }
        | WarmError::Runtime(_) => Status::internal(msg),
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

/// The merge input the eager setup resolved for a `do_get`: the WARM,
/// already-open reader set for the common row path (issue #2310), or the cold
/// token-pruned paths for the aggregate route (which still opens fresh readers —
/// no regression, and a bounded per-group output).
enum DoGetInput {
    /// Aggregate route: cold token-pruned `Data.db` paths.
    Aggregate(Vec<PathBuf>),
    /// Row/point route: a warm reader set from the [`WarmTableRegistry`].
    Rows(Vec<Arc<SSTableReader>>),
}

/// Resolved, ready-to-serve `do_get` inputs produced by the eager setup step
/// (issue #1476): the built producer, its Arrow schema, and the merge input
/// (warm readers or cold aggregate paths). Kept together so the row and
/// aggregate response builders share one setup path.
struct DoGetSetup {
    producer: MergeProducer,
    schema_ref: Arc<ArrowSchema>,
    input: DoGetInput,
}

/// Cross-request setup caches (spec Requirement 8): the schema PARSE is the one
/// per-request `do_get`-setup cost that survives a warm reader-cache hit unless
/// memoized. Keyed on the authoritative, pre-validated DDL string and shared
/// (`Arc`) across the `Clone`d per-RPC handles.
///
/// The directory RESOLVE is deliberately NOT cached (roborev 1639, issue #2310
/// finding 2 round 3): a naive path-string cache is unsound. A recreated table's
/// OLD `<table>-<uuid>` dir commonly still exists on disk (Cassandra/the sidecar
/// doesn't always remove it atomically with the new one landing), so a cheap
/// `is_dir()` revalidation of a cached entry PASSES on the stale pin while a
/// newer sibling dir is the authoritative resolution — and a cached base whose
/// leaf later becomes a symlink swap evades the cold path's resolve-time
/// containment check entirely. A correct revalidation (list the keyspace dir,
/// pick the lexicographically-largest match, re-run containment) costs the same
/// as a fresh resolve, so the cache buys nothing but the unsoundness: `resolve_dir`
/// below re-resolves authoritatively EVERY request, exactly the pre-PR cold
/// posture. This is not a Req-8 regression: Req 8's "approximately zero" covers
/// PARSE work (schema parse, reader open, index/summary/bloom parse) — the
/// directory resolve is the same cost class (one `read_dir`) as the per-request
/// authoritative generation-set probe that spec Requirement 2 itself mandates on
/// every request, so paying it every request was always the design, not a gap.
#[derive(Default)]
struct SetupCaches {
    /// Parsed schema per exact DDL string. Keyed on the full DDL (never a hash —
    /// a hash collision would serve the WRONG schema and corrupt decode). Bounded
    /// in practice by the number of distinct table DDLs queried.
    schemas: Mutex<HashMap<String, Arc<TableSchema>>>,
    /// Work-done probe: schema PARSES actually performed (a cache hit adds 0).
    schema_parses: AtomicU64,
    /// Work-done probe: directory RESOLVES actually performed. Always increments
    /// once per request (live or snapshot mode) — the resolve is authoritative
    /// per-request by design, never elided (see the struct doc above).
    resolves: AtomicU64,
}

/// A point-in-time read of the `do_get`-setup work counters, for the warm-hit
/// elision tests (spec Requirement 8) and the #2289/#1494 bench harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetupWorkSnapshot {
    /// Schema parses performed (CQL DDL → `TableSchema`).
    pub schema_parses: u64,
    /// Directory resolves performed (`DirSource::resolve`).
    pub resolves: u64,
}

/// Flight service over a node-local SSTable data directory.
#[derive(Clone)]
pub struct CqliteFlightService {
    /// Root holding `<keyspace>/<table>[-<uuid>]/` SSTable directories.
    data_dir: PathBuf,
    /// Max rows per emitted Arrow record batch.
    batch_size: usize,
    /// Cross-request warm parse cache (issue #2310): generation-keyed open
    /// readers so a repeated query on unchanged data pays ~0 reader-open/parse.
    /// Shared (`Arc`) across the `Clone`d per-RPC service handles.
    warm: Arc<WarmTableRegistry>,
    /// Cross-request schema-parse + directory-resolve caches (spec Req 8).
    caches: Arc<SetupCaches>,
    /// `do_get` admission ceiling (issue #2420, WS4): bounds concurrently
    /// admitted scans to a configured `K`, acquired before any SSTable open.
    /// Shared (`Arc` inside [`Admission`]) across the `Clone`d per-RPC handles so
    /// every request throttles against the same permit pool.
    admission: Admission,
}

impl CqliteFlightService {
    /// Create a service serving SSTables under `data_dir`, with admission control
    /// configured from the environment (see [`AdmissionConfig::from_env`]).
    pub fn new(data_dir: impl Into<PathBuf>, batch_size: usize) -> Self {
        Self::with_admission(
            data_dir,
            batch_size,
            Admission::new(AdmissionConfig::from_env()),
        )
    }

    /// Create a service with an explicit [`Admission`] ceiling — the wiring point
    /// for the `--max-concurrent-scans` CLI knob (`main`) and the deterministic
    /// admission tests (which supply a tiny `K` + injectable wait timeout).
    pub fn with_admission(
        data_dir: impl Into<PathBuf>,
        batch_size: usize,
        admission: Admission,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            batch_size: batch_size.max(1),
            warm: Arc::new(WarmTableRegistry::new()),
            caches: Arc::new(SetupCaches::default()),
            admission,
        }
    }

    /// The service's admission ceiling (issue #2420) — for `main` (to log the
    /// configured limit) and the admission tests (to hold permits / read the
    /// counters).
    pub fn admission(&self) -> &Admission {
        &self.admission
    }

    /// A point-in-time read of the warm-cache counters (hit/miss/evict/
    /// refresh-outcome + the reader-open work probe) for the #2289/#1494 bench
    /// harness and end-to-end warm-behavior tests (issue #2310).
    pub fn warm_metrics(&self) -> WarmMetricsSnapshot {
        self.warm.metrics().snapshot()
    }

    /// A point-in-time read of the `do_get`-setup work counters (schema parses +
    /// directory resolves) — the spec Req 8 elision probe (issue #2310).
    pub fn setup_work(&self) -> SetupWorkSnapshot {
        SetupWorkSnapshot {
            schema_parses: self.caches.schema_parses.load(Ordering::Relaxed),
            resolves: self.caches.resolves.load(Ordering::Relaxed),
        }
    }

    /// Parse the table schema from the ticket's CQL DDL.
    fn parse_schema(ticket: &FlightTicket) -> Result<TableSchema, Status> {
        parse_cql_schema(&ticket.ddl)
            .map_err(|e| Status::invalid_argument(format!("invalid ddl: {e}")))
    }

    /// The parsed schema for a ticket, reusing a cached parse for a repeat DDL
    /// (spec Req 8: schema parse elided on a warm hit). The CQL parse runs OUTSIDE
    /// the cache lock; a rare concurrent first-parse of the same DDL just parses
    /// twice (both correct), never holds the lock across the parse.
    fn cached_schema(&self, ticket: &FlightTicket) -> Result<Arc<TableSchema>, Status> {
        if let Some(hit) = self
            .caches
            .schemas
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&ticket.ddl)
        {
            return Ok(Arc::clone(hit));
        }
        let schema = Arc::new(Self::parse_schema(ticket)?);
        self.caches.schema_parses.fetch_add(1, Ordering::Relaxed);
        self.caches
            .schemas
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(ticket.ddl.clone())
            .or_insert_with(|| Arc::clone(&schema));
        Ok(schema)
    }

    /// Resolve the SSTable directory for a ticket: the authoritative
    /// `DirSource::resolve` (containment-checked, issue #1430), run EVERY
    /// request — never cached (roborev 1639, issue #2310 finding 2 round 3; see
    /// [`SetupCaches`] for why a directory-resolve cache is unsound). This
    /// mirrors the pre-PR cold posture exactly.
    fn resolve_dir(&self, ticket: &FlightTicket) -> Result<PathBuf, Status> {
        let dir = DirSource::resolve(
            &self.data_dir,
            &ticket.keyspace,
            &ticket.table,
            ticket.snapshot.as_deref(),
        )?
        .into_dir();
        self.caches.resolves.fetch_add(1, Ordering::Relaxed);
        Ok(dir)
    }

    /// Build a producer for a ticket, applying its token-range/predicate/projection
    /// filters. Used by every RPC so the Arrow schema reflects the projection.
    fn build_producer(&self, ticket: &FlightTicket) -> Result<MergeProducer, Status> {
        let schema = self.cached_schema(ticket)?;
        let spec = ScanSpec::from_ticket(ticket, &schema)?;
        let producer = MergeProducer::with_spec((*schema).clone(), self.batch_size, spec)?;
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
        // Cheap, filesystem-free request validation FIRST — BEFORE admission
        // (roborev-1698): parse the ticket bytes and build the producer + Arrow
        // schema (schema-cache lookup, predicate/projection/aggregation-spec
        // construction). None of this touches the filesystem, so it must not wait
        // behind the admission semaphore and must not consume a permit — an
        // invalid ticket can never succeed no matter how many times it is
        // retried, so it fails with its OWN status (`invalid_argument` /
        // `not_found` from ticket/schema/predicate parsing) immediately, never a
        // permit-consuming `UNAVAILABLE` that would make the connector
        // failover-retry an unsatisfiable request into a poison retry storm.
        let (ticket, producer, schema_ref) = match self.validate_do_get_ticket(request) {
            Ok(v) => v,
            Err(status) => return Err((status, metrics)),
        };
        // Admission control (issue #2420, WS4): acquire a permit immediately
        // before any filesystem access (directory resolve / SSTable open) or
        // batch production, so `K` bounds concurrent scans ahead of
        // blocking-pool/fd/memory pressure. On saturation this waits a bounded
        // timeout, then sheds with `UNAVAILABLE` (retry-safe for the #2241
        // connector failover) — never `RESOURCE_EXHAUSTED`, and always before the
        // first batch. A request cancelled WHILE waiting drops this future and
        // never acquires a permit. The returned permit is held for the scan's
        // lifetime and moved into the response stream below (`admitted_stream`), so
        // completion/disconnect/cancel all release it via one RAII drop.
        let permit = match self.admission.acquire().await {
            Ok(permit) => permit,
            Err(status) => return Err((status, metrics)),
        };
        let cancel = CancelFlag::new();
        let mut setup_guard = cancel.drop_guard();
        // Phase timing (issue #2162): begin in the `resolve` phase. The timer
        // captures the `flight.do_get` span (this future runs under it via
        // `.instrument`), so its per-phase histogram samples + span events attach
        // to that span even for the phases that run on the blocking merge pool. On
        // the setup-error path below the timer drops here, recording the `resolve`
        // phase it died in — so a stall that never produces a row still localizes.
        let mut timer = crate::obs::PhaseTimer::start("do_get");
        // Fallible eager resolve: resolve the table's on-disk directory and
        // token-prune the SSTable paths off the reactor (the ticket/producer/
        // schema are already validated above). A missing table surfaces here as a
        // clean `not_found` BEFORE the stream opens.
        let setup = match self
            .do_get_resolve(ticket, producer, schema_ref, &cancel)
            .await
        {
            Ok(setup) => setup,
            Err(status) => return Err((status, metrics)),
        };
        setup_guard.disarm();
        // `resolve` done; enter `merge_setup` (opening SSTables + building the
        // merger — the #2157 stall suspect — happens next, before the first batch).
        timer.transition(crate::obs::PHASE_MERGE_SETUP);
        let DoGetSetup {
            producer,
            schema_ref,
            input,
        } = setup;

        let stream: <Self as FlightService>::DoGetStream = match input {
            // Aggregate output is bounded (one row per group): keep materializing
            // and serve it as a stream, unchanged in content (issue #1476).
            DoGetInput::Aggregate(paths) => {
                crate::streaming::build_aggregate_response(
                    producer, paths, schema_ref, metrics, cancel, timer,
                )
                .await?
            }
            // Row/point path (issue #2310): drive the merge over the WARM,
            // already-open reader set. The merge runs on the blocking pool and
            // sends each batch into a bounded channel; peak resident payload is
            // O(channel capacity · batch_size), not O(result). The merge task
            // handle is detached (a dropped response stream cancels the merge
            // cooperatively).
            DoGetInput::Rows(readers) => {
                let (stream, _merge_handle) = crate::streaming::spawn_streaming_from_readers(
                    producer,
                    readers,
                    schema_ref,
                    metrics,
                    crate::streaming::DO_GET_CHANNEL_CAPACITY,
                    crate::streaming::StreamProbe::default(),
                    cancel,
                    timer,
                );
                stream
            }
        };
        // Hold the admission permit for the scan's lifetime: the wrapper is a
        // transparent pass-through that drops the permit (releasing admission) when
        // the stream completes or the client disconnects (issue #2420).
        Ok(Response::new(crate::admission::admitted_stream(
            stream, permit,
        )))
    }

    /// Cheap, filesystem-free `do_get` request validation (roborev-1698): parse
    /// the ticket bytes and build the producer + Arrow schema (schema-cache
    /// lookup, predicate/projection/aggregation-spec construction). Touches NO
    /// filesystem, so [`Self::do_get_inner`] runs this BEFORE acquiring an
    /// admission permit — an invalid ticket can never succeed regardless of how
    /// many times it is retried, so it must fail with its own status
    /// immediately: never wait behind the admission semaphore, never consume a
    /// permit, never surface as a retry-inviting `UNAVAILABLE`.
    fn validate_do_get_ticket(
        &self,
        request: Request<Ticket>,
    ) -> Result<(FlightTicket, MergeProducer, Arc<ArrowSchema>), Status> {
        let ticket = FlightTicket::from_bytes(&request.into_inner().ticket)?;
        let producer = self.build_producer(&ticket)?;
        let schema_ref = Arc::new(producer.arrow_schema()?);
        Ok((ticket, producer, schema_ref))
    }

    /// Eager, fallible `do_get` filesystem resolve shared by the row and
    /// aggregate paths, run AFTER admission (issue #2420): resolve the table's
    /// on-disk directory and token-prune the SSTable paths for an already
    /// validated `ticket`/`producer`/`schema_ref` (see
    /// [`Self::validate_do_get_ticket`]). A missing table surfaces as `not_found`
    /// here, before any stream opens.
    ///
    /// `DirSource::resolve` (filesystem `is_dir`/`read_dir`, including the
    /// Cassandra `<table>-<uuid>` layout scan) and the token-prune (reads a
    /// sibling `Summary.db` per SSTable) run in ONE `spawn_blocking` closure
    /// (issue #1476, roborev round 3) — letting filesystem access run on the
    /// async request task instead would stall the gRPC reactor for unrelated
    /// RPCs under slow/busy storage. `cancel` is polled inside this same closure
    /// (issue #1476, roborev F1) so a client disconnect during resolve stops it
    /// instead of running to completion; `do_get_inner`'s `CancelGuard` still
    /// covers this whole `.await` for the future-drop case.
    async fn do_get_resolve(
        &self,
        ticket: FlightTicket,
        producer: MergeProducer,
        schema_ref: Arc<ArrowSchema>,
        cancel: &CancelFlag,
    ) -> Result<DoGetSetup, Status> {
        let svc = self.clone();
        let resolve_cancel = cancel.clone();

        tokio::task::spawn_blocking(move || -> Result<DoGetSetup, Status> {
            // Spec Req 8: reuse a cached LIVE-mode resolution on a warm hit instead
            // of re-running `DirSource::resolve` every request.
            let dir = svc.resolve_dir(&ticket)?;

            // Aggregate route keeps the cold path (bounded per-group output, no
            // per-request reader-open regression): resolve + token-prune paths.
            // A cancellation surfaces as `ProducerError::Cancelled` → `aborted`.
            if producer.is_aggregating() {
                let source = DirSource::new(dir.clone());
                let paths = producer.resolve_paths_cancellable(&source, &resolve_cancel)?;
                return Ok(DoGetSetup {
                    producer,
                    schema_ref,
                    input: DoGetInput::Aggregate(paths),
                });
            }

            // Row/point route (issue #2310): obtain the WARM reader set. The
            // registry probes the generation set (authoritative listing /
            // snapshot manifest fast path) and serves cached readers on an
            // unchanged set (zero reader-open/parse), or fail-closed rebuilds
            // only the delta. Cancellation is honored inside `warm_readers`.
            let key = TableKey::new(&ticket.keyspace, &ticket.table);
            let warm = svc
                .warm
                .warm_readers(
                    &key,
                    ddl_hash(&ticket.ddl),
                    &producer.schema,
                    &dir,
                    ticket.snapshot.as_deref(),
                    &resolve_cancel,
                )
                .map_err(warm_error_to_status)?;
            Ok(DoGetSetup {
                producer,
                schema_ref,
                input: DoGetInput::Rows(warm.readers),
            })
        })
        .await
        .map_err(|e| Status::internal(format!("do_get setup panicked: {e}")))?
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
        build_sstables, make_snapshot, simple_schema, total_rows, write_row, KS, SIMPLE_DDL, TBL,
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

    /// The `name` column values across all batches, sorted — a stable, order-
    /// independent value fingerprint for asserting two responses are row-equal.
    fn sorted_name_values(batches: &[RecordBatch]) -> Vec<String> {
        let mut out = Vec::new();
        for b in batches {
            let names = b
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for i in 0..names.len() {
                out.push(names.value(i).to_string());
            }
        }
        out.sort();
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

    // ---- Issue #2310: warm-handle wiring evidence through the do_get surface ----

    /// THE wiring-evidence test (spec Requirements 1/2/6/8): two `do_get`s for the
    /// same table over an unchanged generation set. The FIRST warms the cache
    /// (miss + reader opens); the SECOND is a warm HIT that performs ZERO further
    /// reader-open/parse (the work-done probe), and returns byte-identical rows.
    /// Drives the real public `FlightService::do_get` surface end to end. Fails on
    /// pre-#2310 `main` (no warm state → every request re-opens readers).
    #[test]
    fn do_get_second_request_is_a_warm_hit_with_zero_reader_opens() {
        let schema = simple_schema();
        let (_temp, data_dir, _dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "old", 1, 100)],
                vec![write_row(1, "new", 2, 200), write_row(2, "b", 3, 200)],
            ],
        );
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let (vals1, vals2, opens_after_first) = rt.block_on(async {
            let bytes = ticket(KS, TBL).to_bytes().unwrap();
            let r1 = svc
                .do_get(Request::new(Ticket::new(bytes.clone())))
                .await
                .expect("first do_get");
            let vals1 = sorted_name_values(&decode(r1.into_inner()).await);
            // Work-done probe checkpoint: opens charged BY the (cold) first request.
            let opens_after_first = svc.warm_metrics().reader_opens;
            let r2 = svc
                .do_get(Request::new(Ticket::new(bytes)))
                .await
                .expect("second do_get");
            let vals2 = sorted_name_values(&decode(r2.into_inner()).await);
            (vals1, vals2, opens_after_first)
        });

        assert_eq!(vals1.len(), 2, "two partitions after LWW merge");
        // Value equality (not just row count): the warm hit returns the SAME rows.
        assert_eq!(
            vals2, vals1,
            "warm hit returns value-identical rows, got {vals2:?} vs {vals1:?}"
        );
        assert!(vals1.contains(&"new".to_string()), "newer write wins");

        let m = svc.warm_metrics();
        assert_eq!(m.misses, 1, "exactly one cold build (first request)");
        assert_eq!(m.hits, 1, "exactly one warm hit (second request)");
        assert_eq!(
            m.refresh_rebuilt_delta, 1,
            "the first request recorded a delta rebuild"
        );
        assert_eq!(
            m.refresh_unchanged, 1,
            "the second request recorded an unchanged refresh"
        );
        assert!(
            opens_after_first >= 2,
            "the first request opened both generations' readers, got {opens_after_first}"
        );
        // THE work-done probe: the warm hit opened ZERO further readers — the
        // cumulative open count is UNCHANGED across the second request.
        assert_eq!(
            m.reader_opens, opens_after_first,
            "the warm hit performed zero reader-open/parse (spec Requirement 2)"
        );
    }

    /// Spec Req 8 elision probe (updated round 3, roborev 1639): two IDENTICAL
    /// live-mode `do_get`s re-parse the schema ZERO times on the second request —
    /// PARSE work is what Req 8 covers. The directory RESOLVE is deliberately
    /// authoritative EVERY request (never elided): it increments on both the
    /// first AND second request. See [`SetupCaches`] for why caching it is
    /// unsound (a recreated table's old `<table>-<uuid>` dir commonly still
    /// exists, so a cheap existence re-check cannot distinguish a stale pin from
    /// a live one; and a cached base evades resolve-time containment on a
    /// symlink swap). (The end-to-end latency win the parse-elision counters
    /// underwrite is measured downstream by the #2289/#1494 bench harness.)
    #[test]
    fn do_get_warm_hit_elides_schema_parse_not_dir_resolve() {
        let schema = simple_schema();
        let (_temp, data_dir, _dir) =
            build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let bytes = ticket(KS, TBL).to_bytes().unwrap();
        rt.block_on(async {
            let r1 = svc
                .do_get(Request::new(Ticket::new(bytes.clone())))
                .await
                .expect("first do_get");
            let _ = decode(r1.into_inner()).await;
        });
        let after_first = svc.setup_work();
        assert_eq!(after_first.schema_parses, 1, "first request parses once");
        assert_eq!(after_first.resolves, 1, "first request resolves once");

        rt.block_on(async {
            let r2 = svc
                .do_get(Request::new(Ticket::new(bytes)))
                .await
                .expect("second do_get");
            let _ = decode(r2.into_inner()).await;
        });
        let after_second = svc.setup_work();
        assert_eq!(
            after_second.schema_parses, 1,
            "the warm second request re-parses the schema ZERO times (spec Req 8)"
        );
        assert_eq!(
            after_second.resolves, 2,
            "the directory resolve is authoritative EVERY request, by design \
             (roborev 1639) — it must NOT be elided on the warm second request"
        );
    }

    /// Finding 2 round 3 (#2310, roborev 1639): `resolve_dir` must be
    /// authoritative EVERY request, even when a table's OLD `<table>-<uuid>` dir
    /// is still present on disk (the common case: a recreate does not always
    /// remove the old dir atomically with the new one landing). Red on the
    /// round-2 cached code: the first resolve pins `items-aaaaaaaa`; a cheap
    /// `is_dir()` revalidation of that pin PASSES (the old dir was never
    /// removed), so the cached code serves the STALE `aaaaaaaa` dir forever
    /// instead of the lexicographically-larger, authoritative `bbbbbbbb`.
    #[test]
    fn resolve_dir_is_authoritative_every_request_even_when_old_dir_still_exists() {
        let temp = tempfile::TempDir::new().unwrap();
        let data_dir = temp.path().join("data");
        let ks_dir = data_dir.join(KS);
        let first = ks_dir.join(format!("{TBL}-aaaaaaaa"));
        std::fs::create_dir_all(&first).unwrap();
        let svc = CqliteFlightService::new(data_dir, 1024);
        let t = FlightTicket {
            keyspace: KS.into(),
            table: TBL.into(),
            ddl: SIMPLE_DDL.into(),
            ..Default::default()
        };

        let r1 = svc.resolve_dir(&t).expect("first resolve");
        assert_eq!(r1, first, "first resolve picks the only table-<uuid> dir");

        // The OLD dir is left in place (NOT removed) — the common recreate case
        // a naive existence-only revalidation cannot distinguish from "still
        // live". A newer, lexicographically-larger sibling appears alongside it.
        let second = ks_dir.join(format!("{TBL}-bbbbbbbb"));
        std::fs::create_dir_all(&second).unwrap();

        let r2 = svc.resolve_dir(&t).expect("second resolve");
        assert_eq!(
            r2, second,
            "resolve must be authoritative every request — the still-present old \
             dir must NOT be served over the newer, correct resolution"
        );
    }

    /// Sanity: a table with no dir on disk yet, resolved once, then created —
    /// the next resolve must reach the real dir (basic cold-resolve behavior,
    /// no caching involved after roborev 1639's removal of the live-dir cache).
    #[test]
    fn resolve_reaches_newly_created_table_dir_after_being_absent() {
        let temp = tempfile::TempDir::new().unwrap();
        let data_dir = temp.path().join("data");
        let ks_dir = data_dir.join(KS);
        std::fs::create_dir_all(&ks_dir).unwrap(); // keyspace exists, table does not
        let svc = CqliteFlightService::new(data_dir, 1024);
        let t = FlightTicket {
            keyspace: KS.into(),
            table: TBL.into(),
            ddl: SIMPLE_DDL.into(),
            ..Default::default()
        };

        let r1 = svc.resolve_dir(&t).expect("first resolve");
        assert!(!r1.is_dir(), "no table dir on disk yet, got {r1:?}");

        let real = ks_dir.join(format!("{TBL}-cccccccc"));
        std::fs::create_dir_all(&real).unwrap();

        let r2 = svc.resolve_dir(&t).expect("second resolve");
        assert_eq!(r2, real, "the second resolve must reach the now-real dir");
    }

    /// Snapshot-mode warm path (spec Requirements 1/2/8): two identical `do_get`s
    /// against a `snapshots/<name>/` hardlink dir return BYTE-IDENTICAL batches,
    /// the second a warm hit with zero further reader opens.
    #[test]
    fn do_get_snapshot_mode_second_request_is_a_value_identical_warm_hit() {
        let schema = simple_schema();
        let (_temp, data_dir, table_dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "old", 1, 100)],
                vec![write_row(1, "new", 2, 200), write_row(2, "b", 3, 200)],
            ],
        );
        make_snapshot(&table_dir, "snap1");
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let mut t = ticket(KS, TBL);
        t.snapshot = Some("snap1".into());
        let bytes = t.to_bytes().unwrap();

        let (vals1, vals2, opens_after_first) = rt.block_on(async {
            let r1 = svc
                .do_get(Request::new(Ticket::new(bytes.clone())))
                .await
                .expect("first snapshot do_get");
            let vals1 = sorted_name_values(&decode(r1.into_inner()).await);
            let opens_after_first = svc.warm_metrics().reader_opens;
            let r2 = svc
                .do_get(Request::new(Ticket::new(bytes)))
                .await
                .expect("second snapshot do_get");
            let vals2 = sorted_name_values(&decode(r2.into_inner()).await);
            (vals1, vals2, opens_after_first)
        });

        assert_eq!(vals1.len(), 2, "two partitions after LWW merge");
        assert_eq!(vals2, vals1, "snapshot warm hit is value-identical");
        let m = svc.warm_metrics();
        assert_eq!(m.hits, 1, "the second snapshot request is a warm hit");
        assert_eq!(
            m.reader_opens, opens_after_first,
            "the snapshot warm hit opened zero further readers"
        );
    }

    /// A flush that ADDS a generation between requests is visible on the next
    /// request with zero staleness window (spec Requirement 2): the probe reports
    /// "changed", the rebuild adds exactly the new generation, and the new data
    /// appears. Records a second miss + rebuilt-delta.
    #[test]
    fn do_get_sees_a_newly_added_generation_on_next_request() {
        let schema = simple_schema();
        let (_temp, data_dir, table_dir) =
            build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let bytes = ticket(KS, TBL).to_bytes().unwrap();
        // Warm the cache.
        let n1 = rt.block_on(async {
            let r1 = svc
                .do_get(Request::new(Ticket::new(bytes.clone())))
                .await
                .expect("first do_get");
            total_rows(&decode(r1.into_inner()).await)
        });
        assert_eq!(n1, 1);

        // Simulate a flush OUTSIDE the runtime (append_sstable drives its own):
        // drop a second SSTable (a new generation) into the live table dir.
        append_sstable(&table_dir, &schema, vec![write_row(2, "b", 4, 200)]);

        let rows = rt.block_on(async {
            let r2 = svc
                .do_get(Request::new(Ticket::new(bytes)))
                .await
                .expect("second do_get");
            total_rows(&decode(r2.into_inner()).await)
        });

        assert_eq!(
            rows, 2,
            "the newly-flushed generation is visible immediately"
        );
        let m = svc.warm_metrics();
        assert_eq!(m.misses, 2, "the added generation forced a rebuild");
        assert_eq!(
            m.refresh_rebuilt_delta, 2,
            "both requests recorded delta rebuilds (second added the new gen)"
        );
    }

    /// A pre-cancelled `do_get` performs zero warm-path work and surfaces the
    /// distinct `Aborted` status (spec Requirement 7), never a stale hit.
    #[test]
    fn do_get_setup_pre_cancelled_does_zero_warm_work() {
        let schema = simple_schema();
        let (_temp, data_dir, _dir) =
            build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let err = rt.block_on(async {
            let cancel = CancelFlag::new();
            cancel.cancel();
            let bytes = ticket(KS, TBL).to_bytes().unwrap();
            let (t, producer, schema_ref) = svc
                .validate_do_get_ticket(Request::new(Ticket::new(bytes)))
                .expect("ticket validates");
            match svc.do_get_resolve(t, producer, schema_ref, &cancel).await {
                Ok(_) => panic!("a pre-cancelled setup must abort, not resolve"),
                Err(e) => e,
            }
        });
        assert_eq!(err.code(), tonic::Code::Aborted, "got: {err:?}");
        let m = svc.warm_metrics();
        assert_eq!(m.reader_opens, 0, "a cancelled request opens zero readers");
        assert_eq!(m.misses, 0, "and does no build");
    }

    /// Append one more SSTable generation into an existing live table dir by
    /// running a fresh write-engine flush pointed at the SAME data root.
    fn append_sstable(
        table_dir: &std::path::Path,
        schema: &cqlite_core::schema::TableSchema,
        rows: Vec<cqlite_core::storage::write_engine::Mutation>,
    ) {
        use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
        // table_dir = <data>/<ks>/<table>; recover the data root.
        let data_dir = table_dir.parent().unwrap().parent().unwrap().to_path_buf();
        let wal = table_dir.join(".wal_append");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let config = WriteEngineConfig::new(data_dir, wal, schema.clone());
        let mut engine = WriteEngine::new(config).expect("engine");
        for m in rows {
            engine.write(m).expect("write");
        }
        rt.block_on(engine.flush()).expect("flush").expect("info");
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
            let (t, producer, schema_ref) = svc
                .validate_do_get_ticket(Request::new(Ticket::new(bytes)))
                .expect("ticket validates");
            svc.do_get_resolve(t, producer, schema_ref, &cancel).await
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
            let (t, producer, schema_ref) = svc
                .validate_do_get_ticket(Request::new(Ticket::new(bytes)))
                .expect("ticket validates");
            svc.do_get_resolve(t, producer, schema_ref, &cancel).await
        });
        let setup = setup.expect("uncancelled setup resolves");
        // The warm path (issue #2310) hands over the open reader set; a full-ring
        // token filter keeps both generations (per-reader token prune happens in
        // the merge stage, not here).
        match setup.input {
            DoGetInput::Rows(readers) => assert_eq!(
                readers.len(),
                2,
                "a full-ring token filter keeps both SSTables' warm readers"
            ),
            DoGetInput::Aggregate(_) => {
                panic!("non-aggregating ticket must take the warm row path")
            }
        }
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
