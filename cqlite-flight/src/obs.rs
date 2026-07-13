//! Per-RPC observability for the Arrow Flight service (issue #1041, epic #1031).
//!
//! This module concentrates all telemetry concerns for the gRPC endpoints so the
//! service handlers stay readable:
//!
//! * **Spans** — [`rpc_span`] builds an `info`-level span named `flight.<method>`
//!   and, when the `observability` feature is enabled, parents it to the W3C
//!   `traceparent` carried in the incoming gRPC metadata. Entering the handler
//!   body within this span (via `.instrument(span)`) makes the core
//!   `query.execute` and read-path spans nest under the RPC span, so a client's
//!   distributed trace continues seamlessly into CQLite.
//! * **Metrics** — [`RpcMetrics`] is an RAII recorder: it bumps the in-flight
//!   gauge and, on drop, records the request counter (by method + ok/error) and
//!   the latency histogram, then decrements the gauge. `do_get` additionally
//!   reports rows and bytes streamed per record batch, incrementally, via
//!   [`RpcMetrics::record_batch_progress`] (issue #2162).
//!
//! All metric calls go through `cqlite_core::observability`, which is a no-op
//! when its own feature is off, so the handlers compile and run identically in
//! every configuration. Cardinality is bounded: the only attributes are the
//! fixed `FlightService` method name and an `ok`/`error` status — never tickets,
//! keys, queries, or payloads.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use cqlite_core::observability::{self as obs, catalog, AttrValue};
use tonic::Request;

/// Subsystem label for [`obs::record_error`] from the Flight service.
pub const SUBSYSTEM: &str = "flight";

/// Bounded `cqlite.rpc.status` value for a successful RPC.
const STATUS_OK: &str = "ok";
/// Bounded `cqlite.rpc.status` value for a failed RPC.
const STATUS_ERROR: &str = "error";

/// Build the per-RPC tracing span, parented to the incoming W3C trace context.
///
/// `method` is a `&'static str` from the fixed `FlightService` method set (e.g.
/// `"do_get"`), keeping span-name cardinality bounded. The returned span is
/// entered by the caller via `.instrument(span)` on the handler future.
///
/// # Traceparent propagation
///
/// When the `observability` feature is on, the incoming gRPC metadata is read
/// through the globally-installed W3C text-map propagator
/// (`TraceContextPropagator`, installed in `main`). If the client sent a
/// `traceparent` header, the extracted [`opentelemetry::Context`] is attached as
/// the span's parent with `OpenTelemetrySpanExt::set_parent`, so the server span
/// (and everything nested under it) shares the client's trace ID. With no header
/// or with the feature off, the span is simply a new root.
pub fn rpc_span<T>(method: &'static str, request: &Request<T>) -> tracing::Span {
    let span = tracing::info_span!("flight.rpc", otel.name = method, rpc.method = method);
    #[cfg(feature = "observability")]
    {
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&MetadataExtractor(request.metadata()))
        });
        span.set_parent(parent_cx);
    }
    #[cfg(not(feature = "observability"))]
    {
        let _ = request;
    }
    span
}

/// RAII recorder for the per-RPC gRPC metrics.
///
/// Created at handler entry: it records the in-flight gauge as incremented.
/// Call [`RpcMetrics::ok`] / [`RpcMetrics::error`] to set the terminal status
/// (defaults to `error` so an early `?` return is still counted as a failure),
/// and on drop it emits `cqlite.rpc.requests`, `cqlite.rpc.duration`, and the
/// decremented in-flight gauge.
pub struct RpcMetrics {
    method: &'static str,
    /// Index into [`IN_FLIGHT`] for `method`, resolved once at `start`. The
    /// decrement on completion targets this same shared atomic, so it is correct
    /// even when Tokio moves the RPC future to a different worker thread across an
    /// `.await` (the increment and decrement are not thread-bound).
    method_idx: usize,
    started: Instant,
    status: &'static str,
    rows: u64,
    bytes: u64,
    finished: bool,
}

impl RpcMetrics {
    /// Begin recording an RPC. Increments the process-wide in-flight gauge for
    /// `method`. `method` must be a `&'static str` from the `FlightService`
    /// method set (see [`method_index`]).
    pub fn start(method: &'static str) -> Self {
        let method_idx = method_index(method);
        // Normalize the label to the bounded slot name so an unexpected method
        // string can never leak as a metric attribute: a value not in the fixed
        // set resolves to `method_idx` for the `"other"` slot, and we report that
        // slot's canonical label here (and in `finish`). This keeps metric
        // cardinality capped at exactly the RPC_METHODS set and makes the in-flight
        // gauge label match the shared counter that is actually incremented.
        let method = RPC_METHODS[method_idx];
        // Increment the shared per-method counter and observe the level. Because
        // the counter is process-wide (not thread-local), the matching decrement
        // in `finish` stays correct under Tokio worker-thread hopping.
        let v = IN_FLIGHT[method_idx].fetch_add(1, Ordering::Relaxed) + 1;
        obs::record_gauge(catalog::RPC_IN_FLIGHT, v, &[method_attr(method)]);
        Self {
            method,
            method_idx,
            started: Instant::now(),
            status: STATUS_ERROR,
            rows: 0,
            bytes: 0,
            finished: false,
        }
    }

    /// Mark this RPC as successful.
    pub fn ok(&mut self) {
        self.status = STATUS_OK;
    }

    /// Record one record batch's rows + payload bytes as an INCREMENTAL progress
    /// delta (issue #2162): emit `cqlite.rpc.rows` / `cqlite.rpc.bytes` as a
    /// monotonic counter delta right now, as the batch passes toward the client,
    /// instead of a single aggregate emission at stream end. Called once per
    /// record batch by [`crate::streaming::MeteredDoGetStream`], so a long-running
    /// `do_get` shows the counters climbing while `cqlite.rpc.in_flight` is still
    /// non-zero. The running totals are also accumulated for the terminal
    /// bookkeeping; because the deltas are emitted here, `finish` does NOT re-emit
    /// them (no double counting). Emission is per-batch, never per-row.
    pub fn record_batch_progress(&mut self, rows: u64, bytes: u64) {
        self.rows = self.rows.saturating_add(rows);
        self.bytes = self.bytes.saturating_add(bytes);
        let method = method_attr(self.method);
        if rows > 0 {
            obs::add_counter(catalog::RPC_ROWS, rows, &[method.clone()]);
        }
        if bytes > 0 {
            obs::add_counter(catalog::RPC_BYTES, bytes, &[method]);
        }
    }

    /// Emit the terminal metrics. Idempotent; called from `Drop`.
    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        let method = method_attr(self.method);
        let status: (&'static str, AttrValue) =
            (catalog::attr::RPC_STATUS, AttrValue::StaticStr(self.status));
        obs::add_counter(catalog::RPC_REQUESTS, 1, &[method.clone(), status.clone()]);
        obs::record_histogram(
            catalog::RPC_DURATION,
            self.started.elapsed().as_secs_f64(),
            &[method.clone(), status],
        );
        // `cqlite.rpc.rows` / `cqlite.rpc.bytes` are emitted INCREMENTALLY per
        // record batch by `record_batch_progress` (issue #2162), so `finish` no
        // longer re-adds the accumulated totals here — doing so would double-count
        // the monotonic counters. The accumulated `self.rows`/`self.bytes` remain
        // available for terminal bookkeeping / tests only.
        // Decrement the SAME shared per-method counter incremented in `start`.
        // `fetch_sub` returns the previous value, so the new level is `prev - 1`;
        // a `max(0)` floor guards against an unexpected underflow without ever
        // recording a negative gauge.
        let prev = IN_FLIGHT[self.method_idx].fetch_sub(1, Ordering::Relaxed);
        let level = (prev - 1).max(0);
        obs::record_gauge(catalog::RPC_IN_FLIGHT, level, &[method]);
    }
}

impl Drop for RpcMetrics {
    fn drop(&mut self) {
        self.finish();
    }
}

/// `do_get` execution phases (issue #2162), a fixed, bounded, ordered set. Used
/// as the `cqlite.rpc.phase` attribute value on `cqlite.rpc.phase.duration`; the
/// values are `&'static str` from this closed table so metric cardinality is
/// capped exactly like [`RPC_METHODS`] — never a per-query/ticket/key value.
///
/// - `resolve`: path discovery + token prune (`do_get_setup`).
/// - `merge_setup`: opening every input SSTable + building the k-way merger
///   (`KWayMerger::new`, the #2157 stall suspect) before the first batch.
/// - `stream`: partitions stepping + batches flowing to the client.
pub const PHASE_RESOLVE: &str = "resolve";
/// See [`PHASE_RESOLVE`].
pub const PHASE_MERGE_SETUP: &str = "merge_setup";
/// See [`PHASE_RESOLVE`].
pub const PHASE_STREAM: &str = "stream";

/// The closed set of `do_get` phase labels, in transition order.
const RPC_PHASES: [&str; 3] = [PHASE_RESOLVE, PHASE_MERGE_SETUP, PHASE_STREAM];

/// Normalise a phase label to its bounded slot, so an unexpected value can never
/// leak as a metric attribute (it maps to `resolve`, never an arbitrary string).
fn phase_slot(phase: &str) -> &'static str {
    RPC_PHASES
        .iter()
        .find(|p| **p == phase)
        .copied()
        .unwrap_or(PHASE_RESOLVE)
}

/// Records the wall time a `do_get` spends in each bounded execution phase
/// (issue #2162): `resolve` → `merge_setup` → `stream`.
///
/// Constructed at `do_get` entry (phase `resolve`). [`Self::transition`] closes
/// the current phase — emitting a `cqlite.rpc.phase.duration` histogram sample
/// tagged with the bounded `cqlite.rpc.phase` attribute plus a `tracing` span
/// event — and opens the next. [`Drop`] closes whichever phase is still open, so
/// EVERY entered phase records exactly one sample even on an error/cancel/panic
/// exit, and a phase never entered records none (never a fabricated zero).
///
/// The recorder captures the `do_get` span at construction and re-enters it
/// around each emission, so the span events attach to the `flight.do_get` span
/// even when the later phases run on the blocking merge pool (mirrors
/// [`crate::streaming`]'s span-capture pattern).
pub struct PhaseTimer {
    method: &'static str,
    phase: &'static str,
    started: Instant,
    span: tracing::Span,
}

impl PhaseTimer {
    /// Begin timing at the `resolve` phase for `method` (a `&'static str` from the
    /// bounded `FlightService` method set).
    pub fn start(method: &'static str) -> Self {
        let method = RPC_METHODS[method_index(method)];
        let timer = Self {
            method,
            phase: PHASE_RESOLVE,
            started: Instant::now(),
            span: tracing::Span::current(),
        };
        // Issue #2361: mark the opening phase ACTIVE so a `do_get` that never
        // completes a phase (a wedged merge in `stream`) is still visible as a
        // level, not only via the completion-time duration histogram.
        timer.set_active(1);
        timer
    }

    /// Close the current phase (record its duration) and open `next`.
    pub fn transition(&mut self, next: &'static str) {
        self.record_current();
        // Issue #2361: the closing phase is no longer active; the opening one is.
        self.set_active(0);
        self.phase = phase_slot(next);
        self.started = Instant::now();
        self.set_active(1);
    }

    /// Set the in-flight phase-active gauge (issue #2361) to `value` (1 = the
    /// phase is currently executing, 0 = it has exited), tagged with the bounded
    /// method + phase attributes.
    fn set_active(&self, value: i64) {
        let _entered = self.span.enter();
        obs::record_gauge(
            catalog::RPC_PHASE_ACTIVE,
            value,
            &[
                method_attr(self.method),
                (
                    catalog::attr::RPC_PHASE,
                    AttrValue::StaticStr(phase_slot(self.phase)),
                ),
            ],
        );
    }

    /// Emit the histogram sample + span event for the phase currently open.
    fn record_current(&self) {
        let elapsed = self.started.elapsed().as_secs_f64();
        let _entered = self.span.enter();
        obs::record_histogram(
            catalog::RPC_PHASE_DURATION,
            elapsed,
            &[
                method_attr(self.method),
                (
                    catalog::attr::RPC_PHASE,
                    AttrValue::StaticStr(phase_slot(self.phase)),
                ),
            ],
        );
        tracing::debug!(
            rpc.method = self.method,
            cqlite.rpc.phase = self.phase,
            elapsed_s = elapsed,
            "do_get phase completed"
        );
    }
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        // Close whichever phase is still open — covers the normal terminal phase
        // AND an error/cancel/panic exit that never transitioned further, so a
        // stall that emits zero rows still records the phase it died in.
        self.record_current();
        // Issue #2361: clear the in-flight phase-active gauge on every exit path.
        self.set_active(0);
    }
}

/// Fixed, bounded set of `FlightService` RPC method names, in the order used to
/// index [`IN_FLIGHT`]. Cardinality is exactly the gRPC method set — no ticket,
/// key, query, or payload ever appears here.
const RPC_METHODS: [&str; 11] = [
    "get_flight_info",
    "get_schema",
    "do_get",
    "handshake",
    "list_flights",
    "poll_flight_info",
    "do_put",
    "do_exchange",
    "do_action",
    "list_actions",
    // Catch-all slot for any method name not in the fixed set above. Keeping a
    // bounded fallback means an unexpected `method` never panics or allocates and
    // the gauge cardinality is still capped.
    "other",
];

/// Index of the catch-all slot in [`RPC_METHODS`] / [`IN_FLIGHT`].
const OTHER_IDX: usize = RPC_METHODS.len() - 1;

/// Process-wide, per-method in-flight RPC counters used to drive the in-flight
/// gauge as a level. A single shared atomic per method means the increment (on
/// RPC entry) and the matching decrement (on completion, via `RpcMetrics::Drop`)
/// always target the same counter even when Tokio moves the RPC future between
/// worker threads across `.await`. Cardinality is bounded by [`RPC_METHODS`].
static IN_FLIGHT: [AtomicI64; RPC_METHODS.len()] = {
    // `AtomicI64` is not `Copy`, so the array is built from a const initializer.
    [
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
    ]
};

/// Resolve an RPC `method` name to its index in [`IN_FLIGHT`]. Unknown names map
/// to the bounded catch-all slot, so the gauge cardinality stays capped and the
/// lookup never panics.
fn method_index(method: &str) -> usize {
    RPC_METHODS
        .iter()
        .position(|m| *m == method)
        .unwrap_or(OTHER_IDX)
}

fn method_attr(method: &'static str) -> (&'static str, AttrValue) {
    (catalog::attr::RPC_METHOD, AttrValue::StaticStr(method))
}

/// Read the current process-wide in-flight RPC level for `method` (issue #2264).
///
/// Exposes the same shared per-method counter that drives the `cqlite.rpc.in_flight`
/// gauge, so an end-to-end test can assert that a `do_get` whose client stopped
/// reading and disconnected releases its RPC accounting (the level returns to its
/// pre-RPC baseline). Unknown method names resolve to the bounded catch-all slot
/// (never panics), matching [`RpcMetrics::start`].
pub fn in_flight_level(method: &str) -> i64 {
    IN_FLIGHT[method_index(method)].load(Ordering::Relaxed)
}

/// Record an RPC failure into the error-rate signal (`cqlite.errors.total`,
/// subsystem `flight`), emit a `tracing` log line, and mark the active span
/// errored.
///
/// The handlers return `tonic::Status`, but [`obs::record_error`] keys the
/// counter on a bounded `{category, subsystem}` label set derived from a
/// `cqlite_core::Error` — never the message. We therefore map the gRPC status
/// *code* (a closed set) to a representative `cqlite_core::Error` so the error
/// category is meaningful. No part of the status message is ever recorded in the
/// metric.
///
/// Logging (issue #2193): the OTel error counter is a no-op when the
/// `observability` feature is off (the common case, and how the field flight
/// image was built — see #2128), so before this change a failure that arrived
/// through this hook was invisible in the logs even at `RUST_LOG=debug`. That
/// let a `do_get` streaming-egress failure (issue #2193) close the client stream
/// with `Failed to read message` while the server logged nothing. We now emit a
/// `tracing` event so every recorded failure is visible in the log at a level
/// matching its severity — `error` for a server fault, `warn` for a client
/// fault, and `debug` for the expected `Aborted` of a client disconnect /
/// cooperative cancellation (so normal disconnects stay quiet). The status
/// message is fine to log (unlike the metric, the log is not a bounded-label
/// cardinality surface).
pub fn record_status_error(status: &tonic::Status) {
    use cqlite_core::Error;
    use tonic::Code;
    let code = status.code();
    // A fixed, bounded code→error mapping. The message text is irrelevant to the
    // counter (only the category is used) so a constant placeholder is passed.
    let err = match code {
        Code::NotFound => Error::not_found("flight"),
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => {
            Error::invalid_input("flight")
        }
        Code::Unimplemented => Error::invalid_operation("flight"),
        _ => Error::internal("flight"),
    };
    obs::record_error(&err, SUBSYSTEM);

    let message = status.message();
    match log_level_for(code) {
        // Expected, benign terminal states: a client disconnect / cooperative
        // cancellation surfaces as `Aborted`, and a healthy end is `Ok` (never
        // routed here, but guarded). Keep these off the error/warn logs.
        tracing::Level::DEBUG => {
            tracing::debug!(subsystem = SUBSYSTEM, %code, message, "flight rpc ended");
        }
        // Client-fault codes: the request was bad, not the server.
        tracing::Level::WARN => {
            tracing::warn!(subsystem = SUBSYSTEM, %code, message, "flight rpc failed");
        }
        // Server-fault codes (Internal, Unknown, DataLoss, Unavailable, …): the
        // class that includes a swallowed streaming-egress encode/send failure.
        _ => {
            tracing::error!(subsystem = SUBSYSTEM, %code, message, "flight rpc failed");
        }
    }
}

/// The `tracing` level at which a failing gRPC `code` is logged by
/// [`record_status_error`]: `debug` for the benign terminal states (client
/// disconnect / cancellation), `warn` for a client-fault request, and `error`
/// for a server fault — the class that includes a swallowed streaming-egress
/// encode/send failure (issue #2193). Split out as a pure function so the
/// severity mapping is unit-testable without a `tracing` subscriber.
fn log_level_for(code: tonic::Code) -> tracing::Level {
    use tonic::Code;
    match code {
        Code::Aborted | Code::Cancelled | Code::Ok => tracing::Level::DEBUG,
        Code::NotFound
        | Code::InvalidArgument
        | Code::FailedPrecondition
        | Code::OutOfRange
        | Code::Unauthenticated
        | Code::PermissionDenied
        | Code::AlreadyExists
        | Code::Unimplemented => tracing::Level::WARN,
        _ => tracing::Level::ERROR,
    }
}

/// `opentelemetry::propagation::Extractor` over gRPC request metadata, so the
/// W3C propagator can read the incoming `traceparent`/`tracestate` headers.
#[cfg(feature = "observability")]
struct MetadataExtractor<'a>(&'a tonic::metadata::MetadataMap);

#[cfg(feature = "observability")]
impl opentelemetry::propagation::Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|k| match k {
                tonic::metadata::KeyRef::Ascii(k) => Some(k.as_str()),
                tonic::metadata::KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_span_builds_for_any_request() {
        // A span must always be produced regardless of metadata or feature state.
        let req = Request::new(());
        let span = rpc_span("do_get", &req);
        let _entered = span.enter();
    }

    #[test]
    fn rpc_metrics_lifecycle_is_callable() {
        // The recorder must drive its counters/gauges without panicking in any
        // build (no-op when the core observability feature is off).
        let mut m = RpcMetrics::start("do_get");
        m.record_batch_progress(5, 1024);
        m.record_batch_progress(3, 512);
        m.ok();
        drop(m);
    }

    #[test]
    fn method_index_maps_known_and_unknown() {
        // Every fixed FlightService method resolves to a distinct in-bounds slot.
        for (i, name) in RPC_METHODS.iter().enumerate().take(OTHER_IDX) {
            assert_eq!(method_index(name), i, "{name} maps to its own slot");
        }
        // Unknown names fall into the bounded catch-all slot (no panic, no growth).
        assert_eq!(method_index("not_a_real_method"), OTHER_IDX);
    }

    #[test]
    fn in_flight_gauge_balances_under_thread_hopping() {
        // Simulate Tokio moving an RPC future across worker threads: increment on
        // one thread (RpcMetrics::start), drop on another (the decrement). Because
        // the counter is a process-wide atomic keyed by method — not thread-local
        // — the per-method count must return to its starting level. The balance
        // property holds regardless of other concurrently-running tests, since
        // every start is paired with exactly one drop on the same shared counter.
        //
        // Use `poll_flight_info`, a method no other unit test exercises, so the
        // pre/post snapshots are stable under the parallel test runner.
        let idx = method_index("poll_flight_info");
        let base = IN_FLIGHT[idx].load(Ordering::Relaxed);

        // Start on this thread, hand the recorder to another thread to drop it.
        let m = RpcMetrics::start("poll_flight_info");
        std::thread::spawn(move || drop(m))
            .join()
            .expect("drop thread joins");

        assert_eq!(
            IN_FLIGHT[idx].load(Ordering::Relaxed),
            base,
            "increment + decrement-on-another-thread balances the shared counter"
        );
    }

    #[test]
    fn rpc_metrics_increments_shared_counter_on_start() {
        // `start` must bump the process-wide per-method counter (and `Drop` must
        // restore it). Serialise on a dedicated method slot so the +1 assertion is
        // deterministic: no other unit test creates `do_exchange` metrics.
        let idx = method_index("do_exchange");
        let base = IN_FLIGHT[idx].load(Ordering::Relaxed);
        {
            let _m = RpcMetrics::start("do_exchange");
            assert_eq!(
                IN_FLIGHT[idx].load(Ordering::Relaxed),
                base + 1,
                "start increments the shared per-method counter"
            );
        }
        assert_eq!(
            IN_FLIGHT[idx].load(Ordering::Relaxed),
            base,
            "drop decrements the shared per-method counter back to base"
        );
    }

    #[test]
    fn record_status_error_maps_codes() {
        // Exercise every code arm; the call must not panic and records only the
        // bounded {category, subsystem} signal.
        for status in [
            tonic::Status::not_found("x"),
            tonic::Status::invalid_argument("x"),
            tonic::Status::failed_precondition("x"),
            tonic::Status::out_of_range("x"),
            tonic::Status::unimplemented("x"),
            tonic::Status::internal("x"),
        ] {
            record_status_error(&status);
        }
    }

    #[test]
    fn log_level_matches_fault_severity() {
        use tonic::Code;
        use tracing::Level;
        // Server faults — the swallowed streaming-egress class (issue #2193) —
        // must log at error so a `do_get` encode/send failure is never invisible.
        for code in [
            Code::Internal,
            Code::Unknown,
            Code::DataLoss,
            Code::Unavailable,
        ] {
            assert_eq!(
                log_level_for(code),
                Level::ERROR,
                "{code:?} is a server fault"
            );
        }
        // Client faults log at warn (the request was bad, not the server).
        for code in [
            Code::NotFound,
            Code::InvalidArgument,
            Code::FailedPrecondition,
            Code::OutOfRange,
            Code::Unimplemented,
        ] {
            assert_eq!(
                log_level_for(code),
                Level::WARN,
                "{code:?} is a client fault"
            );
        }
        // Expected benign terminal states (client disconnect / cancellation)
        // stay at debug so normal disconnects don't spam the error log.
        for code in [Code::Aborted, Code::Cancelled, Code::Ok] {
            assert_eq!(log_level_for(code), Level::DEBUG, "{code:?} is benign");
        }
    }

    #[cfg(feature = "observability")]
    #[test]
    fn metadata_extractor_reads_traceparent() {
        use opentelemetry::propagation::Extractor;
        let mut md = tonic::metadata::MetadataMap::new();
        let tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        md.insert("traceparent", tp.parse().expect("ascii value"));
        let ex = MetadataExtractor(&md);
        assert_eq!(ex.get("traceparent"), Some(tp));
        assert!(ex.keys().contains(&"traceparent"));
    }

    #[cfg(feature = "observability")]
    #[test]
    fn propagator_extracts_remote_trace_id() {
        // The W3C propagator, given metadata carrying a `traceparent`, yields a
        // context whose span context holds the client's trace id — this is the
        // exact value `rpc_span` attaches as the span parent.
        use opentelemetry::trace::TraceContextExt;
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let mut md = tonic::metadata::MetadataMap::new();
        md.insert(
            "traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
                .parse()
                .expect("ascii value"),
        );
        let cx =
            opentelemetry::global::get_text_map_propagator(|p| p.extract(&MetadataExtractor(&md)));
        let trace_id = cx.span().span_context().trace_id();
        assert_eq!(
            format!("{trace_id:032x}"),
            "0af7651916cd43dd8448eb211c80319c",
            "extracted context carries the client's trace id"
        );
    }
}
