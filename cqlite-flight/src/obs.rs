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
//!   gauge and, on drop, records the request counter (by method + ok/error),
//!   the latency histogram, and decrements the gauge. `do_get` additionally
//!   reports rows and bytes streamed via [`RpcMetrics::add_rows_bytes`].
//!
//! All metric calls go through `cqlite_core::observability`, which is a no-op
//! when its own feature is off, so the handlers compile and run identically in
//! every configuration. Cardinality is bounded: the only attributes are the
//! fixed `FlightService` method name and an `ok`/`error` status — never tickets,
//! keys, queries, or payloads.

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
    started: Instant,
    status: &'static str,
    rows: u64,
    bytes: u64,
    /// Tracks the current in-flight count so the gauge decrements correctly. A
    /// process-wide atomic per method would be ideal, but bounded methods + a
    /// single shared counter keeps it simple; we record +1 on start and 0-delta
    /// is fine because the gauge is observed as a level, set on each change.
    finished: bool,
}

impl RpcMetrics {
    /// Begin recording an RPC. Increments the in-flight gauge for `method`.
    /// `method` must be a `&'static str` from the `FlightService` method set.
    pub fn start(method: &'static str) -> Self {
        IN_FLIGHT.with(|cell| {
            let v = cell.get() + 1;
            cell.set(v);
            obs::record_gauge(catalog::RPC_IN_FLIGHT, v, &[method_attr(method)]);
        });
        Self {
            method,
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

    /// Add rows + payload bytes streamed (used by `do_get`).
    pub fn add_rows_bytes(&mut self, rows: u64, bytes: u64) {
        self.rows = self.rows.saturating_add(rows);
        self.bytes = self.bytes.saturating_add(bytes);
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
        if self.rows > 0 {
            obs::add_counter(catalog::RPC_ROWS, self.rows, &[method.clone()]);
        }
        if self.bytes > 0 {
            obs::add_counter(catalog::RPC_BYTES, self.bytes, &[method.clone()]);
        }
        IN_FLIGHT.with(|cell| {
            let v = cell.get().saturating_sub(1);
            cell.set(v);
            obs::record_gauge(catalog::RPC_IN_FLIGHT, v, &[method]);
        });
    }
}

impl Drop for RpcMetrics {
    fn drop(&mut self) {
        self.finish();
    }
}

thread_local! {
    /// Per-thread in-flight RPC count, used to drive the in-flight gauge as a
    /// level. The gRPC reactor schedules handlers across worker threads, so this
    /// is an approximation; cardinality and cost stay bounded and it avoids a
    /// shared atomic on the hot path.
    static IN_FLIGHT: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
}

fn method_attr(method: &'static str) -> (&'static str, AttrValue) {
    (catalog::attr::RPC_METHOD, AttrValue::StaticStr(method))
}

/// Record an RPC failure into the error-rate signal (`cqlite.errors.total`,
/// subsystem `flight`) and mark the active span errored.
///
/// The handlers return `tonic::Status`, but [`obs::record_error`] keys the
/// counter on a bounded `{category, subsystem}` label set derived from a
/// `cqlite_core::Error` — never the message. We therefore map the gRPC status
/// *code* (a closed set) to a representative `cqlite_core::Error` so the error
/// category is meaningful. No part of the status message is ever recorded.
pub fn record_status_error(status: &tonic::Status) {
    use cqlite_core::Error;
    use tonic::Code;
    // A fixed, bounded code→error mapping. The message text is irrelevant to the
    // counter (only the category is used) so a constant placeholder is passed.
    let err = match status.code() {
        Code::NotFound => Error::not_found("flight"),
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => {
            Error::invalid_input("flight")
        }
        Code::Unimplemented => Error::invalid_operation("flight"),
        _ => Error::internal("flight"),
    };
    obs::record_error(&err, SUBSYSTEM);
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
        m.add_rows_bytes(5, 1024);
        m.ok();
        drop(m);
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
        let cx = opentelemetry::global::get_text_map_propagator(|p| {
            p.extract(&MetadataExtractor(&md))
        });
        let trace_id = cx.span().span_context().trace_id();
        assert_eq!(
            format!("{trace_id:032x}"),
            "0af7651916cd43dd8448eb211c80319c",
            "extracted context carries the client's trace id"
        );
    }
}
