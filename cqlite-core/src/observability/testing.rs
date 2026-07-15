//! Shared in-memory OpenTelemetry capture harness for observability tests
//! (epic #1031, issue #1043).
//!
//! This module is the **single shared fixture** every observability-correctness
//! test (and future child issue) reuses. It installs in-memory OTLP exporters,
//! runs a flow, force-flushes, and hands back the captured spans and metrics so a
//! test can assert the expected span tree, metric names/units, and bounded
//! attribute sets — all deterministically, with no collector, network, or timing
//! dependence.
//!
//! It is gated behind the `observability` feature (it needs the OTel SDK) and is
//! intended for tests, so it lives under `#[cfg(feature = "observability")]` and
//! depends on `opentelemetry_sdk`'s in-memory exporters (available because the
//! crate enables the SDK's `testing` feature as a dev-dependency; the
//! library/default build links no OTel at all).
//!
//! # Why a single process-global meter provider
//!
//! The metric helpers in [`crate::observability`] record through
//! `opentelemetry::global::meter("cqlite")`, and the concrete `Meter` is cached
//! in a `OnceLock` the first time any metric is recorded in the process. So the
//! global meter provider must be installed **once, before the first metric
//! record**, and cannot be swapped per-test. [`metrics_capture`] therefore
//! returns a handle to a single process-wide [`InMemoryMetricExporter`]; tests
//! call [`MetricsCapture::reset`] + [`MetricsCapture::flush_and_collect`] around
//! their own flow rather than installing a fresh provider each time.
//!
//! ## Metric isolation guarantee
//!
//! The shared exporter is configured with **DELTA** temporality
//! ([`Temporality::Delta`]). Each [`MetricsCapture::flush_and_collect`] therefore
//! reports only the values aggregated *since the previous collect*, not a
//! running cumulative total. [`MetricsCapture::reset`] force-flushes (draining and
//! discarding any prior deltas) and clears the exporter buffer, so a subsequent
//! `flush_and_collect` reflects **only the work done between `reset` and
//! `flush_and_collect`** — never metrics accumulated by an earlier flow or test.
//! This holds even under parallel test execution: although the meter provider is
//! process-global, all metric assertions run inside a single serial test (see
//! `tests/observability_correctness.rs`), and DELTA temporality means that test
//! never re-observes another flow's already-collected counters.
//!
//! Spans, by contrast, flow through `tracing`, so a test can install its own
//! tracer + subscriber for the duration of a flow via [`capture_spans`] and get
//! perfectly isolated span trees with no global state.
//!
//! # Typical use
//!
//! ```ignore
//! use cqlite_core::observability::{self as obs, catalog};
//! use cqlite_core::observability::testing;
//!
//! // Spans: scoped, isolated per call.
//! let spans = testing::capture_spans(|| {
//!     obs::record_histogram(catalog::QUERY_DURATION, 0.001, &[]);
//!     // ... run an instrumented flow inside a `tracing` span ...
//! });
//! assert!(spans.iter().any(|s| s.name == "query.execute"));
//!
//! // Metrics: process-global, reset around the flow.
//! let mc = testing::metrics_capture();
//! mc.reset();
//! obs::add_counter(catalog::READ_ROWS, 7, &[]);
//! let metrics = mc.flush_and_collect();
//! assert_eq!(metrics.counter_sum(catalog::READ_ROWS), 7);
//! ```

use std::sync::OnceLock;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::Value;
use opentelemetry_sdk::metrics::data::AggregatedMetrics;
use opentelemetry_sdk::metrics::data::MetricData;
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, InMemoryMetricExporterBuilder, PeriodicReader, SdkMeterProvider,
    Temporality,
};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tracing_subscriber::prelude::*;

/// Instrumentation scope used by the production telemetry (mirrors `otel::SCOPE`).
const SCOPE: &str = "cqlite";

// ---------------------------------------------------------------------------
// Span capture (per-call, isolated)
// ---------------------------------------------------------------------------

/// A captured, simplified view of an exported span. Wraps the SDK [`SpanData`]
/// with ergonomic accessors so tests read span trees without depending on the
/// SDK's exact field layout.
#[derive(Debug, Clone)]
pub struct CapturedSpan {
    /// Span name (`#[tracing::instrument]` name or `info_span!` name).
    pub name: String,
    /// The raw SDK span data, for advanced assertions (timing, events, links).
    pub data: SpanData,
}

impl CapturedSpan {
    /// This span's own 16-byte span id.
    pub fn span_id(&self) -> opentelemetry::trace::SpanId {
        self.data.span_context.span_id()
    }

    /// This span's parent span id (`SpanId::INVALID` for a root span).
    pub fn parent_span_id(&self) -> opentelemetry::trace::SpanId {
        self.data.parent_span_id
    }

    /// The trace id this span belongs to.
    pub fn trace_id(&self) -> opentelemetry::trace::TraceId {
        self.data.span_context.trace_id()
    }

    /// Look up a bounded span attribute value (as a display string) by key.
    pub fn attribute(&self, key: &str) -> Option<String> {
        self.data
            .attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.as_str().into_owned())
    }

    /// Whether the span's status was set to error (via `record_error` /
    /// `mark_span_error`).
    pub fn is_error(&self) -> bool {
        matches!(self.data.status, opentelemetry::trace::Status::Error { .. })
            || self.attribute("otel.status_code").as_deref() == Some("ERROR")
    }
}

/// A captured set of spans from one [`capture_spans`] call, with tree helpers.
#[derive(Debug, Clone, Default)]
pub struct CapturedSpans {
    spans: Vec<CapturedSpan>,
}

impl CapturedSpans {
    /// Build a [`CapturedSpans`] from already-exported `(name, SpanData)` pairs.
    ///
    /// Lets a test that drives its own tracer (e.g. with a custom sampler) reuse
    /// the tree/name helpers here instead of duplicating them.
    pub fn from_raw(raw: Vec<(String, SpanData)>) -> Self {
        Self {
            spans: raw
                .into_iter()
                .map(|(name, data)| CapturedSpan { name, data })
                .collect(),
        }
    }

    /// All captured spans, in export order.
    pub fn all(&self) -> &[CapturedSpan] {
        &self.spans
    }

    /// Iterate the captured spans.
    pub fn iter(&self) -> std::slice::Iter<'_, CapturedSpan> {
        self.spans.iter()
    }

    /// Find the first span with the given name.
    pub fn find(&self, name: &str) -> Option<&CapturedSpan> {
        self.spans.iter().find(|s| s.name == name)
    }

    /// Whether any captured span has the given name.
    pub fn contains(&self, name: &str) -> bool {
        self.find(name).is_some()
    }

    /// Whether `child` is a direct child of `parent` (matched by name): there
    /// exists a span named `parent` whose span id equals a span named `child`'s
    /// parent span id, sharing the same trace.
    pub fn is_parent_of(&self, parent: &str, child: &str) -> bool {
        let parents: Vec<_> = self.spans.iter().filter(|s| s.name == parent).collect();
        self.spans.iter().filter(|s| s.name == child).any(|c| {
            parents
                .iter()
                .any(|p| p.span_id() == c.parent_span_id() && p.trace_id() == c.trace_id())
        })
    }
}

/// Run `flow` with an isolated in-memory tracer installed as the default
/// `tracing` subscriber, then return every span it produced.
///
/// The tracer + subscriber are scoped to this call (via
/// [`tracing::subscriber::with_default`]), so concurrent or subsequent calls do
/// not see each other's spans. A [`SimpleSpanProcessor`] exports each span
/// synchronously as it closes, and the provider is force-flushed before reading,
/// so the result is deterministic with no timing dependence.
///
/// Spans only close (and thus export) when their guard is dropped — make sure
/// `flow` fully completes any instrumented work (e.g. `block_on` an async flow)
/// before returning.
pub fn capture_spans<F, R>(flow: F) -> CapturedSpans
where
    F: FnOnce() -> R,
{
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer(SCOPE);
    let layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, flow);

    // Ensure everything is flushed before we read.
    let _ = provider.force_flush();

    let spans = exporter
        .get_finished_spans()
        .expect("in-memory span exporter must yield finished spans")
        .into_iter()
        .map(|data| CapturedSpan {
            name: data.name.to_string(),
            data,
        })
        .collect();
    let _ = provider.shutdown();
    CapturedSpans { spans }
}

// ---------------------------------------------------------------------------
// Metric capture (process-global, reset around the flow)
// ---------------------------------------------------------------------------

/// Process-wide in-memory metric exporter handle.
///
/// There is exactly one of these per process because the production metric
/// helpers bind a single global `Meter` on first use (see the module docs).
/// Obtain it via [`metrics_capture`]; [`reset`](Self::reset) before your flow and
/// [`flush_and_collect`](Self::flush_and_collect) after it.
#[derive(Clone)]
pub struct MetricsCapture {
    exporter: InMemoryMetricExporter,
    provider: SdkMeterProvider,
}

/// A snapshot of metrics collected by [`MetricsCapture::flush_and_collect`],
/// with name/unit/value accessors for assertions.
#[derive(Debug, Default)]
pub struct CapturedMetrics {
    entries: Vec<MetricEntry>,
}

/// A single collected metric: its name, unit, kind, and aggregated value(s).
#[derive(Debug, Clone)]
pub struct MetricEntry {
    /// Instrument name (a `catalog::*` constant).
    pub name: String,
    /// Instrument unit (a `catalog::unit::*` string).
    pub unit: String,
    /// The aggregated data points.
    pub points: Vec<MetricPoint>,
}

/// One aggregated data point: its numeric value (as f64) and bounded attributes.
#[derive(Debug, Clone)]
pub struct MetricPoint {
    /// The aggregated value (counter sum, gauge value, or histogram sum).
    pub value: f64,
    /// The bounded attributes as `(key, value-as-string)` pairs.
    pub attributes: Vec<(String, String)>,
}

impl CapturedMetrics {
    /// Every collected metric entry.
    pub fn entries(&self) -> &[MetricEntry] {
        &self.entries
    }

    /// Find a collected metric by name.
    pub fn find(&self, name: &str) -> Option<&MetricEntry> {
        self.entries.iter().find(|m| m.name == name)
    }

    /// Whether a metric with the given name was collected.
    pub fn contains(&self, name: &str) -> bool {
        self.find(name).is_some()
    }

    /// The unit string reported for a metric, if collected.
    pub fn unit(&self, name: &str) -> Option<&str> {
        self.find(name).map(|m| m.unit.as_str())
    }

    /// Sum of all data-point values for a metric (counter total, histogram sum,
    /// or gauge value), or `0.0` if the metric was not collected.
    pub fn counter_sum(&self, name: &str) -> f64 {
        self.find(name)
            .map(|m| m.points.iter().map(|p| p.value).sum())
            .unwrap_or(0.0)
    }

    /// Sum of values for the data points whose attribute set contains ALL of the
    /// given `(key, value)` pairs. Lets a test assert, e.g., that
    /// `cqlite.errors.total{category=…,subsystem=…}` incremented.
    pub fn sum_where(&self, name: &str, required: &[(&str, &str)]) -> f64 {
        let Some(m) = self.find(name) else {
            return 0.0;
        };
        m.points
            .iter()
            .filter(|p| {
                required
                    .iter()
                    .all(|(k, v)| p.attributes.iter().any(|(pk, pv)| pk == k && pv == v))
            })
            .map(|p| p.value)
            .sum()
    }
}

impl MetricsCapture {
    /// Drop everything collected so far so the next
    /// [`flush_and_collect`](Self::flush_and_collect) reflects only the flow run
    /// after this call. Call this immediately before your flow.
    ///
    /// This force-flushes first (draining the SDK's pending DELTA aggregation so
    /// any straggler deltas land in the exporter), then clears the exporter
    /// buffer. Because the exporter uses DELTA temporality, the *next* collect
    /// starts a fresh aggregation window — no cumulative state from before this
    /// `reset` can leak into a later `flush_and_collect`.
    pub fn reset(&self) {
        // Flush first so any straggler deltas land, then clear the buffer. With
        // DELTA temporality this also resets the aggregation window.
        let _ = self.provider.force_flush();
        self.exporter.reset();
    }

    /// Re-emit the always-on baseline instruments (e.g. `cqlite.errors.total` at
    /// 0), exactly as production [`crate::observability::init`] does on startup
    /// (issue #2288). Production uses cumulative temporality so a single seed at
    /// init is visible in every scrape; this harness uses DELTA temporality, so a
    /// test that wants to observe the seeded `0` series in its own collect window
    /// calls this after [`reset`](Self::reset) and before
    /// [`flush_and_collect`](Self::flush_and_collect).
    pub fn seed_baseline(&self) {
        super::otel::register_baseline_instruments();
    }

    /// Force the meter provider to collect + export, then return a snapshot.
    pub fn flush_and_collect(&self) -> CapturedMetrics {
        let _ = self.provider.force_flush();
        let resource_metrics = self
            .exporter
            .get_finished_metrics()
            .expect("in-memory metric exporter must yield collected metrics");

        let mut entries = Vec::new();
        for rm in &resource_metrics {
            for sm in rm.scope_metrics() {
                for metric in sm.metrics() {
                    entries.push(metric_to_entry(metric));
                }
            }
        }
        CapturedMetrics { entries }
    }
}

/// The process-wide [`MetricsCapture`], installing the global in-memory meter
/// provider exactly once on first call.
///
/// MUST be called before any code path records a catalog metric in the process,
/// otherwise the global `Meter` binds to a no-op provider and nothing is
/// captured. Observability tests call this at the start of the relevant test.
pub fn metrics_capture() -> MetricsCapture {
    static CAPTURE: OnceLock<MetricsCapture> = OnceLock::new();
    CAPTURE
        .get_or_init(|| {
            // DELTA temporality so each `flush_and_collect` returns only the
            // values recorded since the previous collect (see the module-level
            // "Metric isolation guarantee"). This prevents an earlier flow/test's
            // cumulative counters from re-appearing in a later collect.
            let exporter = InMemoryMetricExporterBuilder::new()
                .with_temporality(Temporality::Delta)
                .build();
            let reader = PeriodicReader::builder(exporter.clone()).build();
            let provider = SdkMeterProvider::builder().with_reader(reader).build();
            opentelemetry::global::set_meter_provider(provider.clone());
            super::otel::set_metrics_active_for_testing();
            // Mirror production `otel::init` (issue #2288): seed the always-on
            // baseline instruments (e.g. `cqlite.errors.total` at 0) so a scrape
            // of a freshly-started server sees them present, not absent.
            super::otel::register_baseline_instruments();
            MetricsCapture { exporter, provider }
        })
        .clone()
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::I64(i) => i.to_string(),
        Value::F64(f) => f.to_string(),
        other => format!("{other:?}"),
    }
}

/// Convert one SDK `Metric` into the simplified [`MetricEntry`], flattening the
/// f64/u64/i64 aggregation variants into f64 values for uniform assertions.
fn metric_to_entry(metric: &opentelemetry_sdk::metrics::data::Metric) -> MetricEntry {
    let mut points = Vec::new();
    match metric.data() {
        AggregatedMetrics::F64(data) => collect_points_f64(data, &mut points),
        AggregatedMetrics::U64(data) => collect_points_u64(data, &mut points),
        AggregatedMetrics::I64(data) => collect_points_i64(data, &mut points),
    }
    MetricEntry {
        name: metric.name().to_string(),
        unit: metric.unit().to_string(),
        points,
    }
}

macro_rules! collect_points_impl {
    ($fn_name:ident, $ty:ty, $to_f64:expr) => {
        fn $fn_name(data: &MetricData<$ty>, out: &mut Vec<MetricPoint>) {
            let to_f64: fn($ty) -> f64 = $to_f64;
            match data {
                MetricData::Sum(sum) => {
                    for dp in sum.data_points() {
                        out.push(MetricPoint {
                            value: to_f64(dp.value()),
                            attributes: attrs(dp.attributes()),
                        });
                    }
                }
                MetricData::Gauge(gauge) => {
                    for dp in gauge.data_points() {
                        out.push(MetricPoint {
                            value: to_f64(dp.value()),
                            attributes: attrs(dp.attributes()),
                        });
                    }
                }
                MetricData::Histogram(hist) => {
                    for dp in hist.data_points() {
                        out.push(MetricPoint {
                            // For histograms, expose the running sum so a test can
                            // assert "something was recorded" and read total magnitude.
                            value: to_f64(dp.sum()),
                            attributes: attrs(dp.attributes()),
                        });
                    }
                }
                MetricData::ExponentialHistogram(hist) => {
                    for dp in hist.data_points() {
                        out.push(MetricPoint {
                            value: to_f64(dp.sum()),
                            attributes: attrs(dp.attributes()),
                        });
                    }
                }
            }
        }
    };
}

fn attrs<'a>(it: impl Iterator<Item = &'a opentelemetry::KeyValue>) -> Vec<(String, String)> {
    it.map(|kv| (kv.key.as_str().to_string(), value_to_string(&kv.value)))
        .collect()
}

collect_points_impl!(collect_points_f64, f64, |v| v);
collect_points_impl!(collect_points_u64, u64, |v| v as f64);
collect_points_impl!(collect_points_i64, i64, |v| v as f64);

#[cfg(test)]
mod tests {
    use super::*;

    // Span capture uses a per-call isolated tracer, so this self-test needs no
    // global state and is safe under parallel test execution.
    #[test]
    fn capture_spans_records_tree_and_attributes() {
        let spans = capture_spans(|| {
            let parent = tracing::info_span!("parent.span");
            let _g = parent.enter();
            let child = tracing::info_span!("child.span", cqlite.result = "hit");
            child.in_scope(|| {});
        });

        assert!(spans.contains("parent.span"));
        assert!(spans.contains("child.span"));
        assert!(
            spans.is_parent_of("parent.span", "child.span"),
            "child.span must nest under parent.span; saw {:?}",
            spans.iter().map(|s| s.name.clone()).collect::<Vec<_>>()
        );
        let child = spans.find("child.span").expect("child present");
        assert_eq!(child.attribute("cqlite.result").as_deref(), Some("hit"));
    }

    #[test]
    fn captured_spans_from_raw_roundtrips_empty() {
        let empty = CapturedSpans::from_raw(Vec::new());
        assert!(empty.all().is_empty());
        assert!(!empty.contains("anything"));
    }
}
