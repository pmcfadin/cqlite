//! OpenTelemetry runtime wiring (feature = "observability").
//!
//! This module is only compiled when the `observability` feature is on. It owns
//! the OTLP trace + metric exporter setup, the parent-based trace-ID-ratio
//! sampler, the global providers, the lazily-built metric instruments keyed by
//! the [`catalog`](crate::observability::catalog) names, and the
//! [`ObservabilityGuard`] that flushes/shuts down on drop.
//!
//! The public re-exports live in [`crate::observability`]; everything here is
//! reached through those.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use super::otel_instruments::{instruments, Instruments};
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{MetricExporter, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions as semconv;

use crate::error::{Error, Result};
use crate::observability::catalog;
use crate::observability::config::{ObservabilityConfig, OtelProtocol};

/// Instrumentation scope name for all CQLite telemetry.
const SCOPE: &str = "cqlite";

/// Concrete SDK tracer provider stashed by [`init`] so [`tracing_layer`] can
/// build its layer from an `SdkTracer` (which implements `PreSampledTracer`).
/// When `init` is not called (or is inert), a default no-export provider is
/// lazily created so the layer type stays monomorphic and simply drops spans.
static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();
static METRICS_ACTIVE: AtomicBool = AtomicBool::new(false);

/// Build the OTel `Resource` describing this process.
fn build_resource(cfg: &ObservabilityConfig) -> Resource {
    Resource::builder()
        .with_service_name(cfg.service_name.clone())
        .with_attribute(KeyValue::new(
            semconv::attribute::SERVICE_VERSION,
            cfg.service_version.clone(),
        ))
        .build()
}

/// Parent-based trace-ID-ratio sampler from the configured ratio.
fn build_sampler(ratio: f64) -> Sampler {
    Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio.clamp(0.0, 1.0))))
}

fn otlp_protocol(p: OtelProtocol) -> Protocol {
    match p {
        OtelProtocol::Grpc => Protocol::Grpc,
        OtelProtocol::Http => Protocol::HttpBinary,
    }
}

fn build_span_exporter(cfg: &ObservabilityConfig) -> Result<SpanExporter> {
    let builder = match cfg.protocol {
        OtelProtocol::Grpc => SpanExporter::builder()
            .with_tonic()
            .with_endpoint(cfg.endpoint.clone())
            .with_protocol(otlp_protocol(cfg.protocol))
            .with_timeout(cfg.timeout)
            .build(),
        OtelProtocol::Http => SpanExporter::builder()
            .with_http()
            .with_endpoint(cfg.endpoint.clone())
            .with_protocol(otlp_protocol(cfg.protocol))
            .with_timeout(cfg.timeout)
            .build(),
    };
    builder.map_err(|e| Error::configuration(format!("OTLP span exporter init failed: {e}")))
}

fn build_metric_exporter(cfg: &ObservabilityConfig) -> Result<MetricExporter> {
    let builder = match cfg.protocol {
        OtelProtocol::Grpc => MetricExporter::builder()
            .with_tonic()
            .with_endpoint(cfg.endpoint.clone())
            .with_protocol(otlp_protocol(cfg.protocol))
            .with_timeout(cfg.timeout)
            .build(),
        OtelProtocol::Http => MetricExporter::builder()
            .with_http()
            .with_endpoint(cfg.endpoint.clone())
            .with_protocol(otlp_protocol(cfg.protocol))
            .with_timeout(cfg.timeout)
            .build(),
    };
    builder.map_err(|e| Error::configuration(format!("OTLP metric exporter init failed: {e}")))
}

/// RAII guard returned by [`init`](crate::observability::init).
///
/// Dropping it force-flushes and shuts down the trace and metric providers so
/// buffered telemetry is exported before the process exits. An *inert* guard
/// (built when observability is disabled) does nothing on drop.
pub struct ObservabilityGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl std::fmt::Debug for ObservabilityGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservabilityGuard")
            .field("active", &self.tracer_provider.is_some())
            .finish()
    }
}

impl ObservabilityGuard {
    /// An inert guard that installs and flushes nothing.
    pub(crate) fn inert() -> Self {
        Self {
            tracer_provider: None,
            meter_provider: None,
        }
    }

    /// Whether this guard owns live exporters.
    pub fn is_active(&self) -> bool {
        self.tracer_provider.is_some() || self.meter_provider.is_some()
    }

    /// Explicitly flush all pending telemetry without shutting down. Useful for
    /// tests; a normal program can just rely on the drop behaviour.
    pub fn force_flush(&self) {
        if let Some(tp) = &self.tracer_provider {
            let _ = tp.force_flush();
        }
        if let Some(mp) = &self.meter_provider {
            let _ = mp.force_flush();
        }
    }
}

impl Drop for ObservabilityGuard {
    fn drop(&mut self) {
        if let Some(tp) = &self.tracer_provider {
            let _ = tp.force_flush();
            let _ = tp.shutdown();
        }
        if let Some(mp) = &self.meter_provider {
            let _ = mp.force_flush();
            let _ = mp.shutdown();
            METRICS_ACTIVE.store(false, Ordering::Relaxed);
        }
    }
}

/// Initialise OTLP trace + metric export and install the global providers.
///
/// Returns an inert [`ObservabilityGuard`] when `cfg.enabled == false` (no
/// exporters, no global providers touched). Otherwise installs SDK providers as
/// the OpenTelemetry globals and returns a guard that flushes on drop.
///
/// Note: this does NOT install a `tracing` subscriber. Callers compose their own
/// subscriber and add [`tracing_layer`] to it — see the module docs on
/// [`crate::observability`].
pub fn init(cfg: ObservabilityConfig) -> Result<ObservabilityGuard> {
    // Issue #2163 (roborev r4): plumb the presence-oracle verification config
    // field into the runtime switch the read path actually consults — applied
    // UNCONDITIONALLY (even when `cfg.enabled == false`, i.e. OTel export itself
    // is off), since the switch also drives the confirmation-scan + warning-log
    // side effect independent of metric export. `apply_config` itself honors
    // env-overrides-config precedence.
    crate::storage::sstable::reader::presence_verification::apply_config(
        cfg.verify_presence_oracle,
    );

    if !cfg.enabled {
        return Ok(ObservabilityGuard::inert());
    }

    let resource = build_resource(&cfg);

    // Tracing pipeline.
    let span_exporter = build_span_exporter(&cfg)?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_sampler(build_sampler(cfg.sampling_ratio))
        .with_resource(resource.clone())
        .build();
    // Set the global provider (for cross-crate context propagation) and stash a
    // concrete clone so `tracing_layer` can build its layer from the SDK tracer
    // (the boxed global tracer does not implement `PreSampledTracer`).
    let _ = TRACER_PROVIDER.set(tracer_provider.clone());
    global::set_tracer_provider(tracer_provider.clone());

    // Metrics pipeline.
    let metric_exporter = build_metric_exporter(&cfg)?;
    let reader = PeriodicReader::builder(metric_exporter).build();
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();
    global::set_meter_provider(meter_provider.clone());
    METRICS_ACTIVE.store(true, Ordering::Relaxed);

    // Eagerly seed the always-on baseline instruments so a fresh scrape of a
    // just-started server shows them at 0 rather than absent (issue #2288).
    register_baseline_instruments();

    Ok(ObservabilityGuard {
        tracer_provider: Some(tracer_provider),
        meter_provider: Some(meter_provider),
    })
}

#[inline]
pub(crate) fn metrics_active() -> bool {
    METRICS_ACTIVE.load(Ordering::Relaxed)
}

/// Eagerly emit a zero data point for the always-on baseline instruments so they
/// are visible in a scrape of a freshly-started server, before any real activity
/// (issue #2288).
///
/// `cqlite.errors.total` otherwise registers *lazily* — it appears in a metrics
/// backend only on its first increment — so "metric name absent from the
/// backend" was ambiguous between *no errors occurred* and *error counting isn't
/// wired*. A single `add(0)` builds the instrument and publishes a `0` series so
/// absence unambiguously means "not wired". The baseline uses an empty attribute
/// set (no invented `{category, subsystem}` values that would pollute the bounded
/// taxonomy); real errors add their own labeled series alongside it.
pub(crate) fn register_baseline_instruments() {
    // Routed through the emit path so this seeds the SAME instrument a real error
    // increments; there is no per-metric field to reach for any more (#1705 F3).
    add_counter(catalog::ERRORS_TOTAL, 0, &[]);
}

#[cfg(feature = "observability-testing")]
pub(crate) fn set_metrics_active_for_testing() {
    METRICS_ACTIVE.store(true, Ordering::Relaxed);
}

/// Return the `tracing` layer that bridges spans/events into OpenTelemetry, or
/// `None` if observability has not been initialised.
///
/// **Call [`init`] before composing this layer.** The layer is bound at
/// construction time to the exporting tracer provider that `init` installs, so
/// it can only be built once that provider exists. When `init` has not run (or
/// ran with a disabled config), this returns `None` — an honestly inert result —
/// rather than a layer permanently bound to a non-exporting provider. `Option<L>`
/// itself implements `Layer`, so the `None` case is a no-op and callers compose
/// it directly:
///
/// ```ignore
/// use tracing_subscriber::prelude::*;
/// let _guard = cqlite_core::observability::init(cfg)?;   // installs the provider
/// tracing_subscriber::registry()
///     .with(tracing_subscriber::fmt::layer())
///     .with(tracing_subscriber::EnvFilter::from_default_env())
///     .with(cqlite_core::observability::tracing_layer())  // Some(..) after init, else None
///     .init();
/// ```
pub fn tracing_layer<S>() -> Option<impl tracing_subscriber::Layer<S>>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    // Only build a layer once `init` has installed the exporting provider. No
    // ephemeral fallback: a layer bound to a throwaway no-export provider can
    // never start exporting, so returning `None` (inert) is the correct
    // behaviour when uninitialised.
    let provider = TRACER_PROVIDER.get()?;
    let tracer = provider.tracer(SCOPE);
    Some(tracing_opentelemetry::layer().with_tracer(tracer))
}

/// The global CQLite [`Meter`], cached after first use.
pub(super) fn meter() -> &'static Meter {
    static METER: OnceLock<Meter> = OnceLock::new();
    METER.get_or_init(|| global::meter(SCOPE))
}

/// Resolve a catalog metric name to its pre-built u64 counter.
///
/// **ONE implementation, two callers** (issue #1705, roborev B2): the emit path
/// [`add_counter`] below, and the registration-completeness guard in `otel_tests.rs`
/// which asks this function to RESOLVE every catalogued name — an affirmative
/// runtime measurement of "an instrument exists for this name", rather than a text
/// search for a mention of it. `None` means the name has no dedicated instrument
/// and the caller falls back to an ad-hoc one, so a guard can distinguish a wired
/// metric from an unwired one by calling this.
///
/// **No per-metric code** (issue #1705, roborev F3). This used to be a hand-written
/// `catalog::NAME => &i.field` match, i.e. a SECOND statement of each metric's name
/// that could disagree with the one used to build the instrument — a mis-wired arm
/// emitted under the wrong series while every guard stayed green. The instruments
/// are now keyed by the name they were constructed with
/// (`otel_instruments::Registry`), so resolution is a lookup and a mismatch is
/// unrepresentable. Re-introducing a per-metric arm here reds
/// `catalog_tests::a_handwritten_dispatch_arm_is_rejected_because_it_can_mis_wire`.
pub(super) fn counter_for<'a>(i: &'a Instruments, name: &str) -> Option<&'a Counter<u64>> {
    i.counters.get(name)
}

/// Whether the ad-hoc fallback must REFUSE to build an instrument for `name`.
///
/// `true` only for a name DECLARED in [`catalog::STATS_ONLY_METRICS`], whose
/// contract is that it is surfaced through `MemoryStats` and is never on a scrape
/// (issue #1705). An unknown/uncatalogued name returns `false` and keeps the ad-hoc
/// fallback, so no call site silently drops data.
///
/// Called ONLY from the `None` (ad-hoc) branch of the three emit functions, never
/// on the resolved hot path. No panic and no `debug_assert!`: refusing means *not
/// creating an instrument*, and a `debug_assert!` would panic in every debug/test
/// build — including the guard that proves this refusal happens — so misuse is
/// surfaced as a `tracing` event instead.
fn stats_only_refuses_instrument(name: &str) -> bool {
    let refused = catalog::STATS_ONLY_METRICS.iter().any(|m| m.name == name);
    if refused {
        tracing::debug!(
            metric = name,
            "stats-only metric emitted through the OTel path; no instrument created"
        );
    }
    refused
}

/// Add to a u64 counter identified by a catalog name.
///
/// Catalog names use the cached instrument; unknown names build an ad-hoc
/// counter so call sites never silently drop data (catalog names should always
/// be used). A declared stats-only name creates nothing — see
/// [`stats_only_refuses_instrument`].
pub(crate) fn add_counter(name: &'static str, value: u64, attributes: &[KeyValue]) {
    add_counter_with(instruments(), meter(), name, value, attributes)
}

/// [`add_counter`] against an explicit instrument set and meter, so a guard can
/// exercise the real emit path without touching the process-global `OnceLock`s.
pub(super) fn add_counter_with(
    i: &Instruments,
    m: &Meter,
    name: &str,
    value: u64,
    attributes: &[KeyValue],
) {
    match counter_for(i, name) {
        Some(counter) => counter.add(value, attributes),
        None => {
            if stats_only_refuses_instrument(name) {
                return;
            }
            m.u64_counter(name.to_string())
                .build()
                .add(value, attributes)
        }
    }
}

/// Resolve a catalog metric name to its pre-built f64 histogram. Shared by the
/// emit path and the registration guard, and a keyed lookup rather than a
/// per-metric match — see [`counter_for`].
pub(super) fn histogram_for<'a>(i: &'a Instruments, name: &str) -> Option<&'a Histogram<f64>> {
    i.histograms.get(name)
}

/// Record into an f64 histogram identified by a catalog name. A declared
/// stats-only name creates nothing — see [`stats_only_refuses_instrument`].
pub(crate) fn record_histogram(name: &'static str, value: f64, attributes: &[KeyValue]) {
    record_histogram_with(instruments(), meter(), name, value, attributes)
}

/// [`record_histogram`] against an explicit instrument set and meter — see
/// [`add_counter_with`].
pub(super) fn record_histogram_with(
    i: &Instruments,
    m: &Meter,
    name: &str,
    value: f64,
    attributes: &[KeyValue],
) {
    match histogram_for(i, name) {
        Some(hist) => hist.record(value, attributes),
        None => {
            if stats_only_refuses_instrument(name) {
                return;
            }
            m.f64_histogram(name.to_string())
                .build()
                .record(value, attributes)
        }
    }
}

/// Resolve a catalog metric name to its pre-built i64 gauge. Shared by the emit
/// path and the registration guard, and a keyed lookup rather than a per-metric
/// match — see [`counter_for`].
pub(super) fn gauge_for<'a>(i: &'a Instruments, name: &str) -> Option<&'a Gauge<i64>> {
    i.gauges.get(name)
}

/// Record an i64 gauge identified by a catalog name. A declared stats-only name
/// creates nothing — see [`stats_only_refuses_instrument`].
pub(crate) fn record_gauge(name: &'static str, value: i64, attributes: &[KeyValue]) {
    record_gauge_with(instruments(), meter(), name, value, attributes)
}

/// [`record_gauge`] against an explicit instrument set and meter — see
/// [`add_counter_with`].
pub(super) fn record_gauge_with(
    i: &Instruments,
    m: &Meter,
    name: &str,
    value: i64,
    attributes: &[KeyValue],
) {
    match gauge_for(i, name) {
        Some(gauge) => gauge.record(value, attributes),
        None => {
            if stats_only_refuses_instrument(name) {
                return;
            }
            m.i64_gauge(name.to_string())
                .build()
                .record(value, attributes)
        }
    }
}

/// Mark the currently-active `tracing` span as errored and tag it with the
/// telemetry error category. Maps to OTel `otel.status_code = ERROR` plus the
/// bounded `cqlite.error.category` attribute. No raw error message is recorded.
pub(crate) fn mark_span_error(category: crate::observability::ErrorCategory) {
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let span = tracing::Span::current();
    span.set_attribute("otel.status_code", "ERROR");
    span.set_attribute(catalog::attr::ERROR_CATEGORY, category.as_str());
}

/// Extract a W3C `traceparent` header and set the resulting remote context as
/// the parent of `span`. No-op when `traceparent` is absent/empty/unparseable.
///
/// Uses the standard [`TraceContextPropagator`] to parse the header into an
/// OpenTelemetry [`Context`](opentelemetry::Context), then
/// `tracing-opentelemetry`'s `set_parent` to link the `tracing` span to that
/// remote span. Only the `traceparent` header is consulted (no baggage /
/// tracestate), keeping the surface minimal and the behaviour identical to
/// other CQLite hosts.
pub(crate) fn set_span_parent_from_traceparent(span: &tracing::Span, traceparent: Option<&str>) {
    use opentelemetry::propagation::{Extractor, TextMapPropagator};
    use opentelemetry::trace::TraceContextExt;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let header = match traceparent {
        Some(h) if !h.trim().is_empty() => h,
        _ => return,
    };

    /// Single-header carrier exposing only `traceparent` to the propagator.
    struct TraceParentCarrier<'a>(&'a str);
    impl Extractor for TraceParentCarrier<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            if key.eq_ignore_ascii_case("traceparent") {
                Some(self.0)
            } else {
                None
            }
        }
        fn keys(&self) -> Vec<&str> {
            vec!["traceparent"]
        }
    }

    let propagator = TraceContextPropagator::new();
    let cx = propagator.extract(&TraceParentCarrier(header));
    // Only re-parent when extraction produced a valid remote span context;
    // otherwise leave the span attached to its in-process parent.
    if cx.span().span_context().is_valid() {
        span.set_parent(cx);
    }
}

/// Convenience for the default flush timeout used by tests.
#[allow(dead_code)]
pub(crate) const DEFAULT_FLUSH: Duration = Duration::from_secs(1);

/// Tests live in a sibling file so this module stays inside the campsite-rule
/// source target (#1116).
#[cfg(test)]
#[path = "otel_tests.rs"]
mod tests;
