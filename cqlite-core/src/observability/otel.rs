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

use std::sync::OnceLock;
use std::time::Duration;

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

    Ok(ObservabilityGuard {
        tracer_provider: Some(tracer_provider),
        meter_provider: Some(meter_provider),
    })
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
fn meter() -> &'static Meter {
    static METER: OnceLock<Meter> = OnceLock::new();
    METER.get_or_init(|| global::meter(SCOPE))
}

/// Lazily-built, cached instruments for every catalog metric. Building an
/// instrument on each record call is wasteful (re-registration overhead and
/// possible duplicate-instrument churn), so all catalog instruments are
/// constructed once and reused. Non-catalog names fall back to an ad-hoc
/// instrument so call sites never silently drop data.
struct Instruments {
    read_rows: Counter<u64>,
    read_bytes: Counter<u64>,
    read_partitions: Counter<u64>,
    query_rows: Counter<u64>,
    errors_total: Counter<u64>,
    read_duration: Histogram<f64>,
    query_duration: Histogram<f64>,
    compaction_duration: Histogram<f64>,
    sstables_open: Gauge<i64>,
}

fn instruments() -> &'static Instruments {
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let m = meter();
        Instruments {
            read_rows: m
                .u64_counter(catalog::READ_ROWS)
                .with_unit(catalog::unit::ROWS)
                .with_description("Total rows materialised by the read path.")
                .build(),
            read_bytes: m
                .u64_counter(catalog::READ_BYTES)
                .with_unit(catalog::unit::BYTES)
                .with_description("Total bytes read from Data.db (post-decompression).")
                .build(),
            read_partitions: m
                .u64_counter(catalog::READ_PARTITIONS)
                .with_unit(catalog::unit::PARTITIONS)
                .with_description("Total partitions scanned.")
                .build(),
            query_rows: m
                .u64_counter(catalog::QUERY_ROWS)
                .with_unit(catalog::unit::ROWS)
                .with_description("Total rows returned to callers by the query engine.")
                .build(),
            errors_total: m
                .u64_counter(catalog::ERRORS_TOTAL)
                .with_unit(catalog::unit::ERRORS)
                .with_description("Total errors observed, keyed by bounded {category, subsystem}.")
                .build(),
            read_duration: m
                .f64_histogram(catalog::READ_DURATION)
                .with_unit(catalog::unit::SECONDS)
                .with_description("Single read/scan operation duration in seconds.")
                .build(),
            query_duration: m
                .f64_histogram(catalog::QUERY_DURATION)
                .with_unit(catalog::unit::SECONDS)
                .with_description("End-to-end query execution duration in seconds.")
                .build(),
            compaction_duration: m
                .f64_histogram(catalog::COMPACTION_DURATION)
                .with_unit(catalog::unit::SECONDS)
                .with_description("Compaction run duration in seconds.")
                .build(),
            sstables_open: m
                .i64_gauge(catalog::SSTABLES_OPEN)
                .with_unit(catalog::unit::SSTABLES)
                .with_description("Number of SSTables currently held open.")
                .build(),
        }
    })
}

/// Add to a u64 counter identified by a catalog name.
///
/// Catalog names use the cached instrument; unknown names build an ad-hoc
/// counter so call sites never silently drop data (catalog names should always
/// be used).
pub(crate) fn add_counter(name: &'static str, value: u64, attributes: &[KeyValue]) {
    let i = instruments();
    let counter = if name == catalog::READ_ROWS {
        &i.read_rows
    } else if name == catalog::READ_BYTES {
        &i.read_bytes
    } else if name == catalog::READ_PARTITIONS {
        &i.read_partitions
    } else if name == catalog::QUERY_ROWS {
        &i.query_rows
    } else if name == catalog::ERRORS_TOTAL {
        &i.errors_total
    } else {
        meter().u64_counter(name).build().add(value, attributes);
        return;
    };
    counter.add(value, attributes);
}

/// Record into an f64 histogram identified by a catalog name.
pub(crate) fn record_histogram(name: &'static str, value: f64, attributes: &[KeyValue]) {
    let i = instruments();
    let hist = if name == catalog::READ_DURATION {
        &i.read_duration
    } else if name == catalog::QUERY_DURATION {
        &i.query_duration
    } else if name == catalog::COMPACTION_DURATION {
        &i.compaction_duration
    } else {
        meter().f64_histogram(name).build().record(value, attributes);
        return;
    };
    hist.record(value, attributes);
}

/// Record an i64 gauge identified by a catalog name.
pub(crate) fn record_gauge(name: &'static str, value: i64, attributes: &[KeyValue]) {
    let i = instruments();
    if name == catalog::SSTABLES_OPEN {
        i.sstables_open.record(value, attributes);
    } else {
        meter().i64_gauge(name).build().record(value, attributes);
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

/// Convenience for the default flush timeout used by tests.
#[allow(dead_code)]
pub(crate) const DEFAULT_FLUSH: Duration = Duration::from_secs(1);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_disabled_returns_inert_guard() {
        let cfg = ObservabilityConfig::builder().enabled(false).build();
        let guard = init(cfg).expect("inert init never fails");
        assert!(!guard.is_active());
        guard.force_flush(); // no-op, must not panic
    }

    #[test]
    fn sampler_builds_for_ratio() {
        // Just exercise the builder for coverage; sampler has no public getter.
        let _ = build_sampler(0.5);
        let _ = build_sampler(2.0); // clamps internally
    }
}
