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

    Ok(ObservabilityGuard {
        tracer_provider: Some(tracer_provider),
        meter_provider: Some(meter_provider),
    })
}

#[inline]
pub(crate) fn metrics_active() -> bool {
    METRICS_ACTIVE.load(Ordering::Relaxed)
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
    read_partition_lookup: Counter<u64>,
    read_bloom_checks: Counter<u64>,
    read_scan_window_refill: Counter<u64>,
    storage_open_sstables: Counter<u64>,
    storage_open_bytes: Counter<u64>,
    storage_open_tables: Counter<u64>,
    query_rows: Counter<u64>,
    query_rows_scanned: Counter<u64>,
    errors_total: Counter<u64>,
    write_mutations: Counter<u64>,
    flush_rows: Counter<u64>,
    flush_bytes: Counter<u64>,
    flush_sstables: Counter<u64>,
    write_partitions: Counter<u64>,
    write_bytes: Counter<u64>,
    compaction_rows_merged: Counter<u64>,
    compaction_bytes_written: Counter<u64>,
    compaction_sstables_in: Counter<u64>,
    compaction_sstables_out: Counter<u64>,
    compaction_tombstones_purged: Counter<u64>,
    rpc_requests: Counter<u64>,
    rpc_rows: Counter<u64>,
    rpc_bytes: Counter<u64>,
    read_duration: Histogram<f64>,
    query_duration: Histogram<f64>,
    compaction_duration: Histogram<f64>,
    wal_sync_duration: Histogram<f64>,
    flush_duration: Histogram<f64>,
    compression_ratio: Histogram<f64>,
    compaction_finalize_duration: Histogram<f64>,
    compaction_budget_requested: Histogram<f64>,
    compaction_budget_consumed: Histogram<f64>,
    rpc_duration: Histogram<f64>,
    sstables_open: Gauge<i64>,
    memtable_size_bytes: Gauge<i64>,
    memtable_rows: Gauge<i64>,
    compaction_lag: Gauge<i64>,
    rpc_in_flight: Gauge<i64>,
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
            read_partition_lookup: m
                .u64_counter(catalog::READ_PARTITION_LOOKUP)
                .with_unit(catalog::unit::DIMENSIONLESS)
                .with_description("Total partition point lookups, keyed by {result, access_path}.")
                .build(),
            read_bloom_checks: m
                .u64_counter(catalog::READ_BLOOM_CHECKS)
                .with_unit(catalog::unit::DIMENSIONLESS)
                .with_description("Total bloom/BTI-trie presence checks, keyed by {result}.")
                .build(),
            read_scan_window_refill: m
                .u64_counter(catalog::READ_SCAN_WINDOW_REFILL)
                .with_unit(catalog::unit::DIMENSIONLESS)
                .with_description("Windowed scan refills at compression-chunk boundaries.")
                .build(),
            storage_open_sstables: m
                .u64_counter(catalog::STORAGE_OPEN_SSTABLES)
                .with_unit(catalog::unit::SSTABLES)
                .with_description("SSTables discovered and opened, summed across opens.")
                .build(),
            storage_open_bytes: m
                .u64_counter(catalog::STORAGE_OPEN_BYTES)
                .with_unit(catalog::unit::BYTES)
                .with_description("On-disk Data.db bytes across SSTables discovered at open.")
                .build(),
            storage_open_tables: m
                .u64_counter(catalog::STORAGE_OPEN_TABLES)
                .with_unit(catalog::unit::DIMENSIONLESS)
                .with_description("Logical tables represented by SSTables discovered at open.")
                .build(),
            query_rows: m
                .u64_counter(catalog::QUERY_ROWS)
                .with_unit(catalog::unit::ROWS)
                .with_description("Total rows returned to callers by the query engine.")
                .build(),
            query_rows_scanned: m
                .u64_counter(catalog::QUERY_ROWS_SCANNED)
                .with_unit(catalog::unit::ROWS)
                .with_description("Rows examined by SELECT scan before filtering/projection.")
                .build(),
            errors_total: m
                .u64_counter(catalog::ERRORS_TOTAL)
                .with_unit(catalog::unit::ERRORS)
                .with_description("Total errors observed, keyed by bounded {category, subsystem}.")
                .build(),
            write_mutations: m
                .u64_counter(catalog::WRITE_MUTATIONS)
                .with_unit(catalog::unit::ROWS)
                .with_description("Mutations accepted by the write path.")
                .build(),
            flush_rows: m
                .u64_counter(catalog::FLUSH_ROWS)
                .with_unit(catalog::unit::ROWS)
                .with_description("Rows flushed from memtable to L0 SSTables.")
                .build(),
            flush_bytes: m
                .u64_counter(catalog::FLUSH_BYTES)
                .with_unit(catalog::unit::BYTES)
                .with_description("Data.db bytes produced by memtable flushes.")
                .build(),
            flush_sstables: m
                .u64_counter(catalog::FLUSH_SSTABLES)
                .with_unit(catalog::unit::SSTABLES)
                .with_description("L0 SSTables created by memtable flushes.")
                .build(),
            write_partitions: m
                .u64_counter(catalog::WRITE_PARTITIONS)
                .with_unit(catalog::unit::PARTITIONS)
                .with_description("Partitions written by the SSTable writer.")
                .build(),
            write_bytes: m
                .u64_counter(catalog::WRITE_BYTES)
                .with_unit(catalog::unit::BYTES)
                .with_description("Data.db bytes produced by the SSTable writer.")
                .build(),
            compaction_rows_merged: m
                .u64_counter(catalog::COMPACTION_ROWS_MERGED)
                .with_unit(catalog::unit::ROWS)
                .with_description("Rows emitted by compaction merge.")
                .build(),
            compaction_bytes_written: m
                .u64_counter(catalog::COMPACTION_BYTES_WRITTEN)
                .with_unit(catalog::unit::BYTES)
                .with_description("Bytes written to compaction output SSTables.")
                .build(),
            compaction_sstables_in: m
                .u64_counter(catalog::COMPACTION_SSTABLES_IN)
                .with_unit(catalog::unit::SSTABLES)
                .with_description("Input SSTables consumed by compactions.")
                .build(),
            compaction_sstables_out: m
                .u64_counter(catalog::COMPACTION_SSTABLES_OUT)
                .with_unit(catalog::unit::SSTABLES)
                .with_description("Output SSTables produced by compactions.")
                .build(),
            compaction_tombstones_purged: m
                .u64_counter(catalog::COMPACTION_TOMBSTONES_PURGED)
                .with_unit("{tombstone}")
                .with_description("Tombstones genuinely purged during compaction.")
                .build(),
            rpc_requests: m
                .u64_counter(catalog::RPC_REQUESTS)
                .with_unit(catalog::unit::DIMENSIONLESS)
                .with_description("Arrow Flight RPC requests served.")
                .build(),
            rpc_rows: m
                .u64_counter(catalog::RPC_ROWS)
                .with_unit(catalog::unit::ROWS)
                .with_description("Rows returned to Flight clients.")
                .build(),
            rpc_bytes: m
                .u64_counter(catalog::RPC_BYTES)
                .with_unit(catalog::unit::BYTES)
                .with_description("Record-batch payload bytes streamed to Flight clients.")
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
            wal_sync_duration: m
                .f64_histogram(catalog::WAL_SYNC_DURATION)
                .with_unit(catalog::unit::SECONDS)
                .with_description("WAL fsync duration in seconds.")
                .build(),
            flush_duration: m
                .f64_histogram(catalog::FLUSH_DURATION)
                .with_unit(catalog::unit::SECONDS)
                .with_description("Memtable-to-SSTable flush duration in seconds.")
                .build(),
            compression_ratio: m
                .f64_histogram(catalog::COMPRESSION_RATIO)
                .with_unit(catalog::unit::DIMENSIONLESS)
                .with_description("Per-chunk compression ratio.")
                .build(),
            compaction_finalize_duration: m
                .f64_histogram(catalog::COMPACTION_FINALIZE_DURATION)
                .with_unit(catalog::unit::SECONDS)
                .with_description("Compaction finalize duration in seconds.")
                .build(),
            compaction_budget_requested: m
                .f64_histogram(catalog::COMPACTION_BUDGET_REQUESTED)
                .with_unit(catalog::unit::SECONDS)
                .with_description("Maintenance budget requested in seconds.")
                .build(),
            compaction_budget_consumed: m
                .f64_histogram(catalog::COMPACTION_BUDGET_CONSUMED)
                .with_unit(catalog::unit::SECONDS)
                .with_description("Maintenance budget consumed in seconds.")
                .build(),
            rpc_duration: m
                .f64_histogram(catalog::RPC_DURATION)
                .with_unit(catalog::unit::SECONDS)
                .with_description("Arrow Flight RPC handler duration in seconds.")
                .build(),
            sstables_open: m
                .i64_gauge(catalog::SSTABLES_OPEN)
                .with_unit(catalog::unit::SSTABLES)
                .with_description("Number of SSTables currently held open.")
                .build(),
            memtable_size_bytes: m
                .i64_gauge(catalog::MEMTABLE_SIZE_BYTES)
                .with_unit(catalog::unit::BYTES)
                .with_description("Approximate active memtable size in bytes.")
                .build(),
            memtable_rows: m
                .i64_gauge(catalog::MEMTABLE_ROWS)
                .with_unit(catalog::unit::ROWS)
                .with_description("Rows currently buffered in the active memtable.")
                .build(),
            compaction_lag: m
                .i64_gauge(catalog::COMPACTION_LAG)
                .with_unit(catalog::unit::SSTABLES)
                .with_description("Current L0 SSTables pending compaction.")
                .build(),
            rpc_in_flight: m
                .i64_gauge(catalog::RPC_IN_FLIGHT)
                .with_unit(catalog::unit::DIMENSIONLESS)
                .with_description("Arrow Flight RPCs currently being handled.")
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
    let counter = match name {
        catalog::READ_ROWS => &i.read_rows,
        catalog::READ_BYTES => &i.read_bytes,
        catalog::READ_PARTITIONS => &i.read_partitions,
        catalog::READ_PARTITION_LOOKUP => &i.read_partition_lookup,
        catalog::READ_BLOOM_CHECKS => &i.read_bloom_checks,
        catalog::READ_SCAN_WINDOW_REFILL => &i.read_scan_window_refill,
        catalog::STORAGE_OPEN_SSTABLES => &i.storage_open_sstables,
        catalog::STORAGE_OPEN_BYTES => &i.storage_open_bytes,
        catalog::STORAGE_OPEN_TABLES => &i.storage_open_tables,
        catalog::QUERY_ROWS => &i.query_rows,
        catalog::QUERY_ROWS_SCANNED => &i.query_rows_scanned,
        catalog::ERRORS_TOTAL => &i.errors_total,
        catalog::WRITE_MUTATIONS => &i.write_mutations,
        catalog::FLUSH_ROWS => &i.flush_rows,
        catalog::FLUSH_BYTES => &i.flush_bytes,
        catalog::FLUSH_SSTABLES => &i.flush_sstables,
        catalog::WRITE_PARTITIONS => &i.write_partitions,
        catalog::WRITE_BYTES => &i.write_bytes,
        catalog::COMPACTION_ROWS_MERGED => &i.compaction_rows_merged,
        catalog::COMPACTION_BYTES_WRITTEN => &i.compaction_bytes_written,
        catalog::COMPACTION_SSTABLES_IN => &i.compaction_sstables_in,
        catalog::COMPACTION_SSTABLES_OUT => &i.compaction_sstables_out,
        catalog::COMPACTION_TOMBSTONES_PURGED => &i.compaction_tombstones_purged,
        catalog::RPC_REQUESTS => &i.rpc_requests,
        catalog::RPC_ROWS => &i.rpc_rows,
        catalog::RPC_BYTES => &i.rpc_bytes,
        _ => {
            meter().u64_counter(name).build().add(value, attributes);
            return;
        }
    };
    counter.add(value, attributes);
}

/// Record into an f64 histogram identified by a catalog name.
pub(crate) fn record_histogram(name: &'static str, value: f64, attributes: &[KeyValue]) {
    let i = instruments();
    let hist = match name {
        catalog::READ_DURATION => &i.read_duration,
        catalog::QUERY_DURATION => &i.query_duration,
        catalog::COMPACTION_DURATION => &i.compaction_duration,
        catalog::WAL_SYNC_DURATION => &i.wal_sync_duration,
        catalog::FLUSH_DURATION => &i.flush_duration,
        catalog::COMPRESSION_RATIO => &i.compression_ratio,
        catalog::COMPACTION_FINALIZE_DURATION => &i.compaction_finalize_duration,
        catalog::COMPACTION_BUDGET_REQUESTED => &i.compaction_budget_requested,
        catalog::COMPACTION_BUDGET_CONSUMED => &i.compaction_budget_consumed,
        catalog::RPC_DURATION => &i.rpc_duration,
        _ => {
            meter()
                .f64_histogram(name)
                .build()
                .record(value, attributes);
            return;
        }
    };
    hist.record(value, attributes);
}

/// Record an i64 gauge identified by a catalog name.
pub(crate) fn record_gauge(name: &'static str, value: i64, attributes: &[KeyValue]) {
    let i = instruments();
    let gauge = match name {
        catalog::SSTABLES_OPEN => &i.sstables_open,
        catalog::MEMTABLE_SIZE_BYTES => &i.memtable_size_bytes,
        catalog::MEMTABLE_ROWS => &i.memtable_rows,
        catalog::COMPACTION_LAG => &i.compaction_lag,
        catalog::RPC_IN_FLIGHT => &i.rpc_in_flight,
        _ => {
            meter().i64_gauge(name).build().record(value, attributes);
            return;
        }
    };
    gauge.record(value, attributes);
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

    #[test]
    fn traceparent_none_empty_and_invalid_are_noops() {
        // Must not panic for absent / blank / malformed headers.
        let span = tracing::info_span!("test");
        set_span_parent_from_traceparent(&span, None);
        set_span_parent_from_traceparent(&span, Some("   "));
        set_span_parent_from_traceparent(&span, Some("not-a-traceparent"));
    }

    #[test]
    fn traceparent_valid_header_is_accepted() {
        // A well-formed W3C traceparent should parse and re-parent without panic.
        let span = tracing::info_span!("test");
        let valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        set_span_parent_from_traceparent(&span, Some(valid));
    }
}
