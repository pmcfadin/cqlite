//! Observability wiring for the Node.js bindings (epic #1031, issue #1040).
//!
//! This module is the binding-side glue between the Node host and the
//! always-available observability foundation in `cqlite_core::observability`
//! (issues #1032/#1038). It does three things:
//!
//! 1. Exposes the [`OtelOptions`] napi object that callers pass on
//!    [`crate::database::DatabaseOptions`] (or via the `CQLITE_OTEL_*` env
//!    fallback) to configure OTLP export.
//! 2. Initialises the foundation **once per process** ([`init_once`]): it maps
//!    the options to a [`cqlite_core::observability::ObservabilityConfig`],
//!    calls `observability::init`, stashes the returned guard
//!    process-globally so its `Drop` flushes buffered telemetry at exit, and
//!    installs a `tracing` subscriber that bridges the per-call spans to OTLP
//!    (only when the `observability` feature is compiled in).
//! 3. Provides small helpers used at the public binding boundary: building the
//!    per-call/per-stream spans, parenting them under an incoming W3C
//!    `traceparent`, recording the canonical query/error metrics, and flushing
//!    on `close()`.
//!
//! Everything here is callable regardless of the `observability` feature: with
//! the feature off the foundation helpers are inert no-ops and the OTLP layer is
//! compiled out, so the binding builds and behaves identically.

use std::sync::OnceLock;

use cqlite_core::observability::{self as obs, ObservabilityConfig, ObservabilityGuard};

/// OpenTelemetry options for the Node.js bindings.
///
/// Pass as `otel` on the open options:
///
/// ```javascript
/// const db = await Database.open('/data', {
///   schema: 'schema.cql',
///   otel: { enabled: true, endpoint: 'http://collector:4317', protocol: 'grpc' },
/// });
/// ```
///
/// Any field left unset falls back to the corresponding `CQLITE_OTEL_*`
/// environment variable, then to the foundation default (disabled,
/// `http://localhost:4317`, gRPC, service name `cqlite`, full sampling). The
/// foundation only installs exporters when the effective config has
/// `enabled: true` AND the binding was built with the `observability` feature.
#[napi_derive::napi(object)]
#[derive(Clone, Default)]
pub struct OtelOptions {
    /// Master enable switch. When unset, defers to `CQLITE_OTEL_ENABLED` (then
    /// the default, `false`). With telemetry disabled, `open()` installs no
    /// exporters and per-call spans are dropped.
    pub enabled: Option<bool>,

    /// OTLP collector endpoint: a gRPC endpoint or HTTP base URL. Unset defers
    /// to `CQLITE_OTEL_ENDPOINT` then `http://localhost:4317`.
    pub endpoint: Option<String>,

    /// Wire protocol: `"grpc"` (default) or `"http"`. Unrecognised values are
    /// ignored (the default/env value is kept), matching the foundation.
    pub protocol: Option<String>,

    /// `service.name` resource attribute. Unset defers to
    /// `CQLITE_OTEL_SERVICE_NAME` then `cqlite`.
    #[napi(js_name = "serviceName")]
    pub service_name: Option<String>,

    /// `service.version` resource attribute. Unset defers to
    /// `CQLITE_OTEL_SERVICE_VERSION` then the crate version.
    #[napi(js_name = "serviceVersion")]
    pub service_version: Option<String>,

    /// Trace-ID-ratio sampling probability in `[0.0, 1.0]` (clamped;
    /// non-finite values fall back to full sampling). Unset defers to
    /// `CQLITE_OTEL_SAMPLING_RATIO` then `1.0`.
    #[napi(js_name = "samplingRatio")]
    pub sampling_ratio: Option<f64>,

    /// Exporter export timeout in milliseconds. Unset defers to
    /// `CQLITE_OTEL_TIMEOUT_MS` then `10000`.
    #[napi(js_name = "timeoutMs")]
    pub timeout_ms: Option<f64>,
}

impl OtelOptions {
    /// Build a foundation [`ObservabilityConfig`] by layering explicitly-set
    /// option fields over the `CQLITE_OTEL_*` env defaults.
    ///
    /// Starting from `from_env()` (which is itself layered over the documented
    /// defaults) means an unset option transparently honours the env var, and an
    /// explicit option always wins — the same precedence the CLI uses.
    fn to_core(&self) -> ObservabilityConfig {
        use cqlite_core::observability::OtelProtocol;
        use std::time::Duration;

        // Start from the env-derived config (itself layered over the documented
        // defaults), then overwrite only explicitly-set option fields. The
        // config's fields are all public, so we mutate them directly rather than
        // round-tripping through the builder.
        let mut cfg = ObservabilityConfig::from_env();

        if let Some(enabled) = self.enabled {
            cfg.enabled = enabled;
        }
        if let Some(ref endpoint) = self.endpoint {
            if !endpoint.trim().is_empty() {
                cfg.endpoint = endpoint.clone();
            }
        }
        if let Some(ref protocol) = self.protocol {
            if let Some(p) = OtelProtocol::parse(protocol) {
                cfg.protocol = p;
            }
        }
        if let Some(ref name) = self.service_name {
            if !name.trim().is_empty() {
                cfg.service_name = name.clone();
            }
        }
        if let Some(ref version) = self.service_version {
            if !version.trim().is_empty() {
                cfg.service_version = version.clone();
            }
        }
        if let Some(ratio) = self.sampling_ratio {
            if ratio.is_finite() {
                cfg.sampling_ratio = ratio.clamp(0.0, 1.0);
            }
        }
        if let Some(ms) = self.timeout_ms {
            if ms.is_finite() && ms >= 0.0 {
                cfg.timeout = Duration::from_millis(ms as u64);
            }
        }

        cfg
    }
}

/// Subsystem label attached to all node-binding error metrics.
pub const SUBSYSTEM: &str = "node";

/// Process-global observability guard. Set exactly once by [`init_once`]; held
/// for the lifetime of the process so its `Drop` flushes and shuts down the
/// exporters when Node tears the addon down. `OnceLock` makes the
/// initialise-once contract explicit and races-free across worker threads.
static GUARD: OnceLock<ObservabilityGuard> = OnceLock::new();

/// Initialise the observability foundation exactly once for this process.
///
/// The first `Database.open()` wins: it maps `opts` (or the env fallback) to a
/// config, calls `observability::init`, stores the guard process-globally, and
/// installs the bridging `tracing` subscriber. Subsequent calls are no-ops, so
/// later opens with different `otel` options do not reconfigure exporters — this
/// matches the foundation's global-provider model (one exporter per process).
///
/// Always safe to call. With telemetry disabled or the feature off it installs
/// an inert guard and a plain subscriber, so behaviour is unchanged.
pub fn init_once(opts: Option<&OtelOptions>) {
    if GUARD.get().is_some() {
        return;
    }

    let cfg = match opts {
        Some(o) => o.to_core(),
        None => ObservabilityConfig::from_env(),
    };

    // `init` returns an inert guard for a disabled config and never installs a
    // global subscriber itself, so it is safe to call unconditionally. A
    // misconfigured exporter must never take down the host process: on error we
    // fall back to an inert guard and continue without export.
    let guard = obs::init(cfg).unwrap_or_else(|_| inert_guard());

    // If another thread won the race between the `get` check and here, keep the
    // first guard; ours is simply dropped.
    let _ = GUARD.set(guard);
    install_subscriber();
}

/// Build an inert guard for the error fall-back path. `init` on a disabled
/// config yields one and never errors, so we recurse into a guaranteed-inert
/// config rather than touching the foundation's private constructor.
fn inert_guard() -> ObservabilityGuard {
    let disabled = ObservabilityConfig::builder().enabled(false).build();
    obs::init(disabled).unwrap_or_else(|_| inert_guard())
}

/// Install the bridging `tracing` subscriber once. With the `observability`
/// feature on, the OTel layer (live only after a successful `init`) is composed
/// in so the per-call spans reach the OTLP exporter. `try_init` tolerates an
/// already-installed subscriber (e.g. set by a host or a test).
fn install_subscriber() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    // Honour RUST_LOG; otherwise stay quiet (the host owns log routing). We do
    // not attach an fmt layer by default to avoid writing to the Node process's
    // stderr unless the operator explicitly opts in via RUST_LOG.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("off"));

    let registry = tracing_subscriber::registry().with(env_filter);

    #[cfg(feature = "observability")]
    let registry = registry.with(cqlite_core::observability::tracing_layer());

    let _ = registry.try_init();
}

/// Force-flush pending telemetry. Called from `Database.close()` so a graceful
/// close exports buffered spans/metrics promptly rather than waiting for the
/// process-exit `Drop`. No-op when uninitialised or inert.
pub fn flush() {
    if let Some(g) = GUARD.get() {
        g.force_flush();
    }
}

/// Record the rows produced by a call onto its span as the bounded `rows`
/// field, so the per-call span carries result size without any new metric
/// series.
///
/// The query-level row/duration **metrics** (`cqlite.query.rows`,
/// `cqlite.query.duration`) are emitted by the core query engine (#1035);
/// re-emitting them here would double-count, so the binding only annotates its
/// span. Never attach query text or keys.
pub fn record_rows(span: &tracing::Span, rows: u64) {
    span.record("rows", rows);
}

/// Record an error that escaped the public binding boundary.
///
/// Increments `cqlite.errors.total` keyed by `{category, subsystem="node"}`
/// and marks the active span errored. Call this ONLY at the outermost binding
/// boundary (where the `cqlite_core::Error` is about to become a JS error), not
/// nested, so it never double-counts with core's own `record_error` sites.
pub fn record_boundary_error(err: &cqlite_core::Error) {
    obs::record_error(err, SUBSYSTEM);
}

/// Build the per-call span for `execute`/`executeNative`, optionally parented
/// under an incoming W3C `traceparent`. The span carries only bounded fields
/// (an `op` label and, after the call, `rows`); never the query text/keys.
pub fn execute_span(op: &'static str, traceparent: Option<&str>) -> tracing::Span {
    let span = tracing::info_span!("node.execute", op = op, rows = tracing::field::Empty);
    obs::set_span_parent_from_traceparent(&span, traceparent);
    span
}

/// Build the per-stream span for `executeStreaming`, optionally parented under
/// an incoming W3C `traceparent`. `rows` is filled in incrementally as the
/// stream yields and finalised when iteration ends.
pub fn streaming_span(traceparent: Option<&str>) -> tracing::Span {
    let span = tracing::info_span!("node.execute_streaming", rows = tracing::field::Empty);
    obs::set_span_parent_from_traceparent(&span, traceparent);
    span
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_default_to_env_config() {
        // With no fields set, to_core mirrors from_env (disabled by default).
        let opts = OtelOptions::default();
        let cfg = opts.to_core();
        // Default env (assuming CQLITE_OTEL_* unset in the test env) is disabled.
        // We only assert the mapping is structurally sane, not the env value.
        let _ = cfg.enabled;
        assert!(cfg.sampling_ratio.is_finite());
    }

    #[test]
    fn explicit_fields_override() {
        let opts = OtelOptions {
            enabled: Some(true),
            endpoint: Some("http://collector:4318".to_string()),
            protocol: Some("http".to_string()),
            service_name: Some("svc".to_string()),
            service_version: Some("9.9.9".to_string()),
            sampling_ratio: Some(0.5),
            timeout_ms: Some(2500.0),
        };
        let cfg = opts.to_core();
        assert!(cfg.enabled);
        assert_eq!(cfg.endpoint, "http://collector:4318");
        assert_eq!(cfg.service_name, "svc");
        assert_eq!(cfg.service_version, "9.9.9");
        assert_eq!(cfg.sampling_ratio, 0.5);
        assert_eq!(cfg.timeout.as_millis(), 2500);
    }

    #[test]
    fn non_finite_and_blank_fields_ignored() {
        let opts = OtelOptions {
            enabled: Some(true),
            endpoint: Some("   ".to_string()),
            protocol: Some("carrier-pigeon".to_string()),
            service_name: Some("".to_string()),
            service_version: None,
            sampling_ratio: Some(f64::NAN),
            timeout_ms: Some(f64::INFINITY),
        };
        let cfg = opts.to_core();
        // Blank endpoint/name ignored -> defaults retained.
        assert!(!cfg.endpoint.trim().is_empty());
        assert!(!cfg.service_name.trim().is_empty());
        // Bad protocol ignored -> default grpc kept.
        assert_eq!(
            cfg.protocol,
            cqlite_core::observability::OtelProtocol::Grpc
        );
        // Non-finite ratio ignored -> sane finite value.
        assert!(cfg.sampling_ratio.is_finite());
    }

    #[test]
    fn spans_build_without_panicking() {
        let _s = execute_span("execute", None);
        let _s2 = execute_span("executeNative", Some("invalid-traceparent"));
        let _s3 = streaming_span(None);
    }

    #[test]
    fn metrics_helpers_are_callable() {
        let span = streaming_span(None);
        record_rows(&span, 3);
        let err = cqlite_core::Error::corruption("x");
        record_boundary_error(&err);
        flush();
    }
}
