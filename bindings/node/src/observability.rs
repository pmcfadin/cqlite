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
    // Serialise the ENTIRE init path through `get_or_init`: the foundation
    // `obs::init` (which mutates global OTel providers) and the subscriber
    // install run AT MOST ONCE for the process, even under concurrent
    // `Database.open()` from multiple worker threads. Computing the guard inside
    // the closure means the winning thread's guard is the one stored, and no
    // guard that owns live exporters is ever constructed-then-dropped while the
    // globals still reference it: only the single closure run touches the
    // foundation. Losing threads block until the winner finishes, then observe
    // the already-initialised guard and do nothing.
    GUARD.get_or_init(|| {
        let cfg = match opts {
            Some(o) => o.to_core(),
            None => ObservabilityConfig::from_env(),
        };

        // `init` returns an inert guard for a disabled config and never installs
        // a global subscriber itself, so it is safe to call unconditionally. A
        // misconfigured exporter must never take down the host process: on error
        // we fall back to an inert guard and continue without export.
        let guard = obs::init(cfg).unwrap_or_else(|_| inert_guard());

        // Install the bridging subscriber from inside the same once-only closure
        // so the provider set by `init` above is visible to `tracing_layer()`,
        // and so the install also happens exactly once.
        install_subscriber();

        guard
    });
}

/// Build an inert guard for the error fall-back path. `init` on a disabled
/// config yields one and never errors, so we recurse into a guaranteed-inert
/// config rather than touching the foundation's private constructor.
fn inert_guard() -> ObservabilityGuard {
    let disabled = ObservabilityConfig::builder().enabled(false).build();
    obs::init(disabled).unwrap_or_else(|_| inert_guard())
}

/// Default per-layer filter for the OTLP span layer when `RUST_LOG` is unset.
///
/// Spans we want exported are emitted at INFO on the node crate
/// (`node.execute` / `node.execute_streaming`) and at DEBUG/INFO on
/// `cqlite_core` (read/write/compaction instrumentation). To guarantee those
/// reach the OTel layer with no `RUST_LOG`, this allows DEBUG on both targets
/// while leaving everything else `off`. It is applied ONLY to the OTel layer
/// (via `with_filter`), never as a global registry filter — so it cannot gate
/// any other layer, and since no fmt layer is attached, stdout/stderr stay quiet
/// regardless. `OTEL_NODE_TARGET` matches this crate's compiled name (`-`→`_`).
#[cfg(any(feature = "observability", test))]
const OTEL_NODE_TARGET: &str = "cqlite_node";
#[cfg(any(feature = "observability", test))]
const OTEL_CORE_TARGET: &str = "cqlite_core";

/// Build the directive string that allows the binding + core span targets at the
/// level their spans are emitted, with everything else off.
#[cfg(any(feature = "observability", test))]
fn default_otel_directives() -> String {
    format!("off,{OTEL_CORE_TARGET}=debug,{OTEL_NODE_TARGET}=debug")
}

/// Install the bridging `tracing` subscriber once. With the `observability`
/// feature on, the OTel layer (live only after a successful `init`) is composed
/// in so the per-call spans reach the OTLP exporter. `try_init` tolerates an
/// already-installed subscriber (e.g. set by a host or a test).
///
/// The filter is attached as a PER-LAYER filter on the OTel layer rather than as
/// a global registry filter, so it never gates other layers and — critically —
/// the spans reach the OTel layer even when `RUST_LOG` is unset (a global `off`
/// filter would otherwise drop every span before the layer could export it).
/// `RUST_LOG`, when set, still overrides the default directives. No fmt layer is
/// attached, so the Node process's stdout/stderr stay quiet by default.
fn install_subscriber() {
    use tracing_subscriber::util::SubscriberInitExt;

    #[cfg(feature = "observability")]
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::Layer;

        // RUST_LOG overrides; otherwise default to allowing the binding + core
        // span targets so the per-call/per-stream spans are exported with no
        // RUST_LOG set.
        let env_filter = env_filter_or_default();

        let registry = tracing_subscriber::registry()
            .with(cqlite_core::observability::tracing_layer().with_filter(env_filter));

        let _ = registry.try_init();
    }

    // Without the `observability` feature the OTLP layer is compiled out and
    // there is nothing to bridge; install a bare registry (no fmt layer) so the
    // process stays quiet and the macros at the binding boundary remain no-ops.
    #[cfg(not(feature = "observability"))]
    {
        let _ = tracing_subscriber::registry().try_init();
    }
}

/// `RUST_LOG` when set, otherwise the default directives that allow the binding
/// and core span targets so spans are exported even with no `RUST_LOG`.
#[cfg(feature = "observability")]
fn env_filter_or_default() -> tracing_subscriber::EnvFilter {
    use tracing_subscriber::EnvFilter;
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_otel_directives()))
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

    /// Finding 1: with no `RUST_LOG`, the default OTel directives MUST enable the
    /// binding + core span targets at the level the spans are emitted, so the
    /// per-call/per-stream spans reach the OTel layer (otherwise enabling `otel`
    /// would export nothing). We install the default-directive `EnvFilter` as the
    /// active subscriber and assert, via `tracing::enabled!`, that the binding's
    /// INFO span target and a core DEBUG span target are enabled — while an
    /// unrelated target is NOT — proving no blanket `off` filter swallows them.
    #[test]
    fn default_directives_enable_binding_and_core_spans_without_rust_log() {
        use tracing::Level;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::EnvFilter;

        // Build the subscriber exactly as `install_subscriber` would when
        // RUST_LOG is unset (the filter applied to the span layer). We attach the
        // filter at the registry level here ONLY for the probe — it makes
        // `tracing::enabled!` consult these directives without a live OTel layer.
        let subscriber =
            tracing_subscriber::registry().with(EnvFilter::new(default_otel_directives()));

        tracing::subscriber::with_default(subscriber, || {
            // node.execute / node.execute_streaming are emitted at INFO on the
            // node crate target.
            assert!(
                tracing::enabled!(target: "cqlite_node", Level::INFO),
                "node INFO span target must be enabled with default directives (no RUST_LOG)"
            );
            // Core read/write/compaction instrumentation includes DEBUG spans.
            assert!(
                tracing::enabled!(target: "cqlite_core", Level::DEBUG),
                "core DEBUG span target must be enabled with default directives"
            );
            // Everything else stays off so we do not export unrelated noise.
            assert!(
                !tracing::enabled!(target: "some_other_crate", Level::INFO),
                "unrelated targets must remain off under the default directives"
            );
        });
    }

    /// Finding 2: `init_once` must run the foundation init + subscriber install
    /// at most once, even when called repeatedly (the production hazard is
    /// concurrent `Database.open()`). A second call must be a no-op that leaves
    /// the same guard installed.
    #[test]
    fn init_once_is_idempotent() {
        // Default (disabled) config -> inert guard, no global providers touched.
        init_once(None);
        let first = GUARD.get().map(|g| g as *const _);
        init_once(None);
        let second = GUARD.get().map(|g| g as *const _);
        assert!(first.is_some(), "guard installed after first init_once");
        assert_eq!(first, second, "second init_once must not replace the guard");
    }
}
