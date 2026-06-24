//! Observability wiring for the Python bindings (epic #1031, issue #1039).
//!
//! This module bridges CQLite's [`cqlite_core::observability`] foundation into
//! the Python extension module. It is responsible for:
//!
//! 1. **Process-global init** — translating an optional `otel_config` dict (or
//!    the `CQLITE_OTEL_*` environment) into an
//!    [`cqlite_core::observability::ObservabilityConfig`], calling
//!    `observability::init` exactly once per interpreter, and holding the
//!    returned [`ObservabilityGuard`] in a process-global so buffered telemetry
//!    flushes at interpreter shutdown (guard drop). It also installs a
//!    `tracing` subscriber composed with the OTel layer when the
//!    `observability` feature is enabled.
//! 2. **Per-call spans** — [`call_span`] creates the `python.execute` /
//!    `python.execute_streaming` spans that `database.rs` enters around each
//!    operation. When a W3C `traceparent` is supplied (per-call or captured at
//!    open), [`set_traceparent_parent`] re-parents the span so the Rust spans
//!    correlate with the caller's Python OpenTelemetry trace.
//! 3. **Flush** — [`flush`] force-flushes the guard on `Database.close()`.
//!
//! The `subsystem` label for every metric/error emitted from this layer is
//! `"python"`.
//!
//! ## Feature gating
//!
//! Config plumbing, `ensure_initialized`, the per-call span, and `flush` are
//! ALWAYS compiled — the core helpers they call are no-ops when
//! `cqlite-core/observability` is off, so the bindings build and behave
//! identically in either configuration. Only the OTel-specific pieces
//! (subscriber composition, traceparent extraction) are gated behind the
//! bindings' `observability` feature.

use std::sync::OnceLock;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use cqlite_core::observability::{self, ObservabilityConfig, ObservabilityGuard, OtelProtocol};

/// Subsystem label attached to every metric/error emitted from the Python
/// bindings boundary. Bounded by construction.
pub const SUBSYSTEM: &str = "python";

/// Process-global observability guard. Initialised at most once (the first
/// `Database.open`, or the first call that triggers [`ensure_initialized`]) and
/// kept alive for the whole interpreter lifetime so the RAII guard flushes and
/// shuts the exporters down when the process exits.
static GUARD: OnceLock<ObservabilityGuard> = OnceLock::new();

/// Build an [`ObservabilityConfig`] from an optional Python `otel_config` dict.
///
/// Resolution order: start from `ObservabilityConfig::from_env()` (so the
/// `CQLITE_OTEL_*` environment is honoured as the baseline), then override any
/// keys present in the dict. This means a caller can set, e.g., the endpoint via
/// the environment and just flip `{"enabled": True}` in the dict.
///
/// Recognised dict keys (all optional):
/// - `enabled` (bool)
/// - `endpoint` (str)
/// - `protocol` (str — `"grpc"` or `"http"`)
/// - `service_name` (str)
/// - `service_version` (str)
/// - `sampling_ratio` (float, clamped to `[0.0, 1.0]`)
/// - `timeout_ms` (int)
///
/// Unknown keys are rejected with `ValueError` so typos surface immediately
/// rather than being silently ignored.
pub fn config_from_py(
    _py: Python<'_>,
    otel_config: Option<&Bound<'_, PyAny>>,
) -> PyResult<ObservabilityConfig> {
    use pyo3::exceptions::PyValueError;

    let mut cfg = ObservabilityConfig::from_env();

    let Some(obj) = otel_config else {
        return Ok(cfg);
    };

    let dict = obj.downcast::<PyDict>().map_err(|_| {
        PyValueError::new_err("otel_config must be a dict mapping option names to values")
    })?;

    const KNOWN_KEYS: &[&str] = &[
        "enabled",
        "endpoint",
        "protocol",
        "service_name",
        "service_version",
        "sampling_ratio",
        "timeout_ms",
    ];

    // Reject unknown keys up front so a typo never silently disables telemetry.
    for key in dict.keys() {
        let key_str: String = key
            .extract()
            .map_err(|_| PyValueError::new_err("otel_config keys must be strings"))?;
        if !KNOWN_KEYS.contains(&key_str.as_str()) {
            return Err(PyValueError::new_err(format!(
                "unknown otel_config key '{key_str}'; recognised keys: {}",
                KNOWN_KEYS.join(", ")
            )));
        }
    }

    if let Some(v) = dict.get_item("enabled")? {
        cfg.enabled = v
            .extract::<bool>()
            .map_err(|_| PyValueError::new_err("otel_config['enabled'] must be a bool"))?;
    }
    if let Some(v) = dict.get_item("endpoint")? {
        cfg.endpoint = v
            .extract::<String>()
            .map_err(|_| PyValueError::new_err("otel_config['endpoint'] must be a str"))?;
    }
    if let Some(v) = dict.get_item("protocol")? {
        let s: String = v
            .extract()
            .map_err(|_| PyValueError::new_err("otel_config['protocol'] must be a str"))?;
        cfg.protocol = OtelProtocol::parse(&s).ok_or_else(|| {
            PyValueError::new_err(format!(
                "otel_config['protocol'] '{s}' is invalid; expected 'grpc' or 'http'"
            ))
        })?;
    }
    if let Some(v) = dict.get_item("service_name")? {
        cfg.service_name = v
            .extract::<String>()
            .map_err(|_| PyValueError::new_err("otel_config['service_name'] must be a str"))?;
    }
    if let Some(v) = dict.get_item("service_version")? {
        cfg.service_version = v
            .extract::<String>()
            .map_err(|_| PyValueError::new_err("otel_config['service_version'] must be a str"))?;
    }
    if let Some(v) = dict.get_item("sampling_ratio")? {
        let ratio: f64 = v
            .extract()
            .map_err(|_| PyValueError::new_err("otel_config['sampling_ratio'] must be a float"))?;
        // The builder/from_env sanitise non-finite + out-of-range values; reuse
        // the builder so the same clamping rules apply.
        cfg = ObservabilityConfig::builder()
            .enabled(cfg.enabled)
            .endpoint(cfg.endpoint.clone())
            .protocol(cfg.protocol)
            .service_name(cfg.service_name.clone())
            .service_version(cfg.service_version.clone())
            .sampling_ratio(ratio)
            .timeout(cfg.timeout)
            .build();
    }
    if let Some(v) = dict.get_item("timeout_ms")? {
        let ms: u64 = v.extract().map_err(|_| {
            PyValueError::new_err("otel_config['timeout_ms'] must be a non-negative int")
        })?;
        cfg.timeout = Duration::from_millis(ms);
    }

    Ok(cfg)
}

/// Initialise observability once per process from `cfg`.
///
/// The first caller installs the OTel exporters (when the feature is on and the
/// config is enabled) and the process-global tracing subscriber, then stores
/// the [`ObservabilityGuard`] in [`GUARD`] so it lives for the interpreter and
/// flushes on shutdown. Subsequent calls are no-ops — the first config wins, so
/// telemetry is configured by the first `Database.open` and shared by all later
/// databases in the process. This matches the OpenTelemetry single-provider
/// model and avoids reinitialising global exporters per database.
///
/// Never returns an error to the caller: a misconfigured exporter must not stop
/// a `Database.open` from succeeding. Initialisation failures degrade to "no
/// telemetry" silently (the guard simply isn't stored).
pub fn ensure_initialized(cfg: ObservabilityConfig) {
    if GUARD.get().is_some() {
        return;
    }
    let guard = match observability::init(cfg) {
        Ok(g) => g,
        Err(_) => return,
    };
    // Compose the tracing subscriber + OTel layer once the provider exists, so
    // spans emitted from this layer reach the exporter. Safe to call even when
    // the guard is inert (the layer is then `None` / a no-op).
    install_subscriber();
    // If another thread won the race, our guard drops here (flushing nothing of
    // consequence since the winner owns the live exporters).
    let _ = GUARD.set(guard);
}

/// Force-flush any buffered telemetry through the process-global guard.
///
/// Called from `Database.close()` so a short-lived script that closes its
/// database sees its spans exported even before interpreter shutdown. Safe to
/// call when uninitialised (no-op) and idempotent.
pub fn flush() {
    if let Some(guard) = GUARD.get() {
        guard.force_flush();
    }
}

/// Install the process-global `tracing` subscriber composed with the OTel
/// bridge layer.
///
/// `try_init` is tolerant of a subscriber already being set (e.g. when the host
/// application configured `tracing` itself), so this never panics and never
/// clobbers an existing subscriber.
#[cfg(feature = "observability")]
fn install_subscriber() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let registry = tracing_subscriber::registry()
        .with(env_filter)
        .with(observability::tracing_layer());

    let _ = registry.try_init();
}

/// When the feature is off there is no OTel layer to compose, so the subscriber
/// is left to the host application. Per-call spans still compile (and are
/// no-ops without a subscriber).
#[cfg(not(feature = "observability"))]
fn install_subscriber() {}

/// Create the per-call span for a Python binding operation.
///
/// `name` is a `&'static str` span name such as `"python.execute"` or
/// `"python.execute_streaming"`. The returned span is NOT entered — the caller
/// enters it around the work so the GIL-released async section runs inside it.
///
/// The span carries the bounded `cqlite.subsystem = "python"` attribute and a
/// `cqlite.rows` field the caller fills in with `span.record("cqlite.rows", n)`
/// once the operation completes (rows returned for a SELECT, rows affected for
/// DML, or rows yielded for a stream). It NEVER carries query text, partition
/// keys, or other unbounded data.
pub fn call_span(name: &'static str) -> tracing::Span {
    tracing::info_span!(
        "python.call",
        otel.name = name,
        cqlite.subsystem = SUBSYSTEM,
        cqlite.rows = tracing::field::Empty,
    )
}

/// Re-parent `span` to the trace described by a W3C `traceparent` header value,
/// so the per-call Rust span links to the caller's existing OpenTelemetry trace.
///
/// `traceparent` is the value of the W3C `traceparent` header, e.g.
/// `00-<32 hex trace-id>-<16 hex span-id>-01`. A malformed/absent value is
/// ignored (the span keeps its natural parent) — propagation is best-effort and
/// must never fail an operation.
#[cfg(feature = "observability")]
pub fn set_traceparent_parent(span: &tracing::Span, traceparent: Option<&str>) {
    use opentelemetry::propagation::TextMapPropagator;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use std::collections::HashMap;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let Some(tp) = traceparent else {
        return;
    };
    let tp = tp.trim();
    if tp.is_empty() {
        return;
    }

    let mut carrier: HashMap<String, String> = HashMap::with_capacity(1);
    carrier.insert("traceparent".to_string(), tp.to_string());

    let propagator = TraceContextPropagator::new();
    let parent_cx = propagator.extract(&carrier);
    span.set_parent(parent_cx);
}

/// No-op when the feature is off: with no OTel layer there is no remote context
/// to attach, so the traceparent is simply ignored.
#[cfg(not(feature = "observability"))]
pub fn set_traceparent_parent(_span: &tracing::Span, _traceparent: Option<&str>) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_span_is_constructible() {
        // Span creation must work in any build, with or without a subscriber.
        let span = call_span("python.execute");
        let _enter = span.enter();
    }

    #[test]
    fn set_traceparent_tolerates_garbage() {
        let span = call_span("python.execute");
        // None, empty, and malformed must all be silently ignored.
        set_traceparent_parent(&span, None);
        set_traceparent_parent(&span, Some(""));
        set_traceparent_parent(&span, Some("not-a-traceparent"));
    }

    #[test]
    fn flush_is_safe_when_uninitialized() {
        // Must not panic even if no guard was ever set in this process.
        flush();
    }
}
