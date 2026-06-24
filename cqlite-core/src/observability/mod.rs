//! Observability foundation (epic #1031, issues #1032 + #1038).
//!
//! This module is the single shared foundation every other observability issue
//! builds on. It owns OpenTelemetry initialisation, the `CQLITE_OTEL_*` config,
//! the metric-naming [`catalog`], the error-rate schema ([`ErrorCategory`] +
//! [`record_error`]), and graceful shutdown — and it is designed so that
//! **instrumentation call sites are identical whether or not the
//! `observability` feature is enabled.** When the feature is off, every helper
//! here compiles to a no-op and `cargo tree -p cqlite-core` links no
//! OpenTelemetry crates.
//!
//! # The contract for downstream issues (#1033–#1043)
//!
//! Downstream code MUST go through this module rather than touching
//! `opentelemetry`/`tracing-opentelemetry` directly, so telemetry stays
//! consistent and zero-cost-when-off:
//!
//! **Initialisation (CLI #1033, Flight #1041, bindings #1039/#1040):**
//! ```ignore
//! let cfg = observability::ObservabilityConfig::from_env();
//! let _guard = observability::init(cfg)?;            // RAII; flushes on drop
//! // compose the tracing layer into your own subscriber (feature-gated):
//! # #[cfg(feature = "observability")]
//! tracing_subscriber::registry()
//!     .with(tracing_subscriber::fmt::layer())
//!     .with(observability::tracing_layer())
//!     .init();
//! ```
//!
//! **Spans (read/query/write/compaction #1034–#1037):** use the ordinary
//! `tracing` macros (`#[tracing::instrument]`, `tracing::info_span!`) — they are
//! always available and the OTel layer bridges them when active. Do NOT create
//! per-cell spans on hot paths; aggregate with metrics instead.
//!
//! **Metrics:** call [`add_counter`], [`record_histogram`], [`record_gauge`]
//! with a name from [`catalog`] and bounded [`Attr`] attributes whose keys come
//! from [`catalog::attr`]:
//! ```ignore
//! use cqlite_core::observability::{self as obs, catalog, AttrValue};
//! obs::add_counter(catalog::READ_ROWS, n, &[(catalog::attr::SSTABLE_FORMAT, "bti".into())]);
//! obs::record_histogram(catalog::QUERY_DURATION, elapsed.as_secs_f64(), &[]);
//! ```
//!
//! **Errors (#1038):** at every boundary instrumented by #1034–#1037, call
//! [`record_error`] when an error escapes — it increments
//! [`catalog::ERRORS_TOTAL`] keyed by the bounded `{category, subsystem}` label
//! set and marks the active span errored. Never attach the raw error message.
//!
//! # Always-compiled vs feature-gated
//!
//! [`catalog`], [`config`], the [`ErrorCategory`] taxonomy, and the helper
//! signatures here are ALWAYS compiled (they pull in no OTel types), so call
//! sites and tests build in any configuration. Only the exporter/runtime wiring
//! ([`otel`]) is gated behind `observability`.

pub mod catalog;
pub mod config;
mod error_schema;

pub use config::{ObservabilityConfig, ObservabilityConfigBuilder, OtelProtocol};
pub use error_schema::ErrorCategory;

use crate::error::{Error, Result};

#[cfg(feature = "observability")]
mod otel;

#[cfg(feature = "observability")]
pub use otel::{init, tracing_layer, ObservabilityGuard};

/// Shared in-memory OTLP capture harness for observability tests (issue #1043).
///
/// Gated behind the `observability-testing` feature, which pulls in the OTel
/// SDK's in-memory exporters (`InMemorySpanExporter` / `InMemoryMetricExporter`)
/// via `opentelemetry_sdk/testing`. Public so integration tests and future child
/// issues can reuse the same fixture API to assert span trees and metric
/// names/units/attributes. Production `observability` builds never compile this,
/// so they never link the SDK's testing surface.
#[cfg(feature = "observability-testing")]
pub mod testing;

/// A bounded attribute value for catalog metrics.
///
/// Mirrors the small set of types OpenTelemetry attributes accept, but is always
/// available regardless of the `observability` feature so instrumentation call
/// sites compile identically when telemetry is off. Keep values bounded
/// (prefer [`AttrValue::StaticStr`] / numbers); never feed unbounded data such
/// as raw error messages, partition keys, or full query text.
#[derive(Debug, Clone)]
pub enum AttrValue {
    /// An owned string. Use only for genuinely bounded values.
    Str(String),
    /// A `&'static str` — the preferred, allocation-free form.
    StaticStr(&'static str),
    /// A signed integer.
    Int(i64),
    /// A floating-point value.
    Float(f64),
    /// A boolean.
    Bool(bool),
}

impl From<&'static str> for AttrValue {
    fn from(v: &'static str) -> Self {
        AttrValue::StaticStr(v)
    }
}
impl From<String> for AttrValue {
    fn from(v: String) -> Self {
        AttrValue::Str(v)
    }
}
impl From<i64> for AttrValue {
    fn from(v: i64) -> Self {
        AttrValue::Int(v)
    }
}
impl From<u64> for AttrValue {
    fn from(v: u64) -> Self {
        AttrValue::Int(v as i64)
    }
}
impl From<f64> for AttrValue {
    fn from(v: f64) -> Self {
        AttrValue::Float(v)
    }
}
impl From<bool> for AttrValue {
    fn from(v: bool) -> Self {
        AttrValue::Bool(v)
    }
}

/// A single bounded metric attribute: a `&'static str` key (always from
/// [`catalog::attr`]) paired with a bounded [`AttrValue`].
pub type Attr = (&'static str, AttrValue);

#[cfg(feature = "observability")]
fn to_key_values(attrs: &[Attr]) -> Vec<opentelemetry::KeyValue> {
    use opentelemetry::{KeyValue, Value};
    attrs
        .iter()
        .map(|(k, v)| {
            let value = match v {
                AttrValue::Str(s) => Value::from(s.clone()),
                AttrValue::StaticStr(s) => Value::from(*s),
                AttrValue::Int(i) => Value::from(*i),
                AttrValue::Float(f) => Value::from(*f),
                AttrValue::Bool(b) => Value::from(*b),
            };
            KeyValue::new(*k, value)
        })
        .collect()
}

/// Add `value` to the monotonic counter identified by a [`catalog`] name, with
/// bounded [`Attr`] attributes. No-op (and zero-cost) when the `observability`
/// feature is off.
#[inline]
pub fn add_counter(name: &'static str, value: u64, attrs: &[Attr]) {
    #[cfg(feature = "observability")]
    otel::add_counter(name, value, &to_key_values(attrs));
    #[cfg(not(feature = "observability"))]
    {
        let _ = (name, value, attrs);
    }
}

/// Record `value` into the histogram identified by a [`catalog`] name (durations
/// in seconds, sizes in bytes — see the catalog docs). No-op when off.
#[inline]
pub fn record_histogram(name: &'static str, value: f64, attrs: &[Attr]) {
    #[cfg(feature = "observability")]
    otel::record_histogram(name, value, &to_key_values(attrs));
    #[cfg(not(feature = "observability"))]
    {
        let _ = (name, value, attrs);
    }
}

/// Record `value` for the gauge identified by a [`catalog`] name (a current
/// value such as in-flight count or open SSTables). No-op when off.
#[inline]
pub fn record_gauge(name: &'static str, value: i64, attrs: &[Attr]) {
    #[cfg(feature = "observability")]
    otel::record_gauge(name, value, &to_key_values(attrs));
    #[cfg(not(feature = "observability"))]
    {
        let _ = (name, value, attrs);
    }
}

/// Record an error that escaped a critical section (issue #1038).
///
/// Increments [`catalog::ERRORS_TOTAL`] keyed by the bounded
/// `{cqlite.error.category, cqlite.subsystem}` label set and marks the active
/// span as errored. `subsystem` is a `&'static str` (e.g. `"reader"`,
/// `"query"`, `"write"`, `"compaction"`) so its value space stays bounded. The
/// raw error message is never recorded. No-op when the `observability` feature
/// is off.
#[inline]
pub fn record_error(err: &Error, subsystem: &'static str) {
    #[cfg(feature = "observability")]
    {
        let attrs = [
            opentelemetry::KeyValue::new(
                catalog::attr::ERROR_CATEGORY,
                err.obs_category().as_str(),
            ),
            opentelemetry::KeyValue::new(catalog::attr::SUBSYSTEM, subsystem),
        ];
        otel::add_counter(catalog::ERRORS_TOTAL, 1, &attrs);
        otel::mark_span_error(err.obs_category());
    }
    #[cfg(not(feature = "observability"))]
    {
        let _ = (err, subsystem);
    }
}

/// Convenience: run a `Result`-returning closure and [`record_error`] on the
/// `Err` path, returning the result unchanged. Lets call sites instrument an
/// operation without restructuring their error handling.
#[inline]
pub fn record_result<T>(subsystem: &'static str, result: Result<T>) -> Result<T> {
    if let Err(e) = &result {
        record_error(e, subsystem);
    }
    result
}

/// Inert observability guard returned by [`init`] when the `observability`
/// feature is disabled. Flushes and installs nothing.
#[cfg(not(feature = "observability"))]
#[derive(Debug)]
pub struct ObservabilityGuard {
    _private: (),
}

#[cfg(not(feature = "observability"))]
impl ObservabilityGuard {
    /// Always `false` for the inert guard.
    pub fn is_active(&self) -> bool {
        false
    }
    /// No-op flush.
    pub fn force_flush(&self) {}
}

/// Initialise observability from `cfg`. When the `observability` feature is
/// disabled this is a no-op that returns an inert [`ObservabilityGuard`], so
/// callers (CLI, Flight, bindings) can call it unconditionally.
#[cfg(not(feature = "observability"))]
#[inline]
pub fn init(cfg: ObservabilityConfig) -> Result<ObservabilityGuard> {
    let _ = cfg;
    Ok(ObservabilityGuard { _private: () })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers_are_callable_in_any_build() {
        // These must compile and not panic regardless of feature state.
        add_counter(catalog::READ_ROWS, 3, &[(catalog::attr::SSTABLE_FORMAT, "bti".into())]);
        record_histogram(catalog::QUERY_DURATION, 0.001, &[]);
        record_gauge(catalog::SSTABLES_OPEN, 2, &[]);
        let err = Error::corruption("x");
        record_error(&err, "reader");
        let ok: Result<u8> = record_result("reader", Ok(1));
        assert_eq!(ok.ok(), Some(1));
    }

    #[test]
    fn init_returns_guard() {
        // With the feature off this exercises the inert guard; with it on it
        // exercises the disabled-config inert path in otel::init.
        let cfg = ObservabilityConfig::builder().enabled(false).build();
        let guard = init(cfg).expect("init never fails for a disabled config");
        assert!(!guard.is_active());
        guard.force_flush();
    }

    #[test]
    fn attr_value_conversions() {
        assert!(matches!(AttrValue::from("s"), AttrValue::StaticStr("s")));
        assert!(matches!(AttrValue::from(1i64), AttrValue::Int(1)));
        assert!(matches!(AttrValue::from(2u64), AttrValue::Int(2)));
        assert!(matches!(AttrValue::from(true), AttrValue::Bool(true)));
    }
}
