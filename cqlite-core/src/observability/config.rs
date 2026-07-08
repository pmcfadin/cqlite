//! Observability configuration, populated from the standard `CQLITE_OTEL_*`
//! environment variables (epic #1031, issue #1032).
//!
//! The config type is always compiled (it carries no OpenTelemetry types), so
//! callers and tests can construct and inspect it whether or not the
//! `observability` feature is enabled. The actual exporter wiring lives in
//! [`crate::observability`]'s feature-gated `init`.
//!
//! # Environment variables
//!
//! | Variable                       | Type    | Default                      | Meaning |
//! |--------------------------------|---------|------------------------------|---------|
//! | `CQLITE_OTEL_ENABLED`          | bool    | `false`                      | Master switch; when false, `init` is a no-op even with the feature on. |
//! | `CQLITE_OTEL_ENDPOINT`         | string  | `http://localhost:4317`      | OTLP collector endpoint (gRPC) or base URL (HTTP). |
//! | `CQLITE_OTEL_PROTOCOL`         | enum    | `grpc`                       | `grpc` or `http` (HTTP/protobuf). |
//! | `CQLITE_OTEL_SERVICE_NAME`     | string  | `cqlite`                     | `service.name` resource attribute. |
//! | `CQLITE_OTEL_SERVICE_VERSION`  | string  | crate version                | `service.version` resource attribute. |
//! | `CQLITE_OTEL_SAMPLING_RATIO`   | f64     | `1.0`                        | Trace-ID-ratio sampling probability, clamped to `[0.0, 1.0]`. |
//! | `CQLITE_OTEL_TIMEOUT_MS`       | u64     | `10000`                      | Exporter export timeout in milliseconds. |
//!
//! Booleans accept `1/0`, `true/false`, `yes/no`, `on/off` (case-insensitive).

use std::time::Duration;

/// OTLP wire protocol selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OtelProtocol {
    /// OTLP over gRPC (default; port 4317).
    #[default]
    Grpc,
    /// OTLP over HTTP/protobuf (port 4318).
    Http,
}

impl OtelProtocol {
    /// Parse a protocol string. Recognises `grpc` and `http`
    /// (and the alias `http/protobuf`), case-insensitively.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "grpc" => Some(Self::Grpc),
            "http" | "http/protobuf" | "http-protobuf" | "httpbinary" => Some(Self::Http),
            _ => None,
        }
    }

    /// Stable lowercase name (`"grpc"` / `"http"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::Http => "http",
        }
    }
}

/// Default OTLP gRPC endpoint.
pub const DEFAULT_ENDPOINT: &str = "http://localhost:4317";
/// Default service name resource attribute.
pub const DEFAULT_SERVICE_NAME: &str = "cqlite";
/// Default exporter timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_millis(10_000);

/// Runtime observability configuration.
///
/// Construct via [`ObservabilityConfig::from_env`] for the standard
/// `CQLITE_OTEL_*` behaviour, or [`ObservabilityConfig::builder`] to set fields
/// programmatically (e.g. in tests that want a disabled / no-export config).
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    /// Master enable switch. When `false`, `init` returns an inert guard and
    /// installs no exporters or global providers, regardless of the feature.
    pub enabled: bool,
    /// OTLP endpoint (gRPC endpoint or HTTP base URL).
    pub endpoint: String,
    /// Wire protocol.
    pub protocol: OtelProtocol,
    /// `service.name` resource attribute.
    pub service_name: String,
    /// `service.version` resource attribute.
    pub service_version: String,
    /// Trace-ID-ratio sampling probability in `[0.0, 1.0]`. Wrapped in a
    /// parent-based sampler so children of a sampled span are always recorded.
    pub sampling_ratio: f64,
    /// Exporter export timeout.
    pub timeout: Duration,
    /// Opt-in, default-OFF presence-oracle false-negative verification (issue
    /// #2163). When `true`, an SSTable read whose bloom/BTI-trie reports a key
    /// "definitely absent" runs an AUTHORITATIVE confirmation scan and increments
    /// `cqlite.read.bloom.false_negatives` on a contradiction. Populated from
    /// `CQLITE_VERIFY_PRESENCE_ORACLE`; the read path itself consults the
    /// process-global switch in
    /// [`crate::storage::sstable::reader::presence_verification`], which reads the
    /// SAME environment variable, so this field mirrors that runtime state.
    pub verify_presence_oracle: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: DEFAULT_ENDPOINT.to_string(),
            protocol: OtelProtocol::Grpc,
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            sampling_ratio: 1.0,
            timeout: DEFAULT_TIMEOUT,
            verify_presence_oracle: false,
        }
    }
}

impl ObservabilityConfig {
    /// Start from defaults and override fields fluently.
    pub fn builder() -> ObservabilityConfigBuilder {
        ObservabilityConfigBuilder {
            config: Self::default(),
        }
    }

    /// Populate from the `CQLITE_OTEL_*` environment variables, falling back to
    /// the documented defaults for anything unset or unparseable.
    ///
    /// Unparseable values fall back to the default rather than erroring, so a
    /// typo in an env var never crashes the host process.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Some(v) = env_str("CQLITE_OTEL_ENABLED") {
            if let Some(b) = parse_bool(&v) {
                cfg.enabled = b;
            }
        }
        if let Some(v) = env_str("CQLITE_OTEL_ENDPOINT") {
            cfg.endpoint = v;
        }
        if let Some(v) = env_str("CQLITE_OTEL_PROTOCOL") {
            if let Some(p) = OtelProtocol::parse(&v) {
                cfg.protocol = p;
            }
        }
        if let Some(v) = env_str("CQLITE_OTEL_SERVICE_NAME") {
            cfg.service_name = v;
        }
        if let Some(v) = env_str("CQLITE_OTEL_SERVICE_VERSION") {
            cfg.service_version = v;
        }
        if let Some(v) = env_str("CQLITE_OTEL_SAMPLING_RATIO") {
            if let Ok(r) = v.trim().parse::<f64>() {
                // Reject non-finite values (NaN, ±inf) — they survive `clamp`
                // and would violate the documented [0.0, 1.0] contract. Keep the
                // default in that case.
                if r.is_finite() {
                    cfg.sampling_ratio = r.clamp(0.0, 1.0);
                }
            }
        }
        if let Some(v) = env_str("CQLITE_OTEL_TIMEOUT_MS") {
            if let Ok(ms) = v.trim().parse::<u64>() {
                cfg.timeout = Duration::from_millis(ms);
            }
        }
        // Issue #2163: presence-oracle false-negative verification switch. Mirrors
        // the process-global runtime switch the read path consults (same env var).
        if let Some(v) = env_str("CQLITE_VERIFY_PRESENCE_ORACLE") {
            if let Some(b) = parse_bool(&v) {
                cfg.verify_presence_oracle = b;
            }
        }

        cfg.sampling_ratio = sanitize_ratio(cfg.sampling_ratio);
        cfg
    }
}

/// Clamp a sampling ratio into `[0.0, 1.0]`, falling back to full sampling for
/// non-finite inputs (NaN / ±inf) which would otherwise bypass the clamp.
fn sanitize_ratio(r: f64) -> f64 {
    if r.is_finite() {
        r.clamp(0.0, 1.0)
    } else {
        1.0
    }
}

/// Builder for [`ObservabilityConfig`].
#[derive(Debug, Clone)]
pub struct ObservabilityConfigBuilder {
    config: ObservabilityConfig,
}

impl ObservabilityConfigBuilder {
    /// Set the master enable switch.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }
    /// Set the OTLP endpoint.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.config.endpoint = endpoint.into();
        self
    }
    /// Set the wire protocol.
    pub fn protocol(mut self, protocol: OtelProtocol) -> Self {
        self.config.protocol = protocol;
        self
    }
    /// Set the `service.name`.
    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        self.config.service_name = name.into();
        self
    }
    /// Set the `service.version`.
    pub fn service_version(mut self, version: impl Into<String>) -> Self {
        self.config.service_version = version.into();
        self
    }
    /// Set the trace-ID-ratio sampling probability (clamped to `[0.0, 1.0]`;
    /// non-finite values fall back to full sampling).
    pub fn sampling_ratio(mut self, ratio: f64) -> Self {
        self.config.sampling_ratio = sanitize_ratio(ratio);
        self
    }
    /// Set the exporter timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }
    /// Enable/disable presence-oracle false-negative verification (issue #2163).
    pub fn verify_presence_oracle(mut self, verify: bool) -> Self {
        self.config.verify_presence_oracle = verify;
        self
    }
    /// Finish building.
    pub fn build(self) -> ObservabilityConfig {
        let mut cfg = self.config;
        cfg.sampling_ratio = sanitize_ratio(cfg.sampling_ratio);
        cfg
    }
}

fn env_str(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => None,
    }
}

fn parse_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_disabled_and_sane() {
        let cfg = ObservabilityConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.endpoint, DEFAULT_ENDPOINT);
        assert_eq!(cfg.protocol, OtelProtocol::Grpc);
        assert_eq!(cfg.service_name, DEFAULT_SERVICE_NAME);
        assert_eq!(cfg.sampling_ratio, 1.0);
        assert_eq!(cfg.timeout, DEFAULT_TIMEOUT);
        assert_eq!(cfg.service_version, env!("CARGO_PKG_VERSION"));
        assert!(!cfg.verify_presence_oracle);
    }

    #[test]
    fn builder_overrides_and_clamps() {
        let cfg = ObservabilityConfig::builder()
            .enabled(true)
            .endpoint("http://collector:4317")
            .protocol(OtelProtocol::Http)
            .service_name("svc")
            .service_version("9.9.9")
            .sampling_ratio(2.0) // clamps to 1.0
            .timeout(Duration::from_millis(500))
            .build();
        assert!(cfg.enabled);
        assert_eq!(cfg.endpoint, "http://collector:4317");
        assert_eq!(cfg.protocol, OtelProtocol::Http);
        assert_eq!(cfg.service_name, "svc");
        assert_eq!(cfg.service_version, "9.9.9");
        assert_eq!(cfg.sampling_ratio, 1.0);
        assert_eq!(cfg.timeout, Duration::from_millis(500));
    }

    #[test]
    fn negative_ratio_clamps_to_zero() {
        let cfg = ObservabilityConfig::builder().sampling_ratio(-1.0).build();
        assert_eq!(cfg.sampling_ratio, 0.0);
    }

    #[test]
    fn non_finite_ratio_falls_back_to_full_sampling() {
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let cfg = ObservabilityConfig::builder().sampling_ratio(bad).build();
            assert!(cfg.sampling_ratio.is_finite());
            assert_eq!(cfg.sampling_ratio, 1.0);
        }
    }

    #[test]
    fn protocol_parse() {
        assert_eq!(OtelProtocol::parse("grpc"), Some(OtelProtocol::Grpc));
        assert_eq!(OtelProtocol::parse("GRPC"), Some(OtelProtocol::Grpc));
        assert_eq!(OtelProtocol::parse("http"), Some(OtelProtocol::Http));
        assert_eq!(
            OtelProtocol::parse("http/protobuf"),
            Some(OtelProtocol::Http)
        );
        assert_eq!(OtelProtocol::parse("nope"), None);
        assert_eq!(OtelProtocol::Grpc.as_str(), "grpc");
        assert_eq!(OtelProtocol::Http.as_str(), "http");
    }

    #[test]
    fn bool_parsing() {
        for t in ["1", "true", "TRUE", "yes", "on"] {
            assert_eq!(parse_bool(t), Some(true), "{t}");
        }
        for f in ["0", "false", "no", "off"] {
            assert_eq!(parse_bool(f), Some(false), "{f}");
        }
        assert_eq!(parse_bool("maybe"), None);
    }

    // from_env mutates process-global env, so all env-touching assertions live
    // in ONE test to avoid cross-test races (tests share a process).
    #[test]
    fn from_env_parses_and_falls_back() {
        let keys = [
            "CQLITE_OTEL_ENABLED",
            "CQLITE_OTEL_ENDPOINT",
            "CQLITE_OTEL_PROTOCOL",
            "CQLITE_OTEL_SERVICE_NAME",
            "CQLITE_OTEL_SERVICE_VERSION",
            "CQLITE_OTEL_SAMPLING_RATIO",
            "CQLITE_OTEL_TIMEOUT_MS",
            "CQLITE_VERIFY_PRESENCE_ORACLE",
        ];
        // Save + clear.
        let saved: Vec<_> = keys.iter().map(|k| (*k, std::env::var(k).ok())).collect();
        for k in keys {
            std::env::remove_var(k);
        }

        // All unset -> defaults.
        let cfg = ObservabilityConfig::from_env();
        assert!(!cfg.enabled);
        assert_eq!(cfg.endpoint, DEFAULT_ENDPOINT);
        assert!(!cfg.verify_presence_oracle);

        // Set every var.
        std::env::set_var("CQLITE_OTEL_ENABLED", "true");
        std::env::set_var("CQLITE_VERIFY_PRESENCE_ORACLE", "on");
        std::env::set_var("CQLITE_OTEL_ENDPOINT", "http://c:4318");
        std::env::set_var("CQLITE_OTEL_PROTOCOL", "http");
        std::env::set_var("CQLITE_OTEL_SERVICE_NAME", "mysvc");
        std::env::set_var("CQLITE_OTEL_SERVICE_VERSION", "1.2.3");
        std::env::set_var("CQLITE_OTEL_SAMPLING_RATIO", "0.25");
        std::env::set_var("CQLITE_OTEL_TIMEOUT_MS", "2500");
        let cfg = ObservabilityConfig::from_env();
        assert!(cfg.enabled);
        assert_eq!(cfg.endpoint, "http://c:4318");
        assert_eq!(cfg.protocol, OtelProtocol::Http);
        assert_eq!(cfg.service_name, "mysvc");
        assert_eq!(cfg.service_version, "1.2.3");
        assert_eq!(cfg.sampling_ratio, 0.25);
        assert_eq!(cfg.timeout, Duration::from_millis(2500));
        assert!(cfg.verify_presence_oracle);

        // Unparseable ratio / timeout fall back to defaults; bad protocol keeps default.
        std::env::set_var("CQLITE_OTEL_SAMPLING_RATIO", "not-a-number");
        std::env::set_var("CQLITE_OTEL_TIMEOUT_MS", "xyz");
        std::env::set_var("CQLITE_OTEL_PROTOCOL", "carrier-pigeon");
        let cfg = ObservabilityConfig::from_env();
        assert_eq!(cfg.sampling_ratio, 1.0);
        assert_eq!(cfg.timeout, DEFAULT_TIMEOUT);
        assert_eq!(cfg.protocol, OtelProtocol::Grpc);

        // Restore.
        for (k, v) in saved {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
    }
}
