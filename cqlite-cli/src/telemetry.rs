//! CLI telemetry startup: the OpenTelemetry `init` call and the unified
//! `tracing_subscriber` registry, plus the ORDER in which the two must happen.
//!
//! Split out of `main.rs` so the ordering rule (and its issue-#1702 caveat)
//! lives in one small, readable place instead of inline in `run_main`.

use anyhow::Result;
use cqlite_core::observability::{ObservabilityConfig, ObservabilityGuard};

/// Initialise observability and install the logging subscriber, in the order
/// this build requires, returning the RAII guard the caller must keep alive.
///
/// # Why the order differs by feature (issue #1702)
///
/// With `observability` ON, `cqlite_core::observability::init` MUST run first:
/// it installs the OTel provider that `observability::tracing_layer()` returns,
/// and that layer is composed into the registry below — a subscriber built
/// before `init` would compose a dead layer and export nothing.
///
/// With `observability` OFF there is no layer to compose (the composition is
/// `#[cfg]`ed out), so that constraint does not exist — and the reverse order is
/// REQUIRED: the feature-off `init` emits the #1702 "OTel requested but compiled
/// out" warning, and a `tracing` event emitted before a subscriber exists goes
/// to the global no-op subscriber and is LOST. Installing the subscriber first
/// is what makes that warning reach stderr instead of being a second silent
/// no-op layered on the first.
#[cfg(feature = "observability")]
pub fn init_telemetry(core_cfg: ObservabilityConfig, log_level: &str) -> Result<ObservabilityGuard> {
    let guard = cqlite_core::observability::init(core_cfg)
        .map_err(|e| anyhow::anyhow!("Failed to initialize observability: {}", e))?;
    init_tracing_subscriber(log_level);
    Ok(guard)
}

/// Feature-off counterpart: subscriber FIRST, so the #1702 warning is visible.
/// See the feature-on twin above for the full ordering rationale.
#[cfg(not(feature = "observability"))]
pub fn init_telemetry(core_cfg: ObservabilityConfig, log_level: &str) -> Result<ObservabilityGuard> {
    init_tracing_subscriber(log_level);
    cqlite_core::observability::init(core_cfg)
        .map_err(|e| anyhow::anyhow!("Failed to initialize observability: {}", e))
}

/// Install the unified `tracing_subscriber` registry (Issue #1033, Epic #1031).
///
/// Replaces the previous bare `env_logger` init while preserving its behavior:
/// - The fmt layer writes to STDERR ONLY, so stdout stays clean for
///   `--out json`/`csv` (Issue #129 stdout hygiene).
/// - The `EnvFilter` honours `RUST_LOG` when set, otherwise falls back to the
///   `-v`/`-q`-derived `default_level` (same mapping as before).
/// - `tracing-subscriber`'s `tracing-log` feature is enabled, so `.init()`
///   installs a `log -> tracing` bridge (`LogTracer`); existing `log::info!` /
///   `log::debug!` calls continue to appear.
///
/// When the `observability` feature is enabled, the OTel layer returned by
/// `cqlite_core::observability::tracing_layer()` is composed in (it is `None`,
/// hence a no-op layer, unless `init` installed a live provider). When the
/// feature is off, the layer composition is compiled out entirely.
fn init_tracing_subscriber(default_level: &str) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{fmt, EnvFilter};

    // RUST_LOG overrides the verbosity-derived level, matching the old
    // env_logger::from_env behavior.
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));

    // Write all formatted log/span output to STDERR only.
    let fmt_layer = fmt::layer().with_writer(std::io::stderr);

    let registry = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer);

    #[cfg(feature = "observability")]
    let registry = registry.with(cqlite_core::observability::tracing_layer());

    // `try_init` is tolerant of a subscriber already being set (e.g. in tests).
    let _ = registry.try_init();
}
