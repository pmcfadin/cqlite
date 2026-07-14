//! `cqlite-flight` — Arrow Flight server exposing on-the-fly compacted SSTable
//! data. Runs co-located with a Cassandra node and reads its local SSTables.

use std::net::SocketAddr;
use std::path::PathBuf;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use tonic::transport::Server;

use cqlite_flight::admission::{
    Admission, AdmissionConfig, DEFAULT_MAX_CONCURRENT_SCANS, DEFAULT_WAIT_TIMEOUT_MS,
    ENV_MAX_CONCURRENT_SCANS, ENV_WAIT_TIMEOUT_MS,
};
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::shutdown::shutdown_signal;
use std::time::Duration;

/// Command-line arguments.
#[derive(Parser, Debug)]
#[command(
    name = "cqlite-flight",
    about = "Arrow Flight server for compacted CQLite SSTables"
)]
struct Args {
    /// Root directory holding `<keyspace>/<table>[-<uuid>]/` SSTable dirs.
    #[arg(long)]
    data_dir: PathBuf,

    /// Address to listen on.
    #[arg(long, default_value = "0.0.0.0:8815")]
    listen: SocketAddr,

    /// Maximum rows per Arrow record batch.
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    /// Maximum concurrently admitted `do_get` scans (issue #2420, WS4). A `do_get`
    /// acquires a permit before opening any SSTable; past this ceiling requests
    /// wait up to `--admission-wait-timeout-ms`, then are shed with gRPC
    /// `UNAVAILABLE` (retry-safe for the connector's replica failover). Sized from
    /// the blocking-pool (~256) / fd (~1024÷SSTables) ceilings, not core count.
    #[arg(long, env = ENV_MAX_CONCURRENT_SCANS, default_value_t = DEFAULT_MAX_CONCURRENT_SCANS)]
    max_concurrent_scans: usize,

    /// How long a saturated `do_get` waits for an admission permit before it is
    /// rejected with `UNAVAILABLE` (issue #2420, WS4). Short bursts under this
    /// budget are absorbed transparently with no client-visible error.
    #[arg(long, env = ENV_WAIT_TIMEOUT_MS, default_value_t = DEFAULT_WAIT_TIMEOUT_MS)]
    admission_wait_timeout_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `ObservabilityConfig` is always compiled (it carries no OTel types), so
    // reading it here works identically whether or not the `observability`
    // feature is on. Snapshot the bits `log_observability_status` needs before
    // moving `obs_cfg` into `init` below.
    let obs_cfg = cqlite_core::observability::ObservabilityConfig::from_env();
    let obs_enabled = obs_cfg.enabled;
    let obs_endpoint = obs_cfg.endpoint.clone();

    // Initialise observability (issue #1041, epic #1031) BEFORE composing the
    // tracing subscriber, so `observability::tracing_layer()` returns the live
    // OTel export layer. The guard flushes/shuts down OTel on drop; hold it for
    // the whole process lifetime. With the `observability` feature off (or
    // CQLITE_OTEL_ENABLED unset) this is an inert no-op.
    let _otel_guard = cqlite_core::observability::init(obs_cfg)?;
    // Install the global W3C text-map propagator so incoming gRPC `traceparent`
    // metadata can be extracted into an OTel context and used to parent the
    // per-RPC spans (continuing a client's distributed trace into the service).
    #[cfg(feature = "observability")]
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    init_tracing_subscriber();

    // Issue #2128: the published image was previously built without
    // `--features observability`, so `CQLITE_OTEL_ENABLED=true` was silently
    // inert — no metrics, no traces, no error, nothing in the startup log to
    // say why. Make the inert-vs-active state visible instead.
    log_observability_status(obs_enabled, &obs_endpoint);

    let args = Args::parse();
    let listen = args.listen;
    // Admission control (issue #2420, WS4): the owned Semaphore is the real,
    // observable, cancel-releasable ceiling.
    let admission = Admission::new(AdmissionConfig {
        max_concurrent_scans: args.max_concurrent_scans,
        wait_timeout: Duration::from_millis(args.admission_wait_timeout_ms),
    });
    let admission_limit = admission.limit();
    let service = CqliteFlightService::with_admission(args.data_dir, args.batch_size, admission);

    // A coarse tonic transport backstop, generously ABOVE the admission ceiling:
    // it guards the HTTP/2 accept loop / stream table from a client opening far
    // more streams than `K`, but the Semaphore — not this cap — is the real
    // admission throttle (issue #2420). `saturating_mul` avoids overflow on an
    // extreme configured `K`.
    let max_concurrent_streams: u32 = u32::try_from(admission_limit)
        .unwrap_or(u32::MAX)
        .saturating_mul(4)
        .max(1024);

    tracing::info!(
        %listen,
        batch_size = args.batch_size,
        max_concurrent_scans = admission_limit,
        admission_wait_timeout_ms = args.admission_wait_timeout_ms,
        max_concurrent_streams,
        "cqlite-flight starting"
    );
    // Graceful shutdown (issue #1473): on ctrl_c / SIGTERM, tonic stops
    // accepting new connections and drains in-flight RPCs rather than tearing
    // every open stream down abruptly.
    Server::builder()
        .max_concurrent_streams(max_concurrent_streams)
        .add_service(FlightServiceServer::new(service))
        .serve_with_shutdown(listen, shutdown_signal())
        .await?;
    Ok(())
}

/// Install the unified `tracing_subscriber` registry (issue #1041, epic #1031).
///
/// Mirrors the CLI (#1033): an `EnvFilter` (honouring `RUST_LOG`, default
/// `info`) plus a human-readable `fmt` layer, and — when the `observability`
/// feature is enabled — the OTel bridge layer from
/// `cqlite_core::observability::tracing_layer()`. That layer is `None` (a no-op)
/// unless `init()` installed an exporting provider, so this composes safely in
/// every configuration. `init()` MUST have run first.
fn init_tracing_subscriber() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{fmt, EnvFilter};

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let registry = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer());

    #[cfg(feature = "observability")]
    let registry = registry.with(cqlite_core::observability::tracing_layer());

    // `try_init` is tolerant of a subscriber already being set (e.g. in tests).
    let _ = registry.try_init();
}

/// Log the startup observability state so `CQLITE_OTEL_ENABLED=true` is never
/// silently inert (issue #2128).
///
/// `enabled` / `endpoint` come from [`cqlite_core::observability::ObservabilityConfig::from_env`],
/// which is always compiled regardless of the `observability` feature. Two
/// outcomes, both logged only when `enabled`:
///
/// * Feature compiled IN — one `info` line naming the OTLP endpoint traces and
///   metrics are being exported to.
/// * Feature compiled OUT — one `warn` line: the binary honours none of the
///   `CQLITE_OTEL_*` vars, so the operator isn't left wondering why
///   VictoriaMetrics/Tempo stay empty.
///
/// A no-op when `enabled` is false (the default, and the common case for a
/// build that never sets `CQLITE_OTEL_ENABLED`).
fn log_observability_status(enabled: bool, endpoint: &str) {
    if !enabled {
        return;
    }
    #[cfg(feature = "observability")]
    {
        tracing::info!(
            endpoint,
            "observability enabled, exporting to OTLP endpoint"
        );
    }
    #[cfg(not(feature = "observability"))]
    {
        let _ = endpoint;
        tracing::warn!(
            "CQLITE_OTEL_ENABLED is set but this binary was compiled without the \
             `observability` feature — CQLITE_OTEL_* environment variables are inert; \
             no metrics or traces will be exported"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_observability_status_disabled_is_a_noop() {
        // Must never panic regardless of feature state, and there's nothing to
        // assert beyond "it returns" — disabled is the common, silent case.
        log_observability_status(false, "http://localhost:4317");
    }

    #[test]
    fn log_observability_status_enabled_does_not_panic() {
        // Exercises the active branch (info-line-with-feature or
        // warn-line-without-feature depending on how this crate was built).
        log_observability_status(true, "http://localhost:4317");
    }
}
