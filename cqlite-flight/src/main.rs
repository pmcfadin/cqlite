//! `cqlite-flight` — Arrow Flight server exposing on-the-fly compacted SSTable
//! data. Runs co-located with a Cassandra node and reads its local SSTables.

use std::net::SocketAddr;
use std::path::PathBuf;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use tonic::transport::Server;

use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::shutdown::shutdown_signal;

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
    let service = CqliteFlightService::new(args.data_dir, args.batch_size);

    tracing::info!(%listen, batch_size = args.batch_size, "cqlite-flight starting");
    // Graceful shutdown (issue #1473): on ctrl_c / SIGTERM, tonic stops
    // accepting new connections and drains in-flight RPCs rather than tearing
    // every open stream down abruptly.
    Server::builder()
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
        tracing::info!(endpoint, "observability enabled, exporting to OTLP endpoint");
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
