//! `cqlite-flight` — Arrow Flight server exposing on-the-fly compacted SSTable
//! data. Runs co-located with a Cassandra node and reads its local SSTables.

use std::net::SocketAddr;
use std::path::PathBuf;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use tonic::transport::Server;

use cqlite_flight::service::CqliteFlightService;

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
    // Initialise observability (issue #1041, epic #1031) BEFORE composing the
    // tracing subscriber, so `observability::tracing_layer()` returns the live
    // OTel export layer. The guard flushes/shuts down OTel on drop; hold it for
    // the whole process lifetime. With the `observability` feature off (or
    // CQLITE_OTEL_ENABLED unset) this is an inert no-op.
    let _otel_guard = cqlite_core::observability::init(
        cqlite_core::observability::ObservabilityConfig::from_env(),
    )?;
    // Install the global W3C text-map propagator so incoming gRPC `traceparent`
    // metadata can be extracted into an OTel context and used to parent the
    // per-RPC spans (continuing a client's distributed trace into the service).
    #[cfg(feature = "observability")]
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    init_tracing_subscriber();

    let args = Args::parse();
    let listen = args.listen;
    let service = CqliteFlightService::new(args.data_dir, args.batch_size);

    tracing::info!(%listen, batch_size = args.batch_size, "cqlite-flight starting");
    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(listen)
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
