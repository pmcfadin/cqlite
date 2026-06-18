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
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

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
