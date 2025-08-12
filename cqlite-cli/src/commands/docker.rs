/// Docker integration commands for cqlite-cli
use clap::{Args, Subcommand};
use cqlite_core::docker::{DockerContainer, DockerCqlshClient};
use cqlite_core::testing::{CassandraTestRunner, TestSuiteResult};

#[derive(Args)]
pub struct DockerArgs {
    #[command(subcommand)]
    pub command: DockerCommand,
}

#[derive(Subcommand)]
pub enum DockerCommand {
    /// List available Cassandra containers
    List,
    /// Connect to Cassandra container and execute CQL
    Connect {
        /// CQL query to execute
        #[arg(short, long)]
        query: Option<String>,
        /// Container name or ID (optional, auto-detects if not provided)
        #[arg(short, long)]
        container: Option<String>,
    },
    /// Run test suite against Cassandra container
    Test {
        /// Test queries file path
        #[arg(short, long)]
        queries: Option<String>,
        /// Run basic test suite
        #[arg(long)]
        basic: bool,
    },
    /// Check if Cassandra container is ready
    Status,
}
