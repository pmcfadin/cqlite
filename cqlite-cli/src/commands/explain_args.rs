//! Arguments for the `explain` subcommand (issue #4193); split from `cli_types.rs` for the #1116
//! file-size ratchet.

use clap::Args;

use crate::cli::OutputFormat;

/// Arguments for `cqlite explain`.
#[derive(Args, Debug, Clone)]
pub struct ExplainArgs {
    /// Fully-qualified keyspace.table name.
    pub table: String,
    /// Partition-key literal(s), in schema order.
    pub partition_key: String,
    /// Render only cells for this clustering row. Repeat for composite keys.
    #[arg(long)]
    pub clustering: Vec<String>,
    /// Reconciliation clock as epoch seconds or RFC3339. Defaults to wall clock.
    #[arg(long)]
    pub now: Option<String>,
    /// Explain output format. Defaults to the global --out/--format setting.
    #[arg(long, value_enum)]
    pub out: Option<OutputFormat>,
}
