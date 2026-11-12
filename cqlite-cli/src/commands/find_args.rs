//! Arguments for the `find` subcommand (issue #4205); split from `cli_types.rs` for the
//! #1116 file-size ratchet.

use clap::Args;
use std::path::PathBuf;

use crate::cli_types::VerifyOutputArg;

/// Arguments for `cqlite find`.
#[derive(Args, Debug, Clone)]
pub struct FindArgs {
    /// Table directory (`<table>-<32 hex table id>`) holding the generations to probe.
    pub table_dir: PathBuf,
    /// Partition key as a CQL literal (comma-separated literals for a composite key), e.g.
    /// `42` or `'alice'`.
    pub partition_key: String,
    /// Directory of CommitLog `*.log` segments to scan for mutations touching the key.
    #[arg(long)]
    pub commitlog: Option<PathBuf>,
    /// Output format.
    #[arg(long, value_enum, default_value = "text")]
    pub out: VerifyOutputArg,
}
