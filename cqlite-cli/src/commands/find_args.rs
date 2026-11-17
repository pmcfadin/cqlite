//! Arguments for the `find` subcommand (issue #4205); split from `cli_types.rs` for the
//! #1116 file-size ratchet.

use clap::Args;
use std::path::PathBuf;

use crate::cli_types::VerifyOutputArg;

/// `--help` long text for `cqlite find` (kept here so `cli_types.rs` stays under the file-size threshold).
pub const FIND_LONG_ABOUT: &str = "Report, for ONE partition key of a table directory, how EVERY generation answered: `hit` (offset, length or to-eof, max writetime, partition-deletion), `bloom-negative` (BIG Filter.db said no), `index-miss` (Index.db / the BTI trie said no -- also reported when Filter.db is absent), or `scanned` (no usable index, a full schema-driven scan decided). A generation that cannot be opened or probed is reported UNREADABLE with its cause -- never as absent. With --commitlog <dir>, every mutation in every *.log segment touching the key is listed with segment, byte position, writetime and `unflushed` (yes | no | unmeasured(<cause>)); writetime is `unmeasured(<cause>)` for shapes the CommitLog decoder does not model (clustered tables, complex columns, partition deletions) -- never a guess -- and an unreadable segment is its own line. <partition-key> is a CQL literal (comma-separated for a composite key). Needs the global --schema. Exit 0 = report complete (including 'absent everywhere'); 1 = usage error; 2 = any generation or segment could not be read. Example: cqlite --schema ks.cql find ./ks/users-<id> 42 --commitlog ./commitlog --out json";

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
