//! Arguments for the `diff` subcommand (issue #4201); split from `cli_types.rs` for the #1116
//! file-size ratchet.

use clap::{Args, ValueEnum};
use std::path::PathBuf;

/// Output format for `cqlite diff`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DiffOutput {
    /// One finding per line, summary last (the default).
    Text,
    /// One JSON object after the first line.
    Json,
}

/// READ-ONLY: compare two snapshots of one table, replica divergence and resurrection risk (issue #4201)
///
/// Never writes to, creates a file in, or modifies either input directory.
#[derive(Args, Debug, Clone)]
pub struct DiffArgs {
    /// Snapshot A: a flat table directory of SSTable generations (the table's own directory, or a
    /// data root holding exactly one `<keyspace>/<table>-<id>`).
    pub dir_a: PathBuf,
    /// Snapshot B: same shape as A.
    pub dir_b: PathBuf,
    /// Fully-qualified keyspace.table name.
    #[arg(long)]
    pub table: String,
    /// Restrict the comparison to this partition (CQL literal syntax; comma-separated literals for
    /// composite keys). Repeatable. Without it, every partition of either side is compared.
    #[arg(long)]
    pub partition: Vec<String>,
    /// Reconciliation clock as epoch seconds or RFC3339. Defaults to wall clock.
    #[arg(long)]
    pub now: Option<String>,
    /// Output format.
    #[arg(long, value_enum, default_value = "text")]
    pub out: DiffOutput,
}

/// `diff --help` long description.
pub const DIFF_LONG_ABOUT: &str = "READ-ONLY: compare the same table's reconciled state in two snapshot directories (each a flat table directory of generations) and report, per partition, which cells (equal | only-a | only-b | newer-in-a | newer-in-b) and tombstones (only-a | only-b) the two sides disagree about, and whether any divergence is a RESURRECTION RISK: a tombstone one side holds whose shadowed data the other side still holds live. Each risk is qualified before-gc-grace or past-gc-grace at --now (past-gc-grace = the tombstone's local deletion time is strictly older than now minus gc_grace_seconds, so its holder may purge it and the other side's data would return on the next repair or restore). A tombstone both sides hold is omitted: the report shows disagreement only. This verb never writes to, creates files in, or modifies either input directory, and has no repair or apply mode; the report is input to repair, not a write path. Exit 0 = report rendered (divergence is a finding, not a failure); 1 = usage error (a directory that does not resolve to the table, a bad --partition literal); 2 = a generation on either side cannot be read (the message names the side and the path) or the report exceeds the result budget. Example: cqlite --schema ks.tbl.cql diff ./node1/ks/tbl-id ./node2/ks/tbl-id --table ks.tbl --now 1795222425 --out json";
