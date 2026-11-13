//! `cqlite diagnose` arguments (issue #4204). Always compiled, so the `Commands` enum is
//! identical in every build; the verb itself (`diagnose.rs`) is gated like `salvage`/`rebuild`.

use clap::Args;
use std::path::PathBuf;

use crate::cli_types::VerifyOutputArg;

/// Arguments for `cqlite diagnose`.
#[derive(Args, Debug, Clone)]
pub struct DiagnoseArgs {
    /// Table directory holding the SSTable generations (the dir containing the `*-Data.db`
    /// files). Every generation is diagnosed; no schema is needed for the cheap tier.
    pub table_dir: PathBuf,
    /// The instant `gcBefore`, `fully_expired_at_now` and `reclaim_at_now` are evaluated at:
    /// epoch seconds or an RFC 3339 timestamp. Default: the wall clock, sampled once.
    #[arg(long, value_name = "TS")]
    pub now: Option<String>,
    /// Also run ONE bounded scan of each generation: partition-size and clustering-width
    /// histograms, top-N rankings and the raw tombstone counts. Reads Data.db; the default
    /// (cheap) tier reads only Statistics.db/Summary.db/Partitions.db and never opens Data.db.
    #[arg(long)]
    pub deep: bool,
    /// Length cap of each `--deep` ranking list.
    #[arg(long, value_name = "N", default_value_t = 5)]
    pub top: usize,
    /// Rendering of the report on STDOUT. This selects a FORMAT, never a destination: the verb
    /// writes no file anywhere and modifies nothing.
    #[arg(long, value_enum, default_value = "text")]
    pub out: VerifyOutputArg,
}
