//! CLI command handlers.
//!
//! This module is the dispatcher/aggregator for the per-subcommand handler
//! modules. Each subcommand lives in its own sibling file; `mod.rs` declares
//! the modules and re-exports the public surface so callers keep using
//! `crate::commands::<fn>` unchanged (issue #1126 module split).

#![allow(dead_code)]
// Allow deprecated BulletproofReader usage (Issue #190 - experimental reader)
// This will be removed once BulletproofReader is fully replaced with SSTableReader
#![allow(deprecated)]
// The legacy public-surface re-exports below preserve `crate::commands::<fn>`
// call sites; several handlers have no in-tree caller (they were already
// dead-but-public before the split), so silence the re-export's unused warning.
#![allow(unused_imports)]

// Existing per-subcommand modules.
pub mod admin;
pub mod bench;
pub mod schema;
pub mod write;

pub mod delta_export;
pub mod docker;
pub mod info;
pub mod read_commitlog;
pub mod read_sstable;
// `cqlite salvage` (issue #4196): unlike `write.rs`, which gates individual
// write-support items so the module stays unconditional, this file's content
// is ENTIRELY write-support-dependent (it imports
// `cqlite_core::storage::write_engine::salvage` at the top level), so the
// whole module declaration is gated instead. `not(tombstones)` mirrors the
// core module's OWN gate (roborev, issue #4196, round-7 High finding): the
// core `write_engine::salvage` module is
// `#[cfg(all(feature = "write-support", not(feature = "tombstones")))]`, so
// this CLI module — its only cross-crate consumer — must vanish in lockstep
// under `--all-features`, or `cqlite-cli/tombstones` (which forwards
// `cqlite-core/tombstones`, see `Cargo.toml`) would compile against a core
// item that no longer exists.
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
pub mod salvage;
pub mod verify;

// Handlers extracted from the former monolithic `mod.rs` (issue #1126).
pub mod benchmark_sstable;
pub mod export;
pub mod export_sstable;
pub mod import;
pub mod inspect;
pub mod query;
pub mod read;
pub mod schema_load;
pub mod support;

// Re-export the legacy public surface so callers keep using
// `crate::commands::<item>` without change.
pub use benchmark_sstable::benchmark_sstable;
pub use export::export_data;
#[cfg(feature = "state_machine")]
pub use export_sstable::export_sstable;
pub use import::import_data;
pub use inspect::{analyze_sstable, validate_sstable};
#[cfg(feature = "state_machine")]
pub use query::{collect_query_result, collect_rows_until};
pub use query::{execute_query, execute_select_query};
pub use read::{read_sstable, read_sstable_enhanced};
pub(crate) use schema_load::load_schema_file;
pub use support::{ParsedRow, QueryExecutor, QueryExecutorConfig, QueryResult, RealDataParser};

/// Dispatch the `cqlite salvage` verb (issue #4196), or the informative
/// "not built" error when this binary was built without it. Moved out of
/// `main.rs` into this always-compiled module (roborev, issue #4196,
/// round-8 Low finding: the cfg-gated dispatch block was growing an
/// already-over-threshold `main.rs`, campsite rule/#1116) — the two-armed
/// `not(tombstones)`-mirroring gate (round-7 High finding, see
/// `salvage`'s module declaration above for why) lives here instead, so
/// `main.rs` keeps a one-line call regardless of which arm compiles.
pub async fn dispatch_salvage(
    schema: Option<&std::path::Path>,
    args: &crate::cli_types::SalvageArgs,
) -> anyhow::Result<()> {
    #[cfg(all(feature = "write-support", not(feature = "tombstones")))]
    {
        // `execute_salvage_command` owns its WHOLE exit-code space (0/1/2/3,
        // design D3) via direct `std::process::exit` calls on every path,
        // fallible or not (roborev, issue #4196: routing a usage error
        // through `?` here would have sent it through `classify_error`'s
        // `CliExitCode` enum instead, which has no variant equal to 1). It
        // therefore never returns an `Err`.
        salvage::execute_salvage_command(schema, args).await;
        Ok(())
    }
    #[cfg(any(not(feature = "write-support"), feature = "tombstones"))]
    {
        // roborev, issue #4196, round 19 Low finding: this arm previously
        // returned `Err(anyhow!(...))`, which `run_main`'s caller routes
        // through `error::classify_error` — the EXACT indirection this
        // function's own doc above says `execute_salvage_command` avoids
        // ("it therefore never returns an Err"), broken by this ONE other
        // arm. The message's substring "write" matches `classify_error`'s
        // `CliExitCode::WriteError` branch (`error.rs:156-166`) — exit code
        // 6, outside `dispatch_salvage`'s documented 0/1/2/3 space, even
        // though this really is a usage error (the verb was invoked on a
        // build that never compiled it in). Own the exit code directly
        // instead, matching every OTHER salvage failure path.
        let _ = (schema, args);
        eprintln!(
            "Write support is not enabled (or this build has cqlite-core/tombstones on, which \
             the salvage module cannot be built against, roborev issue #4196 round-7). Build \
             with --features write-support (and without --features tombstones) to enable salvage."
        );
        std::process::exit(1);
    }
}
