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
pub mod read_sstable;
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
pub use query::collect_query_result;
pub use query::{execute_query, execute_select_query};
pub use read::{read_sstable, read_sstable_enhanced};
pub(crate) use schema_load::load_schema_file;
pub use support::{ParsedRow, QueryExecutor, QueryExecutorConfig, QueryResult, RealDataParser};
