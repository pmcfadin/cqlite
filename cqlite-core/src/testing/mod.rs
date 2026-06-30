/// Testing utilities module
pub mod dataset_helpers;

pub use dataset_helpers::{
    // Root-agnostic helpers
    list_tables,
    // Explicit-root helpers for tests and tools
    list_tables_at,
    load_metadata,
    load_metadata_at,
    // Strict fixture gate (issue #1230): single shared definition.
    require_fixtures_strict,
    resolve_table_to_sstable_path,
    resolve_table_to_sstable_path_at,
    TableInfo,
};
