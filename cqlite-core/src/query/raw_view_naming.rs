//! The `_raw_sstable_data` table-name suffix (issue #4222) — a tiny,
//! crate-shared naming convention (design.md D1) between the raw view's own
//! interception (`select_executor::raw_view`) and every OTHER place in the
//! engine that resolves "does this table have a schema" from a table-name
//! STRING before a query ever reaches the executor.
//!
//! The concrete defect this module exists to fix: `Database::has_schema_for_table`
//! / `schema_status` (used by the CLI's issue #199 pre-flight schema check,
//! `cqlite-cli/src/main.rs`) resolve the LITERAL table name a query names —
//! for `SELECT * FROM ks.t_raw_sstable_data`, that is the SUFFIXED name, which
//! is never itself a registered schema (only `ks.t` is). Without this shared
//! constant the CLI rejected every raw-view query before `SelectExecutor`
//! ever got a chance to intercept it (design.md D6).

/// Suffix that marks a table reference as the raw-SSTable-view name for its
/// base table (design.md D1). The single source of truth for the literal
/// string — [`select_executor::raw_view`](crate::query::select_executor)'s
/// own copy is a `pub(in ...)` alias of this constant, not a second literal.
pub const RAW_SSTABLE_VIEW_SUFFIX: &str = "_raw_sstable_data";

/// Strip [`RAW_SSTABLE_VIEW_SUFFIX`] from a BARE table name (no keyspace
/// segment — callers needing the qualified form pair this with their own
/// keyspace handling, as `select_executor::raw_view` does). `None` when the
/// suffix is absent, or stripping it would leave an empty base name.
pub fn strip_raw_view_suffix(table_name: &str) -> Option<&str> {
    table_name
        .strip_suffix(RAW_SSTABLE_VIEW_SUFFIX)
        .filter(|base| !base.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_bare_suffix_only() {
        assert_eq!(strip_raw_view_suffix("t_raw_sstable_data"), Some("t"));
        assert_eq!(strip_raw_view_suffix("t"), None);
        assert_eq!(strip_raw_view_suffix("_raw_sstable_data"), None);
    }
}
