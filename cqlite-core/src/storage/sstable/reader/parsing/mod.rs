//! Parsing logic for SSTable block entries and partition data
//!
//! This module contains all parsing functions for converting raw SSTable data
//! into typed entries. It handles:
//! - Block entry parsing (modern and legacy formats)
//! - Composite key parsing with schema awareness
//! - Value parsing using schema types and comparators
//! - Collection types (list, set, map, tuple, UDT)

// Sub-modules
// Bench-only decode shim (issue #1615, Epic H). Compiled only under
// `bench-internals`; adds one `#[doc(hidden)] pub fn decode_value_for_bench` on
// `SSTableReader` so the external `decode` bench can measure the real block-path
// decode dispatch without widening the default public API. Lives in its own tiny
// module so value_parsing.rs stays within the source file-size ratchet.
#[cfg(feature = "bench-internals")]
mod bench_shim;
mod block_entries;
pub(crate) mod byte_comparable; // Needs to be accessible from row_cell_state_machine
pub(crate) mod comparator_value_parsing; // Standalone comparator-based parsing for state machine
mod custom_scalar;
mod frozen_value_parsing; // FROZEN collection i32-BE element framing (issue #2339)
mod key_parsing;
// `pub(crate)` (not `pub`) so the in-crate `fuzz_support` driver (issue #1614)
// can reach `V5CompressedLegacyParser::parse_block_emit` for the block-emit fuzz
// target. This widens crate-internal visibility only — the module and its
// `parse_block_emit` remain unreachable from outside cqlite-core.
pub(crate) mod row_decoder;
#[cfg(test)]
mod schema_fallback_stall_tests;
mod value_parsing;
#[cfg(test)]
mod value_parsing_schema_type_tests;

// Re-export all parsing methods (they're implemented on SSTableReader)
// No explicit re-exports needed since they're all impl blocks on SSTableReader

// Re-export V5CompressedLegacy parser for internal use
pub(in crate::storage::sstable::reader) use row_decoder::V5CompressedLegacyParser;
// Issue #3782: the buffer-extent contract every block-emit parse call states.
pub(in crate::storage::sstable::reader) use row_decoder::BufferExtent;
// ...and publicly, beside `PublicV5CompressedLegacyParser` below: the parser's
// public `parse_block*` methods TAKE a `BufferExtent`, so an out-of-crate
// caller (the issue #166 integration tests) cannot name the argument — hence
// cannot call them at all — unless the type travels the same path. Aliased
// because the crate-internal `use` above already binds the plain name here.
#[doc(hidden)]
pub use row_decoder::BufferExtent as PublicBufferExtent;
// ComplexColumnMeta is used internally within row_decoder.rs;
// delta_scan.rs accesses it via the parse_block_emit_delta closure type
// without needing an explicit re-export (Issue #700, DS4).

// Re-export the sliding-window parse outcome enum (issue #827) so the
// compaction-read streaming driver in data_access.rs can match on it.
pub(in crate::storage::sstable::reader) use row_decoder::ParseStep;

// Issue #2299: row-granular resumable compaction streaming — the state carried
// across a wide partition's chunk-straddling refills and the per-structure step
// outcome, so `stream_all_partitions_for_compaction` drains a wide partition
// row-by-row instead of buffering it whole.
pub(in crate::storage::sstable::reader) use row_decoder::{
    CompactionPartitionState, PartitionStreamStep,
};

// Issue #2299 (roborev blocker): the compaction-stream column resolution, built
// ONCE per scan in `data_access::compaction` and threaded into
// `stream_partition_body_incremental` so a wide partition's per-structure drain
// does NOT rebuild it per row (the buffered driver builds it per PARTITION —
// `partition_driver.rs:179`). Derived purely from the invariant serialization
// header + schema, so one build per scan is semantically identical.
pub(in crate::storage::sstable::reader) use row_decoder::RowColumnResolution;

// Re-export publicly for integration tests (Issue #166 regression tests)
// Using doc(hidden) to keep it out of public documentation but available for testing
#[doc(hidden)]
pub use row_decoder::V5CompressedLegacyParser as PublicV5CompressedLegacyParser;

use std::collections::HashMap;
use std::path::Path;

use tracing::debug;

use crate::{
    schema::{ClusteringColumn, Column, KeyColumn, TableSchema},
    Error, Result,
};

use super::types::SSTableReader;

/// Extract keyspace and table name from SSTable directory path (snapshot-aware).
///
/// SSTable paths follow Cassandra convention:
/// `/path/to/sstables/{keyspace}/{table_name}-{uuid}/nb-1-big-Data.db`, or for a
/// snapshot `.../{table_name}-{uuid}/snapshots/{tag}/nb-1-big-Data.db`.
///
/// This routes through the authoritative snapshot-aware parser
/// ([`crate::storage::sstable::snapshot_path`], issue #2384) so schema-registry
/// resolution and the V5CompressedLegacy block parser resolve the REAL
/// `{keyspace}.{table}` for a snapshot read — never `snapshots`/`{tag}` — matching
/// header resolution and `SSTableManager` keying exactly.
///
/// # Arguments
/// * `path` - Path to the SSTable Data.db file
///
/// # Returns
/// * `Ok((keyspace, table_name))` - Extracted names
/// * `Err(Error::Schema)` - If path doesn't contain enough directory components
///
/// # Examples
/// ```ignore
/// let path = Path::new("/data/test_basic/simple_table-abc/nb-1-big-Data.db");
/// let (keyspace, table) = extract_keyspace_table_from_path(path)?;
/// assert_eq!(keyspace, "test_basic");
/// assert_eq!(table, "simple_table");
/// ```
pub(in crate::storage::sstable::reader) fn extract_keyspace_table_from_path(
    path: &Path,
) -> Result<(String, String)> {
    use crate::storage::sstable::snapshot_path;

    let keyspace = snapshot_path::extract_keyspace(path)
        .ok_or_else(|| Error::schema("SSTable path has no keyspace directory (too shallow)"))?;
    let table_name = snapshot_path::extract_table_name(path)
        .ok_or_else(|| Error::schema("SSTable path has no table directory"))?;

    Ok((keyspace, table_name))
}

impl SSTableReader {
    /// Get table schema using four-tier lookup strategy
    ///
    /// This method implements a fallback chain for resolving table schemas:
    /// 0. **Provided Schema**: Use schema passed from query executor (highest priority)
    /// 1. **SSTable Header**: Check `self.schema` (extracted during SSTable opening from V5.0+ headers)
    /// 2. **Schema Registry**: Look up schema from external registry (loaded via --schema flag)
    /// 3. **Header Construction**: Build basic schema from header column metadata (fallback)
    pub(in crate::storage::sstable::reader) fn get_table_schema(
        &self,
        provided_schema: Option<&TableSchema>,
    ) -> Option<TableSchema> {
        // Strategy 0: Use provided schema from query executor (highest priority)
        if let Some(schema) = provided_schema {
            debug!(
                "get_table_schema: Using provided schema for {}.{}",
                schema.keyspace, schema.table
            );
            return Some(schema.clone());
        }

        // Strategy 1: Use schema extracted from SSTable header (if available)
        if let Some(schema) = self.schema.as_deref() {
            debug!(
                "get_table_schema: Using schema from SSTable header for {}.{}",
                self.header.keyspace, self.header.table_name
            );
            return Some(schema.clone());
        }

        // Strategy 2: Use the schema pre-resolved from the registry (issue #1692).
        //
        // This tier is reached only when the header schema (Strategy 1) is absent.
        // The registry is async (an `Arc<RwLock<SchemaRegistry>>` whose `get_schema`
        // itself awaits), so it CANNOT be consulted safely from this sync fn: the
        // old code `block_on`-ed the async lock on a tokio worker thread, which —
        // with a small runtime and a pending registry WRITE guard — could park the
        // workers and stall the whole runtime (AG3). Instead the registry schema is
        // resolved ONCE, properly awaited, at the async wiring point
        // (`SSTableReader::resolve_registry_schema`, called from
        // `open_reader_with_schema`) and cached in `self.registry_schema`. Here we
        // just read that plain field — no async lock, no `block_on`, no worker
        // parking. A registry miss leaves it `None` and we fall through to header
        // construction below, exactly as the old `block_on` path did on a miss.
        #[cfg(feature = "state_machine")]
        {
            if let Some(schema) = self.registry_schema.as_deref() {
                debug!(
                    "get_table_schema: Using pre-resolved registry schema for {}.{}",
                    schema.keyspace, schema.table
                );
                return Some(schema.clone());
            }
        }

        // For non-state_machine builds, schema_registry is Arc<SchemaRegistry> (not async)
        #[cfg(not(feature = "state_machine"))]
        {
            if let Some(registry) = self.schema_registry.as_ref() {
                // Extract keyspace/table from SSTable path (authoritative source)
                // Directory structure: {keyspace}/{table_name}-{uuid}/Data.db
                let file_path = self.file_path();
                let (keyspace, table_name) = match extract_keyspace_table_from_path(&file_path) {
                    Ok(names) => names,
                    Err(e) => {
                        debug!(
                            "get_table_schema: Failed to extract names from path {}: {}. Falling back to header names.",
                            file_path.display(), e
                        );
                        // Fallback to header names if path parsing fails
                        (self.header.keyspace.clone(), self.header.table_name.clone())
                    }
                };

                // Non-state_machine SchemaRegistry doesn't have async get_schema method
                // This path is currently not implemented for non-async registries
                debug!("get_table_schema: Schema registry lookup not available in non-state_machine builds");
                let _ = (registry, &keyspace, &table_name); // Avoid unused variable warnings
            }
        }

        // Strategy 3: Construct basic schema from header columns (existing logic)
        debug!(
            "get_table_schema: Falling back to header column construction for {}.{}",
            self.header.keyspace, self.header.table_name
        );

        // Try to construct a basic schema from header information
        if self.header.columns.is_empty() {
            return None;
        }

        let mut columns = Vec::new();
        let mut partition_keys = Vec::new();
        let mut clustering_keys = Vec::new();

        // Convert header columns to schema columns
        for col_info in self.header.columns.iter() {
            let column = Column {
                name: col_info.name.clone(),
                data_type: col_info.column_type.clone(), // Use column_type field
                nullable: true,
                default: None,
                // Static-column classification comes from the Statistics.db
                // SerializationHeader (authoritative metadata) carried on
                // ColumnInfo.is_static. Issue #758 / Epic #756.
                is_static: col_info.is_static,
            };

            // Check if this is a key column based on primary key and clustering status
            if col_info.is_primary_key && !col_info.is_clustering {
                // This is a partition key
                partition_keys.push(KeyColumn {
                    name: col_info.name.clone(),
                    data_type: col_info.column_type.clone(),
                    position: partition_keys.len(),
                });
            } else if col_info.is_clustering {
                clustering_keys.push(ClusteringColumn {
                    name: col_info.name.clone(),
                    data_type: col_info.column_type.clone(),
                    position: clustering_keys.len(),
                    order: crate::schema::ClusteringOrder::Asc,
                });
            }

            columns.push(column);
        }

        Some(TableSchema {
            keyspace: self.header.keyspace.clone(),
            table: self.header.table_name.clone(),
            partition_keys,
            clustering_keys,
            columns,
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_extract_keyspace_table_standard_format() {
        // Standard Cassandra format
        let path = Path::new("/data/sstables/test_basic/simple_table-6b0425d0a25111f0a3fef1a551383fb9/nb-1-big-Data.db");
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "test_basic");
        assert_eq!(table, "simple_table");
    }

    #[test]
    fn test_extract_keyspace_table_with_hyphens() {
        // Table name contains hyphens; only the trailing 32-hex table id is stripped.
        let path = Path::new(
            "/data/sstables/my_keyspace/user-profiles-00112233445566778899aabbccddeeff/nb-1-big-Data.db",
        );
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "my_keyspace");
        assert_eq!(table, "user-profiles");
    }

    /// Snapshot layout (issue #2384): schema-registry / block-parser identity
    /// resolution must yield the REAL keyspace/table, not `snapshots`/`{tag}`.
    #[test]
    fn test_extract_keyspace_table_snapshot_layout() {
        let path = Path::new(
            "/data/sstables/myks/mytable-9f3a1c2d4e5f6071829304a5b6c7d8e9/\
             snapshots/cqlite-3b1e7f90/nb-1-big-Data.db",
        );
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "myks");
        assert_eq!(table, "mytable");
    }

    /// BLOCKER 1 regression: a keyspace literally named `snapshots` must not be
    /// misparsed as a snapshot layout in the schema-resolution path either.
    #[test]
    fn test_extract_keyspace_table_snapshots_named_keyspace() {
        let path = Path::new(
            "/data/sstables/snapshots/mytable-9f3a1c2d4e5f6071829304a5b6c7d8e9/nb-1-big-Data.db",
        );
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "snapshots");
        assert_eq!(table, "mytable");
    }

    #[test]
    fn test_extract_keyspace_table_real_test_data() {
        // Real path from test-data
        let path = Path::new("/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6de93b70934a11f08d448925b7a9e804/nb-1-big-Data.db");
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "test_basic");
        assert_eq!(table, "simple_table");
    }

    #[test]
    fn test_extract_keyspace_table_collections() {
        // Collections table from test-data
        let path = Path::new("test-data/datasets/sstables/test_collections/collection_table-6b8c8fb0a25111f0a3fef1a551383fb9/nb-1-big-Data.db");
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "test_collections");
        assert_eq!(table, "collection_table");
    }

    #[test]
    fn test_extract_keyspace_table_invalid_no_parent() {
        // Invalid path - no parent directory
        let path = Path::new("/Data.db");
        let result = extract_keyspace_table_from_path(path);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_keyspace_table_no_id_suffix() {
        // A table directory with no trailing 32-hex id: the whole directory name is
        // the table name (no hyphen split), keyspace resolves normally.
        let path = Path::new("/data/keyspace/tablename/Data.db");
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "keyspace");
        assert_eq!(table, "tablename");
    }

    #[test]
    fn test_extract_keyspace_table_relative_path() {
        // Relative path (should work) with a real 32-hex table id.
        let path =
            Path::new("test_basic/simple_table-6b0425d0a25111f0a3fef1a551383fb9/nb-1-big-Data.db");
        let (keyspace, table) = extract_keyspace_table_from_path(path).unwrap();
        assert_eq!(keyspace, "test_basic");
        assert_eq!(table, "simple_table");
    }
}
