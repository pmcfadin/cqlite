//! Registry-schema pre-resolution for the sync schema-fallback tier (issue #1692).
//!
//! The sync `get_table_schema` must never `block_on` the async schema registry on
//! a tokio worker thread. Instead the registry schema is resolved ONCE — properly
//! awaited — at an async wiring point, and cached in `self.registry_schema` for
//! the sync path to read.

use std::sync::Arc;

use super::parsing::extract_keyspace_table_from_path;
use super::types::SSTableReader;

impl SSTableReader {
    /// Pre-resolve the registry schema into the sync cache (issue #1692, AG3).
    ///
    /// Call this from an async wiring point (e.g. `open_reader_with_schema`, or a
    /// DIRECT caller's own async context), AFTER
    /// [`set_schema_registry`](SSTableReader::set_schema_registry) — or use the
    /// combined [`attach_schema_registry`](SSTableReader::attach_schema_registry).
    /// It resolves the table schema from the async registry a single time —
    /// properly awaited — and caches it in `self.registry_schema`, so the SYNC
    /// schema-fallback tier of `get_table_schema` reads a plain field instead of
    /// `block_on`-ing the async registry lock on a tokio worker thread.
    ///
    /// Cold path only: when the SSTable header already carried a schema
    /// (`self.schema.is_some()`) the sync path short-circuits before the registry
    /// tier, so no resolution is performed. A registry miss leaves the cache
    /// `None`; the sync path then falls through to header-column construction,
    /// exactly as the previous `block_on` path did on a miss.
    pub async fn resolve_registry_schema(&mut self) {
        // Header schema present -> the registry tier is never reached; skip.
        if self.schema.is_some() {
            return;
        }
        let Some(registry_rwlock) = self.schema_registry.clone() else {
            return;
        };

        // Keyspace/table from the SSTable path (authoritative), with the header
        // names as the documented fallback — same resolution the old block_on
        // path used.
        let (keyspace, table_name) = match extract_keyspace_table_from_path(&self.file_path) {
            Ok(names) => names,
            Err(e) => {
                log::debug!(
                    "resolve_registry_schema: path parse failed for {}: {}. Using header names.",
                    self.file_path.display(),
                    e
                );
                (self.header.keyspace.clone(), self.header.table_name.clone())
            }
        };

        let registry = registry_rwlock.read().await;
        match registry.get_schema(&keyspace, &table_name).await {
            Ok(schema) => {
                log::debug!(
                    "resolve_registry_schema: cached registry schema for {}.{}",
                    keyspace,
                    table_name
                );
                self.registry_schema = Some(Arc::new(schema));
            }
            Err(e) => {
                log::debug!(
                    "resolve_registry_schema: no registry schema for {}.{}: {}",
                    keyspace,
                    table_name,
                    e
                );
            }
        }
    }
}
