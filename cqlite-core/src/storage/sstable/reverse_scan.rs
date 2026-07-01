//! BIG (`nb`) reverse partition iteration at the `SSTableManager` seam (Issue #1184).
//!
//! `ORDER BY <clustering> DESC` on a single BIG wide partition is served by walking
//! the promoted `IndexInfo` blocks back-to-front (see
//! [`SSTableReader::big_reverse_partition_rows`]) instead of a post-fetch in-memory
//! sort over a forward full-partition read. This module is the manager-level entry
//! the query executor calls; it prunes to the single candidate generation and
//! delegates the block-walk to the reader. It returns `Ok(None)` (the executor then
//! keeps the in-memory sort) for every case the reverse iterator does not cover —
//! zero or multiple candidate generations (cross-generation reconcile still needs a
//! sort), a non-BIG / narrow partition, a static or variable-width clustering, or an
//! open range-tombstone marker.
//!
//! Kept in its own file (declared from `sstable/mod.rs`, which is over the
//! campsite-rule threshold) so the manager gains the surface without growing
//! `mod.rs`. Tombstones-gated: the seek/reverse paths exist only on the default build.

#![cfg(not(feature = "tombstones"))]

use std::sync::Arc;

use super::{reader, SSTableManager};
use crate::schema::TableSchema;
use crate::types::{TableId, Value};
use crate::{Result, RowKey};

impl SSTableManager {
    /// Reverse single-partition clustering scan for a BIG (`nb`) wide partition.
    ///
    /// Returns `Ok(Some(rows))` with the partition's rows in DESCENDING clustering
    /// order when the BIG reverse iterator applied, or `Ok(None)` to signal the
    /// caller should fall back to the in-memory `ORDER BY DESC` sort.
    pub(crate) async fn scan_partition_clustering_reverse(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        schema: Option<&TableSchema>,
    ) -> Result<Option<Vec<(RowKey, Value)>>> {
        let table_readers = self.table_readers.read().await;
        let Some(reader_list) = Self::resolve_reader_list(&table_readers, table_id.name()) else {
            return Ok(None);
        };

        // Prune to the candidate generations that admit the key. Reverse iteration
        // covers ONLY the single-generation case: with several generations the same
        // (partition, clustering) row may appear in more than one, and reconciling
        // them is the in-memory-sort path's job (cross-generation last-write-wins).
        let candidates: Vec<Arc<reader::SSTableReader>> = reader_list
            .iter()
            .filter(|r| r.might_contain_partition(partition_key))
            .cloned()
            .collect();
        if candidates.len() != 1 {
            return Ok(None);
        }

        candidates[0]
            .big_reverse_partition_rows(partition_key, schema)
            .await
    }
}
