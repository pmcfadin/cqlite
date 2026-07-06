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

use super::SSTableManager;
use crate::schema::TableSchema;
use crate::types::{ScanRow, TableId};
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
    ) -> Result<Option<Vec<(RowKey, ScanRow)>>> {
        // Issue #1591: snapshot the reader list and DROP the read guard before any
        // I/O (candidate prune + the block-walk delegated to the reader).
        let (reader_list, _fully_qualified_match) = self.resolve_reader_snapshot(table_id).await;

        // Prune to the candidate generations that admit the key. Reverse iteration
        // covers ONLY the single-generation case: with several generations the same
        // (partition, clustering) row may appear in more than one, and reconciling
        // them is the in-memory-sort path's job (cross-generation last-write-wins).
        // C4 (#1575): the BTI key hash+encoding is hoisted to once per read here.
        let candidates = Self::prune_candidates(&reader_list, partition_key);
        if candidates.len() != 1 {
            return Ok(None);
        }

        candidates[0]
            .big_reverse_partition_rows(partition_key, schema)
            .await
    }
}
