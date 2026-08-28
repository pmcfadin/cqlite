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
use crate::observability::read_metrics::ReadOpMeter;
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
        // ONE meter for the whole logical read, started at FUNCTION ENTRY (issue
        // #1701, roborev B1/F4/round-4) so the reader-lock + resolution latency below
        // is inside the reported duration. Format-agnostic (`None`) like every other
        // MANAGER-seam meter (`manager_point_read`, the `mod.rs` scans): at entry we
        // do not yet know this is a single-generation read, and a label is attached
        // only when the operation provably reads one SSTable of a known format.
        //
        // DISCARDED on every `Ok(None)` exit — see the returns below.
        let mut meter = ReadOpMeter::start(None);

        // Issue #1591: snapshot the reader list and DROP the read guard before any
        // I/O (candidate prune + the block-walk delegated to the reader).
        let (reader_list, _fully_qualified_match) = self.resolve_reader_snapshot(table_id).await;

        // Prune to the candidate generations that admit the key. Reverse iteration
        // covers ONLY the single-generation case: with several generations the same
        // (partition, clustering) row may appear in more than one, and reconciling
        // them is the in-memory-sort path's job (cross-generation last-write-wins).
        // C4 (#1575): the BTI key hash+encoding is hoisted to once per read here.
        let (candidates, pruned) = Self::prune_candidates(&reader_list, partition_key);

        // Issue #2163 (roborev r7): verify pruned candidates ONLY on the
        // `candidates.len() == 1` FAST PATH below — that branch serves the read
        // DIRECTLY from `candidates[0]` via `big_reverse_partition_rows` and
        // returns without ever reaching the caller's fallback. A false negative
        // that wrongly excluded a co-holding generation on THIS branch is the
        // silent-miss case the switch exists to catch: unverified, the read
        // would (a) never confirm the wrongly-pruned reader's contradiction, AND
        // (b) wrongly take the single-generation direct-serve path instead of
        // the reconciling in-memory-sort fallback that would have merged both
        // generations' rows. Strictly inside the opt-in switch (a no-op when
        // disabled) and fail-open on `Err` (the same loud-error contract the
        // other `verify_pruned_candidates` call sites use).
        //
        // The `candidates.len() != 1` branch deliberately does NOT verify here:
        // every such outcome (zero OR multiple admitted) returns `Ok(None)`, and
        // the caller's fallback (`query::select_executor::lookup`) ALWAYS
        // re-runs the SAME deterministic prune via `scan_partition_clustering`,
        // which DOES call `verify_pruned_candidates` on the identical
        // `reader_list`/`partition_key` (same bloom/trie state ⇒ byte-identical
        // exclusion set). Verifying on BOTH branches would double-COUNT a
        // contradicted negative (once here, once in the fallback) for the SAME
        // logical read — so this call is gated to fire only where the fallback
        // will never run.
        if candidates.len() == 1 {
            Self::verify_pruned_candidates(&pruned, table_id, partition_key).await;
        }

        if candidates.len() != 1 {
            // DECLINED: the caller re-runs this read through the metered forward
            // path (`scan_partition_clustering`), which emits its own operation.
            // `Drop` emits, so the meter must be DISCARDED rather than dropped, or
            // one `ORDER BY DESC` read would report two durations.
            meter.discard();
            return Ok(None);
        }

        let rows = candidates[0]
            .big_reverse_partition_rows(partition_key, schema)
            .await?;

        match &rows {
            // SERVED here: this is the operation. Count the returned rows (the
            // reverse walk materialises them, so this is bookkeeping over a result
            // the caller already holds) and emit once.
            Some(materialized) => {
                meter.record_keys(materialized.iter().map(|(key, _)| key));
                meter.finish();
            }
            // DECLINED inside the reader (narrow/static/variable-width clustering, an
            // open range-tombstone marker): same fallback, same discard.
            None => meter.discard(),
        }
        Ok(rows)
    }
}
