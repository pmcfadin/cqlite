//! The `tombstones` build's partition-TARGETED materializing scans:
//! [`SSTableManager::scan_partition`] and
//! [`SSTableManager::scan_partition_with_cell_metadata`] (split out of `mod.rs` per
//! the campsite rule, epic #1116).
//!
//! That build uses a structurally different reader map and has no bloom-prune
//! `scan_partition` path, so a fully-constrained `WHERE pk = ?` is served by scanning
//! the table and filtering to the partition key. The rows are byte-identical to what
//! the pruned `not(tombstones)` path returns, which keeps the query executor free of
//! any `tombstones` cfg branching — but the ACCESS PATH is a full scan, which is why
//! both functions return `engaged == false` (Epic #951, honest access paths).
//!
//! # ONE meter per logical read, owned by the OUTER targeted API (issue #1701, R9-F1)
//!
//! These two functions are the read OPERATION; the full scan underneath them is an
//! implementation detail of it. Letting the inner scan meter itself made a targeted
//! single-partition read report EVERY row and EVERY partition of the table — a
//! `WHERE pk = ?` over an 8-partition fixture reported `read.rows = 8` where the read
//! delivered 1 — so the same metric meant different things in different feature
//! builds (the `not(tombstones)` `scan_partition_clustering` meters the delivered
//! partition) and contradicted the "rows a read DELIVERED" contract
//! [`catalog::READ_ROWS`](crate::observability::catalog::READ_ROWS) states.
//!
//! So the inner scan is handed [`ReadOpMeter::inert`] — a CONSTRUCTION-time no-op,
//! deliberately distinct from `discard()`, which is the RUNTIME decision a declining
//! boundary makes — and the meter that reports lives here, started at FUNCTION ENTRY
//! (the entry-at-function rule `manager_point_read`'s module doc records) and
//! recording the rows AFTER the `retain`, i.e. exactly what the caller receives.

#![cfg(feature = "tombstones")]

use std::collections::HashMap;

use super::SSTableManager;
use crate::observability::read_metrics::ReadOpMeter;
use crate::types::{ScanRow, TableId};
use crate::{Result, RowKey};

impl SSTableManager {
    /// `tombstones`-build counterpart of [`scan_partition`](Self::scan_partition).
    ///
    /// That build uses a structurally different reader map and has no bloom-prune
    /// `scan_partition` path, so a fully-constrained `WHERE pk = ?` is served by
    /// scanning and filtering to the partition key. The output is a subset of
    /// [`scan`](Self::scan) — identical to what the `not(tombstones)`
    /// `scan_partition` returns — which keeps the query executor free of any
    /// `tombstones` cfg branching.
    ///
    /// Returns `(rows, engaged)` with `engaged == false`: this is a full scan +
    /// retain with NO SSTable prune, so the caller MUST report an honest fallback
    /// access path (`FallbackReason::TombstonesBuildNoPrune`) rather than a targeted
    /// label, even though the rows match the pruned build byte-for-byte (Epic #951).
    pub async fn scan_partition(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<(Vec<(RowKey, ScanRow)>, bool)> {
        // The meter for THIS logical read (module doc): started at entry so the
        // scan's own latency is inside the reported duration, inert underneath so the
        // full scan emits nothing, and recording the DELIVERED rows after `retain`.
        let mut meter = ReadOpMeter::start(None);
        let mut rows = self
            .scan_with_meter(table_id, None, None, None, schema, ReadOpMeter::inert())
            .await?;
        rows.retain(|entry| entry.0.as_bytes() == partition_key);
        meter.record_keys(rows.iter().map(|(k, ..)| k));
        Ok((rows, false))
    }

    /// `tombstones`-build counterpart of
    /// [`scan_partition_with_cell_metadata`](Self::scan_partition_with_cell_metadata).
    ///
    /// That build has no bloom-prune metadata path, so a fully-constrained
    /// `WHERE pk = ?` WRITETIME/TTL read is served by scanning with metadata and
    /// filtering to the partition key, matching the `not(tombstones)` output while
    /// keeping the query executor free of `tombstones` cfg branching.
    ///
    /// Returns `(rows, engaged)` with `engaged == false`: this is a full metadata
    /// scan + retain with NO SSTable prune, so the caller MUST report an honest
    /// fallback access path (`FallbackReason::TombstonesBuildNoPrune`) rather than a
    /// targeted label, even though the rows are byte-identical to the pruned build
    /// (Epic #951, honest access paths).
    pub async fn scan_partition_with_cell_metadata(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<(
        Vec<(
            RowKey,
            ScanRow,
            HashMap<String, crate::types::CellWriteMetadata>,
        )>,
        bool,
    )> {
        // Same ownership rule as `scan_partition` above (module doc).
        let mut meter = ReadOpMeter::start(None);
        let mut rows = self
            .scan_with_cell_metadata_with_meter(
                table_id,
                None,
                None,
                None,
                schema,
                ReadOpMeter::inert(),
            )
            .await?;
        rows.retain(|entry| entry.0.as_bytes() == partition_key);
        meter.record_keys(rows.iter().map(|(k, ..)| k));
        Ok((rows, false))
    }
}
