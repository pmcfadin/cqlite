//! Byte weighting for the bounded partition access-distribution probe on the CORE
//! targeted read path (issue #2827, design D6).
//!
//! The probe counts one access per LOGICAL partition read and weights it by the
//! partition's on-disk extent. This module resolves that weight at the logical
//! boundary, immediately after the read, by asking each candidate generation two
//! questions: does it hold the key, and if so what is the partition's extent.
//!
//! # Residency is RESOLVED, never inferred from the key cache
//!
//! An earlier version read a key-cache MISS as "this generation did not hold the
//! key". That is not what a miss means: the cache is a byte-budgeted LRU that
//! evicts, has a disabled mode returning `None` unconditionally, misses whenever a
//! reader has no generation identity, and is populated by only one resolution path.
//! Under that reading, one surviving cached generation beside one
//! evicted-but-held generation produced a PARTIAL sum published as a fully measured
//! extent — under-pricing the working set, which flatters the cache, and violating
//! the spec's "SHALL NOT silently under-report" outright.
//!
//! So each candidate is classified by
//! [`PartitionResidency`](crate::storage::sstable::reader::partition_successor::PartitionResidency),
//! which keeps three states distinct:
//!
//! - **`NotHeld`** — definitive absence (BTI trie miss, BIG bloom negative, or a C5
//!   range short-circuit). Contributes nothing, and its absence is not a gap.
//! - **`HeldAt(offset)`** — present. The extent is MEASURED as the successor gap.
//! - **`Unknown`** — indeterminate (corrupt trie, unresolvable `Rows.db`, a BIG
//!   `Index.db` miss — which is NOT definitive absence, #1572 — or an index that is
//!   not resident). **Fails closed**: the access is reported
//!   `size_source = unavailable` and contributes ZERO bytes.
//!
//! Pricing therefore does not depend on cache retention at all. The key cache is
//! still consulted, but only for an index-recorded `data_size` — which no Cassandra
//! 5.0 index actually records, so in practice the extent is always the measured gap.
//!
//! # What it will not do to get an answer
//!
//! - **It emits no metric and bumps no read-work counter.** Residency uses the
//!   slice-level trie primitive and the resident index map (and the `_uncounted` BTI
//!   helpers), never the counter-emitting façades, so enabling the probe cannot
//!   perturb `cqlite.read.partition_lookup.total`, `cqlite.read.bloom.checks`,
//!   `cqlite.read.sstables_pruned`, or the #1575/#1647 read-work assertions.
//! - **It never materializes an `Index.db`.** Forcing materialization would defeat
//!   #2412's lazy Summary-guided open and permanently add resident index bytes to
//!   the process — an observability probe must not mutate the read path's state.
//! - **It never estimates a size.** No interpolation, no nominal default (#28).
//!
//! # Cost, stated rather than implied
//!
//! Enabling the probe is NOT free on the BIG path, and the two things it costs are:
//!
//! 1. **A BIG generation whose `Index.db` is not already resident cannot be priced.**
//!    It is reported `Unknown` → `unavailable` → the window is refused. Pricing a
//!    lazily-opened BIG table needs the index resident for some other reason.
//! 2. **The successor-gap resolution is O(partition-count) per access per
//!    generation** for BIG: `successor_partition_offset` takes the minimum
//!    `data_offset` strictly greater than the target over every `Index.db` entry.
//!    On a million-partition table across `k` generations that is ~`k` million
//!    iterations per point read. BTI pays O(depth) instead (one strict-ceiling trie
//!    walk). This is a real per-read cost that only an operator who has switched the
//!    probe on pays, and it is why the probe is default-OFF.
//!
//! # Known coverage limitation
//!
//! [`StorageEngine::scan_partition_with_cell_metadata`] — the WRITETIME/TTL
//! projection's point read — is a logical point read that this module does NOT wrap,
//! so its accesses are invisible to the histogram. The direction is conservative
//! (those partitions are under-counted, understating concentration), but it is a gap:
//! **a workload whose keyed traffic is predominantly WRITETIME/TTL projections is
//! measured badly, and its window MUST NOT be used for the decision.** Recorded in
//! `design.md` D2, the spec's wiring requirement, and the decision note.

use super::StorageEngine;
use crate::observability::partition_access::{self, AccessWeight, AccessWeightBuilder};
#[cfg(not(feature = "tombstones"))]
use crate::storage::sstable::reader::partition_successor::PartitionResidency;
use crate::types::{RowKey, ScanRow, TableId};
use crate::Result;

impl StorageEngine {
    /// [`StorageEngine::scan_partition`] with the #2827 probe attached.
    ///
    /// This is THE logical point-read boundary for the core read path: every
    /// targeted `WHERE pk = ?` — and each key of an `IN` fan-out — funnels through
    /// `scan_partition` exactly once per logical partition read, whichever executor
    /// (streaming, buffered, aggregate) issued it. Recording here therefore counts
    /// each logical read once and only once, no matter how many SSTable generations
    /// the read had to probe underneath.
    ///
    /// It is deliberately NOT recorded at the per-SSTable probe sites. With
    /// size-tiered compaction a live partition is present in *k* generations at
    /// once, so per-probe counting would multiply every partition's repeat count by
    /// roughly *k*, shift the whole histogram right, and manufacture concentration
    /// the workload does not have — a bias toward "the cache is worth building".
    /// Those sites supply byte weights only.
    ///
    /// A failed read records nothing: an access that did not happen is not an
    /// access.
    pub(crate) async fn scan_partition_recorded(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<(Vec<(RowKey, ScanRow)>, bool)> {
        let outcome = self
            .sstables
            .scan_partition(table_id, partition_key, schema)
            .await;
        if outcome.is_ok() {
            self.record_access_if_enabled(table_id, partition_key).await;
        }
        outcome
    }

    /// [`StorageEngine::scan_partition_clustering`] with the #2827 probe attached.
    ///
    /// A clustering-sliced `WHERE pk = ? AND ck …` is still exactly ONE logical
    /// partition read, and it is the boundary the BUFFERED executor takes (the
    /// streaming executor takes `scan_partition`). Both are recorded so the
    /// histogram does not depend on which executor served the query.
    #[cfg(not(feature = "tombstones"))]
    pub(crate) async fn scan_partition_clustering_recorded(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        clustering: Option<&crate::storage::sstable::reader::ClusteringSlice>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<(Vec<(RowKey, ScanRow)>, bool)> {
        let outcome = self
            .sstables
            .scan_partition_clustering(table_id, partition_key, clustering, schema)
            .await;
        if outcome.is_ok() {
            self.record_access_if_enabled(table_id, partition_key).await;
        }
        outcome
    }

    /// [`StorageEngine::scan_partition_clustering_reverse`] with the #2827 probe
    /// attached.
    ///
    /// Records ONLY when the reverse iterator actually served the read
    /// (`Ok(Some(_))`). An `Ok(None)` means "not applicable, fall back", and the
    /// caller then issues `scan_partition_clustering` for the SAME logical read —
    /// recording both would count one read twice.
    #[cfg(not(feature = "tombstones"))]
    pub(crate) async fn scan_partition_clustering_reverse_recorded(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Option<Vec<(RowKey, ScanRow)>>> {
        let outcome = self
            .sstables
            .scan_partition_clustering_reverse(table_id, partition_key, schema)
            .await;
        if matches!(outcome, Ok(Some(_))) {
            self.record_access_if_enabled(table_id, partition_key).await;
        }
        outcome
    }

    /// Resolve the weight and record one logical access. A no-op — and it does not
    /// even resolve the reader snapshot — when the probe is off.
    async fn record_access_if_enabled(&self, table_id: &TableId, partition_key: &[u8]) {
        if !partition_access::enabled() {
            return;
        }
        let weight = self.partition_access_weight(table_id, partition_key).await;
        // The table is part of the entry identity: one recorder serves every table,
        // so the same key bytes in two tables must not merge into one partition.
        partition_access::record_partition_access(
            partition_access::TableScope::from_qualified(table_id.name()),
            partition_key,
            weight,
        );
    }

    /// The on-disk byte weight of ONE logical partition access to `partition_key`,
    /// summed across the SSTable generations the read resolved.
    ///
    /// Call immediately AFTER the targeted read, and only when
    /// [`partition_access::enabled`](crate::observability::partition_access::enabled).
    ///
    /// **This is not cheap on the BIG path.** Per candidate generation it costs a
    /// presence check plus a successor resolution: O(depth) for BTI (one trie walk),
    /// but O(partition-count) for BIG (a linear minimum over the resident `Index.db`
    /// entries). It performs no I/O and materializes nothing — a generation whose
    /// index is not already resident is reported unpriceable instead — but the CPU
    /// cost is real, and is one reason the probe is default-OFF. See the module
    /// header.
    pub(crate) async fn partition_access_weight(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
    ) -> AccessWeight {
        let (readers, _fully_qualified_match) =
            self.sstables.resolve_reader_snapshot(table_id).await;
        let mut weight = AccessWeightBuilder::new();
        for reader in &readers {
            Self::note_reader_contribution(reader, partition_key, &mut weight).await;
        }
        weight.finish()
    }

    /// One candidate's contribution: definitive absence contributes nothing, a
    /// resolved partition contributes its MEASURED extent, and anything
    /// indeterminate fails closed.
    #[cfg(not(feature = "tombstones"))]
    async fn note_reader_contribution(
        reader: &std::sync::Arc<crate::storage::sstable::reader::SSTableReader>,
        partition_key: &[u8],
        weight: &mut AccessWeightBuilder,
    ) {
        match reader.partition_residency(partition_key).await {
            // Definitively absent: contributes nothing, and its absence is not a gap
            // in the measurement.
            PartitionResidency::NotHeld => {}
            PartitionResidency::HeldAt(data_offset) => {
                // An index-recorded size, if one ever exists. No Cassandra 5.0 index
                // format records one, so in practice this never fires — it is kept
                // so a producer that genuinely knows a size is not forced to report
                // a measured one.
                match reader.key_cache_get(partition_key) {
                    Some(loc) if loc.data_size > 0 => weight.note_sized(loc.data_size),
                    _ => {
                        Self::note_measured_extent(reader, data_offset, partition_key, weight).await
                    }
                }
            }
            // Held-or-not could not be determined. FAIL CLOSED: treating this as
            // absence is what lets a partial sum be published as a fully measured
            // extent.
            PartitionResidency::Unknown => weight.note_unsized(),
        }
    }

    /// `tombstones` build: the seek/residency machinery is compiled out with the
    /// path it serves, so nothing is measurable and every access is honestly
    /// unpriceable.
    #[cfg(feature = "tombstones")]
    async fn note_reader_contribution(
        _reader: &std::sync::Arc<crate::storage::sstable::reader::SSTableReader>,
        _partition_key: &[u8],
        weight: &mut AccessWeightBuilder,
    ) {
        weight.note_unsized();
    }

    /// MEASURE one candidate's contribution as the partition's successor gap.
    ///
    /// Any error, or a genuinely unknowable extent, becomes `unavailable` — the
    /// instrument reports a missing extent rather than inventing one (#28).
    #[cfg(not(feature = "tombstones"))]
    async fn note_measured_extent(
        reader: &std::sync::Arc<crate::storage::sstable::reader::SSTableReader>,
        data_offset: u64,
        partition_key: &[u8],
        weight: &mut AccessWeightBuilder,
    ) {
        match reader
            .measure_partition_extent(data_offset, partition_key)
            .await
        {
            Ok(Some(gap)) => weight.note_measured(gap),
            Ok(None) => weight.note_unsized(),
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    "partition-access probe could not measure a partition extent; \
                     recording the access as size_source=unavailable (#2827)"
                );
                weight.note_unsized();
            }
        }
    }
}
