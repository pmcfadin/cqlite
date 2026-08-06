//! Byte weighting for the bounded partition access-distribution probe on the CORE
//! targeted read path (issue #2827, design D6).
//!
//! The probe counts one access per LOGICAL partition read and weights it by the
//! on-disk bytes that read actually resolved. On the core executor's targeted path
//! (`WHERE pk = ?` served by [`StorageEngine::scan_partition`]) the resolution
//! happens inside the SSTable manager, across the candidate generations that hold
//! the key; this module recovers the resolved sizes at the logical boundary,
//! immediately after the read.
//!
//! # It never performs its own lookup, and that is the whole design
//!
//! The weight comes only from what the read that just ran already resolved and
//! recorded in the process-global key→partition-offset cache
//! ([`PartitionLoc`](crate::storage::cache::PartitionLoc), issue #2059). The
//! instrument therefore:
//!
//! - **cannot perturb the read path's own telemetry.** Re-driving `locate` would
//!   double-count `cqlite.read.partition_lookup.total` and
//!   `cqlite.read.bloom.checks` for every access, corrupting an operator's
//!   dashboards exactly when they switch the probe on.
//! - **cannot invent a size.** A partition whose location the read left with no
//!   size — BTI trie resolution stores `data_size = 0`, since the trie records an
//!   offset and nothing else — is reported `size_source = unavailable` and
//!   contributes ZERO bytes. Never bounded from a successor offset, never
//!   defaulted (no-heuristics, #28).
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
//!
//! # Where it fails closed, stated rather than hidden
//!
//! A candidate whose cached location is absent is treated as "did not hold the
//! key" (no contribution), because the read that just ran inserts a location for
//! every generation that DID hold it. The residual case — the key cache disabled,
//! or an entry reclaimed between the read and this call — therefore yields an
//! access with nothing to price, which
//! [`AccessWeightBuilder::finish`](crate::observability::partition_access::AccessWeightBuilder::finish)
//! reports as `unavailable` rather than as zero bytes. A refused window is the
//! cost; the alternative failure would understate the working set, i.e. point
//! toward "the cache fits, build it", which is the one direction a go/no-go
//! instrument must not be wrong in.

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
    /// [`partition_access::enabled`](crate::observability::partition_access::enabled)
    /// — it is cheap (a per-candidate hash-map probe, no I/O) but it is not free,
    /// and the probe is off by default.
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
