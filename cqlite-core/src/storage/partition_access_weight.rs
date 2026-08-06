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
//! # Known limitations, with their DIRECTION stated
//!
//! They fail in DIFFERENT directions, and the difference is the point of stating them:
//! the first can bias the measurement toward "the cache is worth building", the second
//! biases the safe way, and the third only ever REFUSES to measure. None is left
//! implicit.
//!
//! 1. **UNSAFE direction — the generation set is captured before the read, not shared
//!    with it.**
//!    [`Self::snapshot_for_probe`] resolves the reader list immediately BEFORE the
//!    read and holds the `Arc`s across it, so a generation the read used that
//!    compaction removed mid-read is still priced. It does NOT eliminate the race:
//!    the read takes its own snapshot a moment later, so a generation created in
//!    between — a flush, or a compaction output — is priced by neither, and that
//!    partition is UNDER-priced — and under-pricing bytes lets more buckets fit the
//!    budget, so it RAISES `H_max`. Closing it properly means threading the read's own
//!    snapshot out of `SSTableManager`, which is not reachable from here today.
//! 2. **SAFE direction — the table scope is derived from the caller's SPELLING of the
//!    table name (#3345).** This module passes
//!    `TableScope::from_qualified(table_id.name())`, and `TableId` carries the CQL text
//!    as typed, so `ks.users` and a bare `users` are two identities for one table — and
//!    the SSTable manager's deliberate fallback from `keyspace.table` to the bare name
//!    makes both spellings reachable for the same read. A split raises
//!    `distinct_partitions` and counts the partition's bytes twice, so it UNDERSTATES
//!    concentration and inflates the working set: never a false "go". Note the sibling
//!    Flight pricing path (`merge/point_read.rs`) already derives the canonical
//!    `TableScope::new(&schema.keyspace, &schema.table)`, so this site is the odd one
//!    out; #3345 harmonizes them by normalizing against the resolved snapshot's
//!    fully-qualified-match signal, which `snapshot_for_probe` currently discards.
//! 3. **REFUSAL, not a bias — a BIG generation whose `Index.db` is not resident
//!    cannot be priced at all** (see the cost section above), so under #2412's lazy
//!    Summary-guided open such a window is REFUSED rather than priced. A refusal is
//!    never a false "go", so this cannot skew a verdict; what it costs is
//!    availability — the promise that a verdict falls out of the first real window
//!    holds for BTI and for BIG-with-resident-index, not universally.
//!
//! The WRITETIME/TTL projection point read (`scan_partition_with_cell_metadata`) is
//! **no longer** a limitation: it is recorded like every other logical point read.
//! Omitting it was never conservative — an unrecorded access leaves the DENOMINATOR
//! as well as the numerator, so dropping a workload's metadata singletons while
//! keeping its repeat traffic raises `H_max`.

use super::StorageEngine;
use crate::observability::partition_access::{self, AccessWeightBuilder};
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
        let generations = self.snapshot_for_probe(table_id).await;
        let outcome = self
            .sstables
            .scan_partition(table_id, partition_key, schema)
            .await;
        if outcome.is_ok() {
            Self::record_access_if_enabled(generations, table_id, partition_key).await;
        }
        outcome
    }

    /// [`StorageEngine::scan_partition_with_cell_metadata`] with the #2827 probe
    /// attached.
    ///
    /// A `SELECT WRITETIME(col) / TTL(col) … WHERE pk = ?` is a LOGICAL POINT READ
    /// like any other, and omitting it is **not** conservative. An unrecorded access
    /// leaves the DENOMINATOR as well as the numerator, so dropping a workload's
    /// metadata singletons while keeping its repeat traffic RAISES `H_max` —
    /// 1M metadata singletons beside 100 partitions read 100 times each measure
    /// ≈0.99 against a true ≈0.0098, a confident false "go". It is recorded for
    /// exactly that reason.
    pub(crate) async fn scan_partition_with_cell_metadata_recorded(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<(
        Vec<(
            RowKey,
            ScanRow,
            std::collections::HashMap<String, crate::types::CellWriteMetadata>,
        )>,
        bool,
    )> {
        let generations = self.snapshot_for_probe(table_id).await;
        let outcome = self
            .sstables
            .scan_partition_with_cell_metadata(table_id, partition_key, schema)
            .await;
        if outcome.is_ok() {
            Self::record_access_if_enabled(generations, table_id, partition_key).await;
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
        let generations = self.snapshot_for_probe(table_id).await;
        let outcome = self
            .sstables
            .scan_partition_clustering(table_id, partition_key, clustering, schema)
            .await;
        if outcome.is_ok() {
            Self::record_access_if_enabled(generations, table_id, partition_key).await;
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
        let generations = self.snapshot_for_probe(table_id).await;
        let outcome = self
            .sstables
            .scan_partition_clustering_reverse(table_id, partition_key, schema)
            .await;
        if matches!(outcome, Ok(Some(_))) {
            Self::record_access_if_enabled(generations, table_id, partition_key).await;
        }
        outcome
    }

    /// The generation set to price against, captured BEFORE the read.
    ///
    /// Ordering matters and is not incidental. Resolving the snapshot AFTER the read
    /// returned meant a generation the read had used but that compaction removed in
    /// the meantime was simply absent from the pricing — the partition was
    /// under-priced, which flatters the cache, the one direction this instrument
    /// must not be wrong in. Capturing first and holding the `Arc`s keeps every
    /// reader the read could have used alive and priceable for as long as the
    /// weight resolution needs it.
    ///
    /// This narrows the race rather than removing it: the read takes its OWN
    /// snapshot slightly later, so a generation that appears between the two (a
    /// flush, or a compaction output) is priced by neither. See the module header's
    /// limitations.
    ///
    /// **It has a cost, per this file's own standard.** Holding the `Arc`s DEFERS
    /// reclamation of a compacted-away generation's file descriptor, mmap and
    /// resident index until the last in-flight probed read finishes — and on BIG
    /// that read includes the O(partition-count) successor walk, so the deferral is
    /// not instantaneous. Bounded by in-flight point reads, and paid only while the
    /// probe is on.
    ///
    /// Returns `None` — and touches no lock — when the probe is off.
    async fn snapshot_for_probe(
        &self,
        table_id: &TableId,
    ) -> Option<Vec<std::sync::Arc<crate::storage::sstable::reader::SSTableReader>>> {
        if !partition_access::enabled() {
            return None;
        }
        let (readers, _fully_qualified_match) =
            self.sstables.resolve_reader_snapshot(table_id).await;
        Some(readers)
    }

    /// Price and record one logical access against the pre-read generation set.
    ///
    /// A `None` snapshot means the probe was off when the read began; it stays off
    /// for this access even if it was enabled mid-read, because a weight resolved
    /// against a snapshot that was never taken would be a guess.
    async fn record_access_if_enabled(
        generations: Option<Vec<std::sync::Arc<crate::storage::sstable::reader::SSTableReader>>>,
        table_id: &TableId,
        partition_key: &[u8],
    ) {
        let Some(readers) = generations else {
            return;
        };
        let mut weight = AccessWeightBuilder::new();
        for reader in &readers {
            Self::note_reader_contribution(reader, partition_key, &mut weight).await;
        }
        // The table is part of the entry identity: one recorder serves every table,
        // so the same key bytes in two tables must not merge into one partition.
        partition_access::record_partition_access(
            partition_access::TableScope::from_qualified(table_id.name()),
            partition_key,
            weight.finish(),
        );
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
