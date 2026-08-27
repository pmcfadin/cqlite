//! The partition point-read entry points: [`SSTableReader::get`] and
//! [`SSTableReader::get_with_resolution`] (split out of `data_access/mod.rs` per the
//! campsite rule, epic #1116).
//!
//! This is the resolution-mode boundary for a point read — it chooses the BTI trie
//! path or the BIG (`Index.db` + covering-chunk) path, applies the authoritative
//! out-of-range short-circuit, and owns the opt-in presence-oracle false-negative
//! verification. The per-format machinery itself lives in `bti_point` / `big_point`.
//!
//! It is also where the point read reports its READ METRICS (issue #1701): one
//! `cqlite.read.rows` + `cqlite.read.partitions` add and one
//! `cqlite.read.duration` recording per `get`, at operation granularity.

use super::super::SSTableReader;
use crate::observability::read_metrics::ReadOpMeter;
use crate::types::{ScanRow, TableId};
use crate::{Result, RowKey};

impl SSTableReader {
    /// Get a value by key from the SSTable.
    ///
    /// Resolution-mode-agnostic entry point: callers that do not carry the
    /// manager's `resolve_reader_list` signal (e.g. the per-reader helpers in
    /// `partition_lookup`, `schema_aware_reader`, and benchmarks) get the STRICT
    /// table-consistency guard — `fully_qualified_match = false` reproduces exactly
    /// today's `table_ids_match_strict` behavior on the BTI point-lookup path, so
    /// this is a behavior-preserving conservative default. The manager's `get()`
    /// calls [`SSTableReader::get_with_resolution`] with the authoritative signal so
    /// an exact fully-qualified match can accept rows across a benign header-keyspace
    /// divergence (issue #1321, mirroring the seek path #1284).
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<ScanRow>> {
        self.get_with_resolution(table_id, key, false).await
    }

    /// Get a value by key, threading the authoritative resolution mode
    /// (`fully_qualified_match`) into the BTI point-lookup guard (issue #1321).
    ///
    /// See [`SSTableReader::get`] for the resolution-mode contract. Only the BTI
    /// ("da") point-lookup path consults `fully_qualified_match`; the bloom/Index.db/
    /// sequential fallbacks are unaffected by it.
    pub async fn get_with_resolution(
        &self,
        table_id: &TableId,
        key: &RowKey,
        fully_qualified_match: bool,
    ) -> Result<Option<ScanRow>> {
        // Read metrics for the point-read OPERATION (issue #1701): ONE
        // `cqlite.read.rows` / `cqlite.read.partitions` add and ONE
        // `cqlite.read.duration` recording per `get`, labelled with this reader's
        // format. Held across every early return below (the authoritative
        // out-of-range short-circuit included) so a read that resolves ABSENCE
        // still reports its latency — a partition-absent lookup is real read work,
        // and dropping it would bias the latency distribution toward hits. Inert,
        // and free of even an `Instant` sample, when metrics are not collected.
        let mut meter = ReadOpMeter::start(Some(self.sstable_format_label()));
        let outcome = self
            .get_with_resolution_inner(table_id, key, fully_qualified_match)
            .await;
        if let Ok(Some(_)) = &outcome {
            // A resolved point read is exactly one row of exactly one partition.
            meter.record_row(key);
        }
        meter.finish();
        outcome
    }

    /// The point-read body [`get_with_resolution`](Self::get_with_resolution)
    /// measures. Split out so the metric accounting wraps EVERY exit — including
    /// the `?` propagations and the early authoritative-absence return — without
    /// threading an emit call through each of them.
    async fn get_with_resolution_inner(
        &self,
        table_id: &TableId,
        key: &RowKey,
        fully_qualified_match: bool,
    ) -> Result<Option<ScanRow>> {
        // Issue #1576 (C5): O(1) authoritative range short-circuit. If the query key
        // sorts outside this SSTable's [first_key, last_key] bound (Summary.db, in
        // Cassandra token order — no heuristics), the partition is definitely absent;
        // return absence BEFORE any bloom check, Index.db probe, or BTI trie descent.
        // Inclusive bound (== first/last stays in range), so it never drops a present
        // partition. A no-op when no authoritative bound exists (BTI/no Summary).
        if self.partition_key_out_of_range(key.as_bytes()) {
            crate::storage::sstable::read_work_counters::record_range_short_circuit();
            return Ok(None);
        }

        // Issue #831 / #909: BTI ("da") readers resolve partitions via the
        // Partitions.db trie (O(log n)), never via Index.db (absent for BTI) or
        // the sequential scan. The trie is the AUTHORITATIVE presence oracle for a
        // BTI SSTable — it answers present/absent definitively — so we branch here
        // BEFORE the bloom-filter pre-check. Skipping the bloom filter for BTI is
        // both correct (the trie is authoritative; bloom is only an optimization)
        // and necessary: a writer-produced Filter.db whose hashing does not match
        // the reader's would otherwise cause false negatives and drop live
        // partitions (the writer→reader roundtrip #909 must read back). It also
        // guarantees a BTI get() can never fall through to scan_for_key.
        let (row, oracle_pruned) = if self.bti_partitions_db.is_some() {
            self.bti_point_lookup(table_id, key, fully_qualified_match)
                .await?
        } else {
            // BIG ("nb"/uncompressed) readers: raw-key Index.db resolve +
            // covering-chunk seek (issue #1572). The bloom pre-check, the fast
            // Index.db-resolved chunk-targeted decode, and the index-less
            // `scan_for_key` fallback all live in `big_point`.
            self.big_get_with_resolution(table_id, key, fully_qualified_match)
                .await?
        };

        // Issue #2163 (roborev r4): `oracle_pruned` is `true` ONLY when the
        // presence oracle itself (bloom-miss for BIG / trie-miss for BTI) excluded
        // this SSTable from the read BEFORE any decode or scan — the PRIMARY
        // single-reader point-read path, which the spec scenario "a partition
        // point lookup ... through the public read surface" names directly. This
        // is the SAME emit site `might_contain_partition[_encoded]` use (via
        // `emit_sstable_pruned`), so a candidate pre-pruned by
        // `SSTableManager::prune_candidates` (excluded from the candidate list, so
        // `get()` is never called on it for this read) is never double-counted:
        // exactly one of {prune-time check, this get-time check} runs per SSTable
        // per logical read.
        if oracle_pruned {
            self.emit_sstable_pruned();

            // Opt-in presence-oracle false-negative verification: when the
            // default-off switch is enabled, an AUTHORITATIVE confirmation scan
            // proves this exclusion truthful; a contradiction increments
            // `cqlite.read.bloom.false_negatives`. Off by default → this whole
            // block is skipped and the read costs nothing extra. Gated on
            // `oracle_pruned` (not merely `row.is_none()`) so a `None` reached via
            // the primary path's OWN authoritative `scan_for_key` — which already
            // IS the confirming scan — never triggers a REDUNDANT second scan.
            if super::super::presence_verification::enabled() {
                if let Err(e) = self
                    .verify_presence_oracle_negative(table_id, key.as_bytes())
                    .await
                {
                    // Issue #2163 (roborev r5): the READ stays fail-open — a
                    // verification-scan failure (e.g. `scan_for_key` erroring on
                    // corruption or an unreadable SSTable) must NEVER fail (or
                    // even affect) the actual read this opt-in check is merely
                    // double-checking; `row` above is returned unchanged either
                    // way. But a SILENT-MISS DETECTOR that itself fails silently
                    // defeats its own purpose, so the failure is surfaced LOUDLY
                    // instead of discarded: an error-level log with context, AND
                    // a record through the EXISTING error-rate signal
                    // (`cqlite.errors.total{category,subsystem}`, issue #1038) —
                    // never a new metric. `record_error` maps `Error::Corruption`
                    // (the typical `scan_for_key` failure mode) to the bounded
                    // `Corruption` category.
                    tracing::error!(
                        error = %e,
                        sstable_format = self.sstable_format_label(),
                        "opt-in presence-oracle false-negative verification scan FAILED — the \
                         read itself is unaffected (fail-open), but this soundness check could \
                         not run for this SSTable and needs investigation"
                    );
                    crate::observability::record_error(&e, "reader");
                }
            }
        }
        Ok(row)
    }
}
