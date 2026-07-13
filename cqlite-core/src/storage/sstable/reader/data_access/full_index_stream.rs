//! Streaming partition enumeration for the non-stitching read path (issue #2361).
//!
//! [`SSTableReader::iterate_all_partitions_via_full_index`](super::full_index_scan)
//! (issue #2302) resolves every partition of a BIG SSTable through the full
//! `Index.db` offset table, but MATERIALISES the whole result into one sorted
//! `Vec` before returning. The compaction/Flight `do_get` streaming producer then
//! forwards that Vec one entry at a time — so for a huge SSTable (the #2361 field
//! case: 1.13M partitions, uncompressed BIG, which routes to the non-stitching
//! branch of
//! [`stream_all_partitions_for_compaction`](super::super::SSTableReader::stream_all_partitions_for_compaction))
//! the producer builds the ENTIRE table in memory before the first `emit`, the
//! consumer's `LIMIT` early-break and cancellation cannot take effect until that
//! completes, and peak memory grows unbounded.
//!
//! This module adds a TRUE-streaming variant that emits each partition as the
//! index walk resolves it — the same index-ordered (== token-ordered) walk, but
//! without the whole-file `Vec`. The compressed-nb chunk-stitching path already
//! streams through a bounded window; this brings the non-stitching path to
//! parity.
//!
//! ## Fallback safety (why some bails move up-front)
//!
//! The materialising [`iterate_all_partitions_via_full_index`] can bail to a
//! `sequential_scan` fallback on ANY structural inconsistency because it has
//! emitted nothing yet (it only drops a local `Vec`). A STREAMING walk that has
//! already emitted partition `k` into the k-way merge CANNOT then fall back — a
//! re-scan would double-emit and corrupt the merge. So every gate computable from
//! the `Index.db` offset table ALONE (entries present, data section non-empty,
//! index fully parsed, all keys non-empty, offsets strictly ascending, spans fit
//! `u32`) is checked UP-FRONT: a failure returns [`FullIndexStreamOutcome::FellBack`]
//! with nothing emitted, so the caller's `sequential_scan` fallback is still
//! safe. The remaining per-partition checks (index-probe miss, a partition slice
//! that does not fully consume) require reading the partition body, so they can
//! only surface mid-walk — and there they are a hard `Error` (fail-closed), never
//! a silent fallback. For a well-formed BIG SSTable none of them fire.

use super::super::SSTableReader;
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::types::ScanRow;
use crate::{Error, Result, RowKey};
use std::ops::ControlFlow;

/// Outcome of [`SSTableReader::stream_all_partitions_via_full_index`].
pub(in crate::storage::sstable::reader) enum FullIndexStreamOutcome {
    /// Every partition was streamed (the walk ran to completion, hit the
    /// `limit`, or the consumer returned `ControlFlow::Break`).
    Streamed,
    /// An up-front structural gate failed BEFORE any partition was emitted. The
    /// caller must fall back to a (materialising) `sequential_scan`; nothing was
    /// emitted, so the fallback cannot double-emit.
    FellBack,
}

impl SSTableReader {
    /// Streaming sibling of
    /// [`iterate_all_partitions_via_full_index`](Self::iterate_all_partitions_via_full_index):
    /// walk the full `Index.db` offset table and `emit` each partition's rows as
    /// they are resolved, never building a whole-file `Vec`.
    ///
    /// `emit` receives `(row_key, value)` per surviving (tombstone-filtered) row,
    /// in index (token) order, and returns `ControlFlow::Break` to stop early
    /// (consumer dropped / satisfied). `limit`, when set, is a PER-PRODUCER
    /// PARTITION budget: the walk stops after emitting `limit` partitions. See the
    /// caller ([`stream_all_partitions_cancellable`](Self::stream_all_partitions_cancellable))
    /// for the k-way-merge reasoning behind a partition-granular budget.
    pub(in crate::storage::sstable::reader) async fn stream_all_partitions_via_full_index<F>(
        &self,
        scan_cancel: &ScanCancel,
        limit: Option<usize>,
        emit: &mut F,
    ) -> Result<FullIndexStreamOutcome>
    where
        F: FnMut((RowKey, ScanRow)) -> Result<ControlFlow<()>>,
    {
        let Some(index_reader) = &self.index_reader else {
            return Ok(FullIndexStreamOutcome::FellBack);
        };
        let entries = index_reader.get_partition_entries();

        // Exclusive end of the last partition (uncompressed data-section domain).
        let data_section_end = match self.compression_info.as_deref() {
            Some(ci) => ci.data_length,
            None => self
                .stats
                .file_size
                .saturating_sub(self.actual_header_size as u64),
        };

        // Up-front structural gates (no per-partition I/O): any failure => FellBack
        // (nothing emitted; the caller's sequential fallback stays safe). These
        // mirror the same-named checks in the materialising variant, but hoisted
        // BEFORE the first emit so the streaming path never bails mid-walk on a
        // signal it could have seen from the offset table alone.
        if entries.is_empty() || data_section_end == 0 || !index_reader.is_fully_parsed() {
            return Ok(FullIndexStreamOutcome::FellBack);
        }
        for i in 0..entries.len() {
            // Empty partition keys are rejected fail-safe (issue #2302/#2325 — an
            // empty PK round-trips corrupt through BOTH read paths).
            let key = entries[i].raw_key.as_deref().unwrap_or(&[]);
            if key.is_empty() {
                return Ok(FullIndexStreamOutcome::FellBack);
            }
            let start = entries[i].data_offset;
            let end = if i + 1 < entries.len() {
                entries[i + 1].data_offset
            } else {
                data_section_end
            };
            if end <= start || u32::try_from(end - start).is_err() {
                return Ok(FullIndexStreamOutcome::FellBack);
            }
        }

        // All gates passed: stream. Read-shadowing parser (matches sequential_scan
        // + the materialising variant).
        let parser = self.build_v5_parser(true);
        let reader_schema = self.get_table_schema(None);
        let schema = reader_schema.as_ref();

        for i in 0..entries.len() {
            // Cooperative cancellation: one real index-random-read + Data.db parse
            // per partition — poll every entry so a cancelled scan abandons the
            // walk promptly (issue #2264, PER-CALL token issue #2346).
            scan_cancel.check()?;

            // Per-producer partition budget (issue #2361): stop after emitting
            // `limit` partitions. Every prior iteration ran to completion (the only
            // early exits `return` out of the loop), so at the top of iteration `i`
            // exactly `i` partitions have been fully emitted — `i` IS the count.
            if let Some(cap) = limit {
                if i >= cap {
                    return Ok(FullIndexStreamOutcome::Streamed);
                }
            }

            let partition_key = entries[i].raw_key.as_deref().unwrap_or(&[]);
            let Some((data_offset, _size)) =
                self.lookup_partition_with_index(partition_key).await?
            else {
                // Proven present in the offset table up-front, so a probe miss now
                // is an index/data inconsistency — fail closed (may have emitted).
                return Err(Error::corruption(format!(
                    "stream_all_partitions_via_full_index: index probe missed entry {i} \
                     listed in the offset table (index/data inconsistency, issue #2361)"
                )));
            };

            let next_offset = if i + 1 < entries.len() {
                entries[i + 1].data_offset
            } else {
                data_section_end
            };
            if next_offset <= data_offset {
                return Err(Error::corruption(format!(
                    "stream_all_partitions_via_full_index: non-ascending offset at entry {i} \
                     (probe offset {data_offset} >= successor {next_offset}, issue #2361)"
                )));
            }
            let span = next_offset - data_offset;
            let Ok(size) = u32::try_from(span) else {
                return Err(Error::corruption(format!(
                    "stream_all_partitions_via_full_index: partition {i} span {span} \
                     overflows u32 (issue #2361)"
                )));
            };

            let raw = if let Some(ci) = self.compression_info.as_deref() {
                self.read_compressed_offset_window(ci, data_offset, size)
                    .await?
            } else {
                let absolute_offset = data_offset + self.actual_header_size as u64;
                self.read_uncompressed_verified(&self.file, absolute_offset, size as usize)
                    .await?
            };

            // Structural coverage: the slice must decode as exactly one complete
            // partition (issue #2302 Signal B). Mid-walk failure = fail-closed.
            if !self.partition_slice_fully_consumed(&parser, &raw, schema)? {
                return Err(Error::corruption(format!(
                    "stream_all_partitions_via_full_index: partition {i} slice not fully \
                     consumed (truncated/corrupt body, issue #2361)"
                )));
            }

            let parsed = parser.parse_block(&raw, schema, self)?;
            for (_table_id, row_key, value) in parsed {
                if self.filter_tombstone(&value) {
                    match emit((row_key, value))? {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(()) => return Ok(FullIndexStreamOutcome::Streamed),
                    }
                }
            }
        }

        Ok(FullIndexStreamOutcome::Streamed)
    }

    /// Streaming sibling of
    /// [`iterate_all_partitions_cancellable`](Self::iterate_all_partitions_cancellable):
    /// enumerate every partition, emitting each surviving row via `emit` as it is
    /// resolved instead of returning a whole-file `Vec` (issue #2361).
    ///
    /// Routing mirrors the materialising method: a BIG SSTable with a usable
    /// `Index.db` streams through
    /// [`stream_all_partitions_via_full_index`](Self::stream_all_partitions_via_full_index)
    /// (bounded memory, index/token order). Only when that up-front-bails (no
    /// usable/complete index) does it fall back to the MATERIALISING
    /// `sequential_scan` — which cannot stream in token order without collecting +
    /// sorting first, so it stays materialising, bounded instead by `limit`
    /// truncation. The #2361 field table has a valid `Index.db`, so it takes the
    /// streaming branch; the sequential fallback is the rare degenerate path
    /// (missing/malformed index) and is documented, not fixed, here.
    ///
    /// ## `limit` and the k-way merge (partition-granular budget)
    ///
    /// `limit` is a PER-PRODUCER PARTITION budget, not a global row limit. In the
    /// Flight scan each input SSTable is one producer feeding a k-way merger that
    /// interleaves and reconciles across producers, and the consumer breaks at
    /// `limit` POST-reconciliation rows. A producer that has emitted `limit`
    /// partitions has, in the normal case (at least one live row per partition),
    /// already supplied at least `limit` rows on its own (more than the merge can
    /// need), so stopping there is a sound best-effort push-down under the
    /// connector's `limitGuaranteed = false` contract (Trino keeps a global
    /// `Limit` above). The PRIMARY, always-correct bound is still the
    /// bounded-channel backpressure plus prompt cancel teardown: when the consumer
    /// stops pulling, the producer's blocking channel send wakes on receiver-drop
    /// and the producer exits. Residual caveat (scope-flagged): a producer capped
    /// at `limit` partitions that ALL reconcile away under cross-generation
    /// shadowing could under-return; the global Trino `Limit` and the always-on
    /// backpressure bound cover it in practice.
    pub(in crate::storage::sstable::reader) async fn stream_all_partitions_cancellable<F>(
        &self,
        scan_cancel: &ScanCancel,
        limit: Option<usize>,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut((RowKey, ScanRow)) -> Result<ControlFlow<()>>,
    {
        if self.index_reader.is_some() && self.bti_partitions_db.is_none() {
            match self
                .stream_all_partitions_via_full_index(scan_cancel, limit, &mut emit)
                .await?
            {
                FullIndexStreamOutcome::Streamed => return Ok(()),
                FullIndexStreamOutcome::FellBack => {
                    tracing::warn!(
                        "SSTable Index.db is present but the streaming index-random-read path \
                         could not prove every partition resolvable; falling back to a \
                         MATERIALISING sequential scan of Data.db (issues #2361/#2302). This \
                         should not happen for a well-formed BIG SSTable."
                    );
                }
            }
        }

        // Fallback: materialising sequential scan (rare degenerate path). Bounded
        // by `limit` truncation inside `sequential_scan` (LIMIT-after-token-sort).
        let table_id = self.scan_table_id();
        let schema = self.schema.as_deref();
        let entries = self
            .sequential_scan(&table_id, None, None, limit, schema, scan_cancel)
            .await?;
        for (key, value) in entries {
            match emit((key, value))? {
                ControlFlow::Continue(()) => {}
                ControlFlow::Break(()) => return Ok(()),
            }
        }
        Ok(())
    }

    /// Authoritative structural coverage check for ONE index entry's Data.db slice
    /// (issue #2302, roborev jobs 1606/1609/1610): physically decode `raw` as
    /// exactly one partition and require the decode to consume the slice in FULL
    /// AND terminate via a CONFIRMED end-of-partition marker, never a bare "ran out
    /// of bytes" collapse. Shared by the materialising
    /// ([`iterate_all_partitions_via_full_index`](Self::iterate_all_partitions_via_full_index))
    /// and streaming full-index walks (issue #2361).
    ///
    /// Drives with `at_final_chunk = false` DELIBERATELY: that is LENIENT-off, so
    /// every ambiguous "consumed every byte but saw no explicit END_OF_PARTITION
    /// marker" / row-parse-failure / unrepresentable-range case reports
    /// `ParseStep::NeedMore` (never `Emitted`). `Emitted(consumed)` is then
    /// reachable ONLY via the driver's unconditional `is_end_of_partition`
    /// marker-consumption branch — so `consumed == raw.len()` PROVES the terminator
    /// was structurally confirmed as the slice's last byte (Cassandra's own
    /// on-disk completion signal, which CQLite's writer always appends). No
    /// heuristics (issue #28).
    pub(in crate::storage::sstable::reader) fn partition_slice_fully_consumed(
        &self,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
        raw: &[u8],
        schema: Option<&TableSchema>,
    ) -> Result<bool> {
        use crate::storage::sstable::reader::parsing::ParseStep;
        let mut noop = |_row| Ok(std::ops::ControlFlow::Continue(()));
        let step =
            parser.parse_one_partition_for_compaction(raw, schema, self, false, &mut noop)?;
        Ok(matches!(step, ParseStep::Emitted(consumed) if consumed == raw.len()))
    }
}
