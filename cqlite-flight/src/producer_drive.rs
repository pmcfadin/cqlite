//! The BUFFERED (whole-partition) read-path drive loops (campsite split of
//! `producer.rs`, epic #1116).
//!
//! Sibling of `producer_stream.rs`, which owns the ROW-GRANULAR streaming loop
//! (issue #2230, the production `do_get` row path). This module holds the two loops
//! that step the merge one WHOLE partition at a time:
//!
//! * [`MergeProducer::drive_merge`] — the buffered collect route behind the public
//!   `produce` / `produce_from_paths` / `produce_from_resolved`;
//! * [`MergeProducer::drive_aggregate`] — the aggregation route (issue #841), which
//!   every aggregating ticket takes (`bypass_reason` returns `Aggregating`).
//!
//! Both apply Cassandra's STATIC semantics through the SHARED choke point
//! `statics::drive_partition_rows` (issue #3095), so they cannot drift from the
//! streaming loop on statics.

use cqlite_core::export::estimate_arrow_row_bytes;
use cqlite_core::query::{PartitionKeyCache, QueryRow};
use cqlite_core::storage::write_engine::merge::MergeStep;

use crate::agg::AggPlan;
use crate::batch_bytes::BatchByteCap;
use crate::cancel::CancelFlag;
use crate::producer::{BatchSink, MergeProducer, PartitionStepper, ProducerError};
use crate::scan_progress::{ScanProgress, ScanProgressMeter};

impl MergeProducer {
    /// Drive the row-merge loop over `merger`, appending full-row batches.
    ///
    /// Cooperative cancellation (issue #1473) is polled BEFORE each
    /// `merger.step()`, so a cancel (e.g. client disconnect) stops the merge
    /// before collecting/reconciling the next partition — never after performing
    /// one more potentially large partition merge.
    ///
    /// LIMIT pushdown (issue #2129): when the scan carries a row cap
    /// ([`ScanSpec::limit`](crate::filter::ScanSpec)), the merge stops as soon as
    /// `limit` rows have been EMITTED. The cap is counted AFTER the token filter
    /// and the predicate filter, so partitions outside the range and rows the
    /// filter rejects never consume it — a filtered scan returns as many matching
    /// rows as exist up to `limit`, never fewer. `limit == Some(0)` emits nothing
    /// without stepping the merge at all. The cap applies per split; the connector
    /// sets `limitGuaranteed = false` so Trino keeps a global `Limit` above.
    pub(crate) fn drive_merge(
        &self,
        merger: &mut dyn PartitionStepper,
        cancel: &CancelFlag,
        sink: &mut dyn BatchSink,
        progress: &ScanProgress,
        access_path: &'static str,
    ) -> Result<(), ProducerError> {
        let limit = self.spec.limit;
        // A zero cap produces no rows without touching the merge.
        if limit == Some(0) {
            return Ok(());
        }
        // Incremental scan-progress meter (issue #2162): flushes rows_scanned /
        // read.rows / read.partitions deltas at the batch-scale threshold and, via
        // its `Drop`, the remainder on EVERY exit (completion, LIMIT break, cancel,
        // error, panic). `access_path` is `full_scan` for the k-way scan and
        // `streaming_partition_lookup` for the point-read path (issue #2207).
        let mut meter = ScanProgressMeter::new(progress, access_path);
        let mut buffer: Vec<QueryRow> = Vec::with_capacity(self.batch_size);
        let mut emitted: u64 = 0;
        // Issue #2825: running payload-byte estimate for the rows currently
        // buffered. Advanced by exactly one row's estimate per push and reset on
        // every flush — the buffer is never re-measured.
        let mut byte_cap = BatchByteCap::new(self.max_batch_bytes);
        // Issue #2821: Arrow array NODES over the projected output schema,
        // counted ONCE per merge and fed to every pre-materialization egress
        // reservation (the per-node slack term the bare capacity factor misses).
        let n_array_nodes = self.egress_array_nodes()?;
        // Issue #1817: one partition-key decode cache for the whole merge; each
        // partition's rows arrive consecutively, so its key decodes once.
        let mut pk_cache = PartitionKeyCache::default();
        // Issue #2324 (roborev 1633): projection-aware assembly set, computed once.
        let assemble_cols = self.assemble_columns();

        'partitions: loop {
            if cancel.is_cancelled() {
                return Err(ProducerError::Cancelled);
            }
            // Map by VARIANT, not by racing the cancel flag (roborev, issue
            // #2264): the per-run producer thread's compaction scan now
            // propagates a genuine `Error::Cancelled` (preserved through the
            // channel via `MergeProducerError`, not stringified) when it observes
            // `scan_cancel`. Matching the variant directly means a real
            // I/O/corruption error that happens to race a client disconnect is
            // NEVER masked as a clean `Cancelled` abort — only an actual
            // cancellation maps to `ProducerError::Cancelled`.
            let step = merger.step().map_err(|e| match e {
                cqlite_core::Error::Cancelled => ProducerError::Cancelled,
                other => ProducerError::Merge(other),
            })?;
            let MergeStep::Partition { key, rows } = step else {
                break;
            };
            // Token-range filter: drop whole partitions outside the split's range.
            if let Some(token) = &self.spec.token {
                if !token.contains(key.token) {
                    continue;
                }
            }
            // Count a partition actually scanned (post token-range filter).
            meter.record_partition();
            // Issue #3095 (NB2): the partition's entries go through the SHARED static
            // choke point, so this route applies Cassandra's `processPartition()`
            // static semantics identically to the streaming route (statics injected
            // into every clustering row; a rowless-but-static partition yielding
            // exactly one row; no phantom `ck = null` row). It is a plain
            // materialize-each-entry loop for a table with no static column.
            let flow = crate::statics::drive_partition_rows(
                self,
                &key,
                rows,
                &mut pk_cache,
                assemble_cols.as_ref(),
                |row| {
                    // Count a row materialised/examined by the scan (BEFORE the
                    // predicate filter — the `rows_scanned` semantic).
                    meter.record_row();
                    // Predicate pushdown: evaluate the nested filter tree with SQL
                    // Kleene logic and keep the row only when it is definitely True
                    // (Unknown and False both reject — WHERE semantics, issue #834).
                    if let Some(filter) = &self.spec.filter {
                        if !filter.keeps(&row) {
                            return Ok(std::ops::ControlFlow::Continue(()));
                        }
                    }
                    // Dual row-cap / byte-cap boundary (issue #2825), test-then-push:
                    // cut on the row that WOULD cross the cap, before it joins the
                    // buffer. `batch_bytes.rs` documents the rule and its one-row floor.
                    let width = estimate_arrow_row_bytes(&self.columns, &row);
                    if byte_cap.cut_before(width).is_yes() {
                        self.flush_credited(sink, &mut buffer, &mut byte_cap, n_array_nodes)?;
                    }
                    buffer.push(row);
                    emitted += 1;
                    byte_cap.accumulate(width);
                    if buffer.len() >= self.batch_size {
                        self.flush_credited(sink, &mut buffer, &mut byte_cap, n_array_nodes)?;
                    }
                    // LIMIT reached (counted post-filter): stop the merge early.
                    if let Some(cap) = limit {
                        if emitted >= cap {
                            return Ok(std::ops::ControlFlow::Break(()));
                        }
                    }
                    Ok(std::ops::ControlFlow::Continue(()))
                },
            )?;
            if flow.is_break() {
                break 'partitions;
            }
        }

        if !buffer.is_empty() {
            self.flush_credited(sink, &mut buffer, &mut byte_cap, n_array_nodes)?;
        }
        Ok(())
    }

    /// Drive the aggregate-merge loop over `merger`, folding surviving rows into
    /// `state`.
    ///
    /// Cooperative cancellation (issue #1473): see [`Self::drive_merge`] — the
    /// cancel is polled BEFORE each `merger.step()`, so a cancel aborts before
    /// reconciling the next partition, not after.
    pub(crate) fn drive_aggregate(
        &self,
        plan: &AggPlan,
        merger: &mut dyn PartitionStepper,
        cancel: &CancelFlag,
        state: &mut crate::agg::AggState,
    ) -> Result<(), ProducerError> {
        // Issue #1817: one partition-key decode cache for the whole aggregate
        // merge; each partition's rows arrive consecutively, so its key decodes once.
        let mut pk_cache = PartitionKeyCache::default();
        // Issue #2324 (roborev 1633): projection-aware assembly set, computed once.
        let assemble_cols = self.assemble_columns();
        loop {
            if cancel.is_cancelled() {
                return Err(ProducerError::Cancelled);
            }
            // Map by VARIANT, not by racing the cancel flag (mirrors
            // `drive_merge`, issue #2264 roborev): a real I/O/corruption error
            // that happens to land while a client is disconnecting must never
            // be masked as a clean `Cancelled` abort — only a genuine
            // `Error::Cancelled` maps to `ProducerError::Cancelled`.
            let step = merger.step().map_err(|e| match e {
                cqlite_core::Error::Cancelled => ProducerError::Cancelled,
                other => ProducerError::Merge(other),
            })?;
            let MergeStep::Partition { key, rows } = step else {
                break;
            };
            if let Some(token) = &self.spec.token {
                if !token.contains(key.token) {
                    continue;
                }
            }
            // Issue #3095 (NB2): the SAME shared static choke point the row routes
            // use. Without it `SELECT count(*)` over a static-bearing table counted a
            // phantom `ck = null` row per static-bearing partition and MISSED a
            // static-only partition's row — i.e. it disagreed with `SELECT *`'s row
            // count over the same bytes.
            // The aggregate route never breaks early (every surviving row must reach
            // the accumulator), so the returned flow is always `Continue`.
            let _ = crate::statics::drive_partition_rows(
                self,
                &key,
                rows,
                &mut pk_cache,
                assemble_cols.as_ref(),
                |row| {
                    if let Some(filter) = &self.spec.filter {
                        if !filter.keeps(&row) {
                            return Ok(std::ops::ControlFlow::Continue(()));
                        }
                    }
                    plan.accumulate_row(state, &row)?;
                    Ok(std::ops::ControlFlow::Continue(()))
                },
            )?;
        }
        Ok(())
    }
}
