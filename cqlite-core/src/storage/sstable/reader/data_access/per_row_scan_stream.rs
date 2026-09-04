//! The PER-ROW streaming scan (`scan_stream`, issue #790) and its issue-#3124
//! fail-closed producer boundary.
//!
//! Split out of `sequential.rs` (campsite rule, epic #1116): that file is more than
//! twice the ~800-line source target, and the per-row streaming scan is a
//! self-contained responsibility — one spawned producer task per reader, feeding a
//! bounded channel, with the format branch (BTI trie walk / windowed stitch /
//! block-by-block) chosen inside it.
//!
//! # Issue #3124, site 2: a dead sub-scan is not an exhausted generation
//!
//! This task is the PER-GENERATION producer that `SSTableManager::scan_stream`'s
//! fan-out k-way merge primes a head from. Its `JoinHandle` used to be DISCARDED and
//! the bare `mpsc::Receiver` returned, so a task that UNWOUND (a decode panic, an
//! abort) dropped its sender with no error and no terminal item; the merge read that
//! close as "this generation is exhausted" and the query returned FEWER ROWS WITH NO
//! ERROR — one whole generation silently missing. Returning a
//! [`RowScanStream`](super::joined_scan_stream::RowScanStream) makes the close
//! DISAMBIGUATED: the task is joined and a `JoinError` surfaces as `Some(Err(..))`.
//!
//! Included via `#[path = "per_row_scan_stream.rs"] mod per_row_scan_stream;` in
//! [`super`], so it shares `sequential.rs`'s imports through `use super::*`.

use super::*;

impl SSTableReader {
    /// Streaming scan (issue #790): yield `(RowKey, ScanRow)` entries lazily
    /// through a bounded channel instead of materializing the whole result in a
    /// `Vec`. Live heap is bounded by `buffer_size` rows (plus the stitched
    /// data-section buffer) rather than growing O(rows).
    ///
    /// Entries are yielded in on-disk order — token order for a single SSTable —
    /// matching the order of the materializing [`scan`](Self::scan) path. The
    /// bounded channel applies backpressure: parsing pauses when the consumer
    /// falls behind and stops entirely if the consumer is dropped.
    ///
    /// In-flight bound (chunk-stitching SSTables): the windowed pipeline (issue
    /// #1143) materializes one confirmed partition at a time and batches its rows to
    /// amortize the cross-thread wake, so against a stalled consumer the resident
    /// `(RowKey, ScanRow)` count is the SUM of three inherent terms, not one constant:
    /// `buffer_size` (this channel) `+ max_partition_size` (the one fully-materialized
    /// confirmed partition — a pre-existing #1156 windowed-scan term, inherent to any
    /// row-materializing partition scan) `+`
    /// [`MAX_INFLIGHT_BATCH_ROWS`](super::super::scan_stream_windowed::MAX_INFLIGHT_BATCH_ROWS)
    /// (the FIXED, BOUNDED amount the issue-#1143 batching subsystem may run ahead,
    /// regardless of `buffer_size`, holding even for `buffer_size == 1`).
    /// `MAX_INFLIGHT_BATCH_ROWS` bounds the batching subsystem alone, NOT the
    /// `max_partition_size` materialization term. Non-stitching SSTables parse a
    /// whole block before forwarding its rows, so the resident `(RowKey, ScanRow)`
    /// count is bounded by `buffer_size + (one parsed block's entries)`.
    pub fn scan_stream(
        self: std::sync::Arc<Self>,
        table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        buffer_size: usize,
    ) -> RowScanStream {
        // A directly-invoked reader scan is a top-level scan OPERATION: acquire one
        // admission permit (issue #1594). Callers that fan out to multiple readers
        // (`SSTableManager::scan_stream`) instead hold ONE permit for the whole
        // operation and open each sub-scan `Exempt` via `scan_stream_admitted`.
        self.scan_stream_admitted(
            table_id,
            start_key,
            end_key,
            schema,
            buffer_size,
            ScanAdmission::Acquire,
            crate::storage::sstable::reader::ScanErrorReporting::TopLevel,
        )
    }

    /// [`scan_stream`](Self::scan_stream) with an explicit admission context
    /// (issue #1594). Admission is per top-level scan OPERATION: a cross-generation
    /// fan-out merge passes [`ScanAdmission::Exempt`] for each sub-scan while
    /// holding ONE permit for the whole operation, so a single query's fan-out to
    /// `N > cap` generations can never hold-and-wait on itself (the deadlock a
    /// per-sub-scan permit introduced). A direct scan passes
    /// [`ScanAdmission::Acquire`].
    pub(crate) fn scan_stream_admitted(
        self: std::sync::Arc<Self>,
        table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        buffer_size: usize,
        admission: ScanAdmission,
        // Issue #1704: whether anything ENCLOSES this stream. Not derivable from
        // `admission` — see `ScanErrorReporting`.
        reporting: crate::storage::sstable::reader::ScanErrorReporting,
    ) -> RowScanStream {
        let (tx, rx) = mpsc::channel(buffer_size.max(1));
        // Read-metric grain (issue #1701): a DIRECT scan is a top-level read
        // OPERATION and is measured with this reader's format label. An `Exempt`
        // sub-scan is one generation of a fan-out merge — the merge's own stream is
        // the measured operation, so measuring here too would double-count its rows.
        // Sampled before `self` moves into the task below.
        // START the meter BEFORE the spawn (issue #1701, roborev round 7): constructed
        // after it, the producer task could begin — or finish — before timing began, so
        // `read.duration` measured less than the operation. Sampling the format label
        // here also keeps it off `self`, which moves into the task below.
        let meter = match admission {
            ScanAdmission::Acquire => Some(crate::observability::read_metrics::ReadOpMeter::start(
                Some(self.sstable_format_label()),
            )),
            ScanAdmission::Exempt => None,
        };
        // Read-PHASE accumulator (issue #1707): the meter owns it; the producer task
        // and everything it spawns need it EXPLICITLY, because thread-locals are not
        // inherited across a spawn and the phases run on threads that never see the
        // meter. `None` for an `Exempt` sub-scan (its merge is the measured
        // operation) and when metrics are not being collected.
        let phase_sink = meter.as_ref().and_then(|m| m.phase_sink());
        // Issue #3124 (site 2): the task's `JoinHandle` is RETAINED, not discarded.
        // This task is the per-generation producer a fan-out k-way merge primes a
        // head from; before this, a task that UNWOUND (a decode panic, an abort)
        // dropped `tx` with no error and no terminal item, the merge read that close
        // as "this generation is exhausted", and the query returned FEWER ROWS WITH
        // NO ERROR. `RowScanStream` joins the handle on channel close, so a dead
        // producer is an `Err`, never a clean end of stream.
        let task = tokio::spawn(async move {
            if let Err(e) = self
                .run_scan_stream(
                    table_id,
                    start_key,
                    end_key,
                    schema,
                    tx.clone(),
                    admission,
                    phase_sink,
                )
                .await
            {
                // Surface the error to the consumer as a terminal stream item.
                let _ = tx.send(Err(e)).await;
            }
        });
        match meter {
            Some(meter) => RowScanStream::new_measured_rows(rx, task, meter),
            None => RowScanStream::unmetered_as(rx, task, reporting),
        }
    }

    async fn run_scan_stream(
        self: std::sync::Arc<Self>,
        table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        tx: mpsc::Sender<Result<(RowKey, ScanRow)>>,
        admission: ScanAdmission,
        // This scan operation's read-phase accumulator (issue #1707), or `None`.
        phase_sink: Option<std::sync::Arc<crate::observability::ReadPhaseTimings>>,
    ) -> Result<()> {
        // Admission control (issue #1594, F4): a top-level scan operation acquires
        // ONE blocking-pool permit here, at the top, BEFORE opening the cursor or
        // spawning any `spawn_blocking` work, and holds it via this RAII guard for
        // the whole scan (released on every exit — success, error, cancellation).
        // A fan-out merge's sub-scan is `Exempt` (the merge holds the operation's
        // single permit), so it acquires none — a fan-out to `N > cap` generations
        // can never hold-and-wait on itself. No other permit/lock is held while
        // awaiting admission, so this single-permit acquisition cannot deadlock.
        let _admission = match admission {
            ScanAdmission::Acquire => Some(scan_admission::admit().await),
            ScanAdmission::Exempt => None,
        };
        let _scan = self.begin_scan(); // #3853 scan-lifetime madvise seam

        // Issue #3124 (site 2): the ONE test-only fault checkpoint for this task,
        // placed ABOVE every format branch (BTI trie walk, windowed stitch,
        // block-by-block) so killing it reproduces the "sender dropped with no error
        // and no terminator" condition for ANY reader — a checkpoint inside one
        // branch would silently not fire for the other formats. The join that
        // catches it wraps the whole task, so the property proven is
        // branch-agnostic. Compiles to nothing in a production build.
        crate::storage::producer_fault::scan_task_checkpoint(
            crate::storage::producer_fault::ScanTaskSite::PerRowScan,
            || self.file_path(),
        );

        // Issue #1577 (owner-chosen fix, 2026-07-06): BTI (`da`) readers MUST use
        // the SAME per-reader decode path `SSTableReader::scan` uses — the trie-walk
        // `bti_scan_with_metadata` — NOT the block-by-block `read_next_block` +
        // `parse_block_entries_with_schema` decoder below. Issue #3109 moved that
        // dispatch into the SHARED `stream_bti_scan`, which the batched surface
        // (`run_scan_stream_batched`) now calls too: it was added without this branch
        // and silently reproduced the identical #1577 defect. See `stream_bti_scan`
        // for the full rationale (prefix-authority with `scan`, why the
        // block-by-block route is wrong for `da`, and the emission contract). The
        // per-row surface has no request-scoped clock to pin, so it passes
        // `now_secs = None` (the ambient sample), unchanged from #1577.
        if self.bti_partitions_db.is_some() {
            return self
                .stream_bti_scan(
                    start_key.as_ref(),
                    end_key.as_ref(),
                    schema.as_ref(),
                    None,
                    &WindowedOut::PerRow(tx.clone()),
                )
                .await;
        }

        // Issue #815: independent per-scan cursor — no cross-scan serialization.
        // Issue #1577 (rust-reviewer nit): created only for the non-BTI path — the
        // BTI branch above mints its own cursor inside `bti_scan_with_metadata` and
        // returned already, so opening+seeking one here for BTI was a wasted
        // open(2)+seek. Mirrors how `scan`/`get_all_entries`/`scan_with_cell_metadata`
        // gate cursor creation after their BTI early-return.
        let cursor = self.new_scan_cursor().await?;

        // Position at the start of the data section (mirrors sequential_scan).
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }

        if self.requires_chunk_stitching() {
            // Issue #1143: SLIDING-WINDOW stitch+parse on a bounded blocking->async
            // pipeline instead of stitching the ENTIRE Data.db into one growing
            // `Vec<u8>` before parsing (which thrashed the allocator and blew up
            // read p99 under concurrent write load). Keeps only a `window` bounded
            // by `max_partition_size + one_chunk`. Same bounded driver as the
            // compaction read path (issue #827); scan output (key-range + tombstone
            // + `table_ids_match` filters, dropped timestamp) is parity-identical to
            // `parse_stitched_stream`. Full rationale + the in-flight batching bound
            // live in the `scan_stream_windowed` module docs.
            self.run_scan_stream_windowed(
                table_id,
                start_key,
                end_key,
                schema,
                &cursor,
                None,
                WindowedOut::PerRow(tx.clone()),
                phase_sink,
            )
            .await
        } else {
            // Non-stitching formats already read block-by-block; emit per block so
            // only one block's entries are live at a time.
            while let Some(block) = self.read_next_block(&cursor).await? {
                // #1695: observe consumer closure once per BLOCK, not only on a send.
                // The four `continue` paths below (table-id mismatch, below `start`,
                // above `end`, tombstone-filtered) reach no send, so a scan whose
                // filters reject everything used to read and PARSE every block of the
                // table after the caller already had its `QueryTimeout`. Found by
                // sweeping this class rather than waiting for it to be reported: it is
                // the same defect as `generation_merge`'s producer and the query-layer
                // producer, in a third place.
                //
                // A plain check suffices here, where the query layer needed a `select!`
                // on `tx.closed()`: this loop's await is a bounded disk read, not an
                // open-ended wait on another producer, so it always returns to the
                // check. One block is the loop's own unit of work.
                if tx.is_closed() {
                    return Ok(());
                }
                let entries =
                    self.parse_block_entries_with_schema(&block, schema.as_ref(), true)?;
                for (entry_table_id, entry_key, entry_value) in entries {
                    if !table_ids_match(&entry_table_id, &table_id) {
                        continue;
                    }
                    if let Some(ref start) = start_key {
                        if &entry_key < start {
                            continue;
                        }
                    }
                    if let Some(ref end) = end_key {
                        if &entry_key > end {
                            continue;
                        }
                    }
                    if !self.filter_tombstone(&entry_value) {
                        continue;
                    }
                    if tx.send(Ok((entry_key, entry_value))).await.is_err() {
                        return Ok(()); // consumer dropped
                    }
                }
            }
            Ok(())
        }
    }
}
