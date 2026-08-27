//! The BATCHED streaming scan (`scan_stream_batched`, issue #1592 Epic F/F2) — the
//! additive companion to the per-row `scan_stream` whose channel item is a `Vec`
//! BATCH of `(RowKey, ScanRow)` entries rather than a single entry.
//!
//! Split out of `sequential.rs` (campsite rule, epic #1116) alongside its per-row
//! sibling `per_row_scan_stream.rs`: that file is well over the ~800-line source
//! target, and the batched streaming scan is the same self-contained responsibility
//! — one spawned producer task per reader, feeding a bounded channel, with the
//! format branch (BTI trie walk / windowed stitch / block-by-block) chosen inside it.
//!
//! # Issue #3109: the BTI (`da`) dispatch
//!
//! This surface shipped WITHOUT the `bti_partitions_db.is_some()` dispatch that
//! `scan` and `run_scan_stream` have, so a `da` reader fell straight into the
//! non-stitching block loop and decoded through the `V5UncompressedOA` state
//! machine — which honours neither `read_shadowing` nor a caller-pinned read clock.
//! A BTI table streamed here was therefore read UNSHADOWED. Both streaming surfaces
//! now share ONE dispatch, [`SSTableReader::stream_bti_scan`], so a third divergent
//! copy of this decision cannot drift again (the #1577 class).
//!
//! Included via `#[path = "batched_scan_stream.rs"] mod batched_scan_stream;` in
//! [`super`], so it shares `sequential.rs`'s imports through `use super::*`.

use super::*;

impl SSTableReader {
    /// Batched streaming scan (issue #1592, Epic F/F2): the additive companion to
    /// [`scan_stream`](Self::scan_stream) whose channel item is a `Vec` BATCH of
    /// `(RowKey, ScanRow)` entries rather than a single entry. Forwarding one batch
    /// per channel send collapses the one-async-wake-per-row cost the internal
    /// windowed pipeline (issue #1143) was designed to amortize but the public
    /// forwarder then re-flattened away.
    ///
    /// Order and content are identical to [`scan_stream`](Self::scan_stream):
    /// flattening the batches yields exactly the per-row stream. Backpressure is
    /// preserved — the channel is bounded (in BATCHES) and every send observes it.
    pub fn scan_stream_batched(
        self: std::sync::Arc<Self>,
        table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        buffer_size: usize,
    ) -> BatchedScanStream {
        self.scan_stream_batched_admitted(
            table_id,
            start_key,
            end_key,
            schema,
            buffer_size,
            ScanAdmission::Acquire,
            None,
        )
    }

    /// [`scan_stream_batched`](Self::scan_stream_batched) with an explicit
    /// admission context (issue #1594), mirroring
    /// [`scan_stream_admitted`](Self::scan_stream_admitted).
    /// `now_secs` (issue #3058): a caller-pinned read-time TTL clock, threaded to
    /// the read-shadowing decoder so a caller that already captured ONE
    /// reconciliation instant for the request (the Flight single-source fast
    /// path) expires TTL cells at exactly that instant instead of an ambient
    /// wall-clock sample. `None` keeps the ambient sample.
    pub(crate) fn scan_stream_batched_admitted(
        self: std::sync::Arc<Self>,
        table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        buffer_size: usize,
        admission: ScanAdmission,
        now_secs: Option<i64>,
    ) -> BatchedScanStream {
        // The public channel carries BATCHES; sizing it to
        // `ceil(buffer_size / BATCH_EMIT_ROWS)` batches keeps the resident-row
        // budget of this channel comparable to the per-row surface's `buffer_size`
        // rather than `buffer_size * BATCH_EMIT_ROWS`.
        let cap = buffer_size.div_ceil(BATCH_EMIT_ROWS).max(1);
        let (tx, rx) = mpsc::channel(cap);
        // Read-metric grain (issue #1701): identical rule to the per-row surface —
        // an `Acquire` scan is the top-level read OPERATION and is measured with
        // this reader's format label; an `Exempt` sub-scan is not (its merge is).
        // Sampled before `self` moves into the task below.
        let measured_format = match admission {
            ScanAdmission::Acquire => Some(self.sstable_format_label()),
            ScanAdmission::Exempt => None,
        };
        let task = tokio::spawn(async move {
            if let Err(e) = self
                .run_scan_stream_batched(
                    table_id,
                    start_key,
                    end_key,
                    schema,
                    tx.clone(),
                    admission,
                    now_secs,
                )
                .await
            {
                let _ = tx.send(Err(e)).await;
            }
        });
        match measured_format {
            Some(format) => BatchedScanStream::new_measured(rx, task, Some(format)),
            None => BatchedScanStream::new(rx, task),
        }
    }

    async fn run_scan_stream_batched(
        self: std::sync::Arc<Self>,
        table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        tx: mpsc::Sender<Result<Vec<(RowKey, ScanRow)>>>,
        admission: ScanAdmission,
        // Issue #3058: caller-pinned read-time TTL clock (`None` = ambient).
        now_secs: Option<i64>,
    ) -> Result<()> {
        // Admission control (issue #1594, F4): identical discipline to the per-row
        // `run_scan_stream` — one permit per top-level scan operation, held via RAII.
        let _admission = match admission {
            ScanAdmission::Acquire => Some(scan_admission::admit().await),
            ScanAdmission::Exempt => None,
        };

        let cursor = self.open_batched_scan_cursor().await?;

        // Issue #3109: BTI (`da`) readers take the SAME shared dispatch the per-row
        // surface takes — the authoritative trie walk, with read shadowing and the
        // caller's pinned clock applied — instead of the block-by-block decoder
        // below, whose `V5UncompressedOA` state machine drops BOTH. Gated on the
        // SAME `bti_partitions_db.is_some()` condition `scan` and `run_scan_stream`
        // use, so the three surfaces cannot disagree about which readers are BTI.
        // See [`SSTableReader::stream_bti_scan`] for the full rationale.
        //
        // BEHAVIOR DELTA, stated so it is not a surprise: this branch does NOT apply
        // the `table_ids_match(&entry_table_id, &table_id)` filter the block loop
        // below applies — the trie walk returns no per-entry `TableId` to match on.
        // Safe and consistent with the sibling BTI surfaces (`scan`,
        // `sequential_scan`, `run_scan_stream`), which all skip it for the same
        // reason: every entry in this walk comes from the single SSTable being
        // scanned, so the filter can only ever reject rows that DO belong to it
        // (the parser tags entries from the SSTable header, which may carry a bare
        // or default keyspace/table name rather than the query's qualified form).
        //
        // Placed AFTER `open_batched_scan_cursor` deliberately: that call is this
        // task's single, format-branch-independent fault checkpoint (issue #3106),
        // and moving the BTI return above it would make the checkpoint silently
        // unreachable for `da` readers — the exact "armable but never armed" hazard
        // its doc comment warns about. The cursor is then unused on this branch
        // (`bti_scan_with_metadata` mints its own, issue #815); one open(2) is the
        // price of keeping the checkpoint branch-agnostic.
        if self.bti_partitions_db.is_some() {
            drop(cursor);
            return self
                .stream_bti_scan(
                    start_key.as_ref(),
                    end_key.as_ref(),
                    schema.as_ref(),
                    now_secs,
                    &WindowedOut::Batched(tx.clone()),
                )
                .await;
        }

        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }

        if self.requires_chunk_stitching() {
            // Forward the windowed driver's internal batches straight through — no
            // flatten, no re-batch (issue #1592). Same driver, same order/content
            // as the per-row path; only the output surface differs.
            self.run_scan_stream_windowed(
                table_id,
                start_key,
                end_key,
                schema,
                &cursor,
                now_secs,
                WindowedOut::Batched(tx.clone()),
            )
            .await
        } else {
            // Non-stitching formats read block-by-block; emit surviving entries
            // in BATCH_EMIT_ROWS-capped batches (issue #1592). A block with more
            // than BATCH_EMIT_ROWS surviving entries is split across multiple
            // batches so every emitted batch respects `batch.len() <=
            // BATCH_EMIT_ROWS` (the resident-row budget behind the channel cap);
            // the remainder carries over to the next block so we still wake the
            // consumer at most once per full batch.
            //
            // Confirmed rows from successfully-parsed prior blocks live in `batch`
            // until it fills. If a LATER block's read/parse errors mid-scan, flush
            // those confirmed rows to the consumer BEFORE surfacing the terminal
            // error — matching both the per-row `run_scan_stream` contract (each row
            // is sent the instant it is parsed) and the stitching path's
            // `flush_pending` (issue #1143 / #1592, roborev Finding 1). Propagating
            // the error via `?` here would silently drop up to BATCH_EMIT_ROWS-1
            // confirmed rows.
            let mut batch: Vec<(RowKey, ScanRow)> = Vec::with_capacity(BATCH_EMIT_ROWS);
            // Work-probe (issue #2398, extended by #3058): same "changed partition
            // key = one more partition body decoded" accounting `sequential_scan`
            // does, so the single-source `do_get` fast path's full-ring scan is
            // visible to the scan-work counter instead of silently unrecorded. The
            // BTI branch above keeps the same accounting inside
            // `bti_scan_with_metadata` (issue #3109), so routing `da` readers away
            // from this loop does not make their decode work invisible.
            let mut prev_partition_key: Option<RowKey> = None;
            loop {
                let block = match self.read_next_block(&cursor).await {
                    Ok(Some(block)) => block,
                    Ok(None) => break,
                    Err(e) => {
                        if !batch.is_empty() {
                            let _ = tx.send(Ok(std::mem::take(&mut batch))).await;
                        }
                        return Err(e);
                    }
                };
                let entries = match self.parse_batched_block(&block, schema.as_ref(), now_secs) {
                    Ok(entries) => entries,
                    Err(e) => {
                        if !batch.is_empty() {
                            let _ = tx.send(Ok(std::mem::take(&mut batch))).await;
                        }
                        return Err(e);
                    }
                };
                for (entry_table_id, entry_key, entry_value) in entries {
                    // Counted BEFORE every filter — including the table-id filter —
                    // exactly like the sibling sites (`sequential_scan`'s two loops
                    // and the stitched walk): the partition body was DECODED
                    // regardless of whether its rows survive, and a `Data.db` whose
                    // entries carry a non-matching `TableId` (path/header keyspace
                    // mismatch, or `scan_table_id`'s `"default"` fallback) must not
                    // report 0 bodies where `sequential_scan` reports N (roborev,
                    // issue #3058).
                    if prev_partition_key.as_ref() != Some(&entry_key) {
                        crate::storage::sstable::work_counters::add_stream_walk_partition_parsed();
                        prev_partition_key = Some(entry_key.clone());
                    }
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
                    batch.push((entry_key, entry_value));
                    if batch.len() >= BATCH_EMIT_ROWS {
                        if tx.send(Ok(std::mem::take(&mut batch))).await.is_err() {
                            return Ok(()); // consumer dropped
                        }
                        batch.reserve(BATCH_EMIT_ROWS);
                    }
                }
            }
            if !batch.is_empty() && tx.send(Ok(batch)).await.is_err() {
                return Ok(()); // consumer dropped
            }
            Ok(())
        }
    }
}
