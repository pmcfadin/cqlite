//! The ≠1-generation streaming read path: the lazy per-generation fan-out k-way
//! merge and the per-row → batch re-chunker (issues #1579/#1592; fail-closed in
//! #3124).
//!
//! # What lives here
//!
//! * [`spawn_fanout_merge`] — the token-ordered k-way merge over ONE per-row
//!   sub-scan per generation, used by `SSTableManager::scan_stream` whenever the
//!   authoritative cross-generation `KWayMerger` is not applicable (no schema, no
//!   `write-support`, or a merger-construction fallback). This is the query engine's
//!   multi-generation full-scan producer.
//! * [`rechunk_into_batches`] — the adapter that re-chunks that per-row stream into
//!   the public batched surface (`SSTableManager::scan_stream_batched`).
//!
//! # Issue #3124: two discarded `JoinHandle`s and a consumer that trusted a closed
//! channel
//!
//! Both boundaries collapsed a channel DISCONNECT into "the scan finished":
//!
//! * the merge task was `tokio::spawn`ed with its `JoinHandle` DROPPED, and
//!   `scan_stream` returned the bare `mpsc::Receiver`, so a merge task that UNWOUND
//!   dropped `out_tx` with no error and no terminator — every consumer (the query
//!   engine's streaming SELECT, the LIMIT-pushdown re-scan, `scan_stream_batched`)
//!   read that as a complete scan and returned FEWER ROWS WITH NO ERROR;
//! * the re-chunker read `per_row.recv() == None` as end-of-scan, so a dead merge
//!   task (or a dead per-generation sub-scan whose death the merge could not see
//!   either) produced a short batch stream that ENDED CLEANLY.
//!
//! Both now carry [`RowScanStream`]/`BatchedScanStream`, which JOIN their producer on
//! channel close and surface a `JoinError` as an `Error::Internal` — the same
//! mechanism issue #3106 established for the single-source batched scan, generalised
//! over the item type rather than re-invented (the machinery lives in
//! `storage/sstable/reader/data_access/joined_scan_stream.rs`).
//!
//! Extracted from `sstable/mod.rs` (campsite rule, epic #1116): that file is far over
//! the ~800-line source threshold, so the #3124 wiring lands here instead of growing
//! it.

// `spawn_fanout_merge` (and only it) is `not(tombstones)`; a `tombstones` build keeps
// just the re-chunker, so its imports are gated the same way.
#[cfg(not(feature = "tombstones"))]
use std::sync::Arc;

use super::reader::{self, RowScanStream};
#[cfg(not(feature = "tombstones"))]
use crate::storage::producer_fault::{FaultScope, ScanTaskSite};
use crate::types::ScanRow;
#[cfg(not(feature = "tombstones"))]
use crate::types::TableId;
use crate::RowKey;

/// Spawn the lazy per-generation token-ordered k-way merge and return its per-row
/// stream (issue #1579; fail-closed in #3124).
///
/// Emits `(token, key)`-ordered entries — byte-identical to `scan`'s
/// `sort_by_token_order` — with ties broken by reader index, holding at most one
/// primed head per generation resident. The returned [`RowScanStream`] owns the merge
/// task's `JoinHandle`, so a merge task that dies is reported to the caller as an
/// error instead of a clean end of stream (issue #3124, site 1).
#[cfg(not(feature = "tombstones"))]
pub(super) fn spawn_fanout_merge(
    readers: Vec<Arc<reader::SSTableReader>>,
    table_id: TableId,
    start_key: Option<RowKey>,
    end_key: Option<RowKey>,
    schema: Option<crate::schema::TableSchema>,
    buffer_size: usize,
) -> RowScanStream {
    let (out_tx, out_rx) = tokio::sync::mpsc::channel(buffer_size.max(1));

    // Issue #3124 (site 1): this task's ONE test-only fault checkpoint needs the
    // reader identity INSIDE the task, so it is captured as an owned scope here (a
    // zero-sized no-op in a production build). Any generation's path identifies the
    // table directory a test scopes to; an empty reader set yields an empty scope,
    // which matches nothing.
    let fault_scope = FaultScope::capture(|| {
        readers
            .first()
            .map(|reader| reader.file_path())
            .unwrap_or_default()
    });

    // The fan-out k-way merge IS the top-level read operation (issue #1701), and its
    // meter starts HERE — BEFORE the spawn (roborev round 7). Constructed after it, the
    // merge task could begin, or finish, before timing began. FORMAT-AGNOSTIC: the
    // reconciled rows come from possibly mixed BIG/BTI inputs, so no single format label
    // would be honest at this grain (the rule `catalog::READ_ROWS` documents).
    let meter = crate::observability::read_metrics::ReadOpMeter::start(None);

    // The `JoinHandle` is RETAINED (issue #3124, site 1): it is what lets the
    // returned stream tell "the merge finished" apart from "the merge DIED".
    let task = tokio::spawn(async move {
        fault_scope.checkpoint(ScanTaskSite::FanoutMerge);
        // Admission control (issue #1594, F4): this fan-out merge is ONE
        // top-level scan OPERATION that legitimately needs all N per-generation
        // sub-scans live AT ONCE (it primes a head from every sub-scan before
        // draining any). Acquire exactly ONE admission permit here, for the
        // whole operation, and open each sub-scan `Exempt` (below) so the
        // sub-scans do NOT each independently admit. If they did, a fan-out to
        // `N > cap` generations would deadlock: `cap` sub-scans would win
        // permits and park in backpressure while the rest blocked forever at
        // `admit`, and this priming loop — waiting on the blocked sub-scans —
        // would never drain the permit-holders. Held via this RAII guard for
        // the whole merge; released on every exit. See `scan_admission` docs.
        let _admission = reader::scan_stream_windowed::scan_admission::admit().await;

        // Open one streaming scan per reader. Each is `Exempt` — the single
        // permit above covers the whole fan-out operation (issue #1594). Each is a
        // `RowScanStream`, so a sub-scan task that DIES surfaces here as
        // `Some(Err(..))` and is forwarded below as a terminal item, instead of the
        // `None` that used to read as "this generation is exhausted" (issue #3124,
        // site 2).
        let mut streams: Vec<RowScanStream> = readers
            .into_iter()
            .map(|reader| {
                reader.scan_stream_admitted(
                    table_id.clone(),
                    start_key.clone(),
                    end_key.clone(),
                    schema.clone(),
                    buffer_size,
                    reader::scan_stream_windowed::scan_admission::ScanAdmission::Exempt,
                )
            })
            .collect();

        // Prime one head per stream. Each head carries its precomputed
        // Cassandra Murmur3 token so the merge orders by (token, key) — the
        // authoritative cross-SSTable order (issue #1580) — and never hashes a
        // key more than once. Comparing by raw `RowKey` bytes here (as this
        // path previously did) diverged from `scan`'s token order.
        let token_of =
            |key: &RowKey| crate::util::cassandra_murmur3::cassandra_murmur3_token(key.as_bytes());
        let mut heads: Vec<Option<(i64, RowKey, ScanRow)>> = Vec::with_capacity(streams.len());
        for stream in streams.iter_mut() {
            match stream.recv().await {
                Some(Ok((key, row))) => heads.push(Some((token_of(&key), key, row))),
                Some(Err(e)) => {
                    let _ = out_tx.send(Err(e)).await;
                    return;
                }
                None => heads.push(None),
            }
        }

        // K-way merge: repeatedly emit the head with the smallest
        // (token, key), ties broken by reader index to match the stable
        // token-order merge of `scan`.
        loop {
            let mut min_idx: Option<usize> = None;
            for (i, head) in heads.iter().enumerate() {
                if let Some((ref token, ref key, _)) = head {
                    match min_idx {
                        None => min_idx = Some(i),
                        Some(m) => {
                            if let Some((ref min_token, ref min_key, _)) = heads[m] {
                                if (token, key) < (min_token, min_key) {
                                    min_idx = Some(i);
                                }
                            }
                        }
                    }
                }
            }
            let idx = match min_idx {
                Some(idx) => idx,
                None => break, // all streams exhausted
            };

            // Take the winning entry and advance only that stream.
            let entry = match heads[idx].take() {
                Some((_, key, row)) => (key, row),
                None => break, // unreachable: min_idx points to a Some head
            };
            match streams[idx].recv().await {
                Some(Ok((key, row))) => heads[idx] = Some((token_of(&key), key, row)),
                Some(Err(e)) => {
                    let _ = out_tx.send(Err(e)).await;
                    return;
                }
                None => {} // stream exhausted; head stays None
            }

            if out_tx.send(Ok(entry)).await.is_err() {
                return; // consumer dropped
            }
        }
    });

    // Each per-generation sub-scan is opened `Exempt` and therefore unmeasured, so the
    // merged stream carries the operation's meter, started above.
    RowScanStream::new_measured_rows(out_rx, task, meter)
}

/// Re-chunk a per-row streaming scan into `BATCH_EMIT_ROWS`-sized `Vec` batches over
/// a bounded channel (issue #1592). Preserves order and content exactly (FIFO
/// push/flush) and preserves backpressure: the batch channel is bounded, so a stalled
/// consumer stops the drain of the per-row source, which stops the upstream scan. The
/// trailing partial batch is flushed at end. A mid-stream error is forwarded as a
/// terminal item.
///
/// Used for the zero/multi-generation and `tombstones` cases, where a
/// straight-through single-reader hand-off is not applicable (the per-row source is a
/// k-way merge / materialized reconciliation, not one reader).
///
/// Issue #3124 (site 3): the source is a [`RowScanStream`], not a bare
/// `mpsc::Receiver`, so `recv() == None` here means "the per-row producer PROVABLY
/// finished" rather than merely "its sender was dropped" — a dead producer arrives as
/// `Some(Err(..))` and is forwarded as this stream's terminal error. The re-chunker's
/// own task handle is likewise retained by the returned `BatchedScanStream`.
pub(super) fn rechunk_into_batches(
    mut per_row: RowScanStream,
    buffer_size: usize,
) -> reader::BatchedScanStream {
    use reader::scan_stream_windowed::BATCH_EMIT_ROWS;
    // Bound the batch channel so its resident-row budget stays comparable to
    // the per-row surface's `buffer_size`, not `buffer_size * BATCH_EMIT_ROWS`.
    let cap = buffer_size.div_ceil(BATCH_EMIT_ROWS).max(1);
    let (tx, rx) = tokio::sync::mpsc::channel(cap);
    let task = tokio::spawn(async move {
        let mut batch: Vec<(RowKey, ScanRow)> = Vec::with_capacity(BATCH_EMIT_ROWS);
        while let Some(item) = per_row.recv().await {
            match item {
                Ok(entry) => {
                    batch.push(entry);
                    if batch.len() >= BATCH_EMIT_ROWS {
                        if tx.send(Ok(std::mem::take(&mut batch))).await.is_err() {
                            return; // consumer dropped
                        }
                        batch.reserve(BATCH_EMIT_ROWS);
                    }
                }
                Err(e) => {
                    // Flush already-received Ok rows BEFORE surfacing the
                    // error, to match the per-row `scan_stream` guarantee
                    // that confirmed rows are delivered ahead of a terminal
                    // error (issue #1143 / #1592). Dropping them here would
                    // silently lose up to BATCH_EMIT_ROWS-1 rows.
                    if !batch.is_empty() {
                        let _ = tx.send(Ok(std::mem::take(&mut batch))).await;
                    }
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            }
        }
        if !batch.is_empty() {
            let _ = tx.send(Ok(batch)).await;
        }
    });
    reader::BatchedScanStream::new(rx, task)
}

// Issue #3124 END-TO-END pins for sites 1-3 (the fan-out merge task, a per-generation
// sub-scan, and this re-chunker's source): each kills the task under test with a real
// panic and asserts the public surface FAILS instead of returning short rows — after a
// control arm that pins the complete row count. `write-support` because the fixture is
// built with the write engine; `not(tombstones)` because that build routes
// `scan_stream` through the materializing `scan` instead of this fan-out.
#[cfg(all(test, feature = "write-support", not(feature = "tombstones")))]
#[path = "scan_stream_fanout_panic_tests.rs"]
mod panic_tests;
