//! Sliding-window stitch+parse driver for the user-facing streaming scan
//! (issue #1143).
//!
//! Extracted verbatim from `data_access.rs` (epic #1116 file-size split). These
//! are private `impl SSTableReader` methods called via `self.` from
//! `data_access.rs`'s `scan_stream`; behavior is unchanged.

use super::data_access::table_ids_match;
use super::source::ScanCursor;
use super::SSTableReader;
use crate::types::{TableId, Value};
use crate::{Error, Result, RowKey};
use tokio::sync::mpsc;

impl SSTableReader {
    /// Sliding-window stitch+parse driver for the user-facing streaming scan
    /// (issue #1143). The async counterpart of the compaction read path's
    /// [`stream_all_partitions_for_compaction`](Self::stream_all_partitions_for_compaction):
    /// it keeps a `window: Vec<u8>` of decompressed bytes, appends one
    /// decompressed chunk at a time, drains every confirmed partition out of the
    /// front via [`drain_scan_window`](Self::drain_scan_window), and stops at
    /// `NeedMore` to await the next chunk (a partition/row/cell can straddle a
    /// 64 KiB chunk boundary). Live heap is bounded by
    /// `max_partition_size + one_chunk`, not O(file).
    ///
    /// Backpressure: each emitted `(RowKey, Value)` is forwarded via the bounded
    /// `tx` (async `send().await`), so the consumer's lag pauses parsing exactly
    /// as the previous whole-buffer path's `blocking_send` did. Because the parse
    /// happens between `await` points on this async task (not in `spawn_blocking`)
    /// the work is naturally yielding at chunk granularity.
    ///
    /// Cooperative scheduling (issue #1143, finding #2): the old
    /// `parse_stitched_stream` ran the whole-file parse under
    /// `tokio::task::spawn_blocking`; this driver instead parses inline on the
    /// async worker. We yield between partitions and between chunks for free
    /// (the `tx.send().await` / `read_next_block().await` points), but a single
    /// very wide partition can parse with neither point reached, so
    /// [`drain_scan_window`](Self::drain_scan_window) adds an explicit
    /// `tokio::task::yield_now().await` after each partition to keep one wide
    /// partition from monopolizing a worker thread.
    ///
    /// Precondition: `cursor`'s file is seeked to the start of the data section.
    pub(super) async fn run_scan_stream_windowed(
        &self,
        table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        cursor: &ScanCursor,
        tx: &mpsc::Sender<Result<(RowKey, Value)>>,
    ) -> Result<()> {
        use crate::storage::sstable::compression::Compression;

        // Resolve the schema the parser needs (cells lack column names on disk),
        // matching the previous `parse_stitched_stream` resolution exactly.
        let owned_schema = schema.or_else(|| self.get_table_schema(None));
        let parser = self.build_v5_parser();

        // Incompressible-chunk fallback (Bug #639, epic #970, issue #1104):
        // Cassandra stores a chunk RAW when its compressed length would meet or
        // exceed `max_compressed_length`. Honour the same rule as
        // `stitch_all_chunks` so the windowed path decodes identically.
        let max_compressed_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.max_compressed_length as usize)
            .unwrap_or(usize::MAX);

        let mut window: Vec<u8> = Vec::new();
        let mut broke = false;
        let mut chunk_count = 0usize;

        while let Some(compressed_chunk) = self.read_next_block(cursor).await? {
            let decompressed_chunk = if compressed_chunk.len() >= max_compressed_length {
                compressed_chunk
            } else if let Some(compression_reader) = &self.compression_reader {
                let compression = Compression::new(*compression_reader.algorithm())?;
                compression.decompress(&compressed_chunk).map_err(|e| {
                    Error::corruption(format!(
                        "run_scan_stream_windowed: Failed to decompress chunk {}: {}",
                        chunk_count, e
                    ))
                })?
            } else {
                compressed_chunk
            };
            window.extend_from_slice(&decompressed_chunk);
            chunk_count += 1;

            // Not the final chunk yet: drain confirmed partitions; NeedMore means
            // "await more bytes" (a partition straddles this chunk boundary).
            self.drain_scan_window(
                &parser,
                owned_schema.as_ref(),
                &table_id,
                start_key.as_ref(),
                end_key.as_ref(),
                &mut window,
                false,
                tx,
                &mut broke,
            )
            .await?;
            if broke {
                return Ok(());
            }
        }

        // EOF: final drain — a trailing partition with no END_OF_PARTITION marker
        // is now terminal (Done), not a refill request that will never come.
        if !broke {
            self.drain_scan_window(
                &parser,
                owned_schema.as_ref(),
                &table_id,
                start_key.as_ref(),
                end_key.as_ref(),
                &mut window,
                true,
                tx,
                &mut broke,
            )
            .await?;
        }

        log::debug!(
            "run_scan_stream_windowed: drained {} chunks (final window {} bytes)",
            chunk_count,
            window.len()
        );
        Ok(())
    }

    /// Drain every confirmed partition from the front of the sliding `window`,
    /// emitting each surviving `(RowKey, Value)` through `tx` (issue #1143).
    ///
    /// Mirrors [`drain_compaction_window`](Self::drain_compaction_window) but for
    /// the user-facing scan: it drives [`parse_one_partition_with_timestamps`],
    /// drops the per-row timestamp, and applies the same key-range + tombstone
    /// filters the previous whole-buffer `parse_stitched_stream` applied. It
    /// ADDITIONALLY applies a [`table_ids_match`] filter (consistent with the
    /// non-stitching `scan_stream` branch in `data_access.rs`); that filter is a
    /// no-op for a single-table SSTable, so for the single-table corpus the
    /// emitted set is unchanged from the old path. After each `Emitted(consumed)`
    /// the consumed prefix is removed, keeping the window's peak bounded by
    /// `max_partition_size + one_chunk`. Stops at `NeedMore` / `Done` (await the
    /// next chunk / genuine end) or when the consumer is dropped (`*broke`).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn drain_scan_window(
        &self,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
        schema: Option<&crate::schema::TableSchema>,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        window: &mut Vec<u8>,
        at_final_chunk: bool,
        tx: &mpsc::Sender<Result<(RowKey, Value)>>,
        broke: &mut bool,
    ) -> Result<()> {
        use crate::storage::sstable::reader::parsing::ParseStep;

        loop {
            if *broke || window.is_empty() {
                return Ok(());
            }

            // Buffer this partition's surviving entries, then forward them async
            // AFTER the parser returns. `parse_one_partition_with_timestamps`
            // takes a synchronous `FnMut` emit, so we cannot `.await` inside it;
            // a partition's rows are bounded by `max_partition_size`, so this
            // stays within the documented window bound.
            let mut surviving: Vec<(RowKey, Value)> = Vec::new();
            let step = parser.parse_one_partition_with_timestamps(
                window.as_slice(),
                schema,
                self,
                at_final_chunk,
                &mut |(entry_table_id, key, value, _ts)| {
                    // Key-range + tombstone filters match the previous
                    // `parse_stitched_stream`; the `table_ids_match` guard is the
                    // ADDITIONAL filter the non-stitching `scan_stream` branch
                    // also applies (a no-op for single-table SSTables).
                    if !table_ids_match(&entry_table_id, table_id) {
                        return Ok(std::ops::ControlFlow::Continue(()));
                    }
                    if let Some(start) = start_key {
                        if &key < start {
                            return Ok(std::ops::ControlFlow::Continue(()));
                        }
                    }
                    if let Some(end) = end_key {
                        if &key > end {
                            return Ok(std::ops::ControlFlow::Continue(()));
                        }
                    }
                    // Suppress row tombstones from user-facing scan output (#505).
                    if !self.filter_tombstone(&value) {
                        return Ok(std::ops::ControlFlow::Continue(()));
                    }
                    surviving.push((key, value));
                    Ok(std::ops::ControlFlow::Continue(()))
                },
            )?;

            match step {
                ParseStep::Emitted(consumed) => {
                    let take = if consumed == 0 { 1 } else { consumed };
                    window.drain(0..take.min(window.len()));
                    // Forward this partition's surviving entries with backpressure.
                    for entry in surviving {
                        if tx.send(Ok(entry)).await.is_err() {
                            *broke = true; // consumer dropped
                            return Ok(());
                        }
                    }
                    // Cooperative yield (issue #1143, finding #2): a partition with
                    // no surviving entries (all filtered/tombstoned) reaches no
                    // `send().await`, and a very wide partition parses with no
                    // intervening `await`, so yield explicitly after each partition
                    // to avoid monopolizing a worker thread on a multi-thread
                    // runtime. Cheap (no reschedule when the queue is empty).
                    tokio::task::yield_now().await;
                }
                // NeedMore: the partition straddles this chunk boundary. The
                // per-partition parser buffers a partition's rows internally and
                // only invokes our emit closure on a CONFIRMED `Emitted` return,
                // so on `NeedMore` our `surviving` buffer is empty — nothing was
                // forwarded and nothing is dropped. The caller appends the next
                // chunk and we re-parse this partition from its start, so no row
                // is duplicated or lost across the boundary. Record the straddle
                // (issue #1143) so the boundary re-parse path is observable; it is
                // suppressed at the final chunk (parser collapses NeedMore→Done).
                ParseStep::NeedMore => {
                    crate::observability::add_counter(
                        crate::observability::catalog::READ_SCAN_WINDOW_REFILL,
                        1,
                        &[],
                    );
                    return Ok(());
                }
                // Done: genuine end of partitions / terminal truncation.
                ParseStep::Done => return Ok(()),
            }
        }
    }
}
