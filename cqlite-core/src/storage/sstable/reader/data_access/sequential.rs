//! Sequential / index-driven read paths: range scans, full scans, the
//! `scan_for_key` fallback, and the cell-metadata scan. The two bounded STREAMING
//! scans live in the sibling files included at the bottom of this one
//! (`per_row_scan_stream.rs`, `batched_scan_stream.rs`).
//!
//! These cover the BIG (`nb`) `V5CompressedLegacy` formats (chunk-stitched) and
//! the non-stitching block-by-block formats. BTI (`da`) range/full scans are
//! routed here only to delegate to [`SSTableReader::bti_scan_with_metadata`] —
//! `scan`, `sequential_scan` and `scan_with_cell_metadata` each gate on
//! `bti_partitions_db.is_some()` before touching the block loop (issue #3109).
//!
//! File-size note (campsite rule, epic #1116): this file was already over the
//! ~800-line source threshold before issue #2346's `scan_cancel` per-call
//! parameter change (`sequential_scan`), which nudged it further. Issue #3109
//! moved the batched streaming scan out to its own file, shrinking it; it is still
//! over the target and further splits are tracked under #1116.

use super::super::scan_stream_windowed::scan_admission::{self, ScanAdmission};
use super::super::scan_stream_windowed::{WindowedOut, BATCH_EMIT_ROWS};
use super::super::SSTableReader;
use super::joined_scan_stream::{BatchedScanStream, RowScanStream};
use super::model::{
    sort_by_token_order, sort_by_token_order_with_meta, table_ids_match, SCAN_FOR_KEY_CALLS,
};
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::parsing::BufferExtent;
use crate::types::{CellWriteMetadata, ScanRow, TableId};
use crate::{Error, Result, RowKey};
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;
use tokio::sync::mpsc;

/// Classify a `parse_block` failure on the compressed (`V5CompressedLegacy`)
/// point-lookup path (`scan_for_key`) as a soft-miss vs. a fatal error.
///
/// Issue #1411: the scan path ([`SSTableReader::stitch_and_parse_all_chunks`])
/// propagates every `parse_block` error via `?`. The point-lookup path mirrors
/// that so `get()` and `scan()` agree on which failures are fatal — with ONE
/// deliberate exception, the case the original blanket `Err(_) => Ok(None)`
/// existed to protect: this reader has no schema for the table, so it cannot
/// serve the key and the caller must fall through to the next SSTable reader.
///
/// Returns `true` (soft-miss → `Ok(None)`) ONLY when both hold:
/// - `schema_present == false` — the reader supplied no schema, and
/// - the parser reported [`Error::Schema`] — which `V5CompressedLegacy`'s
///   `parse_block` raises ("requires schema for <ks>.<table>") *before* it
///   inspects any bytes when no schema is available.
///
/// Every other failure is non-recoverable and must propagate: real data
/// corruption / malformed blocks (`Corruption`, `InvalidFormat`, `Parse`, I/O,
/// etc.), and — critically — a deep schema/type-resolution `Error::Schema`
/// raised when a schema IS present (a UDT/type that cannot be resolved is a real
/// error, not a missing key). Requiring `!schema_present` prevents that class
/// from ever being masked as "not found".
fn is_parse_soft_miss(schema_present: bool, err: &Error) -> bool {
    !schema_present && matches!(err, Error::Schema(_))
}

impl SSTableReader {
    /// Scan a range of keys
    ///
    /// # Arguments
    /// * `table_id` - The table to scan
    /// * `start_key` - Optional start key for range scan
    /// * `end_key` - Optional end key for range scan
    /// * `limit` - Optional limit on number of results
    /// * `schema` - Optional table schema for schema-aware parsing. When provided,
    ///   enables accurate type detection and avoids heuristic-based parsing.
    ///   Strongly recommended for Cassandra 5.0+ formats.
    ///
    /// # Error counting (issue #1704)
    ///
    /// A failure is counted ONCE into
    /// `cqlite.errors.total{category, subsystem="reader"}` at this boundary. This
    /// is the top-level exit seam of the MATERIALIZING scan (the streaming
    /// surfaces are counted at [`JoinedStream::recv`], and `scan_delta` at its own
    /// terminal send), so the inner steps it delegates to — `sequential_scan`,
    /// `bti_scan_with_metadata`, the index walk — deliberately do NOT count: they
    /// are also reached from `iterate_all_partitions` and the full-index stream,
    /// and counting there too would report one failed scan two or three times.
    /// The category comes from the classifier via
    /// [`crate::observability::record_result`], never from this call site, and the
    /// `Err` is returned unchanged.
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(RowKey, ScanRow)>> {
        crate::observability::record_result(
            "reader",
            self.scan_inner(table_id, start_key, end_key, limit, schema)
                .await,
        )
    }

    /// Implementation of [`scan`](Self::scan); see there for the contract. Kept
    /// separate so the error-counting seam is a single, unmissable wrapper rather
    /// than a `record_error` at each of this function's several early returns.
    async fn scan_inner(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(RowKey, ScanRow)>> {
        let _scan = self.begin_scan(); // #3853 scan-lifetime madvise seam
        tracing::debug!("SSTableReader::scan - Starting scan");
        tracing::debug!("SSTableReader::scan - File path: {:?}", self.file_path());
        tracing::debug!("SSTableReader::scan - Table ID: {}", table_id);
        tracing::debug!("SSTableReader::scan - Start key: {:?}", start_key);
        tracing::debug!("SSTableReader::scan - End key: {:?}", end_key);
        tracing::debug!("SSTableReader::scan - Limit: {:?}", limit);
        tracing::debug!("SSTableReader::scan - Has schema: {}", schema.is_some());
        tracing::debug!("SSTableReader::scan - Has index: {}", self.index.is_some());
        tracing::debug!(
            "SSTableReader::scan - Has bloom filter: {}",
            self.bloom_filter.is_some()
        );

        // Issue #660: BTI ("da") readers have no Index.db/Summary.db. A full scan
        // walks the whole (chunk-compressed) Data.db once and parses every
        // partition — the same partition decode the point-lookup path proves
        // correct, but emitting ALL partitions instead of stopping at the first.
        if self.bti_partitions_db.is_some() {
            let entries = self
                .bti_scan_with_metadata(start_key, end_key, limit, schema, true, None)
                .await?;
            return Ok(entries.into_iter().map(|(k, v, _meta)| (k, v)).collect());
        }

        let mut results = Vec::new();

        // Use index for efficient range scan if available
        if let Some(index) = &self.index {
            tracing::debug!("SSTableReader::scan - Using index-based scan");
            let entries = index.get_range(table_id, start_key, end_key)?;
            tracing::debug!(
                "SSTableReader::scan - Index returned {} entries",
                entries.len()
            );

            // Issue #256 FIX: Fall back to sequential scan when index returns no entries
            //
            // This handles BTI (Big Trie Index) format where parsing may be incomplete or
            // where the index format is not yet fully supported. Without this check, tables
            // using BTI format return 0 rows because:
            // 1. The index exists (so we take the index-based path)
            // 2. But get_range() returns 0 entries (BTI parsing incomplete)
            // 3. The has_zero_size check never triggers (no entries to check)
            // 4. The for loop iterates 0 times, returning empty results
            //
            // Sequential scan correctly parses Data.db directly, bypassing index issues.
            if entries.is_empty() {
                tracing::debug!(
                    "SSTableReader::scan - Index returned 0 entries (BTI format or incomplete parsing), falling back to sequential scan"
                );
                return self
                    .sequential_scan(
                        table_id,
                        start_key,
                        end_key,
                        limit,
                        schema,
                        &self.scan_cancel,
                    )
                    .await;
            }

            // Check if any entry has size=0 (Cassandra 5.0 format)
            let has_zero_size = entries.iter().any(|e| e.size == 0);
            if has_zero_size {
                tracing::debug!("SSTableReader::scan - Index reports size=0 for some entries, using sequential scan fallback");
                return self
                    .sequential_scan(
                        table_id,
                        start_key,
                        end_key,
                        limit,
                        schema,
                        &self.scan_cancel,
                    )
                    .await;
            }

            // Collect ALL index entries (limit applied after sort — BLOCKING-1).
            for (i, entry) in entries.iter().enumerate() {
                // Index offsets are relative to data section start - adjust for header
                let file_offset = entry.offset + self.actual_header_size as u64;
                tracing::debug!(
                    "SSTableReader::scan - Processing index entry {}: index_offset={}, file_offset={}, size={}",
                    i, entry.offset, file_offset, entry.size
                );

                // POINT intent, deliberately (issue #2876, roborev job 4634).
                //
                // This legacy index path looks like a scan but does NOT read
                // sequentially: `Index::get_range` walks `sorted_keys`, i.e. raw
                // key-BYTE order (index.rs:270-277), while Data.db is laid out in
                // Murmur3 TOKEN order. For the default partitioner those two orders
                // are uncorrelated, so these reads are genuinely scattered and
                // `MADV_RANDOM` is the CORRECT advice — the same reasoning that put
                // point lookups on the advised plane in #2210. Routing them to the
                // scan plane would invite readahead that mostly fetches pages this
                // walk never touches.
                //
                // Do NOT "fix" this by sorting `entries` by `entry.offset` before
                // reading to make the access sequential: `results` is sorted into
                // token order AFTER the loop (`sort_by_token_order`, see below) and
                // `limit` is applied AFTER that sort, so LIMIT N must mean "the N
                // token-smallest partitions". Read order is independent of result
                // order here, so an offset-ordered read is safe in principle — but
                // it is a separate optimization with its own regression-test burden,
                // NOT part of removing a wrong advice from the true scan walks.
                // Tracked as follow-up; this line stays on the point plane.
                if let Some(value) = self.read_value_at_offset(file_offset, entry.size).await? {
                    tracing::debug!(
                        "SSTableReader::scan - Successfully read value at offset {}",
                        entry.offset
                    );
                    results.push((entry.key.clone(), value));
                } else {
                    tracing::debug!("SSTableReader::scan - Value at offset {} was filtered out (tombstone or expired)", entry.offset);
                }
            }
        } else {
            // Fallback to sequential scan.  sequential_scan() already returns results in
            // token order (NON-BLOCKING-1: avoid double-sort — return directly).
            tracing::debug!("SSTableReader::scan - No index, falling back to sequential scan");
            let seq_results = self
                .sequential_scan(
                    table_id,
                    start_key,
                    end_key,
                    limit,
                    schema,
                    &self.scan_cancel,
                )
                .await?;
            tracing::debug!(
                "SSTableReader::scan - Sequential scan returned {} results",
                seq_results.len()
            );
            tracing::debug!(
                "SSTableReader::scan - Returning {} final results",
                seq_results.len()
            );
            return Ok(seq_results);
        }

        // Index-based path: sort by Murmur3 token order (ascending token, then key bytes).
        // This matches the on-disk physical order (spec §5, Appendix B §313) and the write
        // engine's DecoratedKey::cmp.  Compute each key's token once before sorting to
        // avoid O(n log n) recomputation inside the comparator.
        sort_by_token_order(&mut results);
        // Limit applied AFTER sort so LIMIT N returns the N token-smallest partitions.
        if let Some(lim) = limit {
            results.truncate(lim);
        }

        tracing::debug!(
            "SSTableReader::scan - Returning {} final results",
            results.len()
        );
        Ok(results)
    }

    /// Get all entries in the SSTable.
    ///
    /// # Tombstone contract (Issue #505)
    ///
    /// This is a **user-facing** accessor: row tombstones are filtered out via
    /// [`Self::filter_tombstone`] and never appear in the returned entries. The
    /// underlying `parse_block` path emits `Value::Tombstone(RowTombstone)` for
    /// deleted rows, but those are suppressed here so callers see exactly the live
    /// rows (matching the previous `Value::Null` suppression behaviour).
    ///
    /// The compaction k-way merger must instead use
    /// [`Self::iterate_all_partitions_for_compaction`], which preserves
    /// `Value::Tombstone` entries (with their authoritative deletion timestamps)
    /// so that tombstone-shadowing semantics can be applied during the merge.
    pub async fn get_all_entries(&self) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        let _scan = self.begin_scan(); // #3853 scan-lifetime madvise seam

        // Issue #660: BTI ("da") tables have no Index.db; route through the
        // whole-Data.db BTI scan, which resolves schema via get_table_schema
        // (header/registry) and decodes every partition. It mints its own
        // per-scan cursor, as does the non-BTI path below (issue #815).
        if self.bti_partitions_db.is_some() {
            let table_id = TableId::new(format!(
                "{}.{}",
                self.header.keyspace, self.header.table_name
            ));
            let entries = self
                .bti_scan_with_metadata(None, None, None, None, false, None)
                .await?;
            return Ok(entries
                .into_iter()
                .map(|(k, v, _meta)| (table_id.clone(), k, v))
                .collect());
        }

        // Issue #815: independent per-scan cursor — no cross-scan serialization.
        let cursor = self.new_scan_cursor().await?;

        let mut results = Vec::new();

        // Reset to beginning of data section
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }

        if self.requires_chunk_stitching() {
            // V5CompressedLegacy: Row payloads can span multiple compressed chunks
            // We must decompress and stitch all chunks together before parsing
            tracing::debug!(
                "V5CompressedLegacy format detected, decompressing and stitching all chunks before parsing"
            );

            // Use shared stitching helper method. Physical enumeration
            // (`get_all_entries`) honours the reader's own cancel field —
            // unchanged pre-#2346 behaviour.
            let entries = self
                .stitch_and_parse_all_chunks(&cursor, None, false, &self.scan_cancel)
                .await?;
            results.extend(entries);
        } else {
            // Other formats: Read and parse blocks individually
            while let Some(block) = self.read_next_block(&cursor).await? {
                let entries = self.parse_block_entries(&block, None, false)?;
                results.extend(entries);
            }
        }

        // Issue #505: suppress row tombstones from user-facing output. The compaction
        // path (iterate_all_partitions_for_compaction) bypasses this filter.
        results.retain(|(_tid, _key, value)| self.filter_tombstone(value));

        Ok(results)
    }

    pub(super) async fn scan_for_key(
        &self,
        table_id: &TableId,
        key: &RowKey,
    ) -> Result<Option<ScanRow>> {
        // Issue #831: record the call so tests can assert the BTI point-lookup
        // path never reaches the sequential scan.
        SCAN_FOR_KEY_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Issue #815: independent per-scan cursor — no cross-scan serialization.
        let cursor = self.new_scan_cursor().await?;

        let header_size = self.calculate_header_size();

        // For V5CompressedLegacy NB format, partitions can span chunk boundaries.
        // The block-by-block parser will miss any partition whose bytes cross a
        // chunk boundary.  Use the same stitched-buffer path that sequential_scan()
        // uses so that get() and scan() share a consistent view of the data.
        // (Issue #517)
        if self.requires_chunk_stitching() {
            tracing::debug!(
                "scan_for_key: V5CompressedLegacy NB detected, using stitched buffer for key lookup"
            );
            // `stitch_all_chunks` reads from the CURRENT cursor position forward,
            // so its precondition is "seeked to the data-section start" (the fresh
            // cursor's chunk index already starts at 0). Each call uses its own
            // cursor (issue #815), so there is no cross-call position to reset.
            {
                let mut file_guard = cursor.file.lock().await;
                file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
            }

            // Issue #1411: separate the chunk-integrity stitch from the schema-aware
            // parse so their failure classes are not conflated. `stitch_all_chunks`
            // drives the authoritative per-chunk CRC32 check
            // (`block_io::read_nb_format_chunk_data`) + decompression; a CRC mismatch,
            // decompression failure, or corrupt offset is corruption and MUST surface
            // (propagate via `?`, matching `scan`), never be masked as a missing key.
            // Only the `parse_block` step below may soft-miss (schema unavailable /
            // wrong table type) so the caller can try the next reader. Previously
            // `stitch_and_parse_all_chunks` combined both under a blanket
            // `Err(_) => Ok(None)` that swallowed CRC corruption on point lookups.
            let stitched_buffer = self.stitch_all_chunks(&cursor).await?;

            // Pass the reader's own schema so that V5CompressedLegacy rows can be fully
            // parsed and their partition RowKeys emitted.  Without a schema, parse_row_v5
            // fails for all rows in a partition, causing no entries to be pushed and making
            // the key comparison always miss even when the key exists.
            let schema_opt = self.get_table_schema(None);
            // Issue #1741: point lookups apply SELECT-semantic read shadowing, so
            // build the parser with read_shadowing = true. The stitch/parse split
            // (issue #1411) is preserved: CRC/decompress failures already surfaced
            // via `stitch_all_chunks` above; only `parse_block` may soft-miss.
            let parser = self.build_v5_parser(true);
            // #3782: `stitch_all_chunks` returned the WHOLE data section.
            let all_entries = match parser.parse_block(
                &stitched_buffer,
                BufferExtent::Complete,
                schema_opt.as_ref(),
                self,
            ) {
                Ok(entries) => entries,
                // Issue #1411 (roborev): the scan path (`stitch_and_parse_all_chunks`)
                // propagates EVERY `parse_block` error via `?`. Mirror that here so
                // `get()` and `scan()` agree on which errors are fatal. The ONLY
                // legitimate soft-miss — the case the original blanket
                // `Err(_) => Ok(None)` existed to protect — is classified by
                // `is_parse_soft_miss`: this reader has no schema for the table, so it
                // cannot serve the key and the caller must try the next SSTable reader.
                // Every other failure (real corruption / malformed block, or a deep
                // schema/type-resolution error when a schema IS present) MUST surface.
                Err(e) if is_parse_soft_miss(schema_opt.is_some(), &e) => {
                    tracing::debug!(
                        "scan_for_key: no schema for this reader ({}); soft-miss so the caller tries the next reader: {}",
                        table_id,
                        e
                    );
                    return Ok(None);
                }
                Err(e) => return Err(e),
            };

            // NOTE: The SSTableIndex is built from 16-byte Murmur3 *digests*, not raw keys,
            // so find_entry() always misses and falls through to this path.  For a found key
            // we stop early (O(found position)); for a key not present we must scan the whole
            // stitched buffer — O(file size).  This O(file) miss cost is an existing
            // limitation of the digest-index design and is tracked separately as a follow-up.
            //
            // NON-BLOCKING-2: Table-id matching is intentionally skipped in the stitching path
            // (consistent with sequential_scan's stitching path).  The V5CompressedLegacy parser
            // returns entries tagged with the table_id from the SSTable header, which may hold
            // default or incorrect values when headers use bare keyspace/table names rather than
            // the query's fully-qualified form.  Since all entries in this stitch buffer come from
            // the single SSTable being queried, skipping the check is correct and safe.
            for (_, entry_key, entry_value) in all_entries {
                if entry_key == *key {
                    // Early-return on first match (BLOCKING-2: don't parse the rest of the file).
                    if !self.filter_tombstone(&entry_value) {
                        return Ok(None);
                    }
                    return Ok(Some(entry_value));
                }
            }

            return Ok(None);
        }

        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }

        // Sequential scan through blocks
        while let Some(block) = self.read_next_block(&cursor).await? {
            let entries = self.parse_block_entries(&block, None, true)?;

            for (entry_table_id, entry_key, entry_value) in entries {
                if table_ids_match(&entry_table_id, table_id) && entry_key == *key {
                    // Extract write time from entry metadata
                    let _write_time = self.extract_write_time_from_entry(&entry_key, &entry_value);

                    // Filter out tombstones and expired data
                    if !self.filter_tombstone(&entry_value) {
                        return Ok(None);
                    }

                    return Ok(Some(entry_value));
                }
            }
        }

        Ok(None)
    }

    /// `scan_cancel` is an explicit PER-CALL cancellation token (issue #2346).
    /// Every existing caller passes `&self.scan_cancel` (the reader's own field,
    /// unchanged pre-#2346 semantics); the compaction path
    /// ([`SSTableReader::iterate_all_partitions_cancellable`]) passes its
    /// caller-supplied token instead, so a shared/cached reader's two concurrent
    /// scans cancel independently.
    pub(in crate::storage::sstable::reader) async fn sequential_scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
        scan_cancel: &ScanCancel,
    ) -> Result<Vec<(RowKey, ScanRow)>> {
        let _scan = self.begin_scan(); // #3853 scan-lifetime madvise seam
        tracing::debug!(
            "SSTableReader::sequential_scan - starting: table_id={table_id}, has_schema={}",
            schema.is_some()
        );

        // Issue #3109: BTI (`da`) readers decode through the authoritative trie
        // walk, NEVER the block loop below. `iterate_all_partitions` is the one
        // caller that reaches here with a `da` reader (both its index branches are
        // gated on `bti_partitions_db.is_none()`), and that loop's state machine
        // drops `read_shadowing` (and fails outright on a schema-required fixture).
        // Posture is otherwise identical: both apply `filter_tombstone` + the key
        // range + sort-then-`limit`. The PER-CALL `scan_cancel` is threaded through
        // (#2264/#2346) — the non-cancellable wrapper polls the READER's own field
        // instead, so a cancelled walk would stitch+parse the whole data section
        // and return `Ok(every row)`.
        if self.bti_partitions_db.is_some() {
            let entries = self
                .bti_scan_with_metadata_cancellable(
                    start_key,
                    end_key,
                    limit,
                    schema,
                    true,
                    None,
                    scan_cancel,
                )
                .await?;
            return Ok(entries.into_iter().map(|(k, v, _meta)| (k, v)).collect());
        }

        // Issue #815: each scan uses its own cursor (private file position and
        // chunk index), so concurrent scans on this reader run in parallel
        // without the per-scan serialization #805 introduced for correctness.
        let cursor = self.new_scan_cursor().await?;

        let mut results = Vec::new();

        let header_size = self.calculate_header_size();
        tracing::debug!(
            "SSTableReader::sequential_scan - Header size: {} bytes",
            header_size
        );

        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
            tracing::debug!(
                "SSTableReader::sequential_scan - Seeked to start of data section at offset {}",
                header_size
            );
        }

        // CRITICAL FIX: V5CompressedLegacy partitions can span chunk boundaries.
        // We must stitch all chunks together before parsing to avoid dropping partitions.
        // Use `requires_chunk_stitching()` as the single source of truth for whether
        // stitching is needed (BLOCKING-3: unified predicate).
        //
        // Note: We intentionally skip table_id matching in the stitching path because the
        // parser may return incorrect table_ids from header defaults.  Since sequential_scan
        // is called with a specific table_id, all entries from this SSTable match it.
        if self.requires_chunk_stitching() {
            tracing::debug!(
                "SSTableReader::sequential_scan - V5CompressedLegacy NB detected, using stitched buffer"
            );

            // Stitch all chunks together (reuse logic from get_all_entries).
            // Thread the PER-CALL cancel token (issue #2346) so a cancelled
            // caller abandons the stitched walk promptly instead of blocking
            // until the entire data section is stitched+parsed — the
            // chunk-stitch loop polls at the same 256-chunk cadence as the
            // non-stitching branch below.
            let all_entries = self
                .stitch_and_parse_all_chunks(&cursor, schema, true, scan_cancel)
                .await?;
            tracing::debug!(
                "SSTableReader::sequential_scan - Stitched parsing returned {} total entries",
                all_entries.len()
            );

            // Apply key-range filter and tombstone filter; collect ALL matching entries
            // before sorting.  Limit is applied AFTER sort so that LIMIT N returns the N
            // token-smallest partitions, not the first N encountered in parse order.
            // (BLOCKING-1: limit-after-order)
            // Work-probe (issue #2398, roborev 1692): same-partition rows arrive
            // consecutively (Data.db is laid out partition-by-partition), so a
            // changed `RowKey` (the partition key) marks a new partition body
            // decoded — matching the "once per partition" semantics of the two
            // index-driven walks. Tracked here, BEFORE the filters below, so it
            // counts decode work regardless of a later tombstone/range skip.
            let mut prev_partition_key: Option<RowKey> = None;
            for (idx, (_entry_table_id, entry_key, entry_value)) in
                all_entries.into_iter().enumerate()
            {
                // Cooperative checkpoint (issue #2346): the chunk-stitch loop
                // poll covers the I/O phase, but `parse_block` materialises every
                // entry in one shot — checkpoint here so a cancelled caller does
                // not walk a huge already-parsed result set to completion, and so
                // the query-engine's ONE `max_execution_time` timeout can elapse
                // mid-walk (issue #1695: the yield, at the same 256-entry cadence,
                // is what makes this future `Pending`).
                scan_cancel.checkpoint(idx).await?;
                if prev_partition_key.as_ref() != Some(&entry_key) {
                    crate::storage::sstable::work_counters::add_stream_walk_partition_parsed();
                    prev_partition_key = Some(entry_key.clone());
                }
                if let Some(start) = start_key {
                    if &entry_key < start {
                        continue;
                    }
                }

                if let Some(end) = end_key {
                    if &entry_key > end {
                        continue;
                    }
                }

                if !self.filter_tombstone(&entry_value) {
                    continue;
                }

                results.push((entry_key, entry_value));
            }

            tracing::debug!(
                "SSTableReader::sequential_scan - Filtered to {} results before limit (limit: {:?})",
                results.len(),
                limit
            );

            // Sort by Murmur3 token order (spec §5, Appendix B §313), then truncate to limit.
            sort_by_token_order(&mut results);
            if let Some(lim) = limit {
                results.truncate(lim);
            }

            tracing::debug!(
                "SSTableReader::sequential_scan - Returning {} results after sort+limit",
                results.len()
            );
            return Ok(results);
        }

        // Non-stitching path for other formats
        let mut block_count = 0;
        // Work-probe (issue #2398, roborev 1692): scoped OUTSIDE the block loop so
        // a partition split across a block boundary is not double-counted; see the
        // stitching branch above for the "changed key = new partition" rationale.
        let mut prev_partition_key: Option<RowKey> = None;
        while let Some(block) = self.read_next_block(&cursor).await? {
            // Cooperative checkpoint (issue #2264): an index-less (Summary.db
            // absent) SSTable materialises EVERY partition here in one pass; poll
            // the token per block so a cancelled Flight `do_get` abandons the walk
            // promptly instead of running to completion under the ~1–2 min backstop.
            // A whole block is coarse, so every iteration is a correct yield point
            // for the chokepoint timeout too (issue #1695).
            scan_cancel.checkpoint_now().await?;
            block_count += 1;
            tracing::debug!(
                "SSTableReader::sequential_scan - Read block {}, size {} bytes",
                block_count,
                block.len()
            );

            let entries = self.parse_block_entries_with_schema(&block, schema, true)?;
            tracing::debug!(
                "SSTableReader::sequential_scan - Block {} contains {} entries",
                block_count,
                entries.len()
            );

            for (i, (entry_table_id, entry_key, entry_value)) in entries.iter().enumerate() {
                // Cooperative cancellation (issue #2264, roborev): the per-block
                // poll above fires ONCE per `read_next_block` call, but an
                // uncompressed/BTI-direct block returns the WHOLE data section as
                // one contiguous unit (per `read_next_block_impl`'s doc comment) —
                // so for that shape `entries` alone can number in the hundreds of
                // thousands. Poll again here at the SAME 256-entry cadence used
                // elsewhere so materialisation honours the interval regardless of
                // how large a single block turned out to be, independent of
                // whichever inner parser branch produced `entries`.
                scan_cancel.checkpoint(i).await?;
                tracing::debug!(
                    "SSTableReader::sequential_scan - Block {} entry {}: table_id='{}', key={:?}",
                    block_count,
                    i,
                    entry_table_id,
                    entry_key
                );

                // Work-probe (issue #2398, roborev 1692): count decode work BEFORE
                // any table-id/range/tombstone filter, matching the other sites.
                if prev_partition_key.as_ref() != Some(entry_key) {
                    crate::storage::sstable::work_counters::add_stream_walk_partition_parsed();
                    prev_partition_key = Some(entry_key.clone());
                }

                // Match table IDs - supports both qualified (keyspace.table) and unqualified (table) formats
                // This allows queries with either format to match SSTables stored with either format
                if !table_ids_match(entry_table_id, table_id) {
                    tracing::debug!("SSTableReader::sequential_scan - Skipping entry: table_id mismatch ('{}' != '{}')",
                              entry_table_id, table_id);
                    continue;
                }

                // Check key range
                if let Some(start) = start_key {
                    if entry_key < start {
                        tracing::debug!(
                            "SSTableReader::sequential_scan - Skipping entry: key < start_key"
                        );
                        continue;
                    }
                }

                if let Some(end) = end_key {
                    if entry_key > end {
                        tracing::debug!(
                            "SSTableReader::sequential_scan - Skipping entry: key > end_key"
                        );
                        continue;
                    }
                }

                // Extract write time from entry metadata
                let _write_time = self.extract_write_time_from_entry(entry_key, entry_value);

                // Filter out tombstones and expired data
                if !self.filter_tombstone(entry_value) {
                    tracing::debug!("SSTableReader::sequential_scan - Skipping entry: filtered out (tombstone or expired)");
                    continue;
                }

                tracing::debug!("SSTableReader::sequential_scan - Including entry in results");
                results.push((entry_key.clone(), entry_value.clone()));
            }
        }

        tracing::debug!(
            "SSTableReader::sequential_scan - Finished scanning {} blocks",
            block_count
        );
        tracing::debug!(
            "SSTableReader::sequential_scan - {} results before sort+limit",
            results.len()
        );

        // Sort by Murmur3 token order (spec §5, Appendix B §313), then apply limit.
        // Limit is applied AFTER sort so that LIMIT N returns the N token-smallest
        // partitions (BLOCKING-1: limit-after-order).
        sort_by_token_order(&mut results);
        if let Some(lim) = limit {
            results.truncate(lim);
        }

        tracing::debug!(
            "SSTableReader::sequential_scan - Returning {} results after sort+limit",
            results.len()
        );
        Ok(results)
    }

    /// Scan a range of keys AND return per-cell write metadata.
    ///
    /// Used when `ProjectionFlags::include_cell_metadata` is set (issue #693).
    /// Falls through to `stitch_and_parse_all_chunks_with_metadata` for
    /// V5CompressedLegacy format (the common path for real SSTables).
    /// Returns `None` as the metadata for non-V5 formats (they do not carry
    /// per-cell timestamps in a way the parser currently surfaces).
    pub async fn scan_with_cell_metadata(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<
        Vec<(
            RowKey,
            ScanRow,
            std::collections::HashMap<String, CellWriteMetadata>,
        )>,
    > {
        tracing::debug!("SSTableReader::scan_with_cell_metadata - Starting");
        let _scan = self.begin_scan(); // #3853 scan-lifetime madvise seam

        // Issue #660: BTI ("da") metadata scan — same whole-Data.db walk as the
        // plain BTI scan, but surfaces per-cell write metadata for WRITETIME/TTL.
        if self.bti_partitions_db.is_some() {
            return self
                .bti_scan_with_metadata(start_key, end_key, limit, schema, true, None)
                .await;
        }

        // Issue #815: independent per-scan cursor — no cross-scan serialization.
        let cursor = self.new_scan_cursor().await?;

        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }

        // V5CompressedLegacy (stitching) path — the common path for Cassandra 5.0 SSTables.
        if self.requires_chunk_stitching() {
            let all_entries = self
                .stitch_and_parse_all_chunks_with_metadata(&cursor, schema)
                .await?;

            let mut results = Vec::new();
            for (_entry_table_id, entry_key, entry_value, cell_meta) in all_entries {
                if let Some(start) = start_key {
                    if &entry_key < start {
                        continue;
                    }
                }
                if let Some(end) = end_key {
                    if &entry_key > end {
                        continue;
                    }
                }
                if !self.filter_tombstone(&entry_value) {
                    continue;
                }
                results.push((entry_key, entry_value, cell_meta));
            }

            sort_by_token_order_with_meta(&mut results);
            if let Some(lim) = limit {
                results.truncate(lim);
            }

            tracing::debug!(
                "SSTableReader::scan_with_cell_metadata - Returning {} results (stitched path)",
                results.len()
            );
            return Ok(results);
        }

        // Non-stitching path: fall back to regular scan + empty metadata.
        // Per-cell metadata is not yet surfaced for block-entry formats.
        let plain = self
            .sequential_scan(
                table_id,
                start_key,
                end_key,
                limit,
                schema,
                &self.scan_cancel,
            )
            .await?;
        Ok(plain
            .into_iter()
            .map(|(k, v)| (k, v, std::collections::HashMap::new()))
            .collect())
    }
}

// Unit + corpus tests for this scan path. Split into a sibling `*_tests.rs` file per
// the campsite rule (#1116/#1135): this source file is well past the ~800-line target.
#[cfg(test)]
#[path = "sequential_tests.rs"]
mod tests;

// The PER-ROW streaming scan (`scan_stream`, issue #790) and its issue-#3124
// fail-closed producer boundary — its own file, since this one is more than twice the
// ~800-line campsite target (epic #1116).
#[path = "per_row_scan_stream.rs"]
mod per_row_scan_stream;

// The BATCHED streaming scan (`scan_stream_batched`, issue #1592) and its issue-#3109
// BTI dispatch — split out of this file for the same campsite reason (epic #1116).
#[path = "batched_scan_stream.rs"]
pub(super) mod batched_scan_stream;
