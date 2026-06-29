//! Sequential / index-driven read paths: range scans, full scans, the
//! `scan_for_key` fallback, the cell-metadata scan, and the bounded streaming
//! scan.
//!
//! These cover the BIG (`nb`) `V5CompressedLegacy` formats (chunk-stitched) and
//! the non-stitching block-by-block formats. BTI (`da`) range/full scans are
//! routed here only to delegate to [`SSTableReader::bti_scan_with_metadata`].

use super::super::SSTableReader;
use super::model::{
    sort_by_token_order, sort_by_token_order_with_meta, table_ids_match, SCAN_FOR_KEY_CALLS,
};
use crate::types::{CellWriteMetadata, TableId, Value};
use crate::{Result, RowKey};
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;
use tokio::sync::mpsc;

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
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(RowKey, Value)>> {
        log::debug!("SSTableReader::scan - Starting scan");
        log::debug!("SSTableReader::scan - File path: {:?}", self.file_path);
        log::debug!("SSTableReader::scan - Table ID: {}", table_id);
        log::debug!("SSTableReader::scan - Start key: {:?}", start_key);
        log::debug!("SSTableReader::scan - End key: {:?}", end_key);
        log::debug!("SSTableReader::scan - Limit: {:?}", limit);
        log::debug!("SSTableReader::scan - Has schema: {}", schema.is_some());
        log::debug!("SSTableReader::scan - Has index: {}", self.index.is_some());
        log::debug!(
            "SSTableReader::scan - Has bloom filter: {}",
            self.bloom_filter.is_some()
        );

        // Issue #660: BTI ("da") readers have no Index.db/Summary.db. A full scan
        // walks the whole (chunk-compressed) Data.db once and parses every
        // partition — the same partition decode the point-lookup path proves
        // correct, but emitting ALL partitions instead of stopping at the first.
        if self.bti_partitions_db.is_some() {
            let entries = self
                .bti_scan_with_metadata(start_key, end_key, limit, schema)
                .await?;
            return Ok(entries.into_iter().map(|(k, v, _meta)| (k, v)).collect());
        }

        let mut results = Vec::new();

        // Use index for efficient range scan if available
        if let Some(index) = &self.index {
            log::debug!("SSTableReader::scan - Using index-based scan");
            let entries = index.get_range(table_id, start_key, end_key)?;
            log::debug!(
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
                log::debug!(
                    "SSTableReader::scan - Index returned 0 entries (BTI format or incomplete parsing), falling back to sequential scan"
                );
                return self
                    .sequential_scan(table_id, start_key, end_key, limit, schema)
                    .await;
            }

            // Check if any entry has size=0 (Cassandra 5.0 format)
            let has_zero_size = entries.iter().any(|e| e.size == 0);
            if has_zero_size {
                log::debug!("SSTableReader::scan - Index reports size=0 for some entries, using sequential scan fallback");
                return self
                    .sequential_scan(table_id, start_key, end_key, limit, schema)
                    .await;
            }

            // Collect ALL index entries (limit applied after sort — BLOCKING-1).
            for (i, entry) in entries.iter().enumerate() {
                // Index offsets are relative to data section start - adjust for header
                let file_offset = entry.offset + self.actual_header_size as u64;
                log::debug!(
                    "SSTableReader::scan - Processing index entry {}: index_offset={}, file_offset={}, size={}",
                    i, entry.offset, file_offset, entry.size
                );

                if let Some(value) = self.read_value_at_offset(file_offset, entry.size).await? {
                    log::debug!(
                        "SSTableReader::scan - Successfully read value at offset {}",
                        entry.offset
                    );
                    results.push((entry.key.clone(), value));
                } else {
                    log::debug!("SSTableReader::scan - Value at offset {} was filtered out (tombstone or expired)", entry.offset);
                }
            }
        } else {
            // Fallback to sequential scan.  sequential_scan() already returns results in
            // token order (NON-BLOCKING-1: avoid double-sort — return directly).
            log::debug!("SSTableReader::scan - No index, falling back to sequential scan");
            let seq_results = self
                .sequential_scan(table_id, start_key, end_key, limit, schema)
                .await?;
            log::debug!(
                "SSTableReader::scan - Sequential scan returned {} results",
                seq_results.len()
            );
            log::debug!(
                "SSTableReader::scan - Returning {} final results",
                seq_results.len()
            );
            return Ok(seq_results);
        }

        // Index-based path: sort by Murmur3 token order (ascending token, then key bytes).
        // This matches the on-disk physical order (spec §5, Appendix B §313) and the write
        // engine's PartitionPosition::cmp.  Compute each key's token once before sorting to
        // avoid O(n log n) recomputation inside the comparator.
        sort_by_token_order(&mut results);
        // Limit applied AFTER sort so LIMIT N returns the N token-smallest partitions.
        if let Some(lim) = limit {
            results.truncate(lim);
        }

        log::debug!(
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
    pub async fn get_all_entries(&self) -> Result<Vec<(TableId, RowKey, Value)>> {
        // Issue #660: BTI ("da") tables have no Index.db; route through the
        // whole-Data.db BTI scan, which resolves schema via get_table_schema
        // (header/registry) and decodes every partition. It mints its own
        // per-scan cursor, as does the non-BTI path below (issue #815).
        if self.bti_partitions_db.is_some() {
            let table_id = TableId::new(format!(
                "{}.{}",
                self.header.keyspace, self.header.table_name
            ));
            let entries = self.bti_scan_with_metadata(None, None, None, None).await?;
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
            log::debug!(
                "V5CompressedLegacy format detected, decompressing and stitching all chunks before parsing"
            );

            // Use shared stitching helper method
            let entries = self.stitch_and_parse_all_chunks(&cursor, None).await?;
            results.extend(entries);
        } else {
            // Other formats: Read and parse blocks individually
            while let Some(block) = self.read_next_block(&cursor).await? {
                let entries = self.parse_block_entries(&block, None)?;
                results.extend(entries);
            }
        }

        // Issue #505: suppress row tombstones from user-facing output. The compaction
        // path (iterate_all_partitions_for_compaction) bypasses this filter.
        results.retain(|(_tid, _key, value)| self.filter_tombstone(value));

        Ok(results)
    }

    /// Streaming scan (issue #790): yield `(RowKey, Value)` entries lazily
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
    /// `(RowKey, Value)` count is the SUM of three inherent terms, not one constant:
    /// `buffer_size` (this channel) `+ max_partition_size` (the one fully-materialized
    /// confirmed partition — a pre-existing #1156 windowed-scan term, inherent to any
    /// row-materializing partition scan) `+`
    /// [`MAX_INFLIGHT_BATCH_ROWS`](super::super::scan_stream_windowed::MAX_INFLIGHT_BATCH_ROWS)
    /// (the FIXED, BOUNDED amount the issue-#1143 batching subsystem may run ahead,
    /// regardless of `buffer_size`, holding even for `buffer_size == 1`).
    /// `MAX_INFLIGHT_BATCH_ROWS` bounds the batching subsystem alone, NOT the
    /// `max_partition_size` materialization term. Non-stitching SSTables honour
    /// `buffer_size` exactly.
    pub fn scan_stream(
        self: std::sync::Arc<Self>,
        table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        buffer_size: usize,
    ) -> mpsc::Receiver<Result<(RowKey, Value)>> {
        let (tx, rx) = mpsc::channel(buffer_size.max(1));
        tokio::spawn(async move {
            if let Err(e) = self
                .run_scan_stream(table_id, start_key, end_key, schema, tx.clone())
                .await
            {
                // Surface the error to the consumer as a terminal stream item.
                let _ = tx.send(Err(e)).await;
            }
        });
        rx
    }

    async fn run_scan_stream(
        self: std::sync::Arc<Self>,
        table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        tx: mpsc::Sender<Result<(RowKey, Value)>>,
    ) -> Result<()> {
        // Issue #815: independent per-scan cursor — no cross-scan serialization.
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
            self.run_scan_stream_windowed(table_id, start_key, end_key, schema, &cursor, &tx)
                .await
        } else {
            // Non-stitching formats already read block-by-block; emit per block so
            // only one block's entries are live at a time.
            while let Some(block) = self.read_next_block(&cursor).await? {
                let entries = self.parse_block_entries_with_schema(&block, schema.as_ref())?;
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

    pub(super) async fn scan_for_key(
        &self,
        table_id: &TableId,
        key: &RowKey,
    ) -> Result<Option<Value>> {
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
            log::debug!(
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

            // Pass the reader's own schema so that V5CompressedLegacy rows can be fully
            // parsed and their partition RowKeys emitted.  Without a schema, parse_row_v5
            // fails for all rows in a partition, causing no entries to be pushed and making
            // the key comparison always miss even when the key exists.
            let schema_opt = self.get_table_schema(None);
            let all_entries = match self
                .stitch_and_parse_all_chunks(&cursor, schema_opt.as_ref())
                .await
            {
                Ok(entries) => entries,
                Err(e) => {
                    // Schema may not be available for this reader (e.g., wrong table type).
                    // Return None so the caller can try the next reader.
                    log::debug!(
                        "scan_for_key: stitch_and_parse_all_chunks failed (schema missing?): {}",
                        e
                    );
                    return Ok(None);
                }
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
            let entries = self.parse_block_entries(&block, None)?;

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

    pub(in crate::storage::sstable::reader) async fn sequential_scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(RowKey, Value)>> {
        log::debug!("SSTableReader::sequential_scan - Starting sequential scan");
        log::debug!("SSTableReader::sequential_scan - Table ID: {}", table_id);
        log::debug!(
            "SSTableReader::sequential_scan - Has schema: {}",
            schema.is_some()
        );

        // Issue #815: each scan uses its own cursor (private file position and
        // chunk index), so concurrent scans on this reader run in parallel
        // without the per-scan serialization #805 introduced for correctness.
        let cursor = self.new_scan_cursor().await?;

        let mut results = Vec::new();

        let header_size = self.calculate_header_size();
        log::debug!(
            "SSTableReader::sequential_scan - Header size: {} bytes",
            header_size
        );

        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
            log::debug!(
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
            log::debug!(
                "SSTableReader::sequential_scan - V5CompressedLegacy NB detected, using stitched buffer"
            );

            // Stitch all chunks together (reuse logic from get_all_entries)
            let all_entries = self.stitch_and_parse_all_chunks(&cursor, schema).await?;
            log::debug!(
                "SSTableReader::sequential_scan - Stitched parsing returned {} total entries",
                all_entries.len()
            );

            // Apply key-range filter and tombstone filter; collect ALL matching entries
            // before sorting.  Limit is applied AFTER sort so that LIMIT N returns the N
            // token-smallest partitions, not the first N encountered in parse order.
            // (BLOCKING-1: limit-after-order)
            for (_entry_table_id, entry_key, entry_value) in all_entries {
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

            log::debug!(
                "SSTableReader::sequential_scan - Filtered to {} results before limit (limit: {:?})",
                results.len(),
                limit
            );

            // Sort by Murmur3 token order (spec §5, Appendix B §313), then truncate to limit.
            sort_by_token_order(&mut results);
            if let Some(lim) = limit {
                results.truncate(lim);
            }

            log::debug!(
                "SSTableReader::sequential_scan - Returning {} results after sort+limit",
                results.len()
            );
            return Ok(results);
        }

        // Non-stitching path for other formats
        let mut block_count = 0;
        while let Some(block) = self.read_next_block(&cursor).await? {
            block_count += 1;
            log::debug!(
                "SSTableReader::sequential_scan - Read block {}, size {} bytes",
                block_count,
                block.len()
            );

            let entries = self.parse_block_entries_with_schema(&block, schema)?;
            log::debug!(
                "SSTableReader::sequential_scan - Block {} contains {} entries",
                block_count,
                entries.len()
            );

            for (i, (entry_table_id, entry_key, entry_value)) in entries.iter().enumerate() {
                log::debug!(
                    "SSTableReader::sequential_scan - Block {} entry {}: table_id='{}', key={:?}",
                    block_count,
                    i,
                    entry_table_id,
                    entry_key
                );

                // Match table IDs - supports both qualified (keyspace.table) and unqualified (table) formats
                // This allows queries with either format to match SSTables stored with either format
                if !table_ids_match(entry_table_id, table_id) {
                    log::debug!("SSTableReader::sequential_scan - Skipping entry: table_id mismatch ('{}' != '{}')",
                              entry_table_id, table_id);
                    continue;
                }

                // Check key range
                if let Some(start) = start_key {
                    if entry_key < start {
                        log::debug!(
                            "SSTableReader::sequential_scan - Skipping entry: key < start_key"
                        );
                        continue;
                    }
                }

                if let Some(end) = end_key {
                    if entry_key > end {
                        log::debug!(
                            "SSTableReader::sequential_scan - Skipping entry: key > end_key"
                        );
                        continue;
                    }
                }

                // Extract write time from entry metadata
                let _write_time = self.extract_write_time_from_entry(entry_key, entry_value);

                // Filter out tombstones and expired data
                if !self.filter_tombstone(entry_value) {
                    log::debug!("SSTableReader::sequential_scan - Skipping entry: filtered out (tombstone or expired)");
                    continue;
                }

                log::debug!("SSTableReader::sequential_scan - Including entry in results");
                results.push((entry_key.clone(), entry_value.clone()));
            }
        }

        log::debug!(
            "SSTableReader::sequential_scan - Finished scanning {} blocks",
            block_count
        );
        log::debug!(
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

        log::debug!(
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
            Value,
            std::collections::HashMap<String, CellWriteMetadata>,
        )>,
    > {
        log::debug!("SSTableReader::scan_with_cell_metadata - Starting");

        // Issue #660: BTI ("da") metadata scan — same whole-Data.db walk as the
        // plain BTI scan, but surfaces per-cell write metadata for WRITETIME/TTL.
        if self.bti_partitions_db.is_some() {
            return self
                .bti_scan_with_metadata(start_key, end_key, limit, schema)
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

            log::debug!(
                "SSTableReader::scan_with_cell_metadata - Returning {} results (stitched path)",
                results.len()
            );
            return Ok(results);
        }

        // Non-stitching path: fall back to regular scan + empty metadata.
        // Per-cell metadata is not yet surfaced for block-entry formats.
        let plain = self
            .sequential_scan(table_id, start_key, end_key, limit, schema)
            .await?;
        Ok(plain
            .into_iter()
            .map(|(k, v)| (k, v, std::collections::HashMap::new()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Integration tests with real SSTable data
    // =========================================================================

    #[tokio::test]
    async fn test_get_nonexistent_key() {
        use std::path::PathBuf;
        use std::sync::Arc;

        // Test with real SSTable data if available
        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
                return;
            }
        };

        let simple_table_dir = datasets_root.join("sstables/test_basic");
        if !simple_table_dir.exists() {
            eprintln!("test_basic not found, skipping test");
            return;
        }

        // Find simple_table
        let table_dir = std::fs::read_dir(&simple_table_dir)
            .ok()
            .and_then(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .find(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("simple_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            });

        let Some(table_path) = table_dir else {
            eprintln!("simple_table not found, skipping");
            return;
        };

        // Find Data.db file
        let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

        let Some(data_path) = data_file else {
            eprintln!("Data.db not found, skipping");
            return;
        };

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        let reader = SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("Failed to open SSTable");

        // Try to get a key that doesn't exist
        let table_id = TableId::new("test_basic.simple_table".to_string());
        let nonexistent_key = RowKey::new(vec![0xFF, 0xFF, 0xFF, 0xFF]); // Very unlikely to exist

        let result = reader.get(&table_id, &nonexistent_key).await;
        assert!(
            result.is_ok(),
            "get() should succeed even for nonexistent key"
        );
        assert!(
            result.unwrap().is_none(),
            "Nonexistent key should return None"
        );
    }

    #[tokio::test]
    async fn test_scan_with_limit() {
        use std::path::PathBuf;
        use std::sync::Arc;

        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
                return;
            }
        };

        let simple_table_dir = datasets_root.join("sstables/test_basic");
        if !simple_table_dir.exists() {
            eprintln!("test_basic not found, skipping test");
            return;
        }

        // Find simple_table
        let table_dir = std::fs::read_dir(&simple_table_dir)
            .ok()
            .and_then(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .find(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("simple_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            });

        let Some(table_path) = table_dir else {
            eprintln!("simple_table not found, skipping");
            return;
        };

        let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

        let Some(data_path) = data_file else {
            eprintln!("Data.db not found, skipping");
            return;
        };

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        let reader = SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("Failed to open SSTable");

        let table_id = TableId::new("test_basic.simple_table".to_string());

        // Test scan with limit
        let result = reader.scan(&table_id, None, None, Some(5), None).await;
        assert!(result.is_ok(), "scan() should succeed");

        let entries = result.unwrap();
        assert!(
            entries.len() <= 5,
            "Scan with limit 5 should return at most 5 entries, got {}",
            entries.len()
        );

        eprintln!("Scan with limit 5 returned {} entries", entries.len());
    }

    #[tokio::test]
    async fn test_scan_full_table() {
        use std::path::PathBuf;
        use std::sync::Arc;

        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
                return;
            }
        };

        let simple_table_dir = datasets_root.join("sstables/test_basic");
        if !simple_table_dir.exists() {
            eprintln!("test_basic not found, skipping test");
            return;
        }

        // Find simple_table
        let table_dir = std::fs::read_dir(&simple_table_dir)
            .ok()
            .and_then(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .find(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("simple_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            });

        let Some(table_path) = table_dir else {
            eprintln!("simple_table not found, skipping");
            return;
        };

        let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

        let Some(data_path) = data_file else {
            eprintln!("Data.db not found, skipping");
            return;
        };

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        let reader = SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("Failed to open SSTable");

        let table_id = TableId::new("test_basic.simple_table".to_string());

        // Full table scan (no limit)
        let result = reader.scan(&table_id, None, None, None, None).await;
        assert!(result.is_ok(), "Full scan should succeed");

        let entries = result.unwrap();
        eprintln!("Full scan returned {} entries", entries.len());
    }

    #[tokio::test]
    async fn test_get_all_entries() {
        use std::path::PathBuf;
        use std::sync::Arc;

        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
                return;
            }
        };

        let simple_table_dir = datasets_root.join("sstables/test_basic");
        if !simple_table_dir.exists() {
            eprintln!("test_basic not found, skipping test");
            return;
        }

        // Find simple_table
        let table_dir = std::fs::read_dir(&simple_table_dir)
            .ok()
            .and_then(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .find(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("simple_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            });

        let Some(table_path) = table_dir else {
            eprintln!("simple_table not found, skipping");
            return;
        };

        let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

        let Some(data_path) = data_file else {
            eprintln!("Data.db not found, skipping");
            return;
        };

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        let reader = SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("Failed to open SSTable");

        // Get all entries (for compaction use case)
        let result = reader.get_all_entries().await;
        assert!(result.is_ok(), "get_all_entries() should succeed");

        let entries = result.unwrap();
        eprintln!("get_all_entries() returned {} entries", entries.len());
    }

    /// Regression test for Issue #480: static cell duplication on read.
    ///
    /// static_columns_table has 100 partitions, each containing one static_block
    /// and one clustering row. CQLite should return exactly 100 result rows — one
    /// per partition — not 200 (which would occur if static rows were emitted as
    /// separate result entries).
    ///
    /// Two bugs were fixed:
    /// 1. Snappy varint collision: bytes `0xC0 0x51` at the start of the Snappy
    ///    stream were misidentified as the V5_0StaticColumns magic number, causing
    ///    the file pointer to advance past part of the compressed data before
    ///    decompression, resulting in "corrupt input" errors.
    /// 2. Static row duplication: static rows were pushed into `results` just like
    ///    clustering rows. They should be accumulated per-partition and merged into
    ///    each subsequent clustering row instead.
    #[tokio::test]
    async fn test_static_columns_table_row_count_issue480() {
        use std::path::PathBuf;
        use std::sync::Arc;

        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping Issue #480 regression test");
                return;
            }
        };

        let table_base = datasets_root.join("sstables/test_basic");
        if !table_base.exists() {
            eprintln!("test_basic dir not found, skipping Issue #480 regression test");
            return;
        }

        // Locate the static_columns_table directory
        let table_dir = std::fs::read_dir(&table_base).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("static_columns_table"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

        let Some(table_path) = table_dir else {
            eprintln!("static_columns_table not found, skipping Issue #480 regression test");
            return;
        };

        // Find the Data.db file (must be real binary, not macOS ._resource_fork)
        let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    let name = e.file_name();
                    let s = name.to_str().unwrap_or("");
                    s.ends_with("-Data.db") && !s.starts_with("._")
                })
                .map(|e| e.path())
        });

        let Some(data_path) = data_file else {
            eprintln!("Data.db not found in static_columns_table dir, skipping");
            return;
        };

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        let reader = SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("Failed to open static_columns_table SSTable");

        let table_id = crate::types::TableId::new("test_basic.static_columns_table".to_string());
        let result = reader.scan(&table_id, None, None, None, None).await;
        assert!(
            result.is_ok(),
            "Scan of static_columns_table should succeed: {:?}",
            result.err()
        );

        let entries = result.unwrap();
        eprintln!(
            "Issue #480 regression: static_columns_table scan returned {} rows",
            entries.len()
        );

        // Expected: 100 rows (one per partition, static data merged into clustering row)
        // Before fix: 0 rows (Snappy decompression failure)
        // After fixing only decompression: 200 rows (static rows emitted separately)
        // After full fix: 100 rows
        assert_eq!(
            entries.len(),
            100,
            "static_columns_table should return 100 rows (one per partition), \
             got {}. Regression for Issue #480: static cell duplication on read.",
            entries.len()
        );
    }
}
