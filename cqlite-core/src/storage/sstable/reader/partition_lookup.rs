//! Partition lookup and index-based access methods for SSTableReader
//!
//! This module contains methods for efficient partition lookup using Index.db,
//! Summary.db, and Statistics.db readers.

use super::SSTableReader;
use crate::schema::registry::ParsingContext;
use crate::types::{ScanRow, TableId};
use crate::{Error, Result, RowKey};
use log::debug;

impl SSTableReader {
    /// Enhanced partition lookup using Index.db reader with promoted index support.
    ///
    /// `partition_key` must be the raw partition-key bytes as produced by
    /// [`PartitionKey::to_bytes`](crate::storage::write_engine::mutation::PartitionKey::to_bytes):
    ///
    /// - **Single-component keys** — raw value bytes (UUID = 16 bytes, int = 4 BE bytes, etc.).
    /// - **Multi-component (composite) keys** — `[len: u16 BE][value bytes][0x00]` per component,
    ///   including a trailing `0x00` after the final component.
    ///
    /// The Index.db key_lookup map is keyed on these exact raw bytes (set when the BIG-format
    /// parser was fixed in Issue #552).  The old digest-based path (which caused every lookup
    /// to miss) has been removed.  On a miss the function returns `Ok(None)` so callers can
    /// fall through to their existing sequential-scan fallback.
    pub async fn lookup_partition_with_index(
        &self,
        partition_key: &[u8],
    ) -> Result<Option<(u64, u32)>> {
        use crate::observability::{self as obs, catalog};

        let _span = tracing::debug_span!("sstable.partition_lookup.index").entered();
        let format = self.sstable_format_label();

        // B4 key→partition-offset cache (issue #1570): a repeated point read of the
        // same present key returns the cached location without re-probing Index.db,
        // so `INDEX_PROBES` stays 0 on the hit. Positive-only, so an absent key is
        // never cached and never fabricates a hit. The cached location is the exact
        // `(offset,size)` a fresh probe resolved (correctness guardrail).
        if let Some(loc) = self.key_offset_cache.get(partition_key) {
            debug!(
                "B4 key-cache hit for partition (raw key len={}): offset={}, size={}",
                partition_key.len(),
                loc.data_offset,
                loc.data_size
            );
            obs::add_counter(
                catalog::READ_PARTITION_LOOKUP,
                1,
                &[
                    (catalog::attr::RESULT, "hit".into()),
                    (catalog::attr::LOOKUP_ROUTE, "index".into()),
                    (catalog::attr::SSTABLE_FORMAT, format.into()),
                ],
            );
            return Ok(Some((loc.data_offset, loc.data_size)));
        }

        if let Some(index_reader) = &self.index_reader {
            // A5/B4 read-work counter (`INDEX_PROBES`; consumer B4): one per real
            // `Index.db` probe. Counted only when a cache miss forces a real probe,
            // so a B4 cache hit above records nothing. No-op in release.
            crate::storage::sstable::read_work_counters::record_index_probe();
            // Direct raw-key lookup — O(1) HashMap lookup.
            // Index.db entries are keyed on the raw partition key bytes since #552;
            // no Murmur3 digest computation is needed or correct here.
            if let Some(entry) = index_reader.lookup_partition(partition_key) {
                debug!(
                    "Found partition via Index.db raw-key lookup: offset={}, size={}",
                    entry.data_offset, entry.data_size
                );
                obs::add_counter(
                    catalog::READ_PARTITION_LOOKUP,
                    1,
                    &[
                        (catalog::attr::RESULT, "hit".into()),
                        (catalog::attr::LOOKUP_ROUTE, "index".into()),
                        (catalog::attr::SSTABLE_FORMAT, format.into()),
                    ],
                );
                // B4: cache this present-key resolution so the next point read of
                // the same key skips the Index.db probe (issue #1570).
                self.key_offset_cache.insert(
                    partition_key,
                    crate::storage::cache::PartitionLoc::new(entry.data_offset, entry.data_size),
                );
                return Ok(Some((entry.data_offset, entry.data_size)));
            } else {
                debug!(
                    "Partition not found in Index.db for raw key (len={})",
                    partition_key.len()
                );
            }
        } else {
            debug!("No Index.db reader available for partition lookup");
        }
        obs::add_counter(
            catalog::READ_PARTITION_LOOKUP,
            1,
            &[
                (catalog::attr::RESULT, "miss".into()),
                (catalog::attr::LOOKUP_ROUTE, "index".into()),
                (catalog::attr::SSTABLE_FORMAT, format.into()),
            ],
        );
        Ok(None)
    }

    /// BTI ("da") partition point lookup via the in-memory Partitions.db trie
    /// (issue #831, building on the verified #755 primitive).
    ///
    /// Encodes `partition_key` into the BTI byte-comparable trie key and walks the
    /// trie to resolve the partition's location. Returns:
    ///
    /// - `Ok(Some(offset))` — the UNCOMPRESSED Data.db byte offset of the
    ///   partition. INVARIANT: this offset indexes into the DECOMPRESSED data
    ///   section, never raw file bytes. The offset is resolved in one of two ways:
    ///     - **NARROW** partition (`BtiPartitionLocation::DataOffset`) — the trie
    ///       returns the Data.db offset directly.
    ///     - **WIDE** partition (`BtiPartitionLocation::RowsOffset`) — the trie
    ///       returns a positive offset into `Rows.db`; we deserialize that
    ///       partition's `TrieIndexEntry` via [`resolve_rows_db_entry`] and use its
    ///       recovered `data_position` (issue #909/#910). Both forms share the same
    ///       uncompressed Data.db offset domain, so the caller treats them
    ///       identically.
    /// - `Ok(None)` — the reader is not BTI, or the key has no trie path (the
    ///   partition is definitely absent from this SSTable).
    /// - `Err(_)` — a structural trie parse error, or a `RowsOffset` was returned
    ///   without an accompanying `Rows.db` (a structurally invalid BTI SSTable).
    ///
    /// Because the BTI trie uses path compression, a returned offset may be a
    /// candidate for a *prefix-colliding* key. The caller (`bti_point_lookup`)
    /// MUST verify the partition-key bytes at the resolved offset equal the
    /// queried key before returning rows.
    ///
    /// [`resolve_rows_db_entry`]: crate::storage::sstable::bti::resolve_rows_db_entry
    pub fn lookup_partition_via_bti_trie(&self, partition_key: &[u8]) -> Result<Option<u64>> {
        use crate::observability::{self as obs, catalog};
        use crate::storage::sstable::bti::{
            lookup_raw_key_in_bti_partitions_slice, resolve_rows_db_entry, BtiPartitionLocation,
        };

        let span = tracing::debug_span!(
            "sstable.partition_lookup.bti_trie",
            partition_shape = tracing::field::Empty,
        );
        let _entered = span.enter();

        let Some(partitions_db) = &self.bti_partitions_db else {
            // Not a BTI reader — no trie to consult (no descent, no count).
            return Ok(None);
        };

        // B4 key→partition-offset cache (issue #1570): a repeated point read of the
        // same present key returns the cached offset WITHOUT a second trie descent,
        // so `TRIE_WALKS` stays 0 on the hit — even across interleaved keys, which
        // the single-entry C3 memo below cannot cover. Trie-hit-only (prefix-collision
        // candidates included; the caller re-verifies the key bytes at the resolved
        // offset), never a trie MISS: so an absent key CAN be cached at its candidate
        // offset, but the cached offset is the exact offset a fresh descent resolves,
        // so re-verification reaches the identical conclusion (correctness guardrail;
        // mirrors the C3 `bti_lookup_memo`). Emit the presence counters (found = true)
        // so a cache-served lookup records the same presence decision a fresh descent
        // does.
        if let Some(loc) = self.key_offset_cache.get(partition_key) {
            Self::emit_bti_presence_counters(true);
            return Ok(Some(loc.data_offset));
        }

        // Issue #1574 (C3) single walk: a single-candidate point read descends the
        // SAME trie for the SAME key twice (candidate prune then seek). Reuse the
        // prune's resolution — a pure function of the immutable trie + key — so the
        // seek returns without a second descent (`TRIE_WALKS` stays 1 per read). A
        // stale/absent slot simply misses and re-walks. The presence-oracle
        // observability is preserved: a memo hit still records the presence decision.
        if let Some(resolved) = self.bti_lookup_memo_get(partition_key) {
            Self::emit_bti_presence_counters(resolved.is_some());
            return Ok(resolved);
        }

        // A5 read-work counter (TRIE_WALKS; consumers C3/C4): one per real trie
        // descent — counted only once we have a `Partitions.db` to descend, so a
        // non-BTI reader that returned above never bumps it. No-op in release
        // (design.md Decision 1/2).
        crate::storage::sstable::read_work_counters::record_trie_walk();

        // Issue #1574 (C3): walk the resident `Arc<Vec<u8>>` trie buffer in place —
        // no per-lookup whole-file copy (the former `Cursor` + `read_exact`).
        let lookup =
            lookup_raw_key_in_bti_partitions_slice(partitions_db.as_slice(), partition_key)
                .map_err(|e| {
                    Error::corruption(format!(
                        "BTI Partitions.db trie lookup failed for partition key (len={}): {}",
                        partition_key.len(),
                        e
                    ))
                });
        let lookup = match lookup {
            Ok(v) => v,
            Err(e) => {
                obs::record_error(&e, "reader");
                return Err(e);
            }
        };

        // The BTI Partitions.db trie IS the presence oracle for a BTI SSTable
        // (BTI has no bloom filter). Emit READ_BLOOM_CHECKS exactly ONCE here — the
        // single common path every BTI presence/point lookup funnels through
        // (get / get_with_spec_readers / get_with_schema_context / point lookup /
        // might_contain_partition). `cqlite.result` is hit for a trie HIT
        // (maybe-present/found: a DataOffset/RowsOffset, possibly a prefix-collision
        // candidate the caller re-verifies) and miss for a trie MISS (definitive
        // absence). A trie parse error returns above without an outcome and is
        // counted as an error, not a bloom check, so this never double counts.
        // Emitting before the per-arm handling (which can still error on a missing
        // Rows.db for a wide partition) guarantees a single emission per call.
        obs::add_counter(
            catalog::READ_BLOOM_CHECKS,
            1,
            &[
                (
                    catalog::attr::RESULT,
                    if lookup.is_some() { "hit" } else { "miss" }.into(),
                ),
                (catalog::attr::SSTABLE_FORMAT, "bti".into()),
            ],
        );

        match lookup {
            Some(BtiPartitionLocation::DataOffset(off)) => {
                span.record("partition_shape", "narrow");
                obs::add_counter(
                    catalog::READ_PARTITION_LOOKUP,
                    1,
                    &[
                        (catalog::attr::RESULT, "hit".into()),
                        (catalog::attr::LOOKUP_ROUTE, "bti_trie".into()),
                        (catalog::attr::SSTABLE_FORMAT, "bti".into()),
                    ],
                );
                debug!(
                    "BTI trie resolved NARROW partition (key len={}) to Data.db offset {}",
                    partition_key.len(),
                    off
                );
                self.bti_lookup_memo_store(partition_key, Some(off));
                // B4: cache this trie-hit resolution (a prefix-collision candidate,
                // re-verified by the caller downstream) so a later interleaved read of
                // the same key skips the trie descent (issue #1570).
                self.key_offset_cache.insert(
                    partition_key,
                    crate::storage::cache::PartitionLoc::offset_only(off),
                );
                Ok(Some(off))
            }
            Some(BtiPartitionLocation::RowsOffset(rows_offset)) => {
                // WIDE partition: the trie pointed at this partition's
                // TrieIndexEntry inside Rows.db. Deserialize it to recover the
                // partition's Data.db start position (`data_position`), which lives
                // in the SAME uncompressed-offset domain the narrow path uses, so
                // the caller can decode the partition identically (#909/#910).
                span.record("partition_shape", "wide");
                let rows_db = match self.bti_rows_db.as_ref().ok_or_else(|| {
                    Error::corruption(format!(
                        "BTI Partitions.db trie returned RowsOffset({}) for partition key \
                         (len={}) but this reader has no Rows.db loaded; the SSTable is \
                         structurally invalid (Rows.db is required for wide partitions).",
                        rows_offset,
                        partition_key.len()
                    ))
                }) {
                    Ok(v) => v,
                    Err(e) => {
                        obs::record_error(&e, "reader");
                        return Err(e);
                    }
                };

                let header = match resolve_rows_db_entry(rows_db.as_slice(), rows_offset as usize)
                    .map_err(|e| {
                        Error::corruption(format!(
                            "BTI Rows.db row-index entry at RowsOffset({}) is unreadable for \
                             partition key (len={}): {}",
                            rows_offset,
                            partition_key.len(),
                            e
                        ))
                    }) {
                    Ok(v) => v,
                    Err(e) => {
                        obs::record_error(&e, "reader");
                        return Err(e);
                    }
                };

                obs::add_counter(
                    catalog::READ_PARTITION_LOOKUP,
                    1,
                    &[
                        (catalog::attr::RESULT, "hit".into()),
                        (catalog::attr::LOOKUP_ROUTE, "bti_trie".into()),
                        (catalog::attr::SSTABLE_FORMAT, "bti".into()),
                    ],
                );
                debug!(
                    "BTI trie resolved WIDE partition (key len={}) via RowsOffset {} -> Data.db \
                     position {} ({} row-index blocks)",
                    partition_key.len(),
                    rows_offset,
                    header.data_position,
                    header.block_count
                );
                self.bti_lookup_memo_store(partition_key, Some(header.data_position));
                // B4: cache this present WIDE-partition resolution (issue #1570).
                self.key_offset_cache.insert(
                    partition_key,
                    crate::storage::cache::PartitionLoc::offset_only(header.data_position),
                );
                Ok(Some(header.data_position))
            }
            None => {
                obs::add_counter(
                    catalog::READ_PARTITION_LOOKUP,
                    1,
                    &[
                        (catalog::attr::RESULT, "miss".into()),
                        (catalog::attr::LOOKUP_ROUTE, "bti_trie".into()),
                        (catalog::attr::SSTABLE_FORMAT, "bti".into()),
                    ],
                );
                self.bti_lookup_memo_store(partition_key, None);
                Ok(None)
            }
        }
    }

    /// Cheap presence oracle: can this SSTable possibly contain `partition_key`?
    ///
    /// Used to prune SSTables before a partition-targeted scan (the query engine's
    /// `WHERE pk = ?` fast path). Returning `false` MUST be definitive — the SSTable
    /// is then skipped entirely without being parsed — so this only ever returns
    /// `false` for an authoritative "absent" signal:
    ///
    /// - **BTI ("da")** readers have no bloom filter; the Partitions.db trie is the
    ///   authoritative present/absent oracle. A trie miss (`Ok(None)`) is definitive
    ///   absence. A trie hit may be a prefix-collision candidate, which is a safe
    ///   *false positive* here (the partition scan re-verifies the key). Any trie
    ///   parse error is treated conservatively as "maybe present".
    /// - **BIG-format** readers consult the bloom filter, which never reports false
    ///   negatives: `might_contain == false` is definitive absence. With no bloom
    ///   filter loaded we cannot prune, so we conservatively return `true`.
    ///
    /// `partition_key` must be the raw partition-key bytes (same encoding the bloom
    /// filter and Index.db/BTI trie are keyed on).
    pub fn might_contain_partition(&self, partition_key: &[u8]) -> bool {
        use crate::observability::{self as obs, catalog};

        if self.bti_partitions_db.is_some() {
            // BTI: trie miss is authoritative absence; any error is conservative.
            // `lookup_partition_via_bti_trie` is the single common path that emits
            // READ_BLOOM_CHECKS for the BTI presence check — do NOT emit again here
            // or the metric would be double counted.
            return matches!(
                self.lookup_partition_via_bti_trie(partition_key),
                Ok(Some(_)) | Err(_)
            );
        }
        // BIG: only record READ_BLOOM_CHECKS when a bloom filter actually exists.
        // With no filter loaded we cannot prune, so we conservatively return `true`
        // WITHOUT recording a check (a no-filter "hit" is not a real bloom check and
        // would inflate the metric).
        match &self.bloom_filter {
            Some(bloom) => {
                let present = bloom.might_contain(partition_key);
                obs::add_counter(
                    catalog::READ_BLOOM_CHECKS,
                    1,
                    &[
                        (
                            catalog::attr::RESULT,
                            if present { "hit" } else { "miss" }.into(),
                        ),
                        (
                            catalog::attr::SSTABLE_FORMAT,
                            self.sstable_format_label().into(),
                        ),
                    ],
                );
                present
            }
            None => true,
        }
    }

    /// Enhanced partition lookup using schema-driven key digest computation
    pub async fn lookup_partition_with_schema_context(
        &self,
        partition_key: &[u8],
        parsing_context: &ParsingContext,
    ) -> Result<Option<(u64, u32)>> {
        if let Some(index_reader) = &self.index_reader {
            // Compute the schema-driven key digest for Index.db lookup
            let key_digest =
                self.compute_partition_key_digest_with_schema(partition_key, parsing_context)?;

            // Use spec-compliant Index.db reader for partition lookup
            if let Some(entry) = index_reader.lookup_partition(&key_digest) {
                debug!(
                    "Found partition via schema-driven Index.db: offset={}, size={}",
                    entry.data_offset, entry.data_size
                );
                return Ok(Some((entry.data_offset, entry.data_size)));
            }
        }
        Ok(None)
    }

    /// Enhanced partition iteration using Summary.db reader
    ///
    /// Note: Token-based range queries are not directly supported because Summary.db
    /// does not store token values (Issue #218). Instead, this iterates all summary
    /// entries and returns all partition data.
    ///
    /// For token-based filtering, compute tokens from partition keys after retrieval.
    ///
    /// ## Issue #500: Sequential-scan fallback for writer-produced SSTables
    ///
    /// The Summary.db → Index.db → Data.db lookup path depends on Index.db format
    /// compatibility between writer and reader (digest format vs. raw-key format).
    /// Locally written SSTables emit raw-key Index.db entries that the reader's
    /// digest-based parser cannot resolve, so the lookup loop returns 0 entries
    /// even though Summary.db enumerates the partitions correctly.
    ///
    /// When that happens we fall back to `sequential_scan`, which walks Data.db
    /// directly. For V5CompressedLegacy NB SSTables (the format the writer emits),
    /// `sequential_scan` uses the chunk-stitching path and returns every partition.
    pub async fn iterate_all_partitions(&self) -> Result<Vec<(RowKey, ScanRow)>> {
        if let Some(summary_reader) = &self.summary_reader {
            let entries = summary_reader.get_entries();
            let mut results = Vec::new();

            for entry in entries {
                // Use Summary.db entry to find the corresponding Index.db entry
                if let Some(_index_reader) = &self.index_reader {
                    // The summary entry provides a position in Index.db
                    // We need to read the partition data from Data.db

                    // For now, use the partition key from the summary entry
                    let partition_key_bytes = &entry.partition_key;

                    // Look up the partition in Index.db to get the actual data offset
                    if let Some((data_offset, data_size)) = self
                        .lookup_partition_with_index(partition_key_bytes)
                        .await?
                    {
                        // Convert Index.db relative offset to absolute file offset
                        // Index.db offsets are relative to data section start (after compression header)
                        let absolute_offset = data_offset + self.actual_header_size as u64;

                        // Read and parse the actual partition data from Data.db
                        match self
                            .parse_partition_at_offset(absolute_offset, data_size)
                            .await?
                        {
                            Some(partition_entries) => {
                                for (row_key, value) in partition_entries {
                                    results.push((row_key, value));
                                }
                            }
                            None => {
                                debug!("Failed to parse partition at offset {}", absolute_offset);
                            }
                        }
                    }
                } else {
                    log::error!("Index reader not available for partition iteration");
                    return Err(Error::corruption(
                        "Index reader required for partition iteration - synthetic data not allowed for Issue #35",
                    ));
                }
            }

            // Only trust the index-based path when EVERY summary entry was resolved.
            // Partial resolution silently drops the unresolved entries; defaulting to
            // `sequential_scan` in that case is strictly safer and still correct on
            // real Cassandra SSTables (sequential_scan returns the same partitions
            // when the index resolves them all).
            if results.len() == entries.len() && !entries.is_empty() {
                debug!("Partition iteration found {} entries", results.len());
                return Ok(results);
            }

            debug!(
                "Index.db lookup resolved {}/{} summary entries; \
                 falling back to sequential_scan (Issue #500)",
                results.len(),
                entries.len()
            );
        }

        // Fallback path: sequential walk of Data.db.
        // Used when Summary.db is absent OR when the Index.db lookup loop returned
        // no entries (Issue #500: writer-produced SSTables emit a raw-key Index.db
        // format the reader's digest-based parser does not resolve).
        let table_id = self.scan_table_id();
        let schema = self.schema.as_deref();
        self.sequential_scan(&table_id, None, None, None, schema)
            .await
    }

    /// Build the TableId used for fallback `sequential_scan` from header metadata.
    ///
    /// The reader populates `header.keyspace` / `header.table_name` from either the
    /// SSTable header or the parent directory path. When the V5CompressedLegacy
    /// stitching path is used, table_id matching is skipped, so any non-empty value
    /// is accepted; for other formats this returns the qualified `keyspace.table`
    /// form so the scan filter matches.
    fn scan_table_id(&self) -> TableId {
        let keyspace = &self.header.keyspace;
        let table_name = &self.header.table_name;
        if !keyspace.is_empty() && !table_name.is_empty() {
            TableId::from(format!("{}.{}", keyspace, table_name))
        } else if !table_name.is_empty() {
            TableId::from(table_name.as_str())
        } else {
            TableId::from("default")
        }
    }

    /// Token range iteration (deprecated - tokens not stored in Summary.db)
    ///
    /// This method is kept for API compatibility but simply delegates to
    /// `iterate_all_partitions()` since Summary.db does not store token values.
    /// Token filtering should be done by the caller after retrieval.
    #[deprecated(
        since = "0.1.0",
        note = "Summary.db does not store tokens. Use iterate_all_partitions() and filter by computed tokens."
    )]
    pub async fn iterate_token_range(
        &self,
        _start_token: i64,
        _end_token: i64,
    ) -> Result<Vec<(RowKey, ScanRow)>> {
        // Token values are not stored in Summary.db (Issue #218)
        // Delegate to all-partition iteration
        self.iterate_all_partitions().await
    }

    /// Get min/max timestamps from Statistics.db reader
    pub async fn get_timestamp_range(&self) -> Result<Option<(i64, i64)>> {
        if let Some(statistics_reader) = &self.statistics_reader {
            let (min_ts, max_ts) = statistics_reader.timestamp_range();
            debug!(
                "Retrieved timestamp range from Statistics.db: {} to {}",
                min_ts, max_ts
            );
            return Ok(Some((min_ts, max_ts)));
        }
        Ok(None)
    }

    /// Get token coverage (deprecated - tokens not stored in Summary.db)
    ///
    /// Note: As of Issue #218, Summary.db does not store token values.
    /// This method now returns None since token coverage cannot be determined
    /// from Summary.db alone. Token computation requires partition keys and
    /// the partitioner algorithm.
    #[deprecated(
        since = "0.1.0",
        note = "Summary.db does not store tokens. Compute tokens from partition keys using the partitioner."
    )]
    pub async fn get_token_coverage(&self) -> Result<Option<(i64, i64)>> {
        // Token values are not stored in Summary.db (Issue #218)
        // Return None - caller should compute tokens from partition keys if needed
        debug!("get_token_coverage: Summary.db does not store token values");
        Ok(None)
    }

    /// Enhanced get method using spec readers for efficient lookup
    pub async fn get_with_spec_readers(
        &self,
        table_id: &TableId,
        key: &RowKey,
    ) -> Result<Option<ScanRow>> {
        use crate::observability::{self as obs, catalog};

        // Issue #1034: BTI ("da") readers resolve partitions via the Partitions.db
        // trie, which is the AUTHORITATIVE presence oracle. Branch to the trie path
        // FIRST and skip the bloom/Index.db pre-check entirely. This mirrors `get()`
        // (which routes BTI to `bti_point_lookup` → `lookup_partition_via_bti_trie`)
        // and is required for correctness: a BTI bloom false negative must never
        // short-circuit the trie lookup. Routing through `get()` also guarantees the
        // BTI presence check emits READ_BLOOM_CHECKS exactly once (from
        // `lookup_partition_via_bti_trie`) instead of being counted here AND again on
        // the fallback path. BIG readers keep the bloom → Index.db behavior below.
        if self.bti_partitions_db.is_some() {
            return self.get(table_id, key).await;
        }

        // Step 1: Use bloom filter for existence check
        if let Some(bloom_filter) = &self.bloom_filter {
            let present = bloom_filter.might_contain(key.as_bytes());
            obs::add_counter(
                catalog::READ_BLOOM_CHECKS,
                1,
                &[
                    (
                        catalog::attr::RESULT,
                        if present { "hit" } else { "miss" }.into(),
                    ),
                    (
                        catalog::attr::SSTABLE_FORMAT,
                        self.sstable_format_label().into(),
                    ),
                ],
            );
            if !present {
                debug!("Bloom filter indicates key does not exist");
                return Ok(None);
            }
        }

        // Step 2: Use Index.db reader for precise partition lookup
        if let Some((offset, size)) = self.lookup_partition_with_index(key.as_bytes()).await? {
            debug!("Using Index.db lookup: offset={}, size={}", offset, size);
            return self.read_value_at_offset(offset, size).await;
        }

        // Step 3: Fallback to existing methods
        debug!("Falling back to legacy lookup methods");
        self.get(table_id, key).await
    }

    /// Enhanced get method using spec readers with schema-driven key digest computation
    pub async fn get_with_schema_context(
        &self,
        table_id: &TableId,
        key: &RowKey,
        parsing_context: &ParsingContext,
    ) -> Result<Option<ScanRow>> {
        use crate::observability::{self as obs, catalog};

        // Issue #1034: BTI ("da") readers resolve partitions via the Partitions.db
        // trie keyed on RAW partition-key bytes, not Index.db key digests, so the
        // schema-driven digest path below does not apply. Branch to the trie path
        // FIRST and skip the bloom/Index.db pre-check entirely, mirroring `get()`
        // (which routes BTI to `bti_point_lookup` → `lookup_partition_via_bti_trie`).
        // This keeps BTI correct (a bloom false negative can never short-circuit the
        // authoritative trie lookup) and ensures the BTI presence check emits
        // READ_BLOOM_CHECKS exactly once instead of being double counted across this
        // helper and its fallback. BIG readers keep the bloom → Index.db behavior.
        if self.bti_partitions_db.is_some() {
            return self.get(table_id, key).await;
        }

        // Step 1: Use bloom filter for existence check
        if let Some(bloom_filter) = &self.bloom_filter {
            let present = bloom_filter.might_contain(key.as_bytes());
            obs::add_counter(
                catalog::READ_BLOOM_CHECKS,
                1,
                &[
                    (
                        catalog::attr::RESULT,
                        if present { "hit" } else { "miss" }.into(),
                    ),
                    (
                        catalog::attr::SSTABLE_FORMAT,
                        self.sstable_format_label().into(),
                    ),
                ],
            );
            if !present {
                debug!("Bloom filter indicates key does not exist");
                return Ok(None);
            }
        }

        // Step 2: Use Index.db reader for precise partition lookup with schema-driven digest
        if let Some((offset, size)) = self
            .lookup_partition_with_schema_context(key.as_bytes(), parsing_context)
            .await?
        {
            debug!(
                "Using schema-driven Index.db lookup: offset={}, size={}",
                offset, size
            );
            return self.read_value_at_offset(offset, size).await;
        }

        // Step 3: Fallback to existing methods
        debug!("Falling back to legacy lookup methods");
        self.get(table_id, key).await
    }
}
