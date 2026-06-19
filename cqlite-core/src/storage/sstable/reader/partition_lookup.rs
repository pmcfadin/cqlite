//! Partition lookup and index-based access methods for SSTableReader
//!
//! This module contains methods for efficient partition lookup using Index.db,
//! Summary.db, and Statistics.db readers.

use super::SSTableReader;
use crate::schema::registry::ParsingContext;
use crate::types::{TableId, Value};
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
        if let Some(index_reader) = &self.index_reader {
            // Direct raw-key lookup — O(1) HashMap lookup.
            // Index.db entries are keyed on the raw partition key bytes since #552;
            // no Murmur3 digest computation is needed or correct here.
            if let Some(entry) = index_reader.lookup_partition(partition_key) {
                debug!(
                    "Found partition via Index.db raw-key lookup: offset={}, size={}",
                    entry.data_offset, entry.data_size
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
        Ok(None)
    }

    /// BTI ("da") partition point lookup via the in-memory Partitions.db trie
    /// (issue #831, building on the verified #755 primitive).
    ///
    /// Encodes `partition_key` into the BTI byte-comparable trie key and walks the
    /// trie to resolve the partition's location. Returns:
    ///
    /// - `Ok(Some(offset))` — the UNCOMPRESSED Data.db byte offset of the
    ///   partition (`BtiPartitionLocation::DataOffset`). INVARIANT: this offset
    ///   indexes into the DECOMPRESSED data section, never raw file bytes.
    /// - `Ok(None)` — the reader is not BTI, or the key has no trie path (the
    ///   partition is definitely absent from this SSTable).
    /// - `Err(_)` — a structural trie parse error, or a `RowsOffset` result (the
    ///   Rows.db read path is not yet implemented; narrow tables like
    ///   `simple_table` always resolve to `DataOffset`).
    ///
    /// Because the BTI trie uses path compression, a returned offset may be a
    /// candidate for a *prefix-colliding* key. The caller (`bti_point_lookup`)
    /// MUST verify the partition-key bytes at the resolved offset equal the
    /// queried key before returning rows.
    pub fn lookup_partition_via_bti_trie(&self, partition_key: &[u8]) -> Result<Option<u64>> {
        use crate::storage::sstable::bti::{
            lookup_raw_key_in_bti_partitions_db, BtiPartitionLocation,
        };

        let Some(partitions_db) = &self.bti_partitions_db else {
            // Not a BTI reader — no trie to consult.
            return Ok(None);
        };

        let mut cursor = std::io::Cursor::new(partitions_db.as_slice());
        match lookup_raw_key_in_bti_partitions_db(&mut cursor, partition_key).map_err(|e| {
            Error::corruption(format!(
                "BTI Partitions.db trie lookup failed for partition key (len={}): {}",
                partition_key.len(),
                e
            ))
        })? {
            Some(BtiPartitionLocation::DataOffset(off)) => {
                debug!(
                    "BTI trie resolved partition (key len={}) to Data.db offset {}",
                    partition_key.len(),
                    off
                );
                Ok(Some(off))
            }
            Some(BtiPartitionLocation::RowsOffset(off)) => Err(Error::unsupported_format(format!(
                "BTI Rows.db read path not yet implemented (trie returned RowsOffset({}) \
                 for partition key len={}). Wide-partition BTI tables are out of scope for \
                 issue #831.",
                off,
                partition_key.len()
            ))),
            None => Ok(None),
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
    pub async fn iterate_all_partitions(&self) -> Result<Vec<(RowKey, Value)>> {
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
    ) -> Result<Vec<(RowKey, Value)>> {
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
    ) -> Result<Option<Value>> {
        // Step 1: Use bloom filter for existence check
        if let Some(bloom_filter) = &self.bloom_filter {
            if !bloom_filter.might_contain(key.as_bytes()) {
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
    ) -> Result<Option<Value>> {
        // Step 1: Use bloom filter for existence check
        if let Some(bloom_filter) = &self.bloom_filter {
            if !bloom_filter.might_contain(key.as_bytes()) {
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
