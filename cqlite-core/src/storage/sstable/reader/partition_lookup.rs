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
    /// Enhanced partition lookup using Index.db reader with promoted index support
    pub async fn lookup_partition_with_index(
        &self,
        partition_key: &[u8],
    ) -> Result<Option<(u64, u32)>> {
        if let Some(index_reader) = &self.index_reader {
            // Compute the proper key digest for Index.db lookup
            let key_digest = match self.compute_partition_key_digest(partition_key).await {
                Ok(digest) => digest,
                Err(e) => {
                    log::warn!("Failed to compute partition key digest: {}", e);
                    return Ok(None);
                }
            };

            // Use spec-compliant Index.db reader for partition lookup
            if let Some(entry) = index_reader.lookup_partition(&key_digest) {
                debug!(
                    "Found partition via Index.db: offset={}, size={}",
                    entry.data_offset, entry.data_size
                );
                return Ok(Some((entry.data_offset, entry.data_size)));
            } else {
                debug!(
                    "Partition not found in Index.db for key digest (len={})",
                    key_digest.len()
                );
            }
        } else {
            debug!("No Index.db reader available for partition lookup");
        }
        Ok(None)
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
                        // Read and parse the actual partition data from Data.db
                        match self
                            .parse_partition_at_offset(data_offset, data_size)
                            .await?
                        {
                            Some(partition_entries) => {
                                for (row_key, value) in partition_entries {
                                    results.push((row_key, value));
                                }
                            }
                            None => {
                                debug!("Failed to parse partition at offset {}", data_offset);
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

            debug!("Partition iteration found {} entries", results.len());
            return Ok(results);
        }

        // Fallback to existing scan method
        self.sequential_scan(&TableId::from("default"), None, None, None, None)
            .await
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
