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

    /// Enhanced token range iteration using Summary.db reader
    pub async fn iterate_token_range(
        &self,
        start_token: i64,
        end_token: i64,
    ) -> Result<Vec<(RowKey, Value)>> {
        if let Some(summary_reader) = &self.summary_reader {
            // Use Summary.db reader for efficient token range queries
            let token_entries = summary_reader.find_entries_in_range(start_token, end_token);
            let mut results = Vec::new();

            for entry in token_entries {
                // Use Summary.db entry to find the corresponding Index.db entry
                if let Some(_index_reader) = &self.index_reader {
                    // The summary entry provides an index offset, which points to Index.db data
                    // We need to read the partition data from the Data.db offset stored in Index.db

                    // For now, reconstruct the partition key from the summary entry
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
                                // Filter entries within the token range
                                for (row_key, value) in partition_entries {
                                    // TODO: Compute actual token for row_key and filter by range
                                    // For now, include all rows from partitions in range
                                    results.push((row_key, value));
                                }
                            }
                            None => {
                                debug!("Failed to parse partition at offset {}", data_offset);
                            }
                        }
                    }
                } else {
                    log::error!("Index reader not available for token range iteration");
                    return Err(Error::corruption(
                        "Index reader required for real token range iteration - synthetic data not allowed for Issue #35",
                    ));
                }
            }

            debug!("Token range iteration found {} entries", results.len());
            return Ok(results);
        }

        // Fallback to existing scan method
        self.sequential_scan(&TableId::from("default"), None, None, None)
            .await
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

    /// Get token coverage from Statistics.db reader
    pub async fn get_token_coverage(&self) -> Result<Option<(i64, i64)>> {
        if let Some(summary_reader) = &self.summary_reader {
            // Get token range from Summary.db instead of Statistics.db
            let summary_data = summary_reader.get_entries();
            if !summary_data.is_empty() {
                let min_token = summary_data
                    .first()
                    .ok_or_else(|| {
                        Error::corruption(
                            "Summary data is unexpectedly empty after non-empty check",
                        )
                    })?
                    .token;
                let max_token = summary_data
                    .last()
                    .ok_or_else(|| {
                        Error::corruption(
                            "Summary data is unexpectedly empty after non-empty check",
                        )
                    })?
                    .token;
                debug!(
                    "Retrieved token coverage from Summary.db: {} to {}",
                    min_token, max_token
                );
                return Ok(Some((min_token, max_token)));
            }
        }
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
