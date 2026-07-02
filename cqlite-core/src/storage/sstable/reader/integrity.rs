//! Integrity checking and health monitoring methods for SSTableReader
//!
//! This module contains methods for checking SSTable integrity, monitoring health,
//! and handling tombstone filtering.

use super::{IntegrityCheckResult, IntegrityStatus, SSTableReader, SSTableReaderHealthMetrics};
use crate::types::Value;
use crate::Result;

use log::{debug, info};
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;

#[cfg(feature = "tombstones")]
use super::super::tombstone_merger::GenerationValue;

#[cfg(feature = "tombstones")]
use crate::{types::TableId, RowKey};

#[cfg(feature = "tombstones")]
use log::warn;

impl SSTableReader {
    /// Get comprehensive reader health and performance metrics
    pub async fn get_health_metrics(&self) -> Result<SSTableReaderHealthMetrics> {
        let stats = self.stats().await?;

        // Calculate actual cache hit rate from atomic counters
        let cache_hit_rate = self.calculate_cache_hit_rate();

        let memory_usage = self.estimate_memory_usage();

        Ok(SSTableReaderHealthMetrics {
            file_path: self.file_path.clone(),
            file_accessible: self.file_path.exists(),
            header_version: self.header.cassandra_version,
            total_file_size: stats.file_size,
            estimated_memory_usage: memory_usage,
            block_cache_entries: self.block_cache.len(),
            block_cache_hit_rate: cache_hit_rate,
            compression_enabled: self.compression_reader.is_some(),
            compression_algorithm: self.header.compression.algorithm.clone(),
            bloom_filter_enabled: self.bloom_filter.is_some(),
            index_available: self.index.is_some(),
            generation: self.generation,
            last_error: None,
        })
    }

    /// Perform integrity check on the SSTable file
    pub async fn perform_integrity_check(&self) -> Result<IntegrityCheckResult> {
        debug!("Starting integrity check for {:?}", self.file_path);

        let mut result = IntegrityCheckResult {
            file_path: self.file_path.clone(),
            total_blocks_checked: 0,
            corrupted_blocks: Vec::new(),
            checksum_mismatches: 0,
            unreadable_blocks: 0,
            total_entries: 0,
            parsing_errors: Vec::new(),
            overall_status: IntegrityStatus::Healthy,
        };

        // Read through a private per-scan cursor (issue #815) so the integrity
        // check never mutates the shared point-read cursor and can run alongside
        // concurrent reads.
        let cursor = self.new_scan_cursor().await?;
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }

        // Check each block. Match on `read_next_block` explicitly rather than
        // `.ok().flatten()` (issue #1396/#1283): a non-recoverable read error —
        // e.g. an uncompressed `CRC.db` chunk mismatch now surfaced by the read
        // path — must be RECORDED as corruption, not silently swallowed into a
        // clean end-of-stream that leaves the file reported Healthy.
        loop {
            let block_data = match self.read_next_block(&cursor).await {
                Ok(Some(block)) => block,
                Ok(None) => break, // clean end of the data section
                Err(e) => {
                    // The next block could not be read/verified: the file is
                    // corrupt from here on. Record it and stop — subsequent
                    // reads would fail the same way.
                    let block_no = result.total_blocks_checked + 1;
                    result.unreadable_blocks += 1;
                    result
                        .parsing_errors
                        .push(format!("Block {}: {}", block_no, e));
                    result.corrupted_blocks.push(block_no);
                    break;
                }
            };
            result.total_blocks_checked += 1;

            // Try to parse block entries
            match self.parse_block_entries(&block_data, None) {
                Ok(entries) => {
                    result.total_entries += entries.len();
                }
                Err(e) => {
                    result
                        .parsing_errors
                        .push(format!("Block {}: {}", result.total_blocks_checked, e));
                    result.corrupted_blocks.push(result.total_blocks_checked);
                }
            }

            // Yield control periodically
            if result.total_blocks_checked % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }

        // Determine overall status
        result.overall_status =
            if !result.corrupted_blocks.is_empty() || !result.parsing_errors.is_empty() {
                IntegrityStatus::Corrupted
            } else if result.checksum_mismatches > 0 {
                IntegrityStatus::Degraded
            } else {
                IntegrityStatus::Healthy
            };

        info!(
            "Integrity check completed for {:?}: {:?}, {} blocks checked, {} entries",
            self.file_path,
            result.overall_status,
            result.total_blocks_checked,
            result.total_entries
        );

        Ok(result)
    }

    /// Enhanced tombstone filtering using TombstoneMerger
    #[cfg(feature = "tombstones")]
    pub(super) fn filter_tombstone(&self, value: &Value) -> bool {
        // Use the fast tombstone check for performance
        let write_time = self.extract_write_time_from_value(value);

        if self
            .tombstone_merger
            .fast_tombstone_check(value, write_time)
        {
            // Value is deleted by tombstone
            return false;
        }

        // Check for TTL expiration on regular values
        if let Some(ttl) = self.extract_ttl_from_value(value) {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as i64)
                .unwrap_or_else(|e| {
                    warn!("Failed to get system time: {}; using fallback value 0", e);
                    0
                });

            if current_time > write_time + ttl {
                // Value has expired
                return false;
            }
        }

        true // Keep valid, non-deleted values
    }

    /// Simple tombstone filtering (fallback when tombstones feature is disabled).
    ///
    /// Row tombstones (`Value::Tombstone(RowTombstone)`) are always filtered out of
    /// user-facing scan/get results, regardless of the `tombstones` feature flag.
    /// This prevents deleted rows that are still present on disk (either from a live
    /// SSTable that contains a tombstone entry, or from a post-compaction SSTable
    /// that preserved tombstone rows for GC purposes) from appearing in query results.
    ///
    /// Cell tombstones (`Value::Tombstone(CellTombstone)`) within a Map are NOT
    /// filtered here — they are preserved so callers can inspect them.  If a caller
    /// needs to suppress null-cell entries, it should do so at the query layer.
    ///
    /// (Issue #505)
    #[cfg(not(feature = "tombstones"))]
    pub(super) fn filter_tombstone(&self, value: &Value) -> bool {
        use crate::types::TombstoneType;
        // Filter out row-level tombstones; keep everything else.
        !matches!(
            value,
            Value::Tombstone(info)
                if info.tombstone_type == TombstoneType::RowTombstone
        )
    }

    /// Enhanced multi-generation tombstone filtering for compaction
    #[cfg(feature = "tombstones")]
    pub async fn filter_with_multi_generation_merge(
        &self,
        table_id: &TableId,
        entries: Vec<(RowKey, Vec<GenerationValue>)>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let mut results = Vec::new();

        log::debug!(
            "Processing {} key groups for multi-generation merge",
            entries.len()
        );

        // Use batch processing for better performance
        const BATCH_SIZE: usize = 1000;

        let batches: Vec<_> = entries.chunks(BATCH_SIZE).collect();

        for (batch_idx, batch) in batches.iter().enumerate() {
            log::debug!(
                "Processing batch {}/{} with {} entries",
                batch_idx + 1,
                batches.len(),
                batch.len()
            );

            let batch_entries = batch.to_vec();
            let merged_results = self
                .tombstone_merger
                .batch_merge_with_tombstones(batch_entries, BATCH_SIZE)?;

            for (key, merged_value) in merged_results {
                if let Some(value) = merged_value {
                    if self.should_include_value_after_merge(&value, table_id, &key)? {
                        results.push((key, value));
                    }
                } else {
                    // Value was completely tombstoned
                    log::debug!("Value for key {:?} was completely tombstoned", key);
                }
            }
        }

        log::debug!(
            "Multi-generation merge completed: {} final results from {} input groups",
            results.len(),
            entries.len()
        );

        Ok(results)
    }

    /// Enhanced filtering logic for post-merge values including collection validation
    #[cfg(feature = "tombstones")]
    #[allow(clippy::only_used_in_recursion)]
    fn should_include_value_after_merge(
        &self,
        value: &Value,
        _table_id: &TableId,
        _key: &RowKey,
    ) -> Result<bool> {
        match value {
            // Skip null values
            Value::Null => Ok(false),

            // For collections, check if they have valid content
            Value::List(list) => Ok(!list.is_empty()),
            Value::Set(set) => Ok(!set.is_empty()),
            Value::Map(map) => Ok(!map.is_empty()),

            // For UDTs, check if they have non-null fields
            Value::Udt(udt) => {
                let has_non_null_fields = udt.fields.iter().any(|field| field.value.is_some());
                Ok(has_non_null_fields)
            }

            // For frozen values, recursively check the inner value
            Value::Frozen(boxed_value) => {
                self.should_include_value_after_merge(boxed_value, _table_id, _key)
            }

            // Tombstones should not be included in final results
            Value::Tombstone(_) => Ok(false),

            // All other value types are included
            _ => Ok(true),
        }
    }

    /// Extract TTL from value metadata
    #[cfg(feature = "tombstones")]
    fn extract_ttl_from_value(&self, value: &Value) -> Option<i64> {
        match value {
            Value::Tombstone(info) => info.ttl,
            _ => None, // Regular values would have TTL in SSTable metadata
        }
    }

    /// Extract write time from value
    #[cfg(feature = "tombstones")]
    fn extract_write_time_from_value(&self, value: &Value) -> i64 {
        match value {
            Value::Tombstone(info) => info.deletion_time,
            _ => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as i64)
                .unwrap_or_else(|e| {
                    warn!("Failed to get system time: {}; using fallback value 0", e);
                    0
                }),
        }
    }
}
