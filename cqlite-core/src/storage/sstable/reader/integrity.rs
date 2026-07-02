//! Integrity checking and health monitoring methods for SSTableReader
//!
//! This module contains methods for checking SSTable integrity, monitoring health,
//! and handling tombstone filtering.

use super::super::verify::{self, VerifyMode};
use super::{IntegrityCheckResult, IntegrityStatus, SSTableReader, SSTableReaderHealthMetrics};
use crate::types::{ScanRow, Value};
use crate::{Error, Result};

use log::{debug, info};

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

    /// Perform an integrity check on the SSTable.
    ///
    /// Issue #1283: this is a THIN PROJECTION over `verify::verify_sstable` — the
    /// single source of truth for SSTable integrity — not an independent check
    /// pipeline. The legacy implementation walked only `Data.db` blocks, so a
    /// corrupt `Index.db` / `Digest.crc32` / `Summary.db` / `Filter.db` or
    /// out-of-order keys (all of which the verifier FAILs) read back `Healthy`
    /// here — a divergent verdict. We now run the authoritative verifier in
    /// `Full` mode over the same SSTable directory and map its `VerifyReport` onto
    /// the legacy `IntegrityCheckResult` shape the (test-only) consumers expect.
    pub async fn perform_integrity_check(&self) -> Result<IntegrityCheckResult> {
        debug!("Starting integrity check for {:?}", self.file_path);

        // The verifier operates on the SSTable's directory (it resolves the
        // generation's components); derive it from the open reader's Data.db path.
        let dir = self.file_path.parent().ok_or_else(|| {
            Error::corruption(format!(
                "SSTable path {:?} has no parent directory to verify",
                self.file_path
            ))
        })?;

        // Delegate to the authoritative engine using the same Config/Platform the
        // reader was opened with. Data corruption is reported as findings inside
        // an Ok(report); only environmental problems return Err.
        let report = verify::verify_sstable(
            dir,
            VerifyMode::Full,
            &self.open_config,
            self.platform.clone(),
        )
        .await?;

        // Project VerifyReport -> IntegrityCheckResult.
        //  - any finding  => Corrupted; none => Healthy (no Degraded — issue #1283).
        //  - rows_scanned => total_entries.
        //  - findings' rendered strings => parsing_errors.
        //  - per-block indices (corrupted_blocks/unreadable_blocks/total_blocks_checked)
        //    are not produced by the verifier and no production consumer reads them,
        //    so they stay best-effort empty/zero.
        let parsing_errors: Vec<String> = report.findings.iter().map(|f| f.to_string()).collect();
        let overall_status = if report.findings.is_empty() {
            IntegrityStatus::Healthy
        } else {
            IntegrityStatus::Corrupted
        };

        let result = IntegrityCheckResult {
            file_path: self.file_path.clone(),
            total_blocks_checked: 0,
            corrupted_blocks: Vec::new(),
            unreadable_blocks: 0,
            total_entries: report.rows_scanned.unwrap_or(0),
            parsing_errors,
            overall_status,
        };

        info!(
            "Integrity check completed for {:?}: {:?}, {} findings, {} rows scanned",
            self.file_path,
            result.overall_status,
            result.parsing_errors.len(),
            result.total_entries
        );

        Ok(result)
    }

    /// Enhanced tombstone filtering using TombstoneMerger
    #[cfg(feature = "tombstones")]
    pub(super) fn filter_tombstone(&self, row: &ScanRow) -> bool {
        // Issue #1334: a live row (`ScanRow::Row`) is always kept here — row-level
        // tombstone suppression only applies to markers. (Cell tombstones inside a
        // live row are preserved for callers to inspect.) Only a marker carries a
        // `Value` whose tombstone/TTL semantics this filter evaluates.
        let value = match row {
            // A live row (decoded or raw undecoded fallback) is always kept.
            ScanRow::Row(_) | ScanRow::RawRow(_) => return true,
            ScanRow::Marker(v) => v,
        };
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
    pub(super) fn filter_tombstone(&self, row: &ScanRow) -> bool {
        use crate::types::TombstoneType;
        // Issue #1334: a live row (`ScanRow::Row`) is always kept; filter out only a
        // row-level tombstone marker.
        !matches!(
            row,
            ScanRow::Marker(Value::Tombstone(info))
                if info.tombstone_type == TombstoneType::RowTombstone
        )
    }

    /// Enhanced multi-generation tombstone filtering for compaction
    #[cfg(feature = "tombstones")]
    pub async fn filter_with_multi_generation_merge(
        &self,
        table_id: &TableId,
        entries: Vec<(RowKey, Vec<GenerationValue>)>,
    ) -> Result<Vec<(RowKey, ScanRow)>> {
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
    fn should_include_value_after_merge(
        &self,
        row: &ScanRow,
        _table_id: &TableId,
        _key: &RowKey,
    ) -> Result<bool> {
        // Issue #1334: the merge now yields whole rows. A live row with at least one
        // cell is included; a marker (row tombstone / null row) or an empty row is
        // suppressed.
        match row {
            ScanRow::Row(cells) => Ok(!cells.is_empty()),
            // A raw undecoded fallback row carries live bytes → included.
            ScanRow::RawRow(bytes) => Ok(!bytes.is_empty()),
            ScanRow::Marker(_) => Ok(false),
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
