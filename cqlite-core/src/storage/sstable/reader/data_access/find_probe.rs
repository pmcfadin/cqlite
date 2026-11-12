//! Per-generation key probe for `cqlite find` (issue #4205, design D1/D2).
//!
//! Where [`SSTableReader::read_single_partition_for_compaction`] collapses "bloom said no" and
//! "the BTI trie said no" into one `DefinitelyAbsent` (and treats a bare BIG `Index.db` miss as
//! inconclusive), a DIAGNOSTIC must name which layer answered. This module composes the SAME
//! lower primitives — the bloom filter, `Index.db` / the BTI trie descent, the successor-offset
//! bound, and the partition seek-decode — directly, preserving that distinction. The fail-safe
//! scan reuses the production streaming compaction driver; it is NOT a second scan
//! implementation and never a byte-pattern search (#28).
//!
//! Read-only: no metric the production read path emits is bumped (the presence oracle's
//! `might_contain_partition` is deliberately not called, since it counts prunes and bloom
//! checks).

use super::super::compaction_row::{CompactionRow, CompactionRowData};
use super::super::SSTableReader;
use crate::storage::scan_cancel::ScanCancel;
use crate::types::RowKey;
use crate::Result;
use std::ops::ControlFlow;

/// How a partition's byte span is known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ByteLength {
    /// `successor_start - start`, both resolved from the index/trie.
    Known(u64),
    /// The last partition of the generation: its span runs to the end of `Data.db`.
    ToEof,
    /// The span could not be measured (a fail-safe-scan hit never resolves an end bound).
    Unmeasured(&'static str),
}

/// Location and timing detail of a partition found in one generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HitDetail {
    /// Uncompressed `Data.db` offset of the partition header; `None` only when a fail-safe
    /// scan found the partition without the driver reporting a boundary (never guessed).
    pub data_offset: Option<u64>,
    /// Span of the partition in `Data.db`.
    pub byte_length: ByteLength,
    /// Maximum timestamp among the partition's decoded rows, cells and deletions.
    pub max_writetime: i64,
    /// Whether the partition carries a partition-level deletion.
    pub has_partition_deletion: bool,
}

/// Result of the fail-safe scan used when no index layer could answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanOutcome {
    /// The scan found the key.
    Hit(HitDetail),
    /// The scan completed and the key is not in this generation.
    Miss,
}

/// What one generation said about one partition key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationProbe {
    /// The key is in this generation.
    Hit(HitDetail),
    /// BIG: the bloom filter reported the key definitely absent.
    BloomNegative,
    /// The index layer answered "absent": a BIG `Index.db` lookup with no entry (including
    /// when `Filter.db` is absent), or a BTI trie miss / prefix collision.
    IndexMiss,
    /// No index layer could answer (absent or unreadable); a full scan decided.
    Scanned(ScanOutcome),
}

/// Fold decoded rows into `(max_writetime, has_partition_deletion)`. `None` when the row set
/// carries no timestamp at all (an empty decode is not a hit and never reaches this).
fn summarize(rows: &[CompactionRow]) -> Option<(i64, bool)> {
    let mut max: Option<i64> = None;
    let mut bump = |t: i64| max = Some(max.map_or(t, |m| m.max(t)));
    let mut partition_deletion = false;
    for row in rows {
        bump(row.row_timestamp);
        match &row.row_data {
            CompactionRowData::PartitionDelete { deletion_time, .. } => {
                partition_deletion = true;
                bump(*deletion_time);
            }
            CompactionRowData::RangeMarker { deletion_time, .. }
            | CompactionRowData::Tombstone { deletion_time, .. } => bump(*deletion_time),
            CompactionRowData::Live {
                simple,
                complex,
                row_deletion,
                row_liveness,
            } => {
                simple.iter().for_each(|c| bump(c.timestamp));
                for col in complex {
                    if let Some((ts, _)) = col.complex_deletion {
                        bump(ts);
                    }
                    col.elements.iter().for_each(|e| bump(e.timestamp));
                }
                if let Some((ts, _)) = row_deletion {
                    bump(*ts);
                }
                if let Some(ts) = row_liveness.marker_timestamp {
                    bump(ts);
                }
            }
        }
    }
    max.map(|m| (m, partition_deletion))
}

impl SSTableReader {
    /// Probe this generation for `partition_key` (raw partition-key bytes), reporting WHICH
    /// layer answered. See the module doc and design D1.
    ///
    /// # Errors
    /// A cancelled scan, or a partition whose `Data.db` bytes fail to decode (the index and
    /// `Data.db` disagree). Both mean the generation could NOT be probed — the caller must
    /// report it with its cause, never as "absent".
    pub async fn probe_generation(
        &self,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
        scan_cancel: &ScanCancel,
    ) -> Result<GenerationProbe> {
        let is_bti = self.is_bti();
        if !is_bti {
            if let Some(bloom) = &self.bloom_filter {
                if !bloom.might_contain(partition_key) {
                    return Ok(GenerationProbe::BloomNegative);
                }
            }
        } else if self.partition_key_out_of_range(partition_key) {
            return Ok(GenerationProbe::IndexMiss);
        }
        if !self.has_partition_index() {
            return self
                .fail_safe_scan(partition_key, schema, scan_cancel)
                .await;
        }

        let offset = if is_bti {
            match self.lookup_partition_via_bti_trie(partition_key) {
                Ok(Some(off)) => off,
                Ok(None) => return Ok(GenerationProbe::IndexMiss),
                Err(_) => {
                    return self
                        .fail_safe_scan(partition_key, schema, scan_cancel)
                        .await
                }
            }
        } else {
            match self.lookup_partition_with_index(partition_key).await {
                Ok(Some((off, _))) => off,
                // Diagnostic fact: the index, taken at face value, has no entry (F1.3 — this
                // is what an absent Filter.db degrades to, never "unmeasured").
                Ok(None) => return Ok(GenerationProbe::IndexMiss),
                Err(_) => {
                    return self
                        .fail_safe_scan(partition_key, schema, scan_cancel)
                        .await
                }
            }
        };

        let end_bound = match self.successor_partition_offset(offset, partition_key).await {
            Ok(b) => b,
            Err(_) => {
                return self
                    .fail_safe_scan(partition_key, schema, scan_cancel)
                    .await
            }
        };

        match self
            .seek_partition_compaction_rows(
                offset,
                end_bound,
                partition_key,
                schema,
                is_bti,
                scan_cancel,
            )
            .await?
        {
            // BTI prefix collision (D1.1): the trie answered, it pointed at another key.
            Some(rows) if rows.is_empty() => Ok(GenerationProbe::IndexMiss),
            Some(rows) => match summarize(&rows) {
                Some((max_writetime, has_partition_deletion)) => {
                    Ok(GenerationProbe::Hit(HitDetail {
                        data_offset: Some(offset),
                        byte_length: match end_bound {
                            Some(end) => ByteLength::Known(end.saturating_sub(offset)),
                            None => ByteLength::ToEof,
                        },
                        max_writetime,
                        has_partition_deletion,
                    }))
                }
                None => {
                    self.fail_safe_scan(partition_key, schema, scan_cancel)
                        .await
                }
            },
            // Could not bound / BIG stale entry pointing at a foreign key: the index cannot be
            // trusted for this key — scan.
            None => {
                self.fail_safe_scan(partition_key, schema, scan_cancel)
                    .await
            }
        }
    }

    /// Full, schema-driven, key-filtered scan of `Data.db` via the production streaming
    /// compaction driver (the same one the point-read `NeedsScan` fallback drives).
    async fn fail_safe_scan(
        &self,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
        scan_cancel: &ScanCancel,
    ) -> Result<GenerationProbe> {
        use super::partition_boundaries::PartitionBoundaryObserver;
        let mut rows: Vec<CompactionRow> = Vec::new();
        let mut offset: Option<u64> = None;
        {
            let mut sink = |start: u64, key: &RowKey| -> Result<()> {
                if offset.is_none() && key.as_bytes() == partition_key {
                    offset = Some(start);
                }
                Ok(())
            };
            self.stream_all_partitions_for_compaction_observed(
                schema,
                scan_cancel,
                PartitionBoundaryObserver::reporting(&mut sink),
                |row: CompactionRow| {
                    if row.key.as_bytes() == partition_key {
                        rows.push(row);
                    }
                    Ok(ControlFlow::Continue(()))
                },
            )
            .await?;
        }
        Ok(GenerationProbe::Scanned(match summarize(&rows) {
            None => ScanOutcome::Miss,
            Some((max_writetime, has_partition_deletion)) => ScanOutcome::Hit(HitDetail {
                data_offset: offset,
                byte_length: ByteLength::Unmeasured("fail-safe scan resolves no end bound"),
                max_writetime,
                has_partition_deletion,
            }),
        }))
    }
}
