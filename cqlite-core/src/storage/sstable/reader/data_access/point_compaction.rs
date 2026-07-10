//! Single-partition compaction seek (issue #2207, Stage 1).
//!
//! The unwired point-read machinery — [`might_contain_partition`] (presence
//! oracle), [`lookup_partition_via_bti_trie`] (BTI `da`) / [`lookup_partition_with_index`]
//! (BIG `nb` Summary/Index) — resolves whether a partition is present and, if so,
//! WHERE. This module composes those into the one public surface the Flight
//! producer needs: given a partition key, return the target partition's rows in
//! the **exact same [`CompactionRow`] form the full-scan compaction stream
//! produces** (tombstones preserved, per-row timestamps), so the k-way merge
//! reconciles the point path byte-identically to the scan path.
//!
//! Pruning is fail-open toward reading (the correctness spine): a candidate is
//! reported [`SinglePartitionCompaction::DefinitelyAbsent`] ONLY on an exact
//! presence-oracle negative; a missing/ambiguous index degrades to
//! [`SinglePartitionCompaction::IndexUnavailable`] (the caller scans that one
//! SSTable), never a wrong or silently-skipped answer (issues #28, #2295).
//!
//! [`might_contain_partition`]: SSTableReader::might_contain_partition
//! [`lookup_partition_via_bti_trie`]: SSTableReader::lookup_partition_via_bti_trie
//! [`lookup_partition_with_index`]: SSTableReader::lookup_partition_with_index

use super::super::compaction_row::CompactionRow;
use super::super::SSTableReader;
use crate::Result;
use std::ops::ControlFlow;

/// Outcome of a single-partition candidate probe against one SSTable.
#[derive(Debug)]
pub enum SinglePartitionCompaction {
    /// The presence oracle proved the key is definitively absent from this
    /// SSTable (an exact bloom negative, or a BTI trie miss). The candidate is
    /// pruned; `cqlite.read.sstables_pruned` was already incremented by the
    /// oracle. Never returned when presence is positive, unknown, or the index is
    /// missing.
    DefinitelyAbsent,
    /// The key MIGHT be present but this SSTable has no usable random-access index
    /// (Summary/Index absent, a #1572-style inconclusive index miss, or a last
    /// partition whose extent cannot be bounded authoritatively). The caller MUST
    /// read this SSTable — scanning its partitions and filtering to the key —
    /// never skip it. The missing index degrades speed, never correctness (#2295).
    IndexUnavailable,
    /// The partition was seeked and decoded. `rows` are its compaction rows
    /// (tombstones preserved), byte-identical to the full-scan stream restricted
    /// to this partition. Empty when the resolved candidate was a prefix-collision
    /// for an absent key (authoritative empty — do NOT fall back).
    Rows(Vec<CompactionRow>),
}

impl SSTableReader {
    /// Probe one SSTable for a single partition, returning its compaction rows via
    /// an authoritative seek — or a prune / scan-fallback signal (issue #2207).
    ///
    /// This is the public core primitive the Flight point-read path composes into
    /// its existing k-way merge. Correctness spine (fail-open toward reading):
    ///
    /// 1. [`might_contain_partition`](Self::might_contain_partition) reports a
    ///    definite negative → [`SinglePartitionCompaction::DefinitelyAbsent`]
    ///    (pruned + counted).
    /// 2. No random-access index → [`SinglePartitionCompaction::IndexUnavailable`]
    ///    (the caller scans this SSTable).
    /// 3. Otherwise resolve the partition's uncompressed offset (BTI trie / BIG
    ///    Index.db) and its authoritative end bound (successor offset / data
    ///    length), materialize ONLY the covering chunk window, and parse the one
    ///    partition with the SAME compaction parser the full scan uses →
    ///    [`SinglePartitionCompaction::Rows`]. A BIG Index.db miss (inconclusive,
    ///    #1572) or an un-boundable last partition degrades to
    ///    [`SinglePartitionCompaction::IndexUnavailable`].
    ///
    /// `partition_key` is the raw partition-key bytes (as
    /// `PartitionKey::to_bytes` produces). `schema` is the authoritative table
    /// schema (the parser needs column names).
    pub async fn read_single_partition_for_compaction(
        &self,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<SinglePartitionCompaction> {
        // 1. Presence oracle — the only source of a definite prune. Emits
        //    `cqlite.read.sstables_pruned` internally on a definite negative.
        if !self.might_contain_partition(partition_key) {
            return Ok(SinglePartitionCompaction::DefinitelyAbsent);
        }

        // 2. No random-access index (Data.db-only snapshot, #2295) → the caller
        //    must scan this SSTable. Never skip a candidate that might hold the key.
        if !self.has_partition_index() {
            return Ok(SinglePartitionCompaction::IndexUnavailable);
        }

        // 3. Resolve the target partition's UNCOMPRESSED Data.db start offset.
        let is_bti = self.is_bti();
        let offset = if is_bti {
            match self.lookup_partition_via_bti_trie(partition_key)? {
                Some(off) => off,
                // A BTI trie miss is AUTHORITATIVE absence (the oracle above should
                // have pruned already; this is a defensive authoritative-empty).
                None => return Ok(SinglePartitionCompaction::Rows(Vec::new())),
            }
        } else {
            match self.lookup_partition_with_index(partition_key).await? {
                Some((off, _size)) => off,
                // A BIG Index.db miss is NOT a definitive absent (#1572): a
                // truncated/partial map can drop an entry for a present partition.
                // Degrade to a full scan of this SSTable, never a wrong empty.
                None => return Ok(SinglePartitionCompaction::IndexUnavailable),
            }
        };

        // 4. Authoritative exclusive end of the target partition: the successor
        //    partition's start (next trie/index entry), or `None` for the last.
        let end_bound = self.successor_partition_offset(offset)?;

        // 5. Materialize `[offset, end)` decompressed and parse the one partition.
        match self
            .seek_partition_compaction_rows(offset, end_bound, partition_key, schema)
            .await?
        {
            Some(rows) => Ok(SinglePartitionCompaction::Rows(rows)),
            // Could not bound the (last) partition authoritatively — fall back to a
            // full scan of this SSTable for correctness (#953 mandate).
            None => Ok(SinglePartitionCompaction::IndexUnavailable),
        }
    }

    /// Materialize the decompressed window covering `[offset, end)` and parse the
    /// single partition that starts at `offset`, collecting its compaction rows.
    ///
    /// Returns `Ok(None)` when the last partition cannot be bounded authoritatively
    /// (no successor and no usable data length) — the caller falls back to a scan.
    /// The parse uses the SAME `build_v5_parser(false)` +
    /// `parse_one_partition_for_compaction` the full-scan compaction stream uses,
    /// so the rows are byte-identical to that stream restricted to this partition.
    async fn seek_partition_compaction_rows(
        &self,
        offset: u64,
        end_bound: Option<u64>,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Option<Vec<CompactionRow>>> {
        let offset = offset as usize;
        let owned_schema = schema.cloned().or_else(|| self.get_table_schema(None));
        let parser = self.build_v5_parser(false);

        // Chunk length drives the two window strategies. Absent/zero → uncompressed
        // (the WriteEngine's `nb` output, and uncompressed BTI): read the whole
        // section once and parse from `offset`. Present → compressed: pull only the
        // chunks covering `[offset, end)`.
        let chunk_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.chunk_length as usize)
            .filter(|&len| len > 0);

        let (window, within) = match chunk_length {
            None => {
                let whole = self.point_read_whole_section().await?;
                (whole, offset)
            }
            Some(len) => {
                let target_chunk = offset / len;
                let window_base = target_chunk * len;
                let within = offset - window_base;
                // Authoritative exclusive end in the uncompressed domain.
                let end = match end_bound {
                    Some(e) => e as usize,
                    None => match self
                        .compression_info
                        .as_ref()
                        .map(|ci| ci.data_length as usize)
                        .filter(|&len| len > offset)
                    {
                        Some(len) => len,
                        // Last partition, unknown length → cannot bound → scan.
                        None => return Ok(None),
                    },
                };
                let window = self
                    .pull_chunk_window(target_chunk, window_base, end)
                    .await?;
                (window, within)
            }
        };

        if within >= window.len() {
            // Nothing decodable at the resolved offset — treat as absent here.
            return Ok(Some(Vec::new()));
        }

        // Parse the FIRST partition at `window[within..]`. Collect every row whose
        // decoded key equals the queried key; a decoded DIFFERENT key means the
        // resolved candidate was a prefix-collision for an absent key.
        let mut rows: Vec<CompactionRow> = Vec::new();
        let mut saw_foreign_key = false;
        parser.parse_one_partition_for_compaction(
            &window[within..],
            owned_schema.as_ref(),
            self,
            true,
            &mut |row: CompactionRow| {
                if row.key.as_bytes() == partition_key {
                    rows.push(row);
                } else {
                    saw_foreign_key = true;
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;

        if saw_foreign_key && rows.is_empty() {
            // Prefix-collision candidate for an absent key: authoritative empty.
            return Ok(Some(Vec::new()));
        }
        Ok(Some(rows))
    }

    /// Pull the decompressed chunks covering `[window_base, end)` starting at
    /// `target_chunk`, returning the concatenated bytes. Never stitches to EOF: it
    /// stops as soon as the window reaches `end` (the target partition's exclusive
    /// end) or the SSTable is exhausted. Mirrors the bounded chunk fetch the
    /// user-facing single-partition seek uses (issue #953), so a head-of-file
    /// point read never decompresses the whole `Data.db`.
    async fn pull_chunk_window(
        &self,
        target_chunk: usize,
        window_base: usize,
        end: usize,
    ) -> Result<Vec<u8>> {
        use super::super::chunk_source::ChunkSource;
        use crate::storage::sstable::compression::Compression;

        let compression_opt = self
            .compression_reader
            .as_ref()
            .map(|cr| Compression::new(*cr.algorithm()))
            .transpose()?;
        let comp_info = self.compression_info.as_deref().ok_or_else(|| {
            crate::Error::corruption(
                "point-read chunk-targeted path requires CompressionInfo but it is absent",
            )
        })?;
        let chunk_source = ChunkSource::new(
            self.point_source.as_ref(),
            comp_info,
            compression_opt.as_ref(),
            &self.chunk_cache,
            self.stats.file_size,
            0, // NB/BTI: chunk offsets are absolute from Data.db byte 0.
            super::NS_BTI_CHUNK,
            self.chunk_cache_id,
        );

        let mut window: Vec<u8> = Vec::new();
        let mut chunk_index = target_chunk;
        while window_base + window.len() < end {
            match chunk_source.chunk(chunk_index)? {
                Some(decompressed) => {
                    chunk_index += 1;
                    window.extend_from_slice(&decompressed);
                }
                None => break, // EOF before `end`: parse what we have.
            }
        }
        Ok(window)
    }
}
