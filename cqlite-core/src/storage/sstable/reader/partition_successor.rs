//! Next-partition (successor) offset resolution for the single-partition seek
//! window (issue #953 / #951; O(depth) local walk, issue #2058).
//!
//! The within-SSTable single-partition seek bounds its decompression window to
//! exactly one partition's byte extent `[target_offset, successor_offset)` using
//! authoritative index/trie layout metadata (never a heuristic boundary scan).
//! Both helpers are gated `#[cfg(not(feature = "tombstones"))]` like the seek path
//! they serve: their callers are `scan_single_partition_clustering`,
//! `read_single_partition_for_compaction`, and `big_reverse_partition_rows`, which
//! the `tombstones` build compiles out (it serves single-partition reads via a full
//! scan + filter, not a seek).
//!
//! Split out of `partition_lookup.rs` (campsite / epic #1116) so the B4 key-cache
//! wiring (issue #1570) does not push that file over the source-size threshold.

#[cfg(not(feature = "tombstones"))]
use super::SSTableReader;
#[cfg(not(feature = "tombstones"))]
use crate::{Error, Result};

#[cfg(not(feature = "tombstones"))]
impl SSTableReader {
    /// Authoritatively resolve the UNCOMPRESSED `Data.db` offset of the partition
    /// that immediately FOLLOWS the partition starting at `target_offset` (whose
    /// partition key is `partition_key`), used to bound the within-SSTable seek's
    /// decompression window to exactly one partition's byte extent (issue #953 / #951
    /// MEDIUM).
    ///
    /// The successor's start offset is the partition's exclusive END: a partition
    /// occupies `[target_offset, successor_offset)`, so decompressing the chunks
    /// covering that half-open range materializes every byte of the target
    /// partition (including a row/cell that spans multiple compression chunks)
    /// without reading any of the next partition. This is authoritative metadata
    /// (the index/trie's own partition layout), NOT a heuristic boundary scan.
    ///
    /// Returns:
    /// - `Ok(Some(off))` — the next partition's start offset (`off > target_offset`).
    /// - `Ok(None)` — `target_offset` is the LAST partition (no successor); the
    ///   caller bounds the end with the authoritative data-section length.
    ///
    /// Resolution is per index format:
    /// - **BTI (`da`)** — the successor is resolved by a SINGLE O(depth) strict-ceiling
    ///   walk of the `Partitions.db` trie keyed on `partition_key`'s byte-comparable
    ///   token ([`partition_successor_in_bti_slice`]). Because the trie stores
    ///   partitions in byte-comparable key order — which for `Murmur3Partitioner`
    ///   equals token order equals `Data.db` layout order — the trie IN-ORDER
    ///   successor is exactly the OFFSET successor (the smallest partition start
    ///   strictly greater than `target_offset`). This replaces the pre-#2058
    ///   whole-trie DFS that enumerated + sorted EVERY partition offset into a
    ///   `OnceLock` array on the first seek. A defensive `> target_offset` guard
    ///   fails safe to `None` (data-section-length bound, a safe over-read that never
    ///   truncates) should a resolved successor not exceed the target — a case the
    ///   real-fixture oracle test proves does not occur (`tests/issue_2058_*`).
    /// - **BIG (`nb`)** — `Index.db` `partition_entries` are sorted by key (==
    ///   `Data.db` order); the successor is the smallest `data_offset` strictly
    ///   greater than `target_offset`.
    ///
    /// [`partition_successor_in_bti_slice`]: crate::storage::sstable::bti::partition_successor_in_bti_slice
    pub(crate) async fn successor_partition_offset(
        &self,
        target_offset: u64,
        partition_key: &[u8],
    ) -> Result<Option<u64>> {
        if self.bti_partitions_db.is_some() {
            return self.bti_successor_partition_offset(target_offset, partition_key);
        }

        // BIG (`nb`): scan the sorted Index.db entries for the smallest data_offset
        // strictly greater than target_offset. `partition_entries` are emitted in
        // key (== Data.db) order, but we take the min over `> target` defensively
        // rather than rely on positional adjacency.
        if let Some(index_reader) = &self.index_reader {
            // Issue #2412 Stage 2: a lazily-opened reader defers the full parse to
            // first use — this successor scan IS that first use. No-op for an
            // eagerly-opened reader.
            index_reader.ensure_materialized(&self.scan_cancel).await?;
            let successor = index_reader
                .get_partition_entries()
                .iter()
                .map(|e| e.data_offset)
                .filter(|&o| o > target_offset)
                .min();
            return Ok(successor);
        }

        // No index available: cannot resolve a successor authoritatively.
        Ok(None)
    }

    /// BTI (`da`) next-partition successor via the O(depth) local strict-ceiling walk
    /// (issue #2058), resolving a WIDE successor's `RowsOffset` through `Rows.db`.
    ///
    /// The walk keys on `partition_key`'s byte-comparable token, so the successor it
    /// returns is byte-identical to the pre-#2058 whole-trie DFS + sorted-offset
    /// `partition_point(<= target_offset)`, for every partition (proven by the
    /// real-fixture oracle test). No shared state / `OnceLock`, so concurrent point
    /// reads on the same reader never race or double-walk.
    fn bti_successor_partition_offset(
        &self,
        target_offset: u64,
        partition_key: &[u8],
    ) -> Result<Option<u64>> {
        use crate::storage::sstable::bti::{
            encode_partition_key_for_bti_trie_uncounted, partition_successor_in_bti_slice,
            resolve_rows_db_entry_uncounted, BtiPartitionLocation,
        };

        let Some(partitions_db) = &self.bti_partitions_db else {
            return Ok(None);
        };

        // Encode WITHOUT the C4 `KEY_HASH_CALLS` counter: a point read already hashed
        // this exact key once (the candidate prune), and the one-hash-per-read
        // invariant (issue #1575) must hold — the successor bound is not a new query
        // key, it re-encodes the same one.
        let encoded = encode_partition_key_for_bti_trie_uncounted(partition_key);

        let location = partition_successor_in_bti_slice(partitions_db.as_slice(), &encoded)
            .map_err(|e| {
                Error::corruption(format!(
                    "BTI Partitions.db next-partition successor walk failed while resolving the \
                     seek end bound (key len={}): {e}",
                    partition_key.len()
                ))
            })?;

        let successor_offset = match location {
            // Narrow successor: its Data.db start is the target's exclusive end.
            Some(BtiPartitionLocation::DataOffset(off)) => Some(off),
            // Wide successor: recover its Data.db start (`data_position`) via Rows.db.
            // Uncounted so the L1 clustering-window `ROWS_DB_ENTRY_RESOLVES == 1`
            // invariant (issue #1647) is not perturbed by this seek-bound resolve.
            Some(BtiPartitionLocation::RowsOffset(rows_offset)) => {
                let rows_db = self.bti_rows_db.as_ref().ok_or_else(|| {
                    Error::corruption(format!(
                        "BTI successor walk returned RowsOffset({rows_offset}) but this reader has \
                         no Rows.db; the SSTable is structurally invalid (Rows.db is required for \
                         wide partitions)."
                    ))
                })?;
                let header =
                    resolve_rows_db_entry_uncounted(rows_db.as_slice(), rows_offset as usize)
                        .map_err(|e| {
                            Error::corruption(format!(
                        "BTI Rows.db row-index entry at RowsOffset({rows_offset}) is unreadable \
                             while resolving the next-partition seek bound: {e}"
                    ))
                        })?;
                Some(header.data_position)
            }
            // No successor: `partition_key` is the LAST partition.
            None => None,
        };

        // Fail-safe (never a truncating bound): the resolved successor MUST start
        // strictly after the target partition. If it does not (a pathological trie /
        // token-collision shape the oracle test proves never occurs on real data),
        // return `None` so the caller bounds the end with the authoritative
        // data-section length — a safe over-read, never a truncation.
        Ok(successor_offset.filter(|&off| off > target_offset))
    }
}

#[cfg(not(feature = "tombstones"))]
impl SSTableReader {
    /// The authoritative UNCOMPRESSED length of this SSTable's `Data.db` data
    /// section — the exclusive end bound the LAST partition takes when
    /// [`successor_partition_offset`](Self::successor_partition_offset) returns
    /// `None` (there is no successor).
    ///
    /// The source depends on whether the table is compressed, and the two are NOT
    /// interchangeable:
    ///
    /// - **Compressed**: `CompressionInfo.db`'s `data_length` field, which Cassandra
    ///   writes as the total UNCOMPRESSED data length. This is the ONLY source for a
    ///   compressed table. If it is absent or zero the answer is `None` — falling
    ///   back to the file length would silently substitute the **compressed** size,
    ///   producing a too-small extent that would then be published as a *measured*
    ///   number. A missing measurement must read as missing.
    /// - **Uncompressed**: the `Data.db` file length. For an uncompressed SSTable the
    ///   file IS the data section, so its length is the uncompressed length — and the
    ///   production write surface is uncompressed-only (#1406).
    ///
    /// `None` when no authoritative length exists, in which case the caller fails
    /// closed and reports `size_source = unavailable` rather than guessing one.
    pub(crate) fn uncompressed_data_section_len(&self) -> Option<u64> {
        match self.compression_info.as_ref() {
            // Compressed: `data_length` or nothing. NEVER the file length.
            Some(info) => (info.data_length > 0).then_some(info.data_length),
            None => {
                let len = self.point_source.len();
                (len > 0).then_some(len)
            }
        }
    }

    /// MEASURE this partition's on-disk extent as the successor gap.
    ///
    /// Returns `Ok(Some(bytes))` when the extent is authoritative:
    /// `successor_offset - data_offset`, or
    /// `uncompressed_data_section_len() - data_offset` for the last partition.
    /// Returns `Ok(None)` when it is genuinely unknowable (no index, or no
    /// data-section length) — the caller then records `unavailable` and contributes
    /// zero bytes rather than estimating (no-heuristics, #28).
    ///
    /// This is the same authoritative index-layout metadata the single-partition
    /// seek uses to bound its decompression window, read at the same reader-level
    /// granularity as the B4 key-offset cache — so a caller at the LOGICAL
    /// point-read boundary can obtain a byte weight without counting per-SSTable
    /// probes (issue #2827, design D2/D6).
    ///
    /// The extent is in UNCOMPRESSED offsets, which is exactly the domain a
    /// decoded-size multiplier is applied to.
    pub(crate) async fn measure_partition_extent(
        &self,
        data_offset: u64,
        partition_key: &[u8],
    ) -> Result<Option<u64>> {
        let end = match self
            .successor_partition_offset(data_offset, partition_key)
            .await?
        {
            Some(successor) => successor,
            None => match self.uncompressed_data_section_len() {
                Some(len) => len,
                None => return Ok(None),
            },
        };
        Ok(end.checked_sub(data_offset).filter(|&gap| gap > 0))
    }
}
