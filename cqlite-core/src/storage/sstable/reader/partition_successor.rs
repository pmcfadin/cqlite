//! Next-partition (successor) offset resolution for the single-partition seek
//! window (issue #953 / #951).
//!
//! The within-SSTable single-partition seek bounds its decompression window to
//! exactly one partition's byte extent `[target_offset, successor_offset)` using
//! authoritative index/trie layout metadata (never a heuristic boundary scan).
//! Both helpers are gated `#[cfg(not(feature = "tombstones"))]` like the seek path
//! they serve: their only caller is `scan_single_partition`, which the `tombstones`
//! build compiles out (it serves single-partition reads via a full scan + filter,
//! not a seek).
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
    /// that immediately FOLLOWS the partition starting at `target_offset`, used to
    /// bound the within-SSTable seek's decompression window to exactly one
    /// partition's byte extent (issue #953 / #951 MEDIUM).
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
    /// - **BTI (`da`)** — the `Partitions.db` trie is enumerated in byte-comparable
    ///   order (which equals `Data.db` layout order) and the resolved offsets are
    ///   cached ([`bti_partition_offsets`]); the successor is the smallest cached
    ///   offset strictly greater than `target_offset` (binary search).
    /// - **BIG (`nb`)** — `Index.db` `partition_entries` are sorted by key (==
    ///   `Data.db` order); the successor is the smallest `data_offset` strictly
    ///   greater than `target_offset`.
    ///
    /// [`bti_partition_offsets`]: Self::bti_partition_offsets
    pub(crate) async fn successor_partition_offset(
        &self,
        target_offset: u64,
    ) -> Result<Option<u64>> {
        if self.bti_partitions_db.is_some() {
            let offsets = self.bti_partition_offsets()?;
            // Smallest offset strictly greater than target_offset.
            let idx = offsets.partition_point(|&o| o <= target_offset);
            return Ok(offsets.get(idx).copied());
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

    /// Enumerate and cache every partition's UNCOMPRESSED `Data.db` start offset
    /// from the BTI `Partitions.db` trie, ascending (issue #953 / #951).
    ///
    /// Computed lazily once and memoised in [`bti_partition_offsets`]: the trie is
    /// DFS-walked in byte-comparable order, each `BtiPartitionLocation` is resolved
    /// to its `Data.db` offset (NARROW → `DataOffset` directly; WIDE → the
    /// `RowsOffset`'s `TrieIndexEntry.data_position` via `Rows.db`), and the
    /// resulting offsets are sorted ascending. The sort makes the cache a clean
    /// successor index regardless of trie emission order.
    ///
    /// [`bti_partition_offsets`]: Self::bti_partition_offsets
    fn bti_partition_offsets(&self) -> Result<&[u64]> {
        use crate::storage::sstable::bti::{
            iterate_partition_locations_in_bti_file, resolve_rows_db_entry, BtiPartitionLocation,
        };

        if let Some(cached) = self.bti_partition_offsets.get() {
            return Ok(cached);
        }

        let Some(partitions_db) = &self.bti_partitions_db else {
            // Not a BTI reader: no trie to enumerate. Cache an empty list so the
            // successor lookup is consistently O(1) and returns no successor.
            let _ = self.bti_partition_offsets.set(Vec::new());
            return Ok(self
                .bti_partition_offsets
                .get()
                .map(Vec::as_slice)
                .unwrap_or(&[]));
        };

        let mut cursor = std::io::Cursor::new(partitions_db.as_slice());
        // Offset-only enumeration (issue #1649): we need only the partition
        // locations here, never the reconstructed token keys, so this path
        // performs zero per-partition key-`Vec` allocations.
        let locations = iterate_partition_locations_in_bti_file(&mut cursor).map_err(|e| {
            Error::corruption(format!(
                "BTI Partitions.db trie enumeration failed while resolving the \
                 next-partition seek bound: {e}"
            ))
        })?;

        let mut offsets = Vec::with_capacity(locations.len());
        for location in locations {
            let off = match location {
                BtiPartitionLocation::DataOffset(off) => off,
                BtiPartitionLocation::RowsOffset(rows_offset) => {
                    let rows_db = self.bti_rows_db.as_ref().ok_or_else(|| {
                        Error::corruption(format!(
                            "BTI Partitions.db enumeration returned RowsOffset({rows_offset}) \
                             but this reader has no Rows.db; the SSTable is structurally invalid \
                             (Rows.db is required for wide partitions)."
                        ))
                    })?;
                    let header = resolve_rows_db_entry(rows_db.as_slice(), rows_offset as usize)
                        .map_err(|e| {
                            Error::corruption(format!(
                                "BTI Rows.db row-index entry at RowsOffset({rows_offset}) is \
                                 unreadable while resolving the next-partition seek bound: {e}"
                            ))
                        })?;
                    header.data_position
                }
            };
            offsets.push(off);
        }
        offsets.sort_unstable();

        // Another thread may have populated the cache between the `get` above and
        // here; `set` fails in that case and we read the winning value back.
        let _ = self.bti_partition_offsets.set(offsets);
        Ok(self
            .bti_partition_offsets
            .get()
            .map(Vec::as_slice)
            .unwrap_or(&[]))
    }
}
