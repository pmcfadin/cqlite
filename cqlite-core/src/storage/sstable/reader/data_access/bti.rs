//! BTI ("da") read paths: trie-resolved point lookups, single-partition seeks,
//! clustering-slice narrowing, and the whole-Data.db BTI scan.
//!
//! BTI SSTables carry no Index.db/Summary.db. The Partitions.db trie is the
//! authoritative present/absent oracle; every method here resolves an
//! uncompressed Data.db offset via the trie and decodes only the chunk window
//! that holds the target partition (issue #831 / #909 / #953 / #954).

use super::super::SSTableReader;
use super::model::{sort_by_token_order_with_meta, SCAN_FOR_KEY_CALLS};
use crate::types::{CellWriteMetadata, ScanRow};
use crate::{Result, RowKey};
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;

// `TableId`, `Error`, and `debug!` are used only by the seek/clustering paths
// (`scan_single_partition_clustering` / `bti_clustering_row_window`), which are
// `not(tombstones)`-gated; the point-read decoders that also used them moved to
// `bti_point.rs` (issue #1599 / G3 split).
#[cfg(not(feature = "tombstones"))]
use super::model::{physical_byte_bounds_for_slice, ClusteringRowWindow, ClusteringSlice};
#[cfg(not(feature = "tombstones"))]
use crate::types::TableId;
#[cfg(not(feature = "tombstones"))]
use crate::Error;
#[cfg(not(feature = "tombstones"))]
use tracing::debug;

impl SSTableReader {
    /// Current value of the test-only `scan_for_key` invocation counter.
    ///
    /// Issue #831: tests use this to assert that a BTI `get()` resolves entirely
    /// through the Partitions.db trie and never falls through to the sequential
    /// scan. See [`SCAN_FOR_KEY_CALLS`].
    pub fn scan_for_key_call_count() -> u64 {
        SCAN_FOR_KEY_CALLS.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Single-partition *seek* for the partition-targeted lookup fast path,
    /// clustering-slice-aware (Issue #953 + #954, Epic #951).
    ///
    /// Where [`scan`](Self::scan) decodes EVERY partition in this SSTable and the
    /// caller retains one, this resolves the target partition's `Data.db` offset
    /// from the authoritative index (the BTI Partitions.db trie or the BIG
    /// `Index.db`) and decodes ONLY that partition — the same per-partition decode
    /// `scan` runs (`parse_block_emit` over the chunk-targeted decompressed
    /// window), so its output is byte-for-byte identical to filtering the full
    /// `scan` result down to `partition_key`.
    ///
    /// Offset domains (no-heuristics: authoritative resolved offsets only):
    /// - **BTI ("da")** — `lookup_partition_via_bti_trie` returns the UNCOMPRESSED
    ///   `Data.db` offset; a trie miss is authoritative absence.
    /// - **BIG (`nb`)** — `lookup_partition_with_index` returns the partition's
    ///   offset into the (uncompressed) data section. A hit is authoritative
    ///   present; a MISS returns `Ok(None)` (the `Index.db` may be digest-keyed or
    ///   incomplete, exactly the `get()` fallback rationale at #517) so the caller
    ///   re-checks via a full scan rather than risk a false negative.
    ///
    /// Prefix-collision / wrong-offset guard: the decode
    /// (`bti_decompress_and_parse_target_all`) re-verifies the decoded partition
    /// key equals `partition_key` before collecting any row, so a BTI
    /// prefix-collision candidate or a stale/mismatched index offset decodes to
    /// nothing and is reported as absent — never a wrong partition. Every
    /// clustering row of the matched partition is collected (not just the first),
    /// so a multi-row partition returns all rows.
    ///
    /// Compiled only for the default (`not(tombstones)`) build: the manager's
    /// seek-driven `scan_partition` exists only there, so under `tombstones` this
    /// would be dead code.
    ///
    /// When `clustering` is `Some(slice)` AND this reader is BTI (`da`) with a
    /// per-partition row index (`Rows.db`), the target partition's authoritative
    /// row index is consulted to resolve the byte extent of the row-index
    /// block(s) covering the requested clustering range, and ONLY that byte window
    /// is decoded — so a `WHERE pk = ? AND ck </>/= ?` slice over a wide partition
    /// decodes O(matched rows + index block slack) rather than the whole
    /// partition. The post-scan `evaluate_leaf` backstop trims the
    /// block-granularity over-read, so the returned rows are a superset of the
    /// exact slice and the final query output is byte-identical to the
    /// full-partition decode + post-filter.
    ///
    /// Returns `Ok(Some((rows, clustering_seek_engaged)))`:
    /// - `clustering_seek_engaged == true` only when the clustering row-index
    ///   narrowing actually bounded the decode (BTI wide partition with a usable
    ///   row index and an encodable bound). The caller reports
    ///   [`AccessPath::ClusteringSlice`](crate::query::access_path::AccessPath::ClusteringSlice)
    ///   in that case.
    /// - `clustering_seek_engaged == false` when the partition was decoded in full
    ///   (no clustering slice, a NARROW BTI partition with no row index, the BIG
    ///   format, or an un-encodable bound). Results are still correct — the caller
    ///   reports the honest `PartitionLookup` path, NOT a fake clustering slice.
    ///
    /// `Ok(None)` mirrors [`scan_single_partition`]: the seek is not applicable
    /// (no authoritative offset) and the caller must fall back to a full scan +
    /// retain.
    ///
    /// [`scan_single_partition`]: crate::storage::sstable::SSTableReader
    #[cfg(not(feature = "tombstones"))]
    pub(crate) async fn scan_single_partition_clustering(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        clustering: Option<&ClusteringSlice>,
        // `true` iff the manager resolved this reader by an EXACT fully-qualified
        // `keyspace.table` match (or the query is unqualified). `false` means a
        // fully-qualified query reached this reader via the bare-name fallback, in
        // which case the seek guard keeps STRICT keyspace matching so it can never
        // return rows from a different keyspace whose table name collides (#1284).
        fully_qualified_match: bool,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Option<(Vec<(RowKey, ScanRow)>, bool)>> {
        // 1. Resolve the partition's uncompressed Data.db offset, and record
        //    whether THIS path's "decoded nothing" is authoritative absence (BTI
        //    trie) or merely inconclusive (BIG Index.db).
        let is_bti = self.bti_partitions_db.is_some();
        // Resolve the target partition's UNCOMPRESSED `Data.db` start offset.
        let offset = if is_bti {
            match self.lookup_partition_via_bti_trie(partition_key)? {
                // Trie hit: candidate uncompressed offset (re-verified on decode).
                Some(off) => off,
                // Trie miss is AUTHORITATIVE absence for BTI (no rows, no seek).
                None => return Ok(Some((Vec::new(), false))),
            }
        } else {
            match self.lookup_partition_with_index(partition_key).await? {
                // Index.db hit: a candidate offset into the data section. (The
                // `data_size` is 0 in writer-produced Index.db, so we do NOT use it
                // as a bound — the successor offset below is the authoritative end.)
                Some((off, _size)) => off,
                // No Index.db hit: cannot seek authoritatively (the index may be
                // digest-keyed / incomplete, exactly the get() #517 rationale).
                // Fall back to a full scan.
                None => return Ok(None),
            }
        };

        // AUTHORITATIVE end bound (issue #953 / #951 MEDIUM): the target partition
        // occupies `[offset, end)`, where `end` is the SUCCESSOR partition's start
        // offset (next trie/index entry). Decompressing exactly the chunks covering
        // that half-open range materializes every byte of the target partition —
        // including a row/cell that SPANS multiple compression chunks — without
        // reading the next partition. This replaces the previous next-partition
        // *boundary-scan* heuristic (a row-count-stability guard that could falsely
        // accept a boundary mid-partition); see `bti_decompress_and_parse_target_all`.
        //
        // `None` means `offset` is the LAST partition (no successor): the callee
        // bounds the end with the authoritative data-section length, or falls back
        // to the safe full-scan path when that length is unknown.
        let end_bound = self.successor_partition_offset(offset)?.map(|e| e as usize);

        let schema_opt = self.get_table_schema(schema);

        // Issue #954 / #1184: resolve the within-partition row-body byte window for a
        // single-column clustering slice from the authoritative index (BTI `Rows.db`
        // trie or BIG promoted `IndexInfo` blocks). The unified resolver lives in
        // `big_promoted.rs` (campsite: keeps this over-threshold file from growing).
        let (row_body_window, decode_end_bound, clustering_engaged) = self
            .resolve_clustering_seek_window(
                is_bti,
                partition_key,
                offset,
                clustering,
                schema_opt.as_ref(),
                end_bound,
            )?;

        // Issue #1184: an engaged BIG clustering narrowing decodes the selected block
        // window via `big_promoted.rs` (partition-key-bytes guard, not the BTI strict
        // table-id match that rejects writer-header SSTables). BTI keeps its decoder.
        if !is_bti && clustering_engaged {
            if let Some(rows) = self
                .big_decode_clustering_window(
                    partition_key,
                    offset,
                    decode_end_bound,
                    row_body_window,
                    schema_opt.as_ref(),
                )
                .await?
            {
                if !rows.is_empty() {
                    super::super::super::work_counters::add_partition_decoded();
                }
                return Ok(Some((rows, true)));
            }
        }

        // 2. Decode ONLY the target partition at the resolved offset, using the
        //    SAME parser the scan path uses. `bti_decompress_and_parse_target_all`
        //    chunk-targets the decompression (decodes just the chunk window that
        //    holds the partition) and re-verifies the decoded key, so this is
        //    O(1) PARTITIONS decoded regardless of the SSTable's partition count.
        //
        //    Issue #953 correctness fix: this collects EVERY clustering row of the
        //    one target partition (bounded by the authoritative successor offset /
        //    data-section length), not just the first row, so a `WHERE pk = ?`
        //    over a multi-clustering-row
        //    partition returns all rows — byte-identical to filtering the full
        //    scan down to `partition_key`. The single-row `*_target` decoder is
        //    still used by the `get()` point-lookup path, which returns one Value.
        //
        //    Issue #954: when `row_body_window` is set, the parse is bounded to the
        //    clustering slice's row-index block extent so only O(slice) rows are
        //    decoded (the post-scan backstop trims the block-granularity slack).
        let parser = self.build_v5_parser(true);
        let key = RowKey::from(partition_key.to_vec());
        let decoded_rows = match self
            .bti_decompress_and_parse_target_all(
                offset as usize,
                decode_end_bound,
                row_body_window,
                &key,
                table_id,
                fully_qualified_match,
                schema_opt.as_ref(),
                &parser,
            )
            .await?
        {
            // Authoritatively bounded decode (rows may be empty for an absent key).
            Some(rows) => rows,
            // The seek could not bound the target partition authoritatively (the
            // LAST partition with an unknown data-section length): fall back to the
            // safe full scan + retain for correctness, per the #953 mandate.
            None => return Ok(None),
        };

        // 3. Record the per-partition decode (Issue #953 / #958): exactly ONE
        //    partition is decoded for a hit regardless of how many clustering rows
        //    it yields — `partitions_decoded` counts partitions, not rows. This is
        //    the signal that proves the within-SSTable seek (vs a full
        //    parse-then-retain). A non-empty decode means the partition exists.
        if !decoded_rows.is_empty() {
            super::super::super::work_counters::add_partition_decoded();
            // Tombstone suppression matches the user-facing scan path
            // (`sequential_scan`/`bti_scan_with_metadata` both apply it),
            // applied per-row so a row tombstone is dropped while live rows in
            // the same partition survive.
            let rows: Vec<(RowKey, ScanRow)> = decoded_rows
                .into_iter()
                .filter(|value| self.filter_tombstone(value))
                .map(|value| (key.clone(), value))
                .collect();
            return Ok(Some((rows, clustering_engaged)));
        }

        // Decoded nothing at the resolved offset. Whether that is authoritative
        // depends on HOW the offset was resolved (Constraint #4: never return a
        // wrong/empty result from an unsupported/inconclusive seek):
        //
        // - **BTI** — the trie is the authoritative present/absent oracle and the
        //   decode re-verified the key, so "decoded nothing" means the trie
        //   candidate was a prefix-collision for an absent key. AUTHORITATIVE
        //   empty: the caller does NOT fall back.
        // - **BIG** — the `Index.db` offset is only a candidate position; its
        //   promoted-index / chunk layout is not as load-bearing as the BTI trie,
        //   so a failed decode at the resolved offset is INCONCLUSIVE (a partition
        //   that straddles a chunk boundary, or a stale offset, can fail the
        //   chunk-targeted decode yet be found by a full parse). Fall back to a
        //   full scan rather than risk a false negative.
        if is_bti {
            // Authoritative absence (prefix-collision candidate for an absent key).
            // No rows decoded, so report the clustering seek as NOT engaged.
            Ok(Some((Vec::new(), false)))
        } else {
            Ok(None)
        }
    }

    /// Resolve the within-partition row-body byte window covering a single-column
    /// clustering slice, using the target partition's authoritative BTI row index
    /// (Issue #954, Epic #951).
    ///
    /// For a WIDE BTI partition the `Partitions.db` trie points at a per-partition
    /// `TrieIndexEntry` in `Rows.db`; that entry's row-index trie maps clustering
    /// **separators** to row-index BLOCK offsets (relative to the partition
    /// start). [`select_row_index_blocks_for_range`] applies the authoritative
    /// separator-floor semantics to pick exactly the blocks whose key interval
    /// intersects `[start, end]`, so the returned byte window is the smallest
    /// authoritative extent that can contain the requested clustering range.
    ///
    /// Returns `Ok(Some(window))` only when the narrowing is authoritative and
    /// useful:
    /// - the reader is BTI with a `Rows.db`,
    /// - the partition is WIDE (`Partitions.db` returned a `RowsOffset`, i.e. the
    ///   partition has a row index — a NARROW partition has no per-partition row
    ///   index to seek within),
    /// - the clustering bound(s) encode to the OSS50 byte-comparable form, and
    /// - the selected block set is non-empty.
    ///
    /// Returns `Ok(None)` (decode the whole partition, report `PartitionLookup`)
    /// for every other case — a NARROW partition, an empty `Rows.db`, an
    /// un-encodable bound, or a slice that selects no block. This is the honest
    /// fallback: correctness is preserved by decoding the full partition and
    /// letting the post-scan backstop filter.
    #[cfg(not(feature = "tombstones"))]
    pub(super) fn bti_clustering_row_window(
        &self,
        partition_key: &[u8],
        slice: &ClusteringSlice,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Option<ClusteringRowWindow>> {
        use crate::storage::sstable::bti::{
            encode_partition_key_for_bti_trie, lookup_partition_in_bti_slice,
            resolve_rows_db_entry, rows_floor_block, rows_strict_ceiling_block,
            BtiPartitionLocation,
        };

        let (Some(partitions_db), Some(rows_db)) = (&self.bti_partitions_db, &self.bti_rows_db)
        else {
            return Ok(None);
        };

        // Resolve the partition's location. Only a WIDE partition (RowsOffset) has
        // a per-partition row index we can seek within; a NARROW partition
        // (DataOffset) has none, so decode it in full.
        // Issue #1574 (C3): walk the resident trie buffer in place (no whole-file copy).
        // Issue #1575 (C4): encode the raw key then walk with the pre-encoded key via
        // the single `lookup_partition_in_bti_slice` primitive every BTI lookup shares.
        let encoded = encode_partition_key_for_bti_trie(partition_key);
        let rows_offset = match lookup_partition_in_bti_slice(partitions_db.as_slice(), &encoded)
            .map_err(|e| {
                Error::corruption(format!(
                    "BTI clustering seek: Partitions.db trie lookup failed (key len={}): {}",
                    partition_key.len(),
                    e
                ))
            })? {
            Some(BtiPartitionLocation::RowsOffset(off)) => off as usize,
            // NARROW partition or absent key: no row index to narrow with.
            Some(BtiPartitionLocation::DataOffset(_)) | None => return Ok(None),
        };

        // Resolve the per-partition row-index entry ONCE (issue #1647 / L1: the
        // pre-L1 path resolved it HERE and again inside `iterate_rows_for_partition`,
        // discarding the first). `header.trie_root` roots the O(key-length)
        // floor/ceiling walks below, so a clustering read never materializes every
        // row-index block.
        let header = resolve_rows_db_entry(rows_db.as_slice(), rows_offset).map_err(|e| {
            Error::corruption(format!(
                "BTI clustering seek: Rows.db entry at RowsOffset({rows_offset}) unreadable: {e}"
            ))
        })?;
        let root = header.trie_root;

        // Per-column reverse order for the FIRST clustering column (single-column
        // scope per #954). A missing/absent schema treats it as ascending.
        let is_reversed: Vec<bool> = schema
            .map(|s| {
                s.clustering_keys
                    .iter()
                    .map(|c| matches!(c.order, crate::schema::ClusteringOrder::Desc))
                    .collect()
            })
            .unwrap_or_default();

        // Encode the CQL bounds into the PHYSICAL byte-comparable order the row
        // index uses, normalizing for a DESC first clustering column (issue #954
        // High-severity correctness fix). The floor/ceiling walks operate purely in
        // physical (on-disk, byte-comparable) order, so the CQL lower/upper bounds
        // must be mapped to the physical-lower/physical-upper sides before the walk.
        // For a DESC column those roles SWAP (see `physical_byte_bounds_for_slice`).
        // An un-encodable bound makes the narrowing unsafe → decode the whole
        // partition (honest fallback).
        let Some((start_bytes, end_bytes)) = physical_byte_bounds_for_slice(slice, &is_reversed)?
        else {
            return Ok(None);
        };

        // Locate the row-body window with two O(key-length) separator walks instead
        // of materializing every row-index block then linearly filtering (issue
        // #1647 / L1). Both mirror Cassandra `RowIndexReader.separatorFloor`:
        //   - floor(start): the block whose half-open interval `[s_i, s_{i+1})`
        //     contains the physical-lower bound (largest separator `<= start`), i.e.
        //     the first block that can hold a row of the slice. `None` when `start`
        //     sorts below the FIRST separator — the trie-implicit first block at the
        //     partition body start (issue #1968), which no stored walk can return.
        //   - strict_ceiling(end): the block with the smallest separator strictly
        //     `> end` = the EXCLUSIVE window end (the successor of `floor(end)`).
        //     `None` when the slice reaches the last block (window runs to the
        //     partition end).
        // These reproduce the pre-L1 `select_row_index_blocks_for_range` window
        // (min selected-block start .. successor of max selected-block) EXACTLY, by
        // construction, for every boundary class (see the rows_floor unit tests).
        let floor_block =
            rows_floor_block(rows_db.as_slice(), root, &start_bytes).map_err(|e| {
                Error::corruption(format!(
                "BTI clustering seek: Rows.db floor walk at RowsOffset({rows_offset}) failed: {e}"
            ))
            })?;
        let ceil_block =
            rows_strict_ceiling_block(rows_db.as_slice(), root, &end_bytes).map_err(|e| {
                Error::corruption(format!(
                    "BTI clustering seek: Rows.db ceiling walk at RowsOffset({rows_offset}) \
                     failed: {e}"
                ))
            })?;

        // The static row precedes the clustering rows and must be merged into each
        // emitted clustering row, so we may only fast-forward PAST it when the table
        // has NO static columns; otherwise decode from the partition body start
        // (`body_start_rel = 0`) so the static prefix is seen (the END bound still
        // narrows). (`test_da.wide_table` has no static columns, so the start narrows
        // too.)
        let has_static = schema
            .map(|s| s.columns.iter().any(|c| c.is_static))
            .unwrap_or(false);

        // IMPLICIT FIRST BLOCK (issue #1968): a `start` below the first separator
        // selects the implicit block at the partition body start, which no walk can
        // return. `rows_floor_block` returns `None` in exactly that case, so it IS
        // the implicit-first signal; the decode must then begin at rel 0 or the
        // earliest clustering rows are dropped.
        let includes_implicit_first_block = floor_block.is_none();

        // The start narrows to the floor block only when NEITHER a static row NOR the
        // implicit first block forces decoding from rel 0.
        let narrows_start = !has_static && !includes_implicit_first_block;

        // CORRECTNESS GUARD (no-heuristics, never wrong results): FLAG_OPEN_MARKER on
        // the floor block's `IndexInfo` means a range tombstone is OPEN at the START
        // of the narrowed window — a deletion opened in an earlier block still shadows
        // rows inside the slice. Skipping earlier blocks would drop that range-open
        // marker and risk resurrecting a deleted row (the post-scan backstop only
        // FILTERS rows, it cannot re-apply a missed range tombstone). Per Cassandra
        // `RowIndexReader.IndexInfo.openDeletion`, the marker on the floor block fully
        // captures any deletion open at the window start, so this is the exact, tight
        // guard: when it fires (only relevant when the start actually narrows) fall
        // back to a full-partition decode. When the decode already starts at rel 0
        // (static / implicit-first) no earlier block is skipped, so no guard is
        // needed. (`test_da.wide_table` has no range tombstones.)
        if narrows_start
            && floor_block
                .as_ref()
                .map(|b| b.open_marker.is_some())
                .unwrap_or(false)
        {
            debug!(
                "BTI clustering seek: floor row-index block carries an open range-tombstone \
                 marker; decoding full partition to preserve range-deletion semantics (no \
                 narrowing)"
            );
            return Ok(None);
        }

        // body_start_rel: the floor block's offset when the start narrows, else the
        // partition body start (rel 0). The block `data_offset` is relative to the
        // partition start (the same domain the parser sees for `window[within..]`).
        let body_start_rel = match &floor_block {
            Some(b) if narrows_start => b.data_offset as usize,
            _ => 0,
        };
        // body_end_rel: the exclusive successor block start, or the partition end
        // (`usize::MAX`, clamped by the caller against the authoritative partition
        // end / data-section length) when the slice reaches the last block.
        let body_end_rel = ceil_block
            .map(|b| b.data_offset as usize)
            .unwrap_or(usize::MAX);

        Ok(Some(ClusteringRowWindow {
            body_start_rel,
            body_end_rel,
        }))
    }

    /// BTI ("da") full scan: decompress the whole Data.db section and parse
    /// every partition in token order (issue #660).
    ///
    /// BTI SSTables carry no Index.db/Summary.db, so a range/full scan cannot
    /// use the index path. Instead we stitch the entire (chunk-compressed) data
    /// section into one buffer and run [`parse_block_with_cell_metadata`], which
    /// walks ALL partitions — the same per-partition decode the point-lookup
    /// path uses, but without stopping at the first match.
    ///
    /// Returns entries with per-cell write metadata so the WRITETIME/TTL scan
    /// (`scan_with_cell_metadata`) and the plain `scan` (which drops the metadata)
    /// can share a single implementation. Results are filtered by the optional
    /// `[start_key, end_key]` range and tombstone-suppressed, then sorted into
    /// Murmur3 token order and truncated to `limit` — identical post-processing
    /// to the V5CompressedLegacy stitched path.
    ///
    /// Uses its own per-scan
    /// [`ScanCursor`](crate::storage::sstable::reader::source::ScanCursor), so it
    /// runs in parallel with other scans on this reader without serialization
    /// (issue #815).
    ///
    /// [`parse_block_with_cell_metadata`]: crate::storage::sstable::reader::parsing::V5CompressedLegacyParser::parse_block_with_cell_metadata
    ///
    /// `read_shadowing` (issue #1741): `true` for user-facing SELECT scans
    /// (`scan`, `scan_with_cell_metadata`), `false` for the physical
    /// `get_all_entries` (integrity verification / data-manager) which must count
    /// every on-disk row.
    pub(super) async fn bti_scan_with_metadata(
        &self,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
        read_shadowing: bool,
    ) -> Result<
        Vec<(
            RowKey,
            ScanRow,
            std::collections::HashMap<String, CellWriteMetadata>,
        )>,
    > {
        let cursor = self.new_scan_cursor().await?;

        // Decompress the entire data section. Precondition for stitch_all_chunks:
        // cursor's file seeked to data-section start (fresh cursor is at chunk 0).
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            // A5 read-work counter (SEEK_CALLS; consumer E4): scan seek-to-data-start
            // before stitching the section. No-op in release (design.md Decision 1/2).
            crate::storage::sstable::read_work_counters::record_seek();
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }
        let whole = self.stitch_all_chunks(&cursor).await?;

        // Resolve schema via the four-tier strategy (provided > header > registry).
        // V5CompressedLegacy partition decode requires a schema (cells lack names).
        let effective_schema = self.get_table_schema(schema);
        let parser = self.build_v5_parser(read_shadowing);
        let parsed =
            parser.parse_block_with_cell_metadata(&whole, effective_schema.as_ref(), self)?;

        let mut results = Vec::new();
        for (_entry_table_id, entry_key, entry_value, cell_meta) in parsed {
            if let Some(start) = start_key {
                if &entry_key < start {
                    continue;
                }
            }
            if let Some(end) = end_key {
                if &entry_key > end {
                    continue;
                }
            }
            if !self.filter_tombstone(&entry_value) {
                continue;
            }
            results.push((entry_key, entry_value, cell_meta));
        }

        sort_by_token_order_with_meta(&mut results);
        if let Some(lim) = limit {
            results.truncate(lim);
        }

        tracing::debug!(
            "SSTableReader::bti_scan_with_metadata - Returning {} results",
            results.len()
        );
        Ok(results)
    }
}
