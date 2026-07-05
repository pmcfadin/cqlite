//! BTI ("da") read paths: trie-resolved point lookups, single-partition seeks,
//! clustering-slice narrowing, and the whole-Data.db BTI scan.
//!
//! BTI SSTables carry no Index.db/Summary.db. The Partitions.db trie is the
//! authoritative present/absent oracle; every method here resolves an
//! uncompressed Data.db offset via the trie and decodes only the chunk window
//! that holds the target partition (issue #831 / #909 / #953 / #954).

use super::super::SSTableReader;
use super::model::{
    bti_lookup_step, sort_by_token_order_with_meta, table_header_consistent_for_seek,
    BtiLookupStep, SCAN_FOR_KEY_CALLS,
};
use crate::types::{CellWriteMetadata, ScanRow, TableId};
use crate::{Error, Result, RowKey};
use log::debug;
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;

#[cfg(not(feature = "tombstones"))]
use super::model::{physical_byte_bounds_for_slice, ClusteringRowWindow, ClusteringSlice};

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
            iterate_rows_for_partition, lookup_raw_key_in_bti_partitions_db, resolve_rows_db_entry,
            select_row_index_blocks_for_range, BtiPartitionLocation,
        };

        let (Some(partitions_db), Some(rows_db)) = (&self.bti_partitions_db, &self.bti_rows_db)
        else {
            return Ok(None);
        };

        // Resolve the partition's location. Only a WIDE partition (RowsOffset) has
        // a per-partition row index we can seek within; a NARROW partition
        // (DataOffset) has none, so decode it in full.
        let mut cursor = std::io::Cursor::new(partitions_db.as_slice());
        let rows_offset = match lookup_raw_key_in_bti_partitions_db(&mut cursor, partition_key)
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

        // Resolve the per-partition row-index entry and enumerate its blocks in
        // ascending byte-comparable (clustering) order.
        let header = resolve_rows_db_entry(rows_db.as_slice(), rows_offset).map_err(|e| {
            Error::corruption(format!(
                "BTI clustering seek: Rows.db entry at RowsOffset({rows_offset}) unreadable: {e}"
            ))
        })?;
        let (_header2, entries) = iterate_rows_for_partition(rows_db.as_slice(), rows_offset)
            .map_err(|e| {
                Error::corruption(format!(
                    "BTI clustering seek: Rows.db trie at RowsOffset({rows_offset}) unreadable: {e}"
                ))
            })?;
        // `iterate_rows_for_partition` re-resolves the header internally; keep the
        // first `header` (identical) for `block_count`/`data_position`.
        let _ = header;
        if entries.is_empty() {
            return Ok(None);
        }

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
        // High-severity correctness fix). `select_row_index_blocks_for_range`
        // operates purely in physical (on-disk, byte-comparable) order, so the CQL
        // lower/upper bounds must be mapped to the physical-lower/physical-upper
        // sides before block selection. For a DESC column those roles SWAP (see
        // `physical_byte_bounds_for_slice`). An un-encodable bound makes the
        // narrowing unsafe → decode the whole partition (honest fallback).
        let Some((start_bytes, end_bytes)) = physical_byte_bounds_for_slice(slice, &is_reversed)?
        else {
            return Ok(None);
        };

        // CORRECTNESS GUARD (no-heuristics, never wrong results): a row-index block
        // carrying an `open_marker` (FLAG_OPEN_MARKER) means a range tombstone is
        // OPEN at that block boundary — a deletion opened in an earlier block can
        // still shadow rows inside the requested slice. Narrowing the decode skips
        // the rows (and the range-marker bytes) before the slice, which would drop
        // that open deletion and risk resurrecting a deleted row. The post-scan
        // backstop only FILTERS rows, it cannot re-apply a missed range tombstone.
        // So when ANY block in this partition's row index carries an open marker we
        // fall back to a full-partition decode (correct, just unnarrowed). The
        // common wide-partition slice (no range tombstones) is unaffected.
        if entries.iter().any(|(_sep, b)| b.open_marker.is_some()) {
            debug!(
                "BTI clustering seek: partition row index has open range-tombstone marker(s); \
                 decoding full partition to preserve range-deletion semantics (no narrowing)"
            );
            return Ok(None);
        }

        let blocks = select_row_index_blocks_for_range(&entries, &start_bytes, &end_bytes);

        // IMPLICIT FIRST BLOCK (issue #1968). A `Rows.db` row-index trie stores a
        // separator per block EXCEPT the first: the block covering keys BELOW the
        // first separator lives at the partition body start and has NO trie entry
        // (mirroring Cassandra `RowIndexReader.separatorFloor`, which returns the
        // partition start for a key below the first separator). Consequently
        // `select_row_index_blocks_for_range` — which operates purely over the
        // stored (separator, block) entries — can NEVER return that implicit block.
        //
        // The requested range overlaps the implicit first block iff its
        // physical-lower bound sorts strictly BELOW the first separator
        // (`start_bytes < entries[0].sep`). The canonical trigger is an OPEN lower
        // bound (`ck < N` / `ck <= N`), whose physical-lower sentinel is `-∞` = the
        // empty slice `b""`, but a closed lower bound below the first separator
        // (`ck >= 2 AND ck < 20`, `ck = 0`) hits it too. When the range includes
        // that block the earliest clustering rows precede every stored block, so the
        // decode MUST begin at the partition body start (rel 0) or those rows are
        // silently dropped (the pre-fix bug returned `ck=8..19` for `ck < 20`).
        // Bind the first stored entry ONCE and reuse it for both the predicate and
        // the entirely-within-implicit-block window below, so the window end can
        // never diverge from the predicate (no `usize::MAX` over-read fallback).
        let first_entry = entries.first();
        let includes_implicit_first_block = first_entry
            .map(|(sep, _)| start_bytes.as_slice() < sep.as_slice())
            .unwrap_or(false);

        if blocks.is_empty() {
            // `includes_implicit_first_block` is true only when `first_entry` is
            // `Some`; the `if let` makes that invariant explicit and reuses the exact
            // entry that satisfied the predicate (its start is the window end).
            if let (true, Some((_sep, first_block))) = (includes_implicit_first_block, first_entry)
            {
                // The range lies ENTIRELY within the implicit first block (its
                // physical-upper is also below the first separator, e.g. `ck <= 3`
                // or `ck = 0`): no stored block overlaps, but the implicit block
                // does. Narrow to [partition body start, first stored block start);
                // the post-scan backstop trims to the exact predicate.
                return Ok(Some(ClusteringRowWindow {
                    body_start_rel: 0,
                    body_end_rel: first_block.data_offset as usize,
                }));
            }
            // No block (implicit or stored) overlaps the range. The slice may still
            // select rows that share the floor block's separator boundary; to stay
            // correct we fall back to a full-partition decode rather than risk
            // dropping a row.
            return Ok(None);
        }

        // Row-body byte window = [first selected block start, end of the LAST
        // selected block). The block `data_offset` is relative to the partition
        // start (the same domain the parser sees for `window[within..]`). The end
        // is the start of the FIRST block AFTER the last selected one (or +∞ via
        // the partition end when the last selected block is the partition's last).
        // The static row precedes the clustering rows and must be merged into each
        // emitted clustering row, so we may only fast-forward PAST it when the
        // table has NO static columns. With a static column present, decode from
        // the partition body start (`body_start_rel = 0`) so the static prefix is
        // seen; the END bound still narrows the decode. (The acceptance fixture
        // `test_da.wide_table` has no static columns, so the start narrows too.)
        let has_static = schema
            .map(|s| s.columns.iter().any(|c| c.is_static))
            .unwrap_or(false);
        // Start at the earliest selected STORED block — UNLESS the table has a
        // static row, or the range also covers the implicit first block (issue
        // #1968); either case must decode from the partition body start (rel 0) so
        // the static prefix / the implicit block's rows are seen. The END bound
        // still narrows the decode in both cases.
        let body_start_rel = if has_static || includes_implicit_first_block {
            0
        } else {
            blocks
                .iter()
                .map(|b| b.data_offset as usize)
                .min()
                .unwrap_or(0)
        };
        let last_selected_off = blocks.iter().map(|b| b.data_offset).max().unwrap_or(0);
        // The exclusive end is the next block's start strictly greater than the
        // last selected block; if none, the window runs to the partition end
        // (`usize::MAX` is clamped by the caller against the authoritative
        // partition end / data-section length).
        let body_end_rel = entries
            .iter()
            .map(|(_sep, b)| b.data_offset)
            .filter(|&off| off > last_selected_off)
            .min()
            .map(|off| off as usize)
            .unwrap_or(usize::MAX);

        Ok(Some(ClusteringRowWindow {
            body_start_rel,
            body_end_rel,
        }))
    }

    /// BTI ("da") point lookup: resolve a partition key via the Partitions.db
    /// trie, decode the partition at the resolved offset, and return its row
    /// `Value` (issue #831).
    ///
    /// Correctness invariants (see issue #831 / #755):
    ///
    /// - **Offset domain**: the trie returns an *uncompressed* Data.db offset, so
    ///   we decode the partition out of the DECOMPRESSED data section, never via
    ///   `read_value_at_offset`/`get_cached_data` (which seek raw file bytes).
    /// - **Own decompression**: `requires_chunk_stitching()` is `false` for BTI,
    ///   so this path decompresses the chunk-compressed Data.db itself via the
    ///   reader's CompressionInfo + compression_reader. Because the trie already
    ///   resolved the EXACT uncompressed offset of the target partition, this only
    ///   decompresses the chunk that contains that offset and continues forward
    ///   chunk-by-chunk ONLY until the target partition is fully parsed — it never
    ///   decompresses earlier chunks or the rest of the file (issue #831 perf
    ///   finding). The whole-section `point_read_whole_section` fallback (one
    ///   positioned read of the entire data section) is used only when chunk
    ///   targeting is impossible (no/zero `chunk_length`).
    /// - **Prefix-collision guard**: the trie may return a candidate for a
    ///   prefix-colliding key, so the decoded partition key is verified to equal
    ///   the queried key before any row is returned.
    ///
    /// `fully_qualified_match` is the authoritative resolution-mode signal threaded
    /// from the manager's `resolve_reader_list` (issue #1321, mirroring #1284's
    /// seek path): `true` iff the query's fully-qualified `keyspace.table` key
    /// matched this reader's map slot EXACTLY (or the query was unqualified),
    /// `false` iff a fully-qualified query reached this reader via the bare-name
    /// fallback. It gates the per-row table-consistency guard in
    /// `bti_decompress_and_parse_target`: an exact FQ match may relax across a
    /// header-keyspace divergence, while a fallback keeps strict keyspace matching.
    pub(super) async fn bti_point_lookup(
        &self,
        table_id: &TableId,
        key: &RowKey,
        fully_qualified_match: bool,
    ) -> Result<Option<ScanRow>> {
        // 1. Resolve the uncompressed Data.db offset via the trie.
        let offset = match self.lookup_partition_via_bti_trie(key.as_bytes())? {
            Some(off) => off as usize,
            None => return Ok(None), // not in this SSTable
        };

        // 2. Obtain a DECOMPRESSED window that contains the target partition.
        //
        //    `window_base` is the uncompressed offset of the window's first byte
        //    and `window` holds the decompressed bytes from there onward. The
        //    target partition starts at `offset - window_base` inside `window`
        //    (INVARIANT 1: the trie offset indexes the uncompressed data section).
        //
        //    For the chunk-targeted path the window starts at the chunk that
        //    contains `offset` (so `window_base = target_chunk * chunk_length`);
        //    for the whole-section fallback the window starts at offset 0
        //    (`window_base = 0`). Either way the parse below uses the same
        //    `within = offset - window_base` index.
        let schema_opt = self.get_table_schema(None);
        let parser = self.build_v5_parser(true);

        let found = self
            .bti_decompress_and_parse_target(
                offset,
                key,
                table_id,
                fully_qualified_match,
                schema_opt.as_ref(),
                &parser,
            )
            .await?;

        match found {
            Some(value) => {
                if !self.filter_tombstone(&value) {
                    return Ok(None);
                }
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Compute the chunk that contains uncompressed `offset`, the uncompressed
    /// offset of that chunk's start, and the within-chunk index — given the
    /// CompressionInfo `chunk_length` (issue #831).
    ///
    /// Returns `(target_chunk, window_base, within)` where
    /// `window_base = target_chunk * chunk_length` and `within = offset - window_base`.
    /// Pure arithmetic so it can be unit-tested independently of any I/O.
    #[inline]
    fn bti_chunk_target(offset: usize, chunk_length: usize) -> (usize, usize, usize) {
        let target_chunk = offset / chunk_length;
        let window_base = target_chunk * chunk_length;
        let within = offset - window_base;
        (target_chunk, window_base, within)
    }

    /// Positional (`pread`) fetch of compressed chunk `chunk_idx` for the
    /// POINT-READ path (issue #1573, C2). Reads via the shared `point_source` — no
    /// per-lookup `open(2)`, no cursor, no mutex — CRC-checks the chunk, and
    /// returns the compressed bytes (the caller decompresses). `Ok(None)` at EOF.
    /// Only called on the chunk-targeted path, where `CompressionInfo` is present.
    pub(super) fn point_read_compressed_chunk(&self, chunk_idx: usize) -> Result<Option<Vec<u8>>> {
        let Some(ci) = self.compression_info.as_deref() else {
            return Ok(None);
        };
        // header_offset is ALWAYS 0 for NB/BTI: CompressionInfo chunk offsets are
        // absolute from Data.db byte 0 (any embedded header is part of the
        // compressed data), exactly as the cursor path hardcodes in
        // `read_next_block_impl`. Passing `actual_header_size` here would shift
        // every chunk read and fail CRC.
        super::super::block_io::read_compressed_chunk_at(
            self.point_source.as_ref(),
            ci,
            chunk_idx,
            self.stats.file_size,
            0,
        )
    }

    /// Positional (`pread`) read of the ENTIRE uncompressed data section for the
    /// point-read whole-section fallback (issue #1573, C2) — used when chunk
    /// targeting is impossible (no/zero `chunk_length`, i.e. an uncompressed BTI or
    /// nb-without-CompressionInfo Data.db). Reads `[header_size, file_size)` in one
    /// positioned read and, when a `CRC.db` is present (uncompressed BIG), verifies
    /// the covering chunks BEFORE the bytes are parsed — preserving the CRC-then-use
    /// ordering the cursor path enforced via `read_uncompressed_data_block`.
    pub(super) async fn point_read_whole_section(&self) -> Result<Vec<u8>> {
        let header_size = self.calculate_header_size() as u64;
        // Authoritative file length straight from the positional source (== the
        // reader's `file_size`; using the source keeps the read self-consistent).
        let end = self.point_source.len();
        let len = end.saturating_sub(header_size);
        let mut whole = vec![0u8; len as usize];
        if len > 0 {
            // Read the section in BOUNDED windows rather than one section-sized
            // `read_exact_at`. A `DirectReadAt` backend allocates a per-call
            // aligned bounce buffer as large as the requested range, so a single
            // whole-section read would transiently ~double resident memory vs the
            // <128MB target for a large section. Windowing caps the bounce buffer
            // at ~`WHOLE_SECTION_READ_WINDOW` regardless of backend (issue #1573
            // roborev); `whole` itself is the returned data and is unavoidable.
            const WHOLE_SECTION_READ_WINDOW: usize = 1 << 20; // 1 MiB
            let mut filled = 0usize;
            while filled < whole.len() {
                let win_end = (filled + WHOLE_SECTION_READ_WINDOW).min(whole.len());
                self.point_source
                    .read_exact_at(header_size + filled as u64, &mut whole[filled..win_end])?;
                filled = win_end;
            }
        }
        // CRC-verify the covering chunk(s) when a CRC.db is present (no-op for BTI
        // and compressed tables) BEFORE returning the bytes. The section is already
        // resident in `whole`, so verify against those in-memory bytes rather than
        // re-reading the identical range from `point_source` — the section is
        // transferred from disk EXACTLY ONCE (issue #1573 roborev), preserving the
        // CRC-before-use ordering and the CRC algorithm unchanged.
        if self.crc_reader.is_some() {
            self.verify_uncompressed_section_in_buffer(header_size, &whole)
                .await?;
        }
        Ok(whole)
    }

    /// Decompress only the chunk(s) needed to fully parse the target partition at
    /// uncompressed `offset`, then parse and return its row value (issue #831).
    ///
    /// Chunk targeting (the fast path): when `CompressionInfo` with a non-zero
    /// `chunk_length` is present, the chunk containing `offset` is
    /// `target_chunk = offset / chunk_length`; we seek that chunk via its
    /// `chunk_offsets` entry, set the cursor's chunk index to `target_chunk`, then
    /// decompress forward chunk-by-chunk, appending each into `window`. After each
    /// appended chunk we attempt to parse the FIRST partition at `window[within..]`
    /// (`within = offset % chunk_length`). The stop condition (correctness-critical
    /// — never return a truncated parse):
    ///   - parse returns `Ok` AND the emit closure fired (a COMPLETE partition was
    ///     decoded) -> stop and return what the closure captured;
    ///   - parse returns `Err` (buffer truncated mid-partition) OR the closure
    ///     never fired -> append the next chunk and retry;
    ///   - `read_next_block()` returns `None` (EOF) and still not parsed -> stop
    ///     (the caller treats `None` as "absent", matching prior behaviour).
    ///
    /// Fallbacks (preserve prior behaviour exactly): when `compression_info` is
    /// `None` (uncompressed BTI Data.db) or `chunk_length` is 0/absent, this
    /// reads the WHOLE section in one positioned read via
    /// `point_read_whole_section` (`window_base = 0`, CRC-verified when a CRC.db
    /// is present) and runs the same single-partition parse.
    ///
    /// Chunks are fetched with positioned (`read_at`) reads on the shared
    /// `point_source` — no per-lookup `open(2)`, no `ScanCursor`, and no mutex.
    /// `chunk_index` is a plain local (a lookup is single-threaded within
    /// itself), so concurrent lookups run in parallel without serialization;
    /// safety comes from `read_at` taking `&self` (issue #1573, superseding the
    /// per-scan-cursor approach of issue #815).
    pub(super) async fn bti_decompress_and_parse_target(
        &self,
        offset: usize,
        key: &RowKey,
        table_id: &TableId,
        // Issue #1321: authoritative resolution mode (see `bti_point_lookup`).
        // Gates the per-row table-consistency guard exactly like the seek path
        // (#1284): an EXACT fully-qualified resolution may accept rows across a
        // benign header-keyspace divergence on a consistent table name, while a
        // fully-qualified query resolved via the bare-name fallback keeps STRICT
        // keyspace matching (no wrong-keyspace rows).
        fully_qualified_match: bool,
        schema_opt: Option<&crate::schema::TableSchema>,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
    ) -> Result<Option<ScanRow>> {
        use crate::storage::sstable::compression::Compression;

        // Issue #1573 (C2): the point path fetches chunks via positioned reads on
        // the shared `point_source` — no per-lookup `open(2)`, no cursor, no mutex.
        // `chunk_index` is a plain local (a lookup is single-threaded within
        // itself); concurrency safety comes from `read_at` being `&self`.

        // Determine the chunk-targeting parameters. `chunk_length == 0` (or no
        // CompressionInfo) means we cannot chunk-target -> whole-section fallback.
        let chunk_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.chunk_length as usize)
            .filter(|&len| len > 0);

        let mut chunk_index = 0usize;
        let (target_chunk, window_base, mut window) = match chunk_length {
            Some(len) => {
                let (target_chunk, window_base, _within) = Self::bti_chunk_target(offset, len);
                // Positioned reads resolve their own offset from the chunk index, so
                // no pre-seek is needed — just start at `target_chunk`.
                chunk_index = target_chunk;
                (target_chunk, window_base, Vec::<u8>::new())
            }
            None => {
                // Whole-section fallback (uncompressed BTI, or chunk_length absent/0):
                // one positioned read of the whole data section, CRC-verified when a
                // CRC.db is present (see `point_read_whole_section`).
                let whole = self.point_read_whole_section().await?;
                (0usize, 0usize, whole)
            }
        };

        // `within` is the start of the target partition inside `window`.
        if offset < window_base {
            return Err(Error::corruption(format!(
                "BTI point lookup: resolved offset {} precedes window base {} (chunk {})",
                offset, window_base, target_chunk
            )));
        }
        let within = offset - window_base;

        // For the chunk-targeted path we still need to populate `window`. For the
        // whole-section fallback `window` is already complete.
        let chunk_targeted = chunk_length.is_some();

        loop {
            // If chunk-targeted, append the next chunk before each parse attempt
            // (the whole-section fallback already has all bytes in `window`).
            if chunk_targeted {
                // Cache key: the ABSOLUTE chunk index about to be read (issue #1567).
                // Shared cache is consulted before decompress; a hit skips the
                // decompressor (the compressed bytes were still read + CRC-checked).
                let this_chunk = chunk_index as u64;
                match self.point_read_compressed_chunk(chunk_index)? {
                    Some(compressed_chunk) => {
                        chunk_index += 1;
                        let key = self.chunk_cache_key(super::NS_BTI_CHUNK, this_chunk);
                        let decompressed_chunk: std::sync::Arc<[u8]> = if let Some(hit) =
                            self.chunk_cache.get(&key)
                        {
                            hit
                        } else if let Some(compression_reader) = &self.compression_reader {
                            let compression = Compression::new(*compression_reader.algorithm())?;
                            let d = compression.decompress(&compressed_chunk).map_err(|e| {
                                Error::corruption(format!(
                                    "BTI point lookup: failed to decompress chunk: {}",
                                    e
                                ))
                            })?;
                            super::model::DECOMPRESS_CALLS
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            self.chunk_cache.insert(key, d)
                        } else {
                            // No compression reader despite CompressionInfo:
                            // treat raw chunk bytes as the decompressed data.
                            self.chunk_cache.insert(key, compressed_chunk)
                        };
                        window.extend_from_slice(&decompressed_chunk);
                    }
                    None => {
                        // EOF: no more chunks. If we never parsed a complete
                        // partition, the partition is treated as absent (matching
                        // the prior whole-section behaviour for an unparseable tail).
                        return Ok(None);
                    }
                }
            }

            // Need at least the partition header to attempt a match.
            if within >= window.len() {
                if chunk_targeted {
                    // Not enough bytes yet; pull the next chunk.
                    continue;
                }
                // Whole-section window can't grow: offset is past the data.
                return Err(Error::corruption(format!(
                    "BTI trie resolved Data.db offset {} beyond decompressed data section ({} bytes)",
                    offset,
                    window.len()
                )));
            }

            // INVARIANT 3 + chunk-straddle gate. The parse/pull/absent decision is
            // factored into the pure `bti_lookup_step` so the chunk-straddle control
            // flow is unit-testable without a multi-chunk fixture (issue #831 review):
            // when the header/key prefix is not yet fully buffered we must NOT invoke
            // the parser on a truncated header (it can skip bytes and emit a later
            // false-positive entry), and must read the next chunk first.
            let key_available =
                Self::bti_partition_key_bytes_available(&window, within, key.as_bytes());
            let key_matches =
                key_available && self.bti_partition_key_matches(&window, within, key.as_bytes());
            match bti_lookup_step(key_available, key_matches, chunk_targeted) {
                BtiLookupStep::Parse => { /* full key prefix buffered and matches */ }
                BtiLookupStep::PullNextChunk => continue,
                BtiLookupStep::Absent => {
                    if key_available {
                        debug!(
                            "BTI trie candidate at offset {} did not match queried key \
                             (prefix collision); treating as absent",
                            offset
                        );
                    }
                    return Ok(None);
                }
            }

            // Attempt to parse the FIRST partition at window[within..]. The parser
            // detects the next partition boundary / 0x01 end-of-partition marker and
            // stops; we break after the first emitted entry. A complete partition
            // means: parse returned Ok AND the closure fired.
            let mut found: Option<ScanRow> = None;
            let mut emitted = false;
            let parse_result = parser.parse_block_emit(
                &window[within..],
                schema_opt,
                self,
                |(tid, entry_key, entry_value)| {
                    emitted = true;
                    // Verify BOTH the emitted table id is consistent with the
                    // queried table AND the parser-decoded partition key equals the
                    // queried key. The table check is resolution-mode-aware (issue
                    // #1321, mirroring the seek path #1284): an EXACT fully-qualified
                    // resolution accepts a consistent table name across a benign
                    // header-keyspace divergence; a fully-qualified query resolved via
                    // the bare-name fallback keeps STRICT keyspace matching so it can
                    // never return another keyspace's same-named rows. A genuinely
                    // different table name is always rejected (issue #831).
                    if table_header_consistent_for_seek(&tid, table_id, fully_qualified_match)
                        && entry_key.as_bytes() == key.as_bytes()
                    {
                        found = Some(entry_value);
                    }
                    Ok(std::ops::ControlFlow::Break(()))
                },
            );

            match parse_result {
                Ok(()) if emitted => {
                    // A COMPLETE partition was decoded — accept it and stop.
                    return Ok(found);
                }
                _ => {
                    // Either Err (truncated mid-partition) or the closure never
                    // fired (no complete partition yet). For the chunk-targeted
                    // path, pull the next chunk and retry; never accept a partial.
                    if chunk_targeted {
                        continue;
                    }
                    // Whole-section fallback already has every byte: a failure here
                    // means the partition genuinely could not be parsed -> absent.
                    return Ok(None);
                }
            }
        }
    }

    /// Collect-ALL-rows variant of [`bti_decompress_and_parse_target`] for the
    /// within-SSTable seek (`scan_single_partition`, Issue #953 / #951).
    ///
    /// [`bti_decompress_and_parse_target`] stops after the FIRST emitted row of the
    /// decoded partition — correct for a `get()` point lookup that returns a single
    /// `Value`, but WRONG for `scan_partition`, which must hand the query layer
    /// EVERY clustering row of the partition so it can apply clustering predicates.
    /// A `WHERE pk = ?` over a table with multiple clustering rows per partition
    /// would otherwise drop every row after the first whenever the seek succeeds
    /// (the original #953 bug — see the multi-row regression test).
    ///
    /// This variant reuses the identical window-building (chunk targeting or
    /// whole-section fallback), the identical prefix-collision key re-verification,
    /// and the identical `parse_block_emit` decode that the user-facing scan path
    /// runs — but instead of breaking after the first row it COLLECTS every row the
    /// parser emits for the ONE target partition. The emit closure keeps each
    /// `Value` whose decoded key equals the queried key (and whose table id
    /// matches) and `Break`s the instant the parser emits a row with a DIFFERENT
    /// partition key.
    ///
    /// Bounding the decompression window (Issue #953 / #951 MEDIUM fix). The seek
    /// must materialize ONLY the chunks covering the target partition — never
    /// stitch to EOF (for a head-of-file point lookup on a large SSTable that would
    /// decompress nearly the whole `Data.db`, full-table I/O for one partition).
    /// The bound is AUTHORITATIVE, not a heuristic boundary scan:
    ///
    ///   - **`end_bound = Some(end)`** — the caller resolved the SUCCESSOR
    ///     partition's uncompressed start offset (next trie/index entry). The
    ///     target partition occupies `[offset, end)`, so we pull chunks only until
    ///     `window.len() >= end - window_base` (or EOF) and then parse ONCE over a
    ///     window that fully contains the partition. Because the WHOLE `[offset,
    ///     end)` extent is decompressed before parsing, a row/cell that spans
    ///     multiple compression chunks is present in full — no mid-stream
    ///     truncation, no boundary guessing. This is the exact bound for every
    ///     non-last partition in both BTI (`da`) and BIG (`nb`).
    ///
    ///   - **`end_bound = None`** — `offset` is the LAST partition (no successor).
    ///     The end is then the authoritative data-section length
    ///     (`CompressionInfo.data_length`); we buffer to that length (or EOF) and
    ///     parse once. If that length is unavailable (no usable `CompressionInfo`),
    ///     we CANNOT bound the last partition authoritatively, so we return
    ///     `Ok(None)` and the caller falls back to the safe full-scan + retain path
    ///     (correctness over optimization). The previous row-count *stability
    ///     guard* — itself a heuristic that could falsely accept a next-partition
    ///     boundary while the target partition was incomplete (a single large
    ///     multi-chunk cell, static/range-marker regions, or a truncated tail
    ///     parsed as garbage headers) — has been REMOVED entirely.
    ///
    /// The whole-section fallback (uncompressed BTI) already has every byte so its
    /// first parse is authoritative regardless of the bound. This yields
    /// byte-for-byte the same rows as the full-scan path filtered down to
    /// `partition_key`.
    ///
    /// Returns:
    /// - `Ok(Some(rows))` — the partition's rows (empty when the trie/index
    ///   candidate was a prefix collision for an absent key). The caller wraps each
    ///   in a `(RowKey, ScanRow)` and applies the same tombstone suppression the scan
    ///   path applies.
    /// - `Ok(None)` — could not bound the (last) partition authoritatively; the
    ///   caller must fall back to a full scan + retain.
    #[cfg(not(feature = "tombstones"))]
    async fn bti_decompress_and_parse_target_all(
        &self,
        offset: usize,
        end_bound: Option<usize>,
        // Issue #954: when `Some((start_rel, end_rel))`, bound the partition's
        // row-body parse to that within-partition byte window (relative to the
        // partition start) so only the clustering slice's row-index block(s) are
        // decoded. `None` decodes the whole partition (the #953 behaviour).
        row_body_window: Option<(usize, usize)>,
        key: &RowKey,
        table_id: &TableId,
        // See `bti_collect_partition_rows`: `true` iff the manager resolved this
        // reader by an exact fully-qualified `keyspace.table` match (or the query
        // was unqualified). Threaded into the seek table-consistency guard (#1284).
        fully_qualified_match: bool,
        schema_opt: Option<&crate::schema::TableSchema>,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
    ) -> Result<Option<Vec<ScanRow>>> {
        // Issue #1573 (C2): positioned reads on the shared `point_source` — no
        // per-lookup `open(2)`, no cursor, no mutex. `chunk_index` is a plain local.

        // Determine the chunk-targeting parameters. `chunk_length == 0` (or no
        // CompressionInfo) means we cannot chunk-target -> whole-section fallback.
        let chunk_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.chunk_length as usize)
            .filter(|&len| len > 0);

        let mut chunk_index = 0usize;
        let (target_chunk, window_base, mut window) = match chunk_length {
            Some(len) => {
                let (target_chunk, window_base, _within) = Self::bti_chunk_target(offset, len);
                // Positioned reads resolve their own offset from the chunk index.
                chunk_index = target_chunk;
                (target_chunk, window_base, Vec::<u8>::new())
            }
            None => {
                // Whole-section fallback (uncompressed BTI, or chunk_length absent/0):
                // one positioned read of the whole data section, CRC-verified when a
                // CRC.db is present (see `point_read_whole_section`).
                let whole = self.point_read_whole_section().await?;
                (0usize, 0usize, whole)
            }
        };

        if offset < window_base {
            return Err(Error::corruption(format!(
                "BTI single-partition seek: resolved offset {} precedes window base {} (chunk {})",
                offset, window_base, target_chunk
            )));
        }
        let within = offset - window_base;
        let chunk_targeted = chunk_length.is_some();

        if chunk_targeted {
            // Resolve the AUTHORITATIVE exclusive end of the target partition in
            // the UNCOMPRESSED offset domain. Non-last partitions are bounded by
            // the successor partition's start (`end_bound`); the LAST partition is
            // bounded by the data-section length. When NEITHER is known we cannot
            // bound the last partition without re-introducing a heuristic, so we
            // return `Ok(None)` and let the caller fall back to a full scan.
            let end_offset = match end_bound {
                Some(end) => end,
                None => match self
                    .compression_info
                    .as_ref()
                    .map(|ci| ci.data_length as usize)
                    .filter(|&len| len > offset)
                {
                    Some(len) => len,
                    None => {
                        debug!(
                            "BTI single-partition seek: last partition at offset {} has no \
                             authoritative end (no successor, no usable data_length); falling \
                             back to full scan",
                            offset
                        );
                        return Ok(None);
                    }
                },
            };

            // Step 1: buffer enough chunks to expose the partition header, then run
            // the prefix-collision / chunk-straddle gate. This bails out cheaply
            // (without decompressing the rest of the partition) when the trie/index
            // candidate is a prefix collision for an absent key.
            loop {
                // Pull a chunk if the header is not yet (fully) buffered.
                if within + 2 > window.len()
                    || !Self::bti_partition_key_bytes_available(&window, within, key.as_bytes())
                {
                    match self
                        .bti_pull_decompressed_chunk(&mut chunk_index, &mut window)
                        .await?
                    {
                        true => continue, // chunk appended; re-check the header
                        false => {
                            // EOF before the header is buffered: nothing decodable
                            // at the resolved offset.
                            return Ok(Some(Vec::new()));
                        }
                    }
                }

                let key_matches = self.bti_partition_key_matches(&window, within, key.as_bytes());
                if !key_matches {
                    debug!(
                        "BTI seek candidate at offset {} did not match queried key \
                         (prefix collision); treating as absent",
                        offset
                    );
                    return Ok(Some(Vec::new()));
                }
                break; // header buffered AND key matches
            }

            // Step 2: buffer EXACTLY the chunks covering `[offset, end_offset)` —
            // never stitch to EOF (the #953 MEDIUM finding: a head-of-file lookup
            // would otherwise decompress the whole file). `end_offset` is in the
            // same uncompressed-offset domain as `window_base + window.len()`, so
            // the window holds the whole partition once `window.len()` reaches
            // `end_offset - window_base` (or EOF — a stale end never reads past
            // EOF). Decompressing the FULL extent before parsing means a row/cell
            // that spans multiple compression chunks is present in full, so the
            // single parse below collects every target row without truncation.
            let needed = end_offset.saturating_sub(window_base);
            while window.len() < needed {
                if !self
                    .bti_pull_decompressed_chunk(&mut chunk_index, &mut window)
                    .await?
                {
                    break; // EOF: window holds all available bytes.
                }
            }
            return self
                .bti_collect_partition_rows(
                    &window,
                    within,
                    row_body_window,
                    key,
                    table_id,
                    fully_qualified_match,
                    schema_opt,
                    parser,
                )
                .map(|(rows, _complete)| Some(rows));
        }

        // Whole-section fallback (uncompressed BTI): every byte is already present,
        // so the first parse is authoritative.
        if within >= window.len() {
            return Err(Error::corruption(format!(
                "BTI trie resolved Data.db offset {} beyond decompressed data section ({} bytes)",
                offset,
                window.len()
            )));
        }
        self.bti_collect_partition_rows(
            &window,
            within,
            row_body_window,
            key,
            table_id,
            fully_qualified_match,
            schema_opt,
            parser,
        )
        .map(|(rows, _complete)| Some(rows))
    }

    /// Read the next compressed chunk from `cursor`, decompress it (if the reader
    /// has a compression algorithm), and append the decompressed bytes to
    /// `window`. Returns `true` when a chunk was appended, `false` at EOF.
    ///
    /// Shared by the chunk-targeted seek so the header-buffering and
    /// partition-bounding loops use one decompression code path; each call bumps
    /// `work_counters::chunks_decompressed` so a test can prove the seek bounded
    /// its decompression to the target partition's chunk span (Issue #953/#951).
    #[cfg(not(feature = "tombstones"))]
    async fn bti_pull_decompressed_chunk(
        &self,
        chunk_index: &mut usize,
        window: &mut Vec<u8>,
    ) -> Result<bool> {
        use crate::storage::sstable::compression::Compression;
        // Issue #1573 (C2): positioned chunk fetch — no cursor, no mutex, no
        // per-lookup open. CRC is verified inside `point_read_compressed_chunk`
        // BEFORE we decompress here (guardrail #1411).
        match self.point_read_compressed_chunk(*chunk_index)? {
            Some(compressed_chunk) => {
                *chunk_index += 1;
                let decompressed_chunk = if let Some(compression_reader) = &self.compression_reader
                {
                    let compression = Compression::new(*compression_reader.algorithm())?;
                    compression.decompress(&compressed_chunk).map_err(|e| {
                        Error::corruption(format!(
                            "BTI single-partition seek: failed to decompress chunk: {}",
                            e
                        ))
                    })?
                } else {
                    // No compression reader despite CompressionInfo: treat the raw
                    // chunk bytes as already-decompressed data.
                    compressed_chunk
                };
                // Issue #953/#951: count every chunk the seek materializes so a
                // bound test can prove the decompression window is bounded to the
                // target partition's chunk span, not stitched to EOF.
                super::super::super::work_counters::add_chunk_decompressed();
                window.extend_from_slice(&decompressed_chunk);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Parse the buffered `window` from `within`, collecting every row of the
    /// FIRST (target) partition and stopping at the next partition boundary.
    ///
    /// Returns `(rows, saw_next_partition)`:
    /// - `rows` — the target partition's row `Value`s (those whose decoded key
    ///   equals `key` and whose table id matches, issue #831 wrong-table guard),
    ///   in on-disk order.
    /// - `saw_next_partition` — `true` iff the parser emitted a fully-decoded row
    ///   whose partition key DIFFERS from `key`, at which point collection stops.
    ///
    /// Because the caller now decompresses the partition's AUTHORITATIVE byte
    /// extent `[offset, end)` before parsing (the successor offset / data-section
    /// length, issue #953 / #951), the window always fully contains the target
    /// partition — there is no mid-partition truncation to resolve. The
    /// `Break`-on-different-key behaviour is defence in depth: when the window's
    /// final chunk overruns slightly into the next partition (chunks are
    /// fixed-size, so the extent rounds up to a chunk boundary), the first
    /// different-key row terminates collection so no next-partition row is ever
    /// kept. The returned flag is currently informational; the caller does not loop
    /// on it (the bound is authoritative, not boundary-scanned).
    ///
    /// Issue #954: when `row_body_window` is `Some((start_rel, end_rel))` the
    /// parse is bounded to that within-partition byte window (relative to the
    /// partition start, i.e. the `window[within..]` slice domain) so only the
    /// clustering slice's row-index block(s) are decoded. `None` parses the whole
    /// partition (the #953 behaviour).
    #[cfg(not(feature = "tombstones"))]
    fn bti_collect_partition_rows(
        &self,
        window: &[u8],
        within: usize,
        row_body_window: Option<(usize, usize)>,
        key: &RowKey,
        table_id: &TableId,
        // Whether the manager resolved this reader by an EXACT fully-qualified
        // `keyspace.table` match (or an unqualified query). When `false` a
        // fully-qualified query reached this reader via the bare-name fallback, so
        // the seek guard keeps STRICT keyspace matching (#1284 review).
        fully_qualified_match: bool,
        schema_opt: Option<&crate::schema::TableSchema>,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
    ) -> Result<(Vec<ScanRow>, bool)> {
        let mut rows: Vec<ScanRow> = Vec::new();
        let mut saw_next_partition = false;
        // Clamp the window's end to the available bytes (`usize::MAX` means "to the
        // partition end"); the start is already within-partition-relative, which is
        // the same domain as `window[within..]`.
        let clamped_window = row_body_window.map(|(start, end)| {
            let avail = window.len().saturating_sub(within);
            (start.min(avail), end.min(avail))
        });
        parser.parse_block_emit_windowed(
            &window[within..],
            schema_opt,
            self,
            clamped_window,
            |(tid, entry_key, entry_value)| {
                if entry_key.as_bytes() == key.as_bytes() {
                    // Header-authoritative table consistency: wrong-table rejected
                    // (#831); a keyspace-divergent same-table query is served ONLY
                    // when resolution was an exact fully-qualified match — a
                    // fallback-resolved query keeps strict keyspace matching so it
                    // never returns another keyspace's same-named rows (#1284).
                    if table_header_consistent_for_seek(&tid, table_id, fully_qualified_match) {
                        rows.push(entry_value);
                    }
                    Ok(std::ops::ControlFlow::Continue(()))
                } else {
                    // First row of the NEXT partition (the authoritative extent
                    // can overrun into it by up to one chunk). Stop here so no
                    // next-partition row is collected; the target partition's rows
                    // are already complete because its whole extent was buffered.
                    saw_next_partition = true;
                    Ok(std::ops::ControlFlow::Break(()))
                }
            },
        )?;
        Ok((rows, saw_next_partition))
    }

    /// Returns true when the `[flags][key_len: u8][key bytes]` prefix at `within`
    /// is fully present in `window` AND `key_len` equals `expected_key.len()`.
    ///
    /// Used by the chunk-targeted BTI lookup to decide whether the INVARIANT-3
    /// key match can be evaluated yet, or whether more chunk bytes must be pulled
    /// first (issue #831).
    fn bti_partition_key_bytes_available(
        window: &[u8],
        within: usize,
        _expected_key: &[u8],
    ) -> bool {
        // Need flags + key_len byte first.
        if within + 2 > window.len() {
            return false;
        }
        let key_len = window[within + 1] as usize;
        // The declared key bytes must all be buffered. (Whether `key_len` equals
        // the expected length is decided by the subsequent match check, which
        // fails fast on a mismatch — here we only require the bytes be present.)
        within + 2 + key_len <= window.len()
    }

    /// Verify the on-disk partition-key bytes at `offset` in the decompressed
    /// data section equal `expected_key` (issue #831, INVARIANT 3).
    ///
    /// Reads the `[flags][key_len: u8][key bytes]` prefix. Returns `false` (rather
    /// than erroring) on any structural mismatch so the caller can treat the trie
    /// candidate as absent.
    fn bti_partition_key_matches(
        &self,
        decompressed: &[u8],
        offset: usize,
        expected_key: &[u8],
    ) -> bool {
        // Need at least flags + key_len.
        if offset + 2 > decompressed.len() {
            return false;
        }
        let key_len = decompressed[offset + 1] as usize;
        let key_start = offset + 2;
        let key_end = key_start + key_len;
        if key_end > decompressed.len() {
            return false;
        }
        &decompressed[key_start..key_end] == expected_key
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

        log::debug!(
            "SSTableReader::bti_scan_with_metadata - Returning {} results",
            results.len()
        );
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Issue #831: BTI chunk-targeting math + window stop-condition logic
    // =========================================================================

    /// The chunk-index arithmetic must match `CompressionInfo`'s definitions:
    /// `target_chunk = off / chunk_length`, `window_base = target_chunk *
    /// chunk_length`, `within = off - window_base` (== `off % chunk_length`).
    #[test]
    fn bti_chunk_target_arithmetic() {
        // Single-chunk case (simple_table fixture shape): chunk_length 16384,
        // offset 0/63/125 all land in chunk 0 with within == offset.
        let chunk_length = 16384;
        for off in [0usize, 63, 125] {
            let (chunk, base, within) = SSTableReader::bti_chunk_target(off, chunk_length);
            assert_eq!(chunk, 0, "off {off} must be in chunk 0");
            assert_eq!(base, 0, "chunk 0 window base must be 0");
            assert_eq!(within, off, "within must equal offset in chunk 0");
        }

        // Multi-chunk arithmetic with a small chunk_length to exercise the math.
        let cl = 100usize;
        // Exactly on a chunk boundary.
        assert_eq!(SSTableReader::bti_chunk_target(100, cl), (1, 100, 0));
        assert_eq!(SSTableReader::bti_chunk_target(200, cl), (2, 200, 0));
        // Inside chunk 1.
        assert_eq!(SSTableReader::bti_chunk_target(150, cl), (1, 100, 50));
        // Just before a boundary.
        assert_eq!(SSTableReader::bti_chunk_target(99, cl), (0, 0, 99));
        // Within always equals off % chunk_length, base = chunk * chunk_length.
        for off in [0usize, 1, 99, 100, 101, 250, 999] {
            let (chunk, base, within) = SSTableReader::bti_chunk_target(off, cl);
            assert_eq!(within, off % cl);
            assert_eq!(base, chunk * cl);
            assert_eq!(base + within, off);
        }
    }

    /// `bti_partition_key_bytes_available` drives the growing-window stop
    /// condition: while the `[flags][key_len][key bytes]` prefix is NOT yet fully
    /// buffered it returns false (the chunk-targeted loop pulls another chunk);
    /// once the declared key bytes have all arrived it returns true (the
    /// INVARIANT-3 key match can be evaluated). This is the SYNTHETIC spanning
    /// test: the key prefix straddles a simulated chunk boundary and the window
    /// grows one byte at a time across it.
    ///
    /// NOTE: a full multi-chunk-spanning parse against a real
    /// `V5CompressedLegacyParser` has NO real BTI DataOffset fixture — these are
    /// narrow partitions that fit within a single chunk — so the spanning *parse*
    /// path is only exercised structurally here via the byte-availability gate
    /// that decides when a parse may even be attempted. This calls the real
    /// associated function (no I/O), so a regression in its boundary math is
    /// caught.
    #[test]
    fn bti_partition_key_bytes_available_growing_window() {
        // Header at within=0: [flags=0x00][key_len=4][k0 k1 k2 k3]. Simulate a
        // window that grows from 0 bytes up to the full prefix; availability must
        // flip to true exactly when all 4 declared key bytes are buffered.
        let expected_key = [0xAA, 0xBB, 0xCC, 0xDD];
        let within = 0usize;
        let full = {
            let mut v = vec![0x00u8, expected_key.len() as u8];
            v.extend_from_slice(&expected_key);
            v
        };

        let avail = |len: usize| {
            SSTableReader::bti_partition_key_bytes_available(&full[..len], within, &expected_key)
        };

        // Not enough for flags+key_len yet.
        assert!(!avail(0));
        assert!(!avail(1));
        // flags+key_len present but key bytes not fully buffered.
        assert!(!avail(2));
        assert!(!avail(3)); // 1 key byte
        assert!(!avail(4)); // 2 key bytes
        assert!(!avail(5)); // 3 key bytes
                            // All 4 key bytes buffered -> available (boundary fully crossed).
        assert!(avail(6));
        assert!(avail(full.len()));

        // A non-zero `within` (target partition not at window start) must use the
        // same relative math.
        let mut padded = vec![0x77u8, 0x88];
        padded.extend_from_slice(&full);
        assert!(!SSTableReader::bti_partition_key_bytes_available(
            &padded[..2 + 5],
            2,
            &expected_key
        ));
        assert!(SSTableReader::bti_partition_key_bytes_available(
            &padded,
            2,
            &expected_key
        ));
    }
}
