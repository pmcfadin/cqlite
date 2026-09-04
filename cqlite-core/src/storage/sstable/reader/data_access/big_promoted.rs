//! BIG ("nb") promoted-index read/seek (Issue #1184).
//!
//! Consumes the DECODED promoted `IndexInfo` blocks
//! ([`decode_promoted_index`](crate::storage::sstable::promoted_index_reader::decode_promoted_index))
//! for a targeted wide partition — until now only `block_count()` was wired (into
//! stats). Two production surfaces live here:
//!
//! * [`SSTableReader::big_clustering_row_window`] — forward clustering-range block
//!   selection: pick the minimal contiguous block range covering a `ClusteringSlice`
//!   and return the within-partition row-body byte window so the BIG seek decodes
//!   only those blocks (mirrors the BTI [`bti_clustering_row_window`] selector).
//! * [`SSTableReader::big_reverse_partition_rows`] — back-to-front reverse iteration:
//!   walk the blocks last→first, decode each block forward into a bounded buffer and
//!   emit it reversed (mirrors Cassandra `SSTableReversedIterator`). Per-iteration
//!   memory is bounded to ONE block, not the whole partition.
//!
//! No-heuristics (Issue #28): block `firstName`/`lastName` are split with a
//! schema-derived [`PrefixLen`] callback (clustering column types), and block bounds
//! are compared as TYPED clustering values against the slice bounds — never raw
//! byte guessing. Any clustering shape this module cannot decode authoritatively
//! (a variable-width clustering column, or a non-`CLUSTERING` block-name kind such
//! as a range-tombstone bound) makes the selector return `Ok(None)`, so the caller
//! falls back to a correct full-partition decode.
//!
//! [`bti_clustering_row_window`]: SSTableReader::bti_clustering_row_window
//! [`PrefixLen`]: crate::storage::sstable::promoted_index_reader::PrefixLen

#![cfg(not(feature = "tombstones"))]

use super::super::SSTableReader;
use super::model::{ClusteringRowWindow, ClusteringSlice};
use crate::parser::types::{parse_cql_value, CqlTypeId};
use crate::parser::vint::parse_vuint;
use crate::schema::{CqlType, TableSchema};
use crate::storage::sstable::promoted_index_reader::{DecodedIndexInfo, DecodedPromotedIndex};
use crate::storage::sstable::reader::parsing::row_decoder::column_decode_error;
use crate::storage::sstable::reader::parsing::BufferExtent;
use crate::types::{ScanRow, Value};
use crate::{Error, Result, RowKey};
use tracing::debug;

/// `ClusteringPrefix.Kind.CLUSTERING` ordinal (a full row clustering name). Block
/// `firstName`/`lastName` for a row carry this kind byte; range-bound names carry a
/// different ordinal and are not decoded by the fixed-width selector.
const CLUSTERING_PREFIX_KIND_CLUSTERING: u8 = 4;

/// Map a fixed-width clustering CQL type to its on-disk byte width and the
/// [`CqlTypeId`] used to decode a serialized clustering value. Returns `None` for
/// variable-width or unsupported types — the caller then skips narrowing (honest
/// fallback, no guessing).
fn fixed_clustering_type(cql_type: &CqlType) -> Option<(CqlTypeId, usize)> {
    match cql_type {
        CqlType::Boolean => Some((CqlTypeId::Boolean, 1)),
        CqlType::TinyInt => Some((CqlTypeId::Tinyint, 1)),
        CqlType::SmallInt => Some((CqlTypeId::Smallint, 2)),
        CqlType::Int => Some((CqlTypeId::Int, 4)),
        CqlType::BigInt | CqlType::Counter => Some((CqlTypeId::BigInt, 8)),
        CqlType::Float => Some((CqlTypeId::Float, 4)),
        CqlType::Double => Some((CqlTypeId::Double, 8)),
        CqlType::Timestamp => Some((CqlTypeId::Timestamp, 8)),
        CqlType::Date => Some((CqlTypeId::Date, 4)),
        CqlType::Time => Some((CqlTypeId::Time, 8)),
        CqlType::Uuid => Some((CqlTypeId::Uuid, 16)),
        CqlType::TimeUuid => Some((CqlTypeId::Timeuuid, 16)),
        _ => None,
    }
}

/// Resolve the per-clustering-column `(CqlTypeId, width)` list when EVERY clustering
/// column is fixed-width (the precondition for an authoritative fixed `PrefixLen`).
/// `None` when the table has no clustering columns or any is variable-width.
fn fixed_clustering_layout(schema: &TableSchema) -> Option<Vec<(CqlTypeId, usize)>> {
    if schema.clustering_keys.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(schema.clustering_keys.len());
    for ck in &schema.clustering_keys {
        let ty = CqlType::parse(&ck.data_type).ok()?;
        out.push(fixed_clustering_type(&ty)?);
    }
    Some(out)
}

/// Byte length of one serialized `ClusteringPrefix` of kind `CLUSTERING` for an
/// all-fixed-width, all-present clustering: `1` (kind) + header VInt + Σ widths.
///
/// Returns `Err` if the slice's kind byte is not `CLUSTERING` (e.g. a range-bound
/// name) or the slice is too short — the decode then fails and the caller falls
/// back to a full-partition decode.
fn clustering_prefix_len(slice: &[u8], widths: &[usize]) -> Result<usize> {
    match slice.first() {
        Some(&CLUSTERING_PREFIX_KIND_CLUSTERING) => {}
        Some(other) => {
            return Err(Error::Corruption(format!(
                "promoted-index block name kind {other:#x} is not CLUSTERING (range-bound name); \
                 cannot narrow with the fixed-width selector"
            )))
        }
        None => {
            return Err(Error::Corruption(
                "promoted-index block name truncated (no kind byte)".to_string(),
            ))
        }
    }
    // Clustering values header VInt (2 bits/column; 0 for all-present). Parse it for
    // its byte length rather than assuming one byte.
    let header_len = match parse_vuint(&slice[1..]) {
        Ok((rest, _v)) => (slice.len() - 1) - rest.len(),
        Err(_) => {
            return Err(Error::Corruption(
                "promoted-index block name truncated (clustering header VInt)".to_string(),
            ))
        }
    };
    let total = 1 + header_len + widths.iter().sum::<usize>();
    if slice.len() < total {
        return Err(Error::Corruption(format!(
            "promoted-index block name needs {total} bytes, slice has {}",
            slice.len()
        )));
    }
    Ok(total)
}

/// Decode the FIRST clustering column's typed [`Value`] from a serialized
/// `CLUSTERING`-kind block name (`[kind][header][value bytes…]`). Returns `None`
/// when the name is not decodable as a fixed-width first column (the caller then
/// treats that bound conservatively as open).
fn first_ck_value(name: &[u8], first: (CqlTypeId, usize)) -> Option<Value> {
    if name.first() != Some(&CLUSTERING_PREFIX_KIND_CLUSTERING) {
        return None;
    }
    let (_rest, _hdr) = parse_vuint(name.get(1..)?).ok()?;
    let header_len = (name.len() - 1) - _rest.len();
    let start = 1 + header_len;
    let (type_id, width) = first;
    let bytes = name.get(start..start + width)?;
    parse_cql_value(bytes, type_id).ok().map(|(_, v)| v)
}

impl SSTableReader {
    /// Resolve the within-partition row-body byte window covering a single-column
    /// clustering slice for a BIG (`nb`) wide partition, using the partition's
    /// decoded promoted `IndexInfo` blocks (Issue #1184).
    ///
    /// Mirrors [`bti_clustering_row_window`](Self::bti_clustering_row_window): pick
    /// the minimal contiguous block range whose `[first_name, last_name]` clustering
    /// envelopes intersect the slice, then return `[first block offset, end of last
    /// selected block)` relative to the partition start (the same domain the parser
    /// sees for `window[within..]`). Block selection is over-inclusive by block
    /// granularity; the post-scan backstop applies the exact bound.
    ///
    /// Returns `Ok(None)` (decode the whole partition, honest `PartitionLookup`)
    /// when narrowing is not authoritative or not useful: no Index.db entry / no
    /// promoted index (a narrow partition), a variable-width clustering column, an
    /// un-decodable block name, a block carrying an open range-tombstone marker, or
    /// an empty block selection.
    pub(super) async fn big_clustering_row_window(
        &self,
        partition_key: &[u8],
        slice: &ClusteringSlice,
        schema: Option<&TableSchema>,
    ) -> Result<Option<ClusteringRowWindow>> {
        let Some(schema) = schema else {
            return Ok(None);
        };
        let Some(layout) = fixed_clustering_layout(schema) else {
            return Ok(None);
        };
        let Some(decoded) = self
            .decode_partition_promoted_index(partition_key, &layout)
            .await?
        else {
            return Ok(None);
        };
        if decoded.entries.is_empty() {
            return Ok(None);
        }
        // CORRECTNESS GUARD (mirror BTI): an open range-tombstone marker at a block
        // boundary can shadow rows inside the slice from an earlier block, which a
        // start-narrowed decode would skip. Fall back to a full decode (the writer
        // emits 0x00 today, so this is defensive).
        if decoded.entries.iter().any(|b| b.end_open_marker.is_some()) {
            debug!(
                "BIG clustering seek: promoted index has open range-tombstone marker(s); \
                 decoding full partition to preserve range-deletion semantics"
            );
            return Ok(None);
        }

        let first = layout[0];
        let Some((lo, hi)) = select_blocks_for_slice(&decoded.entries, slice, first) else {
            return Ok(None);
        };

        // A static row precedes the clustering rows and must be merged into each
        // emitted row, so only fast-forward past it when there are NO static columns.
        let has_static = schema.columns.iter().any(|c| c.is_static);
        let body_start_rel = if has_static {
            0
        } else {
            decoded.entries[lo].offset as usize
        };
        // Exclusive end = start of the first block after the last selected one, or
        // +∞ (partition end, clamped by the caller) when the last block is selected.
        let body_end_rel = if hi + 1 < decoded.entries.len() {
            decoded.entries[hi + 1].offset as usize
        } else {
            usize::MAX
        };
        Ok(Some(ClusteringRowWindow {
            body_start_rel,
            body_end_rel,
        }))
    }

    /// Unified clustering-slice → `(row_body_window, decode_end_bound, engaged)`
    /// resolver shared by the BTI and BIG single-partition seek paths (Issue #954 /
    /// #1184). Extracted from `scan_single_partition_clustering` so the BIG branch
    /// adds no lines to the (over-threshold) `bti.rs`.
    ///
    /// `offset` is the partition's UNCOMPRESSED Data.db start; `end_bound` the
    /// authoritative partition end (successor offset / data length). Returns the
    /// within-partition `(start, end)` byte window, a tightened decode end bound,
    /// and whether the clustering narrowing engaged.
    #[allow(clippy::type_complexity)]
    pub(super) async fn resolve_clustering_seek_window(
        &self,
        is_bti: bool,
        partition_key: &[u8],
        offset: u64,
        clustering: Option<&ClusteringSlice>,
        schema: Option<&TableSchema>,
        end_bound: Option<usize>,
    ) -> Result<(Option<(usize, usize)>, Option<usize>, bool)> {
        let Some(slice) = clustering else {
            return Ok((None, end_bound, false));
        };
        let narrow = if is_bti {
            self.bti_clustering_row_window(partition_key, slice, schema)?
        } else {
            self.big_clustering_row_window(partition_key, slice, schema)
                .await?
        };
        let Some(narrow) = narrow else {
            return Ok((None, end_bound, false));
        };
        // Tighten the decompression end to the slice's upper block extent (a bounded
        // `body_end_rel`; `usize::MAX` means "to the partition end").
        let mut decode_end_bound = end_bound;
        if narrow.body_end_rel != usize::MAX {
            let abs_end = (offset as usize).saturating_add(narrow.body_end_rel);
            decode_end_bound = Some(match end_bound {
                Some(e) => abs_end.min(e),
                None => abs_end,
            });
        }
        Ok((
            Some((narrow.body_start_rel, narrow.body_end_rel)),
            decode_end_bound,
            true,
        ))
    }

    /// Decode this reader's promoted `IndexInfo` blocks for `partition_key`, using a
    /// schema-derived fixed-width `PrefixLen` (Issue #1184). Returns `Ok(None)` when
    /// the partition has no Index.db entry or no promoted index (a narrow partition).
    /// Decode errors (e.g. a non-`CLUSTERING` block name) surface as `Err` so the
    /// caller can treat them as a non-narrowable fallback.
    async fn decode_partition_promoted_index(
        &self,
        partition_key: &[u8],
        layout: &[(CqlTypeId, usize)],
    ) -> Result<Option<DecodedPromotedIndex>> {
        let Some(index_reader) = self.index_reader.as_ref() else {
            return Ok(None);
        };
        // Issue #2412 Stage 2: a lazily-opened reader defers the full parse to
        // first use — this promoted-index lookup IS that first use. No-op for an
        // eagerly-opened reader.
        index_reader.ensure_materialized(&self.scan_cancel).await?;
        let Some(entry) = index_reader.lookup_partition(partition_key) else {
            return Ok(None);
        };
        let Some(promoted) = entry.promoted_index.as_ref() else {
            return Ok(None);
        };
        if promoted.is_empty() {
            return Ok(None);
        }
        let widths: Vec<usize> = layout.iter().map(|(_, w)| *w).collect();
        let prefix_len = move |slice: &[u8]| clustering_prefix_len(slice, &widths);
        // A non-CLUSTERING block name (range bound) or truncation makes the fixed
        // selector inapplicable — surface as Ok(None) so the caller full-decodes,
        // rather than erroring the whole query.
        match promoted.decode(&prefix_len) {
            Ok(decoded) => Ok(Some(decoded)),
            Err(e) => {
                debug!("BIG promoted-index decode not narrowable ({e}); full-partition fallback");
                Ok(None)
            }
        }
    }

    /// Reverse partition iteration for a BIG (`nb`) wide partition (Issue #1184):
    /// walk the decoded promoted `IndexInfo` blocks LAST→FIRST, decode each block
    /// forward into a bounded buffer, and emit it reversed — so the partition is
    /// returned in descending clustering order without a post-fetch in-memory sort
    /// over a full forward read. Per-iteration memory is bounded to ONE block
    /// (mirrors Cassandra `SSTableReversedIterator`).
    ///
    /// Returns `Ok(Some(rows))` (rows in descending clustering order, tombstone
    /// suppressed) when the reverse iterator applied, or `Ok(None)` to fall back to
    /// the in-memory sort: no Index.db offset, a narrow partition with no promoted
    /// index, a variable-width clustering column, a static column present, or an
    /// open range-tombstone marker.
    ///
    /// # Work complexity (Issue #1307)
    ///
    /// Per-iteration **memory** is bounded to ONE promoted-index block: `block_rows`
    /// holds only the rows of the block currently being decoded, is drained into
    /// `out`, and never accumulates more than a single block at a time (mirrors
    /// Cassandra `SSTableReversedIterator`).
    ///
    /// **Total work is O(partition), independent of any `LIMIT`.** This function
    /// walks EVERY promoted-index block back-to-front and materializes the full
    /// partition's rows into `out` before returning; the query-wide `LIMIT` is
    /// applied by the executor's `Limit` step *after* this call. So `ORDER BY ck DESC
    /// LIMIT n` still decodes the whole partition — it is O(partition), not O(n). The
    /// reverse iterator's win over the in-memory-sort fallback is the bounded
    /// per-block memory high-water mark (and avoiding a full forward re-read to sort),
    /// NOT reduced total decode work.
    ///
    /// A back-to-front early-stop (stop once `n` rows are buffered, giving O(n)) is
    /// intentionally NOT implemented: the caller (`targeted_partition_rows`) applies a
    /// post-scan predicate backstop (e.g. `WHERE ck < N ORDER BY ck DESC`) and the
    /// `LIMIT` is applied only *after* that filtering, so truncating the raw scan at
    /// `n` rows here could drop rows a predicate would have removed and under-return.
    /// Threading a *predicate-aware* effective limit down through
    /// `scan_partition_clustering_reverse` is left as future work (Low, Issue #1307).
    pub(crate) async fn big_reverse_partition_rows(
        &self,
        partition_key: &[u8],
        schema: Option<&TableSchema>,
    ) -> Result<Option<Vec<(RowKey, ScanRow)>>> {
        let Some(schema) = schema else {
            return Ok(None);
        };
        // Static columns precede the clustering rows; isolating one block would drop
        // the static prefix. Out of scope here → in-memory sort fallback.
        if schema.columns.iter().any(|c| c.is_static) {
            return Ok(None);
        }
        let Some(layout) = fixed_clustering_layout(schema) else {
            return Ok(None);
        };
        let Some(decoded) = self
            .decode_partition_promoted_index(partition_key, &layout)
            .await?
        else {
            return Ok(None);
        };
        if decoded.entries.is_empty() {
            return Ok(None);
        }
        if decoded.entries.iter().any(|b| b.end_open_marker.is_some()) {
            return Ok(None);
        }

        // Resolve the partition's UNCOMPRESSED Data.db offset (BIG Index.db). A miss
        // means the index can't authoritatively locate it → fall back.
        let Some((offset, _size)) = self.lookup_partition_with_index(partition_key).await? else {
            return Ok(None);
        };
        let end_bound = self
            .successor_partition_offset(offset, partition_key)
            .await?
            .map(|e| e as usize);

        // Decompress exactly the chunks covering the target partition ONCE; block
        // windowing then bounds each per-iteration decode to one block.
        let Some((window, within)) = self
            .decompress_partition_window(offset as usize, end_bound)
            .await?
        else {
            return Ok(None);
        };

        let parser = self.build_v5_parser(true);
        let key = RowKey::from(partition_key.to_vec());
        let avail = window.len().saturating_sub(within);
        let mut out: Vec<(RowKey, ScanRow)> = Vec::new();

        for block in decoded.entries.iter().rev() {
            let body_start = (block.offset as usize).min(avail);
            let body_end = (block.offset as usize)
                .saturating_add(block.width as usize)
                .min(avail);
            let mut block_rows: Vec<ScanRow> = Vec::new();
            // #3782: a chunk-covering partition window; its tail may cut a row.
            let walk = parser.parse_block_emit_windowed(
                &window[within..],
                BufferExtent::Window,
                Some(schema),
                self,
                Some((body_start, body_end)),
                |(_tid, entry_key, entry_value)| {
                    if entry_key.as_bytes() == partition_key {
                        block_rows.push(entry_value);
                    }
                    Ok(std::ops::ControlFlow::Continue(()))
                },
            );
            if let Err(e) = walk {
                // Issue #3721: this INDEX-POSITIONED walk may not answer with the
                // rows collected so far — see `indexed_walk_falls_back`.
                if column_decode_error::indexed_walk_falls_back(&e) {
                    return Ok(None);
                }
                return Err(e);
            }
            // Per-iteration memory high-water mark + block-walk evidence (#1184).
            crate::storage::sstable::work_counters::observe_reverse_block_rows(
                block_rows.len() as u64
            );
            crate::storage::sstable::work_counters::add_reverse_block_decoded();
            // Within a block rows are ascending; reverse to descending, then append.
            for value in block_rows.into_iter().rev() {
                if self.filter_tombstone(&value) {
                    out.push((key.clone(), value));
                }
            }
        }
        Ok(Some(out))
    }

    /// Decompress exactly the chunk window covering the partition `[offset,
    /// end_bound)` (UNCOMPRESSED domain) and return `(window, within)` where
    /// `within = offset - window_base` is the partition start inside `window`
    /// (Issue #1184). `Ok(None)` when the last partition cannot be bounded
    /// authoritatively (no successor and no data length).
    ///
    /// This mirrors the window-building half of the BTI seek but is kept local so
    /// the over-threshold `bti.rs` is not grown.
    ///
    /// I/O path (issue #1573 C2, #1869): chunks are fetched with positioned
    /// (`read_at`) reads on the shared `point_source` — no per-query `new_scan_cursor`
    /// / `open(2)`, no cursor, and no cross-I/O mutex — exactly like the BTI seek
    /// (`bti_decompress_and_parse_target_all`) and the compressed offset-read window
    /// (`read_compressed_offset_window`). CRC-before-decompress (guardrail #1411) is
    /// preserved: `read_compressed_chunk_at` verifies each compressed chunk's inline
    /// CRC32 before it is decompressed, and the uncompressed arm verifies the covering
    /// `CRC.db` chunk(s) before returning bytes.
    pub(super) async fn decompress_partition_window(
        &self,
        offset: usize,
        end_bound: Option<usize>,
    ) -> Result<Option<(Vec<u8>, usize)>> {
        use crate::storage::sstable::compression::Compression;

        let chunk_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.chunk_length as usize)
            .filter(|&len| len > 0);

        match chunk_length {
            Some(_len) => {
                // Compressed Data.db: the window-building is a pure function of
                // `CompressionInfo` + the positional source, so it lives in
                // `compressed_partition_window` where it is unit-tested directly against
                // hand-built fixtures (`big_promoted_seek_tests::window_builder`), without a full
                // write-engine + compressing-writer roundtrip (issue #1869).
                let comp_info = self.compression_info.as_deref().ok_or_else(|| {
                    Error::corruption(
                        "BIG clustering/reverse seek: chunk-targeted path requires CompressionInfo \
                         but it is absent",
                    )
                })?;
                let compression = self
                    .compression_reader
                    .as_ref()
                    .map(|cr| Compression::new(*cr.algorithm()))
                    .transpose()?;
                compressed_partition_window(
                    self.point_source.as_ref(),
                    comp_info,
                    compression.as_ref(),
                    self.stats.file_size,
                    offset,
                    end_bound,
                )
            }
            None => {
                // Uncompressed Data.db: the data section is RAW bytes after the
                // header, so read ONLY the partition's `[offset, end_bound)` span
                // directly instead of stitching the ENTIRE file (Finding 3, roborev
                // #1184). Stitching materialized O(file); the window must be
                // O(partition) — exactly the bound the compressed arm already holds —
                // so the per-iteration "O(block), not O(partition)" claim is honest
                // for uncompressed SSTables too. `offset`/`end_bound` are relative to
                // the data section (after the header); we keep window_base = offset so
                // within = 0.
                let header_size = self.calculate_header_size() as u64;
                let phys_start = header_size.saturating_add(offset as u64);
                let phys_end = match end_bound {
                    Some(end) => header_size.saturating_add(end as u64),
                    // Last partition: extends to the end of the Data.db data section.
                    // The authoritative file length comes from the positional source
                    // (== the reader's `file_size`), so no seek-to-end is needed.
                    None => self.point_source.len(),
                };
                if phys_end <= phys_start {
                    return Ok(None);
                }
                let span = (phys_end - phys_start) as usize;
                // Issue #1396: the promoted-index / reverse-lookup path reads
                // uncompressed Data.db bytes directly; route it through the single
                // CRC-checked positional accessor so a corrupt chunk yields a typed
                // Error::Corruption instead of parsed corrupt bytes / Ok(None).
                let buf = self.read_uncompressed_verified_at(phys_start, span).await?;
                Ok(Some((buf, 0)))
            }
        }
    }

    /// Positional (`pread`) sibling of `read_uncompressed_verified` for this BIG
    /// clustering/reverse seek path (issue #1573 C2, #1869): read `len` raw bytes at
    /// an ABSOLUTE Data.db `offset` on the shared `point_source`, verifying the
    /// covering `CRC.db` chunk(s) BEFORE returning any bytes.
    ///
    /// Unlike the cursor-based `read_uncompressed_verified` this takes no
    /// `BlockSource` cursor and no mutex — the offset is a parameter, so concurrent
    /// seek reads never serialize on a shared file position and never `open(2)` per
    /// query. The CRC check (guardrail #1411) and its typed `Error::Corruption` on
    /// mismatch are identical to the cursor path; the verifier is a no-op when this
    /// reader has no `CRC.db`. Lives here (next to its sole caller) rather than in
    /// the over-threshold `data_access/mod.rs` (campsite rule, epic #1116).
    async fn read_uncompressed_verified_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let size = u32::try_from(len).map_err(|_| {
            Error::corruption(format!(
                "uncompressed read length {len} exceeds u32 range for CRC verification \
                 at Data.db offset 0x{offset:x}"
            ))
        })?;
        // Verify the covering CRC.db chunk(s) BEFORE returning any bytes, on the
        // SAME plane the bytes themselves come off (issue #2876): this is a POINT
        // path, so both stay on the advised `MADV_RANDOM` mapping (issue #2210).
        let point_source = self.point_source.clone();
        self.verify_uncompressed_range(point_source.as_ref(), offset, size)
            .await?;

        let mut buf = vec![0u8; len];
        point_source.read_exact_at(offset, &mut buf)?;
        Ok(super::super::chunk_source::counted_raw_chunk(buf, None)) // #1701 raw exit
    }
}

/// Build the compressed-arm partition window covering `[offset, end_bound)` in the
/// UNCOMPRESSED domain and return `(window, within)` where `within = offset -
/// window_base` is the partition start inside `window` (issue #1184). `Ok(None)` when
/// the last partition cannot be bounded authoritatively (no successor offset and no
/// usable `data_length`).
///
/// Extracted from [`SSTableReader::decompress_partition_window`] (issue #1869) so the
/// window arithmetic is a pure function of `CompressionInfo` + the positional
/// [`ReadAt`](super::super::read_at::ReadAt) source and can be unit-tested directly
/// against hand-built fixtures (`big_promoted_seek_tests::window_builder`) — no full write-engine +
/// compressing-writer roundtrip. I/O-path parity with `read_compressed_offset_window`
/// (`compressed_offset.rs`) is preserved: CRC-before-decompress via
/// [`read_compressed_chunk_at`](super::super::block_io::read_compressed_chunk_at), and
/// the incompressible / raw-chunk fallback (Bug #639) for chunks Cassandra stored
/// uncompressed.
pub(super) fn compressed_partition_window(
    point_source: &dyn super::super::read_at::ReadAt,
    comp_info: &crate::storage::sstable::compression_info::CompressionInfo,
    compression: Option<&crate::storage::sstable::compression::Compression>,
    file_size: u64,
    offset: usize,
    end_bound: Option<usize>,
) -> Result<Option<(Vec<u8>, usize)>> {
    use super::super::block_io;
    use super::super::chunk_source::{count_raw_chunk, ChunkSource};

    let len = comp_info.chunk_length as usize;
    if len == 0 {
        return Err(Error::corruption(
            "BIG clustering/reverse seek: CompressionInfo chunk_length is zero; cannot map a \
             Data.db offset to a compressed chunk",
        ));
    }

    let target_chunk = offset / len;
    let window_base = target_chunk * len;
    if offset < window_base {
        return Err(Error::corruption(format!(
            "BIG clustering/reverse seek: resolved offset {offset} precedes window base \
             {window_base}"
        )));
    }
    let within = offset - window_base;

    // Authoritative exclusive end (successor offset / data length).
    let end_offset = match end_bound {
        Some(end) => end,
        None => {
            let data_length = comp_info.data_length as usize;
            if data_length > offset {
                data_length
            } else {
                return Ok(None);
            }
        }
    };

    // Fail closed on an out-of-range starting chunk (malformed/corrupt promoted-index
    // offset): otherwise the loop below would `break` on the FIRST
    // `read_compressed_chunk_at` EOF signal, leaving `window` empty while `within > 0`,
    // and the caller's `&window[within..]` slice would PANIC. Match the typed
    // corruption error the pre-#1869 code produced via
    // `compressed_chunk_offset(..).ok_or_else(..)`.
    if target_chunk >= comp_info.chunk_offsets.len() {
        return Err(Error::corruption(format!(
            "BIG clustering/reverse seek: resolved chunk {target_chunk} is out of range (only {} \
             compressed chunk(s) in CompressionInfo)",
            comp_info.chunk_offsets.len()
        )));
    }

    // Buffer EXACTLY the chunks covering `[offset, end_offset)` — never stitch to EOF
    // (the #953/#1184 bound: a head-of-file seek must not decompress the whole file).
    // Positioned reads resolve their own offset from the chunk index, so no pre-seek is
    // needed.
    let needed = end_offset.saturating_sub(window_base);
    let max_compressed_length = comp_info.max_compressed_length as usize;
    // Fail closed on a corrupt `CompressionInfo` whose `max_compressed_length == 0`:
    // otherwise the raw-chunk fallback below (`compressed.len() >= max_compressed_length`)
    // is ALWAYS true, so EVERY still-compressed chunk would be returned verbatim as
    // "raw/incompressible" plaintext — never decompressed — and the inline CRC32 (computed
    // over the genuine on-disk bytes) would still pass, silently handing back garbage rows.
    // A valid Cassandra `CompressionInfo` never records a zero `max_compressed_length`.
    if max_compressed_length == 0 {
        return Err(Error::corruption(
            "BIG clustering/reverse seek: CompressionInfo max_compressed_length is zero; \
             cannot distinguish compressed chunks from raw/incompressible ones",
        ));
    }
    let mut window = Vec::<u8>::new();
    let mut chunk_index = target_chunk;
    while window.len() < needed {
        match block_io::read_compressed_chunk_at(
            point_source,
            comp_info,
            chunk_index,
            file_size,
            0, // NB: chunk offsets are absolute from Data.db byte 0
        )? {
            Some(compressed) => {
                chunk_index += 1;
                // Incompressible / raw-chunk fallback (Bug #639, epic #970): Cassandra
                // stores a chunk RAW (uncompressed) when its would-be compressed length
                // meets or exceeds `max_compressed_length`; those bytes are already
                // plaintext, so routing them through the decompressor would fail with a
                // spurious LZ4-decode corruption error on real Cassandra data. Mirror
                // `read_compressed_offset_window` (compressed_offset.rs) exactly. The
                // CRC32 is already validated by `read_compressed_chunk_at` above.
                if compressed.len() >= max_compressed_length {
                    count_raw_chunk(&compressed, compression.map(|c| c.algorithm()));
                    window.extend_from_slice(&compressed);
                } else {
                    // Decompress-only (uncached): historical behavior + its own counter.
                    let decompressed = ChunkSource::decompress_only(compression, compressed)?;
                    crate::storage::sstable::work_counters::add_chunk_decompressed();
                    window.extend_from_slice(&decompressed);
                }
            }
            // EOF. Reaching `None` after ≥1 successful chunk read is the intended "read
            // to end of window" stop. But if we haven't even collected enough bytes to
            // satisfy the caller's `within` offset, the length metadata is
            // inconsistent/corrupt — fail closed rather than hand back a short `window`
            // the caller would slice past.
            None => {
                if window.len() < within {
                    return Err(Error::corruption(format!(
                        "BIG clustering/reverse seek: hit EOF at chunk {chunk_index} after {} \
                         byte(s), before reaching the resolved intra-window offset {within}",
                        window.len()
                    )));
                }
                break;
            }
        }
    }

    // Single choke-point guard (issue #1869): EVERY way the loop can exit with an
    // insufficient window converges here. In particular a non-monotonic/corrupt
    // successor or end bound with `end_offset <= window_base` yields `needed == 0`, so
    // the loop body never runs and `window` stays EMPTY while `within > 0`; the up-front
    // out-of-range guard does NOT fire (target_chunk can be perfectly in range).
    // Returning `Ok(Some((<empty window>, within)))` would make both callers'
    // `&window[within..]` slice PANIC. For a VALID read the covering chunk is read in
    // full, so `within < len <= window.len()` and this never rejects a legitimate case.
    if window.len() < within {
        return Err(Error::corruption(format!(
            "BIG clustering/reverse seek: resolved window is {} byte(s) — shorter than the \
             required intra-window offset {within} (corrupt successor/end bound)",
            window.len()
        )));
    }

    Ok(Some((window, within)))
}

/// Select the minimal contiguous block index range `[lo, hi]` whose clustering
/// envelopes intersect `slice`, comparing TYPED first-column values (no-heuristics).
/// `None` when no block intersects (caller full-decodes). A block whose bound is
/// not decodable as a fixed-width first column is treated as open on that side
/// (conservatively included) — correct because selection is over-inclusive and the
/// post-scan backstop applies the exact bound.
fn select_blocks_for_slice(
    entries: &[DecodedIndexInfo],
    slice: &ClusteringSlice,
    first: (CqlTypeId, usize),
) -> Option<(usize, usize)> {
    let start_bound = slice.start.first();
    let end_bound = slice.end.first();
    let mut lo: Option<usize> = None;
    let mut hi: Option<usize> = None;
    for (i, block) in entries.iter().enumerate() {
        // lower_ok: block_last >= slice.start (over-inclusive). Undecodable → open.
        let lower_ok = match start_bound {
            None => true,
            Some(lo_val) => match first_ck_value(&block.last_name, first) {
                Some(block_last) => block_last
                    .partial_cmp(lo_val)
                    .map(|o| o != std::cmp::Ordering::Less)
                    .unwrap_or(true),
                None => true,
            },
        };
        // upper_ok: block_first <= slice.end. Undecodable → open.
        let upper_ok = match end_bound {
            None => true,
            Some(hi_val) => match first_ck_value(&block.first_name, first) {
                Some(block_first) => block_first
                    .partial_cmp(hi_val)
                    .map(|o| o != std::cmp::Ordering::Greater)
                    .unwrap_or(true),
                None => true,
            },
        };
        if lower_ok && upper_ok {
            lo.get_or_insert(i);
            hi = Some(i);
        }
    }
    match (lo, hi) {
        (Some(lo), Some(hi)) => Some((lo, hi)),
        _ => None,
    }
}
