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

use super::super::source::ScanCursor;
use super::super::SSTableReader;
use super::model::{ClusteringRowWindow, ClusteringSlice};
use crate::parser::types::{parse_cql_value, CqlTypeId};
use crate::parser::vint::parse_vuint;
use crate::schema::{CqlType, TableSchema};
use crate::storage::sstable::promoted_index_reader::{DecodedIndexInfo, DecodedPromotedIndex};
use crate::types::Value;
use crate::{Error, Result, RowKey};
use log::debug;
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;

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
    pub(super) fn big_clustering_row_window(
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
        let Some(decoded) = self.decode_partition_promoted_index(partition_key, &layout)? else {
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
    pub(super) fn resolve_clustering_seek_window(
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
            self.big_clustering_row_window(partition_key, slice, schema)?
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
    fn decode_partition_promoted_index(
        &self,
        partition_key: &[u8],
        layout: &[(CqlTypeId, usize)],
    ) -> Result<Option<DecodedPromotedIndex>> {
        let Some(index_reader) = self.index_reader.as_ref() else {
            return Ok(None);
        };
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
    pub(super) async fn big_reverse_partition_rows(
        &self,
        partition_key: &[u8],
        schema: Option<&TableSchema>,
    ) -> Result<Option<Vec<(RowKey, Value)>>> {
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
        let Some(decoded) = self.decode_partition_promoted_index(partition_key, &layout)? else {
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
        let end_bound = self.successor_partition_offset(offset)?.map(|e| e as usize);

        // Decompress exactly the chunks covering the target partition ONCE; block
        // windowing then bounds each per-iteration decode to one block.
        let Some((window, within)) = self
            .decompress_partition_window(offset as usize, end_bound)
            .await?
        else {
            return Ok(None);
        };

        let parser = self.build_v5_parser();
        let key = RowKey::from(partition_key.to_vec());
        let avail = window.len().saturating_sub(within);
        let mut out: Vec<(RowKey, Value)> = Vec::new();

        for block in decoded.entries.iter().rev() {
            let body_start = (block.offset as usize).min(avail);
            let body_end = (block.offset as usize)
                .saturating_add(block.width as usize)
                .min(avail);
            let mut block_rows: Vec<Value> = Vec::new();
            parser.parse_block_emit_windowed(
                &window[within..],
                Some(schema),
                self,
                Some((body_start, body_end)),
                |(_tid, entry_key, entry_value)| {
                    if entry_key.as_bytes() == partition_key {
                        block_rows.push(entry_value);
                    }
                    Ok(std::ops::ControlFlow::Continue(()))
                },
            )?;
            // Per-iteration memory high-water mark + block-walk evidence (#1184).
            crate::storage::sstable::work_counters::observe_reverse_block_rows(
                block_rows.len() as u64,
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

    /// Decode the rows of a single BIG (`nb`) wide partition bounded to a
    /// clustering-slice block window (Issue #1184). Resolves the partition's
    /// decompressed window, then runs the windowed parser over only the selected
    /// block extent (`row_body_window`), collecting the target partition's rows.
    ///
    /// The target partition is identified by a partition-key-bytes equality check
    /// only — the reader is already scoped to a single table by the manager, so
    /// (exactly as the full-scan `scan().retain(matches_key)` fallback does) no
    /// table-id match is applied. This decodes SSTables whose serialization-header
    /// keyspace/table differ from a fully-qualified query id. Returns `Ok(None)`
    /// when the partition window cannot be resolved (caller falls back to the full
    /// scan).
    pub(super) async fn big_decode_clustering_window(
        &self,
        partition_key: &[u8],
        offset: u64,
        end_bound: Option<usize>,
        row_body_window: Option<(usize, usize)>,
        schema: Option<&TableSchema>,
    ) -> Result<Option<Vec<(RowKey, Value)>>> {
        let Some((window, within)) = self
            .decompress_partition_window(offset as usize, end_bound)
            .await?
        else {
            return Ok(None);
        };
        let parser = self.build_v5_parser();
        let key = RowKey::from(partition_key.to_vec());
        let avail = window.len().saturating_sub(within);
        let clamped = row_body_window.map(|(s, e)| (s.min(avail), e.min(avail)));
        let mut rows: Vec<(RowKey, Value)> = Vec::new();
        parser.parse_block_emit_windowed(
            &window[within..],
            schema,
            self,
            clamped,
            |(_tid, entry_key, entry_value)| {
                if entry_key.as_bytes() == partition_key {
                    if self.filter_tombstone(&entry_value) {
                        rows.push((key.clone(), entry_value));
                    }
                    Ok(std::ops::ControlFlow::Continue(()))
                } else {
                    // First row of the next partition — stop.
                    Ok(std::ops::ControlFlow::Break(()))
                }
            },
        )?;
        Ok(Some(rows))
    }

    /// Decompress exactly the chunk window covering the partition `[offset,
    /// end_bound)` (UNCOMPRESSED domain) and return `(window, within)` where
    /// `within = offset - window_base` is the partition start inside `window`
    /// (Issue #1184). `Ok(None)` when the last partition cannot be bounded
    /// authoritatively (no successor and no data length).
    ///
    /// This mirrors the window-building half of the BTI seek but is kept local so
    /// the over-threshold `bti.rs` is not grown.
    async fn decompress_partition_window(
        &self,
        offset: usize,
        end_bound: Option<usize>,
    ) -> Result<Option<(Vec<u8>, usize)>> {
        let cursor = self.new_scan_cursor().await?;
        let chunk_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.chunk_length as usize)
            .filter(|&len| len > 0);

        let (window_base, mut window) = match chunk_length {
            Some(len) => {
                let target_chunk = offset / len;
                let window_base = target_chunk * len;
                let chunk_start = self
                    .compression_info
                    .as_ref()
                    .and_then(|ci| ci.compressed_chunk_offset(target_chunk))
                    .ok_or_else(|| {
                        Error::corruption(format!(
                            "BIG reverse seek: no compressed offset for target chunk {target_chunk} \
                             (offset {offset}, chunk_length {len})"
                        ))
                    })?;
                {
                    let mut file_guard = cursor.file.lock().await;
                    file_guard.seek(SeekFrom::Start(chunk_start)).await?;
                }
                cursor
                    .chunk_index
                    .store(target_chunk, std::sync::atomic::Ordering::Relaxed);
                (window_base, Vec::<u8>::new())
            }
            None => {
                let header_size = self.calculate_header_size();
                {
                    let mut file_guard = cursor.file.lock().await;
                    file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
                }
                (0usize, self.stitch_all_chunks(&cursor).await?)
            }
        };

        if offset < window_base {
            return Err(Error::corruption(format!(
                "BIG reverse seek: resolved offset {offset} precedes window base {window_base}"
            )));
        }
        let within = offset - window_base;

        if chunk_length.is_some() {
            // Authoritative exclusive end (successor offset / data length).
            let end_offset = match end_bound {
                Some(end) => end,
                None => match self
                    .compression_info
                    .as_ref()
                    .map(|ci| ci.data_length as usize)
                    .filter(|&len| len > offset)
                {
                    Some(len) => len,
                    None => return Ok(None),
                },
            };
            let needed = end_offset.saturating_sub(window_base);
            while window.len() < needed {
                if !self.pull_reverse_chunk(&cursor, &mut window).await? {
                    break; // EOF
                }
            }
        }
        Ok(Some((window, within)))
    }

    /// Read+decompress the next chunk into `window`, returning `false` at EOF.
    /// Bumps `chunks_decompressed` so the reverse path's decompression stays
    /// observably bounded to the target partition's chunk span.
    async fn pull_reverse_chunk(&self, cursor: &ScanCursor, window: &mut Vec<u8>) -> Result<bool> {
        use crate::storage::sstable::compression::Compression;
        match self.read_next_block(cursor).await? {
            Some(compressed_chunk) => {
                let decompressed = if let Some(reader) = &self.compression_reader {
                    let compression = Compression::new(*reader.algorithm())?;
                    compression.decompress(&compressed_chunk).map_err(|e| {
                        Error::corruption(format!("BIG reverse seek: chunk decompress failed: {e}"))
                    })?
                } else {
                    compressed_chunk
                };
                crate::storage::sstable::work_counters::add_chunk_decompressed();
                window.extend_from_slice(&decompressed);
                Ok(true)
            }
            None => Ok(false),
        }
    }
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
