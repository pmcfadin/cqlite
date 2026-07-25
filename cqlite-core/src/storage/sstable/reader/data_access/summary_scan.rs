//! Summary-guided streaming partition enumeration (issue #2412 §C / #2413
//! Option A, Stage 4).
//!
//! The #2361 streaming walk ([`full_index_stream`](super::full_index_stream))
//! emits each partition as the index resolves it, but sources its entries from
//! the resident `Vec<PartitionIndexEntry>` — which requires the whole `Index.db`
//! to be materialised first ([`IndexReader::ensure_materialized`](crate::storage::sstable::index_reader::IndexReader::ensure_materialized),
//! the ~500MB-resident structure #2385 retired at open). This module sources the
//! SAME walk from a FORWARD-STREAMED `Index.db`
//! ([`IndexEntryStream`](crate::storage::sstable::index_reader::IndexEntryStream)),
//! so a warm BIG reader never materialises the full map (spec R3/R4).
//!
//! Two extra wins over the materialising walk:
//! - **No resident `Vec`.** Entry memory is one refill window, partition-count
//!   independent — the property the warm registry (Stage 5) relies on.
//! - **Token pushdown (#2413).** A token-range split begins the walk at the
//!   `Summary.db` sample covering the range start
//!   ([`SummaryReader::scan_start_position_for_token`](crate::storage::sstable::summary_reader::SummaryReader::scan_start_position_for_token))
//!   and stops once the walk passes the range end, so out-of-range partition
//!   bodies are never read/decoded. A compaction consumer passes no range and
//!   walks the full ring.
//!
//! Walk contract preserved from #2361: the `(token, key)` order guard
//! ([`check_token_order`](super::full_index_stream::check_token_order)),
//! per-partition structural coverage
//! ([`partition_slice_fully_consumed`](SSTableReader::partition_slice_fully_consumed),
//! Signal B), cancel-aware polling, and the `stream_walk_partitions_parsed`
//! work-probe. The completeness signal (Signal A, mid-entry truncation) moves to
//! the streaming terminus ([`IndexEntryStream::truncated_tail`](crate::storage::sstable::index_reader::IndexEntryStream)).
//!
//! No-heuristics (issue #28): the start offset is an authoritative `Summary.db`
//! sample position; every entry is authoritative `Index.db` framing; each
//! partition body is bounded by the SUCCESSOR entry's offset (last by the
//! data-section end) — never a guessed size.

use std::ops::ControlFlow;

use super::super::SSTableReader;
use super::full_index_stream::{check_token_order, FullIndexStreamOutcome};
use crate::storage::scan_cancel::ScanCancel;
use crate::types::ScanRow;
use crate::util::cassandra_murmur3::cassandra_murmur3_token;
use crate::{Error, Result, RowKey};

/// Target size (bytes) of the coalesced, chunk-aligned compressed-scan read
/// window (issue #2877), mirroring the uncompressed non-stitching walk's
/// `SEQUENTIAL_WINDOW_TARGET_BYTES` (`full_index_stream.rs`, 4 MiB) precedent.
/// See [`CompressedScanWindow`] for why the window's end is always rounded UP
/// to a `CompressionInfo.chunk_length` boundary rather than cut at the target
/// byte count exactly.
const COMPRESSED_SCAN_WINDOW_TARGET_BYTES: u64 = 4 * 1024 * 1024;

/// A chunk-aligned sliding window over the DECOMPRESSED bytes of a compressed
/// scan, keyed to the UNCOMPRESSED data-section offset domain (issue #2877).
///
/// `walk_in_range_partition_slices` used to call
/// [`read_compressed_offset_window`](SSTableReader::read_compressed_offset_window)
/// once PER PARTITION. That helper maps `[start, start+size)` onto
/// `CompressionInfo.db` chunks and decompresses every chunk it touches with no
/// cross-call memoisation — so a 16-64 KiB chunk holding many narrow partitions
/// was read + decompressed once per partition it contains, entirely bypassing
/// the Epic B decompressed-chunk cache on this hot path (the issue's root
/// cause). This window instead accumulates consecutive in-range partitions and
/// refills in ONE coalesced call whenever the current partition's span is not
/// already covered.
///
/// # Why chunk-ALIGNED, not just chunk-sized
///
/// A window boundary that falls in the MIDDLE of a chunk would make that chunk
/// get decompressed twice: once as the tail of window N, once again as the
/// head of window N+1 (both calls independently map their own `[start, end)`
/// onto `CompressionInfo.chunk_length`-sized chunks and decompress whichever
/// they touch). Rounding every refill's end UP to the next
/// `chunk_length` boundary — and always resuming the NEXT window exactly at the
/// previous window's end — means windows tile the byte space with NO gaps and
/// NO overlaps at chunk granularity, so each chunk is decompressed by EXACTLY
/// ONE window's read call across the whole scan (the acceptance criterion this
/// window exists to satisfy).
///
/// # Interaction with issue #2876
///
/// This still calls the reader's existing
/// [`read_compressed_offset_window`](SSTableReader::read_compressed_offset_window)
/// method unchanged — it only calls it FEWER, LARGER, chunk-aligned times. So
/// whichever positional source that method reads through (the `MADV_RANDOM`
/// point mapping pre-#2876, or the unadvised scan-friendly mapping post-#2876)
/// benefits from this coalescing without this module needing to know which:
/// fewer, larger, sequential, chunk-aligned reads are exactly what an unadvised
/// readahead-friendly mapping wants, and this coalescing does not reach into
/// `point_source`/`scan_mmap` itself, so it cannot defeat whichever plane
/// #2876 wires underneath.
///
/// # Preserved invariants
///
/// - **CRC-before-decompress ordering** (guardrail #1411/#1773): unchanged —
///   every chunk still goes through `read_compressed_offset_window`'s
///   CRC-validated chunk reader, just fewer times.
/// - **`partition_slice_fully_consumed`** (Signal B): unchanged — still checked
///   per partition against the slice this window serves.
/// - **Memory bound**: `max(COMPRESSED_SCAN_WINDOW_TARGET_BYTES, largest
///   partition span)`, rounded up to at most one extra `chunk_length` (a few
///   tens of KiB) — comfortably within the `<128MB` target and `<=4MiB`
///   per-window budget the issue specifies.
struct CompressedScanWindow {
    /// Decompressed bytes of `[start, start + bytes.len())` in the UNCOMPRESSED
    /// data-section offset domain. Empty until the first refill.
    bytes: Vec<u8>,
    /// Start offset of `bytes`, in the same domain. Meaningless while `filled`
    /// is `false`.
    start: u64,
    /// Whether `bytes`/`start` hold a real window yet.
    filled: bool,
}

impl CompressedScanWindow {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            start: 0,
            filled: false,
        }
    }

    /// Fetch chunk-aligned decompressed bytes `[aligned_from, aligned_from +
    /// len)` via the reader's existing `read_compressed_offset_window`, sized
    /// to cover at least up to `need_until` and rounded UP to a `chunk_length`
    /// boundary (never past `data_section_end`). `greedy` additionally pads the
    /// size out to `COMPRESSED_SCAN_WINDOW_TARGET_BYTES` (the coalescing win,
    /// used for a fresh fill); a non-greedy call (an append onto an existing
    /// window, see [`Self::slice`]) fetches only the minimum needed, so
    /// repeated appends cannot make the window balloon past `max(target,
    /// largest partition span)`.
    async fn fetch_aligned(
        reader: &SSTableReader,
        source: &dyn super::super::read_at::ReadAt,
        ci: &crate::storage::sstable::compression_info::CompressionInfo,
        chunk_length: u64,
        aligned_from: u64,
        need_until: u64,
        data_section_end: u64,
        greedy: bool,
    ) -> Result<Vec<u8>> {
        let remaining = data_section_end.saturating_sub(aligned_from);
        let minimal = need_until.saturating_sub(aligned_from);
        let want = if greedy {
            minimal
                .max(COMPRESSED_SCAN_WINDOW_TARGET_BYTES)
                .min(remaining)
        } else {
            minimal.min(remaining)
        };
        let raw_end = aligned_from + want;
        let rounded_end = raw_end.div_ceil(chunk_length) * chunk_length;
        let window_end = rounded_end.min(aligned_from + remaining);
        let window_len = window_end - aligned_from;
        let Ok(window_len_u32) = u32::try_from(window_len) else {
            return Err(Error::corruption(format!(
                "walk_in_range_partition_slices: coalesced compressed window length \
                 {window_len} overflows u32 (issue #2877)"
            )));
        };
        // `source` is the walk's SCAN-intent plane (issue #2876), threaded through
        // so this coalescing window is a CONSUMER of that plane, never a bypass of
        // it.
        reader
            .read_compressed_offset_window(source, ci, aligned_from, window_len_u32)
            .await
    }

    /// Serve `[start, end)` from this window, refilling iff not already
    /// covered. `data_section_end` bounds the last window so it never reads
    /// past the data section.
    ///
    /// Two distinct refill shapes (issue #2877 roborev finding, High —
    /// correctness): a naive "always resume exactly where the previous window
    /// ended" is WRONG whenever the current partition does not start exactly
    /// there.
    /// - **Straddle** (`start` is still inside the buffered window but `end`
    ///   runs past it — the common case once many small partitions have been
    ///   served from one big window): APPEND new chunk-aligned bytes onto the
    ///   tail, preserving the already-decompressed prefix, so a chunk already
    ///   paid for is never re-decompressed.
    /// - **Gap** (`start` is beyond the buffered window entirely — e.g. the
    ///   out-of-range run SKIPPED between a compressed wraparound scan's two
    ///   segments, which are never read so the window never advances for
    ///   them; or the very first fill): REALIGN directly to the chunk
    ///   containing `start`, discarding whatever was buffered. Blindly
    ///   "continuing" from the stale tail here either UNDERFLOWS (`start -
    ///   self.start` when the stale tail is ahead of `start`) or leaves the
    ///   window short of `start` entirely (a false corruption error) —
    ///   exactly the bug this fixes.
    async fn slice(
        &mut self,
        reader: &SSTableReader,
        source: &dyn super::super::read_at::ReadAt,
        ci: &crate::storage::sstable::compression_info::CompressionInfo,
        start: u64,
        end: u64,
        data_section_end: u64,
    ) -> Result<&[u8]> {
        let chunk_length = ci.chunk_length as u64;
        if chunk_length == 0 {
            return Err(Error::corruption(
                "walk_in_range_partition_slices: CompressionInfo chunk_length is zero \
                 (issue #2877)"
                    .to_string(),
            ));
        }
        let have_end = if self.filled {
            self.start + self.bytes.len() as u64
        } else {
            0
        };

        if !self.filled || start < self.start || end > have_end {
            if self.filled && start >= self.start && start <= have_end {
                // Straddle: append.
                let extra = Self::fetch_aligned(
                    reader,
                    source,
                    ci,
                    chunk_length,
                    have_end,
                    end,
                    data_section_end,
                    false,
                )
                .await?;
                self.bytes.extend_from_slice(&extra);
            } else {
                // Gap (or the very first fill, or a defensive `start <
                // self.start`): realign fresh, discarding the stale buffer.
                let aligned_start = (start / chunk_length) * chunk_length;
                self.bytes = Self::fetch_aligned(
                    reader,
                    source,
                    ci,
                    chunk_length,
                    aligned_start,
                    end,
                    data_section_end,
                    true,
                )
                .await?;
                self.start = aligned_start;
            }
            self.filled = true;
        }

        // Drop any now-dead prefix (the walk visits partitions in strictly
        // ascending offset order): bounds memory without touching the
        // chunk-aligned TAIL boundary, so it can never cause a chunk to be
        // re-decompressed.
        if start > self.start {
            let drop_n = (start - self.start) as usize;
            self.bytes.drain(0..drop_n.min(self.bytes.len()));
            self.start = start;
        }

        let lo = (start - self.start) as usize;
        let hi = (end - self.start) as usize;
        if hi > self.bytes.len() {
            return Err(Error::corruption(format!(
                "walk_in_range_partition_slices: coalesced compressed window short by \
                 {} bytes (issue #2877)",
                hi - self.bytes.len()
            )));
        }
        Ok(&self.bytes[lo..hi])
    }
}

/// Half-open `(start_excl, end_incl]` token bound pushed into the per-SSTable walk
/// (issue #2413 Option A). Mirrors the flight `TokenFilter` half-open semantics
/// exactly (including the `start == end` FULL-ring convention, #2228); the flight
/// crate constructs one of these from its `TokenFilter` and a grid test pins that
/// the two agree, so the membership rule lives in ONE place.
#[derive(Debug, Clone, Copy)]
pub struct ScanTokenBound {
    /// Exclusive lower bound.
    pub start_excl: i64,
    /// Inclusive upper bound.
    pub end_incl: i64,
    /// Ring-wraparound segment (`start > end`): keep `token > start || token <= end`.
    pub wraparound: bool,
}

impl ScanTokenBound {
    /// Whether `token` is inside this half-open `(start, end]` range.
    pub fn contains(&self, token: i64) -> bool {
        // #2228: equal endpoints denote the FULL ring, not the empty set.
        if self.start_excl == self.end_incl {
            return true;
        }
        if self.wraparound {
            token > self.start_excl || token <= self.end_incl
        } else {
            token > self.start_excl && token <= self.end_incl
        }
    }

    /// Whether every remaining (token-ascending) partition is guaranteed to be
    /// ABOVE this range, so a forward walk can stop. Only sound for a
    /// non-wraparound range (a wraparound range has in-range tokens at both ends
    /// of the ring, so a forward walk cannot early-stop).
    fn can_stop_past(&self, token: i64) -> bool {
        !self.wraparound && self.start_excl != self.end_incl && token > self.end_incl
    }
}

impl SSTableReader {
    /// The `Index.db` path SIBLING of this reader's CURRENT `Data.db` path (issue
    /// #2412 §C, #2383-aware). Derived from [`Self::file_path`] (the ArcSwap that a
    /// #2383 inode-rebind updates) by swapping the `-Data.db` component suffix, so a
    /// fresh streaming `Index.db` open follows a snapshot rebind rather than
    /// re-opening a torn-down path. Falls back to the reader's recorded open-time
    /// index path when the `Data.db` name is not the expected `*-Data.db` shape.
    fn current_index_db_path(&self) -> std::path::PathBuf {
        let data = self.file_path();
        // Same `-Data.db` → `-Index.db` sibling rule the #2383 rebind uses to repoint
        // the lazy IndexReader, so both stay on the rebound generation (issue #2356).
        crate::storage::sstable::reader::index_db_sibling(&data).unwrap_or_else(|| {
            self.index_reader
                .as_ref()
                .map(|ir| ir.index_path())
                .unwrap_or(data)
        })
    }

    /// Shared Summary-guided FORWARD `Index.db` walk (issue #2412 §C / #2413
    /// Option A): stream in-range partition SLICES from a `Summary.db`-guided start
    /// offset WITHOUT materialising the resident partition map, invoking `decode`
    /// on each in-range partition's exact `Data.db` byte slice.
    ///
    /// Both emit shapes are built on this: the read-shadowing ScanRow decoder
    /// ([`stream_partitions_summary_guided`](Self::stream_partitions_summary_guided),
    /// the non-stitching `V5_0Uncompressed` model) and the full-fidelity
    /// CompactionRow decoder
    /// ([`stream_partitions_summary_guided_compaction`](Self::stream_partitions_summary_guided_compaction),
    /// the chunk-stitching `nb` merge model). Factoring the walk here means the
    /// order guard, coverage check, work-probe, and range/terminus logic are
    /// defined ONCE — the two decoders differ only in how a proven-complete slice
    /// becomes rows.
    ///
    /// `token_bound`: `None` = full ring from offset 0; `Some(bound)` = begin at
    /// the sample covering `start_excl` and stop once past `end_incl`.
    ///
    /// Returns [`FullIndexStreamOutcome::FellBack`] (nothing emitted) when there is
    /// no usable summary/index to stream from. Any inconsistency AFTER the first
    /// emit is a hard [`Error`] (fail-closed): a streaming walk cannot fall back.
    async fn walk_in_range_partition_slices<D>(
        &self,
        scan_cancel: &ScanCancel,
        token_bound: Option<ScanTokenBound>,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
        schema: Option<&crate::schema::TableSchema>,
        decode: &mut D,
    ) -> Result<FullIndexStreamOutcome>
    where
        D: FnMut(&[u8]) -> Result<ControlFlow<()>>,
    {
        let (Some(_index_reader), Some(summary)) =
            (self.index_reader.as_ref(), self.summary_reader.as_ref())
        else {
            return Ok(FullIndexStreamOutcome::FellBack);
        };
        if summary.get_entries().is_empty() {
            return Ok(FullIndexStreamOutcome::FellBack);
        }

        // Exclusive end of the last partition (uncompressed data-section domain),
        // identical to the materialising walk's derivation.
        let data_section_end = match self.compression_info.as_deref() {
            Some(ci) => ci.data_length,
            None => self
                .stats
                .file_size
                .saturating_sub(self.actual_header_size as u64),
        };
        if data_section_end == 0 {
            return Ok(FullIndexStreamOutcome::FellBack);
        }

        // Summary-guided start offset: the covering sample for a token range's
        // exclusive lower bound, else the index start for a full scan.
        //
        // WRAPAROUND (roborev endgame finding, High — silent data loss): a
        // wraparound range (`start_excl > end_incl`, the ring segment crossing the
        // min-token boundary) is `(start_excl, MAX] ∪ [MIN, end_incl]` — its
        // in-range partitions live in TWO disjoint physical regions of the
        // token-ordered `Index.db`: a HIGH-token tail (`token > start_excl`) and a
        // LOW-token head (`token <= end_incl`). This walk is a single FORWARD pass
        // to EOF (`Index.db` is not circular), so starting at
        // `scan_start_position_for_token(start_excl)` (the HIGH segment's start)
        // can NEVER reach the LOW segment's entries — they physically precede that
        // offset and are silently skipped entirely. For a wraparound range the walk
        // MUST start at the index's true beginning (offset 0) so BOTH segments are
        // reachable; `ScanTokenBound::contains` (the per-entry filter below) already
        // selects exactly the two segments, and `can_stop_past` is unconditionally
        // `false` for `wraparound` (verified: the walk never early-stops before
        // EOF), so the pair is coherent — a full walk, filtered.
        let start_position = match token_bound {
            Some(bound) if bound.wraparound => 0,
            Some(bound) => summary.scan_start_position_for_token(bound.start_excl),
            None => 0,
        };

        // Derive the `Index.db` path from the reader's CURRENT (possibly
        // #2383-rebound) `Data.db` path, NOT the index_reader's recorded open-time
        // path: a warm reader rebound across a snapshot teardown keeps its already
        // -open `Data.db` FD (valid even after the snapshot dir is cleared) and
        // swaps only `file_path`, but the streaming index read does a FRESH
        // `File::open` — so it must follow the rebind or ENOENT on the dead
        // snapshot path (#2352). Snapshot components are same-inode hardlinks, so
        // the rebound path's `Index.db` is byte-identical to the open-time one.
        let index_db_path = self.current_index_db_path();
        let mut stream = crate::storage::sstable::index_reader::IndexEntryStream::open(
            &index_db_path,
            start_position,
        )
        .await?;

        // One-entry lookahead so each partition body is bounded by its SUCCESSOR's
        // offset (the last by the data-section end), exactly as the materialising
        // walk bounds `entries[i]` by `entries[i+1]`.
        let mut prev_key: Option<(i64, Vec<u8>)> = None;
        let mut index = 0usize;
        let mut emitted_any = false;
        // Coalesced, chunk-aligned compressed-scan window (issue #2877): serves
        // consecutive in-range partitions from one decompressed window instead
        // of one `read_compressed_offset_window` call per partition. Unused
        // (never filled) on the uncompressed branch.
        let mut compressed_window = CompressedScanWindow::new();
        let Some(mut current) = stream.next().await? else {
            // Zero entries from the start offset: nothing in range (or empty).
            return Ok(FullIndexStreamOutcome::Streamed);
        };

        loop {
            scan_cancel.check()?;

            let cur_key: Vec<u8> = current
                .raw_key
                .as_deref()
                .map(|k| k.to_vec())
                .unwrap_or_default();
            // Empty partition keys round-trip corrupt through BOTH read paths
            // (issue #2302/#2325): before any emit, fall back safely; after an
            // emit, fail closed rather than feed the merger a corrupt shape.
            if cur_key.is_empty() {
                if emitted_any {
                    return Err(Error::corruption(
                        "walk_in_range_partition_slices: empty partition key mid-walk \
                         (issue #2302/#2325)"
                            .to_string(),
                    ));
                }
                return Ok(FullIndexStreamOutcome::FellBack);
            }

            let token = cassandra_murmur3_token(&cur_key);

            // (token, key) order guard — the merge-input contract (issue #2361).
            check_token_order(
                prev_key.as_ref().map(|(t, k)| (*t, k.as_slice())),
                token,
                &cur_key,
                index,
            )?;

            // Read the successor to bound the current partition (and to decide the
            // range stop). The successor of the LAST entry is the data-section end.
            let next = stream.next().await?;
            let end = next
                .as_ref()
                .map(|n| n.data_offset)
                .unwrap_or(data_section_end);

            // Token pushdown (#2413): decode only in-range bodies.
            let in_range = token_bound.map(|b| b.contains(token)).unwrap_or(true);
            if in_range {
                let start = current.data_offset;
                if end <= start {
                    return Err(Error::corruption(format!(
                        "walk_in_range_partition_slices: non-ascending offset at entry \
                         {index} (offset {start} >= successor {end}, issue #2412)"
                    )));
                }
                let span = end - start;
                let Ok(size) = u32::try_from(span) else {
                    return Err(Error::corruption(format!(
                        "walk_in_range_partition_slices: partition {index} span {span} \
                         overflows u32 (issue #2412)"
                    )));
                };

                // SCAN intent (issue #2876): this forward Summary-guided walk reads
                // Data.db in mostly-ascending order, so every read — the partition
                // body AND (uncompressed) its covering `CRC.db` chunks — goes
                // through the reader's UNADVISED `scan_positional_source`, never the
                // `MADV_RANDOM` point mapping whose readahead suppression (#2210)
                // would cost this walk ~one 4 KiB fault per partition.
                //
                // The #2877 coalescing window is the CONSUMER of that plane, never a
                // bypass of it: it makes the SAME scan-plane reads fewer and larger
                // (chunk-aligned), which is precisely what the unadvised mapping's
                // kernel readahead rewards.
                let scan_source = self.scan_positional_source.clone();
                let raw: std::borrow::Cow<'_, [u8]> = if let Some(ci) =
                    self.compression_info.as_deref()
                {
                    // Coalesced chunk-aligned window (issue #2877) — the SAME
                    // `read_compressed_offset_window` call as before, just made
                    // fewer/larger times so a chunk covering many partitions is
                    // decompressed once, not once per partition it contains.
                    std::borrow::Cow::Borrowed(
                        compressed_window
                            .slice(self, scan_source.as_ref(), ci, start, end, data_section_end)
                            .await?,
                    )
                } else {
                    let absolute_offset = start + self.actual_header_size as u64;
                    std::borrow::Cow::Owned(
                        self.read_uncompressed_verified(
                            scan_source.as_ref(),
                            &self.file,
                            absolute_offset,
                            size as usize,
                        )
                        .await?,
                    )
                };
                let raw: &[u8] = &raw;

                // Structural coverage (Signal B): the slice must decode as exactly
                // one complete partition. Mid-walk failure = fail-closed.
                if !self.partition_slice_fully_consumed(parser, raw, schema)? {
                    return Err(Error::corruption(format!(
                        "walk_in_range_partition_slices: partition {index} slice not fully \
                         consumed (truncated/corrupt body, issue #2412)"
                    )));
                }

                // Work-probe (issue #2398): one partition body decoded. A narrow
                // token range must keep this near its in-range slice, not O(all).
                crate::storage::sstable::work_counters::add_stream_walk_partition_parsed();
                emitted_any = true;
                match decode(raw)? {
                    ControlFlow::Continue(()) => {}
                    ControlFlow::Break(()) => return Ok(FullIndexStreamOutcome::Streamed),
                }
            }

            // Advance. Stop early once a non-wraparound range is fully past its end.
            if let Some(bound) = token_bound {
                if bound.can_stop_past(token) {
                    return Ok(FullIndexStreamOutcome::Streamed);
                }
            }
            match next {
                Some(n) => {
                    prev_key = Some((token, cur_key));
                    current = n;
                    index += 1;
                }
                None => break,
            }
        }

        // Terminus completeness (Signal A, issue #2302): for a FULL scan a
        // mid-entry-truncated `Index.db` tail must be detectable — after an emit
        // the streaming walk cannot fall back, so this is a hard fail-closed error
        // (never a silent under-enumeration). A token-range scan does not enumerate
        // the tail, so a truncated tail beyond its range is not its concern.
        if token_bound.is_none() && stream.truncated_tail() {
            return Err(Error::corruption(
                "walk_in_range_partition_slices: Index.db tail truncated mid-entry — \
                 full-scan enumeration is incomplete (Signal A, issue #2302/#2412)"
                    .to_string(),
            ));
        }

        Ok(FullIndexStreamOutcome::Streamed)
    }

    /// Read-shadowing ScanRow decoder over the shared walk (the non-stitching
    /// `V5_0Uncompressed` model): each in-range partition slice is decoded with the
    /// read-shadowing parser (`build_v5_parser(true)`) and its surviving rows are
    /// emitted as `(RowKey, ScanRow)` — byte-identical to
    /// [`stream_all_partitions_via_full_index`](Self::iterate_all_partitions_via_full_index)'s
    /// per-partition emit, but streamed (no resident `Vec`) + token-scoped.
    pub(in crate::storage::sstable::reader) async fn stream_partitions_summary_guided<F>(
        &self,
        scan_cancel: &ScanCancel,
        token_bound: Option<ScanTokenBound>,
        emit: &mut F,
    ) -> Result<FullIndexStreamOutcome>
    where
        F: FnMut((RowKey, ScanRow)) -> Result<ControlFlow<()>>,
    {
        let parser = self.build_v5_parser(true);
        let reader_schema = self.get_table_schema(None);
        let schema = reader_schema.as_ref();
        self.walk_in_range_partition_slices(scan_cancel, token_bound, &parser, schema, &mut |raw| {
            let parsed = parser.parse_block(raw, schema, self)?;
            for (_table_id, row_key, value) in parsed {
                if self.filter_tombstone(&value) {
                    match emit((row_key, value))? {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(()) => return Ok(ControlFlow::Break(())),
                    }
                }
            }
            Ok(ControlFlow::Continue(()))
        })
        .await
    }

    /// Full-fidelity CompactionRow decoder over the shared walk (the chunk-stitching
    /// `nb` merge model): each in-range partition slice is decoded with the
    /// compaction parser (`build_v5_parser(false)`) via
    /// `parse_one_partition_for_compaction` — byte-identical to
    /// [`drain_compaction_window`](Self::stream_all_partitions_for_compaction)'s
    /// per-partition [`CompactionRow`](super::super::compaction_row::CompactionRow)
    /// emit (preserving cell timestamps / tombstone markers the k-way merger's LWW
    /// reconciliation needs), but streamed + token-scoped. `at_final_chunk = true`
    /// because `raw` is exactly one complete, coverage-proven partition slice.
    async fn stream_partitions_summary_guided_compaction<F>(
        &self,
        scan_cancel: &ScanCancel,
        token_bound: Option<ScanTokenBound>,
        schema: Option<&crate::schema::TableSchema>,
        emit: &mut F,
    ) -> Result<FullIndexStreamOutcome>
    where
        F: FnMut(super::super::compaction_row::CompactionRow) -> Result<ControlFlow<()>>,
    {
        use crate::storage::sstable::reader::parsing::ParseStep;
        let parser = self.build_v5_parser(false);
        let owned_schema = schema.cloned().or_else(|| self.get_table_schema(None));
        let sch = owned_schema.as_ref();
        self.walk_in_range_partition_slices(scan_cancel, token_bound, &parser, sch, &mut |raw| {
            let mut broke = false;
            let step = parser.parse_one_partition_for_compaction(
                raw,
                sch,
                self,
                true,
                &mut |row: super::super::compaction_row::CompactionRow| match emit(row)? {
                    ControlFlow::Continue(()) => Ok(ControlFlow::Continue(())),
                    ControlFlow::Break(()) => {
                        broke = true;
                        Ok(ControlFlow::Break(()))
                    }
                },
            )?;
            // The coverage check already proved `raw` decodes as exactly one
            // complete partition, so a non-`Emitted` step here is a structural
            // inconsistency — fail closed rather than silently drop the partition.
            if !matches!(step, ParseStep::Emitted(_)) {
                return Err(Error::corruption(
                    "stream_partitions_summary_guided_compaction: coverage-proven slice \
                     did not decode to a terminated partition (issue #2412)"
                        .to_string(),
                ));
            }
            if broke {
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        })
        .await
    }

    /// Query-serve partition enumeration for the WARM reader-based merge (issue
    /// #2412 §C / #2413 Option A) — the analogue of
    /// [`stream_all_partitions_for_compaction`](Self::stream_all_partitions_for_compaction)
    /// that the flight `do_get` warm path drives (`from_readers::drive_query_stream`).
    ///
    /// Routing MIRRORS `stream_all_partitions_for_compaction`'s per-mechanism emit
    /// shape so the merger reconciles identically, but STREAMS the index (no
    /// resident `Vec`, spec R4) and pushes the token range into the walk (#2413):
    /// - Chunk-stitching (`nb`) readers use the full-fidelity CompactionRow decoder
    ///   ([`stream_partitions_summary_guided_compaction`](Self::stream_partitions_summary_guided_compaction)) —
    ///   byte-identical to `drain_compaction_window`'s emit, so cross-generation
    ///   LWW reconciliation is preserved.
    /// - Non-stitching (`V5_0Uncompressed`) readers use the read-shadowing ScanRow
    ///   decoder ([`stream_partitions_summary_guided`](Self::stream_partitions_summary_guided)) —
    ///   byte-identical to `stream_all_partitions_cancellable`'s emit.
    ///
    /// A `FellBack` (no usable summary/index) routes on to the full-ring
    /// `stream_all_partitions_for_compaction`. Compaction consumers do NOT call
    /// this (they use the path-based stream directly, no token range), so they keep
    /// full-ring byte-parity walks unchanged (spec R3).
    pub async fn stream_all_partitions_for_query<F>(
        &self,
        schema: Option<&crate::schema::TableSchema>,
        scan_cancel: &ScanCancel,
        token_bound: Option<ScanTokenBound>,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut(super::super::compaction_row::CompactionRow) -> Result<ControlFlow<()>>,
    {
        let summary_usable = self
            .summary_reader
            .as_ref()
            .map(|s| !s.get_entries().is_empty())
            .unwrap_or(false);
        if self.index_reader.is_some() && self.bti_partitions_db.is_none() && summary_usable {
            let outcome = if self.requires_chunk_stitching() {
                // `nb` merge model: full-fidelity CompactionRows (cell timestamps /
                // tombstone markers preserved for the merger's LWW reconciliation).
                self.stream_partitions_summary_guided_compaction(
                    scan_cancel,
                    token_bound,
                    schema,
                    &mut emit,
                )
                .await?
            } else {
                // `V5_0Uncompressed` model: read-shadowing rows adapted to
                // CompactionRow (row_timestamp 0), matching the non-stitching
                // compaction stream's emit exactly.
                self.stream_partitions_summary_guided(scan_cancel, token_bound, &mut |(k, v)| {
                    let row =
                        super::super::compaction_row::CompactionRow::from_legacy_value(k, v, 0);
                    emit(row)
                })
                .await?
            };
            if matches!(outcome, FullIndexStreamOutcome::Streamed) {
                return Ok(());
            }
            tracing::warn!(
                "stream_all_partitions_for_query: Summary-guided streaming fell back \
                 (no usable Summary.db/Index.db to stream); using the full-ring \
                 compaction stream (issue #2412)."
            );
        }
        // Full-ring fallback (no usable summary/index): the downstream token filter
        // still bounds the result set, so correctness holds without pushdown.
        self.stream_all_partitions_for_compaction(schema, scan_cancel, emit)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::ScanTokenBound;

    #[test]
    fn contains_non_wraparound_half_open() {
        let b = ScanTokenBound {
            start_excl: 10,
            end_incl: 20,
            wraparound: false,
        };
        assert!(!b.contains(10), "start is exclusive");
        assert!(b.contains(11));
        assert!(b.contains(20), "end is inclusive");
        assert!(!b.contains(21));
    }

    #[test]
    fn contains_equal_endpoints_is_full_ring() {
        let b = ScanTokenBound {
            start_excl: 5,
            end_incl: 5,
            wraparound: false,
        };
        for t in [i64::MIN, -1, 0, 5, 6, i64::MAX] {
            assert!(b.contains(t), "equal endpoints cover every token (#2228)");
        }
    }

    #[test]
    fn contains_wraparound() {
        let b = ScanTokenBound {
            start_excl: 100,
            end_incl: -100,
            wraparound: true,
        };
        assert!(b.contains(101), "above start is in range");
        assert!(b.contains(-100), "at/below end is in range");
        assert!(!b.contains(0), "the interior gap is excluded");
    }

    #[test]
    fn can_stop_past_only_non_wraparound_above_end() {
        let fwd = ScanTokenBound {
            start_excl: 10,
            end_incl: 20,
            wraparound: false,
        };
        assert!(!fwd.can_stop_past(20), "at end still in range");
        assert!(fwd.can_stop_past(21), "past end can stop");
        let wrap = ScanTokenBound {
            start_excl: 100,
            end_incl: -100,
            wraparound: true,
        };
        assert!(
            !wrap.can_stop_past(i64::MAX),
            "a wraparound range never early-stops a forward walk"
        );
    }
}
