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

use crate::storage::sstable::reader::parsing::BufferExtent;
use std::ops::ControlFlow;

use super::super::SSTableReader;
use super::full_index_stream::{check_token_order, FullIndexStreamOutcome};
use crate::storage::scan_cancel::ScanCancel;
use crate::types::ScanRow;
use crate::util::cassandra_murmur3::cassandra_murmur3_token;
use crate::{Error, Result, RowKey};

// Coalesced, chunk-aligned compressed-scan read window (issue #2877), extracted
// so this walk module stays under the campsite-rule source target (epic #1116).
// The walk owns the enumeration; the window owns the one piece of per-scan state a
// COMPRESSED `Data.db` needs.
mod compressed_scan_window;
/// Single-generation, token-scoped, pull-based query ROW stream (issue #3058).
mod query_rows;
/// Sizing constants + the derived read-ahead bounds (issue #3384).
mod query_rows_bounds;
/// The half-open token bound and its Cassandra-derived membership rule (#3634).
mod token_bound;
pub use query_rows::{QueryRowBatch, QueryRowStream, QUERY_ROWS_MAX_READ_AHEAD};
pub use token_bound::ScanTokenBound;

// COMBINED-INTERACTION regression for the #2876 read-intent split x the #2877
// coalescing window: every widened read this walk issues must land on the
// never-`MADV_RANDOM` scan plane and none on the point plane — a property of
// the PAIR that neither fix's own tests can observe. Lives in-crate (not
// `cqlite-core/tests/`) because the per-plane spies and plane setters are
// `#[cfg(test)] pub(crate)`; lives HERE rather than beside
// `reader::read_at_point_tests` so it does not grow the already-over-threshold
// `reader/mod.rs` (campsite rule, epic #1116).
//
// Feature-gated on BOTH `write-support` (the `SSTableWriter`/`WriteEngine` mutations
// that build the fixture) and `lz4` (the fixture is repacked through
// `create_compressor(CompressionAlgorithm::Lz4)`, which errors without it) — the
// same pair the `issue_2877_scan_chunk_coalescing` integration target declares in
// `required-features`. Without the gate the `minimal-build` gate component
// (`cargo test -p cqlite-core --no-default-features --features all-compression
// --lib --no-run`) fails to compile this module's imports.
#[cfg(all(test, feature = "write-support", feature = "lz4"))]
mod scan_plane_coalescing_tests;

use compressed_scan_window::CompressedScanWindow;

/// The token range applied to a walk whose signature does not take one.
///
/// [`SSTableReader::stream_all_partitions_for_query`] pushes its
/// [`ScanTokenBound`] into the Summary-guided walk, which skips an out-of-range
/// partition body rather than decoding it. Its full-ring fallback routes take no
/// bound, so this gate applies the range to their emit instead. Correctness is
/// then a property of the call rather than of which route the reader chose, which
/// matters most for a BTI (`da`) reader: it fails the Summary-guided gate on every
/// call, so the fallback is its only route (issue #3358).
///
/// The verdict is computed once per partition, not once per row. A token is a
/// property of the partition key, and a walk emits a partition's rows
/// consecutively, so a per-row hash would charge every row for an answer that
/// changes at a partition boundary.
struct TokenGate {
    bound: Option<ScanTokenBound>,
    /// The key `admits` below was computed for.
    last: Option<std::sync::Arc<[u8]>>,
    admits: bool,
}

impl TokenGate {
    fn new(bound: Option<ScanTokenBound>) -> Self {
        // `admits` starts true so an unbounded walk answers without touching
        // `last` at all.
        Self {
            bound,
            last: None,
            admits: true,
        }
    }

    /// Whether this row's partition is inside the range.
    fn admits(&mut self, key: &std::sync::Arc<[u8]>) -> bool {
        let Some(bound) = self.bound else {
            return true;
        };
        if self.last.as_ref().is_none_or(|last| last != key) {
            let token = crate::util::cassandra_murmur3::cassandra_murmur3_token(key);
            self.admits = bound.contains(token);
            self.last = Some(key.clone());
        }
        self.admits
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
        // `false` for a wraparound bound (verified: the walk never early-stops
        // before EOF), so the pair is coherent — a full walk, filtered.
        //
        // Wrapping is DERIVED from the endpoints (#3634), so the FULL ring
        // (`start_excl == end_incl`, #2228) takes this arm too — as it must: it
        // admits every token, so the walk has to start at the true beginning.
        let start_position = match token_bound {
            Some(bound) if bound.is_wraparound() => 0,
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
                // through the reader's never-`MADV_RANDOM` `scan_positional_source`,
                // not the `MADV_RANDOM` point mapping whose suppressed readahead (#2210)
                // would cost this walk ~one 4 KiB fault per partition.
                //
                // The #2877 coalescing window is the CONSUMER of that plane, never a
                // bypass of it: it makes the SAME scan-plane reads fewer and larger
                // (chunk-aligned, ramped to 4 MiB), which is precisely what the
                // scan mapping's kernel readahead rewards. Issuing them on the
                // advised point plane instead would reinstate the #2876 field
                // regression while every per-PR test stayed green — the combined
                // interaction `cqlite-core/tests/issue_2877_scan_chunk_coalescing.rs`
                // pins (CASSANDRA-15452's lesson: a userspace scan buffer only helps
                // when it reads the plane that actually reads ahead).
                let scan_source = self.scan_positional_source.clone();
                let raw: std::borrow::Cow<'_, [u8]> =
                    if let Some(ci) = self.compression_info.as_deref() {
                        std::borrow::Cow::Borrowed(
                            compressed_window
                                .slice(self, scan_source.as_ref(), ci, start, end, data_section_end)
                                .await?,
                        )
                    } else {
                        std::borrow::Cow::Owned(
                            self.read_uncompressed_verified(
                                scan_source.as_ref(),
                                &self.file,
                                start + self.actual_header_size as u64,
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
    ///
    /// `now_secs` (issue #3058): when `Some`, the caller's request-scoped
    /// read-time TTL clock is pinned onto the read-shadowing parser instead of
    /// the ambient one it samples at construction, so a caller that already
    /// captured ONE reconciliation `now` (the Flight producer) expires TTL cells
    /// against exactly that instant. `None` keeps the ambient sample.
    ///
    /// `caller_schema` (issue #3058): the AUTHORITATIVE table schema, when the
    /// caller has one (the Flight producer's ticket-DDL schema). It must take
    /// precedence over the reader's own four-tier lookup exactly as the
    /// compaction sibling's `schema` parameter does — an `nb` SSTable header
    /// carries no embedded schema, so decoding it with the reader-derived one
    /// loses the clustering-key columns (they surface as NULL). `None` keeps the
    /// reader-derived resolution.
    pub(in crate::storage::sstable::reader) async fn stream_partitions_summary_guided<F>(
        &self,
        scan_cancel: &ScanCancel,
        token_bound: Option<ScanTokenBound>,
        now_secs: Option<i64>,
        caller_schema: Option<&crate::schema::TableSchema>,
        emit: &mut F,
    ) -> Result<FullIndexStreamOutcome>
    where
        F: FnMut((RowKey, ScanRow)) -> Result<ControlFlow<()>>,
    {
        let _scan = self.begin_scan(); // #3853 scan-lifetime madvise seam
        let parser = self.build_v5_parser(true);
        let parser = match now_secs {
            Some(now) => parser.with_now_secs(now),
            None => parser,
        };
        let reader_schema = caller_schema
            .cloned()
            .or_else(|| self.get_table_schema(None));
        let schema = reader_schema.as_ref();
        self.walk_in_range_partition_slices(scan_cancel, token_bound, &parser, schema, &mut |raw| {
            // #3782: `walk_in_range_partition_slices` runs
            // `partition_slice_fully_consumed` per slice before calling this
            // decode, so `raw` is a proven-complete partition extent.
            let parsed = parser.parse_block(raw, BufferExtent::Complete, schema, self)?;
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
        let _scan = self.begin_scan(); // #3853 scan-lifetime madvise seam
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
        let _scan = self.begin_scan(); // #3853 scan-lifetime madvise seam
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
                //
                // `caller_schema` is the caller's AUTHORITATIVE `schema` (issue
                // #3097): the merge arm previously passed `None` here and resolved
                // the decode schema from the reader's own four-tier lookup, which
                // for a `V5_0Uncompressed` reader (an `nb` header carries no
                // embedded schema) is a header-derived schema whose clustering
                // columns bear the placeholder name `clustering_key` — so a ticket
                // whose DDL declares a real clustering key (`ck`) decoded that
                // column under the wrong name (surfacing as NULL to a projected
                // `SELECT`). Passing `Some(schema)` makes the merge arm honour the
                // caller's schema EXACTLY as the chunk-stitching sibling
                // (`stream_partitions_summary_guided_compaction`) and the fast arm
                // (`query_rows::drive_query_rows`) already do. `stream_partitions_
                // summary_guided`/`walk_in_range_partition_slices` still fall back
                // to the reader-derived lookup when the caller passes `None`
                // (e.g. `stream_all_partitions_for_query(None, …)`), so the no-
                // caller-schema behaviour is preserved. Compaction never routes
                // through this method (it uses `stream_all_partitions_for_
                // compaction` directly), so byte-parity walks are untouched.
                self.stream_partitions_summary_guided(
                    scan_cancel,
                    token_bound,
                    None,
                    schema,
                    &mut |(k, v)| {
                        let row =
                            super::super::compaction_row::CompactionRow::from_legacy_value(k, v, 0);
                        emit(row)
                    },
                )
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
        // Full-ring fallback: the routes below take no `token_bound` in their
        // signatures, so the range is applied to their emit instead of being
        // dropped (issue #3358). A BTI (`da`) reader reaches this fallback on
        // EVERY call — `bti_partitions_db.is_some()` fails the gate above — so
        // before this it was not a fallback for that format but the only route,
        // and `stream_all_partitions_for_query(.., Some(bound), ..)` returned the
        // whole ring. An `nb` generation holding the same rows returned exactly
        // the range, which `issue_2412_wraparound_scan.rs` pins with
        // `assert_eq!`, so the two formats disagreed about what the parameter
        // means.
        //
        // The gate is applied here, once, rather than inside each route: it is
        // the boundary the caller's range was handed to, and the two routes below
        // reach three different walk implementations between them.
        //
        // QUERY-ARM caller-schema fidelity (issue #3097): the summary-guided walk
        // above honours the caller's authoritative `schema`, but this fallback
        // fires when `Summary.db` is absent/unusable (or the walk `FellBack`). For
        // a non-stitching `V5_0Uncompressed` reader, `stream_all_partitions_for_
        // compaction` delegates its non-stitch branch to
        // `stream_all_partitions_cancellable`, whose full-index/sequential routes
        // resolve the decode schema from the READER only — so a ticket-DDL
        // clustering key (`ck`) would again surface as NULL exactly as in the
        // summary-guided branch this issue fixed. So the QUERY arm threads its
        // caller `schema` through `stream_all_partitions_cancellable` directly for
        // that branch (adapting `ScanRow` → `CompactionRow` byte-identically to the
        // compaction non-stitch branch). Compaction never routes through this
        // method, and its own `stream_all_partitions_cancellable` call passes
        // `None`, so compaction's effective decode schema is unchanged (#3097).
        //
        // Chunk-stitching (`nb`) and BTI (`da`) readers keep routing through
        // `stream_all_partitions_for_compaction`, whose stitch/BTI decode already
        // honours the passed `schema` (`schema.cloned().or_else(...)`), so no
        // divergence is introduced there.
        let mut gate = TokenGate::new(token_bound);
        let mut emit = move |row: super::super::compaction_row::CompactionRow| {
            if !gate.admits(&row.key.0) {
                return Ok(ControlFlow::Continue(()));
            }
            emit(row)
        };
        if self.requires_chunk_stitching() || self.bti_partitions_db.is_some() {
            return self
                .stream_all_partitions_for_compaction(schema, scan_cancel, emit)
                .await;
        }
        self.stream_all_partitions_cancellable(scan_cancel, schema, |(key, value)| {
            let row = super::super::compaction_row::CompactionRow::from_legacy_value(key, value, 0);
            emit(row)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    /// The gate's own tests. The end-to-end pin, over a Cassandra-written BTI
    /// generation through `stream_all_partitions_for_query`, is
    /// `tests/issue_3358_bti_query_token_bound.rs`; these cover the parts that
    /// file cannot isolate.
    mod token_gate {
        use super::super::{ScanTokenBound, TokenGate};
        use crate::util::cassandra_murmur3::cassandra_murmur3_token;
        use std::sync::Arc;

        fn key(bytes: &[u8]) -> Arc<[u8]> {
            Arc::from(bytes.to_vec().into_boxed_slice())
        }

        #[test]
        fn no_bound_admits_every_key() {
            let mut gate = TokenGate::new(None);
            for bytes in [&b"a"[..], b"b", b"a"] {
                assert!(gate.admits(&key(bytes)));
            }
        }

        #[test]
        fn a_bound_admits_its_own_partition_only() {
            let inside = key(b"inside");
            let outside = key(b"outside");
            let token = cassandra_murmur3_token(&inside);
            let bound = ScanTokenBound {
                start_excl: token - 1,
                end_incl: token,
            };
            assert!(
                !bound.contains(cassandra_murmur3_token(&outside)),
                "the two keys must fall either side of the bound, or this case \
                 cannot discriminate"
            );
            let mut gate = TokenGate::new(Some(bound));
            assert!(gate.admits(&inside));
            assert!(!gate.admits(&outside));
        }

        /// The memo holds one key's verdict, so it must be recomputed the moment
        /// the key changes — including when the walk returns to a key it has seen.
        #[test]
        fn the_memo_follows_the_partition_boundary() {
            let inside = key(b"inside");
            let outside = key(b"outside");
            let token = cassandra_murmur3_token(&inside);
            let mut gate = TokenGate::new(Some(ScanTokenBound {
                start_excl: token - 1,
                end_incl: token,
            }));
            let verdicts: Vec<bool> = [&inside, &inside, &outside, &outside, &inside]
                .into_iter()
                .map(|k| gate.admits(k))
                .collect();
            assert_eq!(verdicts, vec![true, true, false, false, true]);
        }
    }
}
