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

                let raw = if let Some(ci) = self.compression_info.as_deref() {
                    self.read_compressed_offset_window(ci, start, size).await?
                } else {
                    let absolute_offset = start + self.actual_header_size as u64;
                    self.read_uncompressed_verified(&self.file, absolute_offset, size as usize)
                        .await?
                };

                // Structural coverage (Signal B): the slice must decode as exactly
                // one complete partition. Mid-walk failure = fail-closed.
                if !self.partition_slice_fully_consumed(parser, &raw, schema)? {
                    return Err(Error::corruption(format!(
                        "walk_in_range_partition_slices: partition {index} slice not fully \
                         consumed (truncated/corrupt body, issue #2412)"
                    )));
                }

                // Work-probe (issue #2398): one partition body decoded. A narrow
                // token range must keep this near its in-range slice, not O(all).
                crate::storage::sstable::work_counters::add_stream_walk_partition_parsed();
                emitted_any = true;
                match decode(&raw)? {
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
