//! Streaming partition enumeration for the non-stitching read path (issue #2361).
//!
//! [`SSTableReader::iterate_all_partitions_via_full_index`](super::full_index_scan)
//! (issue #2302) resolves every partition of a BIG SSTable through the full
//! `Index.db` offset table, but MATERIALISES the whole result into one sorted
//! `Vec` before returning. The compaction/Flight `do_get` streaming producer then
//! forwards that Vec one entry at a time — so for a huge SSTable (the #2361 field
//! case: 1.13M partitions, uncompressed BIG, which routes to the non-stitching
//! branch of
//! [`stream_all_partitions_for_compaction`](super::super::SSTableReader::stream_all_partitions_for_compaction))
//! the producer builds the ENTIRE table in memory before the first `emit`, the
//! consumer's `LIMIT` early-break and cancellation cannot take effect until that
//! completes, and peak memory grows unbounded.
//!
//! This module adds a TRUE-streaming variant that emits each partition as the
//! index walk resolves it — the same index-ordered (== token-ordered) walk, but
//! without the whole-file `Vec`. The compressed-nb chunk-stitching path already
//! streams through a bounded window; this brings the non-stitching path to
//! parity.
//!
//! ## Fallback safety (why some bails move up-front)
//!
//! The materialising [`iterate_all_partitions_via_full_index`] can bail to a
//! `sequential_scan` fallback on ANY structural inconsistency because it has
//! emitted nothing yet (it only drops a local `Vec`). A STREAMING walk that has
//! already emitted partition `k` into the k-way merge CANNOT then fall back — a
//! re-scan would double-emit and corrupt the merge. So every gate computable from
//! the `Index.db` offset table ALONE (entries present, data section non-empty,
//! index fully parsed, all keys non-empty, offsets strictly ascending, spans fit
//! `u32`) is checked UP-FRONT: a failure returns [`FullIndexStreamOutcome::FellBack`]
//! with nothing emitted, so the caller's `sequential_scan` fallback is still
//! safe. The remaining per-partition check (a partition slice that does not fully
//! consume) requires reading the partition body, so it can only surface mid-walk
//! — and there it is a hard `Error` (fail-closed), never a silent fallback. For a
//! well-formed BIG SSTable it never fires.
//!
//! ## Sequential read window (issue #2366)
//!
//! The uncompressed walk resolves each partition's `data_offset` DIRECTLY from
//! the `Index.db` offset table (no per-partition random `Index.db` probe) and
//! serves its bytes from a bounded, forward-only sequential read window over
//! Data.db rather than one positioned `seek`+`read_exact`+CRC per partition —
//! turning ~O(partitions) random reads into O(data_section_len / window)
//! sequential reads with bounded peak memory. See
//! [`SEQUENTIAL_WINDOW_TARGET_BYTES`].

use super::super::SSTableReader;
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::parsing::BufferExtent;
use crate::types::ScanRow;
use crate::util::cassandra_murmur3::cassandra_murmur3_token;
use crate::{Error, Result, RowKey};
use std::ops::ControlFlow;

/// Target size (bytes) of the sequential Data.db read window used by the
/// uncompressed non-stitching streaming walk (issue #2366).
///
/// The walk visits partitions in ascending `data_offset` order (== token order
/// == Data.db physical order), so instead of one positioned `seek`+`read_exact`
/// +CRC per partition (~1.13M small random reads at field scale) it fills a
/// bounded in-memory window covering many consecutive partitions with ONE
/// sequential `read_uncompressed_verified` call, then serves each partition's
/// slice from that window. A partition larger than the target still reads in a
/// single window sized to exactly its span (never smaller than one partition),
/// so peak memory is bounded by `max(TARGET, largest_partition_span)`. 4 MiB is
/// large enough to amortise the per-read + CRC overhead across ~hundreds of
/// typical partitions yet small enough to keep the memory target (<128 MiB).
const SEQUENTIAL_WINDOW_TARGET_BYTES: u64 = 4 * 1024 * 1024;

/// Hardening B (issue #2361, roborev rounds 2/3): the streaming walk EMITS each
/// partition to the k-way merger as it resolves it, unlike the materialising
/// sibling ([`SSTableReader::iterate_all_partitions_via_full_index`]), which
/// defensively re-sorts its whole result via `sort_by_token_order` before
/// returning — a post-hoc corrective the streaming path cannot apply (it has
/// already emitted). This is the O(1)-per-partition guard that stands in for
/// that sort: `Index.db` entries are ASSUMED to be in the same total order the
/// materialising sibling produces (matching Data.db's physical layout), but that
/// assumption is never re-verified anywhere else on this path. A violation would
/// silently hand the merger inputs it requires to be sorted, corrupting
/// cross-SSTable reconciliation — so this fails CLOSED (`Error::corruption`)
/// rather than emit out of order (issue #28: authoritative structure only, never
/// a silent best-effort).
///
/// The order compared is EXACTLY `sort_by_token_order`'s: Murmur3 `token`, then
/// (on a token TIE) the raw partition-key bytes, unsigned-lexicographic —
/// `RowKey`/`&[u8]` `cmp` matches Cassandra's on-disk `DecoratedKey`
/// (`ByteBuffer.compareTo`) order. Checking token alone (roborev round 2) missed
/// the tie-break: two distinct keys colliding on one token could be emitted in
/// the wrong relative order — legal per a token-only check but NOT what the
/// materialising sibling (or the merger) requires. A genuine token collision
/// with keys still in ascending byte order is NOT a violation; only a STRICT
/// decrease of the `(token, key)` pair is. Factored out as a pure function
/// (`prev`/`token`/`key`/`index` in, `Result<()>` out) so it is unit-testable
/// without a full `SSTableReader` fixture.
pub(in crate::storage::sstable::reader) fn check_token_order(
    prev: Option<(i64, &[u8])>,
    token: i64,
    key: &[u8],
    index: usize,
) -> Result<()> {
    if let Some((prev_token, prev_key)) = prev {
        // Same total order as the materialising sibling's `sort_by_token_order`.
        if prev_token.cmp(&token).then_with(|| prev_key.cmp(key)) == std::cmp::Ordering::Greater {
            return Err(Error::corruption(format!(
                "stream_all_partitions_via_full_index: partition {index} (token {token}) is out \
                 of order (< previous token {prev_token}) — Index.db entries are not in \
                 (token, key) order (issue #2361)"
            )));
        }
    }
    Ok(())
}

/// Outcome of [`SSTableReader::stream_all_partitions_via_full_index`].
pub(in crate::storage::sstable::reader) enum FullIndexStreamOutcome {
    /// Every partition was streamed (the walk ran to completion, or the
    /// consumer returned `ControlFlow::Break`).
    Streamed,
    /// An up-front structural gate failed BEFORE any partition was emitted. The
    /// caller must fall back to a (materialising) `sequential_scan`; nothing was
    /// emitted, so the fallback cannot double-emit.
    FellBack,
}

impl SSTableReader {
    /// Streaming sibling of
    /// [`iterate_all_partitions_via_full_index`](Self::iterate_all_partitions_via_full_index):
    /// walk the full `Index.db` offset table and `emit` each partition's rows as
    /// they are resolved, never building a whole-file `Vec`.
    ///
    /// `emit` receives `(row_key, value)` per surviving (tombstone-filtered) row,
    /// in index (token) order, and returns `ControlFlow::Break` to stop early
    /// (consumer dropped / satisfied).
    ///
    /// No `limit`/budget parameter (issue #2361, roborev round 2): a per-producer
    /// PARTITION count is not a safe proxy for a row-level `LIMIT` — a producer
    /// cannot know how many of its partitions the k-way merger will keep (a
    /// predicate filter runs at the consumer, and shadowed/tombstoned partitions
    /// contribute zero surviving rows), so any producer-side cap risks
    /// under-returning. `LIMIT` is enforced purely downstream: the consumer's
    /// post-reconciliation early break (`drive_merge`) plus this crate's
    /// cancel-aware Drop teardown (cancel → drop receiver → join) stopping the
    /// producer promptly once the consumer stops pulling — see
    /// [`stream_all_partitions_cancellable`](Self::stream_all_partitions_cancellable).
    pub(in crate::storage::sstable::reader) async fn stream_all_partitions_via_full_index<F>(
        &self,
        scan_cancel: &ScanCancel,
        // Issue #3058: caller-pinned read-time TTL clock (`None` = ambient).
        now_secs: Option<i64>,
        // Issue #3058: the caller's AUTHORITATIVE schema, which must win over the
        // reader's four-tier lookup (an `nb` header carries none, so decoding
        // with the reader-derived schema drops the clustering columns).
        caller_schema: Option<&TableSchema>,
        emit: &mut F,
    ) -> Result<FullIndexStreamOutcome>
    where
        F: FnMut((RowKey, ScanRow)) -> Result<ControlFlow<()>>,
    {
        let Some(index_reader) = &self.index_reader else {
            return Ok(FullIndexStreamOutcome::FellBack);
        };
        // Issue #2412 Stage 2: a lazily-opened reader defers the full parse to
        // first use — a full streaming enumeration IS that first use (Stage 4
        // replaces the SOURCE of entries with a true Summary-guided streaming
        // walk that never materializes the whole map; the walk CONTRACT below is
        // unchanged either way). No-op for an eagerly-opened reader.
        index_reader.ensure_materialized(scan_cancel).await?;
        let entries = index_reader.get_partition_entries();

        // Exclusive end of the last partition (uncompressed data-section domain).
        let data_section_end = match self.compression_info.as_deref() {
            Some(ci) => ci.data_length,
            None => self
                .stats
                .file_size
                .saturating_sub(self.actual_header_size as u64),
        };

        // Up-front structural gates (no per-partition I/O): any failure => FellBack
        // (nothing emitted; the caller's sequential fallback stays safe). These
        // mirror the same-named checks in the materialising variant, but hoisted
        // BEFORE the first emit so the streaming path never bails mid-walk on a
        // signal it could have seen from the offset table alone.
        if entries.is_empty() || data_section_end == 0 || !index_reader.is_fully_parsed() {
            return Ok(FullIndexStreamOutcome::FellBack);
        }
        for i in 0..entries.len() {
            // Empty partition keys are rejected fail-safe (issue #2302/#2325 — an
            // empty PK round-trips corrupt through BOTH read paths).
            let key = entries[i].raw_key.as_deref().unwrap_or(&[]);
            if key.is_empty() {
                return Ok(FullIndexStreamOutcome::FellBack);
            }
            let start = entries[i].data_offset;
            let end = if i + 1 < entries.len() {
                entries[i + 1].data_offset
            } else {
                data_section_end
            };
            if end <= start || u32::try_from(end - start).is_err() {
                return Ok(FullIndexStreamOutcome::FellBack);
            }
        }

        // All gates passed: stream. Read-shadowing parser (matches sequential_scan
        // + the materialising variant).
        let parser = self.build_v5_parser(true);
        let parser = match now_secs {
            Some(now) => parser.with_now_secs(now),
            None => parser,
        };
        let reader_schema = caller_schema
            .cloned()
            .or_else(|| self.get_table_schema(None));
        let schema = reader_schema.as_ref();
        // Hardening B (issue #2361): O(1)-per-partition (token, key)-order guard —
        // see `check_token_order`'s doc for why this stands in for the
        // materialising sibling's defensive `sort_by_token_order`.
        let mut prev_key: Option<(i64, &[u8])> = None;

        // Sequential read window over the UNCOMPRESSED data section (issue #2366):
        // instead of one positioned read + CRC per partition, `window` holds the
        // bytes of `[window_start, window_start + window.len())` (data-section
        // domain, i.e. the `data_offset` domain) and each partition's slice is
        // served from it. Because entries are ascending, `window_start` only ever
        // advances forward — the read pattern is O(data_section_len / TARGET)
        // sequential reads, not O(partitions) random reads. Unused (never filled)
        // on the compressed-offset branch.
        let mut window: Vec<u8> = Vec::new();
        let mut window_start: u64 = 0;
        let mut window_filled = false;

        for i in 0..entries.len() {
            // Cooperative cancellation: poll every entry so a cancelled scan
            // abandons the walk promptly (issue #2264, PER-CALL token issue #2346).
            scan_cancel.check()?;

            let partition_key = entries[i].raw_key.as_deref().unwrap_or(&[]);
            let token = cassandra_murmur3_token(partition_key);
            check_token_order(prev_key, token, partition_key, i)?;
            prev_key = Some((token, partition_key));

            // The partition's data offset comes DIRECTLY from the offset table
            // entry already validated up-front (issue #2366): the former
            // `lookup_partition_with_index` probe returned this identical value via
            // a HashMap lookup that also incremented `INDEX_PROBES` — dropping it
            // removes O(partitions) index probes without weakening any check (the
            // up-front gate already proved every offset present, ascending, and
            // span-representable). This intentionally also skips the
            // `READ_PARTITION_LOOKUP` observability counter and the B4
            // `key_offset_cache` population that `lookup_partition_with_index`
            // performs on the point-read path: a full scan is not a point lookup,
            // so it must not inflate point-read lookup metrics, nor thrash/evict
            // the bounded B4 cache by pushing ~1M entries through it. This is a
            // deliberate divergence from the materialising sibling's
            // `lookup_partition_with_index` call, not an oversight.
            let data_offset = entries[i].data_offset;
            let next_offset = if i + 1 < entries.len() {
                entries[i + 1].data_offset
            } else {
                data_section_end
            };
            if next_offset <= data_offset {
                return Err(Error::corruption(format!(
                    "stream_all_partitions_via_full_index: non-ascending offset at entry {i} \
                     (offset {data_offset} >= successor {next_offset}, issue #2361)"
                )));
            }
            let span = next_offset - data_offset;
            let Ok(size) = u32::try_from(span) else {
                return Err(Error::corruption(format!(
                    "stream_all_partitions_via_full_index: partition {i} span {span} \
                     overflows u32 (issue #2361)"
                )));
            };

            // SCAN intent (issue #2876): a full-`Index.db` stream walks Data.db in
            // ascending offset order, so its body reads and (uncompressed) `CRC.db`
            // covering-chunk reads use the reader's UNADVISED
            // `scan_positional_source`, not the `MADV_RANDOM` point mapping (#2210).
            let scan_source = self.scan_positional_source.clone();
            let control = if let Some(ci) = self.compression_info.as_deref() {
                // Rare non-nb compressed non-stitching branch: unchanged per-partition
                // windowed read (issue #2366 targets the uncompressed field case only).
                let raw = self
                    .read_compressed_offset_window(scan_source.as_ref(), ci, data_offset, size)
                    .await?;
                self.emit_partition_slice(i, &raw, &parser, schema, emit)?
            } else {
                // Uncompressed sequential window (issue #2366). Refill when the
                // partition's slice is not fully covered by the current window.
                if !window_filled
                    || data_offset < window_start
                    || next_offset > window_start + window.len() as u64
                {
                    window_start = data_offset;
                    // One read per window: at least this partition's whole span,
                    // at most the target, clamped to the data section end so we
                    // never read past the last partition.
                    let remaining = data_section_end - window_start;
                    let want = span.max(SEQUENTIAL_WINDOW_TARGET_BYTES).min(remaining);
                    let absolute_offset = window_start + self.actual_header_size as u64;
                    // Count the window refill as one production read-path seek
                    // (`SEEK_CALLS`) — the single positioned read stands in for the
                    // per-partition seeks it replaces, matching the windowed-read
                    // convention in `scan_stream_windowed_read.rs`. This is the
                    // O(N/window) read-pattern evidence for issue #2366.
                    crate::storage::sstable::read_work_counters::record_seek();
                    window = self
                        .read_uncompressed_verified(
                            scan_source.as_ref(),
                            &self.file,
                            absolute_offset,
                            want as usize,
                        )
                        .await?;
                    window_filled = true;
                }
                let lo = (data_offset - window_start) as usize;
                let hi = (next_offset - window_start) as usize;
                let raw = &window[lo..hi];
                self.emit_partition_slice(i, raw, &parser, schema, emit)?
            };

            if control.is_break() {
                return Ok(FullIndexStreamOutcome::Streamed);
            }
        }

        Ok(FullIndexStreamOutcome::Streamed)
    }

    /// Per-partition body of the streaming full-index walk (issue #2361/#2366):
    /// verify the slice decodes as exactly one complete partition, record the
    /// work-probe, parse it, and `emit` each surviving (tombstone-filtered) row.
    /// Returns `ControlFlow::Break` if the consumer asked to stop.
    ///
    /// Factored out of [`Self::stream_all_partitions_via_full_index`] so the
    /// compressed-offset branch and the uncompressed sequential-window branch
    /// share identical decode/emit semantics — only how the `raw` bytes are
    /// obtained differs (issue #2366).
    fn emit_partition_slice<F>(
        &self,
        index: usize,
        raw: &[u8],
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
        schema: Option<&TableSchema>,
        emit: &mut F,
    ) -> Result<ControlFlow<()>>
    where
        F: FnMut((RowKey, ScanRow)) -> Result<ControlFlow<()>>,
    {
        // Structural coverage: the slice must decode as exactly one complete
        // partition (issue #2302 Signal B). Mid-walk failure = fail-closed.
        if !self.partition_slice_fully_consumed(parser, raw, schema)? {
            return Err(Error::corruption(format!(
                "stream_all_partitions_via_full_index: partition {index} slice not fully \
                 consumed (truncated/corrupt body, issue #2361)"
            )));
        }

        // Work-probe (issue #2398): one partition body read + parsed. A
        // token-range split must keep this bounded to its in-range slice, not
        // the SSTable's whole partition count.
        crate::storage::sstable::work_counters::add_stream_walk_partition_parsed();
        // #3782: the coverage check above PROVED this slice is one complete
        // partition consuming all of `raw` (and fails closed otherwise).
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
    }

    /// Streaming sibling of
    /// [`iterate_all_partitions_cancellable`](Self::iterate_all_partitions_cancellable):
    /// enumerate every partition, emitting each surviving row via `emit` as it is
    /// resolved instead of returning a whole-file `Vec` (issue #2361).
    ///
    /// Routing mirrors the materialising method: a BIG SSTable with a usable
    /// `Index.db` streams through
    /// [`stream_all_partitions_via_full_index`](Self::stream_all_partitions_via_full_index)
    /// (bounded memory, index/token order). Only when that up-front-bails (no
    /// usable/complete index) does it fall back to the MATERIALISING
    /// `sequential_scan` — which cannot stream in token order without collecting +
    /// sorting first. The #2361 field table has a valid `Index.db`, so it takes
    /// the streaming branch; the sequential fallback is the rare degenerate path
    /// (missing/malformed index) and is documented, not fixed, here.
    ///
    /// ## No producer-side `LIMIT` budget (issue #2361, roborev round 2)
    ///
    /// Earlier revisions threaded an optional per-producer PARTITION budget here.
    /// It was REMOVED: `LIMIT` counts surviving, post-reconciliation ROWS, not
    /// partitions scanned, and a producer cannot know that count in advance — a
    /// consumer-side predicate filter can thin a partition's rows to zero, and a
    /// tombstoned/cross-generation-shadowed partition contributes zero surviving
    /// rows while still consuming a "budget" slot. Either case makes a
    /// partition-granular producer cap risk returning FEWER rows than exist,
    /// which no `limitGuaranteed = false` contract permits (it only permits
    /// MORE). `LIMIT` is therefore enforced purely downstream: the consumer's
    /// post-reconciliation early break (`drive_merge`) stops pulling, and the
    /// cancel-aware Drop teardown (cancel → drop receiver → join) then stops the
    /// producer promptly — bounding work WITHOUT any risk of under-return.
    ///
    /// `caller_schema` (issue #3097): the caller's AUTHORITATIVE schema, when it
    /// has one. It is threaded into BOTH decode routes below —
    /// [`stream_all_partitions_via_full_index`](Self::stream_all_partitions_via_full_index)
    /// and the `sequential_scan` fallback — so it takes precedence over the
    /// reader's own resolution exactly as the summary-guided merge arm does (an
    /// `nb` `V5_0Uncompressed` header carries no embedded schema, so decoding it
    /// with the reader-derived one drops the clustering columns → NULL). `None`
    /// keeps the reader-derived resolution unchanged, which is what every
    /// COMPACTION caller passes: compaction's effective decode schema on this seam
    /// is byte-identical to origin/main (`None` here → `via_full_index(None)` /
    /// `sequential_scan(self.schema)`). ONLY the QUERY arm's fallback passes
    /// `Some(schema)`, so compaction behaviour is provably untouched (issue #3097).
    pub(in crate::storage::sstable::reader) async fn stream_all_partitions_cancellable<F>(
        &self,
        scan_cancel: &ScanCancel,
        caller_schema: Option<&TableSchema>,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut((RowKey, ScanRow)) -> Result<ControlFlow<()>>,
    {
        if self.index_reader.is_some() && self.bti_partitions_db.is_none() {
            match self
                .stream_all_partitions_via_full_index(scan_cancel, None, caller_schema, &mut emit)
                .await?
            {
                FullIndexStreamOutcome::Streamed => return Ok(()),
                FullIndexStreamOutcome::FellBack => {
                    tracing::warn!(
                        "SSTable Index.db is present but the streaming index-random-read path \
                         could not prove every partition resolvable; falling back to a \
                         MATERIALISING sequential scan of Data.db (issues #2361/#2302). This \
                         should not happen for a well-formed BIG SSTable."
                    );
                }
            }
        }

        // Fallback: materialising sequential scan (rare degenerate path). No
        // producer-side limit (see this method's doc) — the consumer's
        // post-reconciliation LIMIT break bounds it downstream instead.
        //
        // Work-probe (issue #2398, roborev 1693): `sequential_scan` itself owns
        // the `stream_walk_partitions_parsed` accounting for this path (added at
        // its per-partition decode boundary, roborev 1692) — do NOT increment
        // again here. This loop only re-emits `sequential_scan`'s already-decoded
        // results, one call per RETURNED ROW; a second increment here would
        // double-count every partition body relative to what was actually decoded.
        let table_id = self.scan_table_id();
        // Issue #3097: prefer the caller's authoritative schema over the
        // reader-derived one, mirroring the streaming full-index route above.
        // Compaction callers pass `caller_schema = None`, so this collapses to
        // `self.schema.as_deref()` exactly as origin/main did.
        let schema = caller_schema.or(self.schema.as_deref());
        let entries = self
            .sequential_scan(&table_id, None, None, None, schema, scan_cancel)
            .await?;
        for (key, value) in entries {
            match emit((key, value))? {
                ControlFlow::Continue(()) => {}
                ControlFlow::Break(()) => return Ok(()),
            }
        }
        Ok(())
    }

    /// Authoritative structural coverage check for ONE index entry's Data.db slice
    /// (issue #2302, roborev jobs 1606/1609/1610): physically decode `raw` as
    /// exactly one partition and require the decode to consume the slice in FULL
    /// AND terminate via a CONFIRMED end-of-partition marker, never a bare "ran out
    /// of bytes" collapse. Shared by the materialising
    /// ([`iterate_all_partitions_via_full_index`](Self::iterate_all_partitions_via_full_index))
    /// and streaming full-index walks (issue #2361).
    ///
    /// Drives with `at_final_chunk = false` DELIBERATELY: that is LENIENT-off, so
    /// every ambiguous "consumed every byte but saw no explicit END_OF_PARTITION
    /// marker" / row-parse-failure / unrepresentable-range case reports
    /// `ParseStep::NeedMore` (never `Emitted`). `Emitted(consumed)` is then
    /// reachable ONLY via the driver's unconditional `is_end_of_partition`
    /// marker-consumption branch — so `consumed == raw.len()` PROVES the terminator
    /// was structurally confirmed as the slice's last byte (Cassandra's own
    /// on-disk completion signal, which CQLite's writer always appends). No
    /// heuristics (issue #28).
    pub(in crate::storage::sstable::reader) fn partition_slice_fully_consumed(
        &self,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
        raw: &[u8],
        schema: Option<&TableSchema>,
    ) -> Result<bool> {
        use crate::storage::sstable::reader::parsing::ParseStep;
        // Issue #3058: STRUCTURE-ONLY drive. The verdict below reads only the byte
        // consumption, and every row this used to build went straight to a no-op
        // closure — including, per row, a `CellWriteMetadata` map. Dropping that
        // build is what lets the token-scoped single-source read path allocate ZERO
        // metadata maps (spec R3); the framing, and therefore the verdict, is
        // unchanged.
        let step = parser.parse_one_partition_structure_only(raw, schema, self, false)?;
        Ok(matches!(step, ParseStep::Emitted(consumed) if consumed == raw.len()))
    }
}

#[cfg(test)]
mod check_token_order_tests {
    use super::check_token_order;

    /// The FIRST partition (`prev = None`) never fails, regardless of its token.
    #[test]
    fn first_partition_always_passes() {
        assert!(check_token_order(None, i64::MIN, b"a", 0).is_ok());
        assert!(check_token_order(None, 0, b"a", 0).is_ok());
        assert!(check_token_order(None, i64::MAX, b"a", 0).is_ok());
    }

    /// Ascending tokens (the well-formed case) pass at every step, whatever the
    /// key bytes are (token strictly dominates the order).
    #[test]
    fn ascending_tokens_pass() {
        assert!(check_token_order(Some((-100, b"z")), -50, b"a", 1).is_ok());
        assert!(check_token_order(Some((-50, b"z")), 0, b"a", 2).is_ok());
        assert!(check_token_order(Some((0, b"z")), 50, b"a", 3).is_ok());
    }

    /// Equal tokens with ASCENDING (or equal) key bytes — a genuine Murmur3
    /// collision between distinct keys stored in the correct on-disk order — are
    /// NOT a violation.
    #[test]
    fn equal_tokens_ascending_keys_pass() {
        assert!(check_token_order(Some((42, b"aaa")), 42, b"aab", 1).is_ok());
        assert!(check_token_order(Some((42, b"aaa")), 42, b"aaa", 1).is_ok());
        // Unsigned-lexicographic: a high byte is GREATER than a low byte.
        assert!(check_token_order(Some((42, &[0x01])), 42, &[0xff], 1).is_ok());
    }

    /// Hardening B (issue #2361, roborev round 2): a STRICTLY DECREASING token —
    /// the out-of-order-Index.db shape this guard exists to catch — fails closed
    /// with `Error::corruption`, never a silent pass-through to the k-way merger.
    #[test]
    fn strictly_decreasing_token_fails_closed() {
        let result = check_token_order(Some((100, b"a")), 99, b"z", 5);
        assert!(
            matches!(result, Err(crate::Error::Corruption(_))),
            "a decreasing token must fail closed as Error::corruption, got {result:?}"
        );
    }

    /// Hardening B (issue #2361, roborev round 3): on a token TIE, a DECREASING
    /// raw-key byte sequence is also out of order relative to the materialising
    /// sibling's `sort_by_token_order` (`token`, then key bytes) and the merger's
    /// required order — so it too must fail closed, not slip through a token-only
    /// check.
    #[test]
    fn equal_token_decreasing_key_fails_closed() {
        let result = check_token_order(Some((42, b"aab")), 42, b"aaa", 7);
        assert!(
            matches!(result, Err(crate::Error::Corruption(_))),
            "a decreasing key on a token tie must fail closed, got {result:?}"
        );
        // Unsigned-lexicographic: 0xff (prev) then 0x01 (curr) is a decrease.
        let result = check_token_order(Some((42, &[0xff])), 42, &[0x01], 7);
        assert!(
            matches!(result, Err(crate::Error::Corruption(_))),
            "0xff -> 0x01 on a token tie must fail closed (unsigned), got {result:?}"
        );
    }
}
