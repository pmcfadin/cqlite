//! Single-partition compaction seek (issue #2207, Stage 1).
//!
//! The unwired point-read machinery — [`might_contain_partition`] (presence
//! oracle), [`lookup_partition_via_bti_trie`] (BTI `da`) / [`lookup_partition_with_index`]
//! (BIG `nb` Summary/Index) — resolves whether a partition is present and, if so,
//! WHERE. This module composes those into the one public surface the Flight
//! producer needs: given a partition key, return the target partition's rows in
//! the **exact same [`CompactionRow`] form the full-scan compaction stream
//! produces** (tombstones preserved, per-row timestamps), so the k-way merge
//! reconciles the point path byte-identically to the scan path.
//!
//! Pruning is fail-open toward reading (the correctness spine): a candidate is
//! reported [`SinglePartitionCompaction::DefinitelyAbsent`] ONLY on an exact
//! presence-oracle negative; a missing/ambiguous index degrades to
//! [`SinglePartitionCompaction::IndexUnavailable`] (the caller scans that one
//! SSTable), never a wrong or silently-skipped answer (issues #28, #2295).
//!
//! [`might_contain_partition`]: SSTableReader::might_contain_partition
//! [`lookup_partition_via_bti_trie`]: SSTableReader::lookup_partition_via_bti_trie
//! [`lookup_partition_with_index`]: SSTableReader::lookup_partition_with_index

use super::super::compaction_row::CompactionRow;
use super::super::SSTableReader;
use crate::storage::scan_cancel::ScanCancel;
use crate::Result;
use std::ops::ControlFlow;

/// Outcome of a single-partition candidate probe against one SSTable.
///
/// **The three-way absence-vs-failure invariant** (roborev, issue #2207 — state
/// this here, not just in prose elsewhere, since it is the correctness spine
/// every caller relies on):
///
/// - [`DefinitelyAbsent`](Self::DefinitelyAbsent) is returned **ONLY** on an
///   exact presence-oracle negative (a bloom `might_contain == false`, or a BTI
///   trie miss) — the ONE case that may prune a candidate SSTable from the read.
/// - [`IndexUnavailable`](Self::IndexUnavailable) is returned for **EVERY OTHER
///   kind of resolution/read anomaly**: no random-access index at all, an
///   inconclusive BIG Index.db miss (#1572), an unreadable/corrupt index (a BTI
///   trie parse error or an Index.db read error — roborev IMPORTANT-1), an
///   un-boundable last partition, or a resolved offset the materialized window
///   never reached (a bad end bound / truncated-SSTable shape — roborev MEDIUM).
///   None of these may EVER collapse to `DefinitelyAbsent` or an empty `Rows` —
///   only a genuine, fully-materialized decode may report absence.
/// - [`Rows`](Self::Rows) is returned only once a seek has been FULLY EXECUTED
///   (the window reached and covered the target partition's bytes end-to-end)
///   — an empty `Vec` here means a confirmed prefix-collision candidate for an
///   absent key, decoded and verified, not "we could not tell."
#[derive(Debug)]
pub enum SinglePartitionCompaction {
    /// The presence oracle proved the key is definitively absent from this
    /// SSTable (an exact bloom negative, or a BTI trie miss). The candidate is
    /// pruned; `cqlite.read.sstables_pruned` was already incremented by the
    /// oracle. Never returned when presence is positive, unknown, or the index is
    /// missing.
    DefinitelyAbsent,
    /// The key MIGHT be present but this SSTable has no usable random-access index
    /// (Summary/Index absent, a #1572-style inconclusive index miss, an
    /// unreadable/corrupt index, or a resolved offset the window never reached).
    /// The caller MUST read this SSTable — scanning its partitions and filtering
    /// to the key — never skip it. Degrades speed, never correctness (#2295).
    IndexUnavailable,
    /// The partition was seeked and decoded. `rows` are its compaction rows
    /// (tombstones preserved), byte-identical to the full-scan stream restricted
    /// to this partition. Empty when the resolved candidate was a prefix-collision
    /// for an absent key (authoritative empty — do NOT fall back).
    Rows(Vec<CompactionRow>),
}

/// Outcome of decoding a partition at a caller-supplied, already-authoritative
/// offset — the primitive `salvage_sstable` builds its recovery loop on
/// (issue #4196, design D1). See
/// [`decode_partition_at_offset_for_salvage`](SSTableReader::decode_partition_at_offset_for_salvage).
///
/// `write-support`-gated: `salvage_sstable`, its only consumer, does not
/// exist without that feature (`write_engine::salvage` is itself gated
/// `all(feature = "write-support", not(feature = "tombstones"))`), so
/// without this gate a `write-support`-off build (this module's OWN gate is
/// only `not(tombstones)`) sees this as dead code / an unused re-export.
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub(crate) enum PartitionAtOffsetOutcome {
    /// The partition decoded completely and — when the boundary source named
    /// an independent key for this slot — its key matched.
    Rows(Vec<CompactionRow>),
    /// The decoded key at `offset` did not match the boundary source's key
    /// for this slot (spec R4.2, loss class `key-mismatch`).
    KeyMismatch,
    /// A row failed to decode partway through the partition (design D2
    /// atomicity). The caller MUST NOT write any row that had decoded
    /// before the error (the resurrection-bug rationale in D2) — enforced
    /// structurally, not by a count: `drive_partition_sliding`
    /// (`reader/parsing/row_decoder/partition_driver.rs`, design note
    /// "Finding 1 / issue #827") buffers every row of a partition locally
    /// and forwards them to its caller ONLY on structural completion, so a
    /// mid-partition error here is reached with NOTHING externally visible
    /// yet — there is no partial row set to accidentally write.
    ///
    /// This variant previously carried a `rows_decoded_before_failure:
    /// usize` field (roborev, issue #4196, round 21 — removed): it was
    /// ALWAYS `0`, proven by an exhaustive scan (round 20) of a real
    /// multi-row partition — the SAME buffering this doc now describes
    /// means no caller could ever observe a nonzero value, so the field
    /// was dead weight carrying a doc claim ("counts rows that HAD
    /// decoded") the code could never satisfy. See issue #4218 for
    /// reinstating a real count once the partition driver can report
    /// incremental progress.
    DecodeError { error: crate::error::Error },
    /// The materialized window did not cover the partition's authoritative
    /// `[offset, end)` — EOF before the resolved end, or no trustworthy end
    /// could be established for the last partition (spec R2.3 `truncated`).
    Truncated,
    /// The partition's authoritative `[offset, end)` span exceeds
    /// [`SALVAGE_MAX_PLAUSIBLE_PARTITION_BYTES`] — a DISTINCT outcome from
    /// [`Truncated`](Self::Truncated) (roborev, issue #4196, round 17 Low
    /// finding): `Truncated`'s wording ("extends past Data.db's actual
    /// end") is factually FALSE for this case — the file is intact, and the
    /// span was refused as a memory-safety precaution, not because
    /// anything ran out. Conflating the two told an operator of a
    /// genuinely wide (but healthy) partition that their `Data.db` was
    /// truncated when it was not.
    SpanTooWide {
        /// The refused span's width in bytes, for the loss message.
        span_bytes: u64,
    },
}

/// The largest `[offset, end)` span `decode_partition_at_offset_for_salvage`
/// will ever materialize for ONE partition, regardless of what the boundary
/// source claims (roborev, issue #4196, round-15 Medium finding 1 — an Opus
/// whole-module audit). Matches `compression.rs::MAX_DECOMPRESSED_SIZE`'s
/// established 128 MiB convention (this crate's own <128 MB memory target).
///
/// **Why this exists**: for the LAST boundary entry (`end_bound: None`) —
/// including an entry that is only ARTIFICIALLY last because `Index.db`/
/// `Partitions.db` was truncated exactly on an entry boundary and parsed
/// cleanly with fewer real entries — `end` resolves to the WHOLE remaining
/// data section (`section_len` uncompressed, `safe_data_length` compressed),
/// not one partition's worth. Every EXISTING guard (round 9's boundary-side
/// clamp, round 10/12's `data_length`/`chunk_size` zero-fallbacks) bounds
/// `end` against the REAL FILE SIZE, which is exactly what this trigger
/// satisfies — a short boundary source's `end` is `<= safe_data_length` by
/// construction, so it passes every one of them, then allocates the entire
/// remaining multi-GB section (uncompressed: one `read_exact_at`; compressed:
/// `pull_chunk_window` decompressing every remaining real chunk into one
/// resident `Vec`) before the slot is ever classified.
///
/// **The trade-off, stated explicitly**: a genuinely healthy partition wider
/// than 128 MiB is classified `Truncated` rather than recovered under this
/// cap — a false loss, not silence or wrong data. Consistent with this
/// tool's whole design philosophy (design D2/D3): salvage never guesses:
/// a conservative, NAMED loss beats an unbounded allocation that starves or
/// OOM-kills the process running it, on a file this tool exists specifically
/// to recover from. Applied UNCONDITIONALLY to both the `end_bound: None`
/// case the audit trigger names AND a `Some(end_bound)` case (a corrupted,
/// still-plausible-looking middle boundary can name a gap just as wide) —
/// the risk is the SPAN's width, not which resolution path produced it.
pub(crate) const SALVAGE_MAX_PLAUSIBLE_PARTITION_BYTES: u64 = 128 * 1024 * 1024;

/// `true` iff the half-open span `[start, end)` is wider than
/// [`SALVAGE_MAX_PLAUSIBLE_PARTITION_BYTES`] — factored out of BOTH the
/// uncompressed and compressed arms of `decode_partition_at_offset_for_salvage`
/// (roborev, issue #4196, round-15 Medium finding 1) so the ONE piece of
/// arithmetic both bounds checks share is unit-testable directly, without
/// needing an actual 128+ MB fixture for the compressed arm (which reaches
/// this same check via `[window_base, end)`, a span this crate has no
/// practical way to construct a REAL multi-chunk compressed fixture for in
/// a test). `end < start` (never expected, both call sites establish
/// `end > start`/`end >= offset` first) is defensively treated as
/// NOT exceeding, via `saturating_sub`, rather than a panic or a wrap.
pub(crate) fn exceeds_plausible_partition_span(start: usize, end: usize) -> bool {
    (end.saturating_sub(start)) as u64 > SALVAGE_MAX_PLAUSIBLE_PARTITION_BYTES
}

impl SSTableReader {
    /// Probe one SSTable for a single partition, returning its compaction rows via
    /// an authoritative seek — or a prune / scan-fallback signal (issue #2207).
    ///
    /// This is the public core primitive the Flight point-read path composes into
    /// its existing k-way merge. Correctness spine (fail-open toward reading):
    ///
    /// 1. [`might_contain_partition`](Self::might_contain_partition) reports a
    ///    definite negative → [`SinglePartitionCompaction::DefinitelyAbsent`]
    ///    (pruned + counted).
    /// 2. No random-access index → [`SinglePartitionCompaction::IndexUnavailable`]
    ///    (the caller scans this SSTable).
    /// 3. Otherwise resolve the partition's uncompressed offset (BTI trie / BIG
    ///    Index.db) and its authoritative end bound (successor offset / data
    ///    length), materialize ONLY the covering chunk window, and parse the one
    ///    partition with the SAME compaction parser the full scan uses →
    ///    [`SinglePartitionCompaction::Rows`]. A BIG Index.db miss (inconclusive,
    ///    #1572) or an un-boundable last partition degrades to
    ///    [`SinglePartitionCompaction::IndexUnavailable`].
    ///
    /// `partition_key` is the raw partition-key bytes (as
    /// `PartitionKey::to_bytes` produces). `schema` is the authoritative table
    /// schema (the parser needs column names).
    ///
    /// `scan_cancel` is an explicit PER-CALL cancellation token (issue #2346),
    /// mirroring [`SSTableReader::stream_all_partitions_for_compaction`] — not
    /// the reader's own `scan_cancel` field, so a shared/cached `Arc<SSTableReader>`
    /// can serve two concurrent point-read probes with independent cancellation.
    pub async fn read_single_partition_for_compaction(
        &self,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
        scan_cancel: &ScanCancel,
    ) -> Result<SinglePartitionCompaction> {
        // 1. Presence oracle — the only source of a definite prune. Emits
        //    `cqlite.read.sstables_pruned` internally on a definite negative.
        if !self.might_contain_partition(partition_key) {
            return Ok(SinglePartitionCompaction::DefinitelyAbsent);
        }

        // 2. No random-access index (Data.db-only snapshot, #2295) → the caller
        //    must scan this SSTable. Never skip a candidate that might hold the key.
        if !self.has_partition_index() {
            return Ok(SinglePartitionCompaction::IndexUnavailable);
        }

        // 3. Resolve the target partition's UNCOMPRESSED Data.db start offset.
        //
        //    Fail-safe (roborev IMPORTANT-1, #2207 spec): an unreadable/corrupt
        //    index (BTI trie parse error, Index.db read error) degrades to
        //    `IndexUnavailable` — this SSTable is scanned in full and filtered,
        //    never a hard-failed query. The scan path never consults this index
        //    at all, so a broken index here must not turn a query the scan path
        //    would still answer into an `Err`.
        let is_bti = self.is_bti();
        let offset = if is_bti {
            match self.lookup_partition_via_bti_trie(partition_key) {
                Ok(Some(off)) => off,
                // A BTI trie `Ok(None)` is an AUTHORITATIVE-by-construction absence:
                // `bti_trie_resolve` returns `Ok(None)` ONLY for a definitive trie
                // MISS (a fully-descended trie with no matching entry), and routes
                // EVERY degraded/unusable state — a parse error, an out-of-range
                // root_offset, a missing Rows.db for a wide partition — through the
                // `Err(_)` arm below (scan fallback), never through `Ok(None)`. The
                // trie IS the presence oracle for a BTI SSTable, so this is the same
                // signal `might_contain_partition` (step 1) already prunes on; this
                // arm is the defensive equivalent. The correct three-exit answer is
                // `DefinitelyAbsent` (an authoritative prune), NOT `Rows(Vec::new())`
                // — an empty `Rows` is reserved for a FULLY-DECODED prefix-collision
                // (nothing was decoded here), so emitting it would violate the enum
                // contract (see `SinglePartitionCompaction`'s doc).
                Ok(None) => return Ok(SinglePartitionCompaction::DefinitelyAbsent),
                Err(e) => {
                    tracing::debug!(
                        "BTI Partitions.db trie lookup failed during point read; \
                         falling back to a full scan of this SSTable (#2207 fail-safe): {e}"
                    );
                    return Ok(SinglePartitionCompaction::IndexUnavailable);
                }
            }
        } else {
            match self.lookup_partition_with_index(partition_key).await {
                Ok(Some((off, _size))) => off,
                // A BIG Index.db miss is NOT a definitive absent (#1572): a
                // truncated/partial map can drop an entry for a present partition.
                // Degrade to a full scan of this SSTable, never a wrong empty.
                Ok(None) => return Ok(SinglePartitionCompaction::IndexUnavailable),
                Err(e) => {
                    tracing::debug!(
                        "Index.db lookup failed during point read; falling back to a \
                         full scan of this SSTable (#2207 fail-safe): {e}"
                    );
                    return Ok(SinglePartitionCompaction::IndexUnavailable);
                }
            }
        };

        // 4. Authoritative exclusive end of the target partition: the successor
        //    partition's start (next trie/index entry), or `None` for the last.
        //    Same fail-safe class as step 3 — it reads the same index/trie.
        let end_bound = match self.successor_partition_offset(offset, partition_key).await {
            Ok(bound) => bound,
            Err(e) => {
                tracing::debug!(
                    "successor-partition resolution failed during point read; falling \
                     back to a full scan of this SSTable (#2207 fail-safe): {e}"
                );
                return Ok(SinglePartitionCompaction::IndexUnavailable);
            }
        };

        // 5. Materialize `[offset, end)` decompressed and parse the one partition.
        //    `is_bti` decides how a FOREIGN-key decode is interpreted (step below):
        //    a BTI trie resolves by PREFIX (a decoded different key is a genuine
        //    prefix-collision → authoritative empty), whereas a BIG Index.db entry
        //    is an EXACT partition offset (a decoded different key means the entry
        //    is stale/corrupt and pointed at another valid partition → scan
        //    fallback, never a silent drop).
        match self
            .seek_partition_compaction_rows(
                offset,
                end_bound,
                partition_key,
                schema,
                is_bti,
                scan_cancel,
            )
            .await?
        {
            Some(rows) => Ok(SinglePartitionCompaction::Rows(rows)),
            // Could not bound the (last) partition authoritatively — fall back to a
            // full scan of this SSTable for correctness (#953 mandate).
            None => Ok(SinglePartitionCompaction::IndexUnavailable),
        }
    }

    /// Materialize the decompressed window covering `[offset, end)` and parse the
    /// single partition that starts at `offset`, collecting its compaction rows.
    ///
    /// Returns `Ok(None)` when the last partition cannot be bounded authoritatively
    /// (no successor and no usable data length) — the caller falls back to a scan.
    /// The parse uses the SAME `build_v5_parser(false)` +
    /// `parse_one_partition_for_compaction` the full-scan compaction stream uses,
    /// so the rows are byte-identical to that stream restricted to this partition.
    ///
    /// # A FOURTH exit: a propagated decode refusal (#3782 AC2)
    ///
    /// The three-exit spine routes every *anomaly of the index* to `Ok(None)` →
    /// scan fallback. Since #3782 there is a fourth exit that is NOT an anomaly of
    /// the index: `parse_one_partition_for_compaction` is called with
    /// `at_final_chunk = true` over a window PROVEN to cover `[offset, end)`, so a
    /// row that fails to decode there is truncation or corruption of `Data.db`, and
    /// that `Err` PROPAGATES.
    ///
    /// It deliberately does NOT degrade to `Ok(None)`. #3782 AC2 is exactly that a
    /// fatal decode error on the index-random-read path surfaces as that error —
    /// no WARN-and-fall-back-to-sequential-scan detour — because the detour is the
    /// silent degradation the issue exists to remove: the scan would re-read the
    /// same damaged bytes through the tolerant break and answer SHORT, reporting
    /// success for a partition it lost rows from.
    ///
    /// Reachability, since the guard above already covers the truncated-window
    /// cases: `within >= window.len()` and `!reached_end` both return `Ok(None)`
    /// BEFORE the parse, and `pull_chunk_window` materialises WHOLE chunks, so the
    /// window always extends to the end of the chunk containing `end`. A decode
    /// refusal therefore requires the partition body to run past that chunk
    /// boundary — i.e. the index-derived `end` understates the partition's true
    /// extent by at least a chunk, which means `Index.db`/the trie DISAGREES with
    /// `Data.db`. On such an SSTable refusing is the correct answer: the successor
    /// offset is, by construction, the next partition's exact start
    /// (`partition_successor.rs` takes the minimum index offset strictly greater
    /// than this one), so a well-formed pair cannot produce it.
    async fn seek_partition_compaction_rows(
        &self,
        offset: u64,
        end_bound: Option<u64>,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
        is_bti: bool,
        scan_cancel: &ScanCancel,
    ) -> Result<Option<Vec<CompactionRow>>> {
        // Cooperative cancellation (issue #2207, roborev job 1620 MEDIUM): the seek
        // materializes a covering chunk window and parses one partition — the same
        // heavy, previously-uninterruptible work the full-scan compaction stream
        // polls `scan_cancel` inside (issue #2264). Mirror that here so a Flight
        // `do_get` whose client has already disconnected abandons the seek instead
        // of decompressing/parsing to completion. Poll ONCE before materializing
        // (an already-cancelled read exits before any I/O), inside the chunk-window
        // pull loop (`pull_chunk_window`), and once more just before parsing.
        // Issue #2346: `scan_cancel` is now the caller's PER-CALL token, not
        // `self.scan_cancel`.
        scan_cancel.check()?;

        let offset = offset as usize;
        let owned_schema = schema.cloned().or_else(|| self.get_table_schema(None));
        let parser = self.build_v5_parser(false);

        // Chunk length drives the two window strategies. Absent/zero → uncompressed
        // (the WriteEngine's `nb` output, and uncompressed BTI): read the whole
        // section once and parse from `offset`. Present → compressed: pull only the
        // chunks covering `[offset, end)`.
        let chunk_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.chunk_length as usize)
            .filter(|&len| len > 0);

        let (window, within, reached_end) = match chunk_length {
            None => {
                let whole = self.point_read_whole_section().await?;
                // The uncompressed whole-section read is authoritative for the
                // partition's extent; an offset past the section is caught by the
                // `within >= window.len()` guard below.
                (whole, offset, true)
            }
            Some(len) => {
                let target_chunk = offset / len;
                let window_base = target_chunk * len;
                let within = offset - window_base;
                // Authoritative exclusive end in the uncompressed domain.
                let end = match end_bound {
                    Some(e) => e as usize,
                    None => match self
                        .compression_info
                        .as_ref()
                        .map(|ci| ci.data_length as usize)
                        .filter(|&len| len > offset)
                    {
                        Some(len) => len,
                        // Last partition, unknown length → cannot bound → scan.
                        None => return Ok(None),
                    },
                };
                let (window, reached_end) = self
                    .pull_chunk_window(target_chunk, window_base, end, scan_cancel)
                    .await?;
                (window, within, reached_end)
            }
        };

        if within >= window.len() || !reached_end {
            // MEDIUM fix (roborev, issue #2207): the materialized window did NOT
            // cover the full resolved `[offset, end)`. Either it never reached the
            // resolved offset (`within >= window.len()` — a bad/stale end bound),
            // or `pull_chunk_window` hit EOF before `end` (`!reached_end`) on a
            // truncated/corrupt SSTable, leaving only a prefix of the partition's
            // bytes. Parsing that partial buffer with `at_final_chunk = true`
            // FLUSHES a partially-decoded partition and would surface it as
            // authoritative `Rows(...)` — hiding corruption and emitting
            // wrong/short rows. This is NOT the same signal as a genuine
            // prefix-collision absence (below): there we decoded the FULL
            // partition and confirmed a different key; here we could not even
            // materialize the target bytes to check. Treating either as an answer
            // is a false-negative the presence-oracle spine forbids (spec: only an
            // exact bloom negative may prune). Signal `None` so the caller degrades
            // to `IndexUnavailable` (scan this SSTable), never a silent
            // partial/empty answer.
            return Ok(None);
        }

        // Cancellation poll just before the parse — the covering window is now
        // materialized, so a cancel observed here skips the (potentially large)
        // partition decode entirely (issue #2207 fail-safe cancellation).
        scan_cancel.check()?;

        // Parse the FIRST partition at `window[within..]`. Collect every row whose
        // decoded key equals the queried key; a decoded DIFFERENT key means the
        // resolved candidate was a prefix-collision for an absent key.
        let mut rows: Vec<CompactionRow> = Vec::new();
        let mut saw_foreign_key = false;
        parser.parse_one_partition_for_compaction(
            &window[within..],
            owned_schema.as_ref(),
            self,
            true,
            &mut |row: CompactionRow| {
                if row.key.as_bytes() == partition_key {
                    rows.push(row);
                } else {
                    saw_foreign_key = true;
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;

        if saw_foreign_key && rows.is_empty() {
            if is_bti {
                // BTI (`da`): the trie resolves by PREFIX, so a fully-decoded
                // DIFFERENT key at the resolved offset is a genuine prefix-collision
                // for an absent key — authoritative empty (do NOT fall back).
                return Ok(Some(Vec::new()));
            }
            // BIG (`nb`): Index.db entries are EXACT partition offsets, so a decoded
            // FOREIGN key means the entry was stale/corrupt and pointed at a
            // DIFFERENT valid partition. Treating that as authoritative absence
            // would SILENTLY DROP the target key (roborev job 1616, High:
            // fail-safe violation). Degrade to `IndexUnavailable` → the caller
            // scans this SSTable and filters, never a false-empty. (Three-exit
            // invariant: DefinitelyAbsent = exact bloom negative only; Rows = a
            // complete seek of the CORRECT partition; anything anomalous here is a
            // scan fallback.)
            return Ok(None);
        }
        Ok(Some(rows))
    }

    /// Decode ONE partition at a caller-supplied, already-authoritative offset
    /// for `salvage_sstable` (issue #4196, design D1). Unlike
    /// [`SinglePartitionCompaction`], every anomaly is CLASSIFIED rather than
    /// degraded to a scan fallback: salvage's caller enumerated `offset` from
    /// the boundary source itself (`Index.db` / the `Partitions.db` trie), so
    /// there is no alternative source to fall back to for this one slot — the
    /// whole point of salvage is to name what is untrustworthy about it.
    ///
    /// `expected_key` is the boundary source's key for this slot when it
    /// carries one independently (always for BIG; for BTI only a `RowsOffset`
    /// leaf's inline key — see `salvage::boundaries`). `end_bound` is the NEXT
    /// boundary entry's offset (exclusive), or `None` for the last partition
    /// in the file. Reuses the SAME chunk-window materialization and
    /// [`parse_one_partition_for_compaction`] decoder the point-read path uses
    /// above (design D1) — never a fresh parser.
    ///
    /// [`parse_one_partition_for_compaction`]: crate::storage::sstable::reader::parsing::row_decoder::compaction::CompactionParser::parse_one_partition_for_compaction
    ///
    /// `write-support`-gated: see [`PartitionAtOffsetOutcome`]'s doc for why.
    #[cfg(feature = "write-support")]
    pub(crate) async fn decode_partition_at_offset_for_salvage(
        &self,
        offset: u64,
        end_bound: Option<u64>,
        expected_key: Option<&[u8]>,
        schema: Option<&crate::schema::TableSchema>,
        scan_cancel: &ScanCancel,
    ) -> Result<PartitionAtOffsetOutcome> {
        scan_cancel.check()?;

        let offset_usize = offset as usize;
        let owned_schema = schema.cloned().or_else(|| self.get_table_schema(None));
        let parser = self.build_v5_parser(false);

        let chunk_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.chunk_length as usize)
            .filter(|&len| len > 0);
        // The full-consumption check below applies ONLY to the uncompressed
        // branch: a compressed `window` is CHUNK-ALIGNED (`pull_chunk_window`
        // materializes whole chunks), so it can legitimately extend past
        // `end` into the next partition's leading bytes — `within + consumed
        // == window.len()` would be WRONG there.
        let is_uncompressed = chunk_length.is_none();
        // roborev, issue #4196 (round-4 Medium): the DECOMPRESSED-domain
        // `end` this slot's window must not be decoded past — same domain as
        // `offset_usize`/`data_offset` (Index.db positions and
        // `CompressionInfo.data_length` are both decompressed-domain).
        // Populated only in the compressed arm below; the uncompressed arm
        // already gets a full-consumption check (`is_uncompressed`, round 3).
        let mut compressed_end: Option<usize> = None;

        let (window, within, reached_end) = match chunk_length {
            None => {
                // Uncompressed: a BOUNDED positional read of `[offset, end)`
                // only (roborev, issue #4196) — NOT `point_read_whole_section`,
                // which materializes the ENTIRE data section. `recover_one_partition`
                // calls this primitive once per boundary entry, so a whole-section
                // read here would cost O(partitions x file_size) I/O and hold a
                // full-file allocation resident per call, violating the <128 MB
                // target and spec R6 for exactly the uncompressed case salvage is
                // most likely to see (CQLite's own writer only emits uncompressed
                // output). `end` is resolved from the SAME two sources the
                // compressed branch uses (the next boundary entry, or the total
                // data-section length for the last partition) and is NEVER
                // silently clamped to the file's actual length: an `end` that
                // exceeds what is really on disk IS the R2.3 truncation signal,
                // reported as `Truncated` rather than masked by reading fewer
                // bytes than the boundary source promised.
                let header_size = self.calculate_header_size() as u64;
                let file_len = self.point_source.len();
                let section_len = file_len.saturating_sub(header_size) as usize;
                let end = match end_bound {
                    Some(e) => e as usize,
                    None => section_len,
                };
                if offset_usize >= end || end > section_len {
                    return Ok(PartitionAtOffsetOutcome::Truncated);
                }
                // roborev, issue #4196, round-15 Medium finding 1: `end` is
                // bounded against the REAL file (`section_len`) above, but
                // NOT against one partition's plausible extent — for the
                // LAST entry (`end_bound: None`, real or artificially last
                // via a truncated boundary source), `end == section_len`
                // unconditionally, materializing the WHOLE remaining data
                // section in this one `Vec`. See
                // `SALVAGE_MAX_PLAUSIBLE_PARTITION_BYTES`'s doc.
                if exceeds_plausible_partition_span(offset_usize, end) {
                    return Ok(PartitionAtOffsetOutcome::SpanTooWide {
                        span_bytes: (end - offset_usize) as u64,
                    });
                }
                let mut buf = vec![0u8; end - offset_usize];
                self.point_source
                    .read_exact_at(header_size + offset_usize as u64, &mut buf)?;
                (buf, 0usize, true)
            }
            Some(len) => {
                let target_chunk = offset_usize / len;
                let window_base = target_chunk * len;
                let within = offset_usize - window_base;
                // roborev, issue #4196, round-12 Medium finding: round 9/10's
                // clamp lives in `chunks.rs`'s `compressed_chunk_preflight`,
                // whose RETURNED `data_length` only bounds `recover.rs`'s OWN
                // `chunk_range_end` (the chunk-CRC pre-flight's range) — it is
                // never threaded into THIS function, so both the `None`
                // (last-partition) case, which reads `ci.data_length` raw, and
                // the `Some(e)` (non-last) case, which uses `end_bound` raw
                // (a possibly-corrupted NEXT entry's `data_offset`), could
                // still ask `pull_chunk_window` for a window past what the
                // chunk table can really supply. `pull_chunk_window` has no
                // pre-allocation (round 9/10/11's own tests already prove
                // that), but it DOES decompress every remaining real chunk
                // before giving up — for a genuinely large production
                // `Data.db` that means materializing "the rest of the file"
                // into one resident `Vec`, violating spec R6 ("one partition
                // resident") and the <128 MB target. Compute the SAME clamp
                // `chunks.rs` does (`chunk_count * chunk_length`, both
                // already bounded by `CompressionInfo::parse`/`validate`)
                // directly from the reader's own already-open
                // `CompressionInfo` — no value needs threading from
                // `recover.rs` at all — and refuse (`Truncated`) whenever the
                // resolved `end`, from EITHER source, exceeds it.
                let safe_data_length = self.compression_info.as_deref().map(|ci| {
                    let chunk_table_bound =
                        (ci.chunk_offsets.len() as u64).saturating_mul(ci.chunk_length as u64);
                    // Mirrors `chunks.rs::compressed_chunk_preflight`'s OWN
                    // zero-fallback exactly (roborev, issue #4196, round-12
                    // Medium finding, second half found while regression-
                    // testing the first): `ci.data_length` has no lower
                    // bound either, so a ZEROED field must not be treated as
                    // "0 bytes of real data" (which would make every
                    // legitimate `end > 0` look implausible and refuse
                    // EVERY partition, not just the corrupted one) — fall
                    // back to the always-positive structural bound instead.
                    if ci.data_length == 0 {
                        chunk_table_bound
                    } else {
                        ci.data_length.min(chunk_table_bound)
                    }
                });
                let end = match end_bound {
                    Some(e) => e as usize,
                    None => match safe_data_length
                        .map(|l| l as usize)
                        .filter(|&l| l > offset_usize)
                    {
                        Some(l) => l,
                        // Last partition, no CompressionInfo bound to trust:
                        // cannot establish a trustworthy end for this slot.
                        None => return Ok(PartitionAtOffsetOutcome::Truncated),
                    },
                };
                if let Some(safe_len) = safe_data_length {
                    if end as u64 > safe_len {
                        return Ok(PartitionAtOffsetOutcome::Truncated);
                    }
                }
                // roborev, issue #4196, round-15 Medium finding 1: `end` is
                // bounded against `safe_data_length` (the REAL file) above,
                // but NOT against one partition's plausible extent —
                // `pull_chunk_window` decompresses every real chunk in
                // `[window_base, end)` into ONE resident `Vec`, so a short
                // boundary source's `end == safe_data_length` (the
                // `end_bound: None` arm above) materializes the WHOLE
                // remaining data section before this slot is classified.
                // See `SALVAGE_MAX_PLAUSIBLE_PARTITION_BYTES`'s doc.
                if exceeds_plausible_partition_span(window_base, end) {
                    return Ok(PartitionAtOffsetOutcome::SpanTooWide {
                        span_bytes: (end - window_base) as u64,
                    });
                }
                compressed_end = Some(end);
                let (window, reached_end) = self
                    .pull_chunk_window(target_chunk, window_base, end, scan_cancel)
                    .await?;
                (window, within, reached_end)
            }
        };

        if within >= window.len() || !reached_end {
            // The materialized window did not cover the resolved
            // `[offset, end)` — EOF before the partition's authoritative end
            // (a truncated Data.db, spec R2.3 `truncated`).
            return Ok(PartitionAtOffsetOutcome::Truncated);
        }

        scan_cancel.check()?;

        let mut rows: Vec<CompactionRow> = Vec::new();
        let mut key_mismatch = false;
        let decode_result = parser.parse_one_partition_for_compaction(
            &window[within..],
            owned_schema.as_ref(),
            self,
            true,
            &mut |row: CompactionRow| {
                if let Some(expected) = expected_key {
                    if rows.is_empty() && !key_mismatch && row.key.as_bytes() != expected {
                        key_mismatch = true;
                    }
                }
                rows.push(row);
                Ok(ControlFlow::Continue(()))
            },
        );

        match decode_result {
            Ok(step) => {
                // roborev, issue #4196 (High): the uncompressed branch's
                // `end` for the LAST partition comes from the file's ACTUAL
                // length (`section_len`), not a declared one — a Data.db
                // truncated mid-row therefore still satisfies `within <
                // window.len() && reached_end` above (there is nothing left
                // to compare against). `[within..]` is exactly ONE
                // partition's bytes, so a genuinely complete decode must
                // consume the WHOLE window; anything less is leftover bytes
                // the parser did not account for, treated as `Truncated`
                // rather than silently accepted as a short-but-clean
                // partition (the D2 resurrection hazard: a partial row set
                // written as if complete). `Done` (zero bytes consumed) is
                // likewise suspicious when the window is non-empty.
                if is_uncompressed {
                    use crate::storage::sstable::reader::parsing::row_decoder::ParseStep;
                    let fully_consumed = match step {
                        ParseStep::Emitted(consumed) => within + consumed == window.len(),
                        ParseStep::Done => within == window.len(),
                        ParseStep::NeedMore => false, // unreachable at_final_chunk=true
                    };
                    if !fully_consumed {
                        return Ok(PartitionAtOffsetOutcome::Truncated);
                    }
                } else if let Some(end) = compressed_end {
                    // roborev, issue #4196 (round-4 Medium): the compressed
                    // WINDOW is chunk-aligned and LEGITIMATELY extends past
                    // `end` into the next partition's leading bytes (unlike
                    // the uncompressed case above, so an equality check
                    // against `window.len()` would be wrong) — but the
                    // PARSER's own `consumed` count, decoded from
                    // `window[within..]`, is a DIFFERENT quantity: it is how
                    // many bytes THIS ONE partition's content actually
                    // occupied, and for a healthy boundary source that is BY
                    // DEFINITION exactly `end - offset` (`end` names where
                    // the NEXT partition starts). round-11's Medium finding:
                    // the original `consumed <= max_allowed` only rejected
                    // OVER-consumption (decoding past `end` into the next
                    // partition, the round-4 hazard) but silently ACCEPTED
                    // under-consumption too — a corrupted/fabricated
                    // `END_OF_PARTITION` marker that stops the parser EARLY
                    // returns `Rows(prefix)` as if complete, exactly the D2
                    // resurrection hazard the uncompressed branch's equality
                    // check already guards against: the un-decoded tail can
                    // carry a tombstone or later-timestamp cell shadowing
                    // what was already accepted, with no `Loss` recorded.
                    // Require EQUALITY, mirroring the uncompressed arm; a
                    // `Done` covering a NON-EMPTY `[offset, end)` window
                    // means nothing was consumed for bytes the boundary
                    // source says exist — also suspicious, also `Truncated`.
                    use crate::storage::sstable::reader::parsing::row_decoder::ParseStep;
                    let max_allowed = end.saturating_sub(offset_usize);
                    let fully_consumed = match step {
                        ParseStep::Emitted(consumed) => consumed == max_allowed,
                        ParseStep::Done => max_allowed == 0,
                        ParseStep::NeedMore => false, // unreachable at_final_chunk=true
                    };
                    if !fully_consumed {
                        return Ok(PartitionAtOffsetOutcome::Truncated);
                    }
                }
                if key_mismatch {
                    Ok(PartitionAtOffsetOutcome::KeyMismatch)
                } else if rows.is_empty() && expected_key.is_some() {
                    // roborev, issue #4196: the key cross-check above lives
                    // INSIDE the row callback, so a decode that emits ZERO
                    // rows never runs it — a garbage offset that happens to
                    // parse as "no rows" would otherwise silently become an
                    // accepted, nothing-to-write partition (indistinguishable
                    // from a genuine empty reconciliation downstream) with
                    // its key NEVER checked against the boundary source's
                    // claim for this slot. When the boundary source names an
                    // independent key, an empty decode cannot be trusted.
                    Ok(PartitionAtOffsetOutcome::KeyMismatch)
                } else {
                    Ok(PartitionAtOffsetOutcome::Rows(rows))
                }
            }
            Err(error) => {
                if key_mismatch {
                    // The very first row already disagreed with the boundary
                    // source's key for this slot; that IS the finding — do not
                    // also report a decode failure for it.
                    Ok(PartitionAtOffsetOutcome::KeyMismatch)
                } else {
                    Ok(PartitionAtOffsetOutcome::DecodeError { error })
                }
            }
        }
    }

    /// Pull the decompressed chunks covering `[window_base, end)` starting at
    /// `target_chunk`, returning the concatenated bytes plus whether the window
    /// actually reached `end`. Never stitches to EOF: it stops as soon as the
    /// window reaches `end` (the target partition's exclusive end) or the SSTable
    /// is exhausted. Mirrors the bounded chunk fetch the user-facing
    /// single-partition seek uses (issue #953), so a head-of-file point read never
    /// decompresses the whole `Data.db`.
    ///
    /// The returned `bool` is `false` when the chunks ran out (EOF) before the
    /// window covered `end` — a truncated/corrupt SSTable whose partial buffer the
    /// caller MUST NOT parse as authoritative (issue #2207 fail-safe spine).
    async fn pull_chunk_window(
        &self,
        target_chunk: usize,
        window_base: usize,
        end: usize,
        scan_cancel: &ScanCancel,
    ) -> Result<(Vec<u8>, bool)> {
        use super::super::chunk_source::ChunkSource;
        use crate::storage::sstable::compression::Compression;

        let compression_opt = self
            .compression_reader
            .as_ref()
            .map(|cr| Compression::new(*cr.algorithm()))
            .transpose()?;
        let comp_info = self.compression_info.as_deref().ok_or_else(|| {
            crate::Error::corruption(
                "point-read chunk-targeted path requires CompressionInfo but it is absent",
            )
        })?;
        let chunk_source = ChunkSource::new(
            self.point_source.as_ref(),
            comp_info,
            compression_opt.as_ref(),
            &self.chunk_cache,
            self.stats.file_size,
            0, // NB/BTI: chunk offsets are absolute from Data.db byte 0.
            super::NS_BTI_CHUNK,
            self.chunk_cache_id,
        );

        let mut window: Vec<u8> = Vec::new();
        let mut chunk_index = target_chunk;
        let mut pulled: usize = 0;
        while window_base + window.len() < end {
            // Cooperative cancellation (issue #2207, roborev job 1620 MEDIUM): a very
            // wide target partition can span hundreds of chunks; poll at a bounded
            // interval so a cancel is honoured mid-window, mirroring the full-scan
            // stream's `chunk_count & 0xFF == 0` poll (compaction.rs, issue #2264).
            if pulled & 0xFF == 0 {
                scan_cancel.check()?;
            }
            pulled += 1;
            match chunk_source.chunk(chunk_index)? {
                Some(decompressed) => {
                    chunk_index += 1;
                    window.extend_from_slice(&decompressed);
                    // Issue #953/#951 precedent (`bti_pull_decompressed_chunk`):
                    // count every chunk this seek materializes, so a work-done test
                    // can prove the window is bounded to the target partition's
                    // chunk span, not the whole file (issue #2207 IMPORTANT-2).
                    super::super::super::work_counters::add_chunk_decompressed();
                }
                None => break, // EOF before `end`: the window is truncated.
            }
        }
        // Did the materialized window actually cover the full requested extent?
        // `false` ⇒ the chunk source was exhausted before reaching `end` (a
        // truncated/corrupt SSTable) — the caller degrades to a scan fallback
        // rather than parse an incomplete partition (issue #2207 fail-safe).
        let reached_end = window_base + window.len() >= end;
        Ok((window, reached_end))
    }
}

#[cfg(test)]
mod tests {
    use super::{exceeds_plausible_partition_span, SALVAGE_MAX_PLAUSIBLE_PARTITION_BYTES};

    /// roborev, issue #4196, round-15 Medium finding 1: the ONE piece of
    /// arithmetic both the uncompressed and compressed arms of
    /// `decode_partition_at_offset_for_salvage` share — unit-tested
    /// directly since the COMPRESSED arm's trigger needs a real multi-chunk
    /// compressed fixture wider than 128 MB, impractical to construct in a
    /// test; the uncompressed arm's own end-to-end test
    /// (`issue_4196_salvage_round15_bounds.rs`) proves the WIRING at one
    /// real call site, and this proves the SHARED arithmetic is correct
    /// for both.
    #[test]
    fn span_at_exactly_the_ceiling_does_not_exceed() {
        let ceiling = SALVAGE_MAX_PLAUSIBLE_PARTITION_BYTES as usize;
        assert!(!exceeds_plausible_partition_span(0, ceiling));
        assert!(!exceeds_plausible_partition_span(1000, 1000 + ceiling));
    }

    #[test]
    fn span_one_byte_past_the_ceiling_exceeds() {
        let ceiling = SALVAGE_MAX_PLAUSIBLE_PARTITION_BYTES as usize;
        assert!(exceeds_plausible_partition_span(0, ceiling + 1));
        assert!(exceeds_plausible_partition_span(1000, 1000 + ceiling + 1));
    }

    #[test]
    fn small_realistic_spans_never_exceed() {
        assert!(!exceeds_plausible_partition_span(0, 0));
        assert!(!exceeds_plausible_partition_span(0, 100));
        assert!(!exceeds_plausible_partition_span(65536, 131072));
    }

    /// Defensive only — both real call sites establish `end >= start`
    /// before reaching this check; an inverted span must not panic.
    #[test]
    fn inverted_span_does_not_panic_and_does_not_exceed() {
        assert!(!exceeds_plausible_partition_span(100, 0));
    }
}
