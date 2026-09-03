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
