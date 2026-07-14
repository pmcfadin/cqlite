//! Summary-guided bounded-interval BIG point lookup (issue #2412 §B, Stage 3).
//!
//! When a BIG (`nb`/uncompressed) reader was lazily opened over a usable `Summary.db`
//! (design §A), a `WHERE pk = ?` point read resolves the partition's `Data.db` offset
//! by:
//! 1. binary-searching `Summary.db` for the sample interval covering the key
//!    ([`SummaryReader::find_by_key`](crate::storage::sstable::summary_reader::SummaryReader::find_by_key)), then
//! 2. reading/parsing exactly ONE `Index.db` interval (≤ `min_index_interval` entries)
//!    from that position ([`lookup_key_in_interval`]).
//!
//! This REPLACES the pre-Stage-3 fallback that materialized the whole `Index.db` map
//! (`IndexReader::ensure_materialized`) on the point-read path. The interval is bounded
//! by two authoritative `Summary.db` sample positions, so an interval MISS is an
//! authoritative absence — the whole-file `scan_for_key` oracle is not consulted for
//! the common case (design §B; the FellBack/no-summary path keeps it, #1572).
//!
//! No-heuristics (issue #28): every seek offset and interval bound is an authoritative
//! `Summary.db` sample position / `Index.db` entry framing — nothing is inferred from
//! value bytes.

use super::SSTableReader;
use crate::storage::sstable::summary_reader::interval::lookup_key_in_interval;
use crate::Result;

impl SSTableReader {
    /// Whether a BIG point lookup should resolve through ONE `Summary.db`-bounded
    /// `Index.db` interval (design §B) rather than the full resident partition map.
    ///
    /// `true` iff BOTH hold:
    /// - a usable `Summary.db` (at least one sample entry) bounds the walk, AND
    /// - the lazy `Index.db` map has NOT already been materialized.
    ///
    /// The second clause preserves performance for a reader whose full map is already
    /// resident (e.g. a prior full scan called `ensure_materialized`): the in-memory
    /// O(1) probe beats a fresh bounded disk read, and the scan-path re-probes that
    /// route through `lookup_partition_with_index` after materializing keep using the
    /// resident map. It also means the FellBack (absent-`Summary.db`) reader — which
    /// is eagerly materialized at open — never takes this path.
    pub(super) fn should_use_summary_interval(&self) -> bool {
        let summary_usable = self
            .summary_reader
            .as_ref()
            .map(|s| !s.get_entries().is_empty())
            .unwrap_or(false);
        let index_lazy = self
            .index_reader
            .as_ref()
            .map(|ir| !ir.is_materialized())
            .unwrap_or(false);
        summary_usable && index_lazy
    }

    /// Whether `partition_key`'s covering `Summary.db` interval is END-BOUNDED — i.e.
    /// bounded ABOVE by a NEXT authoritative summary sample (`end_position.is_some()`),
    /// not the last (read-to-EOF) interval.
    ///
    /// This is the #1572-safe gate for treating an interval MISS as an AUTHORITATIVE
    /// absence (design §B, hardened): a tail-truncated `Index.db` (the #1572
    /// degraded-input class — whole trailing entries dropped, or a mid-entry cut at
    /// the end) can only lose the HIGHEST-token partitions, which live in the LAST
    /// interval. An end-bounded interval `[sample_i, sample_{i+1})` is delimited by a
    /// next sample whose position sits BEFORE any tail truncation, so its bytes are
    /// intact and a miss within it is genuinely authoritative. The LAST interval
    /// (`end_position == None`, read to EOF) could hide a dropped tail entry, so a miss
    /// there keeps the whole-file `scan_for_key` fallback (`big_get_with_resolution`),
    /// preserving the #1572 get/scan agreement on degraded inputs.
    ///
    /// A pure in-memory binary search (no disk read) — `big_get_with_resolution`
    /// consults it only on an interval miss to classify the absence.
    pub(super) fn covering_interval_is_end_bounded(&self, partition_key: &[u8]) -> bool {
        self.summary_reader
            .as_ref()
            .and_then(|s| s.find_by_key(partition_key))
            .map(|iv| iv.end_position.is_some())
            .unwrap_or(false)
    }

    /// Resolve `partition_key` to its uncompressed `Data.db` offset via the
    /// `Summary.db`-guided bounded `Index.db` interval (issue #2412 §B).
    ///
    /// Returns:
    /// - `Ok(Some((offset, size)))` — an exact partition entry found within the one
    ///   bounded interval (`size` is `0` — BIG `Index.db` stores no partition size —
    ///   consistent with the resident-map path; callers parse forward from `offset`).
    /// - `Ok(None)` — the key is genuinely absent from the interval between two
    ///   authoritative `Summary.db` samples: an AUTHORITATIVE absence. The BIG point
    ///   path (`big_get_with_resolution`) treats this as a definitive "not found"
    ///   without a whole-file `scan_for_key` (design §B).
    ///
    /// Records exactly one real probe (`INDEX_PROBES`, mirroring the resident-map
    /// path's accounting) plus one bounded interval parse
    /// (`cqlite.sstable.index_interval_parses_total`, emitted inside
    /// [`lookup_key_in_interval`], design §F) — never a full `index_parses_total`.
    ///
    /// Precondition: [`Self::should_use_summary_interval`] holds. When the summary /
    /// index handles are unexpectedly absent (they should not be, given the
    /// precondition), returns `Ok(None)` rather than fabricating a hit.
    pub(super) async fn lookup_partition_via_summary_interval(
        &self,
        partition_key: &[u8],
    ) -> Result<Option<(u64, u32)>> {
        use crate::observability::{self as obs, catalog};

        let format = self.sstable_format_label();
        let (Some(summary), Some(index_reader)) =
            (self.summary_reader.as_ref(), self.index_reader.as_ref())
        else {
            return Ok(None);
        };
        let Some(interval) = summary.find_by_key(partition_key) else {
            // Empty summary — the precondition rules this out, but never fabricate.
            return Ok(None);
        };
        let header = summary.get_header();
        let min_index_interval = header.min_index_interval;
        // Roborev job 1709 (High): the entry cap for the read-to-EOF LAST interval
        // must be downsampling-aware, so `sampling_level` is threaded through —
        // never a guess, the already-parsed header field (design doc §A1/no-heuristics).
        let sampling_level = header.sampling_level;

        // One real bounded interval probe (not a B4 cache hit): mirror the
        // resident-map path's `INDEX_PROBES` accounting so a repeated read served
        // from the B4 cache still records zero probes. The distinct
        // `index_interval_parses_total` is emitted inside `lookup_key_in_interval`.
        crate::storage::sstable::read_work_counters::record_index_probe();

        // The reader's CURRENT (possibly #2383-rebound) Index.db path (issue #2356
        // roborev): a lazy warm reader rebound across a snapshot teardown must open the
        // live hardlink here, not the dead open-time path (#2352 class).
        let index_db_path = index_reader.index_path();
        let lookup = lookup_key_in_interval(
            &index_db_path,
            interval,
            partition_key,
            min_index_interval,
            sampling_level,
        )
        .await?;

        match lookup.entry {
            Some(entry) => {
                obs::add_counter(
                    catalog::READ_PARTITION_LOOKUP,
                    1,
                    &[
                        (catalog::attr::RESULT, "hit".into()),
                        (catalog::attr::LOOKUP_ROUTE, "index".into()),
                        (catalog::attr::SSTABLE_FORMAT, format.into()),
                    ],
                );
                // B4: cache this present-key resolution so a repeat point read of the
                // same key skips both the summary search and the interval read
                // (issue #1570). BIG `Index.db` records no size (`data_size == 0`).
                self.key_offset_cache.insert(
                    partition_key,
                    crate::storage::cache::PartitionLoc::new(entry.data_offset, entry.data_size),
                );
                Ok(Some((entry.data_offset, entry.data_size)))
            }
            None => {
                obs::add_counter(
                    catalog::READ_PARTITION_LOOKUP,
                    1,
                    &[
                        (catalog::attr::RESULT, "miss".into()),
                        (catalog::attr::LOOKUP_ROUTE, "index".into()),
                        (catalog::attr::SSTABLE_FORMAT, format.into()),
                    ],
                );
                // Authoritative absence between two summary samples — the caller does
                // NOT fall back to a whole-file scan_for_key (design §B).
                Ok(None)
            }
        }
    }
}
