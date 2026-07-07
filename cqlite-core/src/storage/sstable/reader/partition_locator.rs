//! One format-tagged partition-location façade (issue #1599 / G3).
//!
//! Every point read resolves a partition's uncompressed `Data.db` offset through a
//! SINGLE entry point, [`SSTableReader::locate`], rather than each read path
//! re-implementing the "range-check → presence → offset" sequence. The façade is
//! deliberately THIN: it composes the already-verified primitives
//! ([`partition_key_out_of_range`](SSTableReader::partition_key_out_of_range),
//! [`lookup_partition_with_index`](SSTableReader::lookup_partition_with_index),
//! [`lookup_partition_via_bti_trie`](SSTableReader::lookup_partition_via_bti_trie))
//! so the B4 key→offset cache, the C5 range short-circuit, and the per-format
//! presence ordering keep exactly one implementation each. No behavior changes: the
//! offsets, negatives, and error classification are byte-identical to the legacy
//! per-path calls the migration replaces.
//!
//! ## Two ordering carve-outs (behavior-preserving; see the amended G3 spec)
//!
//! 1. **C5 stays a PRE-DISPATCH guard in `get_with_resolution`** (ahead of the BIG
//!    bloom pre-check), preserving today's `C5 → bloom → index` order for BIG.
//!    `partition_key_out_of_range` is ONE implementation reached both from that
//!    guard AND as `locate`'s step 1. An out-of-range POINT read returns at the
//!    guard, so `locate` is never reached for it and C5 is never double-recorded.
//!    A DIRECT `locate` call (e.g. the parity test, or a future locate-first caller)
//!    still records the short-circuit exactly once as step 1.
//! 2. **The BIG candidate-prune stays BLOOM-based**, NOT routed through
//!    [`locate_encoded`]: a BIG `Index.db` miss is not a definitive absent (#1572
//!    truncated-index invariant), so pruning on an index miss would drop a candidate
//!    that actually holds the partition. Only the BTI prune is congruent with the
//!    façade's trie resolution and uses [`locate_encoded`].

use super::SSTableReader;
use crate::Result;

impl SSTableReader {
    /// Resolve `partition_key` to its uncompressed `Data.db` offset (and size, where
    /// the format records one) through the one format-tagged façade.
    ///
    /// Returns:
    /// - `Ok(Some((offset, size)))` — the partition's uncompressed data-section
    ///   offset. `size` is the Index.db-recorded partition size for BIG, and `0` for
    ///   BTI (the trie records no size). Callers keep treating `size == 0` as "parse
    ///   forward from the offset, do not range-read".
    /// - `Ok(None)` — absent: out of the authoritative `[first_key, last_key]` bound
    ///   (C5), a BTI trie miss (definitive absence), or a BIG `Index.db` miss (which
    ///   the BIG point path treats as inconclusive and re-checks via a scan; see the
    ///   carve-out above — an index miss is NOT a definitive absent).
    /// - `Err(_)` — the same typed `Error::Corruption` the underlying legacy path
    ///   raises (a corrupt trie, a `RowsOffset` with no `Rows.db`, etc.).
    ///
    /// Resolution order:
    /// 1. **C5 range short-circuit** (step 1): if `partition_key` provably sorts
    ///    outside this SSTable's authoritative Summary bound, record one
    ///    `RANGE_SHORT_CIRCUITS` and return absence before any presence work. A no-op
    ///    when no bound exists (BTI / no Summary).
    /// 2. **Format dispatch**: BTI (`da`) walks the `Partitions.db` trie; BIG
    ///    (`nb`/uncompressed) probes the raw-key `Index.db` map. Both reuse the B4
    ///    key-offset cache and their existing presence-counter emissions.
    pub async fn locate(&self, partition_key: &[u8]) -> Result<Option<(u64, u32)>> {
        // Step 1: C5 authoritative range short-circuit (single implementation shared
        // with the `get_with_resolution` pre-dispatch guard). No-op for BTI / no
        // Summary. See the module carve-out: a point read never reaches here for an
        // out-of-range key (the guard returns first), so this only records C5 for a
        // direct `locate` caller.
        if self.partition_key_out_of_range(partition_key) {
            crate::storage::sstable::read_work_counters::record_range_short_circuit();
            return Ok(None);
        }

        // Step 2: per-format presence + offset resolution.
        if self.is_bti() {
            // BTI: the Partitions.db trie is the authoritative presence oracle and
            // records no partition size, so the façade reports size == 0.
            return Ok(self
                .lookup_partition_via_bti_trie(partition_key)?
                .map(|offset| (offset, 0)));
        }
        // BIG: the raw-key Index.db map resolves (offset, size).
        self.lookup_partition_with_index(partition_key).await
    }

    /// [`locate`](Self::locate) with a PRE-ENCODED BTI byte-comparable key, so a
    /// multi-generation candidate prune hashes+encodes the key ONCE (issue #1575 /
    /// C4) instead of once per candidate.
    ///
    /// **BTI-only by contract.** `encoded` MUST equal
    /// `encode_partition_key_for_bti_trie(partition_key)`. The candidate-prune caller
    /// (`SSTableManager::prune_candidates`) invokes this ONLY for BTI readers and
    /// keeps BIG candidates on the bloom-based `might_contain_partition` — a BIG
    /// `Index.db` miss is not a definitive absent (#1572), so a BIG reader must never
    /// be pruned on this trie-resolution path. For a non-BTI reader this consults no
    /// trie and returns `Ok(None)` (the `encoded` argument is unused); the caller's
    /// BTI gate makes that unreachable.
    ///
    /// Semantics otherwise match [`locate`](Self::locate): the C5 step-1 guard runs
    /// (a no-op for BTI, which has no Summary bound), then the pre-encoded trie
    /// resolve. BTI records no size, so a hit is `Some((offset, 0))`.
    ///
    /// BTI-only: crate-internal (`pub(crate)`) and reached solely from the BTI branch
    /// of `SSTableManager::prune_candidates`. Never call it for a BIG reader — a BIG
    /// prune must stay on the bloom filter, and this path would report a misleading
    /// `Ok(None)` "absent" for a BIG `Index.db` miss (#1572).
    pub(crate) fn locate_encoded(
        &self,
        partition_key: &[u8],
        encoded: &[u8; 9],
    ) -> Result<Option<(u64, u32)>> {
        // BTI-only invariant: the sole caller (BTI branch of `prune_candidates`) gates
        // on a BTI reader; a BIG reader here would yield a false-absent (see doc above).
        debug_assert!(
            self.bti_partitions_db.is_some(),
            "locate_encoded is BTI-only; BIG prune must use the bloom filter"
        );
        // Step 1: C5 (single implementation). A no-op for BTI (no Summary bound), so
        // this never short-circuits a BTI prune; kept for congruence with `locate`.
        if self.partition_key_out_of_range(partition_key) {
            crate::storage::sstable::read_work_counters::record_range_short_circuit();
            return Ok(None);
        }
        // Step 2: pre-encoded BTI trie resolve (C4 hoist). Non-BTI readers return
        // `Ok(None)` here, but the prune caller only ever reaches this on BTI.
        Ok(self
            .lookup_partition_via_bti_trie_encoded(partition_key, encoded)?
            .map(|offset| (offset, 0)))
    }
}
