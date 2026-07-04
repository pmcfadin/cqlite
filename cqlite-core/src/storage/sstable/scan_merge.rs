//! Cross-SSTable scan ordering for [`SSTableManager`](super::SSTableManager)
//! (issue #1580, Epic D4).
//!
//! # The ordering contract (the oracle)
//!
//! Cassandra's default `Murmur3Partitioner` orders partitions by their **token**
//! — the Murmur3 hash of the partition-key bytes — with equal-token ties broken by
//! raw key bytes. A full scan / `sstabledump` therefore emits partitions in
//! ascending token order, which in general is **not** the lexicographic order of
//! the raw partition-key bytes (e.g. UUID `0x22…` sorts *before* `0x11…` by token).
//!
//! Each per-reader scan already returns rows in this exact order (the reader's
//! `sort_by_token_order`). The manager previously concatenated every reader's
//! output and RE-SORTED the whole concatenation by raw `RowKey` bytes
//! (`sort_by(|a, b| a.0.cmp(&b.0))`), which both produced the wrong global order
//! and cost a full O(n log n) sort on the async worker.
//!
//! # What this module provides
//!
//! * [`kway_merge_token_order`] — a k-way merge over the per-reader token-ordered
//!   streams that emits the merged stream in token order in **O(n log k)** key
//!   comparisons (k = number of readers), with early-exit once a pushed-down
//!   `limit` is satisfied. This replaces the concat + O(n log n) re-sort.
//! * [`sort_by_token_order`] — a stable token-order sort for the few paths that
//!   have already materialized a merged `Vec` (the multi-generation
//!   `KWayMerger` paths); it corrects the old raw-byte sort while remaining a
//!   no-op when the input is already token-ordered.
//!
//! # Instrumentation
//!
//! [`MANAGER_SCAN_KEY_COMPARISONS`] counts key comparisons performed by the k-way
//! merge, and [`MANAGER_SCAN_FULL_SORTS`] counts materialized full-vector sorts.
//! Tests use these to assert the scan path is O(n log k) and never falls back to a
//! full O(n log n) concat sort. Both use `Relaxed` atomics — the increment is
//! negligible next to per-row decode work.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use crate::types::RowKey;
use crate::util::cassandra_murmur3::cassandra_murmur3_token;

/// Number of key comparisons performed by [`kway_merge_token_order`] since process
/// start. Exposed for the O(n log k) no-full-sort regression assertion.
pub(crate) static MANAGER_SCAN_KEY_COMPARISONS: AtomicU64 = AtomicU64::new(0);

/// Number of materialized full-vector token-order sorts performed by
/// [`sort_by_token_order`] since process start.
pub(crate) static MANAGER_SCAN_FULL_SORTS: AtomicU64 = AtomicU64::new(0);

/// Heap element pairing an ordering key ([`Candidate`]) with its opaque payload
/// `T`. Ordering delegates entirely to the [`Candidate`] so no `T: Ord` bound is
/// needed — the row value/metadata never participates in the merge order.
struct HeapItem<T> {
    cand: Candidate,
    val: T,
}

impl<T> PartialEq for HeapItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cand.cmp(&other.cand) == Ordering::Equal
    }
}
impl<T> Eq for HeapItem<T> {}
impl<T> PartialOrd for HeapItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for HeapItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cand.cmp(&other.cand)
    }
}

/// The ordering key for one reader stream's current head: its precomputed token
/// (hashed once per row, not per comparison), partition key, and reader index.
/// Ordered ascending `(token, key_bytes, reader_idx)`; `reader_idx` breaks ties so
/// equal-`(token, key)` rows from different readers emit in reader order,
/// reproducing the stable concat order the old raw-byte `sort_by` gave. Every
/// [`Candidate::cmp`] increments [`MANAGER_SCAN_KEY_COMPARISONS`].
struct Candidate {
    token: i64,
    key: RowKey,
    reader_idx: usize,
}

impl Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        MANAGER_SCAN_KEY_COMPARISONS.fetch_add(1, AtomicOrdering::Relaxed);
        self.token
            .cmp(&other.token)
            .then_with(|| self.key.cmp(&other.key))
            .then_with(|| self.reader_idx.cmp(&other.reader_idx))
    }
}

/// K-way merge over per-reader token-ordered streams.
///
/// `per_reader[i]` MUST already be sorted in ascending Cassandra token order (with
/// equal-token ties by raw key bytes and clustering rows of one partition
/// contiguous) — which every `SSTableReader` scan guarantees via
/// `sort_by_token_order`. The output is the globally token-ordered concatenation.
///
/// Cost: **O(n log k)** key comparisons (k = number of non-empty streams), versus
/// the O(n log n) full re-sort it replaces. When `limit` is `Some(n)`, the merge
/// stops after emitting `n` rows (D1 pushed-down-limit early-exit).
pub(crate) fn kway_merge_token_order<T>(
    per_reader: Vec<Vec<(RowKey, T)>>,
    limit: Option<usize>,
) -> Vec<(RowKey, T)> {
    // Fast paths avoid heap setup for the overwhelmingly common cases.
    let non_empty = per_reader.iter().filter(|s| !s.is_empty()).count();
    if non_empty <= 1 {
        // Zero or one contributing stream: it is already in token order. Just
        // take it (the reader emitted it sorted) and apply the limit.
        let mut out: Vec<(RowKey, T)> = per_reader
            .into_iter()
            .find(|s| !s.is_empty())
            .unwrap_or_default();
        if let Some(lim) = limit {
            out.truncate(lim);
        }
        return out;
    }

    let total: usize = per_reader.iter().map(|s| s.len()).sum();
    let cap = limit.map_or(total, |l| l.min(total));

    // Convert each stream into a forward iterator so we can pull heads lazily.
    let mut iters: Vec<std::vec::IntoIter<(RowKey, T)>> =
        per_reader.into_iter().map(|s| s.into_iter()).collect();

    // Min-heap over the current head of each stream. `Reverse` turns the max-heap
    // into a min-heap; the payload rides inside `HeapItem` so no `T: Ord` bound.
    let mut heap: BinaryHeap<std::cmp::Reverse<HeapItem<T>>> =
        BinaryHeap::with_capacity(iters.len());
    for (idx, it) in iters.iter_mut().enumerate() {
        if let Some((key, val)) = it.next() {
            let token = cassandra_murmur3_token(key.as_bytes());
            heap.push(std::cmp::Reverse(HeapItem {
                cand: Candidate {
                    token,
                    key,
                    reader_idx: idx,
                },
                val,
            }));
        }
    }

    let mut out: Vec<(RowKey, T)> = Vec::with_capacity(cap);
    while let Some(std::cmp::Reverse(HeapItem { cand, val })) = heap.pop() {
        let idx = cand.reader_idx;
        out.push((cand.key, val));
        if out.len() == cap {
            break; // pushed-down limit satisfied — stop early
        }
        if let Some((key, val)) = iters[idx].next() {
            let token = cassandra_murmur3_token(key.as_bytes());
            heap.push(std::cmp::Reverse(HeapItem {
                cand: Candidate {
                    token,
                    key,
                    reader_idx: idx,
                },
                val,
            }));
        }
    }
    out
}

/// Stable in-place token-order sort of an already-materialized result vector, then
/// truncate to `limit`.
///
/// `key_of` extracts each element's partition `RowKey` (the first tuple field for
/// both the plain `(RowKey, ScanRow)` and the metadata `(RowKey, ScanRow, Meta)`
/// paths). Elements are ordered by ascending `(token, key_bytes)`; the sort is
/// stable, so equal-`(token, key)` rows (a partition's clustering rows) keep their
/// input order.
///
/// Used by the multi-generation `KWayMerger` read paths, which hand back a single
/// merged `Vec` (already token-ordered by the merger): this replaces their prior
/// raw-byte `sort_by`, so a stray raw-byte ordering can never leak out, while the
/// sort is effectively a no-op when the input is already ordered. Computes each
/// key's token once to avoid recomputation inside the comparator.
pub(crate) fn sort_by_token_order<E>(
    results: &mut Vec<E>,
    limit: Option<usize>,
    key_of: impl Fn(&E) -> &RowKey,
) {
    MANAGER_SCAN_FULL_SORTS.fetch_add(1, AtomicOrdering::Relaxed);
    let mut tagged: Vec<(i64, E)> = results
        .drain(..)
        .map(|e| {
            let t = cassandra_murmur3_token(key_of(&e).as_bytes());
            (t, e)
        })
        .collect();
    tagged.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| key_of(&a.1).cmp(key_of(&b.1))));
    results.extend(tagged.into_iter().map(|(_, e)| e));
    if let Some(lim) = limit {
        results.truncate(lim);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // UUID keys whose token order differs from raw-byte order (pinned in
    // `util::cassandra_murmur3` tests): token(0x22) < token(0x11) < token(0x33).
    fn uuid_key(b: u8) -> RowKey {
        RowKey::new(vec![b; 16])
    }

    fn keys_in_order<T>(rows: &[(RowKey, T)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (k, _) in rows {
            let first = k.as_bytes()[0];
            if out.last() != Some(&first) {
                out.push(first);
            }
        }
        out
    }

    #[test]
    fn merge_emits_token_order_across_readers() {
        // Two readers, each internally token-ordered. Reader A: 0x22, 0x33.
        // Reader B: 0x11. Global token order must be 0x22, 0x11, 0x33.
        let reader_a = vec![(uuid_key(0x22), 1u32), (uuid_key(0x33), 3u32)];
        let reader_b = vec![(uuid_key(0x11), 2u32)];
        let merged = kway_merge_token_order(vec![reader_a, reader_b], None);
        assert_eq!(keys_in_order(&merged), vec![0x22, 0x11, 0x33]);
    }

    #[test]
    fn merge_preserves_clustering_rows_and_reader_tiebreak() {
        // Same partition (0x11) present in two readers with multiple clustering
        // rows each. They must stay grouped, reader 0's rows before reader 1's.
        let reader0 = vec![(uuid_key(0x11), 0u32), (uuid_key(0x11), 1u32)];
        let reader1 = vec![(uuid_key(0x11), 2u32), (uuid_key(0x33), 9u32)];
        let merged = kway_merge_token_order(vec![reader0, reader1], None);
        let payloads: Vec<u32> = merged.iter().map(|(_, v)| *v).collect();
        // 0x11 rows first (0,1 from reader0 then 2 from reader1), then 0x33.
        assert_eq!(payloads, vec![0, 1, 2, 9]);
    }

    #[test]
    fn merge_early_exits_on_limit() {
        let reader_a = vec![(uuid_key(0x22), 1u32), (uuid_key(0x33), 3u32)];
        let reader_b = vec![(uuid_key(0x11), 2u32)];
        let merged = kway_merge_token_order(vec![reader_a, reader_b], Some(2));
        assert_eq!(merged.len(), 2);
        assert_eq!(keys_in_order(&merged), vec![0x22, 0x11]);
    }

    #[test]
    fn single_reader_is_passthrough_no_comparisons() {
        let before = MANAGER_SCAN_KEY_COMPARISONS.load(AtomicOrdering::Relaxed);
        let only = vec![(uuid_key(0x33), 1u32), (uuid_key(0x11), 2u32)];
        // Single stream: returned as-is (reader already token-ordered), no merge.
        let merged = kway_merge_token_order(vec![only], None);
        assert_eq!(merged.len(), 2);
        let after = MANAGER_SCAN_KEY_COMPARISONS.load(AtomicOrdering::Relaxed);
        assert_eq!(
            after, before,
            "single-stream fast path must not compare keys"
        );
    }

    /// The no-full-sort assertion (issue #1580): the k-way merge over k readers
    /// must use O(n log k) key comparisons, NOT the O(n log n) of a full re-sort.
    /// With k = 4 and n = 4000, n·log2(k) = 8000 while n·log2(n) ≈ 47726 — the
    /// bound below (≈ 4·n·ceil(log2 k)) sits well under n·log2(n).
    #[test]
    fn merge_comparison_count_is_n_log_k_not_n_log_n() {
        let k = 4usize;
        let per_stream = 1000usize;
        let n = k * per_stream;

        // k readers, each with `per_stream` distinct token-ordered partitions.
        // Distinct 4-byte big-endian int keys, then per-reader token-sorted so the
        // merge input honours its precondition.
        let mut per_reader: Vec<Vec<(RowKey, u32)>> = Vec::with_capacity(k);
        let mut next: i32 = 0;
        for _ in 0..k {
            let mut stream: Vec<(RowKey, u32)> = Vec::with_capacity(per_stream);
            for _ in 0..per_stream {
                let key = RowKey::new(next.to_be_bytes().to_vec());
                stream.push((key, next as u32));
                next += 1;
            }
            stream.sort_by_key(|(k, _)| cassandra_murmur3_token(k.as_bytes()));
            per_reader.push(stream);
        }

        MANAGER_SCAN_KEY_COMPARISONS.store(0, AtomicOrdering::Relaxed);
        let merged = kway_merge_token_order(per_reader, None);
        let cmps = MANAGER_SCAN_KEY_COMPARISONS.load(AtomicOrdering::Relaxed);

        assert_eq!(merged.len(), n, "merge must emit every row");

        // Output is globally token-ordered.
        for w in merged.windows(2) {
            let ta = cassandra_murmur3_token(w[0].0.as_bytes());
            let tb = cassandra_murmur3_token(w[1].0.as_bytes());
            assert!(ta <= tb, "merge output must be ascending token order");
        }

        let log2_k = (usize::BITS - (k - 1).leading_zeros()) as u64; // ceil(log2 k)
        let n_log_k_bound = 4 * (n as u64) * (log2_k + 1);
        let n_log_n = (n as f64 * (n as f64).log2()) as u64;
        assert!(
            cmps <= n_log_k_bound,
            "k-way merge did {cmps} comparisons; expected O(n log k) ≤ {n_log_k_bound} \
             (n={n}, k={k}); a full O(n log n) re-sort would be ≈ {n_log_n}"
        );
        assert!(
            cmps < n_log_n,
            "comparison count {cmps} must be strictly below the O(n log n)≈{n_log_n} \
             full-sort cost the k-way merge replaces"
        );
    }
}
