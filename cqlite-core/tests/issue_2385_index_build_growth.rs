//! Issue #2385 — cold-open super-linear catch: `Index::from_entries` must build
//! in ~O(N log N), not O(N²).
//!
//! Root cause (analysis comment on #2385): `SSTableIndex::add_entry` did a
//! `Vec::binary_search` + `Vec::insert` into `sorted_keys` per entry. `Index.db`
//! arrives in TOKEN order but `RowKey` sorts by raw bytes, so inserts land
//! mid-vector → an O(N) memmove each → O(N²) total. The field measured this as a
//! ~257s cold `LIMIT 5` at 1.42M partitions (local 150k→1.8s, 2M→312s: ~13× data
//! → ~173× time ≈ 13²).
//!
//! Two layers (#4232):
//!
//! * LIVE (merge-gating): `from_entries_matches_add_entry_semantics` is a
//!   deterministic equivalence check against the per-entry `add_entry` path
//!   (ordering, dedup, last-write-wins, counts). It cannot flake on load.
//! * OPT-IN (`#[ignore]`, `cargo test -- --ignored`): the GROWTH RATIO. Building 4×
//!   the entries must cost ≤ ~8× the time (quadratic ≈ 16×, linearithmic ≈ 4.3×).
//!   It is a wall-clock ratio over 18–255 ms samples and flaked under load (9.26x,
//!   9.70x, 8.16x), so it is out of the default gate. No deterministic operation
//!   count is reachable without touching library code: `RowKey` derives `Ord`
//!   over `Arc<[u8]>` and `from_entries` is not generic over the comparator.
//!
//! Insertion order is DESCENDING by key bytes — the worst case for the old
//! binary-search-then-insert loop (every insert lands at position 0 → a full
//! memmove), so the quadratic signal is maximal and deterministic.
//!
//! RED on the pre-fix tree (per-entry insert): ratio ≈ 16 (FAILS ≤ 8).
//! GREEN after the sort-once bulk build: ratio ≈ 4–5.

#![cfg(feature = "state_machine")]

use std::time::Instant;

use cqlite_core::{
    storage::sstable::index::{Index, IndexEntry},
    types::TableId,
    RowKey,
};

/// Build `n` index entries whose keys DESCEND in byte order (worst case for a
/// per-entry `binary_search` + `insert` into a byte-sorted vector: every insert
/// is at position 0).
fn descending_entries(table_id: &TableId, n: u64) -> Vec<IndexEntry> {
    let mut entries = Vec::with_capacity(n as usize);
    for i in 0..n {
        // key = (n - i): first entry is the LARGEST key, each subsequent smaller,
        // so every insertion lands before all prior keys.
        let key_val = n - i;
        entries.push(IndexEntry {
            table_id: table_id.clone(),
            key: RowKey::new(key_val.to_be_bytes().to_vec()),
            offset: i * 64,
            size: 0,
            compressed: false,
        });
    }
    entries
}

/// Fastest `Index::from_entries` build over `runs` attempts (the entry vector is
/// cloned OUTSIDE each timed region). Taking the MIN filters transient scheduler
/// stalls on a loaded box: a single preemption can only inflate an individual
/// sample, and the min discards it — so the growth ratio never false-REDs on a
/// one-off hiccup while still capturing the true O(N²) vs O(N log N) signal.
fn time_build_min(entries: &[IndexEntry], runs: usize) -> f64 {
    let mut best = f64::INFINITY;
    for _ in 0..runs {
        let cloned = entries.to_vec();
        let start = Instant::now();
        let index = Index::from_entries(cloned);
        let elapsed = start.elapsed().as_secs_f64();
        // Prevent the optimizer from eliding the build.
        assert!(!index.is_empty(), "build must retain entries");
        best = best.min(elapsed);
    }
    best
}

/// Deterministic live check: `from_entries` (sort-once bulk build) must equal the
/// incremental `add_entry` path, including duplicates (last write wins).
#[test]
fn from_entries_matches_add_entry_semantics() {
    let table_id = TableId::new("growth_ks.growth_tbl");
    let mut entries = descending_entries(&table_id, 2_000);
    // Duplicate keys with different offsets: the later entry must win.
    for i in 0..50u64 {
        entries.push(IndexEntry {
            table_id: table_id.clone(),
            key: RowKey::new((1 + i * 7).to_be_bytes().to_vec()),
            offset: 1_000_000 + i,
            size: 0,
            compressed: false,
        });
    }

    let bulk = Index::from_entries(entries.clone());
    let mut incremental = Index::new();
    for e in entries.iter().cloned() {
        incremental.add_entry(e);
    }

    assert_eq!(bulk.len(), incremental.len(), "total_entries must match");
    let a: Vec<(Vec<u8>, u64)> = bulk
        .get_range(&table_id, None, None)
        .expect("range")
        .into_iter()
        .map(|e| (e.key.0.to_vec(), e.offset))
        .collect();
    let b: Vec<(Vec<u8>, u64)> = incremental
        .get_range(&table_id, None, None)
        .expect("range")
        .into_iter()
        .map(|e| (e.key.0.to_vec(), e.offset))
        .collect();
    assert_eq!(a.len(), 2_000, "duplicates collapse to unique keys");
    assert!(
        a.windows(2).all(|w| w[0].0 < w[1].0),
        "keys must be strictly ascending by bytes"
    );
    assert_eq!(a, b, "bulk build must equal incremental add_entry result");
}

#[test]
#[ignore = "wall-clock growth ratio flakes under load (#4232); run with --ignored on a quiet host"]
fn index_build_is_not_quadratic() {
    let table_id = TableId::new("growth_ks.growth_tbl");

    // 50k and 200k (4×). Big enough that O(N²) dominates fixed overhead, small
    // enough that even the PRE-FIX quadratic build completes in seconds in a debug
    // test binary (at 800k the O(N²) memmove of the byte-sorted key vector runs
    // ~4 minutes — matching the field's 2M→312s — which is impractical to gate on).
    const N1: u64 = 50_000;
    const N2: u64 = 200_000;

    // Build both entry vectors up front (outside the timed regions).
    let v1 = descending_entries(&table_id, N1);
    let v2 = descending_entries(&table_id, N2);

    // Min-of-3 to filter transient scheduler stalls (loaded-box safe).
    let t1 = time_build_min(&v1, 3);
    let t2 = time_build_min(&v2, 3);

    // Guard against a degenerate near-zero t1 that would inflate the ratio into a
    // false RED on an absurdly fast machine: require a floor before dividing.
    let ratio = if t1 > 1e-4 { t2 / t1 } else { 0.0 };

    eprintln!(
        "index_build growth: N1={N1} took {t1:.4}s, N2={N2} (4x) took {t2:.4}s, ratio={ratio:.2} \
         (quadratic ~16, linearithmic ~4.3, bound 8)"
    );

    // perf-gate-allow: #[ignore]d opt-in perf lane (#4232), not in the default gate.
    assert!(
        ratio <= 8.0,
        "Index::from_entries must build in ~O(N log N): 4x the entries took {ratio:.2}x the time \
         (bound 8; a quadratic per-entry insert gives ~16). N1={N1} t1={t1:.4}s, N2={N2} t2={t2:.4}s"
    );
}
