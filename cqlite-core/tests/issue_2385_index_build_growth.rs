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
//! This pins the fix with a GROWTH RATIO, not an absolute wall-clock (loaded-box
//! safe): building 4× the entries must cost ≤ ~8× the time. A quadratic build
//! gives ~16× (=4²); a linearithmic build gives ~4.3× (4 · log(4N)/log(N)). The
//! bound of 8 sits between them with generous headroom for scheduler noise.
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

/// Time `Index::from_entries` for a prebuilt entry vector (build cost only; the
/// vector is constructed outside the timed region).
fn time_build(entries: Vec<IndexEntry>) -> f64 {
    let start = Instant::now();
    let index = Index::from_entries(entries);
    let elapsed = start.elapsed().as_secs_f64();
    // Prevent the optimizer from eliding the build.
    assert!(index.len() > 0, "build must retain entries");
    elapsed
}

#[test]
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

    let t1 = time_build(v1);
    let t2 = time_build(v2);

    // Guard against a degenerate near-zero t1 that would inflate the ratio into a
    // false RED on an absurdly fast machine: require a floor before dividing.
    let ratio = if t1 > 1e-4 { t2 / t1 } else { 0.0 };

    eprintln!(
        "index_build growth: N1={N1} took {t1:.4}s, N2={N2} (4x) took {t2:.4}s, ratio={ratio:.2} \
         (quadratic ~16, linearithmic ~4.3, bound 8)"
    );

    assert!(
        ratio <= 8.0,
        "Index::from_entries must build in ~O(N log N): 4x the entries took {ratio:.2}x the time \
         (bound 8; a quadratic per-entry insert gives ~16). N1={N1} t1={t1:.4}s, N2={N2} t2={t2:.4}s"
    );
}
