//! Tests for the process-global key→partition-offset cache, split out of
//! `global_key_offset.rs` to keep that file inside the campsite-rule source target
//! (#1116).

use super::*;

fn ident(dev: u64, ino: u64, size: u64, gen: u64) -> GenerationIdentity {
    GenerationIdentity {
        device: dev,
        inode: ino,
        size,
        generation: gen,
        mtime_ns: 0,
    }
}

/// A budget that holds exactly `n` entries whose keys are `key_len` bytes each,
/// on a single shard (deterministic eviction ordering).
fn budget_for(n: usize, key_len: usize) -> usize {
    entry_cost(key_len) * n
}

/// Spec: eviction order (LRU) — single shard sized for exactly 2 entries.
#[test]
fn eviction_order_lru_single_shard() {
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(budget_for(2, 5), 1);
    let g = ident(1, 1, 100, 1);
    cache.insert(g, b"key-A", PartitionLoc::new(10, 100));
    cache.insert(g, b"key-B", PartitionLoc::new(20, 200));
    assert_eq!(cache.get(g, b"key-A"), Some(PartitionLoc::new(10, 100)));
    cache.insert(g, b"key-C", PartitionLoc::new(30, 300));

    assert_eq!(cache.get(g, b"key-A"), Some(PartitionLoc::new(10, 100)));
    assert_eq!(cache.get(g, b"key-C"), Some(PartitionLoc::new(30, 300)));
    assert_eq!(cache.get(g, b"key-B"), None, "LRU entry must be evicted");
    assert!(cache.resident_bytes() <= cache.budget_bytes());
}

/// Byte-accounting regression (roborev round 3): reinserting the SAME
/// `(identity, key)` pair with a DIFFERENT value must NOT grow resident bytes.
/// The `insert` `replaced` branch subtracts the old cost before re-adding the
/// new (identical) cost; a dropped or mis-signed subtraction would double-count
/// bytes on every hot-key reinsert and silently breach the byte budget invariant
/// #2059 exists to enforce. Mirrors the retired per-reader cache's
/// `reinsert_same_key_does_not_grow_resident_bytes`. Single shard so the pair is
/// deterministically co-resident.
#[test]
fn reinsert_same_key_does_not_grow_resident_bytes() {
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(DEFAULT_GLOBAL_KEY_CACHE_BYTES, 1);
    let g = ident(1, 1, 100, 1);
    let key = b"hot-partition-key";

    cache.insert(g, key, PartitionLoc::new(10, 100));
    let bytes_after_first = cache.resident_bytes();
    assert_eq!(cache.len(), 1, "one entry after the first insert");

    // Reinsert the SAME (identity, key) with a DIFFERENT value.
    cache.insert(g, key, PartitionLoc::new(999, 42));

    assert_eq!(
        cache.len(),
        1,
        "reinsert updates in place — no duplicate entry"
    );
    assert_eq!(
        cache.resident_bytes(),
        bytes_after_first,
        "reinserting the same key must not double-count resident bytes"
    );
    // Latest write wins.
    assert_eq!(cache.get(g, key), Some(PartitionLoc::new(999, 42)));
}

/// Spec Requirement 1 scenario: aggregate footprint stays bounded as the
/// number of distinct generations/readers grows far past what the budget holds.
#[test]
fn aggregate_bounded_across_many_generations() {
    // Budget for exactly 4 six-byte-keyed entries, one shard.
    let key_len = 6;
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(budget_for(4, key_len), 1);
    // 200 distinct generations, each inserting one entry — FAR more than 4.
    for gen in 0..200u64 {
        let g = ident(1, gen, 100, gen);
        let key = format!("key-{:02}", gen % 100);
        assert_eq!(key.len(), key_len);
        cache.insert(g, key.as_bytes(), PartitionLoc::new(gen, gen as u32));
        assert!(
            cache.resident_bytes() <= cache.budget_bytes(),
            "resident {} exceeded budget {} after generation {}",
            cache.resident_bytes(),
            cache.budget_bytes(),
            gen
        );
    }
    // Footprint bounded by the byte cap, NOT the generation count.
    assert!(cache.len() <= 4);
}

/// Spec Requirement 1 scenario 2: byte budget is key-size aware, not count.
#[test]
fn large_keys_evict_sooner_than_small_keys() {
    let budget = budget_for(4, 4);
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(budget, 1);
    let g = ident(1, 1, 100, 1);
    let big = vec![b'x'; budget / 2];
    let mut big1 = big.clone();
    big1[0] = b'a';
    let mut big2 = big.clone();
    big2[0] = b'b';
    cache.insert(g, &big1, PartitionLoc::new(1, 1));
    cache.insert(g, &big2, PartitionLoc::new(2, 2));
    assert!(cache.resident_bytes() <= cache.budget_bytes());
    assert_eq!(cache.get(g, &big1), None, "LRU large key must be evicted");
    assert_eq!(cache.get(g, &big2), Some(PartitionLoc::new(2, 2)));
}

/// Spec Requirement 2 scenario: the same partition key in two generations does
/// not alias; a third never-inserted generation misses.
#[test]
fn same_key_two_generations_no_alias() {
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(DEFAULT_GLOBAL_KEY_CACHE_BYTES, 4);
    let g1 = ident(1, 10, 100, 1);
    let g2 = ident(1, 20, 200, 2);
    let g3 = ident(1, 30, 300, 3);
    let key = b"shared-partition-key";
    cache.insert(g1, key, PartitionLoc::new(111, 11));
    cache.insert(g2, key, PartitionLoc::new(222, 22));

    assert_eq!(cache.get(g1, key), Some(PartitionLoc::new(111, 11)));
    assert_eq!(cache.get(g2, key), Some(PartitionLoc::new(222, 22)));
    assert_eq!(cache.get(g3, key), None, "never-inserted generation misses");
}

/// Spec Requirement 4 scenario: a mismatched generation identity is a MISS,
/// not a stale hit (fail-closed).
#[test]
fn mismatched_identity_is_a_miss() {
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(DEFAULT_GLOBAL_KEY_CACHE_BYTES, 4);
    let g1 = ident(1, 10, 100, 1);
    // Same (device, inode, generation) but a DIFFERENT size → distinct identity
    // (a byte-changed generation on a recycled inode).
    let g2 = ident(1, 10, 999, 1);
    cache.insert(g1, b"k", PartitionLoc::new(5, 5));
    assert_eq!(cache.get(g2, b"k"), None, "size mismatch fails closed");
    assert_eq!(cache.get(g1, b"k"), Some(PartitionLoc::new(5, 5)));
}

/// Spec Requirement 4 scenario: a removed generation's entries are invalidated
/// (distinct invalidations counter), and a rebind (unchanged identity) keeps
/// entries valid.
#[test]
fn invalidate_drops_generation_and_counts_distinctly() {
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(DEFAULT_GLOBAL_KEY_CACHE_BYTES, 4);
    let g1 = ident(1, 10, 100, 1);
    let g2 = ident(1, 20, 200, 2);
    cache.insert(g1, b"a", PartitionLoc::new(1, 1));
    cache.insert(g1, b"b", PartitionLoc::new(2, 2));
    cache.insert(g2, b"c", PartitionLoc::new(3, 3));

    let dropped = cache.invalidate(g1);
    assert_eq!(dropped, 2, "both g1 entries dropped");
    assert_eq!(cache.invalidation_count(), 2);
    assert_eq!(cache.eviction_count(), 0, "invalidation is not an eviction");
    assert_eq!(cache.get(g1, b"a"), None);
    assert_eq!(cache.get(g1, b"b"), None);
    // A surviving generation's entry is untouched (rebind-stability analogue:
    // g2's identity is unchanged, so its entry still serves a hit).
    assert_eq!(cache.get(g2, b"c"), Some(PartitionLoc::new(3, 3)));
}

/// Nested-restructure regression (Finding A): `invalidate` drops ONLY the target
/// identity's inner LRU and never touches an unrelated identity's entries, bytes,
/// or recency — the O(1)-per-shard removal, not a full-LRU scan. Single shard so
/// both identities are guaranteed co-resident in the same shard's `HashMap`.
#[test]
fn invalidate_does_not_touch_other_identities() {
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(DEFAULT_GLOBAL_KEY_CACHE_BYTES, 1);
    let g1 = ident(1, 10, 100, 1);
    let g2 = ident(1, 20, 200, 2);
    cache.insert(g1, b"a1", PartitionLoc::new(1, 1));
    cache.insert(g1, b"a2", PartitionLoc::new(2, 2));
    cache.insert(g2, b"b1", PartitionLoc::new(3, 3));
    cache.insert(g2, b"b2", PartitionLoc::new(4, 4));
    let bytes_before = cache.resident_bytes();
    let g2_bytes = entry_cost(2) * 2; // b1 + b2, 2-byte keys

    let dropped = cache.invalidate(g1);
    assert_eq!(dropped, 2, "only g1's two entries dropped");
    // g2's entries, count, and bytes are all exactly as before — untouched.
    assert_eq!(cache.get(g2, b"b1"), Some(PartitionLoc::new(3, 3)));
    assert_eq!(cache.get(g2, b"b2"), Some(PartitionLoc::new(4, 4)));
    assert_eq!(cache.len(), 2, "g2's two entries remain");
    assert_eq!(
        cache.resident_bytes(),
        g2_bytes,
        "only g1's bytes reclaimed"
    );
    assert!(bytes_before > g2_bytes);
    assert_eq!(cache.eviction_count(), 0, "invalidation is not eviction");
    assert_eq!(cache.get(g1, b"a1"), None);
    assert_eq!(cache.get(g1, b"a2"), None);
}

/// Nested-restructure regression (Finding B): a `get` probes the inner LRU with a
/// borrowed `&[u8]` (via `Box<[u8]>: Borrow<[u8]>`), so the hot hit path needs no
/// owned `Box<[u8]>`. Proven behaviorally by matching on a borrowed SUBSLICE of a
/// larger buffer — the caller never materializes an owned key. (Rust can't assert
/// zero-allocation without a custom allocator harness; the API taking `&[u8]` plus
/// the `Shard::get` implementation probing by borrow is the structural guarantee.)
#[test]
fn get_probes_by_borrowed_slice() {
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(DEFAULT_GLOBAL_KEY_CACHE_BYTES, 4);
    let g = ident(1, 1, 100, 1);
    cache.insert(g, b"KEY", PartitionLoc::new(42, 7));
    let buf = b"prefixKEYsuffix";
    assert_eq!(
        cache.get(g, &buf[6..9]),
        Some(PartitionLoc::new(42, 7)),
        "lookup by a borrowed subslice hits — no owned key needed"
    );
}

/// Single global byte budget across the nested per-identity LRUs: budget eviction
/// picks the globally-least-recently-used entry ACROSS identity boundaries (via the
/// per-shard recency clock), never merely the LRU within one identity — so a hot
/// generation does not starve, and a cold entry in ANY identity goes first.
#[test]
fn eviction_crosses_identity_by_global_recency() {
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(budget_for(2, 5), 1);
    let g1 = ident(1, 10, 100, 1);
    let g2 = ident(1, 20, 200, 2);
    cache.insert(g1, b"key-A", PartitionLoc::new(1, 1));
    cache.insert(g2, b"key-B", PartitionLoc::new(2, 2));
    // Touch g1/A so g2/B becomes the global LRU across BOTH identities.
    assert_eq!(cache.get(g1, b"key-A"), Some(PartitionLoc::new(1, 1)));
    // Over budget → evicts the global LRU, which lives in the OTHER identity.
    cache.insert(g2, b"key-C", PartitionLoc::new(3, 3));

    assert_eq!(
        cache.get(g1, b"key-A"),
        Some(PartitionLoc::new(1, 1)),
        "recently-used entry survives across the identity boundary"
    );
    assert_eq!(
        cache.get(g2, b"key-B"),
        None,
        "global LRU evicted regardless of which identity owns it"
    );
    assert_eq!(cache.get(g2, b"key-C"), Some(PartitionLoc::new(3, 3)));
    assert!(cache.resident_bytes() <= cache.budget_bytes());
}

/// Rebind-stability: an entry inserted under an identity is served after a
/// (simulated) path swap that keeps the SAME identity — no invalidation on a
/// rebind (design §C).
#[test]
fn rebind_keeps_entries_valid() {
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(DEFAULT_GLOBAL_KEY_CACHE_BYTES, 4);
    // The identity is (device, inode, size, generation) — path-independent — so
    // a rebind that swaps only the backing path resolves the SAME identity.
    let g = ident(7, 77, 4096, 12);
    cache.insert(g, b"partition", PartitionLoc::new(64, 512));
    // "After the rebind" the reader still computes the same identity g.
    assert_eq!(cache.get(g, b"partition"), Some(PartitionLoc::new(64, 512)));
}

/// Spec Requirement 5 scenario: a disabled cache is a genuine no-op reporting
/// honest zeros for every counter/occupancy accessor.
#[test]
fn disabled_cache_is_a_genuine_no_op() {
    let cache = GlobalKeyOffsetCache::disabled();
    let g = ident(1, 1, 1, 1);
    assert_eq!(cache.budget_bytes(), 0);
    cache.insert(g, b"k", PartitionLoc::new(1, 2));
    assert_eq!(cache.get(g, b"k"), None);
    assert_eq!(cache.invalidate(g), 0);
    assert_eq!(cache.len(), 0);
    assert!(cache.is_empty());
    assert_eq!(cache.resident_bytes(), 0);
    assert_eq!(cache.hit_count(), 0);
    assert_eq!(cache.miss_count(), 0);
    assert_eq!(cache.eviction_count(), 0);
    assert_eq!(cache.invalidation_count(), 0);
}

/// Spec Requirement 6 scenario: counters reflect real activity, evictions and
/// invalidations counted separately; snapshot mirrors the live numbers.
#[test]
fn counters_reflect_real_activity() {
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(budget_for(2, 5), 1);
    let g = ident(1, 1, 100, 1);
    cache.insert(g, b"key-A", PartitionLoc::new(10, 100));
    cache.insert(g, b"key-B", PartitionLoc::new(20, 200));
    assert_eq!(cache.eviction_count(), 0);
    cache.insert(g, b"key-C", PartitionLoc::new(30, 300)); // evicts LRU
    assert_eq!(cache.eviction_count(), 1);

    assert!(cache.get(g, b"key-C").is_some()); // hit
    assert!(cache.get(g, b"absent").is_none()); // miss
    let inval = cache.invalidate(g);
    assert!(inval >= 1);

    let snap = cache.snapshot();
    assert_eq!(snap.hits, 1);
    assert_eq!(snap.misses, 1);
    assert_eq!(snap.evictions, 1);
    assert_eq!(snap.invalidations, inval);
    assert_eq!(snap.capacity_bytes, cache.budget_bytes());
}

/// Spec Requirement 3 (poison recovery): a panic while holding a shard lock
/// must not wedge the cache.
#[test]
fn poisoned_lock_recovers() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::thread;

    let cache = Arc::new(GlobalKeyOffsetCache::with_budget_and_shards(
        DEFAULT_GLOBAL_KEY_CACHE_BYTES,
        1,
    ));
    let g = ident(1, 1, 100, 1);
    cache.insert(g, b"k", PartitionLoc::new(7, 8));

    let poison_cache = Arc::clone(&cache);
    let _ = thread::spawn(move || {
        let _guard = GlobalKeyOffsetCache::lock(&poison_cache.shards[0]);
        panic!("intentional poison");
    })
    .join();

    let res = catch_unwind(AssertUnwindSafe(|| {
        let hit = cache.get(g, b"k");
        cache.insert(g, b"k2", PartitionLoc::new(9, 10));
        hit
    }));
    assert_eq!(res.ok(), Some(Some(PartitionLoc::new(7, 8))));
}

/// Spec Requirement 3 scenario: concurrent readers under eviction pressure stay
/// correct with no torn/aliased locations and no panic; the hit path locks one
/// shard (proven implicitly — the test never deadlocks under a small budget).
#[test]
fn concurrency_soundness() {
    use std::thread;

    // Small budget vs working set so eviction runs constantly; many shards.
    let cache = Arc::new(GlobalKeyOffsetCache::with_budget_bytes(64 * 1024));
    let n_keys = 512u64;
    let n_gens = 4u64;

    let mut handles = Vec::new();
    for t in 0..8u64 {
        let cache = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for round in 0..3000u64 {
                let gi = (t.wrapping_add(round)) % n_gens;
                let g = ident(1, gi, 100 + gi, gi);
                let idx = (t.wrapping_mul(31).wrapping_add(round)) % n_keys;
                let key = format!("k-{gi}-{idx}");
                // The offset encodes the (gen, key) so any returned value must
                // match exactly — never another key's location, never torn.
                let expect = idx.wrapping_mul(1000).wrapping_add(gi);
                match cache.get(g, key.as_bytes()) {
                    Some(loc) => assert_eq!(loc.data_offset, expect),
                    None => cache.insert(g, key.as_bytes(), PartitionLoc::new(expect, 0)),
                }
            }
        }));
    }
    for h in handles {
        h.join().expect("worker must not panic");
    }
    assert!(cache.resident_bytes() <= cache.budget_bytes());
}

#[test]
fn shard_count_rounds_to_power_of_two() {
    let cache = GlobalKeyOffsetCache::with_budget_and_shards(DEFAULT_GLOBAL_KEY_CACHE_BYTES, 100);
    assert_eq!(cache.shards.len(), 128);
    assert_eq!(cache.mask, 127);
}

#[test]
fn global_singleton_is_shared() {
    let a = GlobalKeyOffsetCache::global();
    let b = GlobalKeyOffsetCache::global();
    assert!(
        Arc::ptr_eq(&a, &b),
        "global() returns the one shared instance"
    );
    assert!(a.budget_bytes() > 0);
}
