//! Shared cold-start helper for the #2674/#2714 counter-guard family.
//!
//! Single-purpose companion to `common/mod.rs`, included on its own via
//! `#[path = "common/key_cache_flush.rs"] mod key_cache_flush;` so a guard file
//! pulls in ONLY this helper (not the whole `common` module, whose other utilities
//! would trip `-D warnings` dead_code in a binary that uses just this one).

/// Flush EVERY entry from the process-global B4 key→partition-offset cache
/// (issue #2059) so a counter guard that asserts a COLD "first" read (a real
/// `Index.db` probe / `Partitions.db` trie walk) starts from a genuinely empty
/// cache.
///
/// # Why this exists (issue #2674/#2714 family)
///
/// The B4 cache is a process-global singleton keyed by GENERATION IDENTITY. Every
/// `#[serial]` guard in a binary that opens the SAME on-disk fixture shares one
/// identity, so a SIBLING test that locates/point-reads a key first leaves it warm;
/// `read_work_counters::reset()` zeroes the counters but NOT the cache, so the next
/// test's "first" read is a cache HIT with zero probes and its
/// `index_probes()/trie_walks() >= 1` cold assertion fails — an execution-ORDER coin
/// flip (bare `#[serial]` lock-acquisition order is nondeterministic, biased toward
/// failure under gate CPU contention; #2714). Call this at the top of such a guard,
/// before the measured cold read, to make the cold start deterministic regardless of
/// test order. `#[allow(dead_code)]`: a binary may include this file for a subset of
/// its guards.
///
/// # Invariant — the caller AND every sibling in the binary MUST be `#[serial]`
///
/// This is a PROCESS-GLOBAL flush. In a binary where any counter guard relies on a
/// warm cache (a "second read is a cache HIT / zero probes" assertion), a flush that
/// lands MID-un-serialized-sibling wipes that sibling's warm state and breaks its
/// assertion. So every test in a binary that calls this — and every warm-read guard
/// beside it — MUST carry `#[serial]` (a shared serial group) so the flush can never
/// interleave with another guard's measured window.
#[allow(dead_code)]
pub fn flush_global_key_cache() {
    cqlite_core::storage::cache::GlobalKeyOffsetCache::global().invalidate_all();
}
