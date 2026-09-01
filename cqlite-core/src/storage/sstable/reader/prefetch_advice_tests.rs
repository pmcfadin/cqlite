//! The mmap read-ahead advice policy: `PrefetchMode` -> `madvise` advice.
//!
//! In-crate (not `tests/`) because [`mmap_advice_for`](super::backend_resolve::mmap_advice_for)
//! is `pub(super)` — the mapping is an internal policy decision no external crate
//! can reach, and pinning it at the public surface would require observing a
//! kernel hint that has no observable return value.
//!
//! Unix-only (`madvise` is), so the whole module is gated `#[cfg(all(test, unix))]`
//! at its declaration in `mod.rs`. These are pure, deterministic and
//! host-independent: they assert the POLICY MAPPING, not timing.

use super::backend_resolve::mmap_advice_for;
use crate::config::{PrefetchMode, StorageConfig};

/// Issue #1143 (P0) mechanism guard, RETARGETED by issue #2824:
/// `PrefetchMode::Auto` MUST NOT map to `MADV_SEQUENTIAL` on the mmap backend,
/// and (since #2824) DOES map to `MADV_WILLNEED`.
///
/// The invariant #1143 needs is "`Auto` never yields `Sequential`", not "`Auto`
/// yields nothing". `MADV_SEQUENTIAL`'s harm is **drop-behind**: pages are
/// evicted as a scan passes them, so under concurrent write load the evicted
/// pages are gone when an overlapping scan re-reads them, causing synchronous
/// major page faults on the tokio worker and a ~2x read-side p99 tail
/// regression. `MADV_WILLNEED` queues **asynchronous** read-ahead and has **no**
/// drop-behind semantics, so it does not carry that mechanism — which is why
/// #2824 could turn it on for the default path without reintroducing #1143. The
/// old assert (`mmap_advice_for(Auto) == None`) stated the implementation of the
/// day; this one states the invariant, and is never to be deleted.
///
/// This is deterministic and host-independent, so it reliably fails if
/// drop-behind is reintroduced — unlike a wall-clock tail guard, which cannot
/// force page-cache reclaim on the tiny vendored fixtures. The integration guard
/// `tests/issue_1143_mmap_prefetch_tail_guard.rs` is observational only (it
/// asserts nothing on timing), so THIS is the load-bearing #1143 pin. `Off`
/// still issues no advice; explicit `Sequential`/`WillNeed` remain the caller's
/// opt-in to those hints.
#[test]
fn test_mmap_advice_for_auto_is_willneed_never_sequential() {
    // The durable #1143 invariant: Auto must NEVER emit Sequential
    // (drop-behind). Asserted independently of what Auto DOES emit, so a future
    // policy change cannot quietly drop this protection.
    assert_ne!(
        mmap_advice_for(PrefetchMode::Auto),
        Some(memmap2::Advice::Sequential),
        "issue #1143 REGRESSION: Auto prefetch re-emitting MADV_SEQUENTIAL \
         (drop-behind) — read p99 tail will regress ~2x under write load"
    );
    // The #2824 policy: Auto advises asynchronous read-ahead.
    assert_eq!(
        mmap_advice_for(PrefetchMode::Auto),
        Some(memmap2::Advice::WillNeed),
        "issue #2824: default Auto prefetch must advise MADV_WILLNEED on the \
         scan mapping"
    );
    assert_eq!(mmap_advice_for(PrefetchMode::Off), None);
    // Explicit opt-ins are preserved.
    assert_eq!(
        mmap_advice_for(PrefetchMode::Sequential),
        Some(memmap2::Advice::Sequential)
    );
    assert_eq!(
        mmap_advice_for(PrefetchMode::WillNeed),
        Some(memmap2::Advice::WillNeed)
    );
}

/// Issue #2824: the DEFAULT [`StorageConfig`] resolves to `MADV_WILLNEED` advice
/// on the mmap scan mapping.
///
/// The test above pins the `mmap_advice_for` mapping; this one pins that the
/// shipped default actually travels it — i.e. that `StorageConfig::default()`
/// still selects `PrefetchMode::Auto`, so the behavioural change to the default
/// read path is pinned where a reader looking at defaults will find it. A future
/// default flip to `Off` would silently un-ship #2824 without touching
/// `mmap_advice_for` at all.
#[test]
fn test_default_storage_config_advises_willneed() {
    let storage = StorageConfig::default();
    assert_eq!(
        storage.prefetch,
        PrefetchMode::Auto,
        "issue #2824: the default prefetch mode is the one that carries the \
         MADV_WILLNEED policy"
    );
    assert_eq!(
        mmap_advice_for(storage.prefetch),
        Some(memmap2::Advice::WillNeed),
        "issue #2824: the DEFAULT configuration must advise MADV_WILLNEED on the \
         scan mapping"
    );
}
