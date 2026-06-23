//! Process-global read-work counters for partition-targeted lookups
//! (Issue #958, Epic #951).
//!
//! # Why this exists
//!
//! #949 added a partition-targeted lookup ([`SSTableManager::scan_partition`])
//! that prunes the SSTable set via the bloom filter / BTI trie before parsing,
//! so a `WHERE pk = ?` over a table backed by *N* SSTables touches only the
//! handful of candidates that can hold the key — not all *N*. Correct result
//! rows do not prove that pruning happened: a regression could quietly revert to
//! "open and scan every SSTable, then filter in memory" and still return the
//! right answer.
//!
//! These counters make the *work* observable so a CI test can fail the moment a
//! single-partition read starts scaling with the total SSTable count. They
//! mirror the [`scan_for_key_call_count`](crate::storage::sstable::SSTableReader::scan_for_key_call_count)
//! probe (issue #831) and the access-path probe (issue #960): a process-global
//! atomic with [`reset`]/getter accessors, observable from an integration test
//! without parsing logs and from the streaming path's spawned task.
//!
//! # What each counter counts
//!
//! - [`sstables_scanned`] — incremented **once per candidate SSTable reader that
//!   `scan_partition` actually parses** for a partition-targeted lookup. After
//!   bloom/BTI pruning drops the SSTables that cannot hold the key, every
//!   surviving candidate whose `Data.db` is parsed (whether through the
//!   cross-generation k-way merge or the per-reader concat fallback) bumps this
//!   by one. It is the O(candidates) signal: for a key living in one SSTable it
//!   must stay near 1 (plus any bloom false-positives), never grow to N. It is
//!   **not** incremented on the full-scan path — only the targeted lookup is
//!   instrumented, which is exactly the path #949/#958 protect.
//!
//! - [`partitions_parsed`] — incremented **once per partition row a
//!   partition-targeted lookup returns** (after retaining only the target key /
//!   the merge emits the partition). For a single-partition point lookup this is
//!   0 (absent key) or the number of rows that partition holds; it never scales
//!   with the table's total partition count, which a full scan would.
//!
//! # Cost
//!
//! Each increment is a single `Relaxed` atomic add on the cold per-lookup
//! boundary (once per candidate / once per returned partition), never inside an
//! inner byte-decoding loop, so the hot path is unaffected. The counters are not
//! gated behind `cfg(test)` because integration tests in `tests/` compile
//! against the library crate without its `test` cfg (same rationale as
//! `SCAN_FOR_KEY_CALLS`).

use std::sync::atomic::{AtomicU64, Ordering};

/// Count of candidate SSTables actually parsed by a partition-targeted lookup
/// since the last [`reset`]. See module docs for the exact increment site.
static SSTABLES_SCANNED: AtomicU64 = AtomicU64::new(0);

/// Count of partitions a partition-targeted lookup has returned since the last
/// [`reset`]. See module docs for the exact increment site.
static PARTITIONS_PARSED: AtomicU64 = AtomicU64::new(0);

/// Record that `count` candidate SSTables were parsed by a partition-targeted
/// lookup. Called once per `scan_partition` invocation with the number of
/// surviving (post-prune) candidates whose `Data.db` is parsed.
///
/// Only the default (`not(tombstones)`) build has the bloom/BTI-pruning
/// `scan_partition`; the `tombstones` build serves a single-partition read by a
/// full scan + filter and has no candidate set to count, so the mutators are
/// compiled only for the build whose pruning they instrument. The getters and
/// [`reset`] remain available in every build for the test API.
#[cfg(not(feature = "tombstones"))]
pub(crate) fn add_sstables_scanned(count: u64) {
    SSTABLES_SCANNED.fetch_add(count, Ordering::Relaxed);
}

/// Record that `count` partitions were returned by a partition-targeted lookup.
#[cfg(not(feature = "tombstones"))]
pub(crate) fn add_partitions_parsed(count: u64) {
    PARTITIONS_PARSED.fetch_add(count, Ordering::Relaxed);
}

/// Number of candidate SSTables parsed by partition-targeted lookups since the
/// last [`reset`].
///
/// Tests assert this stays O(candidates) — near 1 for a key in a single SSTable
/// (plus a small allowance for bloom false-positives) — so a regression that
/// reopens every SSTable for a single-partition read fails CI.
pub fn sstables_scanned() -> u64 {
    SSTABLES_SCANNED.load(Ordering::Relaxed)
}

/// Number of partitions returned by partition-targeted lookups since the last
/// [`reset`].
pub fn partitions_parsed() -> u64 {
    PARTITIONS_PARSED.load(Ordering::Relaxed)
}

/// Clear both counters. Tests call this before a query so a stale value from an
/// earlier query cannot satisfy a later assertion. Because the probe is
/// process-global, a test that asserts on it must run without a concurrent query
/// on another thread (the integration tests serialize their own setup).
pub fn reset() {
    SSTABLES_SCANNED.store(0, Ordering::Relaxed);
    PARTITIONS_PARSED.store(0, Ordering::Relaxed);
}

#[cfg(all(test, not(feature = "tombstones")))]
mod tests {
    use super::*;

    #[test]
    fn counters_round_trip() {
        reset();
        assert_eq!(sstables_scanned(), 0);
        assert_eq!(partitions_parsed(), 0);
        add_sstables_scanned(2);
        add_partitions_parsed(5);
        assert_eq!(sstables_scanned(), 2);
        assert_eq!(partitions_parsed(), 5);
        reset();
        assert_eq!(sstables_scanned(), 0);
        assert_eq!(partitions_parsed(), 0);
    }
}
