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
//! - [`chunks_decompressed`] — incremented **once per compression chunk the
//!   single-candidate seek path actually decompresses** while buffering the
//!   target partition's bytes (Issue #953 / #951). Where `partitions_decoded`
//!   proves we returned only one partition, this proves the seek bounded its
//!   *decompression I/O* to that partition's chunk span rather than reading the
//!   whole `Data.db` to EOF. A head-of-file point lookup must NOT decompress the
//!   tail chunks of a large SSTable: a regression that stitches to EOF (the bug
//!   the bound replaces) bumps this by the file's whole chunk count, which the
//!   `issue_953` bound test catches even though `partitions_decoded` stays 1.
//!   Incremented at the seek's single decompression site
//!   (`SSTableReader::bti_pull_decompressed_chunk`), so every chunk the seek
//!   materializes — BIG (`nb`, bounded by the `Index.db` size) or BTI (`da`,
//!   bounded by the next-partition boundary) — is counted exactly once. The
//!   whole-section `stitch_all_chunks` fallback does **not** bump it (it is the
//!   unbounded path the seek avoids when chunk targeting is possible).
//!
//! - [`partitions_decoded`] — incremented **once per partition actually decoded
//!   out of `Data.db`** by the single-candidate *seek* path (Issue #953). Where
//!   `sstables_scanned` proves we touched few SSTables, this proves that *within*
//!   a touched SSTable we did not decode every partition: the seek resolves the
//!   target partition's `Data.db` offset (via the BTI trie or `Index.db`) and
//!   decodes only that one partition, so this stays O(1) for a point lookup. A
//!   regression that reverts the single-candidate path to a full parse-then-retain
//!   would bump this by the SSTable's whole partition count (~N), failing the
//!   `issue_953` bound. It is incremented at the per-partition decode site of the
//!   seek (`SSTableReader::scan_single_partition` → the emit closure that captures
//!   a complete partition), NOT at the result-count boundary. The full-scan +
//!   retain fallback does **not** bump it (it is the unoptimized path the seek
//!   replaces); a candidate that cannot be seeked therefore reads 0 here, which is
//!   why the test asserts a *small upper bound* (decode happened cheaply) rather
//!   than an exact equality.
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

/// The five read-work counters as one value.
///
/// Production code shares a single process-global instance ([`COUNTERS`]) reached
/// through the free functions below; the increment sites and integration probes
/// all operate on that instance. Bundling the atomics in a struct also lets a
/// unit test exercise the add/get/reset contract against a *local* instance,
/// immune to other tests concurrently mutating the global (issue #1071) — the
/// global is shared with read-path code that any parallel test can drive.
struct Counters {
    /// Candidate SSTables actually parsed by a partition-targeted lookup.
    sstables_scanned: AtomicU64,
    /// Partitions a partition-targeted lookup has returned.
    partitions_parsed: AtomicU64,
    /// Partitions DECODED from `Data.db` by the single-candidate seek (Issue #953).
    partitions_decoded: AtomicU64,
    /// Compression chunks DECOMPRESSED by the single-candidate seek (Issue #953 / #951).
    chunks_decompressed: AtomicU64,
    /// Individual rows DECODED within a partition by the seek path (Issue #954).
    rows_decoded: AtomicU64,
}

impl Counters {
    const fn new() -> Self {
        Self {
            sstables_scanned: AtomicU64::new(0),
            partitions_parsed: AtomicU64::new(0),
            partitions_decoded: AtomicU64::new(0),
            chunks_decompressed: AtomicU64::new(0),
            rows_decoded: AtomicU64::new(0),
        }
    }

    #[cfg(not(feature = "tombstones"))]
    fn add_sstables_scanned(&self, count: u64) {
        self.sstables_scanned.fetch_add(count, Ordering::Relaxed);
    }

    #[cfg(not(feature = "tombstones"))]
    fn add_partitions_parsed(&self, count: u64) {
        self.partitions_parsed.fetch_add(count, Ordering::Relaxed);
    }

    #[cfg(not(feature = "tombstones"))]
    fn add_partition_decoded(&self) {
        self.partitions_decoded.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(not(feature = "tombstones"))]
    fn add_chunk_decompressed(&self) {
        self.chunks_decompressed.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(not(feature = "tombstones"))]
    fn add_rows_decoded(&self, count: u64) {
        self.rows_decoded.fetch_add(count, Ordering::Relaxed);
    }

    fn sstables_scanned(&self) -> u64 {
        self.sstables_scanned.load(Ordering::Relaxed)
    }

    fn partitions_parsed(&self) -> u64 {
        self.partitions_parsed.load(Ordering::Relaxed)
    }

    fn partitions_decoded(&self) -> u64 {
        self.partitions_decoded.load(Ordering::Relaxed)
    }

    fn chunks_decompressed(&self) -> u64 {
        self.chunks_decompressed.load(Ordering::Relaxed)
    }

    fn rows_decoded(&self) -> u64 {
        self.rows_decoded.load(Ordering::Relaxed)
    }

    fn reset(&self) {
        self.sstables_scanned.store(0, Ordering::Relaxed);
        self.partitions_parsed.store(0, Ordering::Relaxed);
        self.partitions_decoded.store(0, Ordering::Relaxed);
        self.chunks_decompressed.store(0, Ordering::Relaxed);
        self.rows_decoded.store(0, Ordering::Relaxed);
    }
}

/// The process-global counters every read-path increment site and integration
/// probe shares. Unit tests that assert absolute values use a local
/// [`Counters`] instead (issue #1071).
static COUNTERS: Counters = Counters::new();

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
    COUNTERS.add_sstables_scanned(count);
}

/// Record that `count` partitions were returned by a partition-targeted lookup.
#[cfg(not(feature = "tombstones"))]
pub(crate) fn add_partitions_parsed(count: u64) {
    COUNTERS.add_partitions_parsed(count);
}

/// Record that one partition was DECODED from `Data.db` by the single-candidate
/// seek path (Issue #953). Called once per complete partition the seek decodes
/// at the resolved offset — exactly one for a point lookup that hits, zero for a
/// verified-absent key.
///
/// Gated on `not(tombstones)` like the other mutators: the seek path
/// (`SSTableReader::scan_single_partition`) is only reachable from the default
/// build's `scan_partition`; under `tombstones` the full-scan fallback never
/// seeks, so the counter stays at 0 and the mutator would be dead code.
#[cfg(not(feature = "tombstones"))]
pub(crate) fn add_partition_decoded() {
    COUNTERS.add_partition_decoded();
}

/// Record that one compression chunk was DECOMPRESSED by the single-candidate
/// seek path (Issue #953 / #951). Called once per chunk the seek materializes
/// into its decompressed window while buffering the target partition; the bound
/// (BIG `Index.db` size, BTI next-partition boundary) stops the loop so this
/// stays O(partition chunk span), never the file's whole chunk count.
///
/// Gated on `not(tombstones)` like the other seek-path mutators: only the
/// default build reaches the seek (`bti_pull_decompressed_chunk`); under
/// `tombstones` the full-scan fallback never seeks, so the counter stays 0.
#[cfg(not(feature = "tombstones"))]
pub(crate) fn add_chunk_decompressed() {
    COUNTERS.add_chunk_decompressed();
}

/// Record that one row was DECODED from `Data.db` within a partition by the
/// single-candidate seek path (Issue #954). Called once per row the partition
/// decoder actually parses out of the (clustering-narrowed) byte window — the
/// row-granularity signal that proves a `WHERE pk = ? AND ck </>/= ?` slice
/// query decodes O(matched rows + index block slack), not the whole partition.
///
/// Where [`add_partition_decoded`] counts WHICH partition was touched (1 for a
/// hit), this counts HOW MANY of its clustering rows were parsed: a regression
/// that reverts the clustering seek to a full-partition decode bumps this by the
/// partition's whole row count, failing the `issue_954` bound even though
/// `partitions_decoded` stays 1.
///
/// Gated on `not(tombstones)` like the other seek-path mutators: only the
/// default build reaches the seek; under `tombstones` the full-scan fallback
/// never seeks, so the counter stays 0.
#[cfg(not(feature = "tombstones"))]
pub(crate) fn add_rows_decoded(count: u64) {
    COUNTERS.add_rows_decoded(count);
}

/// Number of candidate SSTables parsed by partition-targeted lookups since the
/// last [`reset`].
///
/// Tests assert this stays O(candidates) — near 1 for a key in a single SSTable
/// (plus a small allowance for bloom false-positives) — so a regression that
/// reopens every SSTable for a single-partition read fails CI.
pub fn sstables_scanned() -> u64 {
    COUNTERS.sstables_scanned()
}

/// Number of partitions returned by partition-targeted lookups since the last
/// [`reset`].
pub fn partitions_parsed() -> u64 {
    COUNTERS.partitions_parsed()
}

/// Number of partitions DECODED from `Data.db` by the single-candidate seek path
/// since the last [`reset`] (Issue #953).
///
/// Tests assert this stays O(1) for a point lookup — a small bound, near 1 for a
/// hit — so a regression that reverts the single-candidate path to a full parse
/// (decoding every partition in the SSTable, then retaining one) fails CI.
pub fn partitions_decoded() -> u64 {
    COUNTERS.partitions_decoded()
}

/// Number of compression chunks DECOMPRESSED by the single-candidate seek path
/// since the last [`reset`] (Issue #953 / #951).
///
/// Tests assert this stays bounded by the target partition's chunk span — a
/// small constant for a point lookup — so a regression that stitches the
/// `Data.db` section to EOF (decompressing every chunk after the target,
/// including the whole tail of a large file for a head-of-file lookup) fails the
/// `issue_953` bound, even though `partitions_decoded` would still read 1.
pub fn chunks_decompressed() -> u64 {
    COUNTERS.chunks_decompressed()
}

/// Number of individual partition rows DECODED from `Data.db` by the
/// single-candidate seek path since the last [`reset`] (Issue #954).
///
/// Tests assert this stays bounded by the requested clustering slice (plus one
/// index block of block-granularity slack) for a `WHERE pk = ? AND ck </>/= ?`
/// query — proving the within-partition seek decodes O(slice), not the whole
/// partition. A regression that decodes every clustering row of the partition
/// (then post-filters) bumps this by the partition's full row count and fails
/// the bound, even though `partitions_decoded` would still read 1.
pub fn rows_decoded() -> u64 {
    COUNTERS.rows_decoded()
}

/// Clear all five process-global counters. Integration tests call this before a
/// query so a stale value from an earlier query cannot satisfy a later
/// assertion. Because the global is shared, an integration test that asserts on
/// it must run without a concurrent query on another thread (the integration
/// tests serialize their own setup); the in-crate unit test sidesteps this
/// entirely by asserting against a local [`Counters`] instance (issue #1071).
pub fn reset() {
    COUNTERS.reset();
}

#[cfg(all(test, not(feature = "tombstones")))]
mod tests {
    use super::*;

    // Exercises the add/get/reset contract against a *local* [`Counters`] rather
    // than the process-global instance reached through the free functions. The
    // global is shared with read-path increment sites that any concurrent test
    // in this binary can drive, so absolute-value assertions on it race
    // nondeterministically (issue #1071). A local instance is owned by this test
    // alone, so the exact-equality checks below are deterministic.
    #[test]
    fn counters_round_trip() {
        let c = Counters::new();
        c.reset();
        assert_eq!(c.sstables_scanned(), 0);
        assert_eq!(c.partitions_parsed(), 0);
        assert_eq!(c.partitions_decoded(), 0);
        assert_eq!(c.chunks_decompressed(), 0);
        assert_eq!(c.rows_decoded(), 0);
        c.add_sstables_scanned(2);
        c.add_partitions_parsed(5);
        c.add_partition_decoded();
        c.add_partition_decoded();
        c.add_chunk_decompressed();
        c.add_chunk_decompressed();
        c.add_chunk_decompressed();
        c.add_rows_decoded(7);
        assert_eq!(c.sstables_scanned(), 2);
        assert_eq!(c.partitions_parsed(), 5);
        assert_eq!(c.partitions_decoded(), 2);
        assert_eq!(c.chunks_decompressed(), 3);
        assert_eq!(c.rows_decoded(), 7);
        c.reset();
        assert_eq!(c.sstables_scanned(), 0);
        assert_eq!(c.partitions_parsed(), 0);
        assert_eq!(c.partitions_decoded(), 0);
        assert_eq!(c.chunks_decompressed(), 0);
        assert_eq!(c.rows_decoded(), 0);
    }
}
