//! Access-path signal for CQL `SELECT` execution (Issue #960, Epic #951).
//!
//! # Why this exists
//!
//! Correct result rows do **not** prove that a storage-layer prune/seek
//! capability is actually wired into the CQL execution path. #949 returned the
//! right rows while still scanning every SSTable and filtering in memory. Tests,
//! reviewers, and (eventually) `EXPLAIN` output need an *explicit, assertable*
//! signal for the access path a `SELECT` chose, so that an accidental fallback
//! to a full scan cannot masquerade as a targeted lookup.
//!
//! This module exposes:
//!
//! 1. [`AccessPath`] — a closed enum describing the access path a SELECT surface
//!    selected. Downstream issues (#958 work counters, #962 fast-path
//!    unification) consume this same enum, so the variant names are part of the
//!    public contract.
//! 2. [`FallbackReason`] — a documented closed set of reasons a SELECT fell back
//!    to a full scan. Recording an *honest* reason here is mandatory: a path that
//!    still full-scans today MUST report [`AccessPath::FullScan`] or
//!    [`AccessPath::FallbackFullScan`], never a fake targeted path.
//! 3. A process-global probe ([`record`], [`last`], [`reset`]) that mirrors the
//!    `scan_for_key_call_count` pattern (issue #831). Because the streaming
//!    SELECT path executes inside a spawned task (a different thread), the probe
//!    is a process-global rather than a thread-local — that is the only mechanism
//!    observable from *both* the materializing and streaming paths and from an
//!    integration test without parsing logs. It is stored lock-free via
//!    `arc_swap::ArcSwapOption` (issue #1595): the signal was written on every
//!    SELECT, so a `Mutex` here was a process-wide serialization point (and a
//!    lock-poisoning surface) with no functional need.
//!
//! # Scope (per #960)
//!
//! #960 only *exposes and reports* the path; it does not make every path
//! targeted (that is #962). The reported path must be **honest** — if the
//! WRITETIME/TTL metadata path still full-scans today, it reports
//! [`AccessPath::FullScan`], and a test pins that current reality so #962 can
//! later flip it.

use arc_swap::ArcSwapOption;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The access path a `SELECT` surface selected for a single SSTable-scan step.
///
/// This is the public, testable signal. Variant names are a shared contract with
/// #958 (work counters) and #962 (fast-path unification); do not rename without
/// updating those consumers and `docs/access-paths.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// snake_case so the serialized tag matches the documented stable `label()` form
// (e.g. "partition_lookup", "fallback_full_scan") rather than the Rust variant
// name. Keep this in lockstep with `label()` and `docs/access-paths.md`.
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AccessPath {
    /// Every SSTable for the table is scanned and rows are filtered in memory.
    /// This is the honest baseline when no usable restriction is present.
    FullScan,

    /// A single, fully-constrained partition (`WHERE pk = ?`) is served by a
    /// partition-targeted lookup that prunes SSTables (bloom/BTI presence) before
    /// scanning. Materializing path (#949 fast path).
    PartitionLookup,

    /// Several fully-constrained partitions (`WHERE pk IN (...)` / token fan-out)
    /// are served by repeated partition-targeted lookups. Reserved for #955; not
    /// yet produced by any surface, but named here so #955 can consume it.
    MultiPartitionLookup,

    /// A partition is targeted and a clustering-key predicate
    /// (`WHERE pk = ? AND ck </>/= ?`) prunes rows within the partition. Reserved
    /// for #954; not yet produced by any surface.
    ClusteringSlice,

    /// The WRITETIME/TTL metadata-projection path resolved a single fully
    /// constrained partition via a targeted lookup. Reserved for #962 — the
    /// metadata path full-scans today (see [`FallbackReason::MetadataScanPath`]).
    MetadataPartitionLookup,

    /// The streaming SELECT path served a single fully-constrained partition via a
    /// partition-targeted lookup (the streaming analogue of [`Self::PartitionLookup`]).
    StreamingPartitionLookup,

    /// A targeted path was not selected and execution fell back to a full scan.
    /// The `reason` is from the documented closed set [`FallbackReason`], so an
    /// accidental fallback cannot look like a targeted success.
    FallbackFullScan {
        /// Why the targeted path was not taken.
        reason: FallbackReason,
    },
}

impl AccessPath {
    /// True if this path scans the whole table (full scan or any fallback to one).
    ///
    /// Tests use this to assert "this query did NOT do a full scan" without
    /// matching every fallback reason.
    pub fn is_full_scan(&self) -> bool {
        matches!(
            self,
            AccessPath::FullScan | AccessPath::FallbackFullScan { .. }
        )
    }

    /// True if this path targeted one or more specific partitions rather than
    /// scanning the whole table.
    pub fn is_targeted(&self) -> bool {
        matches!(
            self,
            AccessPath::PartitionLookup
                | AccessPath::MultiPartitionLookup
                | AccessPath::ClusteringSlice
                | AccessPath::MetadataPartitionLookup
                | AccessPath::StreamingPartitionLookup
        )
    }

    /// A stable, lowercase label suitable for `EXPLAIN`-style output and JSON.
    pub fn label(&self) -> &'static str {
        match self {
            AccessPath::FullScan => "full_scan",
            AccessPath::PartitionLookup => "partition_lookup",
            AccessPath::MultiPartitionLookup => "multi_partition_lookup",
            AccessPath::ClusteringSlice => "clustering_slice",
            AccessPath::MetadataPartitionLookup => "metadata_partition_lookup",
            AccessPath::StreamingPartitionLookup => "streaming_partition_lookup",
            AccessPath::FallbackFullScan { .. } => "fallback_full_scan",
        }
    }
}

/// The documented, closed set of reasons a `SELECT` fell back to a full scan.
///
/// Keep this list in sync with `docs/access-paths.md`. Adding a variant is a
/// public-contract change: it documents a *new, known* reason a query cannot use
/// a targeted path. It must never be used to paper over an unexpected fallback —
/// an unexpected fallback should surface as a failing access-path assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
// snake_case so the serialized form matches the documented stable `label()`
// (e.g. "metadata_scan_path"). Keep in lockstep with `label()`.
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FallbackReason {
    /// No schema was available, so the partition-key columns cannot be
    /// identified to build a targeted lookup.
    NoSchema,

    /// The WHERE clause does not fully constrain the partition key with equality
    /// (partial key, no restriction, or a non-equality restriction such as a
    /// range). Mirrors Cassandra's single-partition-read requirement.
    PartitionKeyNotFullyConstrained,

    /// The fully-constrained partition-key values could not be encoded to the
    /// on-disk key form (e.g. a type mismatch). A full scan is the safe fallback.
    PartitionKeyEncodingFailed,

    /// The WRITETIME/TTL metadata-projection path always full-scans today; it
    /// does not yet route through a partition-targeted lookup. Pinned by tests
    /// so #962 can flip it to [`AccessPath::MetadataPartitionLookup`].
    MetadataScanPath,

    /// The legacy `QueryExecutor` issues an unconditional `storage.scan`; it does
    /// not consult the fast path. Since issue #1750 ad-hoc SELECTs route through
    /// the modern executor, so this reason now covers the remaining legacy SELECT
    /// surfaces (a cached legacy SELECT plan reused via the plan-cache HIT branch).
    /// Tracked by #962 (route the legacy/prepared surfaces through the modern
    /// executor) and #961 (param binding).
    LegacyExecutorPath,

    /// The `tombstones` build compiles out the partition-targeted prune. On that
    /// build the targeted storage surfaces (`scan_partition`,
    /// `scan_partition_with_cell_metadata`) are full-scan + retain fallbacks with
    /// NO bloom/BTI SSTable pruning, so a fully-constrained `WHERE pk = ?` (or
    /// `IN (...)`/WRITETIME-TTL) opens the whole table even though the rows are
    /// byte-identical to the pruned build. The executor reports this honest reason
    /// (rather than a fake targeted label) whenever the storage call returns
    /// `engaged == false` (Epic #951, honest access paths).
    TombstonesBuildNoPrune,
}

impl FallbackReason {
    /// A stable, lowercase label suitable for `EXPLAIN`-style output and JSON.
    pub fn label(&self) -> &'static str {
        match self {
            FallbackReason::NoSchema => "no_schema",
            FallbackReason::PartitionKeyNotFullyConstrained => {
                "partition_key_not_fully_constrained"
            }
            FallbackReason::PartitionKeyEncodingFailed => "partition_key_encoding_failed",
            FallbackReason::MetadataScanPath => "metadata_scan_path",
            FallbackReason::LegacyExecutorPath => "legacy_executor_path",
            FallbackReason::TombstonesBuildNoPrune => "tombstones_build_no_prune",
        }
    }
}

impl std::fmt::Display for AccessPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccessPath::FallbackFullScan { reason } => {
                write!(f, "{} ({})", self.label(), reason.label())
            }
            _ => f.write_str(self.label()),
        }
    }
}

/// Process-global record of the most recent access path selected by a SELECT
/// SSTable-scan step.
///
/// An [`ArcSwapOption`] rather than a thread-local because the streaming SELECT
/// path runs inside a spawned task; the probe must be observable from a different
/// thread than the one that called `execute_streaming`. Lock-free (issue #1595):
/// the signal is written on every SELECT, so it must not be a serialization point.
/// It is not gated behind `cfg(test)` because integration tests in `tests/` compile
/// against the library crate without its `test` cfg (same rationale as
/// `SCAN_FOR_KEY_CALLS`, issue #831).
static LAST_ACCESS_PATH: ArcSwapOption<AccessPath> = ArcSwapOption::const_empty();

/// Record the access path chosen by a SELECT SSTable-scan step.
///
/// Called at each decision boundary in the executor. The last call before a test
/// reads [`last`] reflects the path the query actually took. The store is lock-free
/// and infallible (no poisoning surface); recording is a diagnostic side channel and
/// never aborts a query.
pub fn record(path: AccessPath) {
    LAST_ACCESS_PATH.store(Some(Arc::new(path)));
}

/// Read the most recently recorded access path, or `None` if none was recorded
/// since the last [`reset`].
///
/// Tests assert against this after running a query. Mirrors
/// `SSTableReader::scan_for_key_call_count` (issue #831).
pub fn last() -> Option<AccessPath> {
    LAST_ACCESS_PATH.load_full().map(|path| (*path).clone())
}

/// Clear the recorded access path. Tests call this before a query so a stale
/// value from a previous query cannot satisfy the assertion.
pub fn reset() {
    LAST_ACCESS_PATH.store(None);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_scan_is_not_targeted() {
        assert!(AccessPath::FullScan.is_full_scan());
        assert!(!AccessPath::FullScan.is_targeted());
    }

    #[test]
    fn fallback_is_full_scan_and_carries_reason() {
        let p = AccessPath::FallbackFullScan {
            reason: FallbackReason::NoSchema,
        };
        assert!(p.is_full_scan());
        assert!(!p.is_targeted());
        assert_eq!(p.to_string(), "fallback_full_scan (no_schema)");
    }

    #[test]
    fn partition_lookup_is_targeted() {
        assert!(AccessPath::PartitionLookup.is_targeted());
        assert!(!AccessPath::PartitionLookup.is_full_scan());
        assert_eq!(AccessPath::PartitionLookup.label(), "partition_lookup");
    }

    #[test]
    fn serde_tag_matches_label() {
        // The serialized form must equal the documented stable label(), not the
        // Rust variant name. A simple variant serializes as a bare JSON string.
        assert_eq!(
            serde_json::to_string(&AccessPath::PartitionLookup).unwrap(),
            "\"partition_lookup\""
        );
        assert_eq!(
            serde_json::to_string(&AccessPath::MetadataPartitionLookup).unwrap(),
            "\"metadata_partition_lookup\""
        );
        // FallbackReason tag also matches its label().
        assert_eq!(
            serde_json::to_string(&FallbackReason::MetadataScanPath).unwrap(),
            "\"metadata_scan_path\""
        );
        // Round-trips.
        let p = AccessPath::FallbackFullScan {
            reason: FallbackReason::PartitionKeyNotFullyConstrained,
        };
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(serde_json::from_str::<AccessPath>(&json).unwrap(), p);
    }

    #[test]
    fn probe_round_trips() {
        reset();
        assert_eq!(last(), None);
        record(AccessPath::PartitionLookup);
        assert_eq!(last(), Some(AccessPath::PartitionLookup));
        reset();
        assert_eq!(last(), None);
    }
}
