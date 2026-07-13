//! Flight warm-handle service: generation-keyed parse cache across requests
//! (epic #2310).
//!
//! cqlite-flight is a long-running server that behaves statelessly per request:
//! every `do_get` re-parses the schema, re-resolves the directory, and re-opens
//! every SSTable reader (Index/Summary/Statistics/bloom parse) from cold, then
//! throws it away. This module gives flight a WARM, generation-keyed handle per
//! table so that parse cost is paid once per SSTable generation instead of once
//! per request.
//!
//! ## Module map (split by responsibility, campsite rule)
//!
//! * [`identity`] — inode-stable [`GenerationId`]/[`GenerationSet`] (Decision 1:
//!   the cache key is generation identity, never a directory path or TTL).
//! * [`probe`] — the per-request staleness probe (Decision 2: authoritative
//!   listing backbone + snapshot `manifest.json` fast path, zero staleness
//!   window, no heuristics).
//! * [`budget`] — explicit byte accounting + the fixed named budget (Decision 4).
//! * [`metrics`] — bounded hit/miss/evict/refresh-outcome counters riding the
//!   existing observability contract (Decision 4 metrics surface).
//! * [`registry`] — [`WarmTableRegistry`], the fail-closed diff/swap warm set
//!   (Decision 3: adopts `Database::refresh()`'s #1749 contract).
//!
//! The registry hands [`crate::producer::MergeProducer`] a pre-resolved,
//! pre-parsed `Vec<Arc<SSTableReader>>` (via the #2346 `new_from_readers` /
//! `build_single_partition_merger_from_readers` seams) instead of routing through
//! a core `Database` — flight's `DirSource`/`ScanSpec`/token-prune shape differs
//! from `Database`'s query surface (design Open Question 1).

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::reader::SSTableReader;

pub mod budget;
pub mod identity;
pub mod metrics;
pub mod probe;
pub mod rebuild;
pub mod registry;

pub use identity::{GenerationId, GenerationSet};
pub use metrics::{RefreshOutcome, WarmMetrics, WarmMetricsSnapshot};
pub use registry::{TableKey, WarmTableRegistry};

/// Errors from the warm-handle path.
///
/// Fail-closed by construction (mirrors #1749): a probe error is surfaced rather
/// than serving a stale warm hit, and an open failure during a rebuild leaves the
/// previously warm set intact (the registry never mutates on the error path).
#[derive(Debug, thiserror::Error)]
pub enum WarmError {
    /// The request was cancelled cooperatively (issue #2264/#1473) before or
    /// during probe/rebuild — a clean, expected abort, surfaced by VARIANT (never
    /// masked as another error).
    #[error("warm-handle work cancelled")]
    Cancelled,
    /// The staleness probe could not read the resolved directory. Treated as
    /// "changed / re-resolve failed" — never a stale warm hit.
    #[error("warm-handle probe failed for {path}: {source}")]
    Probe {
        /// Directory that could not be listed.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// A `*-Data.db` directory entry seen during the staleness probe was rejected:
    /// its resolved path escaped the table directory (issue #1430 containment) or
    /// it could not be `stat`ed. Fail-closed exactly like [`Probe`] — treated as
    /// "changed" (full re-resolve), NEVER silently skipped into a smaller
    /// generation set that could mask a generation and serve a stale warm hit
    /// (issue #2310).
    #[error("warm-handle probe rejected entry {path}: {reason}")]
    ProbeEntry {
        /// The offending directory entry.
        path: PathBuf,
        /// Why it was rejected (containment escape / stat failure).
        reason: String,
    },
    /// An added generation failed to open during a rebuild (e.g. a corrupt
    /// `Statistics.db`, #1626). The previously warm set is left fully intact
    /// (fail-closed); this typed error is surfaced.
    #[error("warm-handle rebuild failed to open {path}: {source}")]
    Open {
        /// The `Data.db` that failed to open.
        path: PathBuf,
        /// Underlying open/parse error.
        source: cqlite_core::Error,
    },
    /// The registry could not build the tokio runtime / [`cqlite_core::Platform`]
    /// needed to open readers. Internal fault; the warm set is untouched.
    #[error("warm-handle runtime unavailable: {0}")]
    Runtime(String),
}

/// A pre-resolved, pre-parsed reader set handed to the merge producer for one
/// request (Decision 3). The `Arc<SSTableReader>` clones isolate this request
/// from any concurrent rebuild swap: the in-flight request completes against
/// exactly these readers even if the warm set is rebuilt underneath it (#1749).
#[derive(Clone)]
pub struct WarmSet {
    /// The generation readers, ordered newest-generation-first (LWW tie-break
    /// rank, exactly as the path-based `DirSource` ordering).
    pub readers: Vec<Arc<SSTableReader>>,
    /// The parsed table schema (cached per (table, DDL hash), Decision open-Q 3).
    pub schema: Arc<TableSchema>,
    /// The refresh outcome for this lookup (drives metrics + tests).
    pub outcome: RefreshOutcome,
    /// Reader opens performed on this call — the work-done probe. `0` on a warm
    /// hit (spec Requirement 2/8).
    pub reader_opens: u64,
}

impl std::fmt::Debug for WarmSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `SSTableReader` is not `Debug`; summarise the set instead of printing
        // every reader (keeps the type usable in `Result::expect_err` etc.).
        f.debug_struct("WarmSet")
            .field("readers", &self.readers.len())
            .field("outcome", &self.outcome)
            .field("reader_opens", &self.reader_opens)
            .finish()
    }
}

/// A stable hash of a ticket's CQL DDL, used to key the parsed-schema cache per
/// (table, DDL) (design Open Question 3): the DDL rides the ticket and can in
/// principle change, so keying on it keeps the cache authoritative.
pub fn ddl_hash(ddl: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    ddl.hash(&mut h);
    h.finish()
}
