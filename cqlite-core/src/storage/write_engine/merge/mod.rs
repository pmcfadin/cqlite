//! K-way merge for combining multiple L0 SSTables
//!
//! Implements efficient k-way merge using a binary heap for producing
//! compacted SSTables from multiple runs.
//!
//! File-size note (campsite rule, epic #1116): this file is ~12.7k lines, still
//! far over the ~800-line source threshold. Issue #3139 made a first cut into
//! it, lifting the streaming producer/iterator seam out VERBATIM into the
//! sibling `producer_iter` (adapter + path-based producer thread + consumer +
//! teardown) and `producer_iter_convert` (reader→`MergeEntry` conversion)
//! modules — ~890 lines, pure code motion, no behaviour change. What remains
//! (`KWayMerger` and its reconciliation, plus ~8k lines of inline tests) is
//! still #1116's scope. New code belongs in a sibling module, not here: #2346's
//! shared-reader construction went to `from_readers` for the same reason.
//!
//! ## Architecture
//!
//! The K-way merger uses a min-heap to efficiently merge k sorted SSTable
//! runs into a single output SSTable. Each run maintains a peek buffer for
//! efficient lookahead.
//!
//! ## Ordering
//!
//! The `Ord`/`PartialOrd` impl on `MergeEntry` governs **heap routing only**
//! (which partition/clustering bucket an entry belongs to) — NOT winner
//! selection. Winner selection among entries with the same clustering key is
//! done by `merge_partition_rows` (see "Cell Merge Rule" below), which layers a
//! timestamp + liveness comparison on top.
//!
//! Heap-routing order:
//! 1. Token (ascending) - Primary partitioning
//! 2. Key bytes (ascending) - Hash collision resolution
//! 3. Clustering key (schema-aware) - Within partition ordering
//! 4. Run index (ascending) - Stable tiebreak for routing (NOT the LWW rule)
//!
//! ## Memory Budget (Issue #754 groundwork, Issue #827 streaming read)
//!
//! The bounded `sync_channel` limits how many converted `MergeEntry` values from
//! each source live in memory simultaneously between producer and consumer. The
//! consumer/heap pulls lazily via cursors, so the channel acts as a backpressure
//! valve. Its ROW budget is [`STREAMING_CHANNEL_CAPACITY`], adaptively reduced
//! under concurrent merges (see [`egress_budget`]); since issue #2820 the channel
//! carries BATCHES, so that budget is converted to a message capacity and the
//! TOTAL IN-FLIGHT rows worst case per source is
//! `egress_batch::max_inflight_rows` = `4 × rows_cap` (1024 at the default, 32
//! at the throttled floor — bounded BY the row capacity, plus a byte budget) —
//! see [`egress_batch`]. That is the MEMORY bound; the strictly smaller
//! CHANNEL-RESIDENT figure `rows_resident_in_channel` = `2 × rows_cap` (512) is
//! what the #2419 depth gauge can reach, and the two must not be conflated.
//!
//! The producer thread streams its source via
//! [`stream_all_partitions_for_compaction`](crate::storage::sstable::reader::SSTableReader::stream_all_partitions_for_compaction),
//! which uses a sliding-window incremental stitch+parse: it decompresses one
//! chunk at a time, drains every fully-decoded partition out of the window, and
//! forwards each entry through the bounded channel (batched since issue #2820)
//! before pulling the next chunk. The blocking `SyncSender::send` backpressure plus the bounded window
//! mean a source's decompressed content is NEVER fully resident — peak memory
//! is bounded by roughly `max_partition_size + one_chunk + channel_capacity`
//! per source, independent of total input size (issue #827). The dhat test
//! `tests/test_issue_827_merge_streaming_memory.rs` asserts the 128 MiB bound
//! against inputs whose total decompressed size exceeds it.
//!
//! ## Cell Merge Rule
//!
//! Last-write-wins by timestamp, following Cassandra `Cells#reconcile`:
//! - Keep the entry with the highest timestamp.
//! - If timestamps are equal, the tombstone (Delete) wins over a live entry,
//!   independent of which file it came from (Issue #498).
//! - If timestamp AND liveness are equal, prefer the lower run_index (newer file).
//!
//! Implementation for M5.2 (Issue #382)

#[cfg(feature = "write-support")]
use crate::error::{Error, Result};
#[cfg(feature = "write-support")]
use crate::schema::TableSchema;
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::{
    ClusteringKey, DecoratedKey, PartitionTombstone, RangeTombstone,
};
#[cfg(feature = "write-support")]
use crate::storage::write_engine::reconcile_rules;
// `Value` is no longer used by this file's production code (#2820 moved the
// per-value size walk to `entry_size`), but the in-file test modules reach it
// through `use super::*`.
#[cfg(all(test, feature = "write-support"))]
use crate::types::Value;

#[cfg(feature = "write-support")]
use std::cmp::{Ordering, Reverse};
#[cfg(feature = "write-support")]
use std::collections::{BinaryHeap, VecDeque};
#[cfg(feature = "write-support")]
use std::path::{Path, PathBuf};
#[cfg(feature = "write-support")]
use std::time::{Duration, Instant};

mod model;
#[cfg(feature = "write-support")]
pub use model::{CellData, ComplexDeletion, MergeEntry, MergeStats, MergeStep, RowData};

/// Read-shape reassembly of a merged row's per-column cells (issue #2324):
/// collapse per-element collection cells back into a single `Value::List` /
/// `Value::Set` / `Value::Map` for read consumers that key cells by column name.
mod read_assembly;
/// ONE authority for "can the merged arm ORDER this composite?" — the bypass
/// divergence predicate in `cqlite-flight` asks this rather than keeping its own leaf
/// list, so the two arms cannot disagree (#4063, roborev job 116 F1).
pub use read_assembly::first_unorderable_leaf;
#[cfg(feature = "write-support")]
pub use read_assembly::{assemble_read_cells, assemble_read_cells_with_udts, UdtScope};

/// Single-partition point-read merge builder (issue #2207): assembles a
/// [`KWayMerger`] from per-candidate single-partition runs (seeked or key-filtered)
/// for the Flight `do_get` point-read path. Byte-identical reconciliation to the
/// full-scan merge; only the inputs are narrower.
#[cfg(feature = "write-support")]
mod point_read;
#[cfg(feature = "write-support")]
pub use point_read::{
    build_single_partition_merger, build_single_partition_merger_from_readers,
    build_single_partition_merger_with_registry, PointAccessRecording,
};

/// Warm/shared-reader k-way merge construction (issue #2346): builds a merger
/// directly from already-open `Arc<SSTableReader>`s instead of paths, so a
/// future cached-reader caller (e.g. a Flight warm-handle registry) can drive a
/// merge without re-opening/re-parsing Index/Summary/Statistics/bloom state per
/// request. Reached as `KWayMerger::new_from_readers` (an inherent method added
/// via `impl KWayMerger` in this submodule, mirroring `point_read`'s
/// `from_row_iterators` — no re-export needed). Also hosts
/// `drive_compaction_stream`, the streaming-emit helper shared by BOTH the
/// path-based producer thread (the sibling `producer_iter` module's
/// `SSTableRowIteratorAdapter::producer_thread`) and the new shared-reader
/// producer thread, so the two never drift.
#[cfg(feature = "write-support")]
mod from_readers;

/// Fully-expired SSTable drop classification (issue #1388): the metadata-only
/// `fully_expired_sstables` drop-set used by both compaction surfaces to skip
/// reading SSTables that are entirely past `gcBefore` and overlap-safe.
#[cfg(feature = "write-support")]
mod fully_expired;
#[cfg(feature = "write-support")]
pub use fully_expired::fully_expired_sstables;
#[cfg(feature = "write-support")]
pub(crate) use fully_expired::{reclaim_dropped_whole, split_merge_and_dropped};

/// Repair-state classification + mixed-state rejection for compaction
/// (issue #1021). Reads each input's persisted repair state from `Statistics.db`
/// and either returns the shared state to preserve or rejects a mixed-state set.
#[cfg(feature = "write-support")]
pub mod repair_state;
#[cfg(feature = "write-support")]
pub use repair_state::{classify_inputs, RepairState};

/// Partition-scoped tombstone-carrier pre-scan (issue #1668, stage 1). Extracts
/// the range-tombstone (#933) and partition-deletion (#1072) carriers out of a
/// buffered partition into [`carriers::PartitionCarriers`] as a standalone,
/// testable first pass — the seed for later streaming stages.
#[cfg(feature = "write-support")]
mod carriers;

/// Clustering-group reconciliation kernel, decomposed into named steps
/// (issue #945). See [`reconcile::ReconcileState`].
#[cfg(feature = "write-support")]
mod reconcile;

/// Streaming cluster-group step type (issue #1668; read-path wiring #2230). Wired
/// into compaction AND (via the public re-export below) `cqlite-flight`'s read path.
#[cfg(feature = "write-support")]
mod streaming;
#[cfg(feature = "write-support")]
pub(crate) use streaming::PartitionReconcileCheckpoint;
#[cfg(feature = "write-support")]
pub use streaming::{StreamingMerger, StreamingStep};

/// Schema-aware heap-direct ordering proof (issue #1668, stage 5b) — proves
/// cross-group emission order can be correct straight off a heap, with NO
/// whole-partition `merged.sort_by` afterward. NOT yet wired into
/// `KWayMerger.heap` itself (stage 5c/5d's job); see
/// [`schema_order::schema_ordered_pop_all`].
#[cfg(feature = "write-support")]
mod schema_order;

/// Adapt a merge-path [`CellData`] into the shared [`ReconcileCell`] view
/// (issue #947) so per-cell winner resolution calls
/// [`reconcile_rules::cell_wins`] — the one shared `Cells#reconcile` tie-break —
/// instead of a hand-synced copy. How a tombstone is RECOGNIZED stays here
/// (`Value::Tombstone` payload / IS_DELETED via [`KWayMerger::is_cell_tombstone`]),
/// since that is genuinely type-specific.
#[cfg(feature = "write-support")]
impl reconcile_rules::ReconcileCell for CellData {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
    fn is_tombstone(&self) -> bool {
        KWayMerger::is_cell_tombstone(self)
    }
}

/// A cut position on the clustering axis, used to coalesce range tombstones
/// into a NON-OVERLAPPING canonical sequence (issue #933 / roborev #959 High
/// #1).
///
/// The writer emits each [`RangeTombstone`] as an INDEPENDENT open/close marker
/// pair, sorted by clustering position; the reader pairs markers using a single
/// `pending_range_start`. Overlapping or nested ranges with different bounds
/// would therefore mis-pair on read-back (e.g. `start[1,5] start[2,3] end[2,3]
/// end[1,5]` resurfaces as `[2,3]` and `[Bottom,5]`), corrupting the persisted
/// deletion ranges. Coalescing the cross-SSTable union into disjoint ranges
/// before re-emission mirrors Cassandra's `RangeTombstoneList` invariant
/// (on-disk range tombstones within a partition never overlap).
///
/// A range `[start, end]` is modelled as a closed interval over these cut
/// positions. [`Self::At`] is the infinitesimal point just before (`after =
/// false`) or just after (`after = true`) a clustering prefix; an open bound is
/// [`Self::Bottom`] / [`Self::Top`]. The `after` flag at a SHORTER prefix sorts
/// relative to all longer extensions (Cassandra's kind-weighted prefix
/// ordering), so a prefix end bound `[ck1]` correctly covers every `[ck1, *]`.
#[cfg(feature = "write-support")]
#[derive(Clone)]
enum RangeCut {
    /// Before all clustering keys (start of partition).
    Bottom,
    /// Infinitesimally before (`after == false`) or after (`after == true`) the
    /// clustering prefix.
    At {
        /// Clustering prefix (possibly shorter than the full arity).
        key: ClusteringKey,
        /// `false` = just before the prefix and all its extensions; `true` =
        /// just after them.
        after: bool,
    },
    /// After all clustering keys (end of partition).
    Top,
}

/// Buffered reader for a single SSTable run
///
/// Maintains a peek buffer for efficient lookahead without repeated I/O.
/// Buffer size is fixed at 8KB worth of entries for predictable memory usage.
#[cfg(feature = "write-support")]
struct RunReader {
    /// Abstract SSTable row iterator (boxed, not Debug)
    reader: Box<dyn SSTableRowIterator>,
    /// Peek buffer (FIFO)
    buffer: VecDeque<MergeEntry>,
    /// Target buffer size in bytes (~8KB)
    buffer_size: usize,
    /// Whether this run is exhausted
    exhausted: bool,
}

#[cfg(feature = "write-support")]
impl std::fmt::Debug for RunReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunReader")
            .field("buffer_len", &self.buffer.len())
            .field("buffer_size", &self.buffer_size)
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

#[cfg(feature = "write-support")]
impl RunReader {
    /// Default buffer size (8KB worth of entries)
    const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

    /// Create a new run reader
    fn new(reader: Box<dyn SSTableRowIterator>) -> Self {
        Self {
            reader,
            buffer: VecDeque::new(),
            buffer_size: Self::DEFAULT_BUFFER_SIZE,
            exhausted: false,
        }
    }

    /// Peek at the next entry without consuming it
    ///
    /// Returns None if this run is exhausted.
    ///
    /// Test-only since issue #1664: `refill_heap` was the sole production
    /// caller and now moves the owned entry via [`Self::advance`] instead of
    /// peek+clone. Kept for the `RunReader` API test that exercises lazy refill.
    #[cfg(test)]
    fn peek(&mut self) -> Result<Option<&MergeEntry>> {
        // Refill buffer if empty and not exhausted
        if self.buffer.is_empty() && !self.exhausted {
            self.refill_buffer()?;
        }

        Ok(self.buffer.front())
    }

    /// Advance to the next entry
    ///
    /// Consumes the front entry and returns it.
    fn advance(&mut self) -> Result<Option<MergeEntry>> {
        if let Some(entry) = self.buffer.pop_front() {
            return Ok(Some(entry));
        }

        // Buffer empty, try to refill
        if !self.exhausted {
            self.refill_buffer()?;
            Ok(self.buffer.pop_front())
        } else {
            Ok(None)
        }
    }

    /// Check if this run is exhausted
    fn is_exhausted(&self) -> bool {
        self.exhausted && self.buffer.is_empty()
    }

    /// Refill the peek buffer from the underlying reader
    fn refill_buffer(&mut self) -> Result<()> {
        let mut bytes_buffered = 0;

        while bytes_buffered < self.buffer_size {
            // Issue #2819 (B2): time the BLOCKING recv into the pull-wait
            // accumulator so the row-drive loop excludes it from `stream_merge`
            // (zero cost when no flight sink is installed).
            let next = crate::observability::stream_subphase::time_recv(|| self.reader.next());
            match next {
                Some(Ok(entry)) => {
                    // Estimate entry size for buffer management. `saturating_add`:
                    // the estimator fails CLOSED at `usize::MAX` for a
                    // pathologically deep/wide value (#2820), which must stop
                    // read-ahead, never overflow this accumulator.
                    bytes_buffered =
                        bytes_buffered.saturating_add(Self::estimate_entry_size(&entry));
                    self.buffer.push_back(entry);
                }
                Some(Err(e)) => return Err(e),
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }

        Ok(())
    }

    /// Estimate the memory size of an entry
    ///
    /// Delegates to [`entry_size::estimate_entry_size`], a
    /// sibling module (#1116 campsite rule) that walks nested values with a
    /// bounded iterative traversal and an EXHAUSTIVE `Value` match. The previous
    /// inline version ended in `_ => 32` for every complex variant, which made
    /// both byte budgets denominated in this figure — this read-ahead buffer and
    /// the #2820 egress batch budget — bypassable by large nested payloads.
    fn estimate_entry_size(entry: &MergeEntry) -> usize {
        entry_size::estimate_entry_size(entry)
    }
}

/// Abstract iterator trait for SSTable rows
///
/// This allows the K-way merger to work with different SSTable reader
/// implementations without coupling to specific reader types.
#[cfg(feature = "write-support")]
pub trait SSTableRowIterator: Send {
    /// Get the next row from this SSTable
    fn next(&mut self) -> Option<Result<MergeEntry>>;

    /// Test-only observation hook (issue #2765): the bounded egress
    /// `sync_channel` capacity this run's producer→consumer channel was
    /// constructed with — i.e. the EXACT argument passed to `sync_channel`, so a
    /// wiring test can prove the adaptive per-channel capacity actually reaches
    /// the construction site. `None` for runs that have no such channel
    /// (synthetic/seeked `Vec`-backed iterators).
    ///
    /// Issue #2820: that argument is in MESSAGES (the channel carries BATCHES),
    /// so the hook reports BOTH halves of the conversion — the message capacity
    /// AND the `egress_budget` ROW snapshot it was derived from — and a wiring
    /// test asserts the equivalence. Reporting only one would let a channel
    /// silently built with the ROW budget as its message capacity (a 256x
    /// resident-row blow-up) read as correct.
    #[cfg(test)]
    fn egress_channel_capacity(&self) -> Option<usize> {
        None
    }

    /// Test-only sibling of [`Self::egress_channel_capacity`] (issue #2820): the
    /// ROW capacity that message capacity was derived from.
    #[cfg(test)]
    fn egress_rows_capacity(&self) -> Option<usize> {
        None
    }
}

/// Async-to-sync bridge (`block_on_async`) with a cached, long-lived runtime
/// (Issue #587 panic-safety, Issue #1670 no per-call runtime construction).
mod async_bridge;
#[cfg(feature = "write-support")]
pub(crate) use async_bridge::block_on_async;

/// Path-based streaming producer-thread row iterator (issue #3139): the
/// [`SSTableRowIteratorAdapter`] that wraps an async `SSTableReader` into a sync
/// [`SSTableRowIterator`], its producer thread, the consumer side, and the
/// cancel-aware join-on-drop teardown. Lifted VERBATIM out of this file per the
/// #1116 campsite rule (behaviour unchanged); the shared-reader producer shape
/// stays in [`from_readers`].
#[cfg(feature = "write-support")]
mod constructors;
// Matches `mod constructors` above: the item cannot exist without `write-support`,
// so the re-export must not claim otherwise. Not currently reachable as a break —
// `write_engine` is itself `#[cfg(feature = "write-support")]`, so this file only
// compiles when `constructors` does — but the two cfgs disagreeing is a latent trap
// if that outer gate ever moves (#1704, roborev r6).
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
pub(crate) use constructors::merger_deferring_opens;
mod producer_iter;
#[cfg(feature = "write-support")]
use producer_iter::SSTableRowIteratorAdapter;

/// Reader→merge row/cell conversion helpers for [`SSTableRowIteratorAdapter`]
/// (issue #3139): `CompactionRow` → [`MergeEntry`] / [`RowData`] translation,
/// clustering-key extraction and range-bound translation, shared by BOTH
/// producer shapes. An inherent `impl` block in a sibling file (as
/// [`from_readers`] does), lifted verbatim out of this file per #1116.
#[cfg(feature = "write-support")]
mod producer_iter_convert;

// The producer→consumer CHANNEL PROTOCOL (issue #3120): `MergeMsg` (the DATA
// item plus the two TERMINATORS that make "this run finished" an observed fact
// rather than an inference from a channel disconnect) and the channel-safe
// `MergeProducerError` payload (issue #2264, moved here out of this file per
// #1116). Both producer shapes send it; `producer_iter`'s adapter consumes it.
//
// Deliberately a PLAIN comment, not `///` (issue #2820 roborev r3): an OUTER doc
// on the declaration merges with this file's own inner `//!` header and drags the
// merged fragment's link-resolution scope up to `merge`, so every `[`MergeMsg`]`
// / `super::*` link in producer_msg.rs's module doc silently stops resolving.
// The sibling `producer_gauge`/`channel_depth`/`egress_budget`/`entry_size`/
// `egress_batch` declarations use a plain comment for the same reason.
#[cfg(feature = "write-support")]
mod producer_msg;

/// The per-source streaming-channel ROW budget used at LOW concurrency (a single
/// active merge). Each entry is a few hundred bytes; balances producer/consumer
/// sync overhead against memory footprint. Issue #2765: the UPPER clamp of an
/// adaptive per-merge capacity (see [`egress_budget`]) — under concurrent merges
/// the effective capacity shrinks so the aggregate buffered working set tracks a
/// fixed budget instead of growing as `active_merges × K × per_source`. Both
/// sides of that comparison are in `per_source` units, which is `4 × rows_cap`
/// since #2820 (see [`egress_budget`]'s `per_source` section) — NOT `rows_cap`,
/// which is what this sentence multiplied before the channel carried batches.
///
/// Issue #2820: this stays a ROW budget and stays 256 — every `egress_budget`
/// name, doc and test speaks in rows. It is NOT the `sync_channel` argument any
/// more: the channel carries BATCHES, so the argument is
/// `egress_batch::message_capacity_for_rows(this)`, the per-batch ceiling is
/// `egress_batch::batch_limit_ceiling(this)` and the resident-rows worst case is
/// `egress_batch::max_inflight_rows(this)` = `4 × this`.
#[cfg(feature = "write-support")]
const STREAMING_CHANNEL_CAPACITY: usize = 256;

// Producer-thread gauge (issue #2316): live-count + RAII guard backing
// `cqlite.merge.producer_threads`. Kept in a sibling module to bound this file.
#[cfg(feature = "write-support")]
mod producer_gauge;

// Egress-channel-depth gauge (issue #2419, WS2): process-global live occupancy
// of the bounded producer→consumer `sync_channel` backing
// `cqlite.merge.egress_channel_depth`. Sibling module to bound this file.
#[cfg(feature = "write-support")]
mod channel_depth;

// Adaptive egress budget (issue #2765): process-global active-merge count that
// makes the per-merge `sync_channel` capacity track a FIXED aggregate row
// budget instead of a fixed 256 per merge. Sibling module to bound this file.
// Also owns the doc-hidden `egress_channel_capacity_for` / `active_merge_count`
// integration-test hooks and the `KWayMerger::with_egress_slot` builder (kept
// there, not inline here, per the #1116 campsite file-size rule).
#[cfg(feature = "write-support")]
mod egress_budget;
#[cfg(feature = "write-support")]
pub use egress_budget::{active_merge_count, egress_channel_capacity_for};

// Per-entry heap-size estimation (issue #2820): the EXHAUSTIVE, bounded,
// iterative `MergeEntry`/`Value` size walk that both byte budgets — this
// reader's read-ahead buffer and the egress batcher's `BATCH_EMIT_BYTES_MERGE` —
// are denominated in. Sibling module to bound this file (#1116).
#[cfg(feature = "write-support")]
mod entry_size;

// Batched egress fan-in (issue #2820): the ROWS->MESSAGES capacity conversion for
// the bounded channel, the resident-rows bound, and the producer-side batch
// accumulator that turns one channel message per ROW (measured at 49.9% of
// single-stream CPU, ~94% kernel park/wake) into one per BATCH. Sibling module to
// bound this file; also owns the doc-hidden `merge_egress_batch_probe` hook.
#[cfg(feature = "write-support")]
mod egress_batch;
#[cfg(feature = "write-support")]
pub use egress_batch::{merge_egress_batch_probe, EgressBatchProbe};

// Issue #2361: join-on-drop / backpressured-teardown coverage for the streaming
// merge adapter.
#[cfg(all(test, feature = "write-support"))]
mod teardown_tests;

// Issue #3120: fail-closed pins for a PANICKING producer thread on both producer
// shapes and on both the read and the WRITE (compaction) arm. In-src because the
// `producer-fault-injection` arming API is `cfg(test)`-or-feature only.
#[cfg(all(test, feature = "write-support"))]
mod producer_panic_tests;

// Issue #2765: end-to-end wiring evidence that the adaptive egress-budget
// capacity snapshot reaches BOTH channel-construction sites (`open`,
// `open_from_reader`) and is keyed per k-way MERGE (all source channels of one
// merge share ONE snapshot), in a sibling file to bound this one.
#[cfg(all(test, feature = "write-support"))]
mod egress_wiring_tests;

// Issue #1664: `MergeEntry` double-clone regression guard (kept in a sibling
// file, not inline here, per the #1116 campsite file-size rule).
#[cfg(all(test, feature = "write-support"))]
mod clone_regression_tests;

// Issue #1665: reconcile micro-alloc guard — proves `filter_dropped_columns` no
// longer deep-clones the survivor set (sibling file, #1116 campsite rule).
#[cfg(all(test, feature = "write-support"))]
mod reconcile_microalloc_tests;

// Issue #1669: range-shadowing binary-search guard — proves `apply_range_shadowing`
// costs O(rows + ranges) coverage comparisons, not the former O(rows × ranges)
// linear scan (sibling file, #1116 campsite rule).
#[cfg(all(test, feature = "write-support"))]
mod range_shadowing_binsearch_tests;

/// K-way merger for combining multiple SSTables
///
/// Uses a min-heap to efficiently merge k sorted SSTable runs into a single
/// output. Each run maintains a small peek buffer for efficient lookahead.
///
/// ## Usage
///
/// ```rust,ignore
/// // Create merger from input SSTable paths
/// let merger = KWayMerger::new(input_paths, &schema)?;
///
/// // Option 1: Full merge to output writer
/// let stats = merger.merge(&mut output_writer)?;
///
/// // Option 2: Incremental merge (step-by-step)
/// loop {
///     match merger.step()? {
///         MergeStep::Partition { key, rows } => {
///             // Process partition
///         }
///         MergeStep::Complete => break,
///     }
/// }
/// ```
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub struct KWayMerger {
    /// Input runs (one per SSTable)
    runs: Vec<RunReader>,
    /// Min-heap for efficient merge (issue #1668, stage 5c-i): each element
    /// pairs a `MergeEntry` with `schema_arc` so the heap's OWN pop order is
    /// schema-aware (DESC clustering columns, NULL-first absent trailing
    /// components) — see `schema_order::SchemaOrderedEntry`. Was
    /// `BinaryHeap<Reverse<MergeEntry>>` (the fallback, non-schema-aware
    /// `MergeEntry::Ord`) through stage 5b.
    heap: BinaryHeap<Reverse<schema_order::SchemaOrderedEntry>>,
    /// Current partition being merged (for partition boundary detection)
    current_partition: Option<DecoratedKey>,
    /// Table schema for schema-aware merging
    schema: TableSchema,
    /// `Arc`-wrapped clone of `schema`, ADDITIVE (issue #1668, stage 5c-i) —
    /// cheap to clone per heap entry (a refcount bump, not a deep copy),
    /// used ONLY by `schema_order::SchemaOrderedEntry` so the heap's
    /// comparator can be schema-aware without `KWayMerger` becoming
    /// self-referential (a heap entry cannot hold `&self.schema` while
    /// living inside a sibling field of the SAME struct). Every OTHER
    /// existing `self.schema` use-site in this file is UNCHANGED.
    schema_arc: std::sync::Arc<TableSchema>,
    /// gc_grace cutoff (seconds since epoch): tombstones/cells whose
    /// `local_deletion_time < gc_before_secs` are purgeable.
    ///
    /// Threaded in for deterministic, Cassandra-matching purge decisions
    /// (issue #842 parity harness). NOTE: purging is NOT yet applied during the
    /// merge — see `reconcile_cluster` (issues #845 gc_grace purging, #848
    /// TTL/expiring tie-break). The value is currently carried but unused so the
    /// `cqlite compact --gc-before` plumbing and parity harness can land ahead of
    /// the purge semantics.
    #[allow(dead_code)]
    gc_before_secs: Option<i64>,
    /// "now" (seconds since epoch) used to evaluate TTL expiry during merge.
    /// Carried but not yet consulted — see the note on `gc_before_secs`.
    #[allow(dead_code)]
    now_secs: Option<i64>,
    /// Overlap-safety gate for tombstone purging (#921 finding 1).
    ///
    /// A compaction that merges only a SUBSET of a table's SSTables may leave
    /// data shadowed by a tombstone living in a NON-included overlapping
    /// SSTable. Purging the tombstone in that case resurrects the shadowed data
    /// once both files are later read together. Cassandra gates the purge on the
    /// `CompactionController`'s `maxPurgeableTimestamp` / fully-expired-overlap
    /// check; CQLite has no such controller, so it gates conservatively: a
    /// tombstone is purged ONLY when this flag proves the compaction is safe
    /// (i.e. it spans ALL of the table's SSTables — a major/full compaction — so
    /// no non-included overlapping SSTable can hold shadowed data).
    ///
    /// DEFAULT is `false`: the background/partial path does NOT purge, so it can
    /// never resurrect data. The policy-driven path sets it to `true` only when
    /// the selected inputs cover every candidate SSTable for the table.
    purge_safe: bool,
    /// Overlap-aware per-compaction max-purgeable timestamp (#935, parity with
    /// Cassandra `CompactionController.maxPurgeableTimestamp`).
    ///
    /// For a PARTIAL compaction (`purge_safe == false`) this is the MINIMUM write
    /// timestamp (`markedForDeleteAt`, micros) across every NON-INCLUDED
    /// overlapping SSTable for the table — read from those files' `Statistics.db`
    /// minimum-timestamp bound. A tombstone whose own deletion timestamp is
    /// STRICTLY LESS THAN this value provably predates all data living outside the
    /// compaction set, so purging it can never resurrect shadowed data and is
    /// safe even in a partial compaction. A tombstone at or above this bound is
    /// retained.
    ///
    /// `None` (the default) means no overlap bound was supplied, so a partial
    /// compaction keeps the conservative #921 behavior (no purging). A FULL
    /// compaction ignores this field entirely: `purge_safe == true` already proves
    /// there is no non-included overlapping SSTable, so the bound is `+inf`.
    max_purgeable_timestamp: Option<i64>,
    /// Adaptive egress-budget slot (issue #2765). `Some` for a real
    /// channel-backed merge: the RAII guard that counts this merge in the
    /// process-global active-merge total, so the per-channel capacity of OTHER
    /// concurrent merges tracks true concurrency. Declared LAST so it drops AFTER
    /// `runs` (channels torn down), decrementing the count exactly once at merge
    /// end — even on panic/early-return. `None` for test-only mergers built from
    /// pre-supplied runs that never registered a slot. See [`egress_budget`].
    _egress_slot: Option<egress_budget::ActiveMergeGuard>,
}

/// Report returned by [`compact_sstables`].
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub struct CompactReport {
    /// Metadata for the SSTable written by the compaction.
    pub output: crate::storage::sstable::writer::SSTableInfo,
    /// Statistics about the merge that produced it.
    pub stats: MergeStats,
}

/// Derive the sibling `Statistics.db` path for an SSTable `Data.db` path.
///
/// SSTable components share a `nb-<gen>-big-<Component>.db` stem, so the
/// `Statistics.db` sibling is the `Data.db` filename with `Data.db` →
/// `Statistics.db`, joined back onto the same directory. Falls back to the input
/// path's own location when it has no parent. Shared by every Statistics.db
/// reader in this module (`compute_baseline_min`, `compute_max_purgeable_timestamp`,
/// the UDT-eligibility and effective-schema scans) so the component-naming
/// convention lives in one place.
#[cfg(feature = "write-support")]
fn stats_path_for(data_path: &Path) -> PathBuf {
    let filename = data_path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let stats_filename = filename.replace("Data.db", "Statistics.db");
    data_path.parent().unwrap_or(data_path).join(stats_filename)
}

/// Compute output encoding baselines (min timestamp / local-deletion-time / TTL)
/// from the input SSTables' `Statistics.db` files (two-pass compaction, issue #729).
///
/// The output's delta-encoding baseline must be `<=` every per-partition value in
/// every input, so this returns the minimum across all inputs. Each component is
/// left at its `MAX` sentinel when no input contributes a value — matching the
/// value `SSTableWriter::pre_seed_encoding_baselines` then receives.
///
/// Shared by [`compact_sstables`] and `WriteEngine::start_merge` so the one-shot
/// and policy-driven compaction paths seed baselines identically.
#[cfg(feature = "write-support")]
pub fn compute_baseline_min(input_paths: &[PathBuf]) -> (i64, i32, i32) {
    let mut baseline_min_ts = i64::MAX;
    let mut baseline_min_ldt = i32::MAX;
    let mut baseline_min_ttl = i32::MAX;
    for data_path in input_paths {
        // Derive Statistics.db path from Data.db path
        let stats_path = stats_path_for(data_path);
        if !stats_path.exists() {
            continue;
        }
        let stats_bytes = match std::fs::read(&stats_path) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    "Could not read Statistics.db {:?} for baseline pre-seeding: {}",
                    stats_path,
                    e
                );
                continue;
            }
        };
        match crate::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
            &stats_bytes,
            None,
        ) {
            Ok((_, sstable_stats)) => {
                let ts_stats = &sstable_stats.timestamp_stats;
                baseline_min_ts = baseline_min_ts.min(ts_stats.min_timestamp);
                // Local-deletion-time baseline seeding (#853/#886 Finding 2; sentinel
                // exclusion corrected in #1410). The parser reconstructs
                // `min_deletion_time` as `readUnsignedVInt32() + DELETION_TIME_EPOCH`
                // (EncodingStats.java:289). We normalize to the 32-bit signed bit
                // pattern (`as u32 as i32`) — a far-future LDT in [2^31, 2^32) surfaces
                // as an i64 above i32::MAX and is a valid negative-i32 delta baseline
                // (the exact value the DataWriter delta-encodes against), NOT bad data.
                //
                // EXCLUDE any input that carries NO tombstone from the merged LDT-min,
                // matching `EncodingStats.mergeWith` / `EncodingStats.merge`
                // (EncodingStats.java:113-115, 146): a live-only input must NEVER lower
                // the merged min, or a real tombstone LDT is delta-encoded against a
                // too-low baseline and Data.db diverges (#1410: observed cass=99 vs
                // ours=103, first diff at offset 24, where the row-tombstone LDT delta is
                // emitted). This does NOT touch the writer's on-disk format (a header
                // sentinel change would desync the DataWriter's own LIVE-marker baseline,
                // corrupting live complex-column deletion headers — roborev #1410
                // Finding 1); the decision is made HERE, in the compaction baseline seeder.
                //
                // Two distinct on-disk representations mean "this SSTable has no local
                // deletion time" — and CQLite's own writer serializes a live-only SSTable
                // as a bare `0` (StatisticsMetadata::finalize maps the unset `i32::MAX`
                // min to 0), which is INDISTINGUISHABLE from a genuine tombstone whose
                // real LDT is 0 (an old row tombstone with a sub-second write timestamp)
                // by the `min_deletion_time` VALUE alone. So we use an AUTHORITATIVE
                // "has a tombstone" signal, NOT a heuristic on the value (#28): the STATS
                // `estimatedTombstoneDropTime` histogram, which CQLite's writer populates
                // (via `update_local_deletion_time`) and Cassandra writes for EVERY real
                // tombstone LDT — empty iff the SSTable carries no tombstone. We do NOT
                // exclude by LDT value (roborev #1410 Finding 2): `DELETION_TIME_EPOCH`
                // (2015-09-22) is a perfectly valid EXPLICIT tombstone `localDeletionTime`
                // here, so excluding it by value would drop a real tombstone from the
                // baseline and make the writer reject/mis-encode it. The LIVE marker
                // (`i32::MAX` / `NO_DELETION_TIME`) never reaches the histogram
                // (`update_local_deletion_time` filters it), so an empty histogram already
                // covers every no-deletion representation (`0`, `DELETION_TIME_EPOCH`,
                // `i32::MAX`).
                //
                // CONSERVATIVE on a decode failure (roborev #1410 Finding 3): we decode
                // the histogram DIRECTLY here with `parse_stats_extras` and match on its
                // `Result`, NOT `sstable_stats.tombstone_drop_times` — the latter is an
                // EMPTY vec BOTH for a genuinely live-only SSTable AND for a best-effort
                // extras PARSE FAILURE (corrupt / version-mismatched STATS extras), which
                // must not be conflated. We only treat an input as no-tombstone when the
                // extras decode SUCCEEDS with an empty histogram; on ANY decode error we
                // INCLUDE its LDT (cannot prove live-only → never leave the baseline too
                // high, so the merger's re-emitted tombstones stay >= baseline).
                //   * `Ok` + empty histogram → no tombstone → EXCLUDE.
                //   * `Ok` + non-empty histogram → genuine tombstone (incl. real LDT `0`
                //     or `DELETION_TIME_EPOCH`) → INCLUDE.
                //   * `Err` (unparseable extras) → cannot prove live-only → INCLUDE.
                let ldt_bits = ts_stats.min_deletion_time as u32 as i32;
                let has_tombstone =
                    match crate::parser::repair_metadata::parse_stats_extras(&stats_bytes, None) {
                        Ok(extras) => !extras.tombstone_drop_times.is_empty(),
                        Err(e) => {
                            // Unparseable extras: stay conservative and INCLUDE (do NOT skip
                            // a possibly-real LDT baseline). `true` forces the `.min()` below.
                            tracing::debug!(
                                "STATS-extras decode failed for {:?} during baseline seeding; \
                             conservatively including its LDT baseline: {:?}",
                                stats_path,
                                e
                            );
                            true
                        }
                    };
                if has_tombstone {
                    baseline_min_ldt = baseline_min_ldt.min(ldt_bits);
                }
                if let Some(min_ttl) = ts_stats.min_ttl {
                    if min_ttl > 0 && min_ttl < i32::MAX as i64 {
                        baseline_min_ttl = baseline_min_ttl.min(min_ttl as i32);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Could not parse Statistics.db {:?} for baseline pre-seeding: {:?}",
                    stats_path,
                    e
                );
            }
        }
    }
    (baseline_min_ts, baseline_min_ldt, baseline_min_ttl)
}

/// Issue #2299 fail-CLOSED guard for the direct-stream compaction gate.
///
/// The direct-stream write path (`ActiveMerge::stream_rows_directly`) is selected
/// from `compute_baseline_min`'s LDT baseline surviving at the `i32::MAX`
/// (`NO_DELETION_TIME`) sentinel — read as "no input carries ANY deletion, so
/// there are no range/row/partition tombstones to interleave." But
/// `compute_baseline_min` fails OPEN: an input whose `Statistics.db` is MISSING or
/// fails the top-level parse is silently skipped and never contributes to the
/// baseline. If such a skipped input actually carries a tombstone, the baseline
/// stays at the live sentinel, the direct path is wrongly selected, and that
/// input's tombstones are dropped from the compacted output — previously-deleted
/// data resurrects (a silent data-loss failure mode).
///
/// This returns `false` when ANY input's `Statistics.db` is missing or
/// unparseable at the top level, so the caller can force the always-correct
/// buffered path independently of the (fail-open) baseline seeder. The deletion
/// signal is authoritative metadata only (`Statistics.db`), never a byte heuristic
/// (#28); when it cannot be read we refuse to prove "no deletions" and fall back.
///
/// (An input that PARSES but whose best-effort STATS-extras histogram is
/// unparseable is already handled conservatively inside `compute_baseline_min`:
/// `has_tombstone = true` forces its LDT into the baseline, lowering it below the
/// sentinel — so that case does not reach this guard.)
#[cfg(feature = "write-support")]
pub fn all_input_stats_readable(input_paths: &[PathBuf]) -> bool {
    for data_path in input_paths {
        let stats_path = stats_path_for(data_path);
        if !stats_path.exists() {
            return false;
        }
        let stats_bytes = match std::fs::read(&stats_path) {
            Ok(b) => b,
            Err(_) => return false,
        };
        if crate::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
            &stats_bytes,
            None,
        )
        .is_err()
        {
            return false;
        }
    }
    true
}

/// Expiry-aware LOWER bound for the compaction output's `min_local_deletion_time`
/// delta baseline (issue #1537).
///
/// When expiry is active, an EXPIRED expiring cell converts to a tombstone whose
/// `localDeletionTime` is the CREATION time `ldt - ttl` (Cassandra
/// `AbstractCell.purge` → `localDeletionTime() - ttl()`) — STRICTLY BELOW the input's
/// `min_deletion_time` (the EXPIRY instant `ldt`). Without lowering, the input-derived
/// baseline from [`compute_baseline_min`] sits ABOVE that tombstone's LDT and the
/// DataWriter's unsigned LDT-delta underflows (the below-baseline guard rejects it).
///
/// For each input carrying expiring cells (authoritative signal: a present, positive
/// `max_ttl` — `EncodingStats` tracks TTL only for expiring cells),
/// `min_deletion_time - max_ttl` is a PROVABLY-SAFE lower bound for every `ldt - ttl`
/// (each cell has `ldt >= min_deletion_time`, `ttl <= max_ttl`), and `<=` every
/// live/unchanged LDT too — so the delta never underflows and the output is a
/// self-consistent, readable SSTable. Returns the min floor across inputs (wrapped
/// `i32` GC-clock bits), or `None` when no input carries expiring cells (no-op).
///
/// NOTE (byte-parity, deferred to #1538): for MIXED-`ttl` inputs this floor can be
/// conservatively lower than Cassandra's exact per-cell `min(ldt_i - ttl_i)` (a safe,
/// metadata-only conservatism — a lower min never wrongly purges); no active
/// byte-parity golden exists for compaction TTL-expiry output (blocked on #1538).
/// No-heuristics: derived solely from authoritative Statistics.db.
pub(crate) fn compute_expiry_ttl_ldt_floor(input_paths: &[PathBuf]) -> Option<i32> {
    let mut floor: Option<i32> = None;
    for data_path in input_paths {
        let stats_path = stats_path_for(data_path);
        if !stats_path.exists() {
            continue;
        }
        let stats_bytes = match std::fs::read(&stats_path) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    "Could not read Statistics.db {:?} for expiry-TTL LDT floor: {}",
                    stats_path,
                    e
                );
                continue;
            }
        };
        if let Ok((_, sstable_stats)) =
            crate::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
                &stats_bytes,
                None,
            )
        {
            let ts_stats = &sstable_stats.timestamp_stats;
            // Authoritative "has expiring cells" signal: a present, positive
            // `max_ttl` (EncodingStats tracks TTL only for expiring cells).
            let Some(max_ttl) = ts_stats.max_ttl.filter(|&t| t > 0) else {
                continue;
            };
            // Reinterpret the on-disk `min_deletion_time` bits as UNSIGNED GC-clock
            // seconds (mirrors `compute_baseline_min`) before subtracting the TTL.
            let min_ldt_unsigned = i64::from(ts_stats.min_deletion_time as u32);
            // Clamp the floor at 0 before the `as i32` reinterpret. A well-formed
            // GC-clock creation time (`ldt - ttl`) is always >= 0 — Cassandra never
            // writes a pre-epoch localDeletionTime — so `max_ttl > min_deletion_time`
            // (a negative floor) is impossible for real data. The clamp defends the
            // `as i32` cast against a spurious negative bit pattern should a
            // malformed input ever violate that pre-epoch assumption.
            let input_floor_i64 = (min_ldt_unsigned - max_ttl).max(0);
            // Store back as the wrapped i32 GC-clock bit pattern (the DataWriter's
            // delta baseline representation).
            let input_floor = input_floor_i64 as i32;
            floor = Some(match floor {
                Some(cur) => cur.min(input_floor),
                None => input_floor,
            });
        }
    }
    floor
}

/// Compute the overlap-aware max-purgeable timestamp (#935, parity with
/// Cassandra `CompactionController.maxPurgeableTimestamp`) for a PARTIAL
/// compaction from the SSTables NOT included in it.
///
/// `outside_paths` is the set of Data.db paths for the table's SSTables that are
/// NOT part of this compaction but overlap it (conservatively, every other
/// SSTable for the table). The returned value is the MINIMUM write timestamp
/// (`markedForDeleteAt`, micros) across those files — read from each one's
/// `Statistics.db` minimum-timestamp bound (`EncodingStats.minTimestamp`, the same
/// value Cassandra exposes as `SSTableReader.getMinTimestamp()`).
///
/// A tombstone whose own deletion timestamp is STRICTLY LESS THAN this value
/// provably predates all data living outside the compaction set, so purging it can
/// never resurrect shadowed data even in a partial compaction. Thread the result
/// into the merger via [`KWayMerger::with_max_purgeable_timestamp`].
///
/// Returns `None` when `outside_paths` is empty (no overlap — the caller should
/// treat the compaction as full and use `purge_safe`) or when NONE of the outside
/// `Statistics.db` files could be read/parsed (cannot prove safety → stay
/// conservative and do not purge). A missing/unreadable file for one of several
/// outside SSTables is treated conservatively: it cannot raise the bound, so an
/// unreadable outside file leaves the bound unknown and DISABLES overlap-aware
/// purging rather than risk resurrecting its data.
#[cfg(feature = "write-support")]
pub fn compute_max_purgeable_timestamp(outside_paths: &[PathBuf]) -> Option<i64> {
    if outside_paths.is_empty() {
        return None;
    }

    let mut min_ts = i64::MAX;
    for data_path in outside_paths {
        let stats_path = stats_path_for(data_path);
        // Any outside SSTable we cannot read/parse leaves the bound UNKNOWN: we
        // can no longer prove a tombstone predates its data, so disable
        // overlap-aware purging entirely (conservative — never resurrect data).
        if !stats_path.exists() {
            return None;
        }
        let stats_bytes = match std::fs::read(&stats_path) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    "Could not read Statistics.db {:?} for max-purgeable bound: {}",
                    stats_path,
                    e
                );
                return None;
            }
        };
        match crate::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
            &stats_bytes,
            None,
        ) {
            Ok((_, sstable_stats)) => {
                min_ts = min_ts.min(sstable_stats.timestamp_stats.min_timestamp);
            }
            Err(e) => {
                tracing::warn!(
                    "Could not parse Statistics.db {:?} for max-purgeable bound: {:?}",
                    stats_path,
                    e
                );
                return None;
            }
        }
    }

    Some(min_ts)
}

/// Build the effective compaction schema (#850): `schema` augmented with any
/// static columns that the input SSTables' SerializationHeaders declare but the
/// current schema no longer does (e.g. a static column dropped from the table).
///
/// Cassandra reads static-row PRESENCE from each input SSTable's
/// SerializationHeader, not from the current table metadata. After the last
/// static column is dropped, an older SSTable that still carries static rows must
/// keep its static-row prelude through compaction — otherwise the prelude (and
/// any surviving static data) is dropped, diverging from Cassandra. Re-adding the
/// dropped static column to the schema handed to the merger and writer restores
/// that: the reader decodes the static cells and the writer emits the static
/// prelude, exactly as a header-driven compaction would.
///
/// Returns `schema` unchanged when the inputs declare no static column absent
/// from `schema` (the overwhelmingly common case), so output stays byte-identical
/// for every table whose schema still matches its on-disk data.
#[cfg(feature = "write-support")]
/// How the input SSTable headers constrain bare-name UDT normalization for a
/// compaction (#929). The compaction reader decides complex-vs-simple from the
/// single decode schema, so each column's encoding must be consistent across
/// inputs.
#[derive(Debug, Default, Clone)]
pub struct UdtNormalizationPlan {
    /// Columns SAFE to decode/write as complex, mapped to the EXACT
    /// `UserType(...)` marshal string taken from the input headers. The header
    /// marshal is used verbatim (rather than re-rendered from the registry) so
    /// the decode schema is byte-exact and handles nested-UDT field types the
    /// flush-time renderer intentionally skips (roborev #1019).
    pub eligible_marshals: std::collections::HashMap<String, String>,
    /// Columns with an incompatible encoding across inputs — declared BOTH as a
    /// `UserType(...)` (complex) and as a simple form, OR as two DIFFERENT
    /// `UserType(...)` marshals (a UDT definition mismatch). No single decode
    /// schema is correct, so the caller must fail rather than corrupt
    /// (roborev #1017 / #1019).
    pub conflicts: std::collections::HashSet<String>,
    /// Every column name that appears in ANY input header (complex or simple).
    /// A schema column ABSENT from this set is in no input, so there are no cells
    /// to misdecode and it may be safely registry-normalized for the output
    /// (schema evolution: a UDT column added after the inputs were written —
    /// roborev #1023). Trust this set ONLY when `headers_verified` is true.
    pub observed: std::collections::HashSet<String>,
    /// True only when EVERY input header was successfully read and parsed. When
    /// false, header state is unknown: `observed`/`eligible_marshals` are empty
    /// and the caller MUST NOT treat a column's absence from `observed` as proof
    /// it is absent from the inputs (it could be an unreadable simple-cell input
    /// that registry-normalization would misdecode — roborev #1025).
    pub headers_verified: bool,
}

/// Inspect the input SSTable headers to plan bare-name UDT normalization.
///
/// - An input written without registry support stores the column as a simple
///   `BytesType` cell; that VETOES normalization, or the reader would misdecode
///   that input (roborev #1013).
/// - An older input that simply LACKS the column (schema evolution) must NOT
///   veto: absence is not a simple-cell declaration (roborev #1015).
/// - A column with incompatible encodings across inputs (complex vs simple, or
///   two different `UserType` marshals) is a `conflict`: no single decode schema
///   is correct, so the caller must fail rather than silently drop/corrupt the
///   complex values (roborev #1017 / #1019).
///
/// Conservative: if any input's header is missing or unparseable, returns an
/// empty plan (normalize nothing), since the inputs' forms cannot be confirmed.
pub fn udt_columns_eligible_for_normalization(input_paths: &[PathBuf]) -> UdtNormalizationPlan {
    use std::collections::{HashMap, HashSet};
    // A column header is "UDT-bearing" when its marshal mentions `UserType(`
    // ANYWHERE — whether a bare top-level `UserType(...)` (a multicell complex
    // column) OR a `FrozenType(UserType(...))` / a frozen collection-of-UDT
    // like `FrozenType(ListType(...UserType(...)...))` (single-cell frozen UDT
    // collection-of-UDT). All of these MUST carry their exact header marshal to
    // the compaction output, else re-rendering the bare CQL form collapses the
    // nested UDT to `BytesType` and CQLite can no longer decode the cell from its
    // OWN output header (roborev #1020 Finding 3). `is_complex_column` still
    // decides complex-vs-simple correctly off the copied marshal (a
    // `FrozenType(...)` stays single-cell).
    const USERTYPE_MARKER: &str = "org.apache.cassandra.db.marshal.usertype(";
    // column -> distinct UDT-bearing marshal strings observed (exact, orig case)
    let mut usertype_marshals: HashMap<String, HashSet<String>> = HashMap::new();
    // columns declared as a non-UDT-bearing (plain simple / non-UDT) form
    let mut vetoed: HashSet<String> = HashSet::new();
    for data_path in input_paths {
        let stats_path = stats_path_for(data_path);
        let Ok(stats_bytes) = std::fs::read(&stats_path) else {
            return UdtNormalizationPlan::default();
        };
        match crate::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
            &stats_bytes,
            None,
        ) {
            Ok((_, sstable_stats)) => {
                for c in &sstable_stats.serialization_header_columns {
                    if c.column_type.to_lowercase().contains(USERTYPE_MARKER) {
                        usertype_marshals
                            .entry(c.name.clone())
                            .or_default()
                            .insert(c.column_type.clone());
                    } else {
                        // Declared, but carries no UserType -> a simple/non-UDT cell.
                        vetoed.insert(c.name.clone());
                    }
                }
            }
            Err(_) => return UdtNormalizationPlan::default(),
        }
    }

    let mut plan = UdtNormalizationPlan {
        headers_verified: true,
        ..UdtNormalizationPlan::default()
    };
    plan.observed.extend(vetoed.iter().cloned());
    plan.observed.extend(usertype_marshals.keys().cloned());
    for (column, marshals) in usertype_marshals {
        if vetoed.contains(&column) || marshals.len() != 1 {
            // Mixed complex/simple, or disagreeing UDT definitions -> conflict.
            plan.conflicts.insert(column);
        } else if let Some(marshal) = marshals.into_iter().next() {
            plan.eligible_marshals.insert(column, marshal);
        }
    }
    plan
}

/// Apply the #929 compaction normalization to `schema` (the EFFECTIVE decode
/// schema): copy each eligible column's exact `UserType(...)` marshal from the
/// input headers so the compaction reader treats it as complex
/// (`is_complex_column`) and round-trips the per-field UDT cells. Errors when an
/// input set has an incompatible mixed encoding for a column (see
/// [`UdtNormalizationPlan::conflicts`]) rather than silently corrupting data.
pub(crate) fn apply_udt_marshals_from_inputs(
    schema: &mut TableSchema,
    input_paths: &[PathBuf],
    registry: Option<&crate::schema::UdtRegistry>,
) -> Result<()> {
    let plan = udt_columns_eligible_for_normalization(input_paths);
    if !plan.conflicts.is_empty() {
        let mut cols: Vec<&String> = plan.conflicts.iter().collect();
        cols.sort();
        return Err(Error::InvalidInput(format!(
            "compaction inputs disagree on the encoding of UDT column(s) {cols:?}: some store \
             complex UserType cells and others a simple cell (or a different UDT definition). \
             Refusing to compact to avoid data loss; rewrite the divergent SSTable(s) first."
        )));
    }
    let keyspace = schema.keyspace.clone();
    for column in &mut schema.columns {
        if let Some(marshal) = plan.eligible_marshals.get(&column.name) {
            // Observed as complex in the inputs: use the exact header marshal.
            column.data_type = marshal.clone();
        } else if plan.headers_verified && !plan.observed.contains(&column.name) {
            // Absent from ALL input headers (schema evolution: a UDT column added
            // after the inputs were written). No input cells to misdecode, so a
            // registry render is safe and keeps the output header consistent with
            // future flushes (roborev #1023). Columns OBSERVED as simple are left
            // bare on purpose (normalizing them would misdecode those inputs).
            // Requires `headers_verified`: if a header was unreadable we cannot
            // prove the column is truly absent, so we must not normalize it
            // (roborev #1025).
            if let Some(reg) = registry {
                if let Some(marshal) =
                    crate::storage::sstable::writer::data_writer::resolve_bare_udt_marshal(
                        &column.data_type,
                        &keyspace,
                        reg,
                    )
                {
                    column.data_type = marshal;
                }
            }
        }
    }
    Ok(())
}

pub fn effective_compaction_schema(schema: &TableSchema, input_paths: &[PathBuf]) -> TableSchema {
    use std::collections::HashSet;

    // Names already known to the current schema (regular + static columns and
    // both key kinds) must not be re-added.
    let mut known: HashSet<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    for k in &schema.partition_keys {
        known.insert(k.name.clone());
    }
    for k in &schema.clustering_keys {
        known.insert(k.name.clone());
    }

    // (name, cql_type) for static columns present in the inputs but missing from
    // the current schema. Sorted by name for deterministic output.
    let mut added: Vec<(String, String)> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    for data_path in input_paths {
        let stats_path = stats_path_for(data_path);
        if !stats_path.exists() {
            continue;
        }
        let stats_bytes = match std::fs::read(&stats_path) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    "effective_compaction_schema: cannot read Statistics.db {:?}: {}",
                    stats_path,
                    e
                );
                continue;
            }
        };
        match crate::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
            &stats_bytes,
            None,
        ) {
            Ok((_, sstable_stats)) => {
                for col in &sstable_stats.serialization_header_columns {
                    if col.is_static && !known.contains(&col.name) && seen.insert(col.name.clone())
                    {
                        added.push((col.name.clone(), col.column_type.clone()));
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    "effective_compaction_schema: cannot parse Statistics.db {:?}: {:?}",
                    stats_path,
                    e
                );
            }
        }
    }

    if added.is_empty() {
        return schema.clone();
    }

    added.sort_by(|a, b| a.0.cmp(&b.0));
    tracing::info!(
        "effective_compaction_schema: re-adding {} static column(s) dropped from schema \
         {}.{} but still present in input SSTable headers: {:?}",
        added.len(),
        schema.keyspace,
        schema.table,
        added.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>()
    );

    let mut effective = schema.clone();
    for (name, data_type) in added {
        effective.columns.push(crate::schema::Column {
            name,
            data_type,
            nullable: true,
            default: None,
            is_static: true,
        });
    }
    effective
}

/// Compact an explicit set of input SSTables into a single output SSTable.
///
/// Unlike [`WriteEngine::maintenance_step`](super::WriteEngine::maintenance_step),
/// this is a one-shot, policy-free compaction over exactly `input_paths`, writing
/// the merged result to `output_dir` (the writer appends `keyspace/table/`). It is
/// the engine entry point behind the `cqlite compact` CLI command and the
/// compaction-parity harness (issue #842).
///
/// `input_paths` must be ordered newest-to-oldest: index 0 is the newest run, which
/// wins last-write-wins ties at equal timestamp/liveness.
///
/// `gc_before_secs` / `now_secs` are threaded into the merger for deterministic,
/// Cassandra-matching purge decisions. gc_grace tombstone purging IS applied
/// (issue #845), but ONLY when the compaction is overlap-safe — see
/// `KWayMerger::with_purge_safe` (#921 finding 1). TTL expiry (#848) remains
/// carried-but-not-yet-applied.
/// Compute the gc_grace `gcBefore` cutoff (GC-clock seconds) for a compaction.
///
/// Cassandra purges a tombstone during compaction when its on-disk
/// `localDeletionTime` is strictly less than `gcBefore = now - gc_grace_seconds`
/// (parity `8d47ebb2`). CQLite reads `gc_grace_seconds` from the table schema
/// (the `comments` option map — the same surface `cql_parser` uses), since it
/// is a table parameter not recorded inside SSTable files.
///
/// When the table declares NO `gc_grace_seconds` (#921 finding 3), CQLite falls
/// back to Cassandra's table DEFAULT of `864000` seconds (10 days) — CQL tables
/// commonly omit the option and rely on that default, so disabling purging on
/// absence diverged from Cassandra. A value of exactly `0` is valid (immediate
/// grace, `TableParams` allows it) and yields `gcBefore == now`.
///
/// Returns `None` ONLY when the declared value is INVALID — unparseable or
/// NEGATIVE — which DISABLES purging (a strict no-op): garbage metadata must
/// never cause data to be dropped.
#[cfg(feature = "write-support")]
pub(crate) fn compute_gc_before(schema: &TableSchema, now_secs: i64) -> Option<i64> {
    /// Cassandra's `TableParams.DEFAULT_GC_GRACE_SECONDS` (10 days).
    const DEFAULT_GC_GRACE_SECONDS: i64 = 864_000;

    match schema.comments.get("gc_grace_seconds") {
        // Absent → Cassandra default (10 days).
        None => Some(now_secs - DEFAULT_GC_GRACE_SECONDS),
        // Present → parse. Reject unparseable or negative values conservatively
        // (return None = no purge); 0 is valid (immediate grace).
        Some(s) => match s.trim().parse::<i64>() {
            Ok(gc_grace_seconds) if gc_grace_seconds >= 0 => Some(now_secs - gc_grace_seconds),
            _ => None,
        },
    }
}

#[cfg(feature = "write-support")]
/// Determine which dropped columns still have surviving cells after the merge.
///
/// Runs a merge pre-pass with the decode `schema` (so the dropped-column filter
/// applies identically to the write pass) and collects the names of
/// `dropped_columns` that appear in at least one surviving `Live` cell — i.e.
/// columns re-added with writes after their drop time. `compact_sstables` keeps
/// exactly these dropped columns in the output writer schema; the rest are
/// stripped from the output header. Stops early once every dropped column has
/// been observed surviving.
///
/// A dropped column also counts as surviving when its only surviving state is a
/// retained `ComplexDeletion` marker. A complex (collection / UDT) tombstone for
/// a dropped COMPLEX column lives in `row.complex_deletions`, NOT in `cells`, so
/// counting only live `cells` would strip such a column from the output schema —
/// and the writer only emits complex-element columns present in the schema,
/// silently dropping a complex tombstone the merge decided to RETAIN. Because the
/// merge pre-pass runs reconcile with the SAME `gc_before`/`purge_safe` as the
/// write pass, a marker purged by gc never reaches the yielded `rows`, so a
/// retained marker IS counted while a purged one is NOT (#921 finding 2 / #847).
///
/// `purge_safe` (#921 finding 1) and `max_purgeable_timestamp` (#935) MUST match
/// the values the write pass uses (`compact_sstables`' / `start_merge`'s). The
/// pre-pass builds its merger with the SAME gc cutoff (`gc_before_secs`/`now_secs`),
/// the SAME `purge_safe` flag, AND the SAME overlap bound so its purge decisions
/// are byte-identical to the write pass. Without this, a purgeable tombstone in a
/// dropped column would count as a survivor here (no purging) yet be purged in the
/// real merge (purging on) — leaving an empty dropped column in the output header
/// that this pre-pass exists to strip.
pub(crate) fn compute_surviving_dropped_columns(
    input_paths: Vec<PathBuf>,
    schema: &TableSchema,
    gc_before_secs: Option<i64>,
    now_secs: Option<i64>,
    purge_safe: bool,
    max_purgeable_timestamp: Option<i64>,
) -> Result<std::collections::HashSet<String>> {
    let mut surviving: std::collections::HashSet<String> = std::collections::HashSet::new();
    let total = schema.dropped_columns.len();
    let mut merger = KWayMerger::new_with_gc(input_paths, schema, gc_before_secs, now_secs)?
        .with_purge_safe(purge_safe)
        .with_max_purgeable_timestamp(max_purgeable_timestamp);
    loop {
        match merger.step()? {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for row in &rows {
                    if let RowData::Live { cells } = &row.row_data {
                        for cell in cells {
                            if schema.dropped_columns.contains_key(&cell.column) {
                                surviving.insert(cell.column.clone());
                            }
                        }
                    }
                    // A surviving (within-grace / purge-unsafe) complex-deletion
                    // marker for a dropped COMPLEX column is a survivor too: the
                    // marker lives here, not in `cells`, and the writer must keep
                    // the column to emit it.
                    for cd in &row.complex_deletions {
                        if schema.dropped_columns.contains_key(&cd.column) {
                            surviving.insert(cd.column.clone());
                        }
                    }
                }
                if surviving.len() == total {
                    break; // every dropped column already shown to survive
                }
            }
        }
    }
    Ok(surviving)
}

/// One-shot compaction entry point (behind the `cqlite compact` CLI command).
///
/// `purge_safe` (#921 finding 1) gates gc_grace tombstone purging on overlap
/// safety: pass `true` ONLY when `input_paths` spans EVERY SSTable for the table
/// (so no non-included overlapping SSTable can hold data shadowed by a purged
/// tombstone). When `false`, tombstones are retained even if older than
/// `gcBefore`, so a partial compaction cannot resurrect deleted data.
pub async fn compact_sstables(
    input_paths: Vec<PathBuf>,
    output_dir: &std::path::Path,
    schema: &TableSchema,
    generation: u64,
    gc_before_secs: Option<i64>,
    now_secs: Option<i64>,
    purge_safe: bool,
) -> Result<CompactReport> {
    compact_sstables_with_registry(
        input_paths,
        output_dir,
        schema,
        generation,
        gc_before_secs,
        now_secs,
        purge_safe,
        None,
    )
    .await
}

/// Like [`compact_sstables`], but accepts an optional [`UdtRegistry`] so a UDT
/// column ADDED after the inputs were written (absent from every input header)
/// is normalized to its `UserType(...)` marshal in the output instead of being
/// emitted as a bare `BytesType` column (roborev #1027). Pass `None` to keep the
/// header-only behavior of [`compact_sstables`].
#[allow(clippy::too_many_arguments)]
pub async fn compact_sstables_with_registry(
    input_paths: Vec<PathBuf>,
    output_dir: &std::path::Path,
    schema: &TableSchema,
    generation: u64,
    gc_before_secs: Option<i64>,
    now_secs: Option<i64>,
    purge_safe: bool,
    udt_registry: Option<&crate::schema::UdtRegistry>,
) -> Result<CompactReport> {
    if input_paths.is_empty() {
        return Err(Error::InvalidInput(
            "compaction requires at least one input SSTable".to_string(),
        ));
    }

    // Fully-expired SSTable drop (issue #1388, OQ-1 → (A)): the CLI one-shot has no
    // knowledge of SSTables outside its explicit input list, so the drop is only
    // overlap-safe when the operator asserts `--major` (`purge_safe == true`),
    // which means the input set spans EVERY overlapping SSTable for the table ⇒
    // empty outside set ⇒ +inf overlap bound ⇒ every fully-expired input is
    // provably safe to drop. Without `--major` no drop occurs (conservative,
    // matching the tombstone-purge conservatism). The drop-set is subtracted from
    // the merger's input list BEFORE building the merger, so dropped SSTables are
    // never read/decoded (the perf win); their components are deleted only after
    // the output publishes. `split_merge_and_dropped` handles the all-dropped guard.
    let drop_set: Vec<PathBuf> = if purge_safe {
        fully_expired_sstables(&input_paths, &[], gc_before_secs)
    } else {
        Vec::new()
    };
    let (merge_inputs, dropped_whole) = split_merge_and_dropped(&input_paths, drop_set);
    // From here on, decode/merge only the (drop-filtered) `merge_inputs`. The
    // dropped SSTables are reclaimed after the output publishes.
    let input_paths = merge_inputs;

    // #850: read static-row presence from the input SSTable headers. If a static
    // column is absent from the current schema but an input SSTable still declares
    // it (e.g. it was dropped from the catalog entirely, not retained via
    // `dropped_columns`), the effective schema re-adds it so the merger decodes the
    // static cells and the writer emits the static prelude. Byte-identical to
    // `schema` when no such column exists (the common case). This composes with the
    // #847/#904 dropped-column flow below: the effective schema keeps `schema`'s
    // `dropped_columns` map, so the retained-dropped pre-pass and write-schema
    // derivation operate on it unchanged.
    let mut effective_schema = effective_compaction_schema(schema, &input_paths);
    // #929: copy each UDT column's exact UserType(...) marshal from the input
    // headers onto the EFFECTIVE (decode) schema so the compaction reader treats
    // it as complex (is_complex_column) and round-trips the per-field UDT cells
    // (roborev #1009/#1013/#1015/#1017/#1019). This derives ENTIRELY from the
    // input headers, so it runs unconditionally — a registry is needed only at
    // flush time. No-op when no input advertises a UserType column; errors on a
    // mixed encoding rather than corrupting. `write_schema` inherits it via
    // `for_compaction_output` (clones columns). When `udt_registry` is supplied,
    // a UDT column absent from every input (schema evolution) is registry-
    // normalized so the output header is not a bare/BytesType column.
    apply_udt_marshals_from_inputs(&mut effective_schema, &input_paths, udt_registry)?;

    // Decode with `effective_schema` (retains dropped columns so their input cells
    // parse and can be purged), but WRITE with a post-drop schema: fully-purged
    // dropped columns are stripped from the output serialization header (a natural
    // post-drop reader is not misaligned) while dropped columns with surviving
    // re-added cells are retained (those cells keep a matching header column).
    // Which dropped columns survive is data-dependent and the writer fixes its
    // header before the first row, so determine the surviving set with a merge
    // pre-pass (only when any column is dropped). The pre-pass uses the SAME merge
    // logic as the write pass — including the same `purge_safe` flag (#921 finding
    // 1) and gc cutoff — so the two make IDENTICAL purge decisions and a tombstone
    // purged in the write pass is also purged in the pre-pass (never counted as a
    // surviving cell). See #847 review.
    let retained_dropped = if effective_schema.dropped_columns.is_empty() {
        std::collections::HashSet::new()
    } else {
        compute_surviving_dropped_columns(
            input_paths.clone(),
            &effective_schema,
            gc_before_secs,
            now_secs,
            purge_safe,
            // The one-shot `compact_sstables` entry point only knows its explicit
            // input list, not the rest of the table, so it has no overlap bound to
            // supply (#935 overlap-aware purging is driven by the WriteEngine
            // background path). Purging here is governed solely by `purge_safe`.
            None,
        )?
    };
    // `write_schema` inherits the #929 UDT normalization from the already-
    // normalized `effective_schema` (for_compaction_output clones columns).
    let write_schema = effective_schema.for_compaction_output(&retained_dropped);

    // Issue #1234: thread the registry onto the merge readers so a top-level
    // `frozen<UDT>` value decodes structurally instead of erroring out and
    // dropping the partition. Cloned because the merger owns its readers.
    let merger = KWayMerger::new_with_gc_and_registry(
        input_paths.clone(),
        &effective_schema,
        gc_before_secs,
        now_secs,
        udt_registry.cloned(),
    )?
    .with_purge_safe(purge_safe);

    // Repair-state preservation + mixed-state rejection (issue #1021): reject a
    // mixed repaired/unrepaired/pending-repair input set (Cassandra never mixes
    // them in one compaction) and otherwise carry the shared state forward.
    let repair_state = classify_inputs(&input_paths)?;

    let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
        output_dir.to_path_buf(),
        generation,
        &write_schema,
    )?;
    writer.set_repair_state(
        repair_state.repaired_at,
        repair_state.pending_repair,
        repair_state.is_transient,
    );
    // Compaction output (issue #1222): emit the uncompressed-BIG CRC.db with
    // Cassandra's compaction-only trailing empty-final-chunk CRC32 = 0.
    writer.mark_compaction_output();

    // Two-pass compaction (issue #729): seed the output's encoding baselines from
    // the inputs' Statistics.db before writing any partition.
    let (baseline_min_ts, mut baseline_min_ldt, baseline_min_ttl) =
        compute_baseline_min(&input_paths);
    // Issue #1537: when expiry is active, an expired expiring cell converts to a
    // creation-time (`ldt - ttl`) tombstone whose LDT is BELOW the input-derived
    // baseline; lower the LDT baseline to a provably-safe floor so the DataWriter's
    // unsigned LDT-delta never underflows. No-op when no input carries expiring cells.
    if now_secs.is_some() {
        if let Some(floor) = compute_expiry_ttl_ldt_floor(&input_paths) {
            baseline_min_ldt = baseline_min_ldt.min(floor);
        }
    }
    writer.pre_seed_encoding_baselines(baseline_min_ts, baseline_min_ldt, baseline_min_ttl);

    let mut stats = merger.merge(&mut writer)?;
    let output = writer.finish().await?;

    // Issue #1388: the merged output is now published. Reclaim the dropped-whole
    // SSTables (never read into the merger) via the same component-delete path the
    // WriteEngine background compaction uses for merged inputs. Deletion is
    // best-effort: a failure leaves an invisible orphan (its TOC.txt is removed
    // first) reclaimed on next startup, never a hard error — the output is correct.
    //
    // The one-shot CLI surface deletes NO merge inputs (the operator owns the input
    // dir; the output lands in a separate `--output` dir), so `already_deleted` is
    // EMPTY: in the degenerate all-expired case the SSTable the all-dropped guard
    // retained as a merge input is ALSO in `dropped_whole` and IS reclaimed here
    // (roborev #1388 Medium — closes the former "all-expired input left on disk").
    fully_expired::reclaim_dropped_whole(&dropped_whole, &[], |dropped| {
        if let Err(e) =
            crate::storage::write_engine::WriteEngine::delete_sstable_files_static(dropped)
        {
            tracing::warn!(
                "Failed to delete dropped-whole compaction input {:?}: {} \
                 (output is valid; leftover is an invisible orphan)",
                dropped,
                e
            );
        }
    });
    // Record the drop decision in the report/stats (issue #1388, R4), distinct from
    // the merged inputs so it is assertable from the plan, not just output absence.
    stats.dropped_whole = dropped_whole;

    Ok(CompactReport { output, stats })
}

/// Tally of tombstones GENUINELY PURGED during a partition merge (issue #1037).
///
/// Accumulated only at the true gc_grace / overlap-safe purge decision points in
/// [`KWayMerger::reconcile_cluster_with_overlap_counted`] and
/// [`KWayMerger::merge_partition_rows`] — never from a coarse input-vs-output
/// entry-count diff. Last-write-wins reconciliation collapse (e.g. two duplicate
/// row tombstones merging to one) is deliberately NOT counted, since that is
/// ordinary deduplication rather than a gc/overlap-safe purge. Backs the
/// `cqlite.compaction.tombstones_purged` counter.
#[cfg(feature = "write-support")]
#[derive(Debug, Default, Clone, Copy)]
struct PurgeCounts {
    /// Cell tombstones (simple cells + complex-collection elements) purged.
    cell_tombstones: u64,
    /// Whole-row tombstones purged.
    row_tombstones: u64,
    /// Range-tombstone markers purged.
    range_tombstones: u64,
    /// Complex-deletion (collection/UDT) markers purged.
    complex_deletions: u64,
    /// Partition-level tombstones purged (issue #1072).
    partition_tombstones: u64,
    /// Live cells/rows SHADOWED (suppressed) by a tombstone during reconciliation
    /// (issue #2163). NOT a purge — backs `cqlite.compaction.tombstones_suppressed`
    /// and is deliberately EXCLUDED from [`Self::total`] (the purge total).
    suppressed: u64,
    /// Tombstone markers RETAINED into the merge output (issue #2163). NOT a
    /// purge — backs `cqlite.compaction.tombstones_emitted` and is EXCLUDED from
    /// [`Self::total`].
    emitted: u64,
}

#[cfg(feature = "write-support")]
impl PurgeCounts {
    /// Total tombstones GENUINELY PURGED across every category. The #2163
    /// `suppressed` / `emitted` tallies are intentionally NOT included — they
    /// count shadowing and retention, not purges, so `tombstones_purged`
    /// semantics stay unchanged.
    fn total(self) -> u64 {
        self.cell_tombstones
            .saturating_add(self.row_tombstones)
            .saturating_add(self.range_tombstones)
            .saturating_add(self.complex_deletions)
            .saturating_add(self.partition_tombstones)
    }
}

#[cfg(feature = "write-support")]
impl KWayMerger {
    /// Create a new k-way merger from input SSTable paths
    ///
    /// # Arguments
    ///
    /// * `input_paths` - Paths to input SSTable Data.db files (ordered newest to oldest)
    /// * `schema` - Table schema for schema-aware merging
    ///
    /// # Returns
    ///
    /// A new KWayMerger ready to merge the input SSTables.
    ///
    /// # Errors
    ///
    /// Returns an error if any input SSTable cannot be opened.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let input_paths = vec![
    ///     PathBuf::from("data/nb-1-big-Data.db"),
    ///     PathBuf::from("data/nb-2-big-Data.db"),
    /// ];
    /// let merger = KWayMerger::new(input_paths, &schema)?;
    /// ```
    pub fn new(input_paths: Vec<PathBuf>, schema: &TableSchema) -> Result<Self> {
        Self::new_with_gc(input_paths, schema, None, None)
    }

    /// Like [`KWayMerger::new`], but wires a cooperative
    /// [`ScanCancel`](crate::storage::scan_cancel::ScanCancel) into every input
    /// reader's compaction scan (issue #2264). Used by the Flight `do_get` merge
    /// so a client disconnect abandons an in-flight, index-less full-Data.db walk
    /// within milliseconds instead of the ~1–2 min transport backstop.
    pub fn new_cancellable(
        input_paths: Vec<PathBuf>,
        schema: &TableSchema,
        scan_cancel: crate::storage::scan_cancel::ScanCancel,
    ) -> Result<Self> {
        Self::new_with_gc_and_registry_cancellable(
            input_paths,
            schema,
            None,
            None,
            None,
            scan_cancel,
        )
    }

    /// Create a new k-way merger with explicit purge parameters.
    ///
    /// Identical to [`KWayMerger::new`] but threads an explicit gc_grace cutoff
    /// (`gc_before_secs`) and TTL evaluation time (`now_secs`) into the merge.
    /// This is the deterministic entry point used by `compact_sstables` (the
    /// `cqlite compact` CLI command) and the compaction-parity harness (issue
    /// #842): Cassandra's compaction takes the same `gcBefore`, so purge
    /// decisions cannot diverge between the two engines.
    ///
    /// # Arguments
    ///
    /// * `input_paths` - Paths to input SSTable Data.db files (ordered newest to oldest)
    /// * `schema` - Table schema for schema-aware merging
    /// * `gc_before_secs` - gc_grace cutoff (seconds since epoch), or `None` to not purge
    /// * `now_secs` - "now" (seconds since epoch) for TTL expiry, or `None` for engine default
    #[tracing::instrument(name = "merger.new", level = "debug", skip(input_paths, schema, gc_before_secs, now_secs), fields(inputs = input_paths.len()))]
    pub fn new_with_gc(
        input_paths: Vec<PathBuf>,
        schema: &TableSchema,
        gc_before_secs: Option<i64>,
        now_secs: Option<i64>,
    ) -> Result<Self> {
        Self::new_with_gc_and_registry(input_paths, schema, gc_before_secs, now_secs, None)
    }

    /// Like [`KWayMerger::new_with_gc`], but threads an authoritative
    /// [`UdtRegistry`](crate::schema::UdtRegistry) onto every input SSTable reader
    /// so the compaction read path can decode a top-level `frozen<UDT>` cell
    /// structurally (issue #1234). The one-shot `compact_sstables_with_registry`
    /// and the WriteEngine background compaction pass their configured registry
    /// here; the registry-free `new`/`new_with_gc` paths pass `None`.
    #[tracing::instrument(name = "merger.new_registry", level = "debug", skip(input_paths, schema, gc_before_secs, now_secs, udt_registry), fields(inputs = input_paths.len()))]
    pub fn new_with_gc_and_registry(
        input_paths: Vec<PathBuf>,
        schema: &TableSchema,
        gc_before_secs: Option<i64>,
        now_secs: Option<i64>,
        udt_registry: Option<crate::schema::UdtRegistry>,
    ) -> Result<Self> {
        Self::new_with_gc_and_registry_cancellable(
            input_paths,
            schema,
            gc_before_secs,
            now_secs,
            udt_registry,
            crate::storage::scan_cancel::ScanCancel::default(),
        )
    }

    /// Mark this merge as overlap-safe for tombstone purging (#921 finding 1).
    ///
    /// Set `true` ONLY when the compaction inputs provably span EVERY SSTable
    /// for the table (a major/full compaction), so no non-included overlapping
    /// SSTable can hold data shadowed by a purged tombstone. When `false` (the
    /// default) the gc_grace purge stage is a strict no-op — tombstones are
    /// retained — which can never resurrect data in a partial compaction.
    pub fn with_purge_safe(mut self, purge_safe: bool) -> Self {
        self.purge_safe = purge_safe;
        self
    }

    /// Set the read-time TTL evaluation instant (`now`, epoch seconds) for this
    /// merge (issue #2374/#2789), enabling `expire_ttl_cells` on a READ merge
    /// built through a `now`-less constructor (e.g. the warm
    /// [`new_from_readers`](Self::new_from_readers) path). `gc_before_secs`
    /// stays `None` so NO tombstone is gc-purged — a read reflects deletions, it
    /// does not collect them. A compaction WRITE path threads `now` through its
    /// constructor instead and never calls this.
    #[must_use]
    pub fn with_now_secs(mut self, now_secs: Option<i64>) -> Self {
        self.now_secs = now_secs;
        self
    }

    /// Supply the overlap-aware max-purgeable timestamp for a PARTIAL compaction
    /// (#935, parity with Cassandra `CompactionController.maxPurgeableTimestamp`).
    ///
    /// `max_purgeable_timestamp` is the MINIMUM write timestamp (`markedForDeleteAt`,
    /// micros) across every NON-INCLUDED overlapping SSTable for the table (their
    /// `Statistics.db` min-timestamp bound). With it set, the gc_grace purge stage
    /// additionally purges a tombstone in a partial compaction when the tombstone's
    /// own deletion timestamp is STRICTLY LESS THAN this bound — proving it shadows
    /// nothing outside the compaction set. `None` keeps the conservative #921
    /// behavior (a partial compaction does not purge). Ignored when `purge_safe`
    /// is `true` (a full compaction already has no non-included overlap, so the
    /// effective bound is `+inf`).
    pub fn with_max_purgeable_timestamp(mut self, max_purgeable_timestamp: Option<i64>) -> Self {
        self.max_purgeable_timestamp = max_purgeable_timestamp;
        self
    }

    // `with_egress_slot` (issue #2765) lives in `egress_budget.rs` (its own
    // `impl KWayMerger` block) to keep this over-threshold file from growing
    // (#1116 campsite rule).

    /// Perform a full merge to the output writer
    ///
    /// This is a convenience method that repeatedly calls `step()` until
    /// the merge is complete, writing each partition to the output writer.
    ///
    /// # Arguments
    ///
    /// * `output_writer` - SSTableWriter to write merged output
    ///
    /// # Returns
    ///
    /// Statistics about the merge operation.
    ///
    /// # Errors
    ///
    /// Returns an error if reading or writing fails.
    pub fn merge(
        mut self,
        output_writer: &mut crate::storage::sstable::writer::SSTableWriter,
    ) -> Result<MergeStats> {
        let start_time = Instant::now();
        let mut stats = MergeStats {
            input_files: self.runs.len(),
            output_partitions: 0,
            output_rows: 0,
            bytes_written: 0,
            elapsed: Duration::from_secs(0), // Will be updated at the end
            // The merger only sees the (already drop-filtered) inputs; the caller
            // that computed the drop-set records it (issue #1388).
            dropped_whole: Vec::new(),
        };

        // Stage 5c-iv part 2 (#1668): feed cluster groups DIRECTLY to the
        // incremental writer entry point (part 1) instead of assembling a
        // whole `Vec<Mutation>` and calling `write_partition` once. The
        // (bounded, partition-size-independent) `clustering_key: None`
        // prefix — a static-row carrier and/or range/partition-tombstone
        // carriers — is ALWAYS emitted before any `Some(ck)` row (proven in
        // `streaming.rs`'s `static_row_carrier_always_sorts_first_
        // regardless_of_partition_width` test), so it is buffered just long
        // enough to resolve the partition tombstone / range-tombstones /
        // static ops that `begin_partition_incremental`/`feed_static_row`
        // need UPFRONT. Every subsequent `Some(ck)` row streams straight
        // through `feed_row` with NO `Vec<Mutation>` accumulation.
        //
        // TWO DISTINCT schemas are in play, and mixing them up silently
        // corrupts the output (found via the real #1019 2-generation
        // dropped-column fixture, which no single-run synthetic unit test
        // exercised): `self.schema` is this merger's DECODE-time schema
        // (e.g. `compact_sstables_with_registry`'s `effective_schema`, which
        // still declares a fully-purged dropped column so its cells can be
        // decoded and purge-evaluated) — used ONLY for
        // `merge_entry_to_mutation`. `output_writer`'s OWN schema is the
        // ENCODE-time `write_schema` — the header/column layout actually
        // committed to Data.db (with fully-purged dropped columns already
        // stripped) — required by every `feed_row`/`feed_static_row`/
        // `finish` call, exactly as `write_partition` always used its own
        // `&self.schema` internally rather than a caller-decoded schema.
        let decode_schema = self.schema.clone();
        let write_schema = output_writer.schema().clone();
        let schema_has_static = write_schema.columns.iter().any(|c| c.is_static);
        let mut stream = StreamingMerger::new(&mut self);

        // Outer loop: one iteration per PARTITION, with all partition-scoped
        // state (including `session`, which borrows both `output_writer` and
        // `range_tombstones`) declared FRESH each iteration. This is
        // required, not stylistic: a `session` variable declared ONCE
        // outside this loop and reassigned across partitions would force
        // the borrow checker to unify ITS lifetime parameters across EVERY
        // partition, making `range_tombstones`/`output_writer` look
        // borrowed for the REST OF THE FUNCTION instead of just the current
        // partition. Re-declaring per outer iteration gives each
        // partition's borrows their own independent, iteration-scoped
        // lifetime.
        'partitions: loop {
            let mut partition_tombstone: Option<PartitionTombstone> = None;
            let mut range_tombstones: Vec<RangeTombstone> = Vec::new();
            let mut static_tracker =
                crate::storage::sstable::writer::data_writer::StaticOpsTracker::new();
            let mut static_first_ts: i64 = 0;
            let mut saw_carrier_or_static = false;
            let mut row_count: u64 = 0;
            // Issue #1383 regression fix (crit2-4), issue #1668: buffer this
            // partition's surviving `Some(ck)` row mutations rather than
            // streaming each straight into the writer session as it arrives.
            //
            // The streaming writer session (`begin_partition_incremental`)
            // needs the partition's COMPLETE range-tombstone set UPFRONT: it
            // sorts the range bounds into on-disk markers and interleaves them
            // with rows in clustering order as each row is fed (and uses them
            // to compute each row's `shadow_floor`). But `StreamingMerger`
            // only surfaces a range tombstone's coalesced marker once its
            // CLOSE bound is parsed — which, for a range whose covered rows
            // live in a different (e.g. newer) generation, is strictly AFTER
            // those rows have already streamed past (issue #1668 stage 5d's
            // "range tombstones can arrive AFTER the rows they cover"
            // finding). Opening the session on the first `Some(ck)` row
            // therefore fixed an EMPTY range-tombstone set, then fed every
            // late marker mutation through `feed_row` — which silently dropped
            // it (a range-only mutation has no row content) — so a synthesized
            // boundary vanished from the compacted output entirely.
            //
            // Buffering the rows until PartitionEnd lets us open the session
            // with the FULL, final range-tombstone set, so `feed_row` shadows
            // every covered row correctly AND interleaves the markers in
            // clustering order. A genuinely bounded-memory streaming WRITE for
            // range-tombstone partitions would need the out-of-scope two-pass
            // reader that surfaces a range's OPEN bound early; this buffer is
            // whole-partition-width for such partitions (the reader already
            // materializes the decompressed section anyway — see the
            // streaming dhat test's module doc), while `StreamingMerger`'s
            // own reconciliation stays row-streamed (the memory bound the
            // dhat proof actually exercises).
            let mut buffered_rows: Vec<crate::storage::write_engine::mutation::Mutation> =
                Vec::new();
            let mut partition_stats = crate::storage::sstable::writer::StatisticsMetadata::new();

            loop {
                match stream.step_streaming()? {
                    StreamingStep::ClusterGroup { key: _, row } => {
                        // Skip truly-empty phantom entries (#886/#899
                        // branch-review) — mirrors the original
                        // `.filter(|entry| !entry.is_metadata_only_no_op())`.
                        if row.is_metadata_only_no_op() {
                            continue;
                        }
                        let mutation = Self::merge_entry_to_mutation(*row, &decode_schema)?;
                        // Single fold point for EVERY mutation of this
                        // partition (carrier, static, or clustered row) —
                        // mirrors `write_partition`'s
                        // `for mutation in &mutations { fold... }` loop,
                        // which folds unconditionally regardless of
                        // classification.
                        crate::storage::sstable::writer::stats_fold::fold_mutation_stats(
                            &mut partition_stats,
                            &mutation,
                        );

                        // Classify the (None-keyed) carriers and the static
                        // row; everything else is a real clustering row that
                        // is BUFFERED (see `buffered_rows`' doc) — range
                        // tombstones can still arrive after some rows have
                        // already been buffered, which is exactly why the
                        // whole set must be resolved before the session opens.
                        let is_partition_only = mutation.operations.is_empty()
                            && mutation.partition_tombstone.is_some()
                            && mutation.row_tombstone.is_none()
                            && mutation.range_tombstones.is_empty();
                        let is_range_only = mutation.operations.is_empty()
                            && mutation.partition_tombstone.is_none()
                            && mutation.row_tombstone.is_none()
                            && !mutation.range_tombstones.is_empty();

                        if is_partition_only {
                            partition_tombstone = mutation.partition_tombstone;
                            saw_carrier_or_static = true;
                            continue;
                        }
                        if is_range_only {
                            range_tombstones.extend(mutation.range_tombstones.iter().cloned());
                            saw_carrier_or_static = true;
                            continue;
                        }
                        // A `clustering_key: None` mutation is the resolved
                        // static-row carrier ONLY when the schema actually
                        // declares static columns — Cassandra disallows
                        // static columns on a table with no clustering
                        // columns, so for an UNCLUSTERED table EVERY row
                        // (not just a special carrier) uses
                        // `clustering_key: None` too (mirrors
                        // `write_partition_with_index_blocks`'s own
                        // `schema_has_static` gate: without it, that
                        // table's sole per-partition row would be
                        // misrouted here and silently dropped instead of
                        // written as a real row).
                        if mutation.clustering_key.is_none() && schema_has_static {
                            // The resolved static-row carrier (issue #1668
                            // design verification: at most one, always
                            // first). Counted toward `output_rows` (issue
                            // #1238's original `row_mutations` filter
                            // counts ANY mutation with non-empty
                            // `operations` — a static mutation always has
                            // some — so this mirrors that exactly, not
                            // just the Some(ck) rows).
                            if !saw_carrier_or_static {
                                static_first_ts = mutation.timestamp_micros;
                            }
                            static_tracker.feed(&mutation, &write_schema, None);
                            saw_carrier_or_static = true;
                            row_count += 1;
                            continue;
                        }

                        // A real clustering row (or an unclustered table's
                        // sole `clustering_key: None` row): buffer it for the
                        // single PartitionEnd write once the full
                        // range-tombstone set is known.
                        buffered_rows.push(mutation);
                        row_count += 1;
                    }
                    StreamingStep::PartitionEnd { key } => {
                        // Write the whole partition in ONE session now that
                        // the partition tombstone, the COMPLETE coalesced
                        // range-tombstone set, the static row, and every
                        // surviving clustering row are all known. Skip a
                        // truly empty partition (every entry was
                        // metadata-only-no-op), matching the original
                        // `mutations.is_empty()` skip. A partition with only
                        // carriers/statics but no `Some(ck)` row is still
                        // emittable (#933/#1072).
                        if !buffered_rows.is_empty() || saw_carrier_or_static {
                            let mut session = output_writer.begin_partition_incremental(
                                &key,
                                partition_tombstone.as_ref(),
                                &range_tombstones,
                            )?;
                            if schema_has_static {
                                let merged = std::mem::take(&mut static_tracker).finish();
                                session.feed_static_row(&merged, static_first_ts, &write_schema)?;
                            }
                            for mutation in &buffered_rows {
                                session.feed_row(mutation, &write_schema)?;
                            }
                            let (offset, blocks, emit) = session.finish(&write_schema)?;
                            output_writer.complete_partition_incremental(
                                &key,
                                partition_tombstone.as_ref(),
                                offset,
                                &blocks,
                                emit,
                                &partition_stats,
                            )?;
                            stats.output_partitions += 1;
                            stats.output_rows += row_count;
                        }
                        continue 'partitions;
                    }
                    StreamingStep::Complete => break 'partitions,
                }
            }
        }

        // Issue #1238: report the REAL output Data.db byte count instead of a
        // hardcoded 0. The streaming writer flushes each partition as it is
        // written, so its running position is the authoritative total Data.db
        // size — identical to the `data_size` the caller's later
        // `writer.finish()` reports (only residual scratch, normally empty, is
        // flushed there). This is the writer's own byte accounting, not an
        // estimate or a re-stat of the produced file.
        stats.bytes_written = output_writer.data_bytes_written();

        stats.elapsed = start_time.elapsed();
        Ok(stats)
    }

    /// Perform one merge step (one partition)
    ///
    /// Returns the next merged partition, or Complete if the merge is done.
    /// This allows incremental merging for better memory control.
    ///
    /// # Returns
    ///
    /// - `MergeStep::Partition` - Next merged partition with all its rows
    /// - `MergeStep::Complete` - Merge is complete
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    #[tracing::instrument(name = "merger.step", level = "debug", skip(self))]
    pub fn step(&mut self) -> Result<MergeStep> {
        // Initialize heap on first call
        if self.heap.is_empty() && self.current_partition.is_none() {
            self.initialize_heap()?;
        }

        // If heap is empty, merge is complete
        if self.heap.is_empty() {
            return Ok(MergeStep::Complete);
        }

        // Collect all rows for the next partition
        let mut partition_rows = Vec::new();
        let mut partition_key: Option<DecoratedKey> = None;

        while let Some(Reverse(wrapped)) = self.heap.peek() {
            // Check if we've moved to a new partition
            if let Some(ref current_key) = partition_key {
                if &wrapped.entry.key != current_key {
                    // Partition boundary - stop here
                    break;
                }
            } else {
                // First entry of new partition
                partition_key = Some(wrapped.entry.key.clone());
            }

            // Pop entry from heap
            let Reverse(wrapped) = self
                .heap
                .pop()
                .ok_or_else(|| Error::InvalidInput("Merge heap unexpectedly empty".to_string()))?;
            let entry = wrapped.entry;
            // Read the Copy `run_index` before moving `entry` (issue #1664: the
            // entry is already owned, so move it into partition_rows instead of
            // cloning).
            let run_index = entry.run_index;

            // Add to partition rows
            partition_rows.push(entry);

            // Refill heap from the run we just consumed from
            self.refill_heap(run_index)?;
        }

        if let Some(key) = partition_key {
            // Merge cells within this partition (last-write-wins)
            let merged_rows = self.merge_partition_rows(partition_rows)?;
            Ok(MergeStep::Partition {
                key,
                rows: merged_rows,
            })
        } else {
            Ok(MergeStep::Complete)
        }
    }

    /// Initialize the heap with the first entry from each run
    fn initialize_heap(&mut self) -> Result<()> {
        for run_index in 0..self.runs.len() {
            self.refill_heap(run_index)?;
        }
        Ok(())
    }

    /// Refill the heap from a specific run
    fn refill_heap(&mut self, run_index: usize) -> Result<()> {
        if run_index >= self.runs.len() {
            return Ok(());
        }

        let run = &mut self.runs[run_index];
        if !run.is_exhausted() {
            if let Some(entry) = run.advance()? {
                // Move the owned entry into the heap, paired with the
                // schema-aware comparator's schema (issue #1668, stage 5c-i).
                // Issue #1664: `advance()` returns the OWNED front entry, so we
                // move it in instead of the former peek+clone+discard-advance.
                self.heap
                    .push(Reverse(schema_order::SchemaOrderedEntry::new(
                        entry,
                        self.schema_arc.clone(),
                    )));
            }
        }

        Ok(())
    }

    /// Merge rows within a single partition using **per-cell reconcile**
    /// (Cassandra `org.apache.cassandra.db.rows.Cells#reconcile`).
    ///
    /// The pre-#533 implementation selected a single whole winning `MergeEntry`
    /// per clustering key, which DROPPED columns when two SSTables shared the same
    /// `(pk, ck)` but carried DISJOINT columns (e.g. A→{name}, B→{score} merged to
    /// only B's column). This now reconciles cell-by-cell so disjoint columns from
    /// every input survive (Issue #533).
    ///
    /// Algorithm per clustering-key group:
    ///   1. **Effective row deletion** — among `RowData::Tombstone` entries take the
    ///      max `deletion_time` (`row_del`). A row tombstone shadows any cell whose
    ///      `timestamp <= row_del`.
    ///   2. **Per-column cell reconcile** — across all `RowData::Live` entries, for
    ///      each column name pick the winning cell by:
    ///        - higher `timestamp` wins (last-write-wins);
    ///        - at EQUAL timestamp a cell tombstone (`Value::Tombstone(CellTombstone)`)
    ///          beats a live value (same rule as #498, applied per cell);
    ///        - otherwise the existing winner is kept (stable; heap routing already
    ///          ordered inputs by run_index so the first-seen at a tie is the newer
    ///          file).
    ///   3. **Row-tombstone shadowing per cell** — drop any reconciled cell whose
    ///      `timestamp <= row_del`. The `<=` makes the tombstone win at equal ts,
    ///      consistent with #498. Cells written strictly AFTER `row_del` survive.
    ///   4. **Build the merged result** — if any cells survive, emit a `Live`
    ///      entry whose row timestamp is the max surviving cell timestamp; else if a
    ///      row tombstone was present, emit a `Tombstone` entry at `row_del` so the
    ///      row stays shadowed downstream; else emit nothing.
    ///
    /// Overlap-safety gate for tombstone purging (#921 finding 1, #935):
    /// decide BOTH the effective gc_grace cutoff and the overlap-aware
    /// max-purgeable timestamp every cluster in this merger's output is
    /// reconciled with. Constant for this `KWayMerger`'s whole lifetime
    /// (derived only from its own `purge_safe`/`gc_before_secs`/
    /// `max_purgeable_timestamp` fields, set once at construction) — callers
    /// may compute it once and reuse it across every partition, rather than
    /// re-deriving it per partition.
    ///
    /// Extracted from [`Self::merge_partition_rows`] (issue #1668 stage 5d)
    /// so the streaming reconciliation path (`streaming.rs`) can reuse the
    /// EXACT same derivation without duplicating it.
    ///
    /// - FULL compaction (`purge_safe == true`): no non-included overlapping
    ///   SSTable exists, so the overlap bound is `+inf` (`i64::MAX`) — every
    ///   gc-purgeable tombstone passes the overlap gate, exactly as #845.
    /// - PARTIAL compaction with an overlap bound (#935): purge a tombstone
    ///   only when its own deletion timestamp is STRICTLY LESS THAN the min
    ///   write timestamp of every non-included overlapping SSTable, proving
    ///   it shadows nothing outside the set.
    /// - PARTIAL compaction without a bound (#921 default): retain every
    ///   tombstone — collapse the cutoff to `None` (purge no-op).
    fn effective_gc_settings(&self) -> (Option<i64>, i64) {
        if self.purge_safe {
            (self.gc_before_secs, i64::MAX)
        } else if let Some(bound) = self.max_purgeable_timestamp {
            (self.gc_before_secs, bound)
        } else {
            (None, i64::MIN)
        }
    }

    fn merge_partition_rows(&self, rows: Vec<MergeEntry>) -> Result<Vec<MergeEntry>> {
        use std::collections::BTreeMap;

        // Tombstone-purge accounting (issue #1037). Accumulated at the ACTUAL
        // gc/overlap-safe purge decision points (not derived from an input-vs-
        // output entry-count diff, which both missed cell/complex purges and
        // mis-counted last-write-wins collapse as a purge). See the comment on
        // `COMPACTION_TOMBSTONES_PURGED` emission at the end of this method.
        let mut purges = PurgeCounts::default();

        // Overlap-safety gate for tombstone purging (#921 finding 1, #935):
        // decide BOTH the effective gc_grace cutoff and the overlap-aware
        // max-purgeable timestamp this cluster is reconciled with.
        //
        // - FULL compaction (`purge_safe == true`): no non-included overlapping
        //   SSTable exists, so the overlap bound is `+inf` (`i64::MAX`) — every
        //   gc-purgeable tombstone passes the overlap gate, exactly as #845.
        // - PARTIAL compaction with an overlap bound (#935): purge a tombstone
        //   only when its own deletion timestamp is STRICTLY LESS THAN the min
        //   write timestamp of every non-included overlapping SSTable, proving it
        //   shadows nothing outside the set.
        // - PARTIAL compaction without a bound (#921 default): retain every
        //   tombstone — collapse the cutoff to `None` (purge no-op).
        //
        // Issue #933 builds `clustered_rows` (and splits out range-tombstone
        // carriers) AFTER this gate, below.
        let (effective_gc_before, max_purgeable_timestamp) = self.effective_gc_settings();

        // Partition-scoped tombstone-carrier pre-scan (issue #1668, stage 1).
        // A standalone read-only first pass extracts the partition-level
        // markers — the range-tombstone carriers (#933) and the MAX
        // partition-deletion floor (#1072) — out of the buffered partition into
        // `PartitionCarriers`. This preserves the exact prior behavior:
        //
        //   * Issue #933: range-tombstone carriers are partition-level markers
        //     spanning a clustering range — NOT per-`(pk, ck)` rows — so routing
        //     them through `reconcile_cluster` (which collapses a cluster to ONE
        //     entry) would lose every range but one. Collected here, then
        //     canonicalized (below), then used to (a) shadow covered cells per
        //     cluster and (b) re-emit the survivors so the writer persists them.
        //   * Issue #1072: the synthetic partition-tombstone carriers yield the
        //     MAX `markedForDeleteAt` across ALL sources (with the winning mfda's
        //     localDeletionTime) — the partition's OUTERMOST shadow floor. The
        //     partition key is the same for every entry in this call
        //     (`merge_partition_rows` is per partition), so one floor covers all.
        //
        // Carriers are NOT pushed into `clustered_rows`; the buffering pass below
        // skips them and keeps only the per-`(pk, ck)` rows, exactly as before.
        let carriers::PartitionCarriers {
            mut range_tombstones,
            max_partition_deletion,
            partition_delete_key,
        } = carriers::scan_partition_carriers(&rows);

        let mut clustered_rows: BTreeMap<Option<ClusteringKey>, Vec<MergeEntry>> = BTreeMap::new();

        // Row-count reconciliation (issue #2163): rows entering the reconcile
        // boundary. `rows` is moved by the loop below, so snapshot the count here.
        // Emitted once per merge alongside `rows_out` (`merged.len()`) so the
        // in/out delta is exactly the rows removed by reconciliation.
        let rows_in = rows.len() as u64;

        for row in rows {
            // Skip the partition-scoped carriers already captured above so only
            // per-`(pk, ck)` rows enter `clustered_rows` (mirrors the original
            // inline `continue`s exactly: partition-deletion carrier first, then
            // range-marker carrier).
            if row.is_partition_delete_carrier() && row.partition_deletion.is_some() {
                continue;
            }
            if carriers::is_range_marker_carrier(&row) {
                continue;
            }
            clustered_rows
                .entry(row.clustering_key.clone())
                .or_default()
                .push(row);
        }

        // Coalesce the cross-SSTable union into a NON-OVERLAPPING canonical
        // sequence per partition (newest deletion wins per covered segment). The
        // writer emits each range as an independent open/close marker pair and the
        // reader pairs them with a single pending-start, so overlapping re-emitted
        // ranges would corrupt the persisted ranges on read-back (roborev #959
        // High #1). This also subsumes the old identical-bounds dedup.
        Self::coalesce_range_tombstones(&mut range_tombstones, &self.schema);

        let mut merged = Vec::new();
        for (ck, cluster_rows) in clustered_rows {
            if let Some(entry) = Self::reconcile_cluster_with_overlap_counted(
                ck,
                cluster_rows,
                &self.schema.dropped_columns,
                effective_gc_before,
                max_purgeable_timestamp,
                // #1382: TTL expiry uses the merger's pinned evaluation instant,
                // the SAME `now` that drove `compute_gc_before` so expiry and
                // gc-grace purging agree deterministically.
                self.now_secs,
                &mut purges,
            ) {
                // Issue #933: shadow cells covered by a range tombstone. The merge
                // must do this per-cell (not just per-row at the writer) because a
                // reconciled row's simple cells lose their individual writetimes
                // when converted to a mutation — only the merge still sees each
                // `CellData.timestamp`.
                if let Some(shadowed) =
                    Self::apply_range_shadowing(entry, &range_tombstones, &self.schema)
                {
                    // Issue #1072: apply the partition deletion as the OUTERMOST
                    // floor — after range shadowing — dropping every cell/row
                    // whose timestamp is `<= pmfda` (per-cell `<=`, so strictly-
                    // newer cells survive). A whole-row covered by the partition
                    // floor contributes nothing (the re-emitted partition
                    // tombstone covers it).
                    if let Some(survivor) =
                        Self::apply_partition_shadowing(shadowed, max_partition_deletion)
                    {
                        merged.push(survivor);
                    }
                }
            }
        }

        // Issue #1072: a range tombstone older-or-equal to the partition floor is
        // subsumed by the partition deletion and must not be re-emitted (it would
        // be redundant). A strictly-newer range marker survives. Apply BEFORE the
        // range re-emit loop below.
        if let Some((pmfda, _)) = max_partition_deletion {
            range_tombstones.retain(|(_, rt)| rt.deletion_time > pmfda);
        }

        // Re-emit the surviving range tombstones as carrier entries so the writer
        // persists the markers (otherwise the shadowing above would resurrect the
        // covered rows from a non-compacted SSTable — issue #933 / roborev #959
        // High #2). gc_grace purges a marker only when BOTH conditions hold,
        // matching the cell/row/complex purge paths (issue #1061):
        //   1. the marker's `localDeletionTime` is strictly below `gcBefore`
        //      (gc-grace expired — coordinate with #845), AND
        //   2. the marker's own deletion timestamp is strictly below
        //      `max_purgeable_timestamp` (the min write timestamp of every
        //      non-included overlapping SSTable — #935), proving it shadows
        //      nothing outside the compaction set.
        // For a full / overlap-safe compaction `max_purgeable_timestamp` is
        // `i64::MAX`, which lets every gc-purgeable marker through unchanged for
        // any realistic deletion timestamp (the same strict-`<`-against-`i64::MAX`
        // convention the cell/row/complex gates use above). For a PARTIAL
        // compaction with a finite overlap bound,
        // condition 2 RETAINS a marker at/above the bound — without it the
        // marker could be dropped while still shadowing data in a non-included
        // SSTable, resurrecting that data (issue #1061).
        for (key, rt) in range_tombstones {
            if let Some(gc_before) = effective_gc_before {
                if i64::from(rt.local_deletion_time as u32) < gc_before
                    && rt.deletion_time < max_purgeable_timestamp
                {
                    // Count the range-tombstone marker purge (issue #1037). The
                    // DROP is now overlap-safe: gated on gc_before AND the
                    // overlap bound, like the other tombstone categories (#1061).
                    purges.range_tombstones += 1;
                    continue;
                }
            }
            // Retained (non-purged) range-tombstone marker carried into the output
            // (issue #2163): a tombstone marker emitted, not purged.
            purges.emitted += 1;
            merged.push(
                MergeEntry::new(
                    usize::MAX,
                    key,
                    None,
                    rt.deletion_time,
                    RowData::Live { cells: Vec::new() },
                )
                .with_range_deletion(rt),
            );
        }

        // Issue #1072: re-emit the surviving partition tombstone so even a
        // tombstone-only partition (no rows in any source) still emits a partition
        // deletion — and the deletion keeps shadowing older rows in non-compacted
        // SSTables. gc_grace purges the marker only when BOTH conditions hold,
        // IDENTICAL to the range-marker re-emit gate / #1061 idiom:
        //   1. the partition's `localDeletionTime` is strictly below `gcBefore`
        //      (gc-grace expired), AND
        //   2. its own `markedForDeleteAt` is strictly below
        //      `max_purgeable_timestamp` (the min write timestamp of every
        //      non-included overlapping SSTable — #935), proving it shadows
        //      nothing outside the compaction set.
        // For a full / overlap-safe compaction `max_purgeable_timestamp` is
        // `i64::MAX` so any realistic deletion passes condition 2. For a PARTIAL
        // compaction without an overlap bound `max_purgeable_timestamp` is
        // `i64::MIN`, so condition 2 never fires and the tombstone is always
        // retained (never resurrect).
        if let (Some((pmfda, pldt)), Some(key)) = (max_partition_deletion, partition_delete_key) {
            let purge = match effective_gc_before {
                Some(gc_before) => {
                    i64::from(pldt as u32) < gc_before && pmfda < max_purgeable_timestamp
                }
                None => false,
            };
            if purge {
                purges.partition_tombstones += 1;
            } else {
                // Retained partition tombstone marker carried into the output
                // (issue #2163): a tombstone marker emitted, not purged.
                purges.emitted += 1;
                merged.push(
                    MergeEntry::new(
                        usize::MAX,
                        key,
                        None,
                        pmfda,
                        RowData::Tombstone {
                            deletion_time: pmfda,
                            local_deletion_time: pldt,
                        },
                    )
                    .with_partition_deletion((pmfda, pldt)),
                );
            }
        }

        // Sort merged rows by clustering key for output order
        merged.sort_by(|a, b| match (&a.clustering_key, &b.clustering_key) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(ck_a), Some(ck_b)) => {
                // Use schema-aware comparison if available
                ck_a.compare(ck_b, &self.schema).unwrap_or_else(|e| {
                    tracing::warn!(
                        "Schema-aware clustering key comparison failed, using fallback: {}",
                        e
                    );
                    ck_a.cmp(ck_b)
                })
            }
        });

        // Emit the count of tombstones GENUINELY PURGED during this merge (issue
        // #1037).
        //
        // `COMPACTION_TOMBSTONES_PURGED` now counts ONLY true gc_grace /
        // overlap-safe purges, accumulated at the exact decision points where a
        // tombstone is dropped because it is safe to drop:
        //   - cell tombstones (simple + complex-element) removed by the Step 3c
        //     gc/overlap retain in `reconcile_cluster_with_overlap_counted`;
        //   - row tombstones cleared by that same gc/overlap gate;
        //   - complex-deletion markers removed by that gate;
        //   - range-tombstone markers dropped by the gc-grace gate in the
        //     re-emit loop above.
        // It explicitly does NOT count last-write-wins reconciliation collapse
        // (e.g. two duplicate row tombstones merging to one), which is ordinary
        // deduplication, not a purge. When no gc cutoff applies (partial
        // compaction without an overlap bound) the purge stages are no-ops and
        // this counter stays at zero.
        let purged = purges.total();
        if purged > 0 {
            crate::observability::add_counter(
                crate::observability::catalog::COMPACTION_TOMBSTONES_PURGED,
                purged,
                &[],
            );
        }

        // Correctness / silent-miss signals (issue #2163), emitted once per merge
        // alongside the purge total — never per row/cell. `suppressed` (live data
        // shadowed by a tombstone) and `emitted` (tombstone markers retained) move
        // independently of `tombstones_purged`; the row-count pair lets a dashboard
        // see reconciliation drop ratio + volume.
        if purges.suppressed > 0 {
            crate::observability::add_counter(
                crate::observability::catalog::COMPACTION_TOMBSTONES_SUPPRESSED,
                purges.suppressed,
                &[],
            );
        }
        if purges.emitted > 0 {
            crate::observability::add_counter(
                crate::observability::catalog::COMPACTION_TOMBSTONES_EMITTED,
                purges.emitted,
                &[],
            );
        }
        if rows_in > 0 {
            crate::observability::add_counter(
                crate::observability::catalog::MERGE_ROWS_IN,
                rows_in,
                &[],
            );
            crate::observability::add_counter(
                crate::observability::catalog::MERGE_ROWS_OUT,
                merged.len() as u64,
                &[],
            );
        }

        Ok(merged)
    }

    /// Coalesce range tombstones into a NON-OVERLAPPING canonical sequence per
    /// partition, the winning (newest) deletion time per covered segment (issue
    /// #933 / roborev #959 High #1).
    ///
    /// Each input SSTable's range tombstones are individually non-overlapping
    /// (Cassandra's `RangeTombstoneList` invariant), but the cross-SSTable union
    /// gathered during compaction can overlap with different bounds. The writer
    /// emits each retained range as an independent open/close marker pair and the
    /// reader pairs them with a single `pending_range_start`, so OVERLAPPING
    /// re-emitted ranges would mis-pair on read-back and corrupt the persisted
    /// ranges. Splitting the union into disjoint segments (each carrying the max
    /// `markedForDeleteAt` covering it) keeps the on-disk markers a clean
    /// alternating open/close sequence. This also subsumes the prior
    /// identical-bounds dedup.
    fn coalesce_range_tombstones(
        rts: &mut Vec<(DecoratedKey, RangeTombstone)>,
        schema: &TableSchema,
    ) {
        // Group by partition-key bytes, preserving the first-seen DecoratedKey as
        // the representative for each group (token + raw bytes are identical
        // across the group).
        let mut groups: Vec<(DecoratedKey, Vec<RangeTombstone>)> = Vec::new();
        for (key, rt) in rts.drain(..) {
            if let Some((_, ranges)) = groups.iter_mut().find(|(k, _)| k.key == key.key) {
                ranges.push(rt);
            } else {
                groups.push((key, vec![rt]));
            }
        }

        let mut out: Vec<(DecoratedKey, RangeTombstone)> = Vec::new();
        for (key, ranges) in groups {
            for rt in Self::coalesce_partition_range_tombstones(ranges, schema) {
                out.push((key.clone(), rt));
            }
        }
        *rts = out;
    }

    /// Coalesce the range tombstones of a SINGLE partition into a disjoint,
    /// clustering-sorted sequence (helper for [`Self::coalesce_range_tombstones`]).
    fn coalesce_partition_range_tombstones(
        ranges: Vec<RangeTombstone>,
        schema: &TableSchema,
    ) -> Vec<RangeTombstone> {
        if ranges.len() <= 1 {
            return ranges;
        }

        // Model each range as a closed interval [start_cut, end_cut] with its
        // (mfda, ldt).
        let items: Vec<(RangeCut, RangeCut, i64, i32)> = ranges
            .iter()
            .map(|rt| {
                (
                    Self::range_start_cut(&rt.start),
                    Self::range_end_cut(&rt.end),
                    rt.deletion_time,
                    rt.local_deletion_time,
                )
            })
            .collect();

        // Distinct, sorted cut positions (the candidate segment boundaries).
        let mut cuts: Vec<RangeCut> = Vec::with_capacity(items.len() * 2);
        for (s, e, _, _) in &items {
            cuts.push(s.clone());
            cuts.push(e.clone());
        }
        cuts.sort_by(|a, b| Self::cut_cmp(a, b, schema));
        cuts.dedup_by(|a, b| Self::cut_cmp(a, b, schema) == Ordering::Equal);

        // For each elementary gap (cuts[i], cuts[i+1]), the winning deletion is
        // the max `markedForDeleteAt` among ranges whose closed interval fully
        // contains the gap (start <= lo AND hi <= end).
        let mut segs: Vec<(RangeCut, RangeCut, i64, i32)> = Vec::new();
        for window in cuts.windows(2) {
            let (lo, hi) = (&window[0], &window[1]);
            let mut best: Option<(i64, i32)> = None;
            for (s, e, mfda, ldt) in &items {
                let covers = Self::cut_cmp(s, lo, schema) != Ordering::Greater
                    && Self::cut_cmp(hi, e, schema) != Ordering::Greater;
                if !covers {
                    continue;
                }
                best = Some(match best {
                    Some((bm, bl)) if bm > *mfda || (bm == *mfda && bl >= *ldt) => (bm, bl),
                    _ => (*mfda, *ldt),
                });
            }
            if let Some((mfda, ldt)) = best {
                segs.push((lo.clone(), hi.clone(), mfda, ldt));
            }
        }

        // Merge adjacent segments that share a boundary AND the same deletion
        // (minimal fragmentation); a gap with no covering range breaks the run.
        let mut merged: Vec<(RangeCut, RangeCut, i64, i32)> = Vec::new();
        for seg in segs {
            if let Some(last) = merged.last_mut() {
                if last.2 == seg.2
                    && last.3 == seg.3
                    && Self::cut_cmp(&last.1, &seg.0, schema) == Ordering::Equal
                {
                    last.1 = seg.1;
                    continue;
                }
            }
            merged.push(seg);
        }

        merged
            .into_iter()
            .map(|(lo, hi, mfda, ldt)| RangeTombstone {
                start: Self::cut_to_start_bound(lo),
                end: Self::cut_to_end_bound(hi),
                deletion_time: mfda,
                local_deletion_time: ldt,
            })
            .collect()
    }

    /// Total order of two cut positions on the clustering axis (schema-aware,
    /// honoring per-column ASC/DESC and Cassandra's kind-weighted prefix
    /// ordering). See [`RangeCut`].
    fn cut_cmp(a: &RangeCut, b: &RangeCut, schema: &TableSchema) -> Ordering {
        match (a, b) {
            (RangeCut::Bottom, RangeCut::Bottom) => Ordering::Equal,
            (RangeCut::Bottom, _) => Ordering::Less,
            (_, RangeCut::Bottom) => Ordering::Greater,
            (RangeCut::Top, RangeCut::Top) => Ordering::Equal,
            (RangeCut::Top, _) => Ordering::Greater,
            (_, RangeCut::Top) => Ordering::Less,
            (RangeCut::At { key: ka, after: aa }, RangeCut::At { key: kb, after: ab }) => {
                // Compare the common prefix only; a shorter prefix's `after` flag
                // decides its position relative to every longer extension.
                let l = ka.columns.len().min(kb.columns.len());
                let ta = ClusteringKey {
                    columns: ka.columns[..l].to_vec(),
                };
                let tb = ClusteringKey {
                    columns: kb.columns[..l].to_vec(),
                };
                let ord = ta.compare(&tb, schema).unwrap_or_else(|_| ta.cmp(&tb));
                if ord != Ordering::Equal {
                    return ord;
                }
                match ka.columns.len().cmp(&kb.columns.len()) {
                    Ordering::Equal => aa.cmp(ab),
                    // `a` is the shorter prefix: just-after sorts past every
                    // extension of it, just-before sorts ahead of all of them.
                    Ordering::Less => {
                        if *aa {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    // `b` is the shorter prefix (mirror image).
                    Ordering::Greater => {
                        if *ab {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                }
            }
        }
    }

    /// Left edge (cut) of a range's start bound.
    fn range_start_cut(
        bound: &crate::storage::write_engine::mutation::ClusteringBound,
    ) -> RangeCut {
        use crate::storage::write_engine::mutation::ClusteringBound;
        match bound {
            ClusteringBound::Inclusive(ck) => RangeCut::At {
                key: ck.clone(),
                after: false,
            },
            ClusteringBound::Exclusive(ck) => RangeCut::At {
                key: ck.clone(),
                after: true,
            },
            ClusteringBound::Bottom => RangeCut::Bottom,
            ClusteringBound::Top => RangeCut::Top,
        }
    }

    /// Right edge (cut) of a range's end bound.
    fn range_end_cut(bound: &crate::storage::write_engine::mutation::ClusteringBound) -> RangeCut {
        use crate::storage::write_engine::mutation::ClusteringBound;
        match bound {
            ClusteringBound::Inclusive(ck) => RangeCut::At {
                key: ck.clone(),
                after: true,
            },
            ClusteringBound::Exclusive(ck) => RangeCut::At {
                key: ck.clone(),
                after: false,
            },
            ClusteringBound::Top => RangeCut::Top,
            ClusteringBound::Bottom => RangeCut::Bottom,
        }
    }

    /// Convert a left-edge cut back into a start [`ClusteringBound`].
    fn cut_to_start_bound(
        cut: RangeCut,
    ) -> crate::storage::write_engine::mutation::ClusteringBound {
        use crate::storage::write_engine::mutation::ClusteringBound;
        match cut {
            RangeCut::Bottom => ClusteringBound::Bottom,
            RangeCut::At { key, after: false } => ClusteringBound::Inclusive(key),
            RangeCut::At { key, after: true } => ClusteringBound::Exclusive(key),
            RangeCut::Top => ClusteringBound::Top,
        }
    }

    /// Convert a right-edge cut back into an end [`ClusteringBound`].
    fn cut_to_end_bound(cut: RangeCut) -> crate::storage::write_engine::mutation::ClusteringBound {
        use crate::storage::write_engine::mutation::ClusteringBound;
        match cut {
            RangeCut::Top => ClusteringBound::Top,
            RangeCut::At { key, after: true } => ClusteringBound::Inclusive(key),
            RangeCut::At { key, after: false } => ClusteringBound::Exclusive(key),
            RangeCut::Bottom => ClusteringBound::Bottom,
        }
    }

    /// Whether a range tombstone's clustering range covers `ck`, comparing bounds
    /// SCHEMA-AWARE (honoring per-column ASC/DESC via [`ClusteringKey::compare`],
    /// NOT the schema-agnostic `cmp`) — issue #933 / roborev #959 Medium #3.
    ///
    /// Bounds may be a PREFIX shorter than the full clustering arity; comparing
    /// only the bound's components (via [`ClusteringKey::compare`], which treats an
    /// absent trailing component as a first-sorting NULL) yields the correct
    /// containment for the `DELETE WHERE pk=? AND ck1=?` prefix case.
    /// Shadow the cells / row / metadata of a reconciled cluster entry that are
    /// covered by the partition-level deletion (issue #1072 — the OUTERMOST floor).
    ///
    /// Drops every DATA cell whose own `timestamp` is `<= pmfda` (the `<=` lets the
    /// partition deletion win an equal-ts tie, like #498/#933). Clustering-key
    /// pseudo-cells are retained whenever any data cell survives so the row keeps
    /// its key columns for read-back. A row whose every data cell is shadowed AND
    /// whose row-marker liveness is `<= pmfda` (judged on the MARKER's own
    /// timestamp, #3094) produces nothing. A row tombstone / coexisting row deletion /
    /// complex deletion older-or-equal to the floor is subsumed; a strictly-newer
    /// one survives. `None` floor (`max_partition_deletion == None`) is the common
    /// case and returns the entry untouched.
    fn apply_partition_shadowing(
        entry: MergeEntry,
        max_partition_deletion: Option<(i64, i32)>,
    ) -> Option<MergeEntry> {
        let Some((pmfda, _)) = max_partition_deletion else {
            return Some(entry);
        };

        // A complex (collection) deletion older-or-equal to the partition floor is
        // subsumed; one strictly newer must survive.
        let surviving_complex: Vec<ComplexDeletion> = entry
            .complex_deletions
            .into_iter()
            .filter(|cd| cd.marked_for_delete_at > pmfda)
            .collect();

        // A coexisting row deletion (#932) older-or-equal to the floor is subsumed;
        // a newer one is preserved.
        let surviving_row_del = entry.row_deletion.filter(|(dt, _)| *dt > pmfda);

        match entry.row_data {
            RowData::Tombstone {
                deletion_time,
                local_deletion_time,
            } => {
                // A row tombstone older-or-equal to the partition floor is subsumed
                // by the partition deletion; a strictly-newer one survives.
                if deletion_time > pmfda {
                    let mut rebuilt = MergeEntry::new(
                        entry.run_index,
                        entry.key,
                        entry.clustering_key,
                        deletion_time,
                        RowData::Tombstone {
                            deletion_time,
                            local_deletion_time,
                        },
                    );
                    if !surviving_complex.is_empty() {
                        rebuilt = rebuilt.with_complex_deletions(surviving_complex);
                    }
                    Some(rebuilt)
                } else if !surviving_complex.is_empty() {
                    Some(
                        MergeEntry::new(
                            entry.run_index,
                            entry.key,
                            entry.clustering_key,
                            entry.timestamp,
                            RowData::Live { cells: Vec::new() },
                        )
                        .with_complex_deletions(surviving_complex),
                    )
                } else {
                    None
                }
            }
            RowData::Live { cells } => {
                // Identify clustering pseudo-cells (kept when any data cell
                // survives). A static / unclustered row has no clustering key.
                let ck_names: std::collections::HashSet<String> = entry
                    .clustering_key
                    .as_ref()
                    .map(|ck| ck.columns.iter().map(|(n, _)| n.clone()).collect())
                    .unwrap_or_default();
                let is_data = |c: &CellData| !ck_names.contains(&c.column);

                let kept: Vec<CellData> = cells
                    .into_iter()
                    .filter(|c| !is_data(c) || c.timestamp > pmfda)
                    .collect();
                let has_data = kept.iter().any(is_data);

                // #3094: judged on the MARKER's OWN ts — see `marker_survives_floor`.
                let liveness = entry.row_liveness;
                let marker_live = liveness.marker_survives_floor(entry.timestamp, pmfda);

                if !has_data && !marker_live {
                    if let Some((dt, ldt)) = surviving_row_del {
                        return Some(MergeEntry::new(
                            entry.run_index,
                            entry.key,
                            entry.clustering_key,
                            dt,
                            RowData::Tombstone {
                                deletion_time: dt,
                                local_deletion_time: ldt,
                            },
                        ));
                    }
                    if !surviving_complex.is_empty() {
                        return Some(
                            MergeEntry::new(
                                entry.run_index,
                                entry.key,
                                entry.clustering_key,
                                entry.timestamp,
                                RowData::Live { cells: Vec::new() },
                            )
                            .with_complex_deletions(surviving_complex),
                        );
                    }
                    return None;
                }

                let row_ts = if has_data {
                    kept.iter()
                        .filter(|c| is_data(c))
                        .map(|c| c.timestamp)
                        .max()
                        .unwrap_or(entry.timestamp)
                } else {
                    entry.timestamp
                };

                let mut rebuilt = MergeEntry::new(
                    entry.run_index,
                    entry.key,
                    entry.clustering_key,
                    row_ts,
                    RowData::Live { cells: kept },
                );
                // Issue #2374/#2789: carry the primary-key liveness marker forward
                // when it survives the partition floor, so a key-only live row
                // (INSERT with no/all-null regular columns) that coexists with an
                // older covering partition tombstone stays VISIBLE through the read
                // path. Dropped (default) when the marker did not survive the floor.
                rebuilt = rebuilt.with_row_liveness(if marker_live {
                    entry.row_liveness
                } else {
                    Default::default()
                });
                if !surviving_complex.is_empty() {
                    rebuilt = rebuilt.with_complex_deletions(surviving_complex);
                }
                if let Some((dt, ldt)) = surviving_row_del {
                    rebuilt = rebuilt.with_row_deletion(dt, ldt);
                }
                Some(rebuilt)
            }
        }
    }

    fn range_tombstone_covers_ck(
        ck: &ClusteringKey,
        rt: &RangeTombstone,
        schema: &TableSchema,
    ) -> bool {
        // Issue #1669: count coverage comparisons so a bound test can prove the
        // binary search stays O(rows) — one candidate per row — instead of the
        // former O(rows × ranges) linear scan. Vanishes in production builds.
        #[cfg(test)]
        crate::storage::sstable::work_counters::range_coverage_scope::record();
        use crate::storage::write_engine::mutation::ClusteringBound;

        // Compare `ck` against a bound key over the bound's component count so a
        // prefix bound only compares its present components.
        let cmp = |bound: &ClusteringKey| -> Ordering {
            let n = bound.columns.len();
            let truncated = ClusteringKey {
                columns: ck.columns.iter().take(n).cloned().collect(),
            };
            truncated
                .compare(bound, schema)
                .unwrap_or_else(|_| truncated.cmp(bound))
        };

        let after_start = match &rt.start {
            ClusteringBound::Inclusive(b) => cmp(b) != Ordering::Less,
            ClusteringBound::Exclusive(b) => cmp(b) == Ordering::Greater,
            ClusteringBound::Bottom => true,
            ClusteringBound::Top => false,
        };
        let before_end = match &rt.end {
            ClusteringBound::Inclusive(b) => cmp(b) != Ordering::Greater,
            ClusteringBound::Exclusive(b) => cmp(b) == Ordering::Less,
            ClusteringBound::Top => true,
            ClusteringBound::Bottom => false,
        };
        after_start && before_end
    }

    /// Whether a coalesced range tombstone's END bound lies strictly BEFORE `ck`
    /// on the clustering axis — i.e. `ck` is beyond the range's end, so the range
    /// cannot cover it. This is exactly the negation of the `before_end` test in
    /// [`Self::range_tombstone_covers_ck`], kept in lock-step with it.
    ///
    /// It is the monotonic predicate the #1669 binary search feeds to
    /// [`slice::partition_point`]: because `coalesce_range_tombstones` yields a
    /// per-partition sequence sorted by start bound and DISJOINT, the ranges are
    /// also sorted by end bound, so `range_end_before_ck` is `true` for every
    /// range wholly before `ck` and `false` thereafter — a clean partition point.
    /// The first `false` range is the ONLY candidate that can contain `ck`
    /// (disjointness ⇒ at most one covers it).
    ///
    /// Deliberately does NOT bump the `range_coverage_scope` counter: it is the
    /// cheap `O(log ranges)` search step, distinct from the single authoritative
    /// `range_tombstone_covers_ck` containment check the counter measures.
    fn range_end_before_ck(ck: &ClusteringKey, rt: &RangeTombstone, schema: &TableSchema) -> bool {
        use crate::storage::write_engine::mutation::ClusteringBound;

        // Same prefix-aware comparison as `range_tombstone_covers_ck` (compare
        // `ck` against the bound over the bound's component count).
        let cmp = |bound: &ClusteringKey| -> Ordering {
            let n = bound.columns.len();
            let truncated = ClusteringKey {
                columns: ck.columns.iter().take(n).cloned().collect(),
            };
            truncated
                .compare(bound, schema)
                .unwrap_or_else(|_| truncated.cmp(bound))
        };

        match &rt.end {
            // Negation of the `before_end` arms in `range_tombstone_covers_ck`.
            ClusteringBound::Inclusive(b) => cmp(b) == Ordering::Greater,
            ClusteringBound::Exclusive(b) => cmp(b) != Ordering::Less,
            ClusteringBound::Top => false,
            ClusteringBound::Bottom => true,
        }
    }

    /// Shadow the cells of a reconciled cluster entry that are covered by a range
    /// tombstone (issue #933, the re-applied #846 "Step 2c" made schema-aware).
    ///
    /// Computes the max `markedForDeleteAt` among range tombstones covering this
    /// entry's clustering key, then drops every DATA cell whose own `timestamp` is
    /// `<= floor` (the `<=` boundary lets a deletion win an equal-ts tie, #498).
    /// Clustering-key pseudo-cells are retained whenever any data cell survives so
    /// the row keeps its key columns for read-back. A row whose every data cell is
    /// shadowed AND whose row-marker liveness is `<= floor` produces nothing (the
    /// re-emitted range marker covers it); a coexisting row deletion newer than the
    /// floor is preserved as a row tombstone. A row with no clustering key (static
    /// / unclustered) is never covered by a range tombstone.
    fn apply_range_shadowing(
        entry: MergeEntry,
        range_tombstones: &[(DecoratedKey, RangeTombstone)],
        schema: &TableSchema,
    ) -> Option<MergeEntry> {
        // Fast path for the overwhelmingly common partition with no range
        // tombstones: skip the clustering-key clone and coverage scan entirely.
        if range_tombstones.is_empty() {
            return Some(entry);
        }
        let Some(ck) = entry.clustering_key.clone() else {
            return Some(entry);
        };

        // Issue #1669: binary search for the covering range instead of a linear
        // `filter().max()` scan run per clustering key. `coalesce_range_tombstones`
        // produces, per partition key, a sequence sorted by start bound and
        // DISJOINT (verified: it partitions the clustering axis into segments
        // between distinct sorted cut boundaries and only merges adjacent
        // same-deletion segments — see `coalesce_partition_range_tombstones`). And
        // `apply_range_shadowing` is called from `merge_partition_rows`, which is
        // strictly per-partition, so this whole slice is ONE partition's
        // sorted+disjoint ranges. Disjoint ⇒ at most ONE range covers a given `ck`,
        // so the former max over the covering set is a max over ≤1 element: a
        // binary search finds it. O(rows × ranges) → O(rows × log ranges + ranges).
        //
        // Defensive guard: only take the binary-search path when the slice is
        // provably a single partition matching `entry`. `coalesce_range_tombstones`
        // groups partitions into CONTIGUOUS blocks, so first.key == last.key ⇒ one
        // group; == `entry.key` ⇒ it is `entry`'s partition. Any other shape (a
        // future multi-partition caller) falls back to the original exact linear
        // scan, so correctness never depends on the guarantee holding — only the
        // speedup does.
        let single_partition = match (range_tombstones.first(), range_tombstones.last()) {
            (Some((first, _)), Some((last, _))) => {
                first.key == entry.key.key && last.key == entry.key.key
            }
            _ => false,
        };
        let floor = if single_partition {
            // First range whose end is NOT before `ck` — the unique candidate that
            // can contain `ck` (disjoint + sorted). Verify full containment (both
            // bounds) via the authoritative `range_tombstone_covers_ck`.
            let idx = range_tombstones
                .partition_point(|(_, rt)| Self::range_end_before_ck(&ck, rt, schema));
            range_tombstones.get(idx).and_then(|(key, rt)| {
                (key.key == entry.key.key && Self::range_tombstone_covers_ck(&ck, rt, schema))
                    .then_some(rt.deletion_time)
            })
        } else {
            // Exact pre-#1669 behavior for any non-single-partition slice.
            range_tombstones
                .iter()
                .filter(|(key, rt)| {
                    key.key == entry.key.key && Self::range_tombstone_covers_ck(&ck, rt, schema)
                })
                .map(|(_, rt)| rt.deletion_time)
                .max()
        };
        let Some(floor) = floor else {
            return Some(entry);
        };

        // A complex (collection) deletion OLDER than the covering range is
        // subsumed by the range marker; one STRICTLY NEWER must survive, else
        // older collection elements from a non-compacted SSTable could resurrect
        // (roborev #959 High #2 — `apply_range_shadowing` previously dropped
        // `entry.complex_deletions` on the whole-row-covered path). Filtered once
        // here and reused by every arm below.
        let surviving_complex: Vec<ComplexDeletion> = entry
            .complex_deletions
            .into_iter()
            .filter(|cd| cd.marked_for_delete_at > floor)
            .collect();

        match entry.row_data {
            RowData::Tombstone {
                deletion_time,
                local_deletion_time,
            } => {
                // A row tombstone fully covered by a newer/equal range deletion is
                // redundant (the range marker shadows it); drop it. A strictly
                // newer row tombstone survives.
                if deletion_time > floor {
                    let mut rebuilt = MergeEntry::new(
                        entry.run_index,
                        entry.key,
                        Some(ck),
                        deletion_time,
                        RowData::Tombstone {
                            deletion_time,
                            local_deletion_time,
                        },
                    );
                    if !surviving_complex.is_empty() {
                        rebuilt = rebuilt.with_complex_deletions(surviving_complex);
                    }
                    Some(rebuilt)
                } else if !surviving_complex.is_empty() {
                    // The row tombstone is subsumed by the range, but a newer
                    // complex deletion must persist as a metadata-only carrier.
                    Some(
                        MergeEntry::new(
                            entry.run_index,
                            entry.key,
                            Some(ck),
                            entry.timestamp,
                            RowData::Live { cells: Vec::new() },
                        )
                        .with_complex_deletions(surviving_complex),
                    )
                } else {
                    None
                }
            }
            RowData::Live { cells } => {
                let ck_names: std::collections::HashSet<&str> =
                    ck.columns.iter().map(|(n, _)| n.as_str()).collect();
                let is_data = |c: &CellData| !ck_names.contains(c.column.as_str());

                // Keep clustering pseudo-cells; keep data cells strictly newer than
                // the covering range deletion.
                let kept: Vec<CellData> = cells
                    .into_iter()
                    .filter(|c| !is_data(c) || c.timestamp > floor)
                    .collect();
                let has_data = kept.iter().any(is_data);

                // A coexisting row deletion (#932) older than the range floor is
                // subsumed by the range marker; a newer one is preserved.
                let surviving_row_del = entry.row_deletion.filter(|(dt, _)| *dt > floor);

                // The row-marker liveness survives the range only if its own
                // timestamp is strictly newer than the covering deletion.
                let marker_live = entry.timestamp > floor;

                if !has_data && !marker_live {
                    // Whole row covered by the range. A coexisting row deletion
                    // (#932) newer than the floor wins and subsumes any collection
                    // deletion. Otherwise a complex deletion newer than the floor
                    // must persist as a metadata-only carrier (roborev #959 High
                    // #2); failing that, the re-emitted range marker is the sole
                    // survivor and this entry contributes nothing.
                    if let Some((dt, ldt)) = surviving_row_del {
                        return Some(MergeEntry::new(
                            entry.run_index,
                            entry.key,
                            Some(ck),
                            dt,
                            RowData::Tombstone {
                                deletion_time: dt,
                                local_deletion_time: ldt,
                            },
                        ));
                    }
                    if !surviving_complex.is_empty() {
                        return Some(
                            MergeEntry::new(
                                entry.run_index,
                                entry.key,
                                Some(ck),
                                entry.timestamp,
                                RowData::Live { cells: Vec::new() },
                            )
                            .with_complex_deletions(surviving_complex),
                        );
                    }
                    return None;
                }

                let row_ts = if has_data {
                    kept.iter()
                        .filter(|c| is_data(c))
                        .map(|c| c.timestamp)
                        .max()
                        .unwrap_or(entry.timestamp)
                } else {
                    entry.timestamp
                };

                let mut rebuilt = MergeEntry::new(
                    entry.run_index,
                    entry.key,
                    Some(ck),
                    row_ts,
                    RowData::Live { cells: kept },
                );
                // Issue #2374/#2789: carry the primary-key liveness marker forward
                // when it survives the range floor, so a key-only live row (INSERT
                // with no/all-null regular columns) that coexists with an older
                // covering range tombstone stays VISIBLE through the read path.
                // Dropped (default) when the marker did not survive the floor.
                rebuilt = rebuilt.with_row_liveness(if marker_live {
                    entry.row_liveness
                } else {
                    Default::default()
                });
                if !surviving_complex.is_empty() {
                    rebuilt = rebuilt.with_complex_deletions(surviving_complex);
                }
                if let Some((dt, ldt)) = surviving_row_del {
                    rebuilt = rebuilt.with_row_deletion(dt, ldt);
                }
                Some(rebuilt)
            }
        }
    }

    /// The cell's effective `localDeletionTime` (GC-clock seconds, on-disk
    /// width) for gc_grace purge decisions (#921 finding 1/2).
    ///
    /// The reader surfaces an EXPIRING simple cell's LDT in
    /// [`CellData::local_deletion_time`], but a simple cell TOMBSTONE carries its
    /// LDT inside its `Value::Tombstone(CellTombstone)` payload
    /// (`TombstoneInfo::local_deletion_time`, seconds) — `CellData::local_deletion_time`
    /// stays `None` for it (`row_decoder` only fills that field from
    /// `cell_meta` expiration). Without consulting the tombstone payload, the
    /// purge stage would always see `None` for a cell tombstone and conservatively
    /// retain it, so a purgeable dropped-column tombstone would be (wrongly)
    /// counted as a survivor. Prefer the explicit `CellData` field, then fall back
    /// to a non-zero tombstone-payload LDT; `0` is the "not surfaced" placeholder
    /// and yields `None` (retain on unknown LDT — no-heuristics mandate).
    fn cell_effective_ldt(cell: &CellData) -> Option<i32> {
        if let Some(ldt) = cell.local_deletion_time {
            return Some(ldt);
        }
        if let crate::types::Value::Tombstone(ref info) = cell.value {
            if info.tombstone_type == crate::types::TombstoneType::CellTombstone
                && info.local_deletion_time != 0
            {
                return Some(info.local_deletion_time as i32);
            }
        }
        None
    }

    /// Returns true when a cell carries a cell-level tombstone
    /// (`Value::Tombstone(CellTombstone)`), the representation produced by #505.
    ///
    /// Cell tombstones participate in per-cell reconcile like any other cell, but
    /// at EQUAL timestamp a cell tombstone beats a live value (Cassandra
    /// `Cells#reconcile`, same rule as #498 applied per cell).
    fn is_cell_tombstone(cell: &CellData) -> bool {
        matches!(
            cell.value,
            crate::types::Value::Tombstone(ref info)
                if info.tombstone_type == crate::types::TombstoneType::CellTombstone
        )
            // Complex-element tombstones carry their delete via the IS_DELETED
            // (0x01) flag (epic #899); their value is wrapped as a CellTombstone in
            // `compaction_row_data_to_row_data`, so the `matches!` above already
            // covers them. Keep `is_deleted` as a belt-and-braces signal so a
            // future element representation that sets the flag without the wrapped
            // value still counts as a deletion for the tie-break.
            || cell.is_deleted
    }

    /// Reconcile all entries for a single clustering-key group into at most one
    /// merged `MergeEntry`, applying per-cell last-write-wins plus row-tombstone
    /// shadowing (Issue #533). See [`Self::merge_partition_rows`] for the rules.
    ///
    /// `cluster_rows` is in heap-routing order (run_index ascending within equal
    /// keys), so when two cells tie on both timestamp and liveness the first-seen
    /// (newer file) is kept.
    ///
    /// Thin wrapper over [`Self::reconcile_cluster_with_overlap`] that defaults the
    /// overlap-aware max-purgeable timestamp to `i64::MAX` (unrestricted — the
    /// full-compaction semantics in effect before #935). The production merge path
    /// (`merge_partition_rows`) calls the `_with_overlap` form directly to pass the
    /// real bound for a partial compaction.
    #[cfg(test)]
    fn reconcile_cluster(
        clustering_key: Option<ClusteringKey>,
        cluster_rows: Vec<MergeEntry>,
        dropped_columns: &std::collections::HashMap<String, i64>,
        gc_before_secs: Option<i64>,
    ) -> Option<MergeEntry> {
        Self::reconcile_cluster_with_overlap(
            clustering_key,
            cluster_rows,
            dropped_columns,
            gc_before_secs,
            i64::MAX,
        )
    }

    /// Reconcile a clustering-key group with an explicit overlap-aware
    /// max-purgeable timestamp (#935). See [`Self::reconcile_cluster`] for the base
    /// reconciliation rules.
    ///
    /// Thin wrapper over [`Self::reconcile_cluster_with_overlap_counted`] that
    /// discards the tombstone-purge tally. The production merge path
    /// (`merge_partition_rows`) calls the `_counted` form directly to accumulate
    /// genuine gc/overlap-safe purges for `COMPACTION_TOMBSTONES_PURGED` (#1037);
    /// this wrapper keeps the simpler signature for tests and other callers.
    #[cfg(test)]
    fn reconcile_cluster_with_overlap(
        clustering_key: Option<ClusteringKey>,
        cluster_rows: Vec<MergeEntry>,
        dropped_columns: &std::collections::HashMap<String, i64>,
        gc_before_secs: Option<i64>,
        max_purgeable_timestamp: i64,
    ) -> Option<MergeEntry> {
        let mut sink = PurgeCounts::default();
        Self::reconcile_cluster_with_overlap_counted(
            clustering_key,
            cluster_rows,
            dropped_columns,
            gc_before_secs,
            max_purgeable_timestamp,
            // #1382: this test-only wrapper keeps the pre-#1382 default of NO
            // TTL expiry (`now_secs = None` = strict no-op). Tests that exercise
            // TTL expiry drive the real `compact_sstables` surface instead.
            None,
            &mut sink,
        )
    }

    /// Reconcile a clustering-key group, accumulating genuine gc/overlap-safe
    /// tombstone purges into `purges` (issue #1037).
    ///
    /// Identical merge OUTPUT to [`Self::reconcile_cluster_with_overlap`]; the
    /// only addition is that each true purge decision (a cell tombstone, row
    /// tombstone, or complex deletion dropped because it is gc/overlap-safe to
    /// drop in Step 3c) increments the matching `purges` field. Last-write-wins
    /// reconciliation collapse is NOT counted.
    fn reconcile_cluster_with_overlap_counted(
        clustering_key: Option<ClusteringKey>,
        cluster_rows: Vec<MergeEntry>,
        dropped_columns: &std::collections::HashMap<String, i64>,
        // EFFECTIVE gc_grace cutoff (`gcBefore`, GC-clock seconds), threaded from
        // the merger. A tombstone whose `localDeletionTime < gc_before_secs` is
        // PURGEABLE; `None` disables purging (issue #845).
        //
        // OVERLAP SAFETY (#921 finding 1, #935): the caller
        // (`merge_partition_rows`) collapses this to `None` only when the
        // compaction is a PARTIAL one with NO overlap bound, so the purge stage is
        // a strict no-op there. With a bound it runs and each tombstone is
        // additionally gated on `max_purgeable_timestamp` below.
        gc_before_secs: Option<i64>,
        // EFFECTIVE overlap-aware max-purgeable timestamp (`markedForDeleteAt`,
        // micros), threaded from the merger (#935). A tombstone is purgeable ONLY
        // when its own deletion timestamp is STRICTLY LESS THAN this value, so it
        // provably shadows no data living in a non-included overlapping SSTable.
        // `i64::MAX` for a full compaction (no outside overlap — every gc-purgeable
        // tombstone passes); the min outside timestamp for an overlap-aware partial
        // compaction; `i64::MIN` when purging is disabled (`gc_before_secs` is then
        // `None`, so this is unused).
        max_purgeable_timestamp: i64,
        // Pinned TTL-expiry evaluation instant (`now`, GC-clock seconds), threaded
        // from the merger (#1382). A live expiring cell whose `localDeletionTime`
        // is STRICTLY LESS THAN this is turned into a cell tombstone (Step 3b′)
        // and then purged by the SAME gc/overlap gate as any other cell tombstone.
        // `None` disables expiry (a strict no-op), preserving pre-#1382 behavior.
        now_secs: Option<i64>,
        // Tombstone-purge tally accumulated at the true purge decision points
        // (issue #1037). Never read here; only incremented.
        purges: &mut PurgeCounts,
    ) -> Option<MergeEntry> {
        // Issue #3058: explicit "the compaction reconciler ran" marker (see
        // `storage::read_path_probe`) — a single relaxed add on the merge arm.
        crate::storage::read_path_probe::record_reconcile_entry();
        // Decomposed into named, parity-load-bearing steps in `reconcile.rs`
        // (issue #945). The step ORDER is critical: Step 2b before Steps 3/3c so
        // a surviving complex deletion cannot resurrect a covered element on a
        // later purge (`f66fa14f`); `had_data_before` is captured pre-purge and
        // consulted post-purge (#921 finding 3). Behavior and the #1037 purge
        // tally are byte-identical.
        let mut state = reconcile::ReconcileState::new(clustering_key);
        // Step 1: fold per-entry row/complex/range deletion metadata.
        state.fold_row_deletions(&cluster_rows);
        // Step 2: per-(column, cell_path) last-write-wins winner resolution.
        state.resolve_cell_winners(&cluster_rows);
        // Empty group => nothing to emit (original `let key = key?`).
        if !state.has_key() {
            return None;
        }
        // Step 2b: complex-deletion strict-supersede + shadow-before-purge.
        state.apply_complex_deletions();
        // Step 3: row-tombstone shadowing (tallies #2163 suppression).
        state.shadow_by_row_deletion(purges);
        // Step 3b: dropped-column filtering (captures the phantom-row guard).
        state.filter_dropped_columns(dropped_columns);
        // Step 3b′ (#1382): TTL expiry — convert expired live cells to cell
        // tombstones BEFORE the gc-grace purge so an expired-past-grace cell is
        // dropped by Step 3c and an expired-within-grace cell is emitted as a
        // tombstone (its live value never resurfaces).
        state.expire_ttl_cells(now_secs);
        // Step 3c: gc_grace / overlap-aware tombstone purging (tallies #1037).
        state.purge_gc_grace(gc_before_secs, max_purgeable_timestamp, purges);
        // Step 4: phantom-row guard + emit the merged entry (tallies #2163
        // emitted row-tombstone markers).
        state.build(purges)
    }

    /// Convert reconciled `CellData`s into writer `CellOperation`s (epic #899,
    /// Phase C).
    ///
    /// A simple cell (`is_complex_element == false`) maps 1:1 to a whole-column
    /// `Write` / `WriteWithTtl` / `Delete` exactly as before. A complex element
    /// (`is_complex_element == true`) maps to a
    /// [`CellOperation::WriteComplexElement`] carrying the element's authoritative
    /// `cell_path`, per-element `timestamp` / `ttl` / `local_deletion_time`, and
    /// — crucially, per the no-heuristics mandate — its AUTHORITATIVE `is_deleted`
    /// flag (threaded verbatim from the reader's `ComplexElement.is_deleted`, NOT
    /// re-derived from value/ttl shape; an expiring SET member is value-None,
    /// ttl-Some yet not a tombstone). The element's on-disk `has_empty_value`
    /// decides whether a value is written, so a SET member round-trips with its
    /// member in the cell_path and no cell value.
    ///
    /// [`CellOperation::WriteComplexElement`]: crate::storage::write_engine::mutation::CellOperation::WriteComplexElement
    fn cells_to_cell_operations(
        cells: Vec<CellData>,
    ) -> Vec<crate::storage::write_engine::mutation::CellOperation> {
        use crate::storage::write_engine::mutation::CellOperation;
        use crate::types::{TombstoneType, Value};

        cells
            .into_iter()
            .map(|cell| {
                if cell.is_complex_element {
                    // Per-element op. The on-disk value is present only for a live
                    // element that is NOT empty-value and NOT deleted. A SET member
                    // (has_empty_value) carries its identity in cell_path; a
                    // deleted element carries no value.
                    let value = if cell.is_deleted || cell.has_empty_value {
                        None
                    } else {
                        Some(cell.value)
                    };
                    return CellOperation::WriteComplexElement {
                        column: cell.column,
                        // A complex element always has a cell_path; default to
                        // empty rather than panicking if a future producer omits
                        // it (no unwrap in lib code).
                        cell_path: cell.cell_path.unwrap_or_default(),
                        value,
                        timestamp_micros: cell.timestamp,
                        ttl_seconds: cell.ttl,
                        local_deletion_time: cell.local_deletion_time,
                        is_deleted: cell.is_deleted,
                    };
                }

                // Issue #505: simple cell-level tombstones are represented as
                // Value::Tombstone(CellTombstone); translate to
                // CellOperation::Delete so the writer emits a proper cell tombstone
                // rather than a live cell with a null value.
                if let Value::Tombstone(ref info) = cell.value {
                    if info.tombstone_type == TombstoneType::CellTombstone {
                        // #921 finding 2: preserve the SOURCE cell tombstone's own
                        // `localDeletionTime` (GC clock, seconds) so the writer
                        // emits it verbatim instead of deriving one from the
                        // enclosing mutation's timestamp. A within-grace cell
                        // tombstone that survives THIS compaction keeps its
                        // original GC clock — no drift that would purge it too
                        // early / keep it too long in a LATER compaction. `0` is
                        // the "not surfaced" placeholder (the reader writes it
                        // when the on-disk LDT is absent), in which case we leave
                        // the op LDT `None` so the writer keeps its historical
                        // timestamp-derived behavior. The width conversion mirrors
                        // the row-tombstone path (`info.local_deletion_time as
                        // i32`, #873).
                        let preserved_ldt = match info.local_deletion_time {
                            0 => None,
                            ldt => Some(ldt as i32),
                        };
                        return CellOperation::Delete {
                            column: cell.column,
                            local_deletion_time: preserved_ldt,
                        };
                    }
                }
                if let Some(ttl) = cell.ttl {
                    // #1538: preserve the SOURCE expiring cell's authoritative
                    // on-disk `localDeletionTime` (= writetime_s + ttl) so a live
                    // TTL cell that survives THIS compaction is re-emitted
                    // byte-identically — the writer stamps this LDT verbatim
                    // instead of recomputing `now + ttl` (which would drift by the
                    // compaction wall-clock skew). `None` (LDT not surfaced by the
                    // reader) leaves the writer's historical derivation in place.
                    CellOperation::WriteWithTtl {
                        column: cell.column,
                        value: cell.value,
                        ttl_seconds: ttl,
                        local_deletion_time: cell.local_deletion_time,
                    }
                } else {
                    CellOperation::Write {
                        column: cell.column,
                        value: cell.value,
                    }
                }
            })
            .collect()
    }

    /// Convert a MergeEntry back to Mutation for writing
    pub(crate) fn merge_entry_to_mutation(
        entry: MergeEntry,
        schema: &TableSchema,
    ) -> Result<crate::storage::write_engine::mutation::Mutation> {
        use crate::storage::write_engine::mutation::{
            CellOperation, Mutation, PartitionKey, TableId,
        };

        let partition_key = PartitionKey::from_bytes(&entry.key.key, schema)?;
        let table_id = TableId::new(&schema.keyspace, &schema.table);

        // Issue #1072: a partition-tombstone carrier produces a mutation carrying
        // ONLY a `partition_tombstone` (no operations, no clustering key, no row /
        // range tombstone). The writer (`write_partition`) lifts the
        // partition_tombstone onto the partition HEADER. Emitting any
        // `CellOperation` here (e.g. `DeleteRow` from a `RowData::Tombstone`) would
        // wrongly write a clustering-row tombstone instead. Handled first so the
        // carrier's `row_data` (carried as a deletion so the merge step does not
        // surface a phantom live row) never reaches the operation builder below.
        if let Some((deletion_time, local_deletion_time)) = entry.partition_deletion {
            let mutation = Mutation::new(
                table_id,
                partition_key,
                None,
                Vec::new(),
                deletion_time,
                None,
            );
            let mut mutation = mutation;
            mutation.partition_tombstone =
                Some(crate::storage::write_engine::mutation::PartitionTombstone {
                    deletion_time,
                    local_deletion_time,
                });
            return Ok(mutation);
        }

        // Issue #933: a surviving range tombstone rides on the mutation's
        // `range_tombstones`, which the writer interleaves as on-disk bound markers
        // (and uses to shadow same-partition rows). Captured before `entry` is
        // consumed below. A range carrier has empty operations and no clustering
        // key, so it produces NO row — only the marker is written.
        let range_tombstone = entry.range_deletion.clone();

        // Capture the row tombstone's source LDT (GC-clock seconds) by borrow,
        // before the `match` below moves `cells` out of `entry.row_data` (#873).
        //
        // `0` is the established "LDT not surfaced by the reader" placeholder
        // (the legacy `CompactionRow::from_legacy_value` fallback and pre-V5 row
        // tombstones both build `local_deletion_time: 0`), NOT an authoritative
        // epoch-1970 deletion — a real Cassandra row tombstone always carries a
        // nonzero wall-clock LDT. Only thread a genuinely-surfaced (nonzero) LDT;
        // for the placeholder leave it `None` so the writer keeps deriving LDT
        // from `entry.timestamp` exactly as before this change. Threading `0`
        // here would both lose that fallback and trip the new below-baseline
        // writer guard against a nonzero pre-seeded `min_local_deletion_time`,
        // rejecting previously-valid legacy-path compactions (#873 review #946).
        // A live row leaves it `None` so any cell tombstones keep their historical
        // timestamp-derived behavior.
        let row_tombstone_ldt = match &entry.row_data {
            RowData::Tombstone {
                local_deletion_time,
                ..
            } if *local_deletion_time != 0 => Some(*local_deletion_time),
            _ => None,
        };

        // Issue #932: a coexisting row deletion carried on a LIVE entry must be
        // emitted as the mutation's `row_tombstone` (deletion time decoupled from
        // the row's liveness `timestamp_micros`), so the writer re-emits a
        // `HAS_DELETION` row holding both the deletion and the surviving cells. A
        // pure `RowData::Tombstone` entry carries its deletion via `DeleteRow`
        // (below) and leaves `row_deletion` unset, so this only fires for the
        // coexistence case.
        let coexisting_row_tombstone = match &entry.row_data {
            RowData::Live { .. } => entry.row_deletion,
            RowData::Tombstone { .. } => None,
        };

        // Capture the row tombstone's deletion time (`row_del`) BEFORE moving
        // `row_data` into `operations`. A row tombstone shadows only cells/markers
        // whose timestamp is `<= row_del`; a complex-deletion marker whose
        // `marked_for_delete_at` is STRICTLY GREATER than `row_del` covers a range
        // the row tombstone does NOT, and must still be emitted (see below).
        let row_del = match &entry.row_data {
            RowData::Tombstone { deletion_time, .. } => Some(*deletion_time),
            RowData::Live { .. } => None,
        };

        // Issue #1018: capture each surviving SIMPLE cell's OWN per-cell
        // timestamp BEFORE the cells are consumed by `cells_to_cell_operations`
        // (which turns them into `CellOperation::Write`/`WriteWithTtl`/`Delete`
        // ops that carry no per-cell timestamp). A reconciled row's
        // `entry.timestamp` is the MAX surviving cell timestamp; promoting every
        // sibling to that max would rewrite a cell's timestamp:
        //   * a live `name`@100 next to a `score` cell tombstone@300 → `name`
        //     wrongly rewritten to 300 (the original fix), AND
        //   * a `c1` cell tombstone@100 next to a live `c2`@300 → the tombstone's
        //     marked-for-delete-at wrongly rewritten to 300, so a LATER
        //     compaction that sees `c1` live@200 would be incorrectly shadowed
        //     and DROPPED (over-deletion — roborev HIGH).
        // For a SIMPLE cell tombstone (`Value::Tombstone(CellTombstone)`) the
        // cell's `timestamp` IS its `markedForDeleteAt` (µs); its GC-clock
        // `localDeletionTime` is preserved INDEPENDENTLY via
        // `CellOperation::Delete::local_deletion_time` (#921). Recording any
        // simple cell (live OR tombstone) whose own timestamp DIFFERS from
        // `entry.timestamp` keeps the side-channel empty for the common
        // single-writetime row (zero behavior change there) and lets the writer
        // emit the differing cell with its own explicit timestamp. The map is
        // keyed by column name; per-cell reconciliation already collapses each
        // column to a single surviving op, so one timestamp per column is exact.
        // Complex-element cells already carry their own timestamp via
        // `WriteComplexElement` and are excluded (`!c.is_complex_element`).
        let cell_write_timestamps: Option<std::collections::HashMap<String, i64>> =
            match &entry.row_data {
                RowData::Live { cells } => {
                    let map: std::collections::HashMap<String, i64> = cells
                        .iter()
                        .filter(|c| !c.is_complex_element && c.timestamp != entry.timestamp)
                        .map(|c| (c.column.clone(), c.timestamp))
                        .collect();
                    (!map.is_empty()).then_some(map)
                }
                RowData::Tombstone { .. } => None,
            };

        let mut operations = match entry.row_data {
            RowData::Live { cells } => Self::cells_to_cell_operations(cells),
            RowData::Tombstone { .. } => vec![CellOperation::DeleteRow],
        };

        // Epic #899 Phase C: emit a REAL per-column complex deletion marker for
        // each carried `ComplexDeletion`, replacing the writer's hardcoded LIVE
        // sentinel. The writer pairs this with the column's surviving per-element
        // cells (WriteComplexElement ops above). `reconcile_cluster` has already
        // applied strict-supersede + shadow-before-purge to this carried deletion
        // (issue #887), so the marker emitted here is the surviving (max-mfda)
        // deletion and the paired elements are only the survivors. gc_grace purging
        // of the surviving marker remains future work (#845).
        //
        // Issue #887: for a row that reduces to a ROW TOMBSTONE we must NOT
        // unconditionally drop carried complex-deletion markers. A row tombstone at
        // `row_del` shadows only `timestamp <= row_del`. A carried marker whose
        // `marked_for_delete_at` is STRICTLY GREATER than `row_del` covers elements
        // in `(row_del, mfda]` — including elements living in OTHER SSTables not part
        // of this compaction. Dropping such a marker would let those elements be
        // RESURRECTED. So we emit any marker that strictly supersedes the row
        // tombstone (`mfda > row_del`) alongside the `DeleteRow`, mirroring #887's
        // strict boundary. A marker with `mfda <= row_del` is fully covered by the
        // row tombstone and is dropped. For a live row (`row_del == None`) every
        // carried marker is emitted as before.
        for cd in entry.complex_deletions {
            let strictly_supersedes_row_tombstone =
                row_del.is_none_or(|rd| cd.marked_for_delete_at > rd);
            if strictly_supersedes_row_tombstone {
                operations.push(CellOperation::ComplexDeletion {
                    column: cd.column,
                    marked_for_delete_at: cd.marked_for_delete_at,
                    local_deletion_time: cd.local_deletion_time,
                });
            }
        }

        // Thread the row tombstone's preserved source `localDeletionTime` onto the
        // mutation so the writer emits it verbatim (#873) rather than re-deriving
        // it from `entry.timestamp` (`timestamp_micros / 1_000_000`). That keeps
        // gc_grace semantics intact and avoids underflowing the unsigned
        // row-deletion LDT delta for logical-timestamp deletes. A live row leaves
        // the mutation LDT unset (`None`).
        let mutation = Mutation::new(
            table_id,
            partition_key,
            entry.clustering_key,
            operations,
            entry.timestamp,
            None,
        );
        let mutation = match row_tombstone_ldt {
            Some(ldt) => mutation.with_local_deletion_time(ldt),
            None => mutation,
        };
        // Issue #932: attach the coexisting row deletion (deletion time + LDT) so
        // the writer keeps both the row tombstone and the surviving newer cells.
        let mut mutation = match coexisting_row_tombstone {
            Some((deletion_time, ldt)) => mutation.with_row_tombstone(deletion_time, ldt),
            None => mutation,
        };
        // Issue #933: thread the surviving range tombstone so the writer emits the
        // on-disk bound markers (and shadows same-partition rows the marker covers).
        if let Some(rt) = range_tombstone {
            mutation.range_tombstones.push(rt);
        }
        // Issue #1018: thread per-cell timestamps so the writer preserves each
        // surviving sibling cell's own timestamp instead of the row max — for
        // BOTH a live cell's writetime AND a cell tombstone's markedForDeleteAt.
        mutation.cell_write_timestamps = cell_write_timestamps;
        Ok(mutation)
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::storage::write_engine::mutation::DecoratedKey;

    #[test]
    fn test_merge_entry_ordering_by_token() {
        let entry1 = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        let entry2 = MergeEntry::new(
            0,
            DecoratedKey::new(200, vec![1, 2, 3]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        // Entry with lower token should come first
        assert!(entry1 < entry2);
        assert!(entry2 > entry1);
    }

    #[test]
    fn test_merge_entry_ordering_by_key_bytes() {
        // Same token, different key bytes (hash collision)
        let entry1 = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        let entry2 = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1, 2, 4]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        // Entry with smaller key bytes should come first
        assert!(entry1 < entry2);
        assert!(entry2 > entry1);
    }

    #[test]
    fn test_merge_entry_ordering_by_run_index() {
        // Same token and key, different run indices
        let entry1 = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        let entry2 = MergeEntry::new(
            1,
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        // Entry with lower run_index should come first (newer file wins)
        assert!(entry1 < entry2);
        assert!(entry2 > entry1);
    }

    #[test]
    fn test_merge_entry_min_heap() {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

        let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();

        // Insert in reverse order
        let entry3 = MergeEntry::new(
            0,
            DecoratedKey::new(300, vec![3]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );
        let entry1 = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );
        let entry2 = MergeEntry::new(
            0,
            DecoratedKey::new(200, vec![2]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        heap.push(Reverse(entry3.clone()));
        heap.push(Reverse(entry1.clone()));
        heap.push(Reverse(entry2.clone()));

        // Should pop in ascending order
        assert_eq!(heap.pop().unwrap().0.key.token, 100);
        assert_eq!(heap.pop().unwrap().0.key.token, 200);
        assert_eq!(heap.pop().unwrap().0.key.token, 300);
    }

    #[test]
    fn test_row_data_variants() {
        let live = RowData::Live {
            cells: vec![CellData {
                column: "name".to_string(),
                value: Value::text("Alice".to_string()),
                timestamp: 1000,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        };

        match live {
            RowData::Live { cells } => {
                assert_eq!(cells.len(), 1);
                assert_eq!(cells[0].column, "name");
            }
            _ => panic!("Expected Live variant"),
        }

        let tombstone = RowData::Tombstone {
            deletion_time: 2000,
            local_deletion_time: 1000,
        };

        match tombstone {
            RowData::Tombstone {
                deletion_time,
                local_deletion_time,
            } => {
                assert_eq!(deletion_time, 2000);
                assert_eq!(local_deletion_time, 1000);
            }
            _ => panic!("Expected Tombstone variant"),
        }
    }

    #[test]
    fn test_cell_data_creation() {
        let cell = CellData {
            column: "age".to_string(),
            value: Value::Integer(30),
            timestamp: 1234567890,
            ttl: Some(3600),
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        };

        assert_eq!(cell.column, "age");
        assert_eq!(cell.value, Value::Integer(30));
        assert_eq!(cell.timestamp, 1234567890);
        assert_eq!(cell.ttl, Some(3600));
    }

    #[test]
    fn test_merge_stats_creation() {
        let stats = MergeStats {
            input_files: 5,
            output_partitions: 1000,
            output_rows: 5000,
            bytes_written: 1024 * 1024,
            elapsed: Duration::from_secs(10),
            dropped_whole: Vec::new(),
        };

        assert_eq!(stats.input_files, 5);
        assert_eq!(stats.output_partitions, 1000);
        assert_eq!(stats.output_rows, 5000);
        assert_eq!(stats.bytes_written, 1024 * 1024);
        assert_eq!(stats.elapsed.as_secs(), 10);
    }

    #[test]
    fn test_run_reader_estimate_entry_size() {
        let entry = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1, 2, 3, 4]),
            None,
            1000,
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::text("Alice".to_string()),
                    timestamp: 1000,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        let size = RunReader::estimate_entry_size(&entry);

        // Size should be at least the base struct size plus key bytes
        let expected_min_size = std::mem::size_of::<MergeEntry>() + 4;
        assert!(size >= expected_min_size);
    }

    #[test]
    fn test_kway_merger_empty_input() {
        use crate::schema::{KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let result = KWayMerger::new(vec![], &schema);
        assert!(result.is_err());

        if let Err(Error::InvalidInput(msg)) = result {
            assert!(msg.contains("at least one input file"));
        } else {
            panic!("Expected InvalidInput error");
        }
    }

    #[test]
    fn test_merge_entry_equal_timestamps_prefer_lower_run_index() {
        // Same partition, same clustering, same timestamp
        // Lower run_index should win (newer file)
        let entry_run0 = MergeEntry::new(
            0, // run_index 0 (newer)
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            1000, // same timestamp
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::text("Newer".to_string()),
                    timestamp: 1000,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        let entry_run1 = MergeEntry::new(
            1, // run_index 1 (older)
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            1000, // same timestamp
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::text("Older".to_string()),
                    timestamp: 1000,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        // Entry from run 0 should come first in ordering
        assert!(entry_run0 < entry_run1);
    }

    #[test]
    fn test_merge_entry_tombstone() {
        let tombstone_entry = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            2000,
            RowData::Tombstone {
                deletion_time: 2000,
                local_deletion_time: 1000,
            },
        );

        match tombstone_entry.row_data {
            RowData::Tombstone {
                deletion_time,
                local_deletion_time,
            } => {
                assert_eq!(deletion_time, 2000);
                assert_eq!(local_deletion_time, 1000);
            }
            _ => panic!("Expected Tombstone"),
        }
    }

    #[test]
    fn test_real_merger_delete_wins_at_equal_timestamp() {
        // Issue #498: at EQUAL timestamp, a Delete (tombstone) must beat a Live
        // row regardless of file recency (Cassandra `Cells#reconcile`).
        //
        // We drive the REAL merger entry point (`merge_partition_rows`) with two
        // entries that share the SAME clustering key and the SAME timestamp:
        //   - A: Live, run_index 0  (the NEWER file — would win a run_index tiebreak)
        //   - B: Delete, run_index 1 (the OLDER file)
        //
        // The pre-fix merger sorted equal-timestamp ties by run_index only, so the
        // live row (run_index 0) would win and survive. With the fix the tombstone
        // wins. This test therefore FAILS if the tiebreak reverts to run_index.
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "reconcile_ks".to_string(),
            table: "reconcile_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "value".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        const EQUAL_TS: i64 = 1_700_000_000_000_000;

        let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);

        // A = Live, in the NEWER file (run_index 0).
        let live_entry = MergeEntry::new(
            0,
            partition_key.clone(),
            None,
            EQUAL_TS,
            RowData::Live {
                cells: vec![CellData {
                    column: "value".to_string(),
                    value: Value::text("survivor-if-buggy".to_string()),
                    timestamp: EQUAL_TS,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        // B = Delete (row tombstone), in the OLDER file (run_index 1).
        let tombstone_entry = MergeEntry::new(
            1,
            partition_key.clone(),
            None,
            EQUAL_TS,
            RowData::Tombstone {
                deletion_time: EQUAL_TS,
                local_deletion_time: 2_000_000,
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema_arc: std::sync::Arc::new(schema.clone()),
            schema,
            _egress_slot: None,
        };

        // Drive the real merger. Order the input so the live (newer-file) entry is
        // first — pre-fix this is exactly the entry that wins by run_index.
        let merged = merger
            .merge_partition_rows(vec![live_entry, tombstone_entry])
            .expect("merge_partition_rows must not fail");

        assert_eq!(merged.len(), 1, "one clustering key => one merged winner");

        assert!(
            matches!(merged[0].row_data, RowData::Tombstone { .. }),
            "At equal timestamp the tombstone must win even though the live row is in \
             the newer file (run_index 0). Got a live row => the equal-ts tiebreak \
             reverted to run_index (Issue #498 regression)."
        );
    }

    #[test]
    fn test_real_merger_disjoint_columns_survive_compaction() {
        // Issue #533: when two SSTables share the same (pk, ck) but carry DISJOINT
        // columns, per-cell reconcile must keep cells from BOTH. The pre-fix merger
        // picked one whole winning row and DROPPED the loser's columns.
        //
        //   A (run_index 1, ts=100): {name: "alice"}
        //   B (run_index 0, ts=200): {score: 42}
        //
        // Cassandra `Cells#reconcile` => {name: "alice", score: 42}.
        // The old whole-row-wins code returned only {score: 42} (name LOST).
        use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "disjoint_ks".to_string(),
            table: "disjoint_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: Default::default(),
            }],
            columns: vec![
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "score".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);
        let ck = ClusteringKey {
            columns: vec![("ck".to_string(), Value::Integer(1))],
        };

        // A: older file (run_index 1), only `name` at ts=100.
        let entry_a = MergeEntry::new(
            1,
            partition_key.clone(),
            Some(ck.clone()),
            100,
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::text("alice".to_string()),
                    timestamp: 100,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        // B: newer file (run_index 0), only `score` at ts=200.
        let entry_b = MergeEntry::new(
            0,
            partition_key.clone(),
            Some(ck.clone()),
            200,
            RowData::Live {
                cells: vec![CellData {
                    column: "score".to_string(),
                    value: Value::Integer(42),
                    timestamp: 200,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema_arc: std::sync::Arc::new(schema.clone()),
            schema,
            _egress_slot: None,
        };

        // Pass in heap-routing order (run_index ascending): B then A.
        let merged = merger
            .merge_partition_rows(vec![entry_b, entry_a])
            .expect("merge_partition_rows must not fail");

        assert_eq!(merged.len(), 1, "one clustering key => one merged row");

        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected a Live merged row, got {:?}", other),
        };

        let name = cells.iter().find(|c| c.column == "name");
        let score = cells.iter().find(|c| c.column == "score");

        assert!(
            name.is_some(),
            "disjoint column `name` from the older file was DROPPED — per-cell \
             reconcile regression (Issue #533). Old whole-row-wins code fails here."
        );
        assert!(
            score.is_some(),
            "disjoint column `score` from the newer file is missing"
        );
        assert_eq!(
            name.unwrap().value,
            Value::text("alice".to_string()),
            "`name` must carry A's value"
        );
        assert_eq!(
            score.unwrap().value,
            Value::Integer(42),
            "`score` must carry B's value"
        );

        // Row timestamp must be the max surviving cell timestamp.
        assert_eq!(
            merged[0].timestamp, 200,
            "merged row timestamp must be the max surviving cell timestamp"
        );
    }

    #[test]
    fn test_real_merger_cell_tombstone_beats_live_at_equal_timestamp() {
        // Issue #533/#498 (per cell): when two SSTables write the SAME column at the
        // SAME timestamp, a cell tombstone (Delete) must beat the live value,
        // independent of file recency.
        //
        //   A (run_index 0, NEWER file, ts=100): {score: 42}            (live)
        //   B (run_index 1, OLDER file, ts=100): {score: <cell tombstone>}
        //
        // Cassandra `Cells#reconcile` => score is deleted. The adversarial part: A is
        // the newer file, so a recency-only tiebreak would wrongly keep the live 42.
        use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
        use crate::types::{TombstoneInfo, TombstoneType};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "ct_ks".to_string(),
            table: "ct_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: Default::default(),
            }],
            columns: vec![Column {
                name: "score".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);
        let ck = ClusteringKey {
            columns: vec![("ck".to_string(), Value::Integer(1))],
        };

        // A: newer file (run_index 0), live `score` = 42 at ts=100.
        let entry_a = MergeEntry::new(
            0,
            partition_key.clone(),
            Some(ck.clone()),
            100,
            RowData::Live {
                cells: vec![CellData {
                    column: "score".to_string(),
                    value: Value::Integer(42),
                    timestamp: 100,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        // B: older file (run_index 1), cell tombstone on `score` at the SAME ts=100.
        let entry_b = MergeEntry::new(
            1,
            partition_key.clone(),
            Some(ck.clone()),
            100,
            RowData::Live {
                cells: vec![CellData {
                    column: "score".to_string(),
                    value: Value::Tombstone(Box::new(TombstoneInfo {
                        deletion_time: 100,
                        tombstone_type: TombstoneType::CellTombstone,
                        local_deletion_time: 0,
                        ttl: None,
                        range_start: None,
                        range_end: None,
                    })),
                    timestamp: 100,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema_arc: std::sync::Arc::new(schema.clone()),
            schema,
            _egress_slot: None,
        };

        // Heap-routing order (run_index ascending): A then B.
        let merged = merger
            .merge_partition_rows(vec![entry_a, entry_b])
            .expect("merge_partition_rows must not fail");

        assert_eq!(merged.len(), 1, "one clustering key => one merged row");

        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected a Live merged row, got {:?}", other),
        };
        let score = cells
            .iter()
            .find(|c| c.column == "score")
            .expect("score cell must be present (as a tombstone)");

        assert!(
            matches!(
                score.value,
                Value::Tombstone(ref info) if info.tombstone_type == TombstoneType::CellTombstone
            ),
            "at equal ts the cell tombstone must win over the live value (got {:?}) — \
             a recency-only tiebreak would have kept the newer file's live 42 (#498 per cell)",
            score.value
        );
    }

    #[test]
    fn test_real_merger_value_tiebreak_diverges_from_cassandra() {
        // VERIFICATION (cursor-compaction findings #4 / #21): documents that CQLite's
        // equal-timestamp cell tie-break for two LIVE values of the SAME column
        // DIVERGES from Cassandra.
        //
        // Cassandra `Cells.resolveRegular` (cursor findings #4/#21): on a timestamp
        // tie between two live cells, the cell whose **raw value bytes** are strictly
        // greater (unsigned lexicographic compare, length prefix excluded) wins —
        // file/run order is NOT consulted.
        //
        // CQLite `reconcile_cluster` (merge.rs): on a timestamp tie between two live
        // cells it keeps the FIRST-SEEN cell, i.e. the lower run_index (newer file).
        // Raw value bytes are never compared.
        //
        // Fixture (same pk, no clustering, SAME timestamp, DIFFERENT values):
        //   A (run_index 0, NEWER file): {v: "apple"}    raw bytes 0x61 70 70 6C 65
        //   B (run_index 1, OLDER file): {v: "banana"}   raw bytes 0x62 ...  (GREATER)
        //
        // Cassandra would keep "banana" (greater raw bytes, from the older file).
        // CQLite keeps "apple" (first-seen / newer file). The two rules pick
        // DIFFERENT winners here, so the surviving value is byte-divergent.
        //
        // This test ASSERTS CQLite's current behavior and asserts that it differs
        // from the Cassandra winner. If CQLite is later changed to match Cassandra
        // (compare raw value bytes), THIS TEST WILL FAIL and must be updated to
        // reflect the new, convergent behavior. See
        // docs/garbage-free-compaction-improvements/cqlite-findings-and-applicability.md.
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "tiebreak_ks".to_string(),
            table: "tiebreak_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        const EQUAL_TS: i64 = 1_700_000_000_000_000;
        let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);

        // For CQL `text`, the raw cell value Cassandra compares is exactly the UTF-8
        // bytes. "banana" > "apple" unsigned-lexicographically (0x62 > 0x61).
        let newer_file_value = "apple"; // run_index 0 (newer)
        let older_file_value = "banana"; // run_index 1 (older), greater raw bytes
        assert!(
            older_file_value.as_bytes() > newer_file_value.as_bytes(),
            "fixture invariant: the older file must hold the lexicographically GREATER \
             raw value, so the two tie-break rules pick different winners"
        );

        // The value Cassandra's rule (#4/#21) would keep: greater raw value bytes.
        let cassandra_winner = if older_file_value.as_bytes() > newer_file_value.as_bytes() {
            older_file_value
        } else {
            newer_file_value
        };

        let entry_newer = MergeEntry::new(
            0, // newer file
            partition_key.clone(),
            None,
            EQUAL_TS,
            RowData::Live {
                cells: vec![CellData {
                    column: "v".to_string(),
                    value: Value::text(newer_file_value.to_string()),
                    timestamp: EQUAL_TS,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        let entry_older = MergeEntry::new(
            1, // older file
            partition_key.clone(),
            None,
            EQUAL_TS,
            RowData::Live {
                cells: vec![CellData {
                    column: "v".to_string(),
                    value: Value::text(older_file_value.to_string()),
                    timestamp: EQUAL_TS,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema_arc: std::sync::Arc::new(schema.clone()),
            schema,
            _egress_slot: None,
        };

        // Heap-routing order (run_index ascending): newer file first — exactly what
        // the real merge heap yields for equal (pk, ck).
        let merged = merger
            .merge_partition_rows(vec![entry_newer, entry_older])
            .expect("merge_partition_rows must not fail");

        assert_eq!(merged.len(), 1, "one (pk, ck) group => one merged winner");

        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected a Live merged row, got {:?}", other),
        };
        let surviving = match &cells
            .iter()
            .find(|c| c.column == "v")
            .expect("column `v` must survive")
            .value
        {
            Value::Text(s) => String::from_utf8_lossy(s).into_owned(),
            other => panic!("expected Text value, got {:?}", other),
        };

        // 1) CQLite's actual behavior: first-seen (newer file / lower run_index) wins.
        assert_eq!(
            surviving, newer_file_value,
            "CQLite reconcile_cluster keeps the first-seen (newer file) cell on a \
             timestamp tie; got {:?}",
            surviving
        );

        // 2) The divergence itself, made executable: CQLite's winner is NOT the value
        //    Cassandra's raw-value-bytes rule (#4/#21) would have kept.
        assert_ne!(
            surviving, cassandra_winner,
            "EXPECTED DIVERGENCE (#4/#21): CQLite kept {:?} but Cassandra's \
             Cells.resolveRegular keeps the greater raw value {:?}. If this assertion \
             fails, CQLite now matches Cassandra and the finding is RESOLVED — update \
             cqlite-findings-and-applicability.md and convert this into a convergence test.",
            surviving, cassandra_winner
        );
    }

    #[test]
    fn test_real_merger_same_column_conflict_resolves_by_timestamp() {
        // Issue #533: when both SSTables write the SAME column, the higher-timestamp
        // value wins (last-write-wins), but disjoint columns still survive.
        //
        //   A (run_index 1, ts=100): {name: "old", extra: "a-only"}
        //   B (run_index 0, ts=200): {name: "new"}
        // => {name: "new" (ts=200 wins), extra: "a-only" (survives)}
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "conflict_ks".to_string(),
            table: "conflict_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "extra".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);

        let entry_a = MergeEntry::new(
            1,
            partition_key.clone(),
            None,
            100,
            RowData::Live {
                cells: vec![
                    CellData {
                        column: "name".to_string(),
                        value: Value::text("old".to_string()),
                        timestamp: 100,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                        is_complex_element: false,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                    CellData {
                        column: "extra".to_string(),
                        value: Value::text("a-only".to_string()),
                        timestamp: 100,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                        is_complex_element: false,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                ],
            },
        );

        let entry_b = MergeEntry::new(
            0,
            partition_key.clone(),
            None,
            200,
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::text("new".to_string()),
                    timestamp: 200,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema_arc: std::sync::Arc::new(schema.clone()),
            schema,
            _egress_slot: None,
        };

        let merged = merger
            .merge_partition_rows(vec![entry_b, entry_a])
            .expect("merge_partition_rows must not fail");

        assert_eq!(merged.len(), 1);
        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };

        let name = cells
            .iter()
            .find(|c| c.column == "name")
            .expect("name present");
        let extra = cells
            .iter()
            .find(|c| c.column == "extra")
            .expect("extra (disjoint) must survive");

        assert_eq!(
            name.value,
            Value::text("new".to_string()),
            "same-column conflict must resolve to the higher-timestamp value"
        );
        assert_eq!(
            extra.value,
            Value::text("a-only".to_string()),
            "disjoint column from the older file must survive the conflict merge"
        );
    }

    #[test]
    fn test_real_merger_row_tombstone_shadows_old_cells_keeps_new() {
        // Issue #533 / #505: a row tombstone shadows cells with ts <= row_del but
        // a cell written strictly AFTER the tombstone survives.
        //
        //   A (ts=100): {name: "old"}          -> 100 <= 200 row_del => shadowed
        //   B (ts=200, row tombstone)
        //   C (ts=300): {score: 7}             -> 300 > 200            => survives
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "shadow_ks".to_string(),
            table: "shadow_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "score".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let pk = DecoratedKey::new(100, vec![0, 0, 0, 1]);

        let entry_a = MergeEntry::new(
            2,
            pk.clone(),
            None,
            100,
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::text("old".to_string()),
                    timestamp: 100,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );
        let entry_b = MergeEntry::new(
            1,
            pk.clone(),
            None,
            200,
            RowData::Tombstone {
                deletion_time: 200,
                local_deletion_time: 0,
            },
        );
        let entry_c = MergeEntry::new(
            0,
            pk.clone(),
            None,
            300,
            RowData::Live {
                cells: vec![CellData {
                    column: "score".to_string(),
                    value: Value::Integer(7),
                    timestamp: 300,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema_arc: std::sync::Arc::new(schema.clone()),
            schema,
            _egress_slot: None,
        };

        let merged = merger
            .merge_partition_rows(vec![entry_c, entry_b, entry_a])
            .expect("merge must not fail");

        assert_eq!(merged.len(), 1);
        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!(
                "expected Live (score survives the tombstone), got {:?}",
                other
            ),
        };

        assert!(
            cells.iter().all(|c| c.column != "name"),
            "`name` (ts=100 <= row_del=200) must be shadowed by the row tombstone"
        );
        let score = cells
            .iter()
            .find(|c| c.column == "score")
            .expect("`score` (ts=300 > row_del=200) must survive the row tombstone");
        assert_eq!(score.value, Value::Integer(7));
    }

    #[test]
    fn test_real_merger_row_tombstone_only_emits_tombstone() {
        // When every cell is shadowed by a row tombstone (no later writes), the
        // merger must emit a Tombstone entry so the row stays deleted downstream
        // (preserves #505/#498 absence semantics).
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "ts_only_ks".to_string(),
            table: "ts_only_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let pk = DecoratedKey::new(100, vec![0, 0, 0, 1]);

        let live = MergeEntry::new(
            1,
            pk.clone(),
            None,
            100,
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::text("doomed".to_string()),
                    timestamp: 100,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );
        let tomb = MergeEntry::new(
            0,
            pk.clone(),
            None,
            300,
            RowData::Tombstone {
                deletion_time: 300,
                local_deletion_time: 0,
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema_arc: std::sync::Arc::new(schema.clone()),
            schema,
            _egress_slot: None,
        };

        let merged = merger
            .merge_partition_rows(vec![tomb, live])
            .expect("merge must not fail");

        assert_eq!(merged.len(), 1);
        match &merged[0].row_data {
            RowData::Tombstone { deletion_time, .. } => {
                assert_eq!(*deletion_time, 300, "tombstone deletion_time preserved");
            }
            other => panic!("expected a Tombstone entry, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_step_variants() {
        let key = DecoratedKey::new(100, vec![1, 2, 3]);
        let rows = vec![];

        let partition_step = MergeStep::Partition { key, rows };

        match partition_step {
            MergeStep::Partition { key, rows } => {
                assert_eq!(key.token, 100);
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("Expected Partition variant"),
        }

        let complete_step = MergeStep::Complete;
        match complete_step {
            MergeStep::Complete => {}
            _ => panic!("Expected Complete variant"),
        }
    }

    #[test]
    fn test_cell_merge_last_write_wins_higher_timestamp() {
        // Two cells with different timestamps
        let cell1 = CellData {
            column: "name".to_string(),
            value: Value::text("Old".to_string()),
            timestamp: 1000,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        };

        let cell2 = CellData {
            column: "name".to_string(),
            value: Value::text("New".to_string()),
            timestamp: 2000, // Higher timestamp wins
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        };

        // Cell2 should win in last-write-wins merge
        assert!(cell2.timestamp > cell1.timestamp);
    }

    #[test]
    fn test_memory_budget_calculation() {
        // For k=10 SSTables, memory budget should be ~80KB
        let k = 10;
        let buffer_size_per_run = RunReader::DEFAULT_BUFFER_SIZE;
        let total_memory = k * buffer_size_per_run;

        assert_eq!(buffer_size_per_run, 8 * 1024); // 8KB
        assert_eq!(total_memory, 80 * 1024); // 80KB total
    }

    #[test]
    fn test_merge_entry_to_mutation_live_cells() {
        use crate::schema::{KeyColumn, TableSchema};
        use crate::storage::write_engine::mutation::{CellOperation, DecoratedKey};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        // Encode key as 4-byte big-endian int (42)
        let key_bytes = 42i32.to_be_bytes().to_vec();

        let entry = MergeEntry::new(
            0,
            DecoratedKey::new(1000, key_bytes),
            None,
            999_000_000,
            RowData::Live {
                cells: vec![
                    CellData {
                        column: "name".to_string(),
                        value: Value::text("Alice".to_string()),
                        timestamp: 999_000_000,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                        is_complex_element: false,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                    CellData {
                        column: "age".to_string(),
                        value: Value::Integer(30),
                        timestamp: 999_000_000,
                        ttl: Some(3600),
                        cell_path: None,
                        local_deletion_time: None,
                        is_complex_element: false,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                ],
            },
        );

        let mutation =
            KWayMerger::merge_entry_to_mutation(entry, &schema).expect("conversion should succeed");

        // Partition key should have one column named "id"
        assert_eq!(mutation.partition_key.columns.len(), 1);
        assert_eq!(mutation.partition_key.columns[0].0, "id");

        // Two operations: one Write and one WriteWithTtl
        assert_eq!(mutation.operations.len(), 2);
        assert_eq!(mutation.timestamp_micros, 999_000_000);

        let has_write = mutation
            .operations
            .iter()
            .any(|op| matches!(op, CellOperation::Write { column, .. } if column == "name"));
        let has_ttl_write = mutation.operations.iter().any(|op| {
            matches!(op, CellOperation::WriteWithTtl { column, ttl_seconds, .. }
                if column == "age" && *ttl_seconds == 3600)
        });
        assert!(has_write, "Expected Write operation for 'name'");
        assert!(has_ttl_write, "Expected WriteWithTtl operation for 'age'");
    }

    /// Issue #1018: a reconciled row whose surviving cells carry DIFFERENT write
    /// timestamps must preserve each cell's OWN writetime through the
    /// merge→mutation step. The row's `timestamp_micros` is the MAX surviving
    /// timestamp; cells whose timestamp differs from that max must be recorded in
    /// `Mutation::cell_write_timestamps` so the writer does not promote them to
    /// the row max. Cells at the row max are NOT recorded (they correctly inherit
    /// the row timestamp), keeping the side-channel empty for single-writetime
    /// rows.
    #[test]
    fn merge_entry_preserves_per_cell_write_timestamps() {
        use crate::schema::{KeyColumn, TableSchema};
        use crate::storage::write_engine::mutation::DecoratedKey;
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let cell = |column: &str, ts: i64| CellData {
            column: column.to_string(),
            value: Value::text(column.to_string()),
            timestamp: ts,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        };

        // Row max writetime is 300 (the `late` cell); `early`@100 is a live
        // sibling that must keep its own writetime.
        let entry = MergeEntry::new(
            0,
            DecoratedKey::new(1000, 42i32.to_be_bytes().to_vec()),
            None,
            300,
            RowData::Live {
                cells: vec![cell("early", 100), cell("late", 300)],
            },
        );

        let mutation =
            KWayMerger::merge_entry_to_mutation(entry, &schema).expect("conversion should succeed");

        assert_eq!(mutation.timestamp_micros, 300);
        let cwt = mutation
            .cell_write_timestamps
            .as_ref()
            .expect("per-cell write timestamps must be recorded for a mixed-writetime row");
        // Only the cell whose ts differs from the row max is recorded.
        assert_eq!(cwt.get("early"), Some(&100));
        assert_eq!(cwt.get("late"), None);
        // The lookup helper falls back to the row timestamp for unrecorded cells.
        assert_eq!(mutation.cell_write_timestamp("early"), 100);
        assert_eq!(mutation.cell_write_timestamp("late"), 300);
    }

    /// Issue #1018: a single-writetime row leaves the per-cell side-channel unset
    /// (zero behavior change for the common case).
    #[test]
    fn merge_entry_single_writetime_row_has_no_per_cell_overrides() {
        use crate::schema::{KeyColumn, TableSchema};
        use crate::storage::write_engine::mutation::DecoratedKey;
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let cell = |column: &str| CellData {
            column: column.to_string(),
            value: Value::text(column.to_string()),
            timestamp: 555,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        };

        let entry = MergeEntry::new(
            0,
            DecoratedKey::new(1000, 7i32.to_be_bytes().to_vec()),
            None,
            555,
            RowData::Live {
                cells: vec![cell("a"), cell("b")],
            },
        );

        let mutation =
            KWayMerger::merge_entry_to_mutation(entry, &schema).expect("conversion should succeed");
        assert!(
            mutation.cell_write_timestamps.is_none(),
            "single-writetime row must not record any per-cell overrides"
        );
        assert_eq!(mutation.cell_write_timestamp("a"), 555);
    }

    #[test]
    fn test_merge_entry_to_mutation_tombstone() {
        use crate::schema::{KeyColumn, TableSchema};
        use crate::storage::write_engine::mutation::{CellOperation, DecoratedKey};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let key_bytes = 7i32.to_be_bytes().to_vec();

        let entry = MergeEntry::new(
            0,
            DecoratedKey::new(500, key_bytes),
            None,
            888_000_000,
            RowData::Tombstone {
                deletion_time: 888_000_000,
                local_deletion_time: 1_700_000_000,
            },
        );

        let mutation =
            KWayMerger::merge_entry_to_mutation(entry, &schema).expect("conversion should succeed");

        assert_eq!(mutation.operations.len(), 1);
        assert!(
            matches!(mutation.operations[0], CellOperation::DeleteRow),
            "Expected DeleteRow operation for tombstone entry"
        );
    }

    /// Regression (#853/#886 branch-review, Finding 2): compaction baseline
    /// seeding must NOT discard a far-future local-deletion-time in [2^31, 2^32).
    /// Such LDTs are legitimate (negative i32 bit patterns) after the deletion
    /// marker fixes (#853 / range tombstones). The parser reconstructs them as i64
    /// values ABOVE i32::MAX, and the old `ldt < i32::MAX` guard dropped them, so
    /// the seeded `DataWriter` baseline diverged from the final Statistics.db
    /// baseline. The fix normalizes as unsigned-32 and reinterprets the bits as
    /// i32.
    ///
    /// End-to-end: write a Statistics.db whose `min_local_deletion_time` is a
    /// far-future value, then assert `compute_baseline_min` seeds that exact i32
    /// bit pattern (round-tripping through both the writer's i32->u32 header
    /// encoding and the parser).
    #[test]
    fn compute_baseline_min_keeps_far_future_ldt() {
        use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("temp dir");
        // compute_baseline_min derives the Statistics.db path from a Data.db path.
        let data_path = tmp.path().join("nb-1-big-Data.db");
        std::fs::write(&data_path, b"").expect("touch Data.db");
        let stats_path = tmp.path().join("nb-1-big-Statistics.db");

        // Far-future LDT in [2^31, 2^32): 2^31 + 5, a negative i32 bit pattern.
        let far_future_bits: i32 = ((1u32 << 31) + 5) as i32;
        assert!(
            far_future_bits < 0,
            "sanity: far-future LDT is negative i32"
        );

        let mut meta = StatisticsMetadata::new();
        // A REAL far-future tombstone: drive it through `update_local_deletion_time`
        // (not a bare field assignment) so BOTH the min AND the tombstone histogram are
        // populated — the authoritative "this SSTable has a tombstone" signal the
        // EncodingStats no-deletion sentinel now keys on (#1410).
        meta.update_local_deletion_time(far_future_bits);
        StatisticsWriter::new(stats_path)
            .write(&meta, None)
            .expect("write Statistics.db with far-future LDT");

        let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[data_path]);
        assert_eq!(
            baseline_ldt, far_future_bits,
            "far-future LDT baseline must round-trip as its i32 bit pattern, not be dropped"
        );
    }

    /// The live/no-deletion sentinel (i32::MAX, DeletionTime.LIVE) must NOT lower
    /// the seeded baseline: a Statistics.db with no tombstones leaves the baseline
    /// at i32::MAX.
    #[test]
    fn compute_baseline_min_skips_live_sentinel() {
        use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("temp dir");
        let data_path = tmp.path().join("nb-1-big-Data.db");
        std::fs::write(&data_path, b"").expect("touch Data.db");
        let stats_path = tmp.path().join("nb-1-big-Statistics.db");

        // i32::MAX = no deletions (DeletionTime.LIVE sentinel).
        let mut meta = StatisticsMetadata::new();
        meta.min_local_deletion_time = i32::MAX;
        StatisticsWriter::new(stats_path)
            .write(&meta, None)
            .expect("write Statistics.db with LIVE sentinel");

        let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[data_path]);
        assert_eq!(
            baseline_ldt,
            i32::MAX,
            "live/no-deletion sentinel must not lower the seeded baseline"
        );
    }

    /// #1410: a LIVE-ONLY input (no tombstones recorded → EMPTY tombstone histogram)
    /// must NOT drag the merged LDT baseline down. `compute_baseline_min` excludes it
    /// via the authoritative `tombstone_drop_times` histogram (empty ⇒ no tombstone),
    /// matching `EncodingStats.merge`, which treats a no-deletion input as the merge
    /// identity. When a live-only input is mixed with a tombstone-carrying input (a
    /// real wall-clock LDT), the seeded baseline must equal the TOMBSTONE's LDT.
    /// Regressing this re-introduces the #1410 wrong-delta (raw LDT vs `LDT - minLDT`).
    #[test]
    fn compute_baseline_min_skips_no_deletion_sentinel_mixed_with_tombstone() {
        use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("temp dir");

        // Live-only input: NO tombstones recorded (empty drop-time histogram).
        let live_data = tmp.path().join("nb-1-big-Data.db");
        std::fs::write(&live_data, b"").expect("touch live Data.db");
        let live_meta = StatisticsMetadata::new(); // no update_local_deletion_time call
        StatisticsWriter::new(tmp.path().join("nb-1-big-Statistics.db"))
            .write(&live_meta, None)
            .expect("write live-only Statistics.db");

        // Tombstone-carrying input: a real wall-clock LDT, driven through the
        // authentic `update_local_deletion_time` path (populates min AND histogram).
        let tomb_ldt: i32 = 1_782_950_059; // 2026-07-01 (matches the #1387 fixture LDT).
        let tomb_data = tmp.path().join("nb-2-big-Data.db");
        std::fs::write(&tomb_data, b"").expect("touch tombstone Data.db");
        let mut tomb_meta = StatisticsMetadata::new();
        tomb_meta.update_local_deletion_time(tomb_ldt);
        StatisticsWriter::new(tmp.path().join("nb-2-big-Statistics.db"))
            .write(&tomb_meta, None)
            .expect("write tombstone Statistics.db");

        let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[live_data, tomb_data]);
        assert_eq!(
            baseline_ldt, tomb_ldt,
            "the live-only input's no-deletion sentinel must not lower the baseline \
             below the tombstone input's real LDT (#1410)"
        );
    }

    /// #1410: an ALL-live compaction (every input a no-deletion sentinel) leaves the
    /// LDT baseline at its `i32::MAX` "unseeded" value, so the writer falls back to its
    /// own live-sentinel encoding rather than being dragged down.
    #[test]
    fn compute_baseline_min_all_live_stays_unseeded() {
        use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("temp dir");
        let data_path = tmp.path().join("nb-1-big-Data.db");
        std::fs::write(&data_path, b"").expect("touch Data.db");
        let meta = StatisticsMetadata::new(); // no tombstones -> no-deletion sentinel
        StatisticsWriter::new(tmp.path().join("nb-1-big-Statistics.db"))
            .write(&meta, None)
            .expect("write live-only Statistics.db");

        let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[data_path]);
        assert_eq!(
            baseline_ldt,
            i32::MAX,
            "an all-live compaction must leave the LDT baseline unseeded"
        );
    }

    /// #1410 regression guard: a GENUINE tombstone whose real `localDeletionTime` is a
    /// tiny value (e.g. `0`, as an old row tombstone with a sub-second write timestamp
    /// produces — the compaction_integration scenario) must be INCLUDED in the LDT
    /// baseline, NOT mistaken for the no-deletion sentinel. Driving it through
    /// `update_local_deletion_time` populates the tombstone histogram, so
    /// `compute_baseline_min` sees a non-empty `tombstone_drop_times` and includes the
    /// real `0`, taking the baseline to `0` — otherwise the writer would reject the
    /// re-emitted below-baseline LDT.
    #[test]
    fn compute_baseline_min_includes_genuine_zero_ldt_tombstone() {
        use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("temp dir");
        let data_path = tmp.path().join("nb-1-big-Data.db");
        std::fs::write(&data_path, b"").expect("touch Data.db");
        let mut meta = StatisticsMetadata::new();
        meta.update_local_deletion_time(0); // real tombstone at LDT 0 (histogram set)
        StatisticsWriter::new(tmp.path().join("nb-1-big-Statistics.db"))
            .write(&meta, None)
            .expect("write Statistics.db with a genuine LDT-0 tombstone");

        let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[data_path]);
        assert_eq!(
            baseline_ldt, 0,
            "a genuine LDT-0 tombstone must be included in the baseline, not excluded \
             as the no-deletion sentinel (#1410)"
        );
    }

    /// #1410 (roborev Finding 3): when the STATS-extras histogram decode FAILS
    /// (corrupt / version-mismatched STATS component) `compute_baseline_min` must
    /// stay CONSERVATIVE and INCLUDE the input's LDT baseline — it must NOT treat an
    /// extras PARSE FAILURE like an empty histogram (which would skip a possibly-real
    /// LDT baseline and make the writer reject a re-emitted below-baseline tombstone).
    ///
    /// Setup: write a valid tombstone-carrying `Statistics.db`, then corrupt the FIRST
    /// 4 bytes of the STATS component (the `estimatedPartitionSize` histogram bucket
    /// count) to a negative i32 so `parse_stats_extras` fails with `Corruption`, while
    /// the SERIALIZATION_HEADER EncodingStats (reached via its own TOC offset, a LATER
    /// component) still decodes. Assert the tombstone LDT is included regardless.
    #[test]
    fn compute_baseline_min_conservative_when_stats_extras_unparseable() {
        use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("temp dir");
        let data_path = tmp.path().join("nb-1-big-Data.db");
        std::fs::write(&data_path, b"").expect("touch Data.db");
        let stats_path = tmp.path().join("nb-1-big-Statistics.db");

        let tomb_ldt: i32 = 1_782_950_059; // a real wall-clock tombstone LDT.
        let mut meta = StatisticsMetadata::new();
        meta.update_local_deletion_time(tomb_ldt);
        StatisticsWriter::new(stats_path.clone())
            .write(&meta, None)
            .expect("write tombstone Statistics.db");

        // Locate the STATS component (MetadataType ordinal 2) offset from the TOC and
        // corrupt its first field (estimatedPartitionSize histogram bucket count) to a
        // negative i32 so parse_stats_extras fails, without touching the header offset.
        let mut bytes = std::fs::read(&stats_path).expect("read stats");
        let count = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let mut stats_offset = None;
        for i in 0..count {
            let entry = 8 + i * 8; // after count(4)+CRC(4); each entry = type(4)+offset(4)
            let ty = u32::from_be_bytes([
                bytes[entry],
                bytes[entry + 1],
                bytes[entry + 2],
                bytes[entry + 3],
            ]);
            if ty == 2 {
                stats_offset = Some(u32::from_be_bytes([
                    bytes[entry + 4],
                    bytes[entry + 5],
                    bytes[entry + 6],
                    bytes[entry + 7],
                ]) as usize);
            }
        }
        let off = stats_offset.expect("STATS component (type 2) in TOC");
        // Negative bucket count → skip_estimated_histogram returns Corruption.
        bytes[off..off + 4].copy_from_slice(&(-1i32).to_be_bytes());
        std::fs::write(&stats_path, &bytes).expect("rewrite corrupted stats");

        let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[data_path]);
        assert_eq!(
            baseline_ldt, tomb_ldt,
            "an unparseable STATS-extras section must be treated conservatively \
             (INCLUDE the LDT baseline), not as an empty no-tombstone histogram (#1410)"
        );
    }

    /// #2299 / roborev job 1723 — fail-CLOSED guard for the direct-stream gate.
    ///
    /// `compute_baseline_min` fails OPEN: an input with a MISSING or top-level
    /// UNPARSEABLE `Statistics.db` never lowers `baseline_min_ldt`, so a skipped
    /// tombstone-bearing input would leave the live sentinel intact and the
    /// `#2299` gate (`baseline_min_ldt == i32::MAX`) would wrongly select the
    /// direct-stream path, dropping that input's tombstones (data resurrection).
    /// `all_input_stats_readable` closes that hole: it returns `false` unless EVERY
    /// input's deletion metadata was actually observed, letting the caller force
    /// the always-correct buffered path. Removing the AND in `compaction.rs`
    /// re-opens the data-loss hole and re-greens this test's negative cases.
    #[test]
    fn all_input_stats_readable_fails_closed_on_missing_or_unparseable_stats() {
        use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("temp dir");

        // A well-formed input: Data.db + a valid Statistics.db written through the
        // real writer. Readable → the guard permits proving "no deletions".
        let good_data = tmp.path().join("nb-1-big-Data.db");
        std::fs::write(&good_data, b"").expect("touch good Data.db");
        StatisticsWriter::new(tmp.path().join("nb-1-big-Statistics.db"))
            .write(&StatisticsMetadata::new(), None)
            .expect("write valid Statistics.db");
        assert!(
            all_input_stats_readable(std::slice::from_ref(&good_data)),
            "a well-formed input with a valid Statistics.db must be readable"
        );

        // Missing Statistics.db (roborev's primary scenario): the paired
        // Statistics.db is simply absent. Must fail closed.
        let missing_data = tmp.path().join("nb-2-big-Data.db");
        std::fs::write(&missing_data, b"").expect("touch missing-stats Data.db");
        // (deliberately write NO nb-2-big-Statistics.db)
        assert!(
            !all_input_stats_readable(std::slice::from_ref(&missing_data)),
            "an input whose Statistics.db is missing must NOT be provably deletion-free"
        );

        // Top-level-unparseable Statistics.db: too short to even carry the TOC
        // count/CRC header, so `parse_statistics_with_fallback` errors. Must fail
        // closed (do not infer "no deletions" from an undecodable component).
        let corrupt_data = tmp.path().join("nb-3-big-Data.db");
        std::fs::write(&corrupt_data, b"").expect("touch corrupt-stats Data.db");
        std::fs::write(tmp.path().join("nb-3-big-Statistics.db"), b"\x00\x00")
            .expect("write truncated Statistics.db");
        assert!(
            !all_input_stats_readable(std::slice::from_ref(&corrupt_data)),
            "an input whose Statistics.db is unparseable at the top level must fail closed"
        );

        // MIXED: one readable input + one missing-stats input. A single unreadable
        // input taints the whole merge — the guard must fail closed so the direct
        // path is never taken when any input's deletion metadata is unknown.
        assert!(
            !all_input_stats_readable(&[good_data, missing_data, corrupt_data]),
            "any single unreadable input must force the whole merge to fail closed"
        );
    }

    /// #935: `compute_max_purgeable_timestamp` returns the MIN write timestamp
    /// across the non-included overlapping SSTables (from each `Statistics.db`
    /// `min_timestamp`), so a tombstone older than that bound can be purged.
    #[test]
    fn compute_max_purgeable_timestamp_returns_min_over_outside_sstables() {
        use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("temp dir");
        let write_stats = |gen: u32, min_ts: i64| -> PathBuf {
            let data_path = tmp.path().join(format!("nb-{gen}-big-Data.db"));
            std::fs::write(&data_path, b"").expect("touch Data.db");
            let stats_path = tmp.path().join(format!("nb-{gen}-big-Statistics.db"));
            let mut meta = StatisticsMetadata::new();
            meta.min_timestamp = min_ts;
            StatisticsWriter::new(stats_path)
                .write(&meta, None)
                .expect("write Statistics.db");
            data_path
        };

        let a = write_stats(1, 5_000);
        let b = write_stats(2, 2_500);
        let c = write_stats(3, 9_000);

        let bound = compute_max_purgeable_timestamp(&[a, b, c]);
        assert_eq!(
            bound,
            Some(2_500),
            "the bound must be the minimum min_timestamp across all outside SSTables"
        );

        // Empty outside set → None (caller treats as full / no overlap).
        assert_eq!(
            compute_max_purgeable_timestamp(&[]),
            None,
            "no outside SSTables means no overlap bound"
        );
    }

    /// #935: a missing/unreadable outside `Statistics.db` leaves the bound UNKNOWN
    /// (returns `None`), disabling overlap-aware purging — never resurrect data.
    #[test]
    fn compute_max_purgeable_timestamp_unreadable_outside_disables_purging() {
        use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("temp dir");
        // One readable outside SSTable.
        let readable = tmp.path().join("nb-1-big-Data.db");
        std::fs::write(&readable, b"").expect("touch Data.db");
        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = 1_000;
        StatisticsWriter::new(tmp.path().join("nb-1-big-Statistics.db"))
            .write(&meta, None)
            .expect("write Statistics.db");

        // A second outside SSTable whose Statistics.db does NOT exist.
        let missing = tmp.path().join("nb-2-big-Data.db");
        std::fs::write(&missing, b"").expect("touch Data.db");

        assert_eq!(
            compute_max_purgeable_timestamp(&[readable, missing]),
            None,
            "an unreadable outside Statistics.db must disable overlap-aware purging"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Property tests for compaction merge semantics (Issue #475, Epic #469)
// ─────────────────────────────────────────────────────────────────────────────
//
// Strategy: define a small in-memory `reference_merge` that applies the full
// Cassandra per-key merge rules (timestamp LWW, tombstone shadowing, TTL expiry,
// range tombstone application), generate randomised cell streams with proptest,
// and assert that both the reference and the real KWayMerger agree.
//
// Three coverage areas required by the issue:
//  A. Tombstone shadowing   – delete-ts > write-ts => cell suppressed
//  B. TTL expiry            – write with TTL whose local_deletion_time < merge_time => dropped
//  C. Range tombstone       – row in range with marked_for_delete_at >= cell-ts => dropped
//
// The reference implementation is tested directly via proptest.
// The real merger (merge_partition_rows) is also exercised for the cases it
// handles today (LWW by timestamp for live rows and tombstones).

#[cfg(all(test, feature = "write-support"))]
mod merge_property_tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;

    // ─── Fixed "wall clock" used in all TTL expiry tests ─────────────────────
    // Unix seconds; cells with local_deletion_time < MERGE_TIME_SECS are expired.
    const MERGE_TIME_SECS: i32 = 1_000;

    // ─── Cell operation model ─────────────────────────────────────────────────

    /// The three kinds of cell operations that a compaction must resolve.
    #[derive(Debug, Clone)]
    enum CellOp {
        /// A live write: column <- value, recorded at `timestamp`.
        /// When `local_deletion_time` is Some(t) it is an expiring cell; the
        /// cell is considered dead when `t < MERGE_TIME_SECS`.
        Write {
            timestamp: i64,
            local_deletion_time: Option<i32>,
        },
        /// A cell tombstone (DELETE column): column is dead at `timestamp`.
        Delete { timestamp: i64 },
        /// A range tombstone covering the inclusive integer range
        /// `[start_ck, end_ck]`. Any row whose clustering key integer falls
        /// within the range and whose write-timestamp <= `marked_for_delete_at`
        /// is suppressed.
        RangeTombstone {
            start_ck: u8,
            end_ck: u8,
            marked_for_delete_at: i64,
        },
    }

    /// A single entry in the randomised cell stream.
    ///
    /// We work with small integer partition/clustering/column spaces so
    /// collisions occur frequently and the interesting merge cases arise.
    #[derive(Debug, Clone)]
    struct CellInput {
        /// 0..4
        partition: u8,
        /// 0..4
        clustering: u8,
        /// 0..3
        column: u8,
        op: CellOp,
    }

    // ─── Key type for the merged output map ──────────────────────────────────

    /// (partition, clustering, column) triple identifying a unique cell slot.
    type CellKey = (u8, u8, u8);

    /// What the reference merge produces for a cell slot.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum MergedCell {
        /// The cell is alive with the given write timestamp.
        Live { timestamp: i64 },
        /// The cell is a tombstone (deleted) at the given timestamp.
        Dead { timestamp: i64 },
    }

    // ─── Reference implementation ─────────────────────────────────────────────

    /// Reference merge over a flat cell-stream, applying full Cassandra rules.
    ///
    /// Rules (applied in order):
    /// 1. Per (partition, clustering, column), keep the op with the highest
    ///    `timestamp`. Ties: Delete wins over Write (Cassandra reconcile).
    /// 2. A `RangeTombstone` with `marked_for_delete_at >= cell.timestamp`
    ///    covering a clustering key suppresses the live cell in that slot.
    /// 3. A `Write` whose `local_deletion_time < MERGE_TIME_SECS` (TTL expired)
    ///    is dropped from the output even if it has the highest timestamp.
    fn reference_merge(inputs: &[CellInput]) -> HashMap<CellKey, MergedCell> {
        // ── Step 1: per-slot LWW ──────────────────────────────────────────────
        let mut per_slot: HashMap<CellKey, MergedCell> = HashMap::new();

        // Collect range tombstones grouped by (partition, clustering range).
        let mut range_tombstones: Vec<CellInput> = Vec::new();

        for ci in inputs {
            match &ci.op {
                CellOp::RangeTombstone { .. } => {
                    range_tombstones.push(ci.clone());
                }
                CellOp::Write {
                    timestamp,
                    local_deletion_time,
                } => {
                    // TTL expiry: drop the write if its local_deletion_time has passed.
                    if local_deletion_time
                        .map(|ldt| ldt < MERGE_TIME_SECS)
                        .unwrap_or(false)
                    {
                        // Expired: treat as if this write never happened.
                        continue;
                    }
                    let key = (ci.partition, ci.clustering, ci.column);
                    let candidate = MergedCell::Live {
                        timestamp: *timestamp,
                    };
                    per_slot
                        .entry(key)
                        .and_modify(|existing| {
                            match existing {
                                MergedCell::Live { timestamp: ex_ts } => {
                                    if *timestamp > *ex_ts {
                                        *existing = candidate.clone();
                                    }
                                }
                                MergedCell::Dead { timestamp: ex_ts } => {
                                    // Dead wins over a live cell at the same timestamp;
                                    // only replace if the write is strictly newer.
                                    if *timestamp > *ex_ts {
                                        *existing = candidate.clone();
                                    }
                                }
                            }
                        })
                        .or_insert(candidate);
                }
                CellOp::Delete { timestamp } => {
                    let key = (ci.partition, ci.clustering, ci.column);
                    let candidate = MergedCell::Dead {
                        timestamp: *timestamp,
                    };
                    per_slot
                        .entry(key)
                        .and_modify(|existing| {
                            match existing {
                                MergedCell::Live { timestamp: ex_ts } => {
                                    if *timestamp >= *ex_ts {
                                        // Delete wins at equal timestamp (Cassandra rule).
                                        *existing = candidate.clone();
                                    }
                                }
                                MergedCell::Dead { timestamp: ex_ts } => {
                                    if *timestamp > *ex_ts {
                                        *existing = candidate.clone();
                                    }
                                }
                            }
                        })
                        .or_insert(candidate);
                }
            }
        }

        // ── Step 2: apply range tombstones ────────────────────────────────────
        // A range tombstone suppresses a live cell when:
        //   - partition matches
        //   - clustering key is within [start_ck, end_ck]
        //   - marked_for_delete_at >= cell write timestamp
        per_slot.retain(|&(pk, ck, _col), cell| {
            for rt in &range_tombstones {
                if rt.partition != pk {
                    continue;
                }
                if let CellOp::RangeTombstone {
                    start_ck,
                    end_ck,
                    marked_for_delete_at,
                } = rt.op
                {
                    if ck >= start_ck && ck <= end_ck {
                        if let MergedCell::Live { timestamp } = cell {
                            if marked_for_delete_at >= *timestamp {
                                return false; // suppressed
                            }
                        }
                    }
                }
            }
            true
        });

        // Dead cells (tombstones) are kept in the output so callers can verify
        // they appear rather than a live cell with a lower timestamp.
        per_slot
    }

    // ─── Proptest strategies ──────────────────────────────────────────────────

    fn arb_timestamp() -> impl Strategy<Value = i64> {
        1i64..=20i64
    }

    /// local_deletion_time: sometimes None, sometimes expired (<MERGE_TIME_SECS),
    /// sometimes live (>=MERGE_TIME_SECS).
    fn arb_local_deletion_time() -> impl Strategy<Value = Option<i32>> {
        prop_oneof![
            3 => Just(None),                         // no TTL
            1 => (990i32..=999i32).prop_map(Some),   // expired TTL
            1 => (1000i32..=1010i32).prop_map(Some), // live TTL
        ]
    }

    fn arb_cell_op() -> impl Strategy<Value = CellOp> {
        prop_oneof![
            5 => (arb_timestamp(), arb_local_deletion_time())
                    .prop_map(|(ts, ldt)| CellOp::Write {
                        timestamp: ts,
                        local_deletion_time: ldt,
                    }),
            3 => arb_timestamp().prop_map(|ts| CellOp::Delete { timestamp: ts }),
            2 => (0u8..=3u8, 0u8..=3u8, arb_timestamp()).prop_map(|(s, e, ts)| {
                    let (start_ck, end_ck) = if s <= e { (s, e) } else { (e, s) };
                    CellOp::RangeTombstone {
                        start_ck,
                        end_ck,
                        marked_for_delete_at: ts,
                    }
                }),
        ]
    }

    fn arb_cell_input() -> impl Strategy<Value = CellInput> {
        (0u8..4u8, 0u8..4u8, 0u8..3u8, arb_cell_op()).prop_map(
            |(partition, clustering, column, op)| CellInput {
                partition,
                clustering,
                column,
                op,
            },
        )
    }

    fn arb_cell_stream() -> impl Strategy<Value = Vec<CellInput>> {
        prop::collection::vec(arb_cell_input(), 4..=32)
    }

    // ─── Helper: sort merged output for stable comparison ─────────────────────
    fn sorted_keys(m: &HashMap<CellKey, MergedCell>) -> Vec<(CellKey, MergedCell)> {
        let mut v: Vec<_> = m.iter().map(|(&k, v)| (k, v.clone())).collect();
        v.sort_by_key(|(k, _)| *k);
        v
    }

    // ─── Deterministic unit tests for reference semantics ────────────────────

    #[test]
    fn ref_tombstone_shadows_earlier_write() {
        // Write at ts=5, then Delete at ts=10 => cell must be Dead(10).
        let inputs = vec![
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::Write {
                    timestamp: 5,
                    local_deletion_time: None,
                },
            },
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::Delete { timestamp: 10 },
            },
        ];
        let result = reference_merge(&inputs);
        assert_eq!(
            result.get(&(0, 0, 0)),
            Some(&MergedCell::Dead { timestamp: 10 }),
            "Delete(ts=10) must shadow Write(ts=5)"
        );
    }

    #[test]
    fn ref_write_not_shadowed_by_older_tombstone() {
        // Write at ts=10, Delete at ts=5 => cell must be Live(10).
        let inputs = vec![
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::Write {
                    timestamp: 10,
                    local_deletion_time: None,
                },
            },
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::Delete { timestamp: 5 },
            },
        ];
        let result = reference_merge(&inputs);
        assert_eq!(
            result.get(&(0, 0, 0)),
            Some(&MergedCell::Live { timestamp: 10 }),
            "Write(ts=10) must win over Delete(ts=5)"
        );
    }

    #[test]
    fn ref_delete_wins_at_equal_timestamp() {
        // Write at ts=5, Delete at ts=5 => Delete must win (Cassandra reconcile).
        let inputs = vec![
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::Write {
                    timestamp: 5,
                    local_deletion_time: None,
                },
            },
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::Delete { timestamp: 5 },
            },
        ];
        let result = reference_merge(&inputs);
        assert_eq!(
            result.get(&(0, 0, 0)),
            Some(&MergedCell::Dead { timestamp: 5 }),
            "Delete must win at equal timestamp (Cassandra reconcile rule)"
        );
    }

    #[test]
    fn ref_expired_ttl_drops_cell() {
        // Write at ts=5 with local_deletion_time=500 (< MERGE_TIME_SECS=1000).
        // No other ops => cell should be absent from merged output.
        let inputs = vec![CellInput {
            partition: 0,
            clustering: 0,
            column: 0,
            op: CellOp::Write {
                timestamp: 5,
                local_deletion_time: Some(500), // expired
            },
        }];
        let result = reference_merge(&inputs);
        assert!(
            !result.contains_key(&(0, 0, 0)),
            "Expired TTL cell must be absent from merged output"
        );
    }

    #[test]
    fn ref_live_ttl_keeps_cell() {
        // Write at ts=5 with local_deletion_time=1500 (>= MERGE_TIME_SECS=1000).
        // Cell should still be present.
        let inputs = vec![CellInput {
            partition: 0,
            clustering: 0,
            column: 0,
            op: CellOp::Write {
                timestamp: 5,
                local_deletion_time: Some(1500), // not expired
            },
        }];
        let result = reference_merge(&inputs);
        assert_eq!(
            result.get(&(0, 0, 0)),
            Some(&MergedCell::Live { timestamp: 5 }),
            "Non-expired TTL cell must be present"
        );
    }

    #[test]
    fn ref_range_tombstone_suppresses_row_in_range() {
        // Write at ts=5 for clustering key 2 in partition 0.
        // RangeTombstone covering [0, 5] with marked_for_delete_at=10 => suppressed.
        let inputs = vec![
            CellInput {
                partition: 0,
                clustering: 2,
                column: 0,
                op: CellOp::Write {
                    timestamp: 5,
                    local_deletion_time: None,
                },
            },
            CellInput {
                partition: 0,
                clustering: 0, // column/clustering fields ignored for RT; range is in op
                column: 0,
                op: CellOp::RangeTombstone {
                    start_ck: 0,
                    end_ck: 5,
                    marked_for_delete_at: 10,
                },
            },
        ];
        let result = reference_merge(&inputs);
        assert!(
            !result.contains_key(&(0, 2, 0)),
            "Cell with ts=5 at clustering=2 must be suppressed by RangeTombstone(mfda=10, [0,5])"
        );
    }

    #[test]
    fn ref_range_tombstone_does_not_suppress_newer_write() {
        // Write at ts=15, RangeTombstone with mfda=10 => not suppressed.
        let inputs = vec![
            CellInput {
                partition: 0,
                clustering: 2,
                column: 0,
                op: CellOp::Write {
                    timestamp: 15,
                    local_deletion_time: None,
                },
            },
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::RangeTombstone {
                    start_ck: 0,
                    end_ck: 5,
                    marked_for_delete_at: 10,
                },
            },
        ];
        let result = reference_merge(&inputs);
        assert_eq!(
            result.get(&(0, 2, 0)),
            Some(&MergedCell::Live { timestamp: 15 }),
            "Write(ts=15) must NOT be suppressed by RangeTombstone(mfda=10)"
        );
    }

    #[test]
    fn ref_range_tombstone_only_applies_within_partition() {
        // Write in partition 1, RangeTombstone in partition 0 => not suppressed.
        let inputs = vec![
            CellInput {
                partition: 1,
                clustering: 2,
                column: 0,
                op: CellOp::Write {
                    timestamp: 5,
                    local_deletion_time: None,
                },
            },
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::RangeTombstone {
                    start_ck: 0,
                    end_ck: 5,
                    marked_for_delete_at: 10,
                },
            },
        ];
        let result = reference_merge(&inputs);
        assert_eq!(
            result.get(&(1, 2, 0)),
            Some(&MergedCell::Live { timestamp: 5 }),
            "RangeTombstone in partition 0 must not affect partition 1"
        );
    }

    // ─── Property tests ───────────────────────────────────────────────────────

    proptest! {
            #![proptest_config(ProptestConfig::with_cases(64))]

            // Property A: Tombstone shadowing
            // After merging, for every (partition, clustering, column) cell slot, if
            // the reference says Dead(ts=T) then the highest-timestamp Delete in the
            // input stream for that slot must have timestamp T.
            #[test]
            fn prop_tombstone_shadowing_consistent(inputs in arb_cell_stream()) {
                let merged = reference_merge(&inputs);

                for (&(pk, ck, col), cell) in &merged {
                    if let MergedCell::Dead { timestamp: dead_ts } = cell {
                        // Find the highest-timestamp Delete for this slot in the input.
                        let best_delete = inputs.iter()
                            .filter(|ci| ci.partition == pk && ci.clustering == ck && ci.column == col)
                            .filter_map(|ci| {
                                if let CellOp::Delete { timestamp } = ci.op {
                                    Some(timestamp)
                                } else {
                                    None
                                }
                            })
                            .max();

                        prop_assert!(
                            best_delete.is_some(),
                            "Dead cell at ({},{},{}) but no Delete in inputs",
                            pk, ck, col
                        );
                        prop_assert_eq!(
                            best_delete.unwrap(),
                            *dead_ts,
                            "Dead cell timestamp must equal best Delete timestamp for ({},{},{})",
                            pk, ck, col
                        );
                    }
                }
            }

            // Property B: TTL expiry
            // After merging, no cell should be Live if all its Write ops are TTL-expired.
            #[test]
            fn prop_ttl_expiry_no_expired_live_cells(inputs in arb_cell_stream()) {
                let merged = reference_merge(&inputs);

                for (&(pk, ck, col), cell) in &merged {
                    if let MergedCell::Live { .. } = cell {
                        // There must be at least one non-expired Write for this slot.
                        let has_live_write = inputs.iter()
                            .filter(|ci| ci.partition == pk && ci.clustering == ck && ci.column == col)
                            .any(|ci| {
                                if let CellOp::Write { local_deletion_time, .. } = &ci.op {
                                    // A live write: no TTL, or TTL not expired.
                                    local_deletion_time
                                        .map(|ldt| ldt >= MERGE_TIME_SECS)
                                        .unwrap_or(true)
                                } else {
                                    false
                                }
                            });

                        prop_assert!(
                            has_live_write,
                            "Live cell at ({},{},{}) but all writes are expired",
                            pk, ck, col
                        );
                    }
                }
            }

            // Property C: Range tombstone application
            // After merging, no Live cell should exist that is fully covered by a
            // range tombstone whose marked_for_delete_at >= the cell's write timestamp.
            #[test]
            fn prop_range_tombstone_suppresses_covered_live_cells(inputs in arb_cell_stream()) {
                let merged = reference_merge(&inputs);

                // Collect all range tombstones from the input stream.
                let range_tombstones: Vec<(u8, u8, u8, i64)> = inputs.iter()
                    .filter_map(|ci| {
                        if let CellOp::RangeTombstone { start_ck, end_ck, marked_for_delete_at } = ci.op {
                            Some((ci.partition, start_ck, end_ck, marked_for_delete_at))
                        } else {
                            None
                        }
                    })
                    .collect();

                for (&(pk, ck, _col), cell) in &merged {
                    if let MergedCell::Live { timestamp } = cell {
                        // Verify no range tombstone shadows this cell.
                        for &(rt_pk, start_ck, end_ck, mfda) in &range_tombstones {
                            if rt_pk == pk && ck >= start_ck && ck <= end_ck && mfda >= *timestamp {
                                prop_assert!(
                                    false,
                                    "Live cell at ({},{}) ts={} should be suppressed by \
                                     RangeTombstone(part={}, [{},{}], mfda={})",
                                    pk, ck, timestamp, rt_pk, start_ck, end_ck, mfda
                                );
                            }
                        }
                    }
                }
            }

            // Property D: LWW correctness
            // Every Live cell in the merged output must have a timestamp equal to
            // the maximum non-expired Write timestamp for that cell slot.
            #[test]
            fn prop_live_cell_has_max_write_timestamp(inputs in arb_cell_stream()) {
                let merged = reference_merge(&inputs);

                for (&(pk, ck, col), cell) in &merged {
                    if let MergedCell::Live { timestamp: live_ts } = cell {
                        // Find the maximum non-expired Write timestamp for this slot.
                        let max_ts = inputs.iter()
                            .filter(|ci| ci.partition == pk && ci.clustering == ck && ci.column == col)
                            .filter_map(|ci| {
                                if let CellOp::Write { timestamp, local_deletion_time } = &ci.op {
                                    // Only include non-expired writes.
                                    let not_expired = local_deletion_time
                                        .map(|ldt| ldt >= MERGE_TIME_SECS)
                                        .unwrap_or(true);
                                    if not_expired { Some(*timestamp) } else { None }
                                } else {
                                    None
                                }
                            })
                            .max();

                        prop_assert_eq!(
                            max_ts,
                            Some(*live_ts),
                            "Live cell at ({},{},{}) must have max non-expired write timestamp",
                            pk, ck, col
                        );
                    }
                }
            }

            // Property E: Output is deterministic (idempotent reference)
            // Calling reference_merge twice on the same input produces identical output.
            #[test]
            fn prop_reference_merge_is_deterministic(inputs in arb_cell_stream()) {
                let result_a = reference_merge(&inputs);
                let result_b = reference_merge(&inputs);
                prop_assert_eq!(
                    sorted_keys(&result_a),
                    sorted_keys(&result_b),
                    "reference_merge must be deterministic"
                );
            }

            // Property F: Real merger LWW parity
            // For cell streams containing only non-expired Writes (no Deletes,
            // no RangeTombstones, no TTL), the real KWayMerger.merge_partition_rows
            // must agree with the reference on which row wins per clustering key.
            //
            // We drive merge_partition_rows directly with synthetic MergeEntry inputs
            // that represent the Write ops.
            #[test]
            fn prop_real_merger_lww_agrees_with_reference(
                entries in prop::collection::vec(
                    // (clustering_key 0..4, run_index 0..2, timestamp 1..20)
                    (0u8..4u8, 0usize..2usize, 1i64..=20i64),
                    2..=12usize,
                )
            ) {
                use crate::schema::{Column, KeyColumn};
                use std::collections::HashMap as SchemaMap;

                let schema = TableSchema {
                    keyspace: "prop_test_ks".to_string(),
                    table: "prop_test_table".to_string(),
                    partition_keys: vec![KeyColumn {
                        name: "id".to_string(),
                        data_type: "int".to_string(),
                        position: 0,
                    }],
                    clustering_keys: vec![],
                    columns: vec![Column {
                        name: "value".to_string(),
                        data_type: "text".to_string(),
                        nullable: true,
                        default: None,
                        is_static: false,
                    }],
                    comments: SchemaMap::new(),
                    dropped_columns: SchemaMap::new(),
                };

                // Build MergeEntry stream — one per (ck, run_index, timestamp) tuple.
                // All entries share the same partition.
                let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);
                let merge_entries: Vec<MergeEntry> = entries.iter().map(|&(ck, run_index, ts)| {
                    let ck_key = ClusteringKey {
                        columns: vec![("ck".to_string(), Value::TinyInt(ck as i8))],
                    };
                    MergeEntry::new(
                        run_index,
                        partition_key.clone(),
                        Some(ck_key),
                        ts,
                        RowData::Live {
                            cells: vec![CellData {
                                column: "value".to_string(),
                                value: Value::Integer(ts as i32),
                                timestamp: ts,
                                ttl: None,
                                cell_path: None,
                                local_deletion_time: None,
                                                        is_complex_element: false,
                                is_deleted: false,
                                has_empty_value: false,
    }],
                        },
                    )
                }).collect();

                // Drive the real merger.
                let merger = KWayMerger {
                    runs: vec![],
                    heap: std::collections::BinaryHeap::new(),
                    current_partition: None,
                    gc_before_secs: None,
                    now_secs: None,
                    purge_safe: false,
                    max_purgeable_timestamp: None,
                    schema: schema.clone(),
                    schema_arc: std::sync::Arc::new(schema.clone()),
                    _egress_slot: None,
                };
                let real_merged = merger.merge_partition_rows(merge_entries.clone())
                    .expect("merge_partition_rows must not fail");

                // Build the reference result: per clustering-key int, highest timestamp wins.
                // (run_index as tie-breaker: lower run_index wins at equal ts — same as merger)
                let mut ref_map: HashMap<u8, (i64, usize)> = HashMap::new();
                for &(ck, run_index, ts) in &entries {
                    ref_map.entry(ck)
                        .and_modify(|(best_ts, best_run)| {
                            if ts > *best_ts || (ts == *best_ts && run_index < *best_run) {
                                *best_ts = ts;
                                *best_run = run_index;
                            }
                        })
                        .or_insert((ts, run_index));
                }

                // Verify each winner in the real output matches the reference.
                prop_assert_eq!(
                    real_merged.len(),
                    ref_map.len(),
                    "real merger output row count must match reference"
                );

                for entry in &real_merged {
                    let ck_byte = match entry.clustering_key.as_ref()
                        .and_then(|ck| ck.columns.first())
                        .map(|(_, v)| v)
                    {
                        Some(Value::TinyInt(b)) => *b as u8,
                        _ => {
                            prop_assert!(false, "unexpected clustering key value");
                            unreachable!()
                        }
                    };

                    let (ref_ts, _ref_run) = ref_map[&ck_byte];
                    prop_assert_eq!(
                        entry.timestamp,
                        ref_ts,
                        "real merger winner timestamp must match reference for ck={}",
                        ck_byte
                    );
                }
            }

            // Property G: Tombstone wins over live row at same clustering key in real merger
            // When a Tombstone and a Live row have the same clustering key, the one with
            // the higher timestamp must win — and the real merger must reflect this.
            #[test]
            fn prop_real_merger_tombstone_vs_live(
                ts_write in 1i64..=10i64,
                ts_delete in 1i64..=20i64,
            ) {
                use crate::schema::{Column, KeyColumn};
                use std::collections::HashMap as SchemaMap;

                let schema = TableSchema {
                    keyspace: "prop_test_ks".to_string(),
                    table: "prop_test_table".to_string(),
                    partition_keys: vec![KeyColumn {
                        name: "id".to_string(),
                        data_type: "int".to_string(),
                        position: 0,
                    }],
                    clustering_keys: vec![],
                    columns: vec![Column {
                        name: "value".to_string(),
                        data_type: "text".to_string(),
                        nullable: true,
                        default: None,
                        is_static: false,
                    }],
                    comments: SchemaMap::new(),
                    dropped_columns: SchemaMap::new(),
                };

                let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);
                let ck = ClusteringKey {
                    columns: vec![("ck".to_string(), Value::TinyInt(0))],
                };

                // Deliberately give the LIVE row the NEWER file (run_index 0) and the
                // tombstone the OLDER file (run_index 1). A run_index-only tiebreak at
                // equal timestamp would wrongly pick the live row; the Cassandra
                // liveness rule must pick the tombstone regardless of file recency.
                let live_entry = MergeEntry::new(
                    0, // run_index 0 = newer file
                    partition_key.clone(),
                    Some(ck.clone()),
                    ts_write,
                    RowData::Live {
                        cells: vec![CellData {
                            column: "value".to_string(),
                            value: Value::Integer(42),
                            timestamp: ts_write,
                            ttl: None,
                            cell_path: None,
                            local_deletion_time: None,
                                                is_complex_element: false,
                            is_deleted: false,
                            has_empty_value: false,
    }],
                    },
                );
                let tombstone_entry = MergeEntry::new(
                    1, // run_index 1 = older file
                    partition_key.clone(),
                    Some(ck.clone()),
                    ts_delete,
                    RowData::Tombstone {
                        deletion_time: ts_delete,
                        local_deletion_time: 2000,
                    },
                );

                let merger = KWayMerger {
                    runs: vec![],
                    heap: std::collections::BinaryHeap::new(),
                    current_partition: None,
                    gc_before_secs: None,
                    now_secs: None,
                    purge_safe: false,
                    max_purgeable_timestamp: None,
                    schema: schema.clone(),
                    schema_arc: std::sync::Arc::new(schema.clone()),
                    _egress_slot: None,
                };
                let merged = merger.merge_partition_rows(vec![live_entry, tombstone_entry])
                    .expect("merge_partition_rows must not fail");

                prop_assert_eq!(merged.len(), 1, "one clustering key => one merged row");

                let winner = &merged[0];
                if ts_delete > ts_write {
                    // Tombstone has higher timestamp => should win.
                    prop_assert!(
                        matches!(winner.row_data, RowData::Tombstone { .. }),
                        "Tombstone(ts={}) must win over Live(ts={})",
                        ts_delete, ts_write
                    );
                } else if ts_write > ts_delete {
                    // Live write has higher timestamp => should win.
                    prop_assert!(
                        matches!(winner.row_data, RowData::Live { .. }),
                        "Live(ts={}) must win over Tombstone(ts={})",
                        ts_write, ts_delete
                    );
                } else {
                    // Equal timestamps: the tombstone (Delete) ALWAYS wins, matching
                    // Cassandra `Cells#reconcile`. This must hold regardless of file
                    // recency — the assertion previously carved this case out, hiding
                    // the run_index-only tiebreak bug (Issue #498).
                    prop_assert!(
                        matches!(winner.row_data, RowData::Tombstone { .. }),
                        "At equal ts={}, Tombstone must win over Live (Cassandra reconcile rule)",
                        ts_delete
                    );
                }
            }
        }
}

// Streaming channel / cursor mechanism tests (issues #754 / #2820), moved to a
// `*_tests.rs` sibling per the #1116 campsite rule — see that file's header.
#[cfg(all(test, feature = "write-support"))]
#[path = "streaming_channel_tests.rs"]
mod streaming_channel_tests;

// ─────────────────────────────────────────────────────────────────────────────
// Issue #823 (Epic #817): complex-column (multi-cell collection / non-frozen UDT)
// merge behaviour.
//
// ADDITIVE TEST MODULE — flagged per the issue brief. This module is `#[cfg(test)]`
// only and adds NO production code. It exists because the authoritative reconcile
// function `KWayMerger::reconcile_cluster` and the reader→merge adapter
// `SSTableRowIteratorAdapter::value_to_row_data` are private, so the gating
// behaviour can only be value-asserted from inside the `merge` module.
//
// These tests ESTABLISH (do not aspire to) the current behaviour so the findings
// doc can cite real code. They assert the divergence where one exists.
// ─────────────────────────────────────────────────────────────────────────────
#[cfg(all(test, feature = "write-support"))]
mod issue_823_complex_column_merge {
    use super::*;
    use crate::types::{TombstoneInfo, TombstoneType, UdtField, UdtValue, Value};

    fn dk(byte: u8) -> DecoratedKey {
        DecoratedKey::from_key_bytes(vec![byte]).expect("token")
    }

    fn live(run_index: usize, row_ts: i64, cells: Vec<CellData>) -> MergeEntry {
        MergeEntry::new(run_index, dk(1), None, row_ts, RowData::Live { cells })
    }

    fn scalar_cell(column: &str, value: &str, ts: i64) -> CellData {
        CellData::new(column.to_string(), Value::text(value.to_string()), ts)
    }

    // ── Issue #847: dropped-column cell filtering during compaction ──────────
    // A column dropped at drop_time discards cells with `timestamp <= drop_time`;
    // cells written after the drop (re-added) survive. The drop time comes from
    // the `dropped_columns` map (#904). `cell.timestamp` is the cell's OWN
    // writetime (#886/#899 enrichment), so purging is exact per cell even when
    // sibling cells in the same row carry different timestamps (#922 — end-to-end
    // coverage in `tests/issue_922_per_cell_dropped_purge.rs`).

    /// A cell of a dropped column written at/before the drop time is filtered; a
    /// sibling column with no drop entry is untouched.
    #[test]
    fn dropped_column_cell_at_or_before_drop_time_is_filtered() {
        let row = live(
            0,
            200,
            vec![
                scalar_cell("name", "alice", 100),
                scalar_cell("legacy", "stale", 100),
            ],
        );
        let mut dropped = ::std::collections::HashMap::new();
        dropped.insert("legacy".to_string(), 150); // cell ts=100 <= 150

        let merged = KWayMerger::reconcile_cluster(None, vec![row], &dropped, None)
            .expect("a live row must be emitted (name survives)");
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };
        assert_eq!(cells.len(), 1, "the dropped-column cell must be discarded");
        assert_eq!(cells[0].column, "name");
    }

    /// A cell written strictly after the drop time survives the reconcile filter.
    #[test]
    fn dropped_column_cell_after_drop_time_survives() {
        let row = live(0, 200, vec![scalar_cell("legacy", "fresh", 200)]);
        let mut dropped = ::std::collections::HashMap::new();
        dropped.insert("legacy".to_string(), 150); // cell ts=200 > 150

        let merged = KWayMerger::reconcile_cluster(None, vec![row], &dropped, None)
            .expect("a live row must be emitted (cell post-dates the drop)");
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].column, "legacy");
        assert_eq!(cells[0].value, Value::text("fresh".to_string()));
    }

    /// Equal timestamp (cell ts == drop_time) is discarded — the `<=` boundary.
    #[test]
    fn dropped_column_cell_at_exact_drop_time_is_filtered() {
        let row = live(0, 150, vec![scalar_cell("legacy", "edge", 150)]);
        let mut dropped = ::std::collections::HashMap::new();
        dropped.insert("legacy".to_string(), 150);

        assert!(
            KWayMerger::reconcile_cluster(None, vec![row], &dropped, None).is_none(),
            "cell at exactly drop_time must be discarded, leaving no surviving cells"
        );
    }

    /// When every cell belongs to a fully-dropped column, the row emits nothing.
    #[test]
    fn all_cells_dropped_yields_no_row() {
        let row = live(
            0,
            120,
            vec![scalar_cell("a", "x", 100), scalar_cell("b", "y", 110)],
        );
        let mut dropped = ::std::collections::HashMap::new();
        dropped.insert("a".to_string(), 200);
        dropped.insert("b".to_string(), 200);

        assert!(
            KWayMerger::reconcile_cluster(None, vec![row], &dropped, None).is_none(),
            "a row whose every cell is a dropped-column cell emits nothing"
        );
    }

    /// An empty `dropped_columns` map is a no-op: all cells survive.
    #[test]
    fn empty_dropped_map_is_noop() {
        let row = live(
            0,
            200,
            vec![
                scalar_cell("name", "alice", 100),
                scalar_cell("legacy", "stale", 100),
            ],
        );
        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![row],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("a live row must be emitted");
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };
        assert_eq!(cells.len(), 2, "no drops configured → every cell survives");
    }

    /// Issue #922 — exact per-cell purging. One row carries two cells with
    /// DISTINCT writetimes: a pre-drop cell of the dropped column (`legacy`@100)
    /// and a post-drop cell of another column (`name`@300). The dropped cell is
    /// purged by its OWN timestamp (100 <= drop_time 150) while `name` survives by
    /// its own (300 > 150) — proving the filter is per-cell, not row-timestamp.
    #[test]
    fn dropped_column_purge_is_exact_per_cell_within_one_row() {
        let row = live(
            0,
            300, // row liveness ts = newest cell; must NOT govern the older cell
            vec![
                scalar_cell("name", "alice", 300),
                scalar_cell("legacy", "stale", 100),
            ],
        );
        let mut dropped = ::std::collections::HashMap::new();
        dropped.insert("legacy".to_string(), 150);

        let merged = KWayMerger::reconcile_cluster(None, vec![row], &dropped, None)
            .expect("name survives, so a live row must be emitted");
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };
        assert_eq!(cells.len(), 1, "only the post-drop `name` cell survives");
        assert_eq!(cells[0].column, "name");
        assert_eq!(cells[0].timestamp, 300);
    }

    /// GATING TEST — multi-cell merge granularity.
    ///
    /// A non-frozen collection (here: a list) is read back from an SSTable as a
    /// SINGLE top-level column whose `Value` is the whole collection. Two SSTable
    /// runs that each wrote different *paths* (different list positions) of the
    /// same column therefore arrive as two `CellData` that share the same
    /// `cell.column` string but carry different whole-collection `Value`s.
    ///
    /// `reconcile_cluster` keys winners on `cell.column` ONLY. This test asserts
    /// the observed consequence: the two path-writes do NOT merge into a combined
    /// collection; instead ONE whole cell wins by timestamp (whole-group collapse,
    /// NOT per-path merge).
    #[test]
    fn multicell_collection_collapses_whole_column_not_per_path() {
        // Newer run (run_index 0) wrote element "b"; older run wrote element "a".
        // In real Cassandra these are two cells at paths p_b and p_a that union to
        // [a, b]. Here each run surfaces the column as a whole list value.
        let newer = live(
            0,
            200,
            vec![CellData {
                column: "tags".to_string(),
                value: Value::List(vec![Value::text("b".to_string())]),
                timestamp: 200,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        );
        let older = live(
            1,
            100,
            vec![CellData {
                column: "tags".to_string(),
                value: Value::List(vec![Value::text("a".to_string())]),
                timestamp: 100,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        );

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![newer, older],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("a live row must be emitted");

        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };

        // ESTABLISHED BEHAVIOUR: exactly ONE cell for column "tags" survives —
        // there is no per-path union. The newer (ts=200) whole value wins.
        assert_eq!(
            cells.len(),
            1,
            "column-name keyed merge collapses to one cell"
        );
        assert_eq!(cells[0].column, "tags");
        assert_eq!(
            cells[0].value,
            Value::List(vec![Value::text("b".to_string())]),
            "winner is the higher-timestamp WHOLE collection value, not a union \
             of [a, b] — confirms whole-group collapse, NOT per-path merge (#18)"
        );
    }

    /// GATING TEST — non-frozen UDT field-level writes also collapse.
    ///
    /// A non-frozen UDT is multi-cell in Cassandra (one cell per field path, paths
    /// ordered by SIGNED ShortType field index). The reader surfaces it as a single
    /// `Value::Udt` under one column. Two runs writing different fields therefore
    /// collide on the column name and do NOT field-merge.
    #[test]
    fn nonfrozen_udt_collapses_whole_column_not_per_field() {
        let mk_udt = |field: &str, v: &str| {
            Value::Udt(Box::new(UdtValue {
                type_name: "addr".to_string(),
                keyspace: "ks".to_string(),
                fields: vec![UdtField {
                    name: field.to_string(),
                    value: Some(Value::text(v.to_string())),
                }],
            }))
        };

        // Newer run wrote field "city"; older run wrote field "zip".
        let newer = live(
            0,
            200,
            vec![CellData {
                column: "address".to_string(),
                value: mk_udt("city", "SF"),
                timestamp: 200,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        );
        let older = live(
            1,
            100,
            vec![CellData {
                column: "address".to_string(),
                value: mk_udt("zip", "94105"),
                timestamp: 100,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        );

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![newer, older],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("a live row must be emitted");
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };

        assert_eq!(
            cells.len(),
            1,
            "UDT column collapses to one whole-value cell"
        );
        // The "zip" write is lost: no per-field merge happens. The signed-ShortType
        // path ordering required by #18 is therefore unreachable in this engine.
        assert_eq!(
            cells[0].value,
            mk_udt("city", "SF"),
            "newer whole-UDT value wins; older field write is dropped — no per-field \
             merge, so #18 path-ordering does not apply"
        );
    }

    /// GATING TEST — the reader→merge adapter representation.
    ///
    /// Confirms the structural root cause: `value_to_row_data` turns a row (a
    /// top-level `Value::Map` of column-name → value) into one `CellData` per
    /// top-level column. A collection value nested under a column stays a single
    /// nested `Value`; it is NOT exploded into per-path cells. There is no
    /// `cell_path`/collection-key anywhere in `CellData`.
    #[test]
    fn adapter_produces_one_cell_per_top_level_column() {
        let row = Value::Map(vec![
            (Value::text("id".to_string()), Value::text("k1".to_string())),
            (
                Value::text("tags".to_string()),
                // Whole collection nested under a single column.
                Value::List(vec![
                    Value::text("a".to_string()),
                    Value::text("b".to_string()),
                ]),
            ),
        ]);

        let row_data = SSTableRowIteratorAdapter::value_to_row_data(&row, 500).expect("row data");
        let cells = match row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };

        // Two TOP-LEVEL columns → two cells. The collection is NOT split per path.
        assert_eq!(cells.len(), 2, "one CellData per top-level column");
        let tags = cells
            .iter()
            .find(|c| c.column == "tags")
            .expect("tags cell present");
        assert_eq!(
            tags.value,
            Value::List(vec![
                Value::text("a".to_string()),
                Value::text("b".to_string())
            ]),
            "the collection arrives as ONE nested Value, never as per-path cells"
        );
    }

    /// #14/#17 complex deletion — current status check.
    ///
    /// A complex (collection-level) deletion is, in Cassandra, a tombstone scoped
    /// to a single complex column with its own deletion time. CQLite has no
    /// per-column complex-deletion representation: `RowData::Tombstone` is whole-row
    /// only, and a `Value::Tombstone(CellTombstone)` is treated as a per-CELL
    /// tombstone keyed on the column name (collapsing with any sibling cell).
    ///
    /// This test asserts the consequence relevant to #14/#17: an equal-timestamp
    /// ROW deletion supersedes a CELL tombstone on a column (drop-on-equality via
    /// the `timestamp > row_del` filter), which is the row-vs-complex equality rule
    /// — but there is no path-scoped complex deletion to apply it to. We assert the
    /// row-vs-cell equality behaviour that DOES exist.
    #[test]
    fn row_deletion_supersedes_equal_ts_cell_tombstone() {
        let row_tomb = MergeEntry::new(
            0,
            dk(1),
            None,
            100,
            RowData::Tombstone {
                deletion_time: 100,
                local_deletion_time: 0,
            },
        );
        let cell_tomb = live(
            1,
            100,
            vec![CellData {
                column: "tags".to_string(),
                value: Value::Tombstone(Box::new(TombstoneInfo {
                    deletion_time: 100,
                    tombstone_type: TombstoneType::CellTombstone,
                    local_deletion_time: 0,
                    ttl: None,
                    range_start: None,
                    range_end: None,
                })),
                timestamp: 100,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        );

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![row_tomb, cell_tomb],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("row tombstone keeps the row shadowed");

        // Equal-ts row deletion wins: no surviving cell, row stays a tombstone.
        match merged.row_data {
            RowData::Tombstone { deletion_time, .. } => {
                assert_eq!(deletion_time, 100, "row tombstone preserved at its ts");
            }
            RowData::Live { cells } => {
                panic!("expected row to stay shadowed, got live cells: {:?}", cells)
            }
        }
    }

    // ── Issue #848 (Epic #921): tombstone-vs-expiring(TTL) tie-break ──────────
    //
    // Parity Cassandra `a62c749` (`Cells#reconcile`): at EQUAL timestamp a cell
    // DELETION wins over a LIVE/expiring cell, decided BEFORE any
    // `localDeletionTime` compare. `main` carries `ttl`/`local_deletion_time` on
    // `CellData` (now populated for simple cells via SimpleCell), so an expiring
    // cell at equal ts must NOT resurrect data over a same-ts cell tombstone.

    /// Build a single-cell expiring (TTL) `CellData` for column `v`.
    fn expiring_cell(value: &str, ts: i64, ttl: u32, ldt: i32) -> CellData {
        CellData {
            column: "v".to_string(),
            value: Value::text(value.to_string()),
            timestamp: ts,
            ttl: Some(ttl),
            cell_path: None,
            local_deletion_time: Some(ldt),
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        }
    }

    /// Build a single-cell tombstone `CellData` for column `v`.
    fn cell_tombstone(ts: i64, ldt: i32) -> CellData {
        CellData {
            column: "v".to_string(),
            value: Value::Tombstone(Box::new(TombstoneInfo {
                deletion_time: ts,
                tombstone_type: TombstoneType::CellTombstone,
                local_deletion_time: ldt as i64,
                ttl: None,
                range_start: None,
                range_end: None,
            })),
            timestamp: ts,
            ttl: None,
            cell_path: None,
            local_deletion_time: Some(ldt),
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        }
    }

    /// At equal timestamp the cell tombstone beats an expiring cell — EXPIRING
    /// arrives FIRST (newer run_index) so a naive recency/first-seen pick would
    /// resurrect it. The tombstone must still win (parity `a62c749`).
    #[test]
    fn issue_848_tombstone_beats_expiring_at_equal_ts_expiring_first() {
        const TS: i64 = 200;
        // EXPIRING cell carries a LARGER localDeletionTime than the tombstone, so
        // a (wrong) localDeletionTime-first tie-break would pick the expiring
        // cell. The deletion must be decided BEFORE that compare.
        let expiring = live(
            0,
            TS,
            vec![expiring_cell("resurrected-if-buggy", TS, 3600, 9_999)],
        );
        let tombstone = live(1, TS, vec![cell_tombstone(TS, 1_000)]);

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![expiring, tombstone],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("a row must be emitted");

        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };
        assert_eq!(cells.len(), 1, "single column `v`");
        assert!(
            KWayMerger::is_cell_tombstone(&cells[0]),
            "at equal ts the cell TOMBSTONE must win over the expiring cell \
             (before any localDeletionTime compare); got {:?}",
            cells[0].value
        );
        assert!(
            cells[0].ttl.is_none(),
            "the surviving winner is the tombstone, which carries no TTL"
        );
    }

    /// Same equal-ts tie-break with the source order REVERSED — tombstone arrives
    /// FIRST. The tombstone must still win (order-independence; the existing
    /// `tombstone beats live` rule already covered this, pinned here too).
    #[test]
    fn issue_848_tombstone_beats_expiring_at_equal_ts_tombstone_first() {
        const TS: i64 = 200;
        let tombstone = live(0, TS, vec![cell_tombstone(TS, 1_000)]);
        let expiring = live(
            1,
            TS,
            vec![expiring_cell("resurrected-if-buggy", TS, 3600, 9_999)],
        );

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![tombstone, expiring],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("a row must be emitted");

        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };
        assert_eq!(cells.len(), 1, "single column `v`");
        assert!(
            KWayMerger::is_cell_tombstone(&cells[0]),
            "at equal ts the cell TOMBSTONE must win regardless of source order; \
             got {:?}",
            cells[0].value
        );
    }

    /// A STRICTLY NEWER expiring cell still beats an older tombstone — the
    /// tie-break only fires at EQUAL timestamp, so timestamp recency is honored
    /// first (parity `a62c749`: timestamp compared before deletion-status).
    #[test]
    fn issue_848_newer_expiring_beats_older_tombstone() {
        let tombstone = live(0, 100, vec![cell_tombstone(100, 1_000)]);
        let expiring = live(1, 200, vec![expiring_cell("survives", 200, 3600, 9_999)]);

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![tombstone, expiring],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("a row must be emitted");

        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };
        assert_eq!(cells.len(), 1, "single column `v`");
        assert!(
            !KWayMerger::is_cell_tombstone(&cells[0]),
            "a strictly NEWER expiring cell (ts=200) beats the older tombstone \
             (ts=100); the deletion tie-break only fires at equal ts"
        );
        assert_eq!(cells[0].value, Value::text("survives".to_string()));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue #886 (Epic #842): reader→merge entry enrichment (PLUMBING ONLY)
// ─────────────────────────────────────────────────────────────────────────────
//
// These tests prove the new per-cell metadata (`cell_path`, `local_deletion_time`)
// and the first-class complex/range-deletion entities are threaded through the
// merge entry AND that reconciliation is byte-for-byte UNCHANGED — the carried
// fields are present but not yet acted on. The behavior that consumes them lands
// in #844 (per-cell-path collection/UDT merge) and #846/#848 (range tombstones /
// tombstone-vs-expiring tie-break).
#[cfg(all(test, feature = "write-support"))]
mod issue_886_merge_entry_enrichment {
    use super::*;
    use crate::storage::write_engine::mutation::ClusteringBound;
    use crate::types::Value;

    fn dk(byte: u8) -> DecoratedKey {
        DecoratedKey::from_key_bytes(vec![byte]).expect("token")
    }

    /// The new `CellData` constructor defaults the enriched fields to `None`,
    /// matching what the reader currently supplies.
    #[test]
    fn celldata_new_defaults_enriched_fields_to_none() {
        let cell = CellData::new("c".to_string(), Value::Integer(7), 100);
        assert_eq!(cell.ttl, None);
        assert_eq!(
            cell.local_deletion_time, None,
            "LDT defaults None (plumbing)"
        );
        assert_eq!(cell.cell_path, None, "cell_path defaults None (plumbing)");
    }

    /// The enriched fields round-trip through `MergeEntry`/`CellData` clone +
    /// equality unchanged (proves they are real, carried state).
    #[test]
    fn enriched_celldata_round_trips_through_merge_entry() {
        let cell = CellData {
            column: "m".to_string(),
            value: Value::text("v".to_string()),
            timestamp: 500,
            ttl: Some(3600),
            local_deletion_time: Some(1_700_000_000),
            cell_path: Some(vec![0x00, 0x01]),
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        };
        let entry = MergeEntry::new(
            0,
            dk(1),
            None,
            500,
            RowData::Live {
                cells: vec![cell.clone()],
            },
        );
        let cloned = entry.clone();
        let cells = match cloned.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {other:?}"),
        };
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].local_deletion_time, Some(1_700_000_000));
        assert_eq!(cells[0].cell_path, Some(vec![0x00, 0x01]));
        assert_eq!(cells[0].ttl, Some(3600));
        assert_eq!(cells[0], cell, "enriched cell survives clone + equality");
    }

    /// The reader→merge adapter (`value_to_row_data`) populates the enriched
    /// fields. Today the reader's `(RowKey, Value, ts)` compaction stream does
    /// not surface per-cell ttl / LDT / cell-path, so they are threaded as
    /// `None` — but the fields exist on every produced cell (plumbing present).
    #[test]
    fn value_to_row_data_threads_enriched_fields_as_none() {
        // Map case: top-level columns surfaced as a map (key = column name).
        let map = Value::Map(vec![(
            Value::text("name".to_string()),
            Value::text("alice".to_string()),
        )]);
        let row_data = SSTableRowIteratorAdapter::value_to_row_data(&map, 100)
            .expect("value_to_row_data must succeed");
        let cells = match row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {other:?}"),
        };
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].column, "name");
        assert_eq!(cells[0].timestamp, 100, "live cell inherits row ts (#533)");
        assert_eq!(cells[0].local_deletion_time, None);
        assert_eq!(cells[0].cell_path, None);
        assert_eq!(cells[0].ttl, None);

        // Single-value case wraps as one "value" cell with the same defaults.
        let single = SSTableRowIteratorAdapter::value_to_row_data(&Value::Integer(42), 200)
            .expect("value_to_row_data must succeed");
        let cells = match single {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {other:?}"),
        };
        assert_eq!(cells[0].local_deletion_time, None);
        assert_eq!(cells[0].cell_path, None);
    }

    /// A `MergeEntry` can carry a first-class complex deletion. The builder
    /// attaches it; the field is non-empty but reconciliation ignores it.
    #[test]
    fn merge_entry_carries_complex_deletion_without_acting_on_it() {
        let complex = ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 1234,
            local_deletion_time: 1_700_000_000,
        };
        let entry = MergeEntry::new(0, dk(1), None, 100, RowData::Live { cells: vec![] })
            .with_complex_deletions(vec![complex.clone()]);
        assert_eq!(entry.complex_deletions, vec![complex]);

        // Plumbing-only: reconcile_cluster does NOT consult complex_deletions.
        // A cell written at the same ts as the complex deletion still survives,
        // proving no shadowing behavior was introduced (#844 owns that).
        let cell = CellData::new("tags".to_string(), Value::text("a".to_string()), 1234);
        let live = MergeEntry::new(0, dk(1), None, 1234, RowData::Live { cells: vec![cell] })
            .with_complex_deletions(vec![ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: 1234,
                local_deletion_time: 1_700_000_000,
            }]);
        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![live],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("live row must be emitted");
        match merged.row_data {
            RowData::Live { cells } => {
                assert_eq!(
                    cells.len(),
                    1,
                    "complex deletion must NOT shadow (plumbing)"
                );
                assert_eq!(cells[0].column, "tags");
            }
            other => panic!("expected Live, got {other:?}"),
        }
    }

    /// A `MergeEntry` can carry a first-class range deletion (reusing the shared
    /// open-ended `TombstoneInfo`). The field is populated but reconciliation
    /// ignores it — covered cells are NOT shadowed by this issue (#846 owns it).
    #[test]
    fn merge_entry_carries_range_deletion_without_acting_on_it() {
        // Open-ended range tombstone: both bounds None (the representation the
        // tombstone_merger open-ended utilities use).
        let range = RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 5000,
            local_deletion_time: 0,
        };
        let cell = CellData::new("v".to_string(), Value::Integer(1), 1000);
        let entry = MergeEntry::new(0, dk(1), None, 1000, RowData::Live { cells: vec![cell] })
            .with_range_deletion(range.clone());
        assert_eq!(entry.range_deletion, Some(range));

        // Plumbing-only: the covered cell (ts=1000 < range ts=5000) STILL
        // survives reconcile because range deletions are not yet applied.
        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![entry],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("live row must be emitted");
        match merged.row_data {
            RowData::Live { cells } => {
                assert_eq!(
                    cells.len(),
                    1,
                    "range deletion must NOT shadow covered cell (plumbing)"
                );
            }
            other => panic!("expected Live, got {other:?}"),
        }
    }

    /// `reconcile_cluster` must PRESERVE the carried complex/range deletion
    /// metadata from its input rows onto the returned entry (#886 plumbing
    /// preservation). Without this, the metadata threaded by the reader is
    /// silently dropped before downstream consumers (#899) can see it. The
    /// preservation is behavior-neutral: the surviving cells (normal reconcile
    /// output) are identical to the no-metadata case.
    #[test]
    fn reconcile_cluster_preserves_carried_deletion_metadata() {
        let complex_a = ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 1234,
            local_deletion_time: 1_700_000_000,
        };
        let complex_b = ComplexDeletion {
            column: "notes".to_string(),
            marked_for_delete_at: 999,
            local_deletion_time: 1_700_000_001,
        };
        let range_low = RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 3000,
            local_deletion_time: 0,
        };
        let range_high = RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 7000,
            local_deletion_time: 0,
        };

        // Two input rows in the same cluster, each carrying distinct metadata.
        let row0 = MergeEntry::new(
            0,
            dk(1),
            None,
            2000,
            RowData::Live {
                cells: vec![CellData::new("v".to_string(), Value::Integer(2), 2000)],
            },
        )
        .with_complex_deletions(vec![complex_a.clone()])
        .with_range_deletion(range_low.clone());
        let row1 = MergeEntry::new(
            1,
            dk(1),
            None,
            1000,
            RowData::Live {
                cells: vec![CellData::new("w".to_string(), Value::Integer(1), 1000)],
            },
        )
        .with_complex_deletions(vec![complex_a.clone(), complex_b.clone()])
        .with_range_deletion(range_high.clone());

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![row0, row1],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("live row must be emitted");

        // complex_deletions: PHASE A NEUTRALITY (roborev #863) — accumulated as a
        // simple first-seen union, deduplicated. reconcile does NOT act on them
        // (no strict-supersede, no shadowing); that is Phase C. row0 contributes
        // "tags" first, then row1 adds "notes" (its "tags" is a dup).
        assert_eq!(
            merged.complex_deletions,
            vec![complex_a.clone(), complex_b.clone()],
            "complex deletions: first-seen union, deduplicated (Phase A neutral)"
        );
        // range_deletion: the highest deletion timestamp wins.
        assert_eq!(
            merged.range_deletion,
            Some(range_high),
            "range deletion with the highest deletion timestamp must be carried"
        );

        // Behavior-neutral: normal reconcile output (surviving cells) is unchanged
        // versus the same inputs with NO carried metadata.
        let plain0 = MergeEntry::new(
            0,
            dk(1),
            None,
            2000,
            RowData::Live {
                cells: vec![CellData::new("v".to_string(), Value::Integer(2), 2000)],
            },
        );
        let plain1 = MergeEntry::new(
            1,
            dk(1),
            None,
            1000,
            RowData::Live {
                cells: vec![CellData::new("w".to_string(), Value::Integer(1), 1000)],
            },
        );
        let plain = KWayMerger::reconcile_cluster(
            None,
            vec![plain0, plain1],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("live row must be emitted");
        assert_eq!(
            merged.row_data, plain.row_data,
            "carrying deletion metadata must not change surviving-cell output"
        );
        assert_eq!(merged.timestamp, plain.timestamp);
        assert!(plain.complex_deletions.is_empty());
        assert_eq!(plain.range_deletion, None);
    }

    /// A row-tombstone-only cluster (no surviving cells) must still carry the
    /// metadata onto the emitted tombstone entry.
    #[test]
    fn reconcile_cluster_preserves_metadata_on_tombstone_entry() {
        let complex = ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 10,
            local_deletion_time: 1_700_000_000,
        };
        let row = MergeEntry::new(
            0,
            dk(1),
            None,
            500,
            RowData::Tombstone {
                deletion_time: 500,
                local_deletion_time: 0,
            },
        )
        .with_complex_deletions(vec![complex.clone()]);

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![row],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("row tombstone must be emitted");
        assert!(matches!(merged.row_data, RowData::Tombstone { .. }));
        assert_eq!(merged.complex_deletions, vec![complex]);
    }

    /// Regression (#853/#886 branch-review, Finding 3): a cluster carrying complex
    /// and/or range deletion metadata but with NO surviving cells and NO row
    /// tombstone must STILL emit a metadata-only entry so the carried deletion
    /// metadata survives reconciliation. Previously `built` was `None` for this
    /// case and the metadata was silently dropped before the preservation logic
    /// could run.
    #[test]
    fn reconcile_cluster_emits_metadata_only_entry_when_no_row_produced() {
        let complex = ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 4242,
            local_deletion_time: 1_700_000_000,
        };
        let range = RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 8888,
            local_deletion_time: 0,
        };

        // Empty Live row (no cells), no row tombstone, but carrying both kinds of
        // deletion metadata.
        let row = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
            .with_complex_deletions(vec![complex.clone()])
            .with_range_deletion(range.clone());

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![row],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("metadata-only cluster must still emit an entry");

        // The emitted entry carries the metadata and has no live cells.
        match &merged.row_data {
            RowData::Live { cells } => {
                assert!(
                    cells.is_empty(),
                    "metadata-only entry must have no live cells"
                );
            }
            other => panic!("expected empty Live, got {other:?}"),
        }
        assert_eq!(merged.complex_deletions, vec![complex]);
        assert_eq!(merged.range_deletion, Some(range));
    }

    /// Behavior-neutral guard for Finding 3: an empty `Live` cluster with NO
    /// carried metadata and NO row tombstone must STILL collapse to `None`
    /// (nothing is emitted). The metadata-only path only adds an entry when
    /// deletion metadata would otherwise be lost.
    #[test]
    fn reconcile_cluster_empty_live_without_metadata_yields_none() {
        let row = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] });
        assert!(
            KWayMerger::reconcile_cluster(
                None,
                vec![row],
                &::std::collections::HashMap::new(),
                None
            )
            .is_none(),
            "empty live row with no metadata must not emit an entry"
        );
    }

    /// Epic #899 Phase C: a COMPLEX-DELETION-only synthetic entry (empty `Live`
    /// row carrying a `ComplexDeletion`, no ops, no row tombstone) must NO LONGER
    /// be skipped — the writer now emits a real complex-deletion marker for it
    /// (`merge_entry_to_mutation` produces a `CellOperation::ComplexDeletion`), so
    /// a fully-deleted-collection marker reaches the SSTable instead of being
    /// dropped. Issue #933: a RANGE-deletion-only carrier is ALSO now writer-
    /// visible (the writer emits its bound markers), so it too is no longer skipped.
    #[test]
    fn complex_deletion_only_entry_reaches_writer_but_range_only_is_skipped() {
        let complex = ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 4242,
            local_deletion_time: 1_700_000_000,
        };
        let range = RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 8888,
            local_deletion_time: 0,
        };

        // Complex-deletion-only: now WRITER-VISIBLE (not a no-op) — the marker
        // must be emitted, not dropped.
        let complex_only = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
            .with_complex_deletions(vec![complex.clone()]);
        assert!(
            !complex_only.is_metadata_only_no_op(),
            "Phase C: a complex-deletion-only entry must reach the writer"
        );

        // Range-deletion-only (no complex deletion): now WRITER-VISIBLE (#933) —
        // `merge_entry_to_mutation` threads it onto the mutation's range_tombstones
        // so the writer emits the on-disk bound markers; dropping it would
        // resurrect the rows it shadowed.
        let range_only = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
            .with_range_deletion(range.clone());
        assert!(
            !range_only.is_metadata_only_no_op(),
            "#933: a range-deletion-only carrier must reach the writer"
        );

        // Both kinds present → the complex deletion is writer-visible, so the
        // entry is NOT skipped (the marker must survive).
        let both = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
            .with_complex_deletions(vec![complex])
            .with_range_deletion(range);
        assert!(
            !both.is_metadata_only_no_op(),
            "a carried complex deletion keeps the entry writer-visible"
        );

        // The complex-deletion-only entry survives reconciliation AND stays
        // writer-visible end-to-end.
        let reconciled = KWayMerger::reconcile_cluster(
            None,
            vec![complex_only],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("complex-deletion-only cluster must still emit an entry");
        assert!(
            !reconciled.is_metadata_only_no_op(),
            "the reconciled complex-deletion entry must reach the writer"
        );
    }

    /// Guard the boundary of the skip predicate: entries that carry real content
    /// (live cells, a row tombstone, or no carried metadata) must NOT be treated
    /// as metadata-only no-ops, so genuine rows are never dropped on the writer
    /// path.
    #[test]
    fn non_metadata_only_entries_are_not_skipped() {
        // Live row with a real cell — must be written.
        let live = MergeEntry::new(
            0,
            dk(1),
            None,
            100,
            RowData::Live {
                cells: vec![CellData::new(
                    "name".to_string(),
                    Value::text("v".to_string()),
                    100,
                )],
            },
        );
        assert!(!live.is_metadata_only_no_op());

        // Empty live row WITHOUT any metadata: a true phantom no-op. It never
        // survives reconcile in practice, but the defensive filter DOES treat it as
        // skippable (#933 redefinition: no cells, no complex, no range, no row
        // deletion).
        let empty_no_meta = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] });
        assert!(empty_no_meta.is_metadata_only_no_op());

        // Row tombstone — must be written (real deletion).
        let tomb = MergeEntry::new(
            0,
            dk(1),
            None,
            500,
            RowData::Tombstone {
                deletion_time: 500,
                local_deletion_time: 0,
            },
        )
        .with_range_deletion(RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 8888,
            local_deletion_time: 0,
        });
        assert!(
            !tomb.is_metadata_only_no_op(),
            "a row tombstone is real content even when carrying range metadata"
        );
    }

    /// Equality of two default merge entries is unaffected by the new carried
    /// fields (both empty/None), so existing assert_eq!-based tests stay valid.
    #[test]
    fn default_carried_fields_preserve_merge_entry_equality() {
        let a = MergeEntry::new(0, dk(1), None, 100, RowData::Live { cells: vec![] });
        let b = MergeEntry::new(0, dk(1), None, 100, RowData::Live { cells: vec![] });
        assert_eq!(a, b);
        assert!(a.complex_deletions.is_empty());
        assert_eq!(a.range_deletion, None);
    }
}

/// Epic #899, Phase A (roborev #863): the reader→merge per-element substrate is
/// POPULATED (the foundation), but Phase A is BEHAVIOR-NEUTRAL: the merge OUTPUT
/// path emits one whole-collection `CellData` per complex column (byte-identical
/// to pre-Phase-A) while the per-element data rides alongside on the
/// `CompactionRow` and `MergeEntry.complex_deletions` for Phase C. These tests
/// pin both the substrate and the neutral output, and the per-`(column,
/// cell_path)` reconcile CAPABILITY that Phase C will use.
#[cfg(all(test, feature = "write-support"))]
mod issue_899_per_element_merge {
    use super::*;
    use crate::storage::sstable::reader::compaction_row::{
        CompactionRowData, ComplexColumn, ComplexElement,
    };
    use crate::types::Value;

    fn dk(byte: u8) -> DecoratedKey {
        DecoratedKey::from_key_bytes(vec![byte]).expect("token")
    }

    fn element(path: &[u8], val: &str, ts: i64) -> ComplexElement {
        ComplexElement {
            cell_path: path.to_vec(),
            value: Some(Value::text(val.to_string())),
            decoded_key: None,
            timestamp: ts,
            ttl: None,
            local_deletion_time: None,
            is_deleted: false,
            has_empty_value: false,
        }
    }

    /// (a) PHASE C FLIP: a multi-cell collection now surfaces ONE per-element
    /// `CellData` PER `ComplexElement` (populated cell_path + per-element
    /// timestamp), NOT one collapsed whole-column cell. Each cell carries the
    /// element's authoritative path/ts so per-`(column, cell_path)` reconcile and
    /// the per-element writer emit are byte-faithful to Cassandra's multi-cell
    /// layout.
    #[test]
    fn multi_cell_collection_surfaces_one_celldata_per_element() {
        let elements = vec![
            element(&[0xAA], "a", 100),
            element(&[0xBB], "b", 200),
            element(&[0xCC], "c", 300),
        ];
        assert_eq!(elements.len(), 3);

        // PHASE C: per-element emit (cell_path + per-element ts preserved); the
        // collapsed_value is no longer threaded to the writer (kept only for the
        // user-facing read contract).
        let row_data = CompactionRowData::Live {
            simple: vec![],
            complex: vec![ComplexColumn {
                column: "tags".to_string(),
                complex_deletion: None,
                elements,
                collapsed_value: Value::Set(vec![
                    Value::text("a".to_string()),
                    Value::text("b".to_string()),
                    Value::text("c".to_string()),
                ]),
            }],
            row_deletion: None,
            row_liveness: Default::default(),
        };

        let (row, complex_deletions, _row_deletion, _row_liveness) =
            SSTableRowIteratorAdapter::compaction_row_data_to_row_data(row_data, 999);
        assert!(complex_deletions.is_empty(), "no complex deletion present");

        let cells = match row {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {other:?}"),
        };
        assert_eq!(cells.len(), 3, "one CellData per element (Phase C flip)");
        for c in &cells {
            assert_eq!(c.column, "tags");
            assert!(c.is_complex_element, "each is a complex element");
            assert!(!c.is_deleted, "live elements");
            assert!(c.cell_path.is_some(), "per-element cell_path populated");
        }
        // The per-element cells keep their OWN timestamps (NOT promoted to the
        // row timestamp 999) — the fidelity gain Phase C delivers.
        let mut by_path: Vec<(Vec<u8>, i64, Value)> = cells
            .into_iter()
            .map(|c| (c.cell_path.expect("path"), c.timestamp, c.value))
            .collect();
        by_path.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(by_path[0], (vec![0xAA], 100, Value::text("a".to_string())));
        assert_eq!(by_path[1], (vec![0xBB], 200, Value::text("b".to_string())));
        assert_eq!(by_path[2], (vec![0xCC], 300, Value::text("c".to_string())));
    }

    /// (b) FOUNDATION: a real complex deletion reaches `MergeEntry.complex_deletions`
    /// as a first-class `ComplexDeletion` (not a boolean), carried but not yet
    /// consumed by the writer (Phase C).
    #[test]
    fn complex_deletion_reaches_merge_entry_complex_deletions() {
        let row_data = CompactionRowData::Live {
            simple: vec![],
            complex: vec![ComplexColumn {
                column: "tags".to_string(),
                complex_deletion: Some((12_345, 1_700_000_000)),
                elements: vec![element(&[0xAB], "x", 20_000)],
                collapsed_value: Value::Set(vec![Value::text("x".to_string())]),
            }],
            row_deletion: None,
            row_liveness: Default::default(),
        };

        let (_row, complex_deletions, _row_deletion, _row_liveness) =
            SSTableRowIteratorAdapter::compaction_row_data_to_row_data(row_data, 20_000);
        assert_eq!(complex_deletions.len(), 1);
        assert_eq!(complex_deletions[0].column, "tags");
        assert_eq!(complex_deletions[0].marked_for_delete_at, 12_345);
        assert_eq!(complex_deletions[0].local_deletion_time, 1_700_000_000);
    }

    /// (c) Per-`(column, cell_path)` reconcile keeps disjoint elements written to
    /// the same column across two SSTables (the case the old whole-column key
    /// collapsed to one survivor).
    #[test]
    fn reconcile_keeps_disjoint_elements_per_cell_path() {
        let newer = MergeEntry::new(
            0,
            dk(1),
            None,
            200,
            RowData::Live {
                cells: vec![CellData {
                    column: "tags".to_string(),
                    value: Value::text("b".to_string()),
                    timestamp: 200,
                    ttl: None,
                    cell_path: Some(vec![0xBB]),
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );
        let older = MergeEntry::new(
            1,
            dk(1),
            None,
            100,
            RowData::Live {
                cells: vec![CellData {
                    column: "tags".to_string(),
                    value: Value::text("a".to_string()),
                    timestamp: 100,
                    ttl: None,
                    cell_path: Some(vec![0xAA]),
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![newer, older],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("a live row must be emitted");
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {other:?}"),
        };
        // BOTH elements survive (disjoint cell paths), unlike the old
        // whole-column-keyed collapse which kept only one.
        assert_eq!(cells.len(), 2, "disjoint elements both survive");
        let mut paths: Vec<Vec<u8>> = cells
            .iter()
            .map(|c| c.cell_path.clone().expect("cell_path"))
            .collect();
        paths.sort();
        assert_eq!(paths, vec![vec![0xAA], vec![0xBB]]);
    }

    /// ISSUE #887 (parity f66fa14f): a surviving complex deletion SHADOWS the
    /// elements it covers BEFORE the marker is purged. An element whose timestamp
    /// is `<= markedForDeleteAt` is shadowed; an element strictly newer survives.
    /// The marker still rides along on `MergeEntry.complex_deletions` so the writer
    /// can emit it (and the per-element survivors) faithfully.
    #[test]
    fn complex_deletion_shadows_covered_elements_before_purge() {
        let old_el = MergeEntry::new(
            1,
            dk(1),
            None,
            100,
            RowData::Live {
                cells: vec![CellData {
                    column: "tags".to_string(),
                    value: Value::text("old".to_string()),
                    timestamp: 100,
                    ttl: None,
                    cell_path: Some(vec![0x01]),
                    local_deletion_time: None,
                    is_complex_element: true,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        );
        let new_el = MergeEntry::new(
            0,
            dk(1),
            None,
            300,
            RowData::Live {
                cells: vec![CellData {
                    column: "tags".to_string(),
                    value: Value::text("new".to_string()),
                    timestamp: 300,
                    ttl: None,
                    cell_path: Some(vec![0x02]),
                    local_deletion_time: None,
                    is_complex_element: true,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        )
        .with_complex_deletions(vec![ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 200,
            local_deletion_time: 1_700_000_000,
        }]);

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![new_el, old_el],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("a live row must be emitted");

        // The complex deletion is carried so the writer can emit the marker...
        assert_eq!(
            merged.complex_deletions,
            vec![ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: 200,
                local_deletion_time: 1_700_000_000,
            }],
            "complex deletion is carried on the MergeEntry"
        );

        // ...and reconcile shadows the covered element (ts 100 <= mfda 200) BEFORE
        // purge: only the strictly-newer element (ts 300 > mfda 200) survives.
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {other:?}"),
        };
        assert_eq!(
            cells.len(),
            1,
            "the covered element (ts<=mfda) is shadowed before purge (#887 f66fa14f)"
        );
        assert_eq!(
            cells[0].cell_path.as_deref(),
            Some([0x02].as_slice()),
            "the strictly-newer element (ts>mfda) survives"
        );
    }

    /// ACCEPTANCE #887 (1) — parity bd244649: when two SSTables carry complex
    /// deletions on the SAME column at EQUAL `markedForDeleteAt`, the merged
    /// deletion does NOT supersede the active one (only STRICTLY-greater
    /// supersedes). At equal mfda the boundary is still `<=`, so an element at
    /// exactly `mfda` is shadowed, but an element strictly newer survives — proving
    /// the equal-ts deletions did not collapse into a stronger one.
    #[test]
    fn complex_deletion_equal_timestamps_do_not_supersede() {
        let covered = MergeEntry::new(
            1,
            dk(1),
            None,
            200,
            RowData::Live {
                cells: vec![CellData {
                    column: "tags".to_string(),
                    value: Value::text("at-mfda".to_string()),
                    timestamp: 200,
                    ttl: None,
                    cell_path: Some(vec![0x01]),
                    local_deletion_time: None,
                    is_complex_element: true,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        )
        .with_complex_deletions(vec![ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 200,
            local_deletion_time: 1_700_000_100,
        }]);
        let survivor = MergeEntry::new(
            0,
            dk(1),
            None,
            300,
            RowData::Live {
                cells: vec![CellData {
                    column: "tags".to_string(),
                    value: Value::text("after".to_string()),
                    timestamp: 300,
                    ttl: None,
                    cell_path: Some(vec![0x02]),
                    local_deletion_time: None,
                    is_complex_element: true,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        )
        // SAME column, EQUAL marked_for_delete_at — must NOT supersede the active.
        .with_complex_deletions(vec![ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 200,
            local_deletion_time: 1_700_000_000,
        }]);

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![survivor, covered],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("a live row must be emitted");

        // The deletion at mfda=200 survives (one carried marker for `tags`)...
        let tags_dels: Vec<&ComplexDeletion> = merged
            .complex_deletions
            .iter()
            .filter(|d| d.column == "tags")
            .collect();
        assert_eq!(
            tags_dels.len(),
            1,
            "the union keeps one complex deletion for the column"
        );
        assert_eq!(
            tags_dels[0].marked_for_delete_at, 200,
            "equal-ts deletions do not produce a stronger mfda (bd244649)"
        );

        // ...the strictly-newer element (ts 300 > 200) survives; the at-mfda element
        // (ts 200 <= 200) is shadowed.
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {other:?}"),
        };
        assert_eq!(
            cells.len(),
            1,
            "only the strictly-newer element survives the active deletion"
        );
        assert_eq!(
            cells[0].cell_path.as_deref(),
            Some([0x02].as_slice()),
            "ts>mfda element survives; equal-ts deletions did not shadow it"
        );
    }

    /// ACCEPTANCE #887 (2) — parity bd244649 + f66fa14f: a STRICTLY-superseding
    /// complex deletion shadows every covered element (ts <= mfda) BEFORE the marker
    /// is purged, so a later purge cannot resurrect them. An element with ts STRICTLY
    /// GREATER than the surviving mfda MUST survive.
    #[test]
    fn complex_deletion_strict_supersede_shadows_before_purge() {
        // Older source: weaker deletion (mfda 100) + element at ts 50 (covered by
        // the strong deletion below) + element at ts 500 (survives everything).
        let weak = MergeEntry::new(
            1,
            dk(1),
            None,
            500,
            RowData::Live {
                cells: vec![
                    CellData {
                        column: "tags".to_string(),
                        value: Value::text("ancient".to_string()),
                        timestamp: 50,
                        ttl: None,
                        cell_path: Some(vec![0x01]),
                        local_deletion_time: None,
                        is_complex_element: true,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                    CellData {
                        column: "tags".to_string(),
                        value: Value::text("survivor".to_string()),
                        timestamp: 500,
                        ttl: None,
                        cell_path: Some(vec![0x03]),
                        local_deletion_time: None,
                        is_complex_element: true,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                ],
            },
        )
        .with_complex_deletions(vec![ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 100,
            local_deletion_time: 1_700_000_000,
        }]);
        // Newer source: STRONG deletion (mfda 300, strictly > 100) + element at ts
        // 300 (== strong mfda → covered).
        let strong = MergeEntry::new(
            0,
            dk(1),
            None,
            300,
            RowData::Live {
                cells: vec![CellData {
                    column: "tags".to_string(),
                    value: Value::text("covered".to_string()),
                    timestamp: 300,
                    ttl: None,
                    cell_path: Some(vec![0x02]),
                    local_deletion_time: None,
                    is_complex_element: true,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        )
        .with_complex_deletions(vec![ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 300,
            local_deletion_time: 1_700_000_200,
        }]);

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![strong, weak],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("a live row must be emitted");

        // The strong deletion (mfda 300) strictly supersedes the weak one (100).
        let tags_dels: Vec<&ComplexDeletion> = merged
            .complex_deletions
            .iter()
            .filter(|d| d.column == "tags")
            .collect();
        assert_eq!(
            tags_dels.len(),
            1,
            "one surviving complex deletion for tags"
        );
        assert_eq!(
            tags_dels[0].marked_for_delete_at, 300,
            "the strictly-greater mfda supersedes (bd244649)"
        );

        // Covered elements (ts 50 and ts 300, both <= 300) are shadowed BEFORE purge;
        // only the ts 500 element (strictly > 300) survives (f66fa14f).
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {other:?}"),
        };
        assert_eq!(
            cells.len(),
            1,
            "all elements with ts<=mfda are shadowed before purge (no resurrection)"
        );
        assert_eq!(
            cells[0].cell_path.as_deref(),
            Some([0x03].as_slice()),
            "the strictly-newer element (ts>mfda) survives"
        );
        assert_eq!(cells[0].timestamp, 500);
    }

    /// ISSUE #887: a row that reduces to a ROW TOMBSTONE at `row_del = T_low` must
    /// STILL emit any carried complex-deletion marker whose `marked_for_delete_at =
    /// T_high` is STRICTLY GREATER than `row_del`.
    ///
    /// A row tombstone shadows only `timestamp <= row_del`. A complex deletion at
    /// `mfda = T_high > T_low` covers elements in `(T_low, T_high]` — including
    /// elements that live in OTHER SSTables NOT part of this compaction. If the
    /// marker is dropped, those elements would be RESURRECTED. So the emitted
    /// mutation must carry BOTH the `DeleteRow` AND the `ComplexDeletion{T_high}`.
    #[test]
    fn row_tombstone_keeps_strictly_newer_complex_deletion_marker() {
        use crate::schema::{Column, KeyColumn, TableSchema};
        use crate::storage::write_engine::mutation::CellOperation;
        use std::collections::HashMap;

        const T_LOW: i64 = 100; // row tombstone (row_del)
        const T_HIGH: i64 = 300; // complex deletion mfda (strictly > row_del)

        // Source A (newest, run 0): a ROW TOMBSTONE at T_low.
        let row_tomb = MergeEntry::new(
            0,
            dk(1),
            None,
            T_LOW,
            RowData::Tombstone {
                deletion_time: T_LOW,
                local_deletion_time: 0,
            },
        );

        // Source B (older, run 1): carries the STRONGER complex deletion (mfda
        // T_high > row_del) plus three elements (shadowed-by-row, shadowed-by-complex,
        // survivor).
        let carrier = MergeEntry::new(
            1,
            dk(1),
            None,
            T_HIGH + 200,
            RowData::Live {
                cells: vec![
                    CellData {
                        column: "tags".to_string(),
                        value: Value::text("shadowed_by_row".to_string()),
                        timestamp: T_LOW,
                        ttl: None,
                        cell_path: Some(vec![0x01]),
                        local_deletion_time: None,
                        is_complex_element: true,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                    CellData {
                        column: "tags".to_string(),
                        value: Value::text("shadowed_by_complex".to_string()),
                        timestamp: T_HIGH,
                        ttl: None,
                        cell_path: Some(vec![0x02]),
                        local_deletion_time: None,
                        is_complex_element: true,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                    CellData {
                        column: "tags".to_string(),
                        value: Value::text("survivor".to_string()),
                        timestamp: T_HIGH + 200,
                        ttl: None,
                        cell_path: Some(vec![0x03]),
                        local_deletion_time: None,
                        is_complex_element: true,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                ],
            },
        )
        .with_complex_deletions(vec![ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: T_HIGH,
            local_deletion_time: 1_700_000_000,
        }]);

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![row_tomb, carrier],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("a row must be emitted (it carries a row tombstone + survivor)");

        // The strictly-newer element (ts T_high+200) survives both deletions; the
        // ts<=T_low and T_low<ts<=T_high elements are shadowed BEFORE purge.
        let cells = match &merged.row_data {
            RowData::Live { cells } => cells.clone(),
            other => panic!("expected Live (survivor present), got {other:?}"),
        };
        assert_eq!(
            cells.len(),
            1,
            "only the element with ts > T_high survives the complex deletion"
        );
        assert_eq!(cells[0].cell_path.as_deref(), Some([0x03].as_slice()));
        assert_eq!(cells[0].timestamp, T_HIGH + 200);

        // The strictly-newer complex deletion marker is preserved on the merge entry.
        let tags_dels: Vec<&ComplexDeletion> = merged
            .complex_deletions
            .iter()
            .filter(|d| d.column == "tags")
            .collect();
        assert_eq!(tags_dels.len(), 1, "the T_high complex deletion is carried");
        assert_eq!(tags_dels[0].marked_for_delete_at, T_HIGH);

        // Schema with a non-frozen complex column `tags`.
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "tags".to_string(),
                data_type: "list<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        // The crux: `merge_entry_to_mutation` MUST emit BOTH a `DeleteRow` AND the
        // strictly-newer `ComplexDeletion` (mfda = T_high). `merge_entry_to_mutation`
        // decodes the partition key against the schema's `int` PK, so the key must be
        // a 4-byte big-endian int.
        let int_pk = DecoratedKey::new(1, 1i32.to_be_bytes().to_vec());
        let tomb_entry = MergeEntry::new(
            0,
            int_pk,
            None,
            T_LOW,
            RowData::Tombstone {
                deletion_time: T_LOW,
                local_deletion_time: 0,
            },
        )
        .with_complex_deletions(vec![ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: T_HIGH,
            local_deletion_time: 1_700_000_000,
        }]);

        let mutation = KWayMerger::merge_entry_to_mutation(tomb_entry, &schema)
            .expect("conversion should succeed");

        let has_delete_row = mutation
            .operations
            .iter()
            .any(|op| matches!(op, CellOperation::DeleteRow));
        assert!(has_delete_row, "the row tombstone must still be emitted");

        let strictly_newer_marker = mutation.operations.iter().any(|op| {
            matches!(
                op,
                CellOperation::ComplexDeletion {
                    column,
                    marked_for_delete_at,
                    ..
                } if column == "tags" && *marked_for_delete_at == T_HIGH
            )
        });
        assert!(
            strictly_newer_marker,
            "the strictly-newer (mfda > row_del) complex deletion marker must be \
             emitted alongside the row tombstone (else (row_del, mfda] resurrects)"
        );
    }

    /// ISSUE #887, complement: a carried complex-deletion marker that is FULLY
    /// COVERED by the row tombstone (`mfda <= row_del`) IS dropped — the row
    /// tombstone already shadows its entire range. Mirrors #887's strict boundary
    /// (equal does not supersede).
    #[test]
    fn row_tombstone_drops_fully_covered_complex_deletion_marker() {
        use crate::schema::{Column, KeyColumn, TableSchema};
        use crate::storage::write_engine::mutation::CellOperation;
        use std::collections::HashMap;

        const ROW_DEL: i64 = 300;

        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "tags".to_string(),
                data_type: "list<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let int_pk = DecoratedKey::new(1, 1i32.to_be_bytes().to_vec());
        for covered_mfda in [ROW_DEL - 100, ROW_DEL] {
            let entry = MergeEntry::new(
                0,
                int_pk.clone(),
                None,
                ROW_DEL,
                RowData::Tombstone {
                    deletion_time: ROW_DEL,
                    local_deletion_time: 0,
                },
            )
            .with_complex_deletions(vec![ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: covered_mfda,
                local_deletion_time: 1_700_000_000,
            }]);

            let mutation = KWayMerger::merge_entry_to_mutation(entry, &schema)
                .expect("conversion should succeed");

            assert!(
                mutation
                    .operations
                    .iter()
                    .all(|op| !matches!(op, CellOperation::ComplexDeletion { .. })),
                "a marker with mfda ({covered_mfda}) <= row_del ({ROW_DEL}) is fully \
                 covered by the row tombstone and must be dropped (strict boundary)"
            );
            assert!(
                matches!(mutation.operations.as_slice(), [CellOperation::DeleteRow]),
                "the fully-covered case reduces to exactly [DeleteRow]"
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue #822 (Epic #817): merge ordering / semantic invariants
// ─────────────────────────────────────────────────────────────────────────────
//
// VALUE-ASSERTING tests for three findings from the garbage-free-compaction
// review. Each test drives REAL merge code paths (no mocks):
//   - #10  DESC empty-vs-valued clustering ordering, via ClusteringKey::compare
//          and the live merge sort in merge_partition_rows.
//   - #13/#3 tombstone-beats-expiring at EQUAL timestamp, via reconcile_cluster
//          (exercised through merge_partition_rows), plus strict writer flag
//          semantics for CELL_IS_EXPIRING / CELL_IS_DELETED exclusivity.
//   - #22  header-driven static / column superset — DIVERGENT: CQLite's
//          compaction writer derives hasStatic and the column set from the
//          supplied TableSchema, NOT from the merged input SerializationHeaders.
//          The test documents and pins that divergence.
#[cfg(all(test, feature = "write-support"))]
mod issue_822_merge_ordering_semantics {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
    use crate::storage::sstable::writer::{DataWriter, StatisticsMetadata};
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, DecoratedKey, Mutation, PartitionKey, TableId,
    };
    use crate::types::{TombstoneInfo, TombstoneType, Value};
    use std::collections::HashMap;

    // ── Wire-format flag constants (mirror data_writer.rs; protocol constants,
    //    NOT a reimplementation of writer logic — the load-bearing assertions are
    //    on the REAL bytes produced by `DataWriter::write_partition`). ──────────
    const ROW_HAS_TIMESTAMP: u8 = 0x04;
    const ROW_HAS_DELETION: u8 = 0x10;
    const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
    const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;
    const EXTENDED_IS_STATIC: u8 = 0x01;
    const CELL_IS_DELETED: u8 = 0x01;
    const CELL_IS_EXPIRING: u8 = 0x02;

    /// Deterministic stats baselines so temporal deltas are small single-byte
    /// vints, keeping the manual byte walk simple (mirrors issue_821).
    fn writer_stats() -> StatisticsMetadata {
        let mut s = StatisticsMetadata::new();
        s.min_timestamp = 1_000_000;
        s.min_ttl = 0;
        s.min_local_deletion_time = 0;
        s
    }

    /// Read a Cassandra unsigned vint at `pos`; returns `(value, bytes_consumed)`.
    fn read_vuint(data: &[u8], pos: usize) -> (u64, usize) {
        let first = data[pos];
        let extra = first.leading_ones() as usize;
        assert!(extra < 8, "9-byte vint not expected in this framing");
        let mask: u64 = 0xFFu64 >> (extra + 1);
        let mut value = (first as u64) & mask;
        for i in 0..extra {
            value = (value << 8) | data[pos + 1 + i] as u64;
        }
        (value, extra + 1)
    }

    /// 4-byte big-endian int partition-key bytes.
    fn int_key_bytes(n: i32) -> Vec<u8> {
        n.to_be_bytes().to_vec()
    }

    /// Partition-header byte size for a 4-byte int PK:
    /// 2 (u16 key-length) + 4 (key) + 4 (LDT i32) + 8 (mfda i64) = 18.
    const INT_PK_HEADER_SIZE: usize = 2 + 4 + 4 + 8;

    /// Schema with a single clustering column whose sort order is configurable.
    fn schema_one_clustering(ck_name: &str, ck_type: &str, order: ClusteringOrder) -> TableSchema {
        TableSchema {
            keyspace: "issue_822".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: ck_name.to_string(),
                data_type: ck_type.to_string(),
                position: 0,
                order,
            }],
            columns: vec![Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn empty_merger(schema: TableSchema) -> KWayMerger {
        KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema_arc: std::sync::Arc::new(schema.clone()),
            schema,
            _egress_slot: None,
        }
    }

    fn ck_text(col: &str, s: &str) -> ClusteringKey {
        ClusteringKey {
            columns: vec![(col.to_string(), Value::text(s.to_string()))],
        }
    }

    // ── #10: DESC empty-vs-valued clustering ordering ────────────────────────

    /// Direct assertion on `ClusteringKey::compare`: an EMPTY clustering value
    /// must route through the column's reversed-ness.
    ///
    /// Cassandra rule: under a reversed (DESC) clustering column, the empty
    /// clustering value sorts AFTER valued ones. CQLite models the empty value
    /// as `Text("")`; `compare_values` makes `"" < "a"` (ASC), and `compare`
    /// reverses that for DESC, so empty becomes GREATER (sorts last). Under ASC
    /// empty stays smallest. This test pins both directions.
    #[test]
    fn issue_10_desc_empty_vs_valued_via_clustering_compare() {
        let empty = ck_text("ck", "");
        let valued = ck_text("ck", "a");

        // ASC: empty < valued (empty sorts first).
        let asc = schema_one_clustering("ck", "text", ClusteringOrder::Asc);
        assert_eq!(
            empty.compare(&valued, &asc).expect("compare must not fail"),
            Ordering::Less,
            "ASC: empty clustering value must sort BEFORE a valued one"
        );

        // DESC: empty > valued (empty sorts last). This is the load-bearing
        // assertion for #10 — empty-vs-valued must be routed through reversed-ness.
        let desc = schema_one_clustering("ck", "text", ClusteringOrder::Desc);
        assert_eq!(
            empty
                .compare(&valued, &desc)
                .expect("compare must not fail"),
            Ordering::Greater,
            "DESC: empty clustering value must sort AFTER a valued one (reversed-ness \
             must apply to empty-vs-valued comparison)"
        );

        // Symmetry: valued-vs-empty is the mirror image under DESC.
        assert_eq!(
            valued
                .compare(&empty, &desc)
                .expect("compare must not fail"),
            Ordering::Less,
            "DESC: a valued clustering value must sort BEFORE the empty one"
        );
    }

    /// End-to-end through the real merge sort in `merge_partition_rows`: with a
    /// DESC clustering column, the emitted row order must place the empty
    /// clustering value LAST (after valued rows).
    #[test]
    fn issue_10_desc_empty_vs_valued_in_merge_output_order() {
        let schema = schema_one_clustering("ck", "text", ClusteringOrder::Desc);
        let merger = empty_merger(schema);

        let pk = DecoratedKey::new(7, vec![0, 0, 0, 7]);
        const TS: i64 = 1_700_000_000_000_000;

        let make = |ck: &str| {
            MergeEntry::new(
                0,
                pk.clone(),
                Some(ck_text("ck", ck)),
                TS,
                RowData::Live {
                    cells: vec![CellData {
                        column: "v".to_string(),
                        value: Value::text(format!("row-{ck}")),
                        timestamp: TS,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                        is_complex_element: false,
                        is_deleted: false,
                        has_empty_value: false,
                    }],
                },
            )
        };

        // Feed in deliberately scrambled input order.
        let input = vec![make(""), make("b"), make("a")];
        let merged = merger
            .merge_partition_rows(input)
            .expect("merge_partition_rows must not fail");

        let order: Vec<String> = merged
            .iter()
            .map(|e| match &e.clustering_key {
                Some(ck) => match &ck.columns[0].1 {
                    Value::Text(s) => String::from_utf8_lossy(s).into_owned(),
                    other => format!("{other:?}"),
                },
                None => "<none>".to_string(),
            })
            .collect();

        // DESC valued order is "b","a"; the empty value sorts LAST.
        assert_eq!(
            order,
            vec!["b".to_string(), "a".to_string(), "".to_string()],
            "DESC merge output: valued rows descending, empty clustering value LAST"
        );
    }

    // ── #13/#3: tombstone beats expiring at EQUAL timestamp ───────────────────

    /// At EQUAL timestamp a (cell) tombstone must beat an EXPIRING (TTL) cell.
    /// In CQLite's merge model an "expiring" cell is a live `CellData` with
    /// `ttl = Some(_)`; a cell tombstone is `Value::Tombstone(CellTombstone)`.
    /// `reconcile_cluster` (driven via `merge_partition_rows`) must keep the
    /// tombstone regardless of which file (run_index) it came from.
    #[test]
    fn issue_13_tombstone_beats_expiring_at_equal_ts() {
        let schema = schema_one_clustering("ck", "text", ClusteringOrder::Asc);
        let merger = empty_merger(schema);

        let pk = DecoratedKey::new(11, vec![0, 0, 0, 11]);
        const TS: i64 = 1_700_000_000_000_000;

        // Build both rows so the SAME clustering key bucket is formed: the cells
        // include the clustering column "ck" plus the data column "v".
        let ck_cell = || CellData {
            column: "ck".to_string(),
            value: Value::text("c".to_string()),
            timestamp: TS,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        };

        // Expiring (TTL) live cell for column "v", in the NEWER file (run 0).
        // run_index 0 would win a recency tiebreak — so if the tombstone still
        // wins, it is the equal-ts tombstone rule, not recency.
        let expiring = MergeEntry::new(
            0,
            pk.clone(),
            Some(ck_text("ck", "c")),
            TS,
            RowData::Live {
                cells: vec![
                    ck_cell(),
                    CellData {
                        column: "v".to_string(),
                        value: Value::text("expiring-if-buggy".to_string()),
                        timestamp: TS,
                        ttl: Some(3600),
                        cell_path: None,
                        local_deletion_time: None,
                        is_complex_element: false,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                ],
            },
        );

        // Cell tombstone for column "v", in the OLDER file (run 1), same ts.
        let tombstone = MergeEntry::new(
            1,
            pk.clone(),
            Some(ck_text("ck", "c")),
            TS,
            RowData::Live {
                cells: vec![
                    ck_cell(),
                    CellData {
                        column: "v".to_string(),
                        value: Value::Tombstone(Box::new(TombstoneInfo {
                            deletion_time: TS,
                            tombstone_type: TombstoneType::CellTombstone,
                            local_deletion_time: 0,
                            ttl: None,
                            range_start: None,
                            range_end: None,
                        })),
                        timestamp: TS,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                        is_complex_element: false,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                ],
            },
        );

        // Drive the real merger with the expiring (newer file) first.
        let merged = merger
            .merge_partition_rows(vec![expiring, tombstone])
            .expect("merge_partition_rows must not fail");

        assert_eq!(merged.len(), 1, "one clustering key => one merged winner");

        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live row, got {other:?}"),
        };
        let v_cell = cells
            .iter()
            .find(|c| c.column == "v")
            .expect("column v must survive (as a tombstone)");

        assert!(
            KWayMerger::is_cell_tombstone(v_cell),
            "At equal ts the cell tombstone must beat the expiring (TTL) cell, even \
             though the expiring cell is in the newer file (run 0). Got a live value \
             => tombstone-vs-expiring tie reverted to recency (#13/#3 regression)."
        );
        assert!(
            v_cell.ttl.is_none(),
            "Surviving cell tombstone must carry no TTL (IS_DELETED and IS_EXPIRING \
             are mutually exclusive)."
        );
    }

    /// Mirror case: tombstone is in the NEWER file and the expiring cell is
    /// older — tombstone still wins (it never depends on file order).
    #[test]
    fn issue_13_tombstone_beats_expiring_irrespective_of_run_index() {
        let schema = schema_one_clustering("ck", "text", ClusteringOrder::Asc);
        let merger = empty_merger(schema);

        let pk = DecoratedKey::new(12, vec![0, 0, 0, 12]);
        const TS: i64 = 1_700_000_000_000_000;

        let ck_cell = || CellData {
            column: "ck".to_string(),
            value: Value::text("c".to_string()),
            timestamp: TS,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        };

        let tombstone = MergeEntry::new(
            0,
            pk.clone(),
            Some(ck_text("ck", "c")),
            TS,
            RowData::Live {
                cells: vec![
                    ck_cell(),
                    CellData {
                        column: "v".to_string(),
                        value: Value::Tombstone(Box::new(TombstoneInfo {
                            deletion_time: TS,
                            tombstone_type: TombstoneType::CellTombstone,
                            local_deletion_time: 0,
                            ttl: None,
                            range_start: None,
                            range_end: None,
                        })),
                        timestamp: TS,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                        is_complex_element: false,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                ],
            },
        );

        let expiring = MergeEntry::new(
            1,
            pk.clone(),
            Some(ck_text("ck", "c")),
            TS,
            RowData::Live {
                cells: vec![
                    ck_cell(),
                    CellData {
                        column: "v".to_string(),
                        value: Value::text("expiring-if-buggy".to_string()),
                        timestamp: TS,
                        ttl: Some(3600),
                        cell_path: None,
                        local_deletion_time: None,
                        is_complex_element: false,
                        is_deleted: false,
                        has_empty_value: false,
                    },
                ],
            },
        );

        let merged = merger
            .merge_partition_rows(vec![tombstone, expiring])
            .expect("merge_partition_rows must not fail");

        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live row, got {other:?}"),
        };
        let v_cell = cells.iter().find(|c| c.column == "v").expect("v survives");
        assert!(
            KWayMerger::is_cell_tombstone(v_cell),
            "Tombstone must win the equal-ts tie over an expiring cell regardless of \
             run_index ordering."
        );
    }

    /// Strict flag semantics (#13/#3) PINNED ON REAL WRITTEN BYTES.
    ///
    /// On the WRITER side, `CELL_IS_EXPIRING` means `ttl != NO_TTL` and is mutually
    /// exclusive with `CELL_IS_DELETED`. The old version of this test only built a
    /// merge-model `CellData` and asserted its `ttl` field — tautological, proving
    /// nothing about production. This version drives the REAL production path:
    /// `DataWriter::write_partition` over a partition where the SAME column at the
    /// SAME timestamp is written both as an EXPIRING cell (`WriteWithTtl`) and as a
    /// cell tombstone (`Delete`). The writer's own equal-ts reconciliation
    /// (`merge_row_group`: a `Delete` wins the timestamp tie over a `WriteWithTtl`)
    /// must keep the tombstone, and the writer's own cell serializer
    /// (`write_tombstone_cell`) must emit a flags byte with `CELL_IS_DELETED` set
    /// and `CELL_IS_EXPIRING` NOT set. We assert on the actual Data.db bytes,
    /// mirroring the byte-walk in tests/issue_821_writer_byte_invariants.rs.
    #[test]
    fn issue_3_tombstone_beats_expiring_and_writer_never_sets_both_flags() {
        // int PK, int clustering, single regular text column `v`.
        let schema = TableSchema {
            keyspace: "issue_822".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        const TS: i64 = 2_000_000;
        let key = DecoratedKey::new(1, int_key_bytes(1));

        // Same clustering key, same timestamp: one EXPIRING write and one cell
        // DELETE of column `v`. The writer's equal-ts reconciliation must keep the
        // tombstone.
        let expiring = Mutation::new(
            TableId::new("issue_822", "tbl"),
            PartitionKey::single("pk", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(7))),
            vec![CellOperation::WriteWithTtl {
                column: "v".to_string(),
                value: Value::text("expiring-if-buggy".to_string()),
                ttl_seconds: 3600,
                local_deletion_time: None,
            }],
            TS,
            None,
        );
        let tombstone = Mutation::new(
            TableId::new("issue_822", "tbl"),
            PartitionKey::single("pk", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(7))),
            vec![CellOperation::Delete {
                column: "v".to_string(),
                local_deletion_time: None,
            }],
            TS,
            None,
        );

        // Drive the REAL writer (expiring first so a recency bug would surface it).
        let mut w = DataWriter::new(writer_stats());
        w.write_partition(&key, &[expiring, tombstone], &schema, None, &[])
            .expect("write_partition must succeed");
        let bytes = w.finish().expect("finish must succeed");

        // Walk to the single row's cell flags byte.
        //
        // Layout after the 18-byte int-PK partition header:
        //   [row_flags u8]
        //   [clustering prefix: 1 header byte + 4 int value bytes = 5]
        //   [row_size vint][prev_size vint]
        //   [timestamp delta vint  (ROW_HAS_TIMESTAMP)]
        //   [column bitmap vint    (NOT ROW_HAS_ALL_COLUMNS — a Delete is present)]
        //   [cell flags u8]   ← the byte under test
        let mut p = INT_PK_HEADER_SIZE;
        let row_flags = bytes[p];
        p += 1;
        // The surviving op is a cell DELETE, so HAS_ALL_COLUMNS must NOT be set
        // (the column subset/bitmap is written), and the row keeps its liveness
        // timestamp from the (losing) expiring write.
        assert_eq!(
            row_flags & ROW_HAS_EXTENDED_FLAGS,
            0,
            "regular (non-static) row expected"
        );
        assert_ne!(
            row_flags & ROW_HAS_TIMESTAMP,
            0,
            "row keeps liveness ts from the expiring write"
        );
        assert_eq!(
            row_flags & ROW_HAS_ALL_COLUMNS,
            0,
            "a surviving cell tombstone forces a column subset/bitmap (NOT all-columns)"
        );
        // Clustering prefix: 1 header byte + 4 int value bytes.
        p += 1 + 4;
        // row_size vint, then prev_size vint (inside the body).
        let (_row_size, rs_len) = read_vuint(&bytes, p);
        p += rs_len;
        let (_prev_size, ps_len) = read_vuint(&bytes, p);
        p += ps_len;
        // Liveness timestamp delta vint (ROW_HAS_TIMESTAMP is set, no TTL on row).
        let (_ts_delta, ts_len) = read_vuint(&bytes, p);
        p += ts_len;
        // Column subset bitmap vint (NOT all-columns).
        let (_bitmap, bm_len) = read_vuint(&bytes, p);
        p += bm_len;

        // The single surviving cell's flags byte.
        let cell_flags = bytes[p];
        assert_ne!(
            cell_flags & CELL_IS_DELETED,
            0,
            "At equal ts the cell tombstone must win and be serialized as a deleted \
             cell (CELL_IS_DELETED set). Flags byte = {cell_flags:#04x}"
        );
        assert_eq!(
            cell_flags & CELL_IS_EXPIRING,
            0,
            "A tombstone cell must NOT carry CELL_IS_EXPIRING — IS_DELETED and \
             IS_EXPIRING are mutually exclusive. Flags byte = {cell_flags:#04x}"
        );
        assert_ne!(
            cell_flags & (CELL_IS_DELETED | CELL_IS_EXPIRING),
            CELL_IS_DELETED | CELL_IS_EXPIRING,
            "the writer must never set BOTH CELL_IS_DELETED and CELL_IS_EXPIRING on \
             one cell. Flags byte = {cell_flags:#04x}"
        );

        // The exact flags byte: tombstone (CELL_IS_DELETED) plus HAS_EMPTY_VALUE
        // (0x04), and crucially NOT CELL_IS_EXPIRING. write_tombstone_cell emits
        // CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE = 0x05.
        assert_eq!(
            cell_flags, 0x05,
            "surviving cell tombstone must serialize as CELL_IS_DELETED|HAS_EMPTY \
             (0x05); got {cell_flags:#04x}"
        );
    }

    /// Issue #848: writer-path equal-ts tie-break, REVERSED source order. The
    /// cell tombstone arrives FIRST, the expiring write SECOND (newer in
    /// recency). The writer's `cells` LWW must still keep the tombstone — an
    /// equal-ts expiring write does NOT supersede an existing cell tombstone
    /// (parity `a62c749`). Pins the writer path agrees with `reconcile_cluster`
    /// in BOTH source orders.
    #[test]
    fn issue_848_writer_tombstone_beats_expiring_tombstone_first() {
        let schema = TableSchema {
            keyspace: "issue_822".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        const TS: i64 = 2_000_000;
        let key = DecoratedKey::new(1, int_key_bytes(1));

        let tombstone = Mutation::new(
            TableId::new("issue_822", "tbl"),
            PartitionKey::single("pk", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(7))),
            vec![CellOperation::Delete {
                column: "v".to_string(),
                local_deletion_time: None,
            }],
            TS,
            None,
        );
        let expiring = Mutation::new(
            TableId::new("issue_822", "tbl"),
            PartitionKey::single("pk", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(7))),
            vec![CellOperation::WriteWithTtl {
                column: "v".to_string(),
                value: Value::text("expiring-if-buggy".to_string()),
                ttl_seconds: 3600,
                local_deletion_time: None,
            }],
            TS,
            None,
        );

        // Tombstone FIRST so a recency bug favoring the later expiring write
        // would surface (the reverse of issue_3_tombstone_beats_expiring).
        let mut w = DataWriter::new(writer_stats());
        w.write_partition(&key, &[tombstone, expiring], &schema, None, &[])
            .expect("write_partition must succeed");
        let bytes = w.finish().expect("finish must succeed");

        let mut p = INT_PK_HEADER_SIZE;
        let row_flags = bytes[p];
        p += 1;
        assert_eq!(
            row_flags & ROW_HAS_ALL_COLUMNS,
            0,
            "a surviving cell tombstone forces a column subset/bitmap"
        );
        // Clustering prefix: 1 header byte + 4 int value bytes.
        p += 1 + 4;
        let (_row_size, rs_len) = read_vuint(&bytes, p);
        p += rs_len;
        let (_prev_size, ps_len) = read_vuint(&bytes, p);
        p += ps_len;
        let (_ts_delta, ts_len) = read_vuint(&bytes, p);
        p += ts_len;
        let (_bitmap, bm_len) = read_vuint(&bytes, p);
        p += bm_len;

        let cell_flags = bytes[p];
        assert_ne!(
            cell_flags & CELL_IS_DELETED,
            0,
            "tombstone-first: at equal ts the cell tombstone must still win \
             (CELL_IS_DELETED set). Flags byte = {cell_flags:#04x}"
        );
        assert_eq!(
            cell_flags & CELL_IS_EXPIRING,
            0,
            "the surviving tombstone must NOT carry CELL_IS_EXPIRING. \
             Flags byte = {cell_flags:#04x}"
        );
        assert_eq!(
            cell_flags, 0x05,
            "surviving cell tombstone serializes as CELL_IS_DELETED|HAS_EMPTY \
             (0x05); got {cell_flags:#04x}"
        );
    }

    /// Companion merge-layer invariant (kept, but now secondary to the byte-level
    /// assertion above): the equal-ts reconcile in `reconcile_cluster` keeps the
    /// tombstone and never produces a `CellData` that is BOTH a cell tombstone AND
    /// carries a TTL. This is the precondition that lets the writer keep the two
    /// flags exclusive; the writer side is now pinned on real bytes above.
    #[test]
    fn issue_3_merge_layer_tombstone_carries_no_ttl_precondition() {
        let tomb = CellData {
            column: "v".to_string(),
            value: Value::Tombstone(Box::new(TombstoneInfo {
                deletion_time: 1,
                tombstone_type: TombstoneType::CellTombstone,
                local_deletion_time: 0,
                ttl: None,
                range_start: None,
                range_end: None,
            })),
            timestamp: 1,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        };
        assert!(KWayMerger::is_cell_tombstone(&tomb));
        assert!(
            tomb.ttl.is_none(),
            "A cell tombstone must not carry a TTL (precondition for flag exclusivity)."
        );

        let expiring = CellData {
            column: "v".to_string(),
            value: Value::text("x".to_string()),
            timestamp: 1,
            ttl: Some(60),
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        };
        assert!(
            !KWayMerger::is_cell_tombstone(&expiring),
            "An expiring (TTL) cell is LIVE, not a tombstone."
        );
        assert!(expiring.ttl.is_some(), "Expiring cell carries a TTL.");
    }

    // ── #22: header-driven static / column superset (DIVERGENT) ──────────────

    /// #22 is DIVERGENT in CQLite. Cassandra derives `hasStatic` and the column
    /// superset from the merged input SSTables' `SerializationHeader`s, so a
    /// DROPPED static column (whose static rows still exist on disk) is still
    /// emitted. CQLite's compaction writer instead derives `hasStatic` and the
    /// column set purely from the supplied `TableSchema`
    /// (`schema.columns.iter().any(|c| c.is_static)` in data_writer.rs). There is
    /// NO `SerializationHeader` read anywhere under `storage/write_engine/`.
    ///
    /// This test PINS THE DIVERGENCE ON REAL WRITER OUTPUT (not a reimplemented
    /// predicate). The old version asserted on a local copy of
    /// `|s| s.columns.iter().any(|c| c.is_static)`, so it stayed green even if the
    /// production writer changed. This version drives the REAL production path —
    /// `DataWriter::write_partition` — with the SAME partition data (a mutation
    /// that writes a static cell AND a regular cell) under two schemas:
    ///
    ///   * schema WITH the static column → the writer MUST emit a static-row
    ///     prelude (a row with `ROW_HAS_EXTENDED_FLAGS` + `EXTENDED_IS_STATIC`),
    ///   * schema with the static column DROPPED → the writer MUST NOT emit any
    ///     static-row prelude, even though a real cluster's on-disk
    ///     SerializationHeader still records the static column.
    ///
    /// Both observations are read from the OBSERVED Data.db bytes, so a change to
    /// the writer's static-emission decision (e.g. becoming header-driven) flips
    /// this test instead of silently passing. The static op data is byte-identical
    /// across the two runs; ONLY the schema differs — so the divergence is
    /// attributable purely to the schema-driven `schema_has_static` decision in
    /// `write_partition`.
    #[test]
    fn issue_22_static_emission_is_schema_driven_not_header_driven_divergent() {
        // Schema WITH a static column.
        let schema_with_static = TableSchema {
            keyspace: "issue_822".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![
                Column {
                    name: "s".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: true,
                },
                Column {
                    name: "v".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        // Same table after the static column was DROPPED from the schema. On a real
        // cluster the input SSTables' SerializationHeader still records the static
        // column and static rows still exist on disk — but CQLite only sees the
        // schema.
        let mut schema_dropped_static = schema_with_static.clone();
        schema_dropped_static.columns.retain(|c| c.name != "s");

        // IDENTICAL partition data for both runs: a mutation that writes the static
        // column `s` AND a regular cell `v`. Only the SCHEMA differs between runs.
        let key = DecoratedKey::new(1, int_key_bytes(1));
        let mutation = || {
            Mutation::new(
                TableId::new("issue_822", "tbl"),
                PartitionKey::single("pk", Value::Integer(1)),
                Some(ClusteringKey::single("ck", Value::Integer(7))),
                vec![
                    CellOperation::Write {
                        column: "s".to_string(),
                        value: Value::text("static-val".to_string()),
                    },
                    CellOperation::Write {
                        column: "v".to_string(),
                        value: Value::text("row-val".to_string()),
                    },
                ],
                2_000_000,
                None,
            )
        };

        // Deterministically walk one unfiltered starting at its flags byte `pos`.
        // Returns `(flags, ext_flags, next_pos)`. Modeled on
        // `tests/issue_821_writer_byte_invariants.rs::walk_unfiltered`: it parses
        // the partition structure (flags → ext flags → clustering prefix →
        // row_size vint → body) so we land on real row-flag bytes only — we never
        // scan the buffer for a stray high-bit byte (VInt lengths/timestamps and
        // cell payload bytes can legitimately set bit 0x80). The int-clustering
        // schema's non-static clustering prefix is 1 header byte + 4 value bytes.
        const INT_CLUSTERING_PREFIX_LEN: usize = 1 + 4;
        let walk_unfiltered = |bytes: &[u8], pos: usize| -> (u8, Option<u8>, usize) {
            let mut p = pos;
            let flags = bytes[p];
            p += 1;
            let mut ext = None;
            if flags & ROW_HAS_EXTENDED_FLAGS != 0 {
                ext = Some(bytes[p]);
                p += 1;
            }
            let is_static = ext.is_some_and(|e| e & EXTENDED_IS_STATIC != 0);
            // Non-static rows carry a clustering prefix before row_size; static
            // rows do not.
            if !is_static {
                p += INT_CLUSTERING_PREFIX_LEN;
            }
            let (row_size, rs_len) = read_vuint(bytes, p);
            p += rs_len;
            // row_size counts the body (prev_size vint + remaining body); the next
            // unfiltered begins immediately after it.
            let next = p + row_size as usize;
            (flags, ext, next)
        };

        // Parse the first unfiltered after the int-PK partition header and report
        // whether it is a static-row prelude. The static row, if present, is
        // ALWAYS the first unfiltered (this schema/layout is known), so we assert
        // on the flags byte at the parsed partition-header position — not on an
        // arbitrary high-bit byte found elsewhere in the buffer.
        let first_unfiltered_is_static = |bytes: &[u8]| -> bool {
            let (flags, ext, _next) = walk_unfiltered(bytes, INT_PK_HEADER_SIZE);
            flags & ROW_HAS_EXTENDED_FLAGS != 0 && ext.is_some_and(|e| e & EXTENDED_IS_STATIC != 0)
        };

        // ── Run 1: schema WITH static → static prelude MUST be emitted. ──
        let mut w_with = DataWriter::new(writer_stats());
        w_with
            .write_partition(&key, &[mutation()], &schema_with_static, None, &[])
            .expect("write_partition (with static) must succeed");
        let bytes_with = w_with.finish().expect("finish (with static)");
        assert!(
            first_unfiltered_is_static(&bytes_with),
            "schema WITH a static column: the writer must emit a static-row prelude \
             (ROW_HAS_EXTENDED_FLAGS | EXTENDED_IS_STATIC) as the first unfiltered"
        );

        // ── Run 2: schema with static DROPPED → NO static prelude. ──
        // Same partition data; only the schema lost the static column.
        let mut w_drop = DataWriter::new(writer_stats());
        w_drop
            .write_partition(&key, &[mutation()], &schema_dropped_static, None, &[])
            .expect("write_partition (dropped static) must succeed");
        let bytes_drop = w_drop.finish().expect("finish (dropped static)");
        assert!(
            !first_unfiltered_is_static(&bytes_drop),
            "DIVERGENT (#22): with the static column dropped from the SCHEMA, CQLite's \
             writer emits NO static-row prelude — even though a real cluster's on-disk \
             SerializationHeader still records the static column and Cassandra \
             (header-driven) would still emit its static rows. The writer's decision \
             is schema-driven, observed here on the real Data.db bytes. If CQLite \
             becomes header-driven this assertion must change."
        );

        // Belt and suspenders: no EXTENDED_IS_STATIC row exists ANYWHERE in the
        // dropped-static output, proving the static prelude was not merely
        // relocated. We DETERMINISTICALLY walk the unfiltered chain (parsing each
        // row's flags/ext/clustering/row_size) until the END_OF_PARTITION sentinel,
        // rather than scanning every buffer byte for a high bit — VInt and cell
        // payload bytes can legitimately set 0x80, so a raw scan would be
        // spuriously fragile.
        const END_OF_PARTITION: u8 = 0x01;
        let mut p = INT_PK_HEADER_SIZE;
        while p < bytes_drop.len() {
            // The unfiltered chain is terminated by the END_OF_PARTITION sentinel,
            // which occupies a real row-flags position (so this check is exact).
            if bytes_drop[p] == END_OF_PARTITION {
                break;
            }
            let (flags, ext, next) = walk_unfiltered(&bytes_drop, p);
            if flags & ROW_HAS_EXTENDED_FLAGS != 0 {
                assert_eq!(
                    ext.expect("HAS_EXTENDED_FLAGS implies an ext-flags byte") & EXTENDED_IS_STATIC,
                    0,
                    "no EXTENDED_IS_STATIC row may appear once the static column is \
                     dropped from the schema"
                );
            }
            assert!(next > p, "unfiltered walk must advance");
            p = next;
        }

        // Sanity: ROW_HAS_DELETION is unused here (no tombstones), so the constant
        // is referenced to keep the wire-format mirror complete and avoid an
        // unused-const warning under -D warnings.
        let _ = ROW_HAS_DELETION;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// #886 (Epic #842) branch-review: phantom EMPTY partition on the writer path
// ─────────────────────────────────────────────────────────────────────────────
//
// A partition whose ONLY merged row is a metadata-only no-op (empty Live cells
// carrying complex/range deletion metadata) must NOT produce a partition in the
// output SSTable. After filtering those entries the `mutations` Vec is empty;
// calling `SSTableWriter::write_partition` with no mutations would still emit a
// partition header/end marker and register the key in Index/Filter/Summary/
// statistics — a PHANTOM empty partition. The writer path must skip such
// partitions entirely and not count them in the output partition/row stats.
#[cfg(all(test, feature = "write-support"))]
mod issue_886_empty_partition_skip {
    use super::*;
    use crate::schema::{KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::{DecoratedKey, PartitionKey};
    use std::collections::HashMap;

    /// Single-column `int` partition-key schema with one regular `name` column.
    fn schema() -> TableSchema {
        TableSchema {
            keyspace: "i886".to_string(),
            table: "phantom".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![crate::schema::Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// Valid on-disk partition-key bytes for `id = n` under [`schema`], built
    /// through the shared codec so the merge→writer path can decode them.
    fn pk_bytes(schema: &TableSchema, n: i32) -> Vec<u8> {
        PartitionKey::single("id", Value::Integer(n))
            .to_bytes(schema)
            .expect("encode int partition key")
    }

    /// An in-memory run yielding a fixed list of `MergeEntry`s in order.
    struct VecIterator(std::vec::IntoIter<MergeEntry>);
    impl SSTableRowIterator for VecIterator {
        fn next(&mut self) -> Option<Result<MergeEntry>> {
            self.0.next().map(Ok)
        }
    }

    fn merger_over(entries: Vec<MergeEntry>, schema: TableSchema) -> KWayMerger {
        KWayMerger {
            runs: vec![RunReader::new(Box::new(VecIterator(entries.into_iter())))],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema_arc: std::sync::Arc::new(schema.clone()),
            schema,
            _egress_slot: None,
        }
    }

    /// END-TO-END writer-path test (#886): a partition whose ONLY merged row is a
    /// metadata-only no-op must produce NO partition in the output SSTable, must
    /// not be counted in `MergeStats.output_partitions`/`output_rows`, and must
    /// not register a key in Index/Filter/Summary/statistics — proven by the
    /// writer's authoritative on-disk `SSTableInfo.partition_count`. A sibling
    /// NORMAL partition in the same merge must still be written unchanged.
    #[tokio::test]
    async fn empty_partition_is_skipped_on_writer_path() {
        let schema = schema();

        // Partition token 1: ONLY a truly-empty no-op (empty Live, no metadata).
        // It reconciles to nothing, so after filtering this partition's mutations
        // are empty. (A range-deletion carrier is no longer a no-op under #933, so
        // a genuinely-empty entry is used here to exercise the phantom-skip path.)
        let meta_only = MergeEntry::new(
            0,
            DecoratedKey::new(1, pk_bytes(&schema, 1)),
            None,
            0,
            RowData::Live { cells: vec![] },
        );
        assert!(
            meta_only.is_metadata_only_no_op(),
            "test precondition: the token-1 entry must be a metadata-only no-op"
        );

        // Partition token 2: a NORMAL live row with a real cell — must be written.
        let live = MergeEntry::new(
            0,
            DecoratedKey::new(2, pk_bytes(&schema, 2)),
            None,
            100,
            RowData::Live {
                cells: vec![CellData::new(
                    "name".to_string(),
                    Value::text("survivor".to_string()),
                    100,
                )],
            },
        );

        let merger = merger_over(vec![meta_only, live], schema.clone());

        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
            temp_dir.path().to_path_buf(),
            1,
            &schema,
        )
        .expect("create writer");
        // Issue #1668 stage 5c-iv part 2: `KWayMerger::merge` streams
        // partitions through `begin_partition_incremental`, which requires
        // pre-seeded encoding baselines — matching the one real production
        // caller (`compact_sstables_with_registry`). No tombstones/TTLs in
        // this data, so seed a safe floor below every cell timestamp.
        writer.pre_seed_encoding_baselines(0, i32::MAX, i32::MAX);

        let stats = merger.merge(&mut writer).expect("merge must succeed");

        // The metadata-only partition contributes nothing to the output stats.
        assert_eq!(
            stats.output_partitions, 1,
            "only the normal partition counts as an output partition (no phantom)"
        );
        assert_eq!(
            stats.output_rows, 1,
            "only the normal partition's single live row counts toward output rows"
        );

        // The writer's on-disk partition counter is incremented exactly once per
        // `write_partition` call. If the phantom partition had been written it
        // would be 2; the skip keeps it at 1.
        let info = writer.finish().await.expect("finish must succeed");
        assert_eq!(
            info.partition_count, 1,
            "the output SSTable must contain exactly ONE partition; a phantom EMPTY \
             partition for the metadata-only-only key would make this 2"
        );
    }

    /// Guard: a partition with real content is unaffected — both a normal live
    /// partition AND a metadata-only no-op coexisting in the SAME partition (the
    /// live row survives) still writes that one partition.
    #[tokio::test]
    async fn partition_with_real_content_is_still_written() {
        let schema = schema();

        let live = MergeEntry::new(
            0,
            DecoratedKey::new(7, pk_bytes(&schema, 7)),
            None,
            200,
            RowData::Live {
                cells: vec![CellData::new(
                    "name".to_string(),
                    Value::text("keep-me".to_string()),
                    200,
                )],
            },
        );

        let merger = merger_over(vec![live], schema.clone());

        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
            temp_dir.path().to_path_buf(),
            1,
            &schema,
        )
        .expect("create writer");
        // Issue #1668 stage 5c-iv part 2: see the sibling test above for why
        // this pre-seed is required.
        writer.pre_seed_encoding_baselines(0, i32::MAX, i32::MAX);

        let stats = merger.merge(&mut writer).expect("merge must succeed");
        assert_eq!(stats.output_partitions, 1);
        assert_eq!(stats.output_rows, 1);

        let info = writer.finish().await.expect("finish must succeed");
        assert_eq!(
            info.partition_count, 1,
            "a normal partition must still be written"
        );
    }
}

/// Issue #912: clustering-row tombstones must carry their clustering identity
/// through the compaction stream so they reconcile in their OWN clustering bucket
/// instead of collapsing into the partition's single `None` bucket (where they
/// would mis-reconcile against each other and against the static row).
#[cfg(all(test, feature = "write-support"))]
mod issue_912_row_tombstone_clustering_identity {
    use super::*;
    use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
    use crate::storage::sstable::reader::compaction_row::{
        CompactionRow, CompactionRowData, SimpleCell,
    };
    use crate::types::RowKey;
    use std::collections::HashMap;

    fn clustered_schema() -> TableSchema {
        TableSchema {
            keyspace: "ks912".to_string(),
            table: "t912".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: Default::default(),
            }],
            columns: vec![Column {
                name: "v".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn row_tombstone(ck: i64) -> CompactionRowData {
        CompactionRowData::Tombstone {
            deletion_time: 1_000 + ck,
            local_deletion_time: 42,
            clustering: vec![("ck".to_string(), Value::Integer(ck as i32))],
        }
    }

    /// Core fix: a row tombstone's captured clustering prefix yields a concrete
    /// `ClusteringKey`, and two tombstones at distinct clustering keys produce
    /// DISTINCT keys (pre-#912 both returned `None`).
    #[test]
    fn tombstone_clustering_identity_is_distinct() {
        let schema = clustered_schema();

        let ck5 = SSTableRowIteratorAdapter::extract_clustering_key_from_compaction(
            &row_tombstone(5),
            &schema,
        );
        let ck9 = SSTableRowIteratorAdapter::extract_clustering_key_from_compaction(
            &row_tombstone(9),
            &schema,
        );

        assert_eq!(
            ck5,
            Some(ClusteringKey {
                columns: vec![("ck".to_string(), Value::Integer(5))],
            }),
            "row tombstone must carry its clustering identity (#912)"
        );
        assert_ne!(
            ck5, ck9,
            "distinct clustering-row tombstones must not share a bucket"
        );
    }

    /// A tombstone with no captured clustering (a static row, or an unclustered
    /// table; a PARTIAL prefix is refused upstream, #3809) keeps the `None` bucket.
    #[test]
    fn tombstone_without_clustering_falls_into_none_bucket() {
        let schema = clustered_schema();
        let bare = CompactionRowData::Tombstone {
            deletion_time: 1,
            local_deletion_time: 0,
            clustering: Vec::new(),
        };
        assert_eq!(
            SSTableRowIteratorAdapter::extract_clustering_key_from_compaction(&bare, &schema),
            None
        );
    }

    /// A live clustering row still resolves its clustering key from its surfaced
    /// simple cells (unchanged path), matching the tombstone's key for the same
    /// `ck` so they share a reconcile bucket.
    #[test]
    fn live_and_tombstone_same_ck_share_bucket() {
        let schema = clustered_schema();
        let live = CompactionRowData::Live {
            simple: vec![SimpleCell {
                column: "ck".to_string(),
                value: Value::Integer(5),
                timestamp: 10,
                ttl: None,
                local_deletion_time: None,
            }],
            complex: Vec::new(),
            row_deletion: None,
            row_liveness: Default::default(),
        };
        let live_ck =
            SSTableRowIteratorAdapter::extract_clustering_key_from_compaction(&live, &schema);
        let tomb_ck = SSTableRowIteratorAdapter::extract_clustering_key_from_compaction(
            &row_tombstone(5),
            &schema,
        );
        assert_eq!(
            live_ck, tomb_ck,
            "same ck => same bucket for live row and its tombstone"
        );
        assert!(live_ck.is_some());
    }

    /// End-to-end through `build_merge_entry`: two row-tombstone `CompactionRow`s
    /// in one partition produce two MergeEntries in DISTINCT clustering buckets, so
    /// `merge_partition_rows` keeps BOTH tombstones. Pre-#912 both collapsed into
    /// the `None` bucket and only one survived.
    #[test]
    fn two_row_tombstones_do_not_collapse_in_merge() {
        let schema = clustered_schema();
        let pk = RowKey::new(vec![0, 0, 0, 7]);

        let e5 = SSTableRowIteratorAdapter::build_merge_entry(
            0,
            CompactionRow {
                key: pk.clone(),
                row_timestamp: 1_005,
                row_data: row_tombstone(5),
            },
            &schema,
        )
        .expect("build_merge_entry");
        let e9 = SSTableRowIteratorAdapter::build_merge_entry(
            1,
            CompactionRow {
                key: pk.clone(),
                row_timestamp: 1_009,
                row_data: row_tombstone(9),
            },
            &schema,
        )
        .expect("build_merge_entry");

        assert_ne!(
            e5.clustering_key, e9.clustering_key,
            "two distinct row tombstones must land in distinct buckets (#912)"
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: std::collections::BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema: schema.clone(),
            schema_arc: std::sync::Arc::new(schema.clone()),
            _egress_slot: None,
        };
        let merged = merger
            .merge_partition_rows(vec![e5, e9])
            .expect("merge_partition_rows");

        assert_eq!(
            merged.len(),
            2,
            "both clustering-row tombstones must survive; pre-#912 they collapsed to one"
        );
        assert!(
            merged
                .iter()
                .all(|m| matches!(m.row_data, RowData::Tombstone { .. })),
            "both surviving entries must be row tombstones"
        );
    }
}

/// Issue #873: a row tombstone's source `localDeletionTime` (LDT, the GC clock
/// instant in seconds) must be preserved through the compaction merge→rewrite
/// path. Pre-#873 `reconcile_cluster` tracked only the winning `deletion_time`
/// and rebuilt the surviving tombstone with `local_deletion_time: 0`, and
/// `merge_entry_to_mutation` passed `None` for the mutation LDT — so the writer
/// re-derived the LDT from the deletion *timestamp* (`timestamp_micros /
/// 1_000_000`). That breaks gc_grace semantics and, for a pathological logical
/// (non-wall-clock) timestamp delete, can underflow the unsigned row-deletion
/// LDT delta in the writer and corrupt Data.db.
#[cfg(all(test, feature = "write-support"))]
mod issue_873_preserve_row_tombstone_ldt {
    use super::*;
    use crate::schema::{KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::DecoratedKey;
    use std::collections::HashMap;

    fn unclustered_schema() -> TableSchema {
        TableSchema {
            keyspace: "ks873".to_string(),
            table: "t873".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn tombstone_entry(run_index: usize, deletion_time: i64, ldt: i32) -> MergeEntry {
        MergeEntry::new(
            run_index,
            DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
            None,
            deletion_time,
            RowData::Tombstone {
                deletion_time,
                local_deletion_time: ldt,
            },
        )
    }

    /// `reconcile_cluster` must carry the winning tombstone's source LDT through
    /// to the rebuilt surviving tombstone instead of hardcoding 0, and the LDT it
    /// keeps must be the one paired with the winning (max) `deletion_time` — NOT
    /// re-derived from that timestamp.
    #[test]
    fn reconcile_cluster_preserves_winning_tombstone_ldt() {
        // deletion_time chosen so it does NOT equal LDT (which is GC-clock seconds):
        // a logical-timestamp delete where micros and the wall-clock LDT diverge.
        let deletion_time = 1_000_000_000_000_000i64; // micros
        let source_ldt = 1_700_000_000i32; // seconds (wall clock)

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![tombstone_entry(0, deletion_time, source_ldt)],
            &HashMap::new(),
            None,
        )
        .expect("a surviving row tombstone must be emitted");

        match merged.row_data {
            RowData::Tombstone {
                deletion_time: dt,
                local_deletion_time,
            } => {
                assert_eq!(dt, deletion_time, "deletion_time must survive");
                assert_eq!(
                    local_deletion_time, source_ldt,
                    "the source LDT must be preserved, not reset to 0 nor derived from the timestamp"
                );
            }
            other => panic!("expected Tombstone, got {:?}", other),
        }
    }

    /// When several row tombstones reconcile, the LDT kept must pair with the
    /// winning (max) `deletion_time`, not the max LDT and not the first-seen LDT.
    #[test]
    fn reconcile_cluster_keeps_ldt_paired_with_max_deletion_time() {
        // Older delete (smaller deletion_time) but LARGER LDT; newer delete
        // (larger deletion_time) with a SMALLER LDT. The winner is the newer
        // delete, so its (smaller) LDT must be the one carried through.
        let older = tombstone_entry(0, 100_000_000, 1_900_000_000);
        let newer = tombstone_entry(1, 200_000_000, 1_500_000_000);

        let merged = KWayMerger::reconcile_cluster(None, vec![older, newer], &HashMap::new(), None)
            .expect("a surviving row tombstone must be emitted");

        match merged.row_data {
            RowData::Tombstone {
                deletion_time,
                local_deletion_time,
            } => {
                assert_eq!(deletion_time, 200_000_000, "the newer delete must win");
                assert_eq!(
                    local_deletion_time, 1_500_000_000,
                    "the LDT carried must be the one paired with the winning deletion_time"
                );
            }
            other => panic!("expected Tombstone, got {:?}", other),
        }
    }

    /// `merge_entry_to_mutation` must thread a row tombstone's LDT onto the
    /// produced mutation so the writer emits it verbatim instead of re-deriving it
    /// from `timestamp_micros`. A live row keeps `local_deletion_time == None`.
    #[test]
    fn merge_entry_to_mutation_threads_row_tombstone_ldt() {
        let schema = unclustered_schema();
        let deletion_time = 1_000_000_000_000_000i64;
        let source_ldt = 1_700_000_000i32;

        let mutation = KWayMerger::merge_entry_to_mutation(
            tombstone_entry(0, deletion_time, source_ldt),
            &schema,
        )
        .expect("conversion should succeed");

        assert_eq!(
            mutation.local_deletion_time,
            Some(source_ldt),
            "the row tombstone's source LDT must be threaded onto the mutation"
        );
        assert_eq!(
            mutation.effective_local_deletion_time(),
            source_ldt,
            "the writer must use the preserved LDT, not the timestamp-derived one"
        );
        // The timestamp-derived LDT would be deletion_time / 1_000_000 =
        // 1_000_000_000, which must NOT be what the writer would stamp.
        assert_ne!(
            mutation.effective_local_deletion_time() as i64,
            deletion_time / 1_000_000,
            "the LDT must NOT be re-derived from the deletion timestamp"
        );
    }

    /// A row tombstone whose LDT is the `0` "not surfaced" placeholder (the legacy
    /// `from_legacy_value` fallback and pre-V5 row tombstones) must NOT pin an
    /// explicit LDT — doing so would lose the writer's timestamp-derived fallback
    /// and could trip the below-baseline guard against a nonzero pre-seeded
    /// `min_local_deletion_time`, regressing previously-valid compactions (#946).
    #[test]
    fn merge_entry_to_mutation_placeholder_ldt_stays_unset() {
        let schema = unclustered_schema();
        let deletion_time = 1_000_000_000_000_000i64;

        let mutation = KWayMerger::merge_entry_to_mutation(
            // ldt = 0 is the reader's "no LDT surfaced" sentinel, not a real delete.
            tombstone_entry(0, deletion_time, 0),
            &schema,
        )
        .expect("conversion should succeed");

        assert_eq!(
            mutation.local_deletion_time, None,
            "a placeholder (0) LDT must leave the mutation LDT unset so the writer \
             keeps deriving it from the timestamp, exactly as before #873"
        );
    }

    /// A live (non-tombstone) row must leave the mutation LDT unset so the writer
    /// keeps its historical timestamp-derived behavior for cell tombstones.
    #[test]
    fn merge_entry_to_mutation_live_row_leaves_ldt_none() {
        let schema = unclustered_schema();
        let entry = MergeEntry::new(
            0,
            DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
            None,
            100,
            RowData::Live {
                cells: vec![CellData::new(
                    "value".to_string(),
                    Value::text("alive".to_string()),
                    100,
                )],
            },
        );

        let mutation =
            KWayMerger::merge_entry_to_mutation(entry, &schema).expect("conversion should succeed");
        assert_eq!(
            mutation.local_deletion_time, None,
            "a live row must not pin an explicit LDT"
        );
    }

    /// Issue #932: when a row tombstone (older) reconciles with a surviving cell
    /// (newer) for the same `(pk, ck)`, `reconcile_cluster` must emit a LIVE entry
    /// carrying the surviving cell AND the coexisting `row_deletion` (so the
    /// deletion keeps shadowing older cells of other columns in SSTables not part
    /// of a partial compaction). Pre-#932 the row deletion was DROPPED.
    #[test]
    fn reconcile_cluster_attaches_row_deletion_when_cells_survive() {
        let deletion_time = 100i64;
        let source_ldt = 1_700_000_000i32;
        // A row tombstone at ts=100 and a live `name` cell at ts=300 (> 100).
        let tomb = tombstone_entry(1, deletion_time, source_ldt);
        let live = MergeEntry::new(
            0,
            DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
            None,
            300,
            RowData::Live {
                cells: vec![CellData::new(
                    "name".to_string(),
                    Value::text("new".to_string()),
                    300,
                )],
            },
        );

        let merged = KWayMerger::reconcile_cluster(None, vec![tomb, live], &HashMap::new(), None)
            .expect("a live coexistence row must be emitted");

        match &merged.row_data {
            RowData::Live { cells } => {
                assert_eq!(cells.len(), 1, "the surviving name cell must remain");
                assert_eq!(cells[0].column, "name");
                assert_eq!(cells[0].timestamp, 300);
            }
            other => panic!("expected a Live coexistence row, got {:?}", other),
        }
        assert_eq!(
            merged.row_deletion,
            Some((deletion_time, source_ldt)),
            "the coexisting row deletion (and its source LDT) must be preserved on the live entry"
        );
    }

    /// Issue #932: an older cell shadowed by the coexisting row deletion must be
    /// dropped from the surviving set (a cell at ts <= row_del is shadowed) while
    /// the deletion is still carried for the newer survivor.
    #[test]
    fn reconcile_cluster_shadows_older_cell_under_coexisting_deletion() {
        let deletion_time = 100i64;
        let tomb = tombstone_entry(1, deletion_time, 1_700_000_000);
        // Older `score` cell at ts=50 (<= 100, must be shadowed) and a newer
        // `name` cell at ts=300 (> 100, must survive), in two runs.
        let older = MergeEntry::new(
            2,
            DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
            None,
            50,
            RowData::Live {
                cells: vec![CellData::new("score".to_string(), Value::Integer(999), 50)],
            },
        );
        let newer = MergeEntry::new(
            0,
            DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
            None,
            300,
            RowData::Live {
                cells: vec![CellData::new(
                    "name".to_string(),
                    Value::text("new".to_string()),
                    300,
                )],
            },
        );

        let merged =
            KWayMerger::reconcile_cluster(None, vec![tomb, older, newer], &HashMap::new(), None)
                .expect("a live coexistence row must be emitted");

        match &merged.row_data {
            RowData::Live { cells } => {
                assert_eq!(cells.len(), 1, "only the newer cell survives");
                assert_eq!(
                    cells[0].column, "name",
                    "the older `score` cell is shadowed"
                );
            }
            other => panic!("expected a Live coexistence row, got {:?}", other),
        }
        assert_eq!(
            merged.row_deletion.map(|(dt, _)| dt),
            Some(deletion_time),
            "the row deletion must be preserved to keep shadowing across SSTables"
        );
    }

    /// Issue #932: `merge_entry_to_mutation` must emit the coexisting row deletion
    /// as `Mutation::row_tombstone` (decoupled from the row's liveness
    /// `timestamp_micros`) so the writer re-emits a `HAS_DELETION` row holding both
    /// the deletion and the surviving cells.
    #[test]
    fn merge_entry_to_mutation_emits_coexisting_row_tombstone() {
        let schema = unclustered_schema();
        let deletion_time = 100i64;
        let source_ldt = 1_700_000_000i32;
        let entry = MergeEntry::new(
            0,
            DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
            None,
            300,
            RowData::Live {
                cells: vec![CellData::new(
                    "name".to_string(),
                    Value::text("new".to_string()),
                    300,
                )],
            },
        )
        .with_row_deletion(deletion_time, source_ldt);

        let mutation =
            KWayMerger::merge_entry_to_mutation(entry, &schema).expect("conversion should succeed");

        assert_eq!(
            mutation.row_tombstone,
            Some((deletion_time, source_ldt)),
            "the coexisting row deletion must be emitted as Mutation::row_tombstone"
        );
        assert_eq!(
            mutation.timestamp_micros, 300,
            "the mutation's liveness timestamp stays the row write time, NOT the deletion time"
        );
        assert!(
            mutation
                .operations
                .iter()
                .any(|op| matches!(op, crate::storage::write_engine::mutation::CellOperation::Write { column, .. } if column == "name")),
            "the surviving cell must still be emitted alongside the row tombstone"
        );
    }

    /// An in-memory run yielding a fixed list of `MergeEntry`s in order, so the
    /// readback test can drive the FULL production merge→writer path
    /// (`merge_entry_to_mutation` + `write_partition`) rather than a hand-rolled
    /// shortcut.
    struct VecIterator(std::vec::IntoIter<MergeEntry>);
    impl SSTableRowIterator for VecIterator {
        fn next(&mut self) -> Option<Result<MergeEntry>> {
            self.0.next().map(Ok)
        }
    }

    /// Readback regression: a row tombstone whose `deletion_time` (micros) and
    /// `local_deletion_time` (GC-clock seconds) intentionally DIFFER must survive
    /// a full compaction rewrite (merge → SSTableWriter → on-disk Data.db →
    /// reader) with BOTH values intact. The reader re-surfaces the row-tombstone
    /// `deletion_time` (markedForDeleteAt, micros) on the compaction read path;
    /// the LDT is the on-disk `localDeletionTime` the writer must have encoded
    /// without underflowing the unsigned delta.
    #[tokio::test]
    async fn row_tombstone_ldt_survives_compaction_rewrite() {
        use crate::platform::Platform;
        use crate::storage::sstable::reader::compaction_row::CompactionRowData;
        use crate::Config;
        use std::sync::Arc;

        let schema = unclustered_schema();
        let deletion_time = 1_000_000_000_000_000i64; // micros
        let source_ldt = 1_700_000_000i32; // seconds, deliberately != deletion_time/1e6

        let merger = KWayMerger {
            runs: vec![RunReader::new(Box::new(VecIterator(
                vec![tombstone_entry(0, deletion_time, source_ldt)].into_iter(),
            )))],
            heap: std::collections::BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema: schema.clone(),
            schema_arc: std::sync::Arc::new(schema.clone()),
            _egress_slot: None,
        };

        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
            temp_dir.path().to_path_buf(),
            1,
            &schema,
        )
        .expect("create writer");
        // Issue #1668 stage 5c-iv part 2: `KWayMerger::merge` now streams
        // partitions through `begin_partition_incremental`, which requires
        // pre-seeded encoding baselines (it cannot buffer a whole partition
        // to discover the minimum LDT/timestamp before emitting bytes,
        // unlike `write_partition`) — exactly what the one real production
        // caller (`compact_sstables_with_registry`) always does. Seed with
        // this test's own known minimums, matching that real usage.
        writer.pre_seed_encoding_baselines(deletion_time, source_ldt, i32::MAX);

        // Drives merge_partition_rows → merge_entry_to_mutation → write_partition
        // (the production rewrite path). Must not underflow the LDT delta.
        merger
            .merge(&mut writer)
            .expect("merge+write must succeed (no LDT underflow)");
        let info = writer.finish().await.expect("finish must succeed");

        // Read the row tombstone back from the on-disk SSTable and assert the LDT
        // round-tripped (the writer encoded the preserved value, not 0 / derived).
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let reader = crate::storage::sstable::reader::SSTableReader::open(
            &info.data_path,
            &config,
            platform,
        )
        .await
        .expect("open written SSTable");
        let rows = reader
            .iterate_all_partitions_for_compaction(Some(&schema))
            .await
            .expect("compaction iterator");
        let mut saw_tombstone = false;
        for row in rows {
            if let CompactionRowData::Tombstone {
                deletion_time: dt,
                local_deletion_time,
                ..
            } = row.row_data
            {
                saw_tombstone = true;
                assert_eq!(dt, deletion_time, "deletion_time must round-trip");
                assert_eq!(
                    local_deletion_time, source_ldt,
                    "local_deletion_time must round-trip the preserved source value"
                );
            }
        }
        assert!(
            saw_tombstone,
            "the rewritten SSTable must contain the row tombstone"
        );
    }
}

// ── Issue #845 (Epic #921): gc_grace / gcBefore tombstone purging ────────────
//
// Parity Cassandra `8d47ebb2` (`cursor-compaction-completion`): a tombstone
// whose `localDeletionTime < gcBefore` is PURGEABLE and dropped from the
// compaction output; one within grace (`localDeletionTime >= gcBefore`) is
// RETAINED. The purge runs as a SEPARATE stage AFTER shadow-before-purge
// (#887) and the row-tombstone / dropped-column filters, so covered cells
// are already removed before any marker is purged (no resurrection). When
// `gc_before_secs` is `None`, the stage is a strict no-op.
#[cfg(all(test, feature = "write-support"))]
mod issue_845_gc_grace_purge {
    use super::*;
    use crate::schema::{KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::{ClusteringBound, DecoratedKey};
    use crate::types::{TombstoneInfo, TombstoneType};
    use std::collections::HashMap;

    fn dk(byte: u8) -> DecoratedKey {
        DecoratedKey::from_key_bytes(vec![byte]).expect("token")
    }

    fn live(run_index: usize, row_ts: i64, cells: Vec<CellData>) -> MergeEntry {
        MergeEntry::new(run_index, dk(1), None, row_ts, RowData::Live { cells })
    }

    fn unclustered_schema() -> TableSchema {
        TableSchema {
            keyspace: "ks845".to_string(),
            table: "t845".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn tombstone_entry(run_index: usize, deletion_time: i64, ldt: i32) -> MergeEntry {
        MergeEntry::new(
            run_index,
            DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
            None,
            deletion_time,
            RowData::Tombstone {
                deletion_time,
                local_deletion_time: ldt,
            },
        )
    }

    /// A single-cell tombstone `CellData` for column `v` carrying its LDT.
    fn cell_tombstone(ts: i64, ldt: i32) -> CellData {
        CellData {
            column: "v".to_string(),
            value: Value::Tombstone(Box::new(TombstoneInfo {
                deletion_time: ts,
                tombstone_type: TombstoneType::CellTombstone,
                local_deletion_time: ldt as i64,
                ttl: None,
                range_start: None,
                range_end: None,
            })),
            timestamp: ts,
            ttl: None,
            cell_path: None,
            local_deletion_time: Some(ldt),
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        }
    }

    /// A complex-deletion marker whose LDT is older than gcBefore is purged; an
    /// equivalent marker within grace is retained. Boundary: `== gcBefore` is
    /// RETAINED (only strictly-less purges).
    #[test]
    fn issue_845_complex_deletion_purged_when_older_than_gc_before() {
        const GC_BEFORE: i64 = 1_700_000_000;
        // Marker covers NOTHING (no live elements), so purging cannot resurrect.
        let make = |ldt: i32| {
            MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
                .with_complex_deletions(vec![ComplexDeletion {
                    column: "tags".to_string(),
                    marked_for_delete_at: 50,
                    local_deletion_time: ldt,
                }])
        };

        // Older than gcBefore → purgeable → the marker is dropped, and with no
        // surviving data + no row tombstone the whole entry vanishes.
        let purged = KWayMerger::reconcile_cluster(
            None,
            vec![make((GC_BEFORE - 1) as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        );
        assert!(
            purged.is_none(),
            "a complex-deletion marker older than gcBefore must be purged, \
             leaving nothing to emit"
        );

        // Within grace (strictly newer) → retained.
        let retained = KWayMerger::reconcile_cluster(
            None,
            vec![make((GC_BEFORE + 1) as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        )
        .expect("a within-grace marker must keep a metadata-only entry");
        assert_eq!(
            retained.complex_deletions.len(),
            1,
            "a complex-deletion marker within grace must be retained"
        );

        // Boundary: LDT == gcBefore is RETAINED (only `< gcBefore` purges).
        let boundary = KWayMerger::reconcile_cluster(
            None,
            vec![make(GC_BEFORE as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        )
        .expect("a marker at exactly gcBefore must be retained");
        assert_eq!(
            boundary.complex_deletions.len(),
            1,
            "localDeletionTime == gcBefore is within grace (only `<` purges)"
        );
    }

    /// A row tombstone whose LDT is older than gcBefore is purged; an equivalent
    /// one within grace is retained. Boundary `== gcBefore` is RETAINED.
    #[test]
    fn issue_845_row_tombstone_purged_when_older_than_gc_before() {
        const GC_BEFORE: i64 = 1_700_000_000;
        // deletion_time (micros) chosen so it does not equal LDT (seconds).
        let dt = 1_000_000_000_000_000i64;

        let older = KWayMerger::reconcile_cluster(
            None,
            vec![tombstone_entry(0, dt, (GC_BEFORE - 1) as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        );
        assert!(
            older.is_none(),
            "a row tombstone older than gcBefore must be purged (nothing emitted)"
        );

        let within = KWayMerger::reconcile_cluster(
            None,
            vec![tombstone_entry(0, dt, (GC_BEFORE + 1) as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        )
        .expect("a within-grace row tombstone must be retained");
        assert!(
            matches!(within.row_data, RowData::Tombstone { .. }),
            "a within-grace row tombstone must survive"
        );

        let boundary = KWayMerger::reconcile_cluster(
            None,
            vec![tombstone_entry(0, dt, GC_BEFORE as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        )
        .expect("a row tombstone at exactly gcBefore must be retained");
        assert!(
            matches!(boundary.row_data, RowData::Tombstone { .. }),
            "localDeletionTime == gcBefore is within grace (only `<` purges)"
        );
    }

    /// A simple cell tombstone whose LDT is older than gcBefore is purged; one
    /// within grace is retained. Boundary `== gcBefore` is RETAINED.
    #[test]
    fn issue_845_cell_tombstone_purged_when_older_than_gc_before() {
        const GC_BEFORE: i64 = 1_700_000_000;
        // A live cell on a SEPARATE column keeps the row alive so we observe the
        // tombstone cell being dropped (not the row collapsing). Its ts is well
        // above any tombstone ts so row-tombstone shadowing is irrelevant here.
        let keep = CellData::new("name".to_string(), Value::text("alive".to_string()), 500);

        let count_tombstone_cells = |ldt: i32| -> usize {
            let merged = KWayMerger::reconcile_cluster(
                None,
                vec![live(0, 500, vec![keep.clone(), cell_tombstone(100, ldt)])],
                &::std::collections::HashMap::new(),
                Some(GC_BEFORE),
            )
            .expect("a live row must be emitted");
            match merged.row_data {
                RowData::Live { cells } => cells
                    .iter()
                    .filter(|c| KWayMerger::is_cell_tombstone(c))
                    .count(),
                other => panic!("expected Live, got {other:?}"),
            }
        };

        assert_eq!(
            count_tombstone_cells((GC_BEFORE - 1) as i32),
            0,
            "a cell tombstone older than gcBefore must be purged from the row"
        );
        assert_eq!(
            count_tombstone_cells((GC_BEFORE + 1) as i32),
            1,
            "a cell tombstone within grace must be retained"
        );
        assert_eq!(
            count_tombstone_cells(GC_BEFORE as i32),
            1,
            "localDeletionTime == gcBefore is within grace (only `<` purges)"
        );
    }

    /// When `gc_before_secs` is `None` the purge stage is a strict no-op: an
    /// ancient tombstone (LDT far in the past) is RETAINED, preserving the
    /// pre-#845 behavior.
    #[test]
    fn issue_845_no_gc_before_retains_everything() {
        let dt = 1_000_000_000_000_000i64;
        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![tombstone_entry(0, dt, 1)],
            &::std::collections::HashMap::new(),
            None,
        )
        .expect("without gcBefore the tombstone must be retained");
        assert!(
            matches!(merged.row_data, RowData::Tombstone { .. }),
            "with gc_before_secs = None nothing is purged"
        );
    }

    /// CRITICAL no-resurrection guard: a complex-deletion marker that SHADOWS a
    /// covered element (ts <= mfda) must remove that element in Step 2b BEFORE
    /// the marker is purged in the gc stage. After purging the marker, the
    /// covered element must NOT reappear.
    #[test]
    fn issue_845_purge_does_not_resurrect_shadowed_element() {
        const GC_BEFORE: i64 = 1_700_000_000;
        const MFDA: i64 = 200;
        // A complex ELEMENT (carries a cell_path) of column `tags` written at
        // ts == MFDA, so it is shadowed (`ts <= mfda`) by the marker.
        let covered = CellData {
            column: "tags".to_string(),
            value: Value::text("ghost".to_string()),
            timestamp: MFDA,
            ttl: None,
            cell_path: Some(vec![1, 2, 3]),
            local_deletion_time: None,
            is_complex_element: true,
            is_deleted: false,
            has_empty_value: false,
        };
        let entry = MergeEntry::new(
            0,
            dk(1),
            None,
            MFDA,
            RowData::Live {
                cells: vec![covered],
            },
        )
        .with_complex_deletions(vec![ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: MFDA,
            // Marker is older than gcBefore → purgeable.
            local_deletion_time: (GC_BEFORE - 1) as i32,
        }]);

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![entry],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        );
        // The covered element was shadowed (removed) in Step 2b, then the marker
        // was purged: nothing survives, so no entry is emitted — and crucially
        // the shadowed element is NOT resurrected as a live cell.
        assert!(
            merged.is_none(),
            "purging the marker must not resurrect the element it shadowed"
        );
    }

    /// `compute_gc_before` derives `now - gc_grace_seconds` from the schema's
    /// `gc_grace_seconds` comment. When ABSENT it falls back to Cassandra's
    /// table DEFAULT of 864000s (#921 finding 3); INVALID values return `None`.
    #[test]
    fn issue_845_compute_gc_before_from_schema() {
        let mut schema = unclustered_schema();
        let now = 2_000_000_000i64;

        // gc_grace_seconds = 0 → gcBefore == now (immediate purge eligibility).
        schema
            .comments
            .insert("gc_grace_seconds".to_string(), "0".to_string());
        assert_eq!(compute_gc_before(&schema, now), Some(now));

        // gc_grace_seconds = 864000 (10 days) → gcBefore == now - 864000.
        schema
            .comments
            .insert("gc_grace_seconds".to_string(), "864000".to_string());
        assert_eq!(compute_gc_before(&schema, now), Some(now - 864_000));

        // Unparseable → None (never purge on bad metadata).
        schema
            .comments
            .insert("gc_grace_seconds".to_string(), "not-a-number".to_string());
        assert_eq!(compute_gc_before(&schema, now), None);
    }

    /// #921 finding 3: a table that OMITS `gc_grace_seconds` must use
    /// Cassandra's DEFAULT of 864000s (10 days), not disable purging. A NEGATIVE
    /// or unparseable value is rejected conservatively (`None`, no purge); 0 is
    /// valid (immediate grace).
    #[test]
    fn issue_921_compute_gc_before_defaults_to_864000_when_absent() {
        let mut schema = unclustered_schema();
        let now = 2_000_000_000i64;

        // ABSENT → Cassandra default 864000s (NOT None / disabled).
        assert_eq!(
            compute_gc_before(&schema, now),
            Some(now - 864_000),
            "missing gc_grace_seconds must use the Cassandra default of 864000s"
        );

        // 0 is a VALID value (immediate grace) per Cassandra → gcBefore == now.
        schema
            .comments
            .insert("gc_grace_seconds".to_string(), "0".to_string());
        assert_eq!(
            compute_gc_before(&schema, now),
            Some(now),
            "gc_grace_seconds = 0 is valid (immediate grace)"
        );

        // NEGATIVE → None (never purge on out-of-range metadata).
        schema
            .comments
            .insert("gc_grace_seconds".to_string(), "-1".to_string());
        assert_eq!(
            compute_gc_before(&schema, now),
            None,
            "a negative gc_grace_seconds must disable purging (no-op)"
        );

        // Unparseable → None.
        schema
            .comments
            .insert("gc_grace_seconds".to_string(), "garbage".to_string());
        assert_eq!(
            compute_gc_before(&schema, now),
            None,
            "an unparseable gc_grace_seconds must disable purging (no-op)"
        );
    }

    /// #921 finding 2: on-disk `localDeletionTime` is an UNSIGNED 32-bit count.
    /// A far-future LDT with bit 31 set (e.g. `0x8000_0000` ≈ year 2038) is
    /// carried as a NEGATIVE `i32`. The purge compare must normalize it as
    /// unsigned (`i64::from(ldt as u32)`) so it reads as a LARGE future second
    /// and is NOT purged by a normal `gcBefore`. The pre-fix `as i64` path made
    /// it look ancient and purged the tombstone immediately (resurrection bug).
    #[test]
    fn issue_921_unsigned_local_deletion_time_not_purged() {
        // bit 31 set: as i32 this is negative (-2147483648); as u32 it is
        // 2_147_483_648 seconds ≈ 2038-01-19 — far in the future.
        let future_ldt_bits = 0x8000_0000u32 as i32;
        assert!(future_ldt_bits < 0, "the wrapped LDT is a negative i32");
        // A normal gcBefore well below the unsigned value: nothing should purge.
        const GC_BEFORE: i64 = 1_700_000_000;
        assert!(
            i64::from(future_ldt_bits as u32) > GC_BEFORE,
            "the unsigned LDT is in the future relative to gcBefore"
        );

        // (a) Row tombstone with the far-future LDT must be RETAINED.
        let dt = 1_000_000_000_000_000i64;
        let row = KWayMerger::reconcile_cluster(
            None,
            vec![tombstone_entry(0, dt, future_ldt_bits)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        )
        .expect("a far-future row tombstone must NOT be purged");
        assert!(
            matches!(row.row_data, RowData::Tombstone { .. }),
            "an unsigned far-future row tombstone must survive a normal gcBefore"
        );

        // (b) Complex-deletion marker with the far-future LDT must be RETAINED.
        let complex = KWayMerger::reconcile_cluster(
            None,
            vec![
                MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
                    .with_complex_deletions(vec![ComplexDeletion {
                        column: "tags".to_string(),
                        marked_for_delete_at: 50,
                        local_deletion_time: future_ldt_bits,
                    }]),
            ],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        )
        .expect("a far-future complex-deletion marker must NOT be purged");
        assert_eq!(
            complex.complex_deletions.len(),
            1,
            "an unsigned far-future complex marker must survive a normal gcBefore"
        );

        // (c) Cell tombstone with the far-future LDT must be RETAINED.
        let keep = CellData::new("name".to_string(), Value::text("alive".to_string()), 500);
        let cell = KWayMerger::reconcile_cluster(
            None,
            vec![live(
                0,
                500,
                vec![keep, cell_tombstone(100, future_ldt_bits)],
            )],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        )
        .expect("a live row must be emitted");
        let tombstone_cells = match cell.row_data {
            RowData::Live { cells } => cells
                .iter()
                .filter(|c| KWayMerger::is_cell_tombstone(c))
                .count(),
            other => panic!("expected Live, got {other:?}"),
        };
        assert_eq!(
            tombstone_cells, 1,
            "an unsigned far-future cell tombstone must survive a normal gcBefore"
        );

        // Control: a genuinely ancient LDT (bit 31 clear, < gcBefore) IS purged,
        // proving the normalization did not disable purging wholesale.
        let ancient = KWayMerger::reconcile_cluster(
            None,
            vec![tombstone_entry(0, dt, (GC_BEFORE - 1) as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        );
        assert!(
            ancient.is_none(),
            "a genuinely ancient row tombstone must still be purged"
        );
    }

    /// #921 finding 1 (SAFETY-CRITICAL): a PARTIAL compaction (not overlap-safe)
    /// must NOT purge tombstones — purging one could resurrect data shadowed in a
    /// non-included overlapping SSTable. A MAJOR/full compaction (overlap-safe)
    /// purges. Drives the gate through `merge_partition_rows`, which collapses
    /// the gc_grace cutoff to `None` unless `purge_safe` is set.
    #[test]
    fn issue_921_partial_compaction_does_not_purge_but_major_does() {
        const GC_BEFORE: i64 = 1_700_000_000;
        let dt = 1_000_000_000_000_000i64;
        // A row tombstone old enough to be purgeable (LDT < gcBefore).
        let ancient_ldt = (GC_BEFORE - 1) as i32;

        // Build a merger with NO runs (merge_partition_rows ignores `runs`),
        // gc_before set, and toggle only `purge_safe`.
        let merger = |purge_safe: bool| KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            schema: unclustered_schema(),
            schema_arc: std::sync::Arc::new(unclustered_schema()),
            gc_before_secs: Some(GC_BEFORE),
            now_secs: None,
            purge_safe,
            max_purgeable_timestamp: None,
            _egress_slot: None,
        };

        // PARTIAL compaction (purge_safe = false): the purgeable row tombstone is
        // RETAINED — the partial path must never drop a tombstone.
        let partial = merger(false)
            .merge_partition_rows(vec![tombstone_entry(0, dt, ancient_ldt)])
            .expect("merge must succeed");
        assert_eq!(
            partial.len(),
            1,
            "a partial (non-overlap-safe) compaction must RETAIN the tombstone"
        );
        assert!(
            matches!(partial[0].row_data, RowData::Tombstone { .. }),
            "the retained entry must still be a row tombstone (no resurrection risk)"
        );

        // MAJOR compaction (purge_safe = true): the same purgeable tombstone IS
        // dropped — overlap-safe, so purging cannot resurrect anything.
        let major = merger(true)
            .merge_partition_rows(vec![tombstone_entry(0, dt, ancient_ldt)])
            .expect("merge must succeed");
        assert!(
            major.is_empty(),
            "a major (overlap-safe) compaction must PURGE the ancient tombstone"
        );
    }

    /// #1061 (SAFETY-CRITICAL): the surviving-range-tombstone re-emit loop in
    /// `merge_partition_rows` must gate a marker purge on BOTH `gc_before` AND
    /// the overlap bound (`rt.deletion_time < max_purgeable_timestamp`), exactly
    /// like the cell/row/complex purge paths. In a PARTIAL compaction with a
    /// finite overlap bound, a range tombstone at/above that bound may still
    /// shadow data in a non-included SSTable, so dropping it would resurrect
    /// that data. This drives the gate through the real `merge_partition_rows`.
    #[test]
    fn issue_1061_range_tombstone_purge_respects_overlap_bound() {
        const GC_BEFORE: i64 = 1_700_000_000;
        // Old enough for the gc_grace condition to be satisfied on its own.
        let ancient_ldt = (GC_BEFORE - 1) as i32;
        // Finite overlap bound (min write timestamp of a non-included SSTable).
        const BOUND: i64 = 2_000;

        // Build a merger exactly like #921's: no runs, gc_before set, toggle
        // `purge_safe` and `max_purgeable_timestamp`.
        let merger = |purge_safe: bool, bound: Option<i64>| KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            schema: unclustered_schema(),
            schema_arc: std::sync::Arc::new(unclustered_schema()),
            gc_before_secs: Some(GC_BEFORE),
            now_secs: None,
            purge_safe,
            max_purgeable_timestamp: bound,
            _egress_slot: None,
        };

        // A whole-partition range-tombstone CARRIER entry (issue #933): empty
        // live row whose only payload is the range deletion.
        let carrier = |deletion_time: i64| {
            let rt = RangeTombstone {
                start: ClusteringBound::Bottom,
                end: ClusteringBound::Top,
                deletion_time,
                local_deletion_time: ancient_ldt,
            };
            MergeEntry::new(
                0,
                dk(1),
                None,
                deletion_time,
                RowData::Live { cells: vec![] },
            )
            .with_range_deletion(rt)
        };

        // (1) PARTIAL compaction, marker AT the bound (`deletion_time == BOUND`):
        // RETAINED. `BOUND < BOUND` is false, so the overlap guard blocks the
        // purge even though the gc_grace condition (`ancient_ldt < gcBefore`) holds.
        let at_bound = merger(false, Some(BOUND))
            .merge_partition_rows(vec![carrier(BOUND)])
            .expect("merge must succeed");
        assert_eq!(
            at_bound.len(),
            1,
            "a range tombstone AT the overlap bound must be RETAINED (#1061)"
        );
        assert!(
            at_bound[0].range_deletion.is_some(),
            "the retained entry must still carry the range-tombstone marker"
        );

        // (2) PARTIAL compaction, marker ABOVE the bound: also RETAINED.
        let above_bound = merger(false, Some(BOUND))
            .merge_partition_rows(vec![carrier(BOUND + 1_000)])
            .expect("merge must succeed");
        assert_eq!(
            above_bound.len(),
            1,
            "a range tombstone ABOVE the overlap bound must be RETAINED (#1061)"
        );

        // (3) Control: PARTIAL compaction, marker strictly BELOW the bound AND
        // gc_grace-expired: PURGED — proving the overlap guard does not disable
        // purging wholesale.
        let below_bound = merger(false, Some(BOUND))
            .merge_partition_rows(vec![carrier(BOUND - 1)])
            .expect("merge must succeed");
        assert!(
            below_bound.is_empty(),
            "a gc-expired range tombstone strictly BELOW the overlap bound is purgeable"
        );

        // (4) Control: a MAJOR/overlap-safe compaction (`purge_safe = true`,
        // bound collapses to i64::MAX) purges the same at-bound marker — the fix
        // does not weaken the overlap-safe purge path.
        let overlap_safe = merger(true, None)
            .merge_partition_rows(vec![carrier(BOUND)])
            .expect("merge must succeed");
        assert!(
            overlap_safe.is_empty(),
            "an overlap-safe (major) compaction still purges the gc-expired marker"
        );
    }

    /// #921 finding 2: a row tombstone with `local_deletion_time == 0` is the
    /// "LDT not surfaced" placeholder used by legacy/pre-V5 paths. It must be
    /// treated as UNKNOWN and RETAINED under purge-safe compaction (never purge
    /// on unknown LDT). Only a real, non-zero LDT strictly below `gcBefore`
    /// purges; a within-grace LDT is retained.
    #[test]
    fn issue_921_row_tombstone_ldt_zero_is_unknown_and_retained() {
        const GC_BEFORE: i64 = 1_700_000_000;
        let dt = 1_000_000_000_000_000i64;

        // LDT == 0 (unknown placeholder): RETAINED even though `0 < gcBefore`.
        let unknown = KWayMerger::reconcile_cluster(
            None,
            vec![tombstone_entry(0, dt, 0)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        )
        .expect("a row tombstone with unknown (0) LDT must be RETAINED, not purged");
        assert!(
            matches!(unknown.row_data, RowData::Tombstone { .. }),
            "LDT==0 is the unknown placeholder and must be retained (never purge \
             on unknown LDT)"
        );

        // A REAL, non-zero ancient LDT (< gcBefore) still purges.
        let ancient = KWayMerger::reconcile_cluster(
            None,
            vec![tombstone_entry(0, dt, (GC_BEFORE - 1) as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        );
        assert!(
            ancient.is_none(),
            "a real non-zero ancient row tombstone (LDT < gcBefore) must still purge"
        );

        // Within-grace LDT (>= gcBefore) is retained.
        let within = KWayMerger::reconcile_cluster(
            None,
            vec![tombstone_entry(0, dt, (GC_BEFORE + 1) as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        )
        .expect("a within-grace row tombstone must be retained");
        assert!(
            matches!(within.row_data, RowData::Tombstone { .. }),
            "a within-grace row tombstone (LDT >= gcBefore) must survive"
        );
    }

    /// #921 finding 3: a CLUSTERED row whose only non-key data is a PURGEABLE
    /// cell tombstone must emit NOTHING after the gc purge — no phantom key-only
    /// live row. The `purged_to_empty` determination must run AFTER the cell
    /// tombstone purge. A clustered row with REAL surviving (non-key) data still
    /// emits a live row.
    #[test]
    fn issue_921_clustered_row_with_only_purgeable_cell_tombstone_emits_nothing() {
        const GC_BEFORE: i64 = 1_700_000_000;
        // Clustering key column `ck` (a pseudo-cell that stays in the cell list).
        let ck = ClusteringKey {
            columns: vec![("ck".to_string(), Value::text("c1".to_string()))],
        };
        // The clustering-key pseudo-cell carried alongside the data cells (mirrors
        // `extract_clustering_key` keeping CK columns in the cell list for
        // read-back). It is NOT a data cell (its column name is in `ck_names`).
        let ck_cell = CellData::new("ck".to_string(), Value::text("c1".to_string()), 100);

        let make = |extra: Vec<CellData>| {
            let mut cells = vec![ck_cell.clone()];
            cells.extend(extra);
            MergeEntry::new(0, dk(1), Some(ck.clone()), 100, RowData::Live { cells })
        };

        // Only non-key data is a purgeable (ancient LDT) cell tombstone on `v`.
        // After the gc purge the row has only the CK pseudo-cell left → it must
        // be recognized as purged-to-empty and emit NOTHING (no phantom live row).
        let purged = KWayMerger::reconcile_cluster(
            Some(ck.clone()),
            vec![make(vec![cell_tombstone(50, (GC_BEFORE - 1) as i32)])],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        );
        assert!(
            purged.is_none(),
            "a clustered row whose only non-key data is a purgeable cell tombstone \
             must emit NOTHING (no phantom key-only live row) after the gc purge"
        );

        // Control: a clustered row with REAL surviving non-key data still emits a
        // live row (the purge of the tombstone does not collapse a real row).
        let kept = KWayMerger::reconcile_cluster(
            Some(ck.clone()),
            vec![make(vec![
                CellData::new("v".to_string(), Value::text("real".to_string()), 60),
                cell_tombstone(50, (GC_BEFORE - 1) as i32),
            ])],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        )
        .expect("a clustered row with real surviving data must emit a live row");
        match kept.row_data {
            RowData::Live { cells } => {
                assert!(
                    cells
                        .iter()
                        .any(|c| c.column == "v" && !KWayMerger::is_cell_tombstone(c)),
                    "the real surviving `v` cell must remain in the emitted live row"
                );
                assert!(
                    !cells.iter().any(KWayMerger::is_cell_tombstone),
                    "the purgeable cell tombstone must still be purged from the live row"
                );
            }
            other => panic!("expected Live row, got {other:?}"),
        }
    }

    // ── Issue #935: overlap-aware partial-compaction purging ─────────────────
    //
    // Parity Cassandra `CompactionController.maxPurgeableTimestamp` /
    // `getPurgeEvaluator` (`time -> time < minTimestamp`): in a PARTIAL
    // compaction a tombstone is purgeable only when BOTH its gc grace has
    // elapsed (`localDeletionTime < gcBefore`) AND its own deletion timestamp
    // (`markedForDeleteAt`) is STRICTLY LESS THAN the minimum write timestamp of
    // every non-included overlapping SSTable — so it provably shadows nothing
    // outside the compaction set. `reconcile_cluster_with_overlap` takes that
    // bound as `max_purgeable_timestamp`; `i64::MAX` is the full-compaction fast
    // path (unrestricted, identical to #845).

    /// A row tombstone that PROVABLY predates all non-included overlapping data
    /// (`markedForDeleteAt < bound`) and is past grace is PURGED in a partial
    /// compaction.
    #[test]
    fn issue_935_partial_compaction_purges_row_tombstone_below_overlap_bound() {
        const GC_BEFORE: i64 = 1_700_000_000;
        // Outside SSTables' min write timestamp. The tombstone's markedForDeleteAt
        // is strictly below it, so it shadows nothing outside the set.
        const BOUND: i64 = 1_000_000_000_000_000;
        let mfda = BOUND - 1; // strictly older than every outside cell

        let purged = KWayMerger::reconcile_cluster_with_overlap(
            None,
            vec![tombstone_entry(0, mfda, (GC_BEFORE - 1) as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
            BOUND,
        );
        assert!(
            purged.is_none(),
            "a row tombstone older than every non-included overlapping SSTable \
             (markedForDeleteAt < bound) and past grace must be purged even in a \
             partial compaction (#935)"
        );
    }

    /// A row tombstone whose `markedForDeleteAt >= bound` COULD shadow data in a
    /// non-included overlapping SSTable, so it is RETAINED even though its gc
    /// grace has elapsed. Boundary `== bound` is retained (only strictly-less
    /// purges).
    #[test]
    fn issue_935_partial_compaction_retains_row_tombstone_at_or_above_overlap_bound() {
        const GC_BEFORE: i64 = 1_700_000_000;
        const BOUND: i64 = 1_000_000_000_000_000;

        // markedForDeleteAt strictly ABOVE the bound: outside data could be older
        // than the tombstone and thus shadowed → must retain.
        let above = KWayMerger::reconcile_cluster_with_overlap(
            None,
            vec![tombstone_entry(0, BOUND + 1, (GC_BEFORE - 1) as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
            BOUND,
        )
        .expect("a tombstone that could shadow outside data must be retained");
        assert!(
            matches!(above.row_data, RowData::Tombstone { .. }),
            "a row tombstone with markedForDeleteAt > overlap bound must survive a \
             partial compaction (#935): outside data may be shadowed"
        );

        // Boundary: markedForDeleteAt EXACTLY at the bound is retained — an outside
        // SSTable could hold a cell at exactly the bound that the tombstone would
        // shadow (`time < minTimestamp` is the purge predicate, so `==` retains).
        let boundary = KWayMerger::reconcile_cluster_with_overlap(
            None,
            vec![tombstone_entry(0, BOUND, (GC_BEFORE - 1) as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
            BOUND,
        )
        .expect("a tombstone at exactly the overlap bound must be retained");
        assert!(
            matches!(boundary.row_data, RowData::Tombstone { .. }),
            "markedForDeleteAt == overlap bound is retained (only `<` purges, #935)"
        );
    }

    /// The overlap gate is independent of the gc-grace gate: a tombstone below the
    /// overlap bound but still WITHIN grace is retained (both gates must allow the
    /// purge).
    #[test]
    fn issue_935_within_grace_tombstone_retained_despite_overlap_bound() {
        const GC_BEFORE: i64 = 1_700_000_000;
        const BOUND: i64 = 1_000_000_000_000_000;

        let within = KWayMerger::reconcile_cluster_with_overlap(
            None,
            vec![tombstone_entry(0, BOUND - 1, (GC_BEFORE + 1) as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
            BOUND,
        )
        .expect("a within-grace tombstone must be retained regardless of overlap");
        assert!(
            matches!(within.row_data, RowData::Tombstone { .. }),
            "a tombstone within gc grace must be retained even when its \
             markedForDeleteAt is below the overlap bound (#935)"
        );
    }

    /// A purgeable simple cell tombstone is dropped in a partial compaction only
    /// when its write timestamp is below the overlap bound; at/above the bound it
    /// is retained. The control row carries a live cell on another column so the
    /// row survives and we observe the per-cell decision.
    #[test]
    fn issue_935_partial_compaction_cell_tombstone_overlap_gate() {
        const GC_BEFORE: i64 = 1_700_000_000;
        const BOUND: i64 = 5_000;

        // Below the bound (and past grace) → purged from the surviving cells.
        let purged = KWayMerger::reconcile_cluster_with_overlap(
            None,
            vec![live(
                0,
                100,
                vec![
                    CellData::new("keep".to_string(), Value::text("x".to_string()), 9_000),
                    cell_tombstone(BOUND - 1, (GC_BEFORE - 1) as i32),
                ],
            )],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
            BOUND,
        )
        .expect("the live `keep` cell keeps the row alive");
        match purged.row_data {
            RowData::Live { cells } => assert!(
                !cells.iter().any(KWayMerger::is_cell_tombstone),
                "a cell tombstone below the overlap bound must be purged (#935)"
            ),
            other => panic!("expected Live row, got {other:?}"),
        }

        // At/above the bound → retained (could shadow an outside cell).
        let retained = KWayMerger::reconcile_cluster_with_overlap(
            None,
            vec![live(
                0,
                100,
                vec![
                    CellData::new("keep".to_string(), Value::text("x".to_string()), 9_000),
                    cell_tombstone(BOUND, (GC_BEFORE - 1) as i32),
                ],
            )],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
            BOUND,
        )
        .expect("the live `keep` cell keeps the row alive");
        match retained.row_data {
            RowData::Live { cells } => assert!(
                cells.iter().any(KWayMerger::is_cell_tombstone),
                "a cell tombstone at the overlap bound must be retained (#935)"
            ),
            other => panic!("expected Live row, got {other:?}"),
        }
    }

    /// A complex-deletion marker is purged in a partial compaction only when its
    /// `marked_for_delete_at` is below the overlap bound.
    #[test]
    fn issue_935_partial_compaction_complex_deletion_overlap_gate() {
        const GC_BEFORE: i64 = 1_700_000_000;
        const BOUND: i64 = 5_000;
        let make = |mfda: i64| {
            MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
                .with_complex_deletions(vec![ComplexDeletion {
                    column: "tags".to_string(),
                    marked_for_delete_at: mfda,
                    local_deletion_time: (GC_BEFORE - 1) as i32,
                }])
        };

        // Below the bound and past grace → purged (nothing left to emit).
        let purged = KWayMerger::reconcile_cluster_with_overlap(
            None,
            vec![make(BOUND - 1)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
            BOUND,
        );
        assert!(
            purged.is_none(),
            "a complex-deletion marker below the overlap bound must be purged (#935)"
        );

        // At the bound → retained as a metadata-only entry.
        let retained = KWayMerger::reconcile_cluster_with_overlap(
            None,
            vec![make(BOUND)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
            BOUND,
        )
        .expect("a marker at the overlap bound must be retained");
        assert_eq!(
            retained.complex_deletions.len(),
            1,
            "a complex-deletion marker at the overlap bound must be retained (#935)"
        );
    }

    /// The full-compaction fast path (`max_purgeable_timestamp == i64::MAX`)
    /// purges a past-grace tombstone regardless of its timestamp — identical to
    /// the pre-#935 behavior, so the overlap gate is a strict no-op there.
    #[test]
    fn issue_935_full_compaction_bound_is_unrestricted() {
        const GC_BEFORE: i64 = 1_700_000_000;
        // A very large markedForDeleteAt that no realistic outside bound would
        // exceed still purges under the full-compaction +inf bound.
        let purged = KWayMerger::reconcile_cluster_with_overlap(
            None,
            vec![tombstone_entry(0, i64::MAX - 1, (GC_BEFORE - 1) as i32)],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
            i64::MAX,
        );
        assert!(
            purged.is_none(),
            "the full-compaction +inf overlap bound must purge any past-grace \
             tombstone, identical to #845 (#935 no-op for full compactions)"
        );
    }

    /// #921 finding 2: the dropped-column survivor pre-pass
    /// (`compute_surviving_dropped_columns`) must count a RETAINED
    /// `ComplexDeletion` marker for a dropped COMPLEX column as a survivor.
    ///
    /// A complex (collection) tombstone lives in `row.complex_deletions`, NOT in
    /// `cells`. The pre-fix pre-pass counted only live `cells`, so a dropped
    /// complex column whose only survivor is a within-grace / purge-unsafe
    /// complex-deletion marker was stripped from the output schema — and since the
    /// writer only emits complex-element columns present in the schema, the marker
    /// was silently dropped despite the merge deciding to RETAIN it.
    ///
    /// This drives the FULL merge→writer→on-disk path: it writes a `tags`
    /// complex-deletion marker (no live elements) to a real SSTable, then runs the
    /// survivor pre-pass over that file with `tags` dropped. With no gc (`None`),
    /// the marker is RETAINED so `tags` MUST be counted as a survivor; with a gc
    /// cutoff strictly above the marker's LDT (and `purge_safe`), the marker is
    /// PURGED so `tags` MUST NOT be counted. Mirrors the cell-vs-marker asymmetry
    /// the writer sees.
    #[tokio::test]
    async fn issue_921_complex_deletion_marker_counts_as_dropped_column_survivor() {
        use crate::schema::Column;

        // An in-memory run yielding a fixed `MergeEntry` so we drive the FULL
        // production merge→writer path onto a real on-disk SSTable.
        struct VecIterator(std::vec::IntoIter<MergeEntry>);
        impl SSTableRowIterator for VecIterator {
            fn next(&mut self) -> Option<Result<MergeEntry>> {
                self.0.next().map(Ok)
            }
        }

        // Schema with a non-frozen complex column `tags set<text>`. Drop map is
        // applied per-call below.
        fn schema_with_tags(dropped: HashMap<String, i64>) -> TableSchema {
            TableSchema {
                keyspace: "ks921".to_string(),
                table: "t921".to_string(),
                partition_keys: vec![KeyColumn {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    position: 0,
                }],
                clustering_keys: vec![],
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        data_type: "int".to_string(),
                        nullable: false,
                        default: None,
                        is_static: false,
                    },
                    Column {
                        name: "tags".to_string(),
                        data_type: "set<text>".to_string(),
                        nullable: true,
                        default: None,
                        is_static: false,
                    },
                ],
                comments: HashMap::new(),
                dropped_columns: dropped,
            }
        }

        const MARKER_LDT: i32 = 1_700_000_000;
        const MARKER_MFDA: i64 = 1_600_000_000_000_000; // micros

        // Write an SSTable whose single partition carries ONLY a `tags`
        // complex-deletion marker (no surviving live elements, no other cells), so
        // the column's sole survivor is the marker — exactly the case the pre-fix
        // pre-pass missed.
        let write_schema = schema_with_tags(HashMap::new());
        let entry = MergeEntry::new(
            0,
            DecoratedKey::new(7, 7i32.to_be_bytes().to_vec()),
            None,
            0,
            RowData::Live { cells: vec![] },
        )
        .with_complex_deletions(vec![ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: MARKER_MFDA,
            local_deletion_time: MARKER_LDT,
        }]);

        let merger = KWayMerger {
            runs: vec![RunReader::new(Box::new(VecIterator(
                vec![entry].into_iter(),
            )))],
            heap: std::collections::BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema: write_schema.clone(),
            schema_arc: std::sync::Arc::new(write_schema.clone()),
            _egress_slot: None,
        };

        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
            temp_dir.path().to_path_buf(),
            1,
            &write_schema,
        )
        .expect("create writer");
        // Issue #1668 stage 5c-iv part 2: `KWayMerger::merge` now streams
        // partitions through `begin_partition_incremental`, which requires
        // pre-seeded encoding baselines — exactly what the one real
        // production caller (`compact_sstables_with_registry`) always does.
        // Seed with this test's own known marker minimums.
        writer.pre_seed_encoding_baselines(MARKER_MFDA, MARKER_LDT, i32::MAX);
        merger.merge(&mut writer).expect("merge+write must succeed");
        let info = writer.finish().await.expect("finish must succeed");
        let data_path = info.data_path;

        // Drop `tags`; the column stays in `columns` (decode contract) with a drop
        // time well before the marker so a re-add filter does not interfere.
        let mut dropped = HashMap::new();
        dropped.insert("tags".to_string(), 1_i64);
        let drop_schema = schema_with_tags(dropped);

        // (a) No gc → the marker is RETAINED → `tags` MUST be a survivor.
        let retained = compute_surviving_dropped_columns(
            vec![data_path.clone()],
            &drop_schema,
            None,
            None,
            false,
            None,
        )
        .expect("survivor pre-pass (no gc) must succeed");
        assert!(
            retained.contains("tags"),
            "a RETAINED complex-deletion marker for a dropped complex column must \
             count as a survivor so the writer keeps the column to emit it; got {retained:?}"
        );

        // (b) gc strictly above the marker's LDT + purge_safe → the marker is
        // PURGED → `tags` MUST NOT be a survivor (stripped from the output schema).
        let gc_before = i64::from(MARKER_LDT) + 1;
        let purged = compute_surviving_dropped_columns(
            vec![data_path],
            &drop_schema,
            Some(gc_before),
            Some(gc_before),
            true,
            None,
        )
        .expect("survivor pre-pass (purging) must succeed");
        assert!(
            !purged.contains("tags"),
            "a PURGED complex-deletion marker must NOT count as a survivor (the \
             dropped column is stripped from the output schema); got {purged:?}"
        );
    }
}

// ── Issue #929: bare-name UDT normalization survives compaction ──────────────
//
// A flush normalizes a bare-name non-frozen UDT column (e.g. `addr person`) to
// its `UserType(...)` marshal via the registry and writes complex per-field
// cells with a matching SERIALIZATION_HEADER. Compaction must apply the SAME
// normalization to its output write-schema, or it would rewrite the column as a
// single simple cell and degrade the header to `BytesType` (roborev #1007).
#[cfg(all(test, feature = "write-support"))]
mod issue_929_bare_udt_compaction {
    use super::*;
    use crate::schema::{Column, CqlType, KeyColumn, UdtRegistry};
    use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
    use crate::types::{UdtField, UdtTypeDef, UdtValue, Value};

    fn person_registry() -> UdtRegistry {
        let mut reg = UdtRegistry::new();
        reg.register_udt(
            UdtTypeDef::new("test_ks".to_string(), "person".to_string())
                .with_field("name".to_string(), CqlType::Text, true)
                .with_field("age".to_string(), CqlType::Int, true),
        );
        reg
    }

    // Schema whose UDT column is declared with the BARE name `person`.
    fn bare_udt_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "addr".to_string(),
                    data_type: "person".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        }
    }

    const PERSON_USERTYPE: &str = "org.apache.cassandra.db.marshal.UserType(test_ks,706572736f6e,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616765:org.apache.cassandra.db.marshal.Int32Type)";

    fn header_has(stats_path: &std::path::Path, needle: &str) -> bool {
        let bytes = std::fs::read(stats_path).expect("read Statistics.db");
        bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
    }

    #[tokio::test]
    async fn bare_udt_column_survives_compaction_with_registry() {
        let schema = bare_udt_schema();
        let registry = person_registry();

        // 1. Flush an input SSTable WITH the registry: the column is normalized to
        //    UserType(...) and written as complex per-field cells.
        let in_dir = tempfile::TempDir::new().expect("in dir");
        let mut writer =
            crate::storage::sstable::writer::SSTableWriter::with_expected_partitions_and_registry(
                in_dir.path().to_path_buf(),
                1,
                &schema,
                1,
                Some(&registry),
            )
            .expect("input writer");
        let udt = Value::Udt(Box::new(UdtValue {
            type_name: "person".to_string(),
            keyspace: "test_ks".to_string(),
            fields: vec![
                UdtField {
                    name: "name".to_string(),
                    value: Some(Value::text("Alice".to_string())),
                },
                UdtField {
                    name: "age".to_string(),
                    value: Some(Value::Integer(30)),
                },
            ],
        }));
        let mutation = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            None,
            vec![CellOperation::Write {
                column: "addr".to_string(),
                value: udt,
            }],
            1_000_000,
            None,
        );
        let key = mutation.decorated_key(&schema).expect("decorated key");
        writer
            .write_partition(key, vec![mutation])
            .expect("write partition");
        let input = writer.finish().await.expect("finish input");
        assert!(
            header_has(&input.stats_path, PERSON_USERTYPE),
            "input header must advertise the UDT column as UserType"
        );

        // 2. Compact WITH the registry, passing the BARE-name schema (as the engine
        //    does). The output must re-apply normalization, not degrade to BytesType.
        let out_dir = tempfile::TempDir::new().expect("out dir");
        let report = compact_sstables(
            vec![input.data_path.clone()],
            out_dir.path(),
            &schema,
            2,
            None,
            None,
            true,
        )
        .await
        .expect("compaction");

        assert!(
            header_has(&report.output.stats_path, PERSON_USERTYPE),
            "compaction output header must keep the UDT column as UserType (not degrade to BytesType)"
        );

        // 3. Read the compacted output back through the compaction path and assert
        //    the per-field UDT cells SURVIVED as a complex column with the original
        //    field values (not collapsed/dropped). The reader keys complex-vs-simple
        //    off the schema's data_type, so read with the normalized (UserType) form.
        let mut read_schema = schema.clone();
        crate::storage::sstable::writer::data_writer::normalize_schema_udts(
            &mut read_schema,
            &registry,
        );
        let config = crate::Config::default();
        let platform = std::sync::Arc::new(
            crate::platform::Platform::new(&config)
                .await
                .expect("platform"),
        );
        let reader = crate::storage::sstable::reader::SSTableReader::open(
            &report.output.data_path,
            &config,
            platform,
        )
        .await
        .expect("open compacted output");
        let rows = reader
            .iterate_all_partitions_for_compaction(Some(&read_schema))
            .await
            .expect("compaction iterator");

        let mut saw_addr = false;
        for row in &rows {
            if let crate::storage::sstable::reader::compaction_row::CompactionRowData::Live {
                complex,
                ..
            } = &row.row_data
            {
                if let Some(addr) = complex.iter().find(|c| c.column == "addr") {
                    saw_addr = true;
                    // Two per-field cells survived: name (idx 0) and age (idx 1).
                    // The compaction read path surfaces per-element UDT values
                    // byte-faithfully, so `age` may arrive typed (Integer) or as
                    // its raw 4-byte big-endian form — both mean the value
                    // survived rather than being dropped/collapsed.
                    assert_eq!(
                        addr.elements.len(),
                        2,
                        "both UDT field cells must survive, got: {:?}",
                        addr.elements
                    );
                    let values: Vec<Option<Value>> =
                        addr.elements.iter().map(|e| e.value.clone()).collect();
                    let name_ok = values.iter().any(|v| {
                        matches!(v, Some(Value::Text(s)) if s == "Alice")
                            || matches!(v, Some(Value::Blob(b)) if b.as_ref() == b"Alice")
                    });
                    assert!(
                        name_ok,
                        "UDT field `name` value must survive compaction, got: {values:?}"
                    );
                    let age_ok = values.iter().any(|v| {
                        matches!(v, Some(Value::Integer(30)))
                            || matches!(v, Some(Value::Blob(b)) if b.as_ref() == 30i32.to_be_bytes())
                    });
                    assert!(
                        age_ok,
                        "UDT field `age` value must survive compaction, got: {values:?}"
                    );
                }
            }
        }
        assert!(
            saw_addr,
            "compacted output must contain the `addr` UDT as a COMPLEX column (per-field cells \
             survived rather than being dropped/collapsed)"
        );
    }

    /// #1013 safety: an input written WITHOUT a registry stores the bare UDT
    /// column as a single simple `BytesType` cell. Compacting it WITH a registry
    /// must NOT normalize/upgrade that column (the header gate sees it is not a
    /// UserType in the input), so the reader does not misdecode the simple cell.
    /// The output keeps the column simple (header has no UserType for it).
    #[tokio::test]
    async fn simple_cell_input_not_upgraded_on_compaction() {
        let schema = bare_udt_schema();

        // Input written WITHOUT a registry: `addr` is a single simple cell whose
        // header type is NOT a UserType marshal.
        let in_dir = tempfile::TempDir::new().expect("in dir");
        let mut writer = crate::storage::sstable::writer::SSTableWriter::with_expected_partitions(
            in_dir.path().to_path_buf(),
            1,
            &schema,
            1,
        )
        .expect("input writer");
        let udt = Value::Udt(Box::new(UdtValue {
            type_name: "person".to_string(),
            keyspace: "test_ks".to_string(),
            fields: vec![UdtField {
                name: "name".to_string(),
                value: Some(Value::text("Bob".to_string())),
            }],
        }));
        let mutation = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(2)),
            None,
            vec![CellOperation::Write {
                column: "addr".to_string(),
                value: udt,
            }],
            1_000_000,
            None,
        );
        let key = mutation.decorated_key(&schema).expect("decorated key");
        writer
            .write_partition(key, vec![mutation])
            .expect("write partition");
        let input = writer.finish().await.expect("finish input");
        assert!(
            !header_has(&input.stats_path, "UserType("),
            "input written without registry must store the column as a simple (non-UserType) cell"
        );

        // The header gate must report NO complex UDT columns for this input.
        let plan = udt_columns_eligible_for_normalization(std::slice::from_ref(&input.data_path));
        assert!(
            plan.eligible_marshals.is_empty() && plan.conflicts.is_empty(),
            "a simple-cell input must not be reported as eligible or conflicting"
        );

        // Compact WITH the registry: the gate prevents normalization, so the
        // output keeps the column simple (no spurious UserType upgrade, no
        // misdecode panic).
        let out_dir = tempfile::TempDir::new().expect("out dir");
        let report = compact_sstables(
            vec![input.data_path.clone()],
            out_dir.path(),
            &schema,
            2,
            None,
            None,
            true,
        )
        .await
        .expect("compaction must not panic on a simple-cell input");
        assert!(
            !header_has(&report.output.stats_path, "UserType("),
            "compaction must not upgrade a simple-cell input's column to UserType"
        );
    }

    /// #1015 schema evolution: an older input that simply LACKS the UDT column
    /// (absent from its header) must NOT veto normalization. Compacting it with a
    /// newer input that DOES carry the complex UDT must still normalize the
    /// column so the newer input's complex cells survive.
    #[tokio::test]
    async fn absent_column_in_older_input_does_not_veto_normalization() {
        let registry = person_registry();
        let full_schema = bare_udt_schema();

        // Input A (newer): complex `addr` written WITH the registry.
        let a_dir = tempfile::TempDir::new().expect("a dir");
        let mut wa =
            crate::storage::sstable::writer::SSTableWriter::with_expected_partitions_and_registry(
                a_dir.path().to_path_buf(),
                1,
                &full_schema,
                1,
                Some(&registry),
            )
            .expect("writer a");
        let udt = Value::Udt(Box::new(UdtValue {
            type_name: "person".to_string(),
            keyspace: "test_ks".to_string(),
            fields: vec![UdtField {
                name: "name".to_string(),
                value: Some(Value::text("Carol".to_string())),
            }],
        }));
        let ma = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            None,
            vec![CellOperation::Write {
                column: "addr".to_string(),
                value: udt,
            }],
            1_000_000,
            None,
        );
        let ka = ma.decorated_key(&full_schema).expect("key a");
        wa.write_partition(ka, vec![ma]).expect("write a");
        let input_a = wa.finish().await.expect("finish a");

        // Input B (older): a schema that LACKS `addr` entirely (has a `note`
        // column instead). Its header never declares `addr`.
        let older_schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "note".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };
        let b_dir = tempfile::TempDir::new().expect("b dir");
        let mut wb = crate::storage::sstable::writer::SSTableWriter::with_expected_partitions(
            b_dir.path().to_path_buf(),
            1,
            &older_schema,
            1,
        )
        .expect("writer b");
        let mb = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(2)),
            None,
            vec![CellOperation::Write {
                column: "note".to_string(),
                value: Value::text("x".to_string()),
            }],
            1_000_000,
            None,
        );
        let kb = mb.decorated_key(&older_schema).expect("key b");
        wb.write_partition(kb, vec![mb]).expect("write b");
        let input_b = wb.finish().await.expect("finish b");

        // `addr` is eligible: declared UserType in A, absent (not simple) in B.
        let plan = udt_columns_eligible_for_normalization(&[
            input_a.data_path.clone(),
            input_b.data_path.clone(),
        ]);
        assert!(
            plan.eligible_marshals.contains_key("addr") && plan.conflicts.is_empty(),
            "a column absent from an older input must remain eligible, got: {plan:?}"
        );

        // Compact both with the registry: `addr` must be normalized so A's complex
        // cells survive into the output (header advertises UserType).
        let out_dir = tempfile::TempDir::new().expect("out dir");
        let report = compact_sstables(
            vec![input_a.data_path.clone(), input_b.data_path.clone()],
            out_dir.path(),
            &full_schema,
            2,
            None,
            None,
            true,
        )
        .await
        .expect("compaction");
        assert!(
            header_has(&report.output.stats_path, PERSON_USERTYPE),
            "newer input's complex UDT column must survive compaction with an older input \
             that lacks the column"
        );
    }

    /// #1017 mixed encoding: the SAME column stored as complex `UserType` in one
    /// input and a simple cell in another cannot be represented by a single
    /// decode schema. Compaction must FAIL (not silently drop/corrupt the complex
    /// values) so the operator can rewrite the simple-cell SSTable first.
    #[tokio::test]
    async fn mixed_encoding_inputs_fail_compaction() {
        let registry = person_registry();
        let schema = bare_udt_schema();

        // Input A: complex `addr` (written WITH the registry).
        let a_dir = tempfile::TempDir::new().expect("a dir");
        let mut wa =
            crate::storage::sstable::writer::SSTableWriter::with_expected_partitions_and_registry(
                a_dir.path().to_path_buf(),
                1,
                &schema,
                1,
                Some(&registry),
            )
            .expect("writer a");
        let udt = Value::Udt(Box::new(UdtValue {
            type_name: "person".to_string(),
            keyspace: "test_ks".to_string(),
            fields: vec![UdtField {
                name: "name".to_string(),
                value: Some(Value::text("Dave".to_string())),
            }],
        }));
        let ma = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            None,
            vec![CellOperation::Write {
                column: "addr".to_string(),
                value: udt,
            }],
            1_000_000,
            None,
        );
        let ka = ma.decorated_key(&schema).expect("key a");
        wa.write_partition(ka, vec![ma]).expect("write a");
        let input_a = wa.finish().await.expect("finish a");

        // Input B: same `addr` column as a SIMPLE cell (written WITHOUT registry).
        let b_dir = tempfile::TempDir::new().expect("b dir");
        let mut wb = crate::storage::sstable::writer::SSTableWriter::with_expected_partitions(
            b_dir.path().to_path_buf(),
            1,
            &schema,
            1,
        )
        .expect("writer b");
        let udt_b = Value::Udt(Box::new(UdtValue {
            type_name: "person".to_string(),
            keyspace: "test_ks".to_string(),
            fields: vec![UdtField {
                name: "name".to_string(),
                value: Some(Value::text("Eve".to_string())),
            }],
        }));
        let mb = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(2)),
            None,
            vec![CellOperation::Write {
                column: "addr".to_string(),
                value: udt_b,
            }],
            1_000_000,
            None,
        );
        let kb = mb.decorated_key(&schema).expect("key b");
        wb.write_partition(kb, vec![mb]).expect("write b");
        let input_b = wb.finish().await.expect("finish b");

        // The plan flags `addr` as a conflict (UserType in A, simple in B).
        let plan = udt_columns_eligible_for_normalization(&[
            input_a.data_path.clone(),
            input_b.data_path.clone(),
        ]);
        assert!(
            plan.conflicts.contains("addr"),
            "mixed-encoding column must be reported as a conflict, got: {plan:?}"
        );

        // Compaction must refuse rather than corrupt.
        let out_dir = tempfile::TempDir::new().expect("out dir");
        let err = compact_sstables(
            vec![input_a.data_path.clone(), input_b.data_path.clone()],
            out_dir.path(),
            &schema,
            2,
            None,
            None,
            true,
        )
        .await
        .expect_err("mixed-encoding compaction must fail");
        let msg = format!("{err}");
        assert!(
            msg.contains("disagree on the encoding") && msg.contains("addr"),
            "error must explain the mixed-encoding conflict, got: {msg}"
        );
    }

    /// #1019 nested UDT: compaction copies the EXACT `UserType(...)` marshal from
    /// the input header (which may contain a nested `UserType` the flush-time
    /// renderer intentionally skips). A column whose on-disk marshal nests
    /// another UserType must survive compaction byte-exact (header preserved) and
    /// be decoded as complex, never dropped.
    #[tokio::test]
    async fn nested_usertype_column_survives_compaction() {
        // outer { i: inner { x: int } } — a UDT whose field is itself a UDT.
        const INNER_MARSHAL: &str =
            "org.apache.cassandra.db.marshal.UserType(test_ks,696e6e6572,78:org.apache.cassandra.db.marshal.Int32Type)";
        // outer(test_ks, hex "outer", hex "i" : <inner marshal>)
        let outer_marshal = format!(
            "org.apache.cassandra.db.marshal.UserType(test_ks,6f75746572,69:{INNER_MARSHAL})"
        );

        // Schema whose `addr` column already carries the full nested marshal, so
        // the writer treats it as complex without needing the registry to render
        // it (which it could not — nested UDTs are skipped at flush).
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "addr".to_string(),
                    data_type: outer_marshal.clone(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };

        let in_dir = tempfile::TempDir::new().expect("in dir");
        let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
            in_dir.path().to_path_buf(),
            1,
            &schema,
        )
        .expect("input writer");
        // `addr` = outer{ i: inner{ x: 5 } }.
        let addr = Value::Udt(Box::new(UdtValue {
            type_name: "outer".to_string(),
            keyspace: "test_ks".to_string(),
            fields: vec![UdtField {
                name: "i".to_string(),
                value: Some(Value::Udt(Box::new(UdtValue {
                    type_name: "inner".to_string(),
                    keyspace: "test_ks".to_string(),
                    fields: vec![UdtField {
                        name: "x".to_string(),
                        value: Some(Value::Integer(5)),
                    }],
                }))),
            }],
        }));
        let mutation = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            None,
            vec![CellOperation::Write {
                column: "addr".to_string(),
                value: addr,
            }],
            1_000_000,
            None,
        );
        let key = mutation.decorated_key(&schema).expect("decorated key");
        writer.write_partition(key, vec![mutation]).expect("write");
        let input = writer.finish().await.expect("finish");
        assert!(
            header_has(&input.stats_path, &outer_marshal),
            "input header must carry the exact nested UserType marshal"
        );

        // The plan copies the nested marshal verbatim (NOT a registry re-render).
        let plan = udt_columns_eligible_for_normalization(std::slice::from_ref(&input.data_path));
        assert_eq!(
            plan.eligible_marshals.get("addr").map(String::as_str),
            Some(outer_marshal.as_str()),
            "compaction must copy the exact nested marshal from the input header"
        );

        // Compaction (header-driven, no registry needed) must preserve the nested
        // marshal byte-exact in the output header (not degraded to BytesType).
        let out_dir = tempfile::TempDir::new().expect("out dir");
        let report = compact_sstables(
            vec![input.data_path.clone()],
            out_dir.path(),
            &schema,
            2,
            None,
            None,
            true,
        )
        .await
        .expect("compaction");
        assert!(
            header_has(&report.output.stats_path, &outer_marshal),
            "nested UserType column must survive compaction byte-exact"
        );
    }

    /// #1023 schema evolution: a bare UDT column ADDED after the inputs were
    /// written is absent from every input header, so the header gate leaves it
    /// bare. The engine's configured registry must normalize it (it has no input
    /// cells to misdecode) so compaction output is not emitted as BytesType.
    #[tokio::test]
    async fn newly_added_bare_udt_column_normalized_via_registry() {
        let registry = person_registry();

        // Input written with an OLDER schema lacking `addr` (only id + note).
        let older_schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "note".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };
        let in_dir = tempfile::TempDir::new().expect("in dir");
        let mut writer = crate::storage::sstable::writer::SSTableWriter::with_expected_partitions(
            in_dir.path().to_path_buf(),
            1,
            &older_schema,
            1,
        )
        .expect("writer");
        let m = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            None,
            vec![CellOperation::Write {
                column: "note".to_string(),
                value: Value::text("x".to_string()),
            }],
            1_000_000,
            None,
        );
        let k = m.decorated_key(&older_schema).expect("key");
        writer.write_partition(k, vec![m]).expect("write");
        let input = writer.finish().await.expect("finish");

        // The NEW schema adds the bare UDT column `addr` (absent from the input).
        let mut new_schema = bare_udt_schema();
        // `addr` is absent from the input header, so it is registry-normalized.
        crate::storage::write_engine::merge::apply_udt_marshals_from_inputs(
            &mut new_schema,
            std::slice::from_ref(&input.data_path),
            Some(&registry),
        )
        .expect("apply");
        let addr = new_schema
            .columns
            .iter()
            .find(|c| c.name == "addr")
            .expect("addr column");
        assert_eq!(
            addr.data_type, PERSON_USERTYPE,
            "a UDT column absent from all inputs must be registry-normalized, not left bare"
        );

        // Without a registry the column stays bare (free-fn / no-registry path).
        let mut bare_again = bare_udt_schema();
        crate::storage::write_engine::merge::apply_udt_marshals_from_inputs(
            &mut bare_again,
            std::slice::from_ref(&input.data_path),
            None,
        )
        .expect("apply no-registry");
        assert_eq!(
            bare_again
                .columns
                .iter()
                .find(|c| c.name == "addr")
                .unwrap()
                .data_type,
            "person",
            "without a registry an absent column stays bare"
        );
    }

    /// #1025 unknown header state: if an input header cannot be read/parsed, the
    /// column's true encoding is unknown. The absent-column registry fallback
    /// MUST NOT fire (it could misdecode an unreadable simple-cell input), so the
    /// bare column is left bare.
    #[tokio::test]
    async fn unreadable_header_does_not_trigger_registry_normalization() {
        let registry = person_registry();

        // A path whose sibling Statistics.db does not exist -> header unverified.
        let dir = tempfile::TempDir::new().expect("dir");
        let bogus = dir.path().join("nb-1-big-Data.db");

        let plan = udt_columns_eligible_for_normalization(std::slice::from_ref(&bogus));
        assert!(
            !plan.headers_verified,
            "an unreadable header must leave the plan unverified"
        );

        let mut schema = bare_udt_schema();
        crate::storage::write_engine::merge::apply_udt_marshals_from_inputs(
            &mut schema,
            &[bogus],
            Some(&registry),
        )
        .expect("apply");
        assert_eq!(
            schema
                .columns
                .iter()
                .find(|c| c.name == "addr")
                .unwrap()
                .data_type,
            "person",
            "with unverified headers, a bare UDT column must NOT be registry-normalized"
        );
    }
}

/// Roborev #959 fixes on the #933 range-tombstone compaction path:
///   - High #1: overlapping cross-SSTable ranges with different bounds must be
///     coalesced into a NON-OVERLAPPING canonical sequence before re-emission
///     (the writer emits independent open/close marker pairs and the reader
///     pairs them with a single pending-start, so overlaps corrupt on read-back).
///   - High #2: `apply_range_shadowing` must preserve a complex (collection)
///     deletion newer than the covering range when the row is otherwise fully
///     shadowed (it previously dropped `complex_deletions`).
#[cfg(all(test, feature = "write-support"))]
mod issue_959_range_tombstone_fixes {
    use super::*;
    use crate::schema::{Column, KeyColumn};
    use crate::storage::write_engine::mutation::ClusteringBound;
    use crate::types::Value;
    use std::collections::HashMap;

    fn dk(byte: u8) -> DecoratedKey {
        DecoratedKey::from_key_bytes(vec![byte]).expect("token")
    }

    /// Single-column `int` partition + single-column `int` clustering schema.
    fn schema_int_ck(order: crate::schema::ClusteringOrder) -> TableSchema {
        TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![crate::schema::ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order,
            }],
            columns: vec![Column {
                name: "v".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn ck(n: i32) -> ClusteringKey {
        ClusteringKey {
            columns: vec![("ck".to_string(), Value::Integer(n))],
        }
    }

    fn rt(start: ClusteringBound, end: ClusteringBound, dt: i64) -> RangeTombstone {
        RangeTombstone {
            start,
            end,
            deletion_time: dt,
            local_deletion_time: (dt / 1_000_000) as i32,
        }
    }

    /// High #1: two OVERLAPPING ranges with different bounds split into three
    /// disjoint segments, the inner (newer) deletion winning its overlap.
    #[test]
    fn overlapping_ranges_coalesce_to_disjoint_segments() {
        let schema = schema_int_ck(Default::default());
        // [1,5] @100 overlapped by [2,3] @200.
        let mut rts = vec![
            (
                dk(1),
                rt(
                    ClusteringBound::Inclusive(ck(1)),
                    ClusteringBound::Inclusive(ck(5)),
                    100,
                ),
            ),
            (
                dk(1),
                rt(
                    ClusteringBound::Inclusive(ck(2)),
                    ClusteringBound::Inclusive(ck(3)),
                    200,
                ),
            ),
        ];
        KWayMerger::coalesce_range_tombstones(&mut rts, &schema);

        assert_eq!(rts.len(), 3, "union splits into 3 disjoint segments");
        // [1, 2) @100
        assert_eq!(rts[0].1.start, ClusteringBound::Inclusive(ck(1)));
        assert_eq!(rts[0].1.end, ClusteringBound::Exclusive(ck(2)));
        assert_eq!(rts[0].1.deletion_time, 100);
        // [2, 3] @200 (the newer inner range wins the overlap)
        assert_eq!(rts[1].1.start, ClusteringBound::Inclusive(ck(2)));
        assert_eq!(rts[1].1.end, ClusteringBound::Inclusive(ck(3)));
        assert_eq!(rts[1].1.deletion_time, 200);
        // (3, 5] @100
        assert_eq!(rts[2].1.start, ClusteringBound::Exclusive(ck(3)));
        assert_eq!(rts[2].1.end, ClusteringBound::Inclusive(ck(5)));
        assert_eq!(rts[2].1.deletion_time, 100);

        // Every emitted segment is a valid, well-ordered, non-overlapping range:
        // each end is >= its start and each start is > the previous end.
        for i in 0..rts.len() {
            let s = KWayMerger::range_start_cut(&rts[i].1.start);
            let e = KWayMerger::range_end_cut(&rts[i].1.end);
            assert_ne!(
                KWayMerger::cut_cmp(&s, &e, &schema),
                Ordering::Greater,
                "segment {i} start must not exceed its end"
            );
            if i > 0 {
                // Non-overlap: the previous end cut must not exceed this start
                // cut. Equality is fine and expected — an exclusive end and an
                // inclusive start at the same value share one boundary cut while
                // covering complementary value sets (e.g. `< 2` then `>= 2`).
                let prev_e = KWayMerger::range_end_cut(&rts[i - 1].1.end);
                assert_ne!(
                    KWayMerger::cut_cmp(&prev_e, &s, &schema),
                    Ordering::Greater,
                    "segment {i} must not overlap the previous segment"
                );
            }
        }
    }

    /// Identical bounds across inputs collapse to one range at the newest
    /// deletion (the old `canonicalize` behavior is subsumed).
    #[test]
    fn identical_bounds_collapse_to_newest() {
        let schema = schema_int_ck(Default::default());
        let mut rts = vec![
            (
                dk(1),
                rt(
                    ClusteringBound::Inclusive(ck(1)),
                    ClusteringBound::Inclusive(ck(3)),
                    100,
                ),
            ),
            (
                dk(1),
                rt(
                    ClusteringBound::Inclusive(ck(1)),
                    ClusteringBound::Inclusive(ck(3)),
                    200,
                ),
            ),
        ];
        KWayMerger::coalesce_range_tombstones(&mut rts, &schema);
        assert_eq!(rts.len(), 1);
        assert_eq!(rts[0].1.deletion_time, 200);
        assert_eq!(rts[0].1.start, ClusteringBound::Inclusive(ck(1)));
        assert_eq!(rts[0].1.end, ClusteringBound::Inclusive(ck(3)));
    }

    /// Adjacent (touching) ranges at the SAME deletion merge into one.
    #[test]
    fn adjacent_same_deletion_ranges_merge() {
        let schema = schema_int_ck(Default::default());
        let mut rts = vec![
            (
                dk(1),
                rt(
                    ClusteringBound::Inclusive(ck(1)),
                    ClusteringBound::Inclusive(ck(3)),
                    100,
                ),
            ),
            (
                dk(1),
                rt(
                    ClusteringBound::Exclusive(ck(3)),
                    ClusteringBound::Inclusive(ck(5)),
                    100,
                ),
            ),
        ];
        KWayMerger::coalesce_range_tombstones(&mut rts, &schema);
        assert_eq!(rts.len(), 1, "touching equal-deletion ranges coalesce");
        assert_eq!(rts[0].1.start, ClusteringBound::Inclusive(ck(1)));
        assert_eq!(rts[0].1.end, ClusteringBound::Inclusive(ck(5)));
    }

    /// Distinct partition keys are coalesced independently (never merged).
    #[test]
    fn distinct_partitions_not_merged() {
        let schema = schema_int_ck(Default::default());
        let mut rts = vec![
            (
                dk(1),
                rt(ClusteringBound::Bottom, ClusteringBound::Top, 100),
            ),
            (
                dk(2),
                rt(ClusteringBound::Bottom, ClusteringBound::Top, 100),
            ),
        ];
        KWayMerger::coalesce_range_tombstones(&mut rts, &schema);
        assert_eq!(rts.len(), 2);
        assert_ne!(rts[0].0.key, rts[1].0.key);
    }

    /// High #2: a complex deletion NEWER than the covering range survives as a
    /// metadata carrier when the row is otherwise fully shadowed.
    #[test]
    fn newer_complex_deletion_survives_full_range_shadow() {
        let schema = schema_int_ck(Default::default());
        // Row at ck=2 with only its clustering pseudo-cell (no live data) plus a
        // collection deletion @200; covered by a whole-partition range @100.
        let entry = MergeEntry::new(
            0,
            dk(1),
            Some(ck(2)),
            50,
            RowData::Live {
                cells: vec![CellData::new("ck".to_string(), Value::Integer(2), 50)],
            },
        )
        .with_complex_deletions(vec![ComplexDeletion {
            column: "v".to_string(),
            marked_for_delete_at: 200,
            local_deletion_time: 0,
        }]);
        let range = vec![(
            dk(1),
            rt(ClusteringBound::Bottom, ClusteringBound::Top, 100),
        )];

        let out = KWayMerger::apply_range_shadowing(entry, &range, &schema)
            .expect("a complex deletion newer than the range must survive");
        assert_eq!(
            out.complex_deletions.len(),
            1,
            "the newer collection deletion is preserved"
        );
        assert_eq!(out.complex_deletions[0].marked_for_delete_at, 200);
        match out.row_data {
            RowData::Live { cells } => assert!(cells.is_empty(), "carrier holds no data cells"),
            other => panic!("expected an empty Live carrier, got {other:?}"),
        }
    }

    /// A complex deletion OLDER than the covering range is subsumed and dropped,
    /// leaving nothing for the re-emitted range marker to duplicate.
    #[test]
    fn older_complex_deletion_subsumed_by_range() {
        let schema = schema_int_ck(Default::default());
        let entry = MergeEntry::new(
            0,
            dk(1),
            Some(ck(2)),
            50,
            RowData::Live {
                cells: vec![CellData::new("ck".to_string(), Value::Integer(2), 50)],
            },
        )
        .with_complex_deletions(vec![ComplexDeletion {
            column: "v".to_string(),
            marked_for_delete_at: 50,
            local_deletion_time: 0,
        }]);
        let range = vec![(
            dk(1),
            rt(ClusteringBound::Bottom, ClusteringBound::Top, 100),
        )];

        assert!(
            KWayMerger::apply_range_shadowing(entry, &range, &schema).is_none(),
            "an older complex deletion is subsumed; nothing survives"
        );
    }

    /// Issue #2374/#2789 (rust-reviewer BLOCKER 2): a key-only LIVE row (an
    /// `INSERT` with only its primary-key liveness marker, e.g. no regular
    /// columns / all-null regulars) that coexists with an OLDER covering RANGE
    /// tombstone survives shadowing (its marker timestamp beats the floor). The
    /// surviving `RowData::Live` rebuild in `apply_range_shadowing` must carry
    /// the marker forward via `with_row_liveness` — before the fix it dropped to
    /// `RowLiveness::default()` (has_marker=false), so the READ-path visibility
    /// rule (`marker_live_at`) then wrongly HID the row. Cassandra returns it.
    #[test]
    fn key_only_live_row_keeps_marker_through_range_shadow() {
        use crate::storage::sstable::reader::compaction_row::RowLiveness;
        let schema = schema_int_ck(Default::default());
        // Key-only live row at ck=2: only the clustering pseudo-cell (no data),
        // liveness marker @200 (live-forever); covered by a range @100.
        let entry = MergeEntry::new(
            0,
            dk(1),
            Some(ck(2)),
            200,
            RowData::Live {
                cells: vec![CellData::new("ck".to_string(), Value::Integer(2), 200)],
            },
        )
        .with_row_liveness(RowLiveness {
            has_marker: true,
            expires_at_seconds: None,
            marker_timestamp: Some(200),
        });
        let range = vec![(
            dk(1),
            rt(ClusteringBound::Bottom, ClusteringBound::Top, 100),
        )];

        let out = KWayMerger::apply_range_shadowing(entry, &range, &schema)
            .expect("a key-only live row newer than the range must survive");
        assert!(
            out.row_liveness.marker_live_at(1_000),
            "the surviving row must carry its liveness marker forward (BLOCKER 2, range)"
        );
    }

    /// BLOCKER 2, partition variant: same key-only-live-row invariant through
    /// `apply_partition_shadowing`.
    #[test]
    fn key_only_live_row_keeps_marker_through_partition_shadow() {
        use crate::storage::sstable::reader::compaction_row::RowLiveness;
        let schema = schema_int_ck(Default::default());
        let _ = &schema; // schema unused by partition shadowing, kept for parity.
        let entry = MergeEntry::new(
            0,
            dk(1),
            Some(ck(2)),
            200,
            RowData::Live {
                cells: vec![CellData::new("ck".to_string(), Value::Integer(2), 200)],
            },
        )
        .with_row_liveness(RowLiveness {
            has_marker: true,
            expires_at_seconds: None,
            marker_timestamp: Some(200),
        });

        let out = KWayMerger::apply_partition_shadowing(entry, Some((100, 0)))
            .expect("a key-only live row newer than the partition floor must survive");
        assert!(
            out.row_liveness.marker_live_at(1_000),
            "the surviving row must carry its liveness marker forward (BLOCKER 2, partition)"
        );
    }

    /// BLOCKER 2 negative: a key-only row whose marker is OLDER-or-equal to the
    /// covering floor does NOT survive as a phantom live row — the marker must
    /// NOT be carried forward (marker_live stays false). Range case fully
    /// shadows to `None`; the invariant is that no live-marker survivor leaks.
    #[test]
    fn key_only_expired_marker_does_not_survive_range_shadow() {
        use crate::storage::sstable::reader::compaction_row::RowLiveness;
        let schema = schema_int_ck(Default::default());
        // Marker @80 (older than the range @100) → fully shadowed, no survivor.
        let entry = MergeEntry::new(
            0,
            dk(1),
            Some(ck(2)),
            80,
            RowData::Live {
                cells: vec![CellData::new("ck".to_string(), Value::Integer(2), 80)],
            },
        )
        .with_row_liveness(RowLiveness {
            has_marker: true,
            expires_at_seconds: None,
            marker_timestamp: Some(200),
        });
        let range = vec![(
            dk(1),
            rt(ClusteringBound::Bottom, ClusteringBound::Top, 100),
        )];
        assert!(
            KWayMerger::apply_range_shadowing(entry, &range, &schema).is_none(),
            "a marker older than the covering range must not survive as a live row"
        );
    }
}
