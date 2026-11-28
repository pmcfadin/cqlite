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
use self::trace::{NoTrace, TombstoneKind, TombstoneRecord, TraceSink};
#[cfg(feature = "write-support")]
use crate::error::{Error, Result};
#[cfg(feature = "write-support")]
use crate::schema::TableSchema;
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::{
    ClusteringKey, DecoratedKey, PartitionTombstone, RangeTombstone,
};
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
use std::path::PathBuf;
#[cfg(feature = "write-support")]
use std::time::{Duration, Instant};

mod input_stats;
pub(crate) use input_stats::InputStats;

mod model;
#[cfg(feature = "write-support")]
pub use model::{CellData, ComplexDeletion, MergeEntry, MergeStats, MergeStep, RowData};

/// Public reconciliation decision-trail sink and event types (issue #4193).
pub mod trace;

/// Read-shape reassembly of a merged row's per-column cells (issue #2324):
/// collapse per-element collection cells back into a single `Value::List` /
/// `Value::Set` / `Value::Map` for read consumers that key cells by column name.
mod read_assembly;
/// ONE authority for "does this column have a cell that SURVIVES reconciliation?" — the
/// Flight row-visibility decision asks this for a column whose liveness is ambiguous
/// from timestamps alone, instead of guessing from raw cells (#2339, roborev job 128).
#[cfg(feature = "write-support")]
pub use read_assembly::column_has_surviving_live_cell;
/// ONE authority for "can the merged arm ORDER this composite?" — the bypass
/// divergence predicate in `cqlite-flight` asks this rather than keeping its own leaf
/// list, so the two arms cannot disagree (#4063, roborev job 116 F1).
///
/// Gated like every sibling re-export in this file. That is REDUNDANT today — both
/// `storage::write_engine` and this `merge` module are already
/// `#[cfg(feature = "write-support")]`, so nothing here compiles without it — and it
/// is kept because the neighbours do it and because the redundancy is what makes the
/// line survive a change to the enclosing gate (roborev job 118, whose stated
/// `--no-default-features` compile failure does NOT reproduce for that reason).
#[cfg(feature = "write-support")]
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
    build_single_partition_merger_with_registry, build_single_partition_merger_with_trace,
    build_traced_merger_with_registry, PointAccessRecording,
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
pub(crate) mod fully_expired;
#[cfg(feature = "write-support")]
pub use fully_expired::fully_expired_sstables;
pub(crate) use fully_expired::fully_expired_sstables_in;
#[cfg(feature = "write-support")]
pub(crate) use fully_expired::{reclaim_dropped_whole, split_merge_and_dropped};

/// Repair-state classification + mixed-state rejection for compaction
/// (issue #1021). Reads each input's persisted repair state from `Statistics.db`
/// and either returns the shared state to preserve or rejects a mixed-state set.
#[cfg(feature = "write-support")]
pub mod repair_state;
pub(crate) use repair_state::classify_inputs_in;
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
#[cfg(feature = "write-support")]
mod reconcile_cluster;

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

// ---- #4658: the former 12k-line body, split by responsibility (mechanical moves) ----

/// Buffered reader over one SSTable run.
#[cfg(feature = "write-support")]
mod run_reader;
#[cfg(feature = "write-support")]
use run_reader::RunReader;
#[cfg(feature = "write-support")]
pub use run_reader::SSTableRowIterator;

/// Baseline and purge-bound preparation from the inputs' `Statistics.db`.
#[cfg(feature = "write-support")]
mod baseline;
#[cfg(feature = "write-support")]
pub use baseline::{
    all_input_stats_readable, compute_baseline_min, compute_gc_before,
    compute_max_purgeable_timestamp,
};
#[cfg(feature = "write-support")]
pub(crate) use baseline::{
    all_input_stats_readable_in, compute_baseline_min_in, compute_expiry_ttl_ldt_floor_in,
    compute_max_purgeable_timestamp_in,
};

/// Bare-name UDT normalisation and the effective compaction schema.
#[cfg(feature = "write-support")]
mod udt_normalization;
#[cfg(feature = "write-support")]
pub(crate) use udt_normalization::{
    apply_udt_marshals_from_inputs, apply_udt_marshals_from_inputs_in,
    effective_compaction_schema_in,
};
#[cfg(feature = "write-support")]
pub use udt_normalization::{
    effective_compaction_schema, udt_columns_eligible_for_normalization, UdtNormalizationPlan,
};

/// One-shot compaction and the dropped-column survivor pre-pass.
#[cfg(feature = "write-support")]
mod compact;
#[cfg(feature = "write-support")]
pub(crate) use compact::compute_surviving_dropped_columns;
#[cfg(feature = "write-support")]
pub use compact::{compact_sstables, compact_sstables_with_registry, CompactReport};

// `KWayMerger`'s impl blocks, by responsibility.
#[cfg(feature = "write-support")]
mod merger_mutation;
#[cfg(feature = "write-support")]
mod merger_open;
#[cfg(feature = "write-support")]
mod merger_partition;
#[cfg(feature = "write-support")]
mod merger_step;

// Tests, next to the code they test.
#[cfg(all(test, feature = "write-support"))]
mod baseline_conversion_tests;
#[cfg(all(test, feature = "write-support"))]
mod merge_entry_tests;
#[cfg(all(test, feature = "write-support"))]
#[path = "merge_property_tests.rs"]
mod merge_property_tests;
#[cfg(all(test, feature = "write-support"))]
mod real_merger_tests;

// ─────────────────────────────────────────────────────────────────────────────
// Issue #823 (Epic #817): complex-column (multi-cell collection / non-frozen UDT)
// merge behaviour.
//
// ADDITIVE TEST MODULE — flagged per the issue brief. This module is `#[cfg(test)]`
// only and adds NO production code. It exists because the authoritative reconcile
// function `KWayMerger::<NoTrace>::reconcile_cluster` and the reader→merge adapter
// `SSTableRowIteratorAdapter::value_to_row_data` are private, so the gating
// behaviour can only be value-asserted from inside the `merge` module.
//
// These tests ESTABLISH (do not aspire to) the current behaviour so the findings
// doc can cite real code. They assert the divergence where one exists.
// ─────────────────────────────────────────────────────────────────────────────
#[cfg(all(test, feature = "write-support"))]
#[path = "issue_823_complex_column_merge_tests.rs"]
mod issue_823_complex_column_merge;

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
#[path = "issue_886_merge_entry_enrichment_tests.rs"]
mod issue_886_merge_entry_enrichment;

/// Epic #899, Phase A (roborev #863): the reader→merge per-element substrate is
/// POPULATED (the foundation), but Phase A is BEHAVIOR-NEUTRAL: the merge OUTPUT
/// path emits one whole-collection `CellData` per complex column (byte-identical
/// to pre-Phase-A) while the per-element data rides alongside on the
/// `CompactionRow` and `MergeEntry.complex_deletions` for Phase C. These tests
/// pin both the substrate and the neutral output, and the per-`(column,
/// cell_path)` reconcile CAPABILITY that Phase C will use.
#[cfg(all(test, feature = "write-support"))]
#[path = "issue_899_per_element_merge_tests.rs"]
mod issue_899_per_element_merge;

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
#[path = "issue_822_merge_ordering_semantics_tests.rs"]
mod issue_822_merge_ordering_semantics;

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
#[path = "issue_886_empty_partition_skip_tests.rs"]
mod issue_886_empty_partition_skip;

/// Issue #912: clustering-row tombstones must carry their clustering identity
/// through the compaction stream so they reconcile in their OWN clustering bucket
/// instead of collapsing into the partition's single `None` bucket (where they
/// would mis-reconcile against each other and against the static row).
#[cfg(all(test, feature = "write-support"))]
#[path = "issue_912_row_tombstone_clustering_identity_tests.rs"]
mod issue_912_row_tombstone_clustering_identity;

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
#[path = "issue_873_preserve_row_tombstone_ldt_tests.rs"]
mod issue_873_preserve_row_tombstone_ldt;

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
#[path = "issue_845_gc_grace_purge_tests.rs"]
mod issue_845_gc_grace_purge;

// ── Issue #929: bare-name UDT normalization survives compaction ──────────────
//
// A flush normalizes a bare-name non-frozen UDT column (e.g. `addr person`) to
// its `UserType(...)` marshal via the registry and writes complex per-field
// cells with a matching SERIALIZATION_HEADER. Compaction must apply the SAME
// normalization to its output write-schema, or it would rewrite the column as a
// single simple cell and degrade the header to `BytesType` (roborev #1007).
#[cfg(all(test, feature = "write-support"))]
#[path = "issue_929_bare_udt_compaction_tests.rs"]
mod issue_929_bare_udt_compaction;

/// Roborev #959 fixes on the #933 range-tombstone compaction path:
///   - High #1: overlapping cross-SSTable ranges with different bounds must be
///     coalesced into a NON-OVERLAPPING canonical sequence before re-emission
///     (the writer emits independent open/close marker pairs and the reader
///     pairs them with a single pending-start, so overlaps corrupt on read-back).
///   - High #2: `apply_range_shadowing` must preserve a complex (collection)
///     deletion newer than the covering range when the row is otherwise fully
///     shadowed (it previously dropped `complex_deletions`).
#[cfg(all(test, feature = "write-support"))]
#[path = "issue_959_range_tombstone_fixes_tests.rs"]
mod issue_959_range_tombstone_fixes;

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
// `pub(crate)` (issue #4196): `salvage_sstable` calls
// `SSTableRowIteratorAdapter::build_merge_entry` directly to reuse the exact
// CompactionRow -> MergeEntry conversion compaction uses.
#[cfg(feature = "write-support")]
pub(crate) use producer_iter::SSTableRowIteratorAdapter;

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
pub struct KWayMerger<S: TraceSink = NoTrace> {
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
    /// gc_grace cutoff (epoch seconds): tombstones with `local_deletion_time <` this
    /// are purgeable (#842, #845; applied in `reconcile_cluster`).
    #[allow(dead_code)]
    gc_before_secs: Option<i64>,
    /// "now" (epoch seconds) used to evaluate TTL expiry during merge (#848).
    #[allow(dead_code)]
    now_secs: Option<i64>,
    /// Overlap-safety gate for tombstone purging (#921 finding 1).
    ///
    /// A partial compaction may leave data shadowed by a tombstone in a NON-included
    /// overlapping SSTable; purging that tombstone would resurrect it. CQLite has no
    /// `CompactionController`, so a tombstone is purged ONLY when this flag proves the
    /// compaction spans ALL of the table's SSTables (major/full). DEFAULT `false`: the
    /// partial path never purges; the policy-driven path sets `true` only for full coverage.
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
    /// Static-dispatch decision-trail sink. `NoTrace` is zero-sized in every
    /// existing constructor; traced callers replace it with a recording sink.
    trace: S,
}

/// Tally of tombstones GENUINELY PURGED during a partition merge (issue #1037).
///
/// Accumulated only at the true gc_grace / overlap-safe purge decision points in
/// [`KWayMerger::<NoTrace>::reconcile_cluster_with_overlap_counted`] and
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

// Streaming channel / cursor mechanism tests (issues #754 / #2820), moved to a
// `*_tests.rs` sibling per the #1116 campsite rule — see that file's header.
#[cfg(all(test, feature = "write-support"))]
#[path = "streaming_channel_tests.rs"]
mod streaming_channel_tests;
