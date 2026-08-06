//! Single-partition point-read merge assembly (issue #2207, Stage 1/2 core side).
//!
//! Builds a [`KWayMerger`] over ONE run per candidate SSTable, each yielding only
//! the target partition's entries, so the Flight `do_get` point path reconciles
//! through the SAME merge as the full scan — byte-identically, only over fewer
//! partitions (design candidate (c)).
//!
//! Per candidate the core primitive
//! [`SSTableReader::read_single_partition_for_compaction`] decides:
//! - `DefinitelyAbsent` → the candidate is pruned (the presence oracle already
//!   incremented `cqlite.read.sstables_pruned`); no run is built.
//! - `Rows(rows)` → a seeked run yielding exactly those compaction rows.
//! - `IndexUnavailable` → a fail-safe run that scans this ONE SSTable's whole
//!   compaction stream and forwards only the target partition's entries — never
//!   skipping a candidate that might hold the key (#2295 Data.db-only shape).
//!
//! Run index (LWW tie-break rank) is the candidate's position in the input
//! `paths` list, identical to the full-scan merger, so reconciliation ties break
//! the same way.

use super::{
    egress_budget, KWayMerger, MergeEntry, RunReader, SSTableRowIterator, SSTableRowIteratorAdapter,
};
use crate::observability::partition_access;
use crate::schema::{TableSchema, UdtRegistry};
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::{CompactionRow, SSTableReader};
use crate::{Error, Result};
use std::collections::{BinaryHeap, VecDeque};
use std::path::{Path, PathBuf};
// Unconditional (issue #2346): `build_single_partition_merger_from_readers`
// takes `Vec<Arc<SSTableReader>>` regardless of the `tombstones` feature (its
// `NeedsScan` fail-safe still needs `SSTableReader`/`Arc` either way).
use std::sync::Arc;

// Seek-only imports (the point-read primitive is `not(tombstones)` gated, like the
// underlying single-partition seek machinery it composes).
#[cfg(not(feature = "tombstones"))]
use super::block_on_async;
#[cfg(not(feature = "tombstones"))]
use crate::config::DiskAccessMode;
#[cfg(not(feature = "tombstones"))]
use crate::platform::Platform;
#[cfg(not(feature = "tombstones"))]
use crate::storage::sstable::reader::SinglePartitionCompaction;
#[cfg(not(feature = "tombstones"))]
use crate::Config;

impl KWayMerger {
    /// Build a k-way merger from pre-constructed run iterators (issue #2207).
    ///
    /// Unlike [`KWayMerger::new`], which opens each input SSTable and streams its
    /// WHOLE compaction scan, this accepts runs the caller has already scoped —
    /// the single-partition point-read path hands one run per candidate SSTable,
    /// each yielding ONLY the target partition's entries (a seeked `Vec` or a
    /// key-filtered stream). The reconciliation, heap, and per-partition merge are
    /// IDENTICAL to the full-scan path — only the inputs are narrower — so the
    /// point path reconciles byte-identically to the scan.
    ///
    /// `runs` must be non-empty and ordered newest-to-oldest (run index = LWW
    /// tie-break rank), exactly as [`KWayMerger::new`]'s `input_paths` are.
    pub fn from_row_iterators(
        runs: Vec<Box<dyn SSTableRowIterator>>,
        schema: &TableSchema,
    ) -> Result<Self> {
        if runs.is_empty() {
            return Err(Error::InvalidInput(
                "K-way merge requires at least one input run".to_string(),
            ));
        }
        schema.validate_dropped_columns()?;
        // Child modules see the parent's private fields; build the merger in the
        // same shape as `new_with_gc_and_registry_cancellable` (no purge params —
        // the point path never compacts).
        let runs = runs.into_iter().map(RunReader::new).collect();
        Ok(Self {
            runs,
            heap: BinaryHeap::new(),
            current_partition: None,
            schema: schema.clone(),
            // Issue #1668, stage 5c-i: `Arc`-wrapped clone of `schema`, used
            // only by the heap's schema-aware comparator (see the field doc).
            // (Semantic-merge fix: #1668 landed on main between #2207's branch
            // point and its merge; this constructor predates the field.)
            schema_arc: std::sync::Arc::new(schema.clone()),
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            // Issue #2765: the point-read builders open their fail-safe adapters
            // (and snapshot the shared capacity) OUTSIDE this constructor, then
            // attach the matching slot guard via `with_egress_slot`. A merger
            // built directly from pre-supplied runs registers no slot.
            _egress_slot: None,
        })
    }
}

/// A run backed by a pre-built `Vec` of merge entries — the seeked single
/// partition. Yields each entry once, in order, then reports exhaustion.
///
/// Only the default `not(tombstones)` build seeks (the alternate build always
/// scan-falls-back), so this is dead there.
#[cfg_attr(feature = "tombstones", allow(dead_code))]
struct VecRun {
    entries: VecDeque<MergeEntry>,
}

impl SSTableRowIterator for VecRun {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        self.entries.pop_front().map(Ok)
    }
}

/// A run that forwards ONLY the target partitions' entries from an SSTable's full
/// compaction stream — the index-unavailable fail-safe. Byte-identical to the
/// full scan's contribution for those partitions (same stream, same decode), just
/// with every other partition dropped before the merge sees it.
struct SinglePartitionFilterRun {
    inner: SSTableRowIteratorAdapter,
    keys: std::collections::HashSet<Vec<u8>>,
}

impl SSTableRowIterator for SinglePartitionFilterRun {
    /// Issue #2765: delegate the egress-capacity observation to the wrapped
    /// adapter so the point-read fail-safe channel is observable by wiring tests.
    #[cfg(test)]
    fn egress_channel_capacity(&self) -> Option<usize> {
        self.inner.egress_channel_capacity()
    }

    fn next(&mut self) -> Option<Result<MergeEntry>> {
        loop {
            match self.inner.next() {
                Some(Ok(entry)) => {
                    if self.keys.contains(&entry.key.key) {
                        return Some(Ok(entry));
                    }
                    // A non-target partition: drop it and pull the next entry.
                }
                // An error or exhaustion propagates unchanged (a cancelled scan
                // stays a distinct `Error::Cancelled`, issue #2264).
                other => return other,
            }
        }
    }
}

/// What one candidate SSTable reported about ONE requested key's on-disk size —
/// the byte-weight half of the partition access-distribution probe (issue #2827).
///
/// The probe sites supply byte weights ONLY; they never count accesses. Counting
/// here would multiply a partition's repeat count by the number of generations
/// holding it and manufacture concentration the workload does not have, which is a
/// bias toward "build the cache". The single access is recorded once per logical
/// point read by the builder below.
/// Only the default `not(tombstones)` seek path resolves per-key sizes (the
/// alternate build always falls back to a whole-file scan and produces no notes),
/// so the variants are dead there — same shape as [`PathProbe`].
#[cfg_attr(feature = "tombstones", allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeySizeNote {
    /// This SSTable does not hold the key, so it contributes nothing either way.
    NotHeld,
    /// This SSTable holds the key and its index recorded an authoritative on-disk
    /// size for it.
    Sized(u32),
    /// This SSTable holds the key and its on-disk extent was MEASURED as the
    /// partition's successor gap.
    Measured(u64),
    /// This SSTable holds (or may hold) the key but no authoritative extent is
    /// available — e.g. a fail-safe whole-file scan resolves no per-partition
    /// layout, or no data-section length is known. Never estimated.
    Unsized,
}

/// Whether a merger builder records the #2827 partition accesses itself.
///
/// The reader-based builder has TWO callers with opposite needs, so this cannot be a
/// property of the function:
///
/// - the **core executor's** multi-generation targeted read reaches it through
///   `generation_merge::seek_merge_generations_for_read`, and the executor already
///   records the access at its own logical boundary
///   (`StorageEngine::scan_partition_clustering`) — recording here too would count
///   one logical read twice;
/// - the **Flight warm point path** reaches it directly, and this IS its logical
///   point-read boundary, so recording must happen here or not at all.
///
/// Counting stays once per logical partition read either way. Nothing is ever
/// recorded at a per-SSTable probe site, which would multiply a repeat count by the
/// number of generations holding the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PointAccessRecording {
    /// This call is the logical point-read boundary: record one access per key.
    Record,
    /// The caller records at its own logical boundary; do not record here.
    CallerRecords,
}

/// A candidate probe plus, when the probe is enabled, one [`KeySizeNote`] per
/// requested key in the requested order.
struct ProbeOutcome {
    probe: PathProbe,
    /// Empty when the probe is disabled — the default, so this costs nothing.
    notes: Vec<KeySizeNote>,
}

/// Per-candidate probe outcome across ALL requested keys. `Seeked`/`Empty` are
/// produced only by the default `not(tombstones)` seek path (the alternate build
/// always returns `NeedsScan`), so they are dead under `tombstones`.
#[cfg_attr(feature = "tombstones", allow(dead_code))]
enum PathProbe {
    /// No requested key is present in this SSTable (all definite-absent / empty).
    Empty,
    /// The seeked compaction rows for every present key (combined; same generation
    /// so they share one run index).
    Seeked(Vec<CompactionRow>),
    /// This SSTable has no usable index — scan it, filtered to the requested keys.
    NeedsScan,
}

/// Assemble a [`KWayMerger`] that reads ONLY the target `keys` across the
/// candidate `paths` (issue #2207).
///
/// Returns `Ok(None)` when no candidate holds any target key (every one a definite
/// presence-oracle negative / prefix-collision) — the caller streams zero rows.
/// Otherwise returns a merger the caller drives through its existing reconciliation
/// loop; it emits one `MergeStep::Partition` per distinct present key, reconciled
/// across generations exactly as the full-scan merge would.
///
/// `paths` must be the same (token-pruned) candidate list, in the same order, the
/// full-scan path would merge — run index is the position here, so LWW tie-breaks
/// match. `keys` are raw partition-key bytes (as `PartitionKey::to_bytes`
/// produces). `scan_cancel` is threaded as a PER-CALL token into every probe/scan
/// (issue #2346) so a client disconnect abandons an in-flight fail-safe scan
/// promptly (#2264).
pub fn build_single_partition_merger(
    paths: Vec<PathBuf>,
    keys: &[Vec<u8>],
    schema: &TableSchema,
    scan_cancel: ScanCancel,
) -> Result<Option<KWayMerger>> {
    build_single_partition_merger_with_registry(paths, keys, schema, None, scan_cancel)
}

/// Like [`build_single_partition_merger`], but threads an authoritative
/// [`UdtRegistry`] onto every candidate reader (issue #2349) so a `frozen<UDT>`
/// cell inside a collection decodes structurally instead of as opaque bytes — the
/// cold point-read analogue of the full-scan
/// [`KWayMerger::new_with_gc_and_registry_cancellable`]. Passing `None` is
/// byte-identical to the registry-free path. Used by the Flight point-read route so
/// point and full-scan reads never diverge (issue #1918 differential lane).
pub fn build_single_partition_merger_with_registry(
    paths: Vec<PathBuf>,
    keys: &[Vec<u8>],
    schema: &TableSchema,
    udt_registry: Option<&UdtRegistry>,
    scan_cancel: ScanCancel,
) -> Result<Option<KWayMerger>> {
    schema.validate_dropped_columns()?;
    if keys.is_empty() {
        return Ok(None);
    }

    // Canonicalize the requested key set (roborev suggestion, issue #2207):
    // dedup identical raw keys — a `pk IN (5, 5)` ticket must not double-seek the
    // same partition — and sort by Murmur3 token, the natural probing order
    // (matches the ascending order `MergeEntry::Ord` requires each run to
    // yield). Correctness does not depend on this order (each candidate's
    // combined entries are sorted again below before wrapping in `VecRun`), but
    // canonicalizing up front avoids redundant work and reads more naturally.
    let mut canonical: Vec<Vec<u8>> = keys.to_vec();
    canonical.sort_by_key(|k| crate::util::cassandra_murmur3::cassandra_murmur3_token(k));
    canonical.dedup();
    let keys: &[Vec<u8>] = &canonical;

    let key_set: std::collections::HashSet<Vec<u8>> = keys.iter().cloned().collect();

    // Issue #2765 (LAZY registration): a point read whose candidates ALL seek
    // (`PathProbe::Seeked` → in-memory `VecRun`s, ZERO egress channels) buffers
    // NOTHING, so it must NOT occupy an active-merge slot — else high-QPS Flight
    // point reads would inflate the process-global count and throttle a
    // concurrent channel-backed compaction/full-scan toward `MIN_CAP`. Defer
    // `begin_merge` to the FIRST `NeedsScan` egress channel and memoize its
    // snapshot so every channel in THIS merge still shares ONE capacity.
    let mut egress: Option<(usize, egress_budget::ActiveMergeGuard)> = None;

    let mut runs: Vec<Box<dyn SSTableRowIterator>> = Vec::new();
    // Byte weights for the partition access-distribution probe (issue #2827), one
    // accumulator per requested key, summed across the candidates that resolved it.
    // Allocated only when the probe is enabled (it is OFF by default).
    // The path-based builder IS the cold Flight point path's logical boundary, so it
    // always records. Extent measurement is nonetheless gated on the probe being on:
    // it is real work (an index materialize + a successor resolution per key per
    // generation) and must never be paid by a process that asked for no telemetry.
    let collect = partition_access::enabled();
    let mut weights: Vec<partition_access::AccessWeightBuilder> = if collect {
        vec![partition_access::AccessWeightBuilder::new(); keys.len()]
    } else {
        Vec::new()
    };
    for (run_index, path) in paths.iter().enumerate() {
        // Cooperative cancellation (#2264): honour a cancel BEFORE each candidate's
        // seek/open, so a disconnected point read does no further per-SSTable work.
        scan_cancel.check()?;

        let ProbeOutcome { probe, notes } = probe_path(
            path,
            keys,
            schema,
            udt_registry,
            scan_cancel.clone(),
            collect,
        )?;
        fold_size_notes(&mut weights, &notes);
        match probe {
            PathProbe::Empty => {
                // Pruned / absent for every key. No run.
            }
            PathProbe::Seeked(rows) => {
                if rows.is_empty() {
                    continue;
                }
                let mut entries: Vec<MergeEntry> = Vec::with_capacity(rows.len());
                for row in rows {
                    // Issue #2096: one merge entry decoded from `Data.db` by the
                    // SEEK path (this candidate held the target partition). Counting
                    // per built entry — the same unit the full-scan run counts per
                    // `Ok(entry)` — keeps `merge_run_entries_decoded` apples-to-
                    // apples between the two paths.
                    crate::storage::sstable::work_counters::add_merge_run_entry_decoded();
                    entries.push(SSTableRowIteratorAdapter::build_merge_entry(
                        run_index, row, schema,
                    )?);
                }
                // BLOCKER fix (roborev, issue #2207): `rows` was accumulated in
                // REQUESTED-KEY order (IN-list / Or order), not token order. Every
                // `SSTableRowIterator` MUST yield entries in ascending `MergeEntry`
                // order (token, key, clustering) — `refill_heap` buffers only ONE
                // entry per run at a time and relies on that invariant (mod.rs
                // `step`/`refill_heap`). An out-of-order run causes `step()` to
                // silently split one run's contribution across two heap pops,
                // duplicating rows and breaking cross-generation reconciliation
                // (a newer generation's overwrite/tombstone no longer shadows the
                // older one). Sort once, here, before wrapping in `VecRun`.
                entries.sort();
                runs.push(Box::new(VecRun {
                    entries: entries.into(),
                }));
            }
            PathProbe::NeedsScan => {
                // Fail-safe: scan this ONE SSTable, forwarding only the target
                // partitions. The missing index costs speed, never correctness.
                // Snapshot the shared capacity on the FIRST egress channel only.
                let channel_capacity = egress.get_or_insert_with(egress_budget::begin_merge).0;
                let adapter = SSTableRowIteratorAdapter::open(
                    path,
                    run_index,
                    schema,
                    udt_registry.cloned(),
                    scan_cancel.clone(),
                    channel_capacity,
                )?;
                runs.push(Box::new(SinglePartitionFilterRun {
                    inner: adapter,
                    keys: key_set.clone(),
                }));
            }
        }
    }

    // ONE access per LOGICAL partition read, recorded after every candidate has
    // reported (issue #2827). This is the point-read path's logical boundary: the
    // key list is canonical (deduplicated), so `pk IN (5, 5)` is one access, and a
    // key present in seven generations is still one access carrying the SUM of the
    // per-generation on-disk sizes. Recorded even when no candidate held the key —
    // a miss is a real access, and omitting it would understate the singleton
    // bucket. A no-op when the probe is disabled.
    record_logical_accesses(schema, keys, weights);

    if runs.is_empty() {
        // No candidate produced a run; any snapshot guard drops here.
        return Ok(None);
    }
    let merger = KWayMerger::from_row_iterators(runs, schema)?;
    // Attach the slot ONLY if a `NeedsScan` egress channel was actually opened;
    // an all-`Seeked` (channel-less) point read registers no slot.
    Ok(Some(match egress {
        Some((_, guard)) => merger.with_egress_slot(guard),
        None => merger,
    }))
}

/// Reader-based analogue of [`build_single_partition_merger`] (issue #2346):
/// assembles a [`KWayMerger`] that reads ONLY the target `keys`, but across
/// already-open, possibly-SHARED `readers` instead of `paths` — the seam a
/// future cached-reader caller (e.g. a Flight warm-handle registry) uses to
/// avoid a fresh per-request reader-open/Index/Summary/Statistics/bloom parse.
///
/// Semantics mirror `build_single_partition_merger` exactly (same
/// canonicalization, same three-way per-candidate probe via [`probe_reader`],
/// same [`SinglePartitionFilterRun`] fail-safe, same run-index ordering) — only
/// WHO opens/owns the `SSTableReader` differs. `readers` must be the same
/// (token-pruned) candidate list, in the same order, the full-scan reader-based
/// merger ([`KWayMerger::new_from_readers`]) would merge.
/// `recording` (issue #2827) governs ONLY whether this call records one partition
/// access per requested key; it changes no rows, no ordering and no probe behaviour.
/// It is an explicit caller decision because this builder's two callers sit at
/// different levels — see [`PointAccessRecording`].
pub fn build_single_partition_merger_from_readers(
    readers: Vec<Arc<SSTableReader>>,
    keys: &[Vec<u8>],
    schema: &TableSchema,
    scan_cancel: ScanCancel,
    recording: PointAccessRecording,
) -> Result<Option<KWayMerger>> {
    schema.validate_dropped_columns()?;
    if keys.is_empty() {
        return Ok(None);
    }

    let mut canonical: Vec<Vec<u8>> = keys.to_vec();
    canonical.sort_by_key(|k| crate::util::cassandra_murmur3::cassandra_murmur3_token(k));
    canonical.dedup();
    let keys: &[Vec<u8>] = &canonical;

    let key_set: std::collections::HashSet<Vec<u8>> = keys.iter().cloned().collect();

    // Issue #2765 (LAZY registration, see the path-based builder above): occupy an
    // active-merge slot ONLY when a `NeedsScan` egress channel is actually opened;
    // an all-`Seeked` point read buffers nothing and registers no slot. The
    // snapshot is memoized so every channel in THIS merge shares ONE capacity.
    let mut egress: Option<(usize, egress_budget::ActiveMergeGuard)> = None;

    // `CallerRecords` must measure NOTHING. Measuring an extent costs an index
    // materialize plus an O(N) `Index.db` successor scan per key per generation, and
    // under `CallerRecords` the result is discarded and re-derived by the caller —
    // a per-read O(N) scan whose output is thrown away.
    let collect = recording == PointAccessRecording::Record && partition_access::enabled();
    let mut weights: Vec<partition_access::AccessWeightBuilder> = if collect {
        vec![partition_access::AccessWeightBuilder::new(); keys.len()]
    } else {
        Vec::new()
    };

    let mut runs: Vec<Box<dyn SSTableRowIterator>> = Vec::new();
    for (run_index, reader) in readers.into_iter().enumerate() {
        // Cooperative cancellation (#2264): honour a cancel BEFORE each candidate's
        // probe, so a disconnected point read does no further per-SSTable work.
        scan_cancel.check()?;

        let ProbeOutcome { probe, notes } = probe_reader(
            Arc::clone(&reader),
            keys,
            schema,
            scan_cancel.clone(),
            collect,
        )?;
        fold_size_notes(&mut weights, &notes);
        match probe {
            PathProbe::Empty => {
                // Pruned / absent for every key. No run.
            }
            PathProbe::Seeked(rows) => {
                if rows.is_empty() {
                    continue;
                }
                let mut entries: Vec<MergeEntry> = Vec::with_capacity(rows.len());
                for row in rows {
                    // Issue #2096: one merge entry decoded from `Data.db` by the
                    // SEEK path (this candidate held the target partition), same
                    // accounting as the path-based builder above.
                    crate::storage::sstable::work_counters::add_merge_run_entry_decoded();
                    entries.push(SSTableRowIteratorAdapter::build_merge_entry(
                        run_index, row, schema,
                    )?);
                }
                // See `build_single_partition_merger`'s identical sort — every
                // `SSTableRowIterator` MUST yield ascending `MergeEntry` order.
                entries.sort();
                runs.push(Box::new(VecRun {
                    entries: entries.into(),
                }));
            }
            PathProbe::NeedsScan => {
                // Fail-safe: scan this ONE shared reader's compaction stream,
                // forwarding only the target partitions — never a fresh
                // path-based open (issue #2346's whole point). `reader` (the
                // ORIGINAL, not the clone `probe_reader` consumed above) is
                // still available here.
                // Point-read fail-safe: a specific-key filter, not a range scan —
                // no token bound is pushed (issue #2412; the key set bounds it).
                // Snapshot the shared capacity on the FIRST egress channel only.
                let channel_capacity = egress.get_or_insert_with(egress_budget::begin_merge).0;
                let adapter = SSTableRowIteratorAdapter::open_from_reader(
                    reader,
                    run_index,
                    schema,
                    scan_cancel.clone(),
                    None,
                    channel_capacity,
                )?;
                runs.push(Box::new(SinglePartitionFilterRun {
                    inner: adapter,
                    keys: key_set.clone(),
                }));
            }
        }
    }

    // ONE access per LOGICAL partition read, when this call is the boundary
    // (issue #2827). Recorded even when no candidate held the key — a miss is a real
    // access, and omitting it would understate the singleton bucket.
    if collect {
        record_logical_accesses(schema, keys, weights);
    }

    if runs.is_empty() {
        // No candidate produced a run; any snapshot guard drops here.
        return Ok(None);
    }
    let merger = KWayMerger::from_row_iterators(runs, schema)?;
    // Attach the slot ONLY if a `NeedsScan` egress channel was actually opened.
    Ok(Some(match egress {
        Some((_, guard)) => merger.with_egress_slot(guard),
        None => merger,
    }))
}

/// Probe an ALREADY-OPEN `reader` for every requested key via the core seek
/// primitive (default `not(tombstones)` build) — the shared core BOTH
/// [`probe_path`] (path-based, opens its own reader then delegates here) and
/// [`build_single_partition_merger_from_readers`] (reader-based) use, so the
/// two never diverge (issue #2346).
///
/// A single SSTable's index availability is a property of the SSTable (not the
/// key), so the FIRST `IndexUnavailable` short-circuits to [`PathProbe::NeedsScan`]
/// (scan the whole file, filtered). Otherwise every present key's seeked rows are
/// concatenated (they share this SSTable's generation / run index).
#[cfg(not(feature = "tombstones"))]
async fn probe_reader_async(
    reader: &SSTableReader,
    keys: &[Vec<u8>],
    schema: &TableSchema,
    scan_cancel: &ScanCancel,
    collect_notes: bool,
) -> Result<ProbeOutcome> {
    let mut notes: Vec<KeySizeNote> = Vec::new();
    let mut rows: Vec<CompactionRow> = Vec::new();
    for key in keys {
        match reader
            .read_single_partition_for_compaction(key, Some(schema), scan_cancel)
            .await?
        {
            SinglePartitionCompaction::DefinitelyAbsent => {
                if collect_notes {
                    notes.push(KeySizeNote::NotHeld);
                }
            }
            SinglePartitionCompaction::Rows(mut r) => {
                if collect_notes {
                    notes.push(resolved_size_note(reader, key).await);
                }
                rows.append(&mut r)
            }
            // Index availability is per-SSTable, not per-key: fall back to
            // scanning the whole file once, filtered to the key set.
            SinglePartitionCompaction::IndexUnavailable => {
                return Ok(ProbeOutcome {
                    probe: PathProbe::NeedsScan,
                    // A whole-file fail-safe scan resolves no per-partition size,
                    // so EVERY key this SSTable might hold is unpriceable through
                    // it. Reported as unavailable rather than skipped, so the
                    // window's byte total is visibly incomplete instead of quietly
                    // short (issue #2827, design D6).
                    notes: if collect_notes {
                        vec![KeySizeNote::Unsized; keys.len()]
                    } else {
                        Vec::new()
                    },
                });
            }
        }
    }
    let probe = if rows.is_empty() {
        PathProbe::Empty
    } else {
        PathProbe::Seeked(rows)
    };
    Ok(ProbeOutcome { probe, notes })
}

/// This SSTable's authoritative on-disk extent for `key`.
///
/// The partition's location comes from what the read that just ran already resolved
/// into the process-global key→partition-offset cache (issue #2059's
/// `PartitionLoc`), so the instrument never re-drives a lookup and can never
/// perturb the read path's own telemetry. The EXTENT is then measured as the
/// successor gap — `[data_offset, successor_offset)`, bounding to the authoritative
/// uncompressed data-section length for the last partition — which is the same
/// authoritative index-layout bound the single-partition seek uses to size its
/// decompression window.
///
/// Nothing is ever estimated. Where no location is available at all (the key cache
/// is off, or the entry was reclaimed) or no extent is resolvable, the note is
/// [`KeySizeNote::Unsized`]: the access is still counted, and it contributes zero
/// bytes under `size_source = unavailable`.
#[cfg(not(feature = "tombstones"))]
async fn resolved_size_note(reader: &SSTableReader, key: &[u8]) -> KeySizeNote {
    let Some(loc) = reader.key_cache_get(key) else {
        return KeySizeNote::Unsized;
    };
    if loc.data_size > 0 {
        return KeySizeNote::Sized(loc.data_size);
    }
    match reader.measure_partition_extent(loc.data_offset, key).await {
        Ok(Some(gap)) => KeySizeNote::Measured(gap),
        Ok(None) | Err(_) => KeySizeNote::Unsized,
    }
}

/// Fold one candidate's per-key size notes into the per-key weight accumulators.
///
/// A no-op when the probe is disabled (both vectors are empty).
fn fold_size_notes(weights: &mut [partition_access::AccessWeightBuilder], notes: &[KeySizeNote]) {
    for (w, note) in weights.iter_mut().zip(notes.iter()) {
        match note {
            KeySizeNote::NotHeld => {}
            KeySizeNote::Sized(n) => w.note_sized(*n),
            KeySizeNote::Measured(n) => w.note_measured(*n),
            KeySizeNote::Unsized => w.note_unsized(),
        }
    }
}

/// Record ONE logical partition access per requested key (issue #2827).
///
/// A no-op when the probe is disabled — `weights` is empty then, so this does not
/// even iterate.
fn record_logical_accesses(
    schema: &TableSchema,
    keys: &[Vec<u8>],
    weights: Vec<partition_access::AccessWeightBuilder>,
) {
    // The table is part of the entry identity — see `TableScope`. The schema is the
    // authority here (the builders are driven per-table), so no formatting or
    // allocation is needed on this path.
    let scope = partition_access::TableScope::new(&schema.keyspace, &schema.table);
    for (key, weight) in keys.iter().zip(weights) {
        partition_access::record_partition_access(scope, key, weight.finish());
    }
}

/// Open `path` ONCE and probe it for every requested key, delegating to
/// [`probe_reader_async`] (issue #2346) once the reader is open.
///
/// Runs the async open + seek on the shared bridge runtime. The reader is opened
/// with buffered I/O (never mmap/direct — the file may be deleted after the
/// read), matching the full-scan producer's file-lifetime contract (issue #591).
#[cfg(not(feature = "tombstones"))]
fn probe_path(
    path: &Path,
    keys: &[Vec<u8>],
    schema: &TableSchema,
    udt_registry: Option<&UdtRegistry>,
    scan_cancel: ScanCancel,
    collect_notes: bool,
) -> Result<ProbeOutcome> {
    let path = path.to_path_buf();
    let schema = schema.clone();
    let keys: Vec<Vec<u8>> = keys.to_vec();
    let udt_registry = udt_registry.cloned();
    block_on_async(async move {
        let mut config = Config::default();
        config.storage.use_mmap = false;
        config.storage.disk_access_mode = DiskAccessMode::Buffered;
        let platform = Arc::new(Platform::new(&config).await?);
        let mut reader = SSTableReader::open(&path, &config, platform).await?;
        // Issue #2349: decode a `frozen<UDT>`-in-collection cell structurally on the
        // point-read seek path too, matching the full-scan cold reader.
        if let Some(registry) = udt_registry {
            reader.set_udt_registry(registry);
        }
        probe_reader_async(&reader, &keys, &schema, &scan_cancel, collect_notes).await
    })
}

/// Probe an ALREADY-OPEN, possibly-SHARED `reader` (issue #2346) — the
/// reader-based analogue of [`probe_path`], used by
/// [`build_single_partition_merger_from_readers`]. `reader` is an `Arc` clone
/// (the caller keeps the original for the `NeedsScan` fail-safe path), moved
/// into the bridged future so no borrow needs to outlive this call.
#[cfg(not(feature = "tombstones"))]
fn probe_reader(
    reader: Arc<SSTableReader>,
    keys: &[Vec<u8>],
    schema: &TableSchema,
    scan_cancel: ScanCancel,
    collect_notes: bool,
) -> Result<ProbeOutcome> {
    let schema = schema.clone();
    let keys: Vec<Vec<u8>> = keys.to_vec();
    block_on_async(async move {
        probe_reader_async(&reader, &keys, &schema, &scan_cancel, collect_notes).await
    })
}

/// Tombstones-build fallback: the single-partition SEEK machinery
/// (`successor_partition_offset`, `point_read_whole_section`, …) is
/// `not(tombstones)` only, so under the alternate `tombstones` feature every
/// candidate degrades to a full scan filtered to the key set — correct, just
/// without the seek speedup. Keeps the public builder API identical across builds.
#[cfg(feature = "tombstones")]
fn probe_path(
    _path: &Path,
    _keys: &[Vec<u8>],
    _schema: &TableSchema,
    _udt_registry: Option<&UdtRegistry>,
    _scan_cancel: ScanCancel,
    _collect_notes: bool,
) -> Result<ProbeOutcome> {
    Ok(ProbeOutcome {
        probe: PathProbe::NeedsScan,
        notes: Vec::new(),
    })
}

/// Tombstones-build fallback for the reader-based probe (issue #2346) — mirrors
/// [`probe_path`]'s `tombstones` fallback above.
#[cfg(feature = "tombstones")]
fn probe_reader(
    _reader: Arc<SSTableReader>,
    _keys: &[Vec<u8>],
    _schema: &TableSchema,
    _scan_cancel: ScanCancel,
    _collect_notes: bool,
) -> Result<ProbeOutcome> {
    Ok(ProbeOutcome {
        probe: PathProbe::NeedsScan,
        notes: Vec::new(),
    })
}
