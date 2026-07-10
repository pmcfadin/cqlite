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

use super::{KWayMerger, MergeEntry, RunReader, SSTableRowIterator, SSTableRowIteratorAdapter};
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::CompactionRow;
use crate::{Error, Result};
use std::collections::{BinaryHeap, VecDeque};
use std::path::{Path, PathBuf};

// Seek-only imports (the point-read primitive is `not(tombstones)` gated, like the
// underlying single-partition seek machinery it composes).
#[cfg(not(feature = "tombstones"))]
use super::block_on_async;
#[cfg(not(feature = "tombstones"))]
use crate::config::DiskAccessMode;
#[cfg(not(feature = "tombstones"))]
use crate::platform::Platform;
#[cfg(not(feature = "tombstones"))]
use crate::storage::sstable::reader::{SSTableReader, SinglePartitionCompaction};
#[cfg(not(feature = "tombstones"))]
use crate::Config;
#[cfg(not(feature = "tombstones"))]
use std::sync::Arc;

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
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
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
/// produces). `scan_cancel` is wired onto every opened reader so a client
/// disconnect abandons an in-flight fail-safe scan promptly (#2264).
pub fn build_single_partition_merger(
    paths: Vec<PathBuf>,
    keys: &[Vec<u8>],
    schema: &TableSchema,
    scan_cancel: ScanCancel,
) -> Result<Option<KWayMerger>> {
    schema.validate_dropped_columns()?;
    if keys.is_empty() {
        return Ok(None);
    }
    let key_set: std::collections::HashSet<Vec<u8>> = keys.iter().cloned().collect();

    let mut runs: Vec<Box<dyn SSTableRowIterator>> = Vec::new();
    for (run_index, path) in paths.iter().enumerate() {
        // Cooperative cancellation (#2264): honour a cancel BEFORE each candidate's
        // seek/open, so a disconnected point read does no further per-SSTable work.
        scan_cancel.check()?;

        match probe_path(path, keys, schema, scan_cancel.clone())? {
            PathProbe::Empty => {
                // Pruned / absent for every key. No run.
            }
            PathProbe::Seeked(rows) => {
                if rows.is_empty() {
                    continue;
                }
                let mut entries = VecDeque::with_capacity(rows.len());
                for row in rows {
                    entries.push_back(SSTableRowIteratorAdapter::build_merge_entry(
                        run_index, row, schema,
                    )?);
                }
                runs.push(Box::new(VecRun { entries }));
            }
            PathProbe::NeedsScan => {
                // Fail-safe: scan this ONE SSTable, forwarding only the target
                // partitions. The missing index costs speed, never correctness.
                let adapter = SSTableRowIteratorAdapter::open(
                    path,
                    run_index,
                    schema,
                    None,
                    scan_cancel.clone(),
                )?;
                runs.push(Box::new(SinglePartitionFilterRun {
                    inner: adapter,
                    keys: key_set.clone(),
                }));
            }
        }
    }

    if runs.is_empty() {
        return Ok(None);
    }
    Ok(Some(KWayMerger::from_row_iterators(runs, schema)?))
}

/// Open `path` ONCE and probe it for every requested key via the core seek
/// primitive, combining the outcome (default `not(tombstones)` build).
///
/// A single SSTable's index availability is a property of the SSTable (not the
/// key), so the FIRST `IndexUnavailable` short-circuits to [`PathProbe::NeedsScan`]
/// (scan the whole file, filtered). Otherwise every present key's seeked rows are
/// concatenated (they share this SSTable's generation / run index).
///
/// Runs the async open + seek on the shared bridge runtime. The reader is opened
/// with buffered I/O (never mmap/direct — the file may be deleted after the read)
/// and the cooperative cancel token wired in, matching the full-scan producer.
#[cfg(not(feature = "tombstones"))]
fn probe_path(
    path: &Path,
    keys: &[Vec<u8>],
    schema: &TableSchema,
    scan_cancel: ScanCancel,
) -> Result<PathProbe> {
    let path = path.to_path_buf();
    let schema = schema.clone();
    let keys: Vec<Vec<u8>> = keys.to_vec();
    block_on_async(async move {
        let mut config = Config::default();
        config.storage.use_mmap = false;
        config.storage.disk_access_mode = DiskAccessMode::Buffered;
        let platform = Arc::new(Platform::new(&config).await?);
        let mut reader = SSTableReader::open(&path, &config, platform).await?;
        reader.set_scan_cancel(scan_cancel);

        let mut rows: Vec<CompactionRow> = Vec::new();
        for key in &keys {
            match reader
                .read_single_partition_for_compaction(key, Some(&schema))
                .await?
            {
                SinglePartitionCompaction::DefinitelyAbsent => {}
                SinglePartitionCompaction::Rows(mut r) => rows.append(&mut r),
                // Index availability is per-SSTable, not per-key: fall back to
                // scanning the whole file once, filtered to the key set.
                SinglePartitionCompaction::IndexUnavailable => {
                    return Ok(PathProbe::NeedsScan);
                }
            }
        }
        if rows.is_empty() {
            Ok(PathProbe::Empty)
        } else {
            Ok(PathProbe::Seeked(rows))
        }
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
    _scan_cancel: ScanCancel,
) -> Result<PathProbe> {
    Ok(PathProbe::NeedsScan)
}
