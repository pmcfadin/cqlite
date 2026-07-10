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

use super::{block_on_async, KWayMerger, MergeEntry, SSTableRowIterator, SSTableRowIteratorAdapter};
use crate::config::DiskAccessMode;
use crate::platform::Platform;
use crate::schema::TableSchema;
use crate::storage::sstable::reader::{
    CompactionRow, SSTableReader, SinglePartitionCompaction,
};
use crate::storage::scan_cancel::ScanCancel;
use crate::{Config, Result};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A run backed by a pre-built `Vec` of merge entries — the seeked single
/// partition. Yields each entry once, in order, then reports exhaustion.
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

/// Per-candidate probe outcome across ALL requested keys.
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
/// primitive, combining the outcome.
///
/// A single SSTable's index availability is a property of the SSTable (not the
/// key), so the FIRST `IndexUnavailable` short-circuits to [`PathProbe::NeedsScan`]
/// (scan the whole file, filtered). Otherwise every present key's seeked rows are
/// concatenated (they share this SSTable's generation / run index).
///
/// Runs the async open + seek on the shared bridge runtime. The reader is opened
/// with buffered I/O (never mmap/direct — the file may be deleted after the read)
/// and the cooperative cancel token wired in, matching the full-scan producer.
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
