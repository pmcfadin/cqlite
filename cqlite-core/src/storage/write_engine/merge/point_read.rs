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
use crate::storage::sstable::reader::{SSTableReader, SinglePartitionCompaction};
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

/// A run that forwards ONLY the target partition's entries from an SSTable's full
/// compaction stream — the index-unavailable fail-safe. Byte-identical to the
/// full scan's contribution for this partition (same stream, same decode), just
/// with the other partitions dropped before the merge sees them.
struct SinglePartitionFilterRun {
    inner: SSTableRowIteratorAdapter,
    key: Vec<u8>,
}

impl SSTableRowIterator for SinglePartitionFilterRun {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        loop {
            match self.inner.next() {
                Some(Ok(entry)) => {
                    if entry.key.key == self.key {
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

/// Assemble a [`KWayMerger`] that reads ONLY `partition_key` across the candidate
/// `paths` (issue #2207).
///
/// Returns `Ok(None)` when every candidate is a definite presence-oracle negative
/// (no SSTable holds the key) — the caller streams zero rows. Otherwise returns a
/// merger the caller drives through its existing reconciliation loop.
///
/// `paths` must be the same (token-pruned) candidate list, in the same order, the
/// full-scan path would merge — run index is the position here, so LWW tie-breaks
/// match. `scan_cancel` is wired onto every opened reader so a client disconnect
/// abandons an in-flight fail-safe scan promptly (#2264).
pub fn build_single_partition_merger(
    paths: Vec<PathBuf>,
    partition_key: &[u8],
    schema: &TableSchema,
    scan_cancel: ScanCancel,
) -> Result<Option<KWayMerger>> {
    schema.validate_dropped_columns()?;

    let mut runs: Vec<Box<dyn SSTableRowIterator>> = Vec::new();
    for (run_index, path) in paths.iter().enumerate() {
        // Cooperative cancellation (#2264): honour a cancel BEFORE each candidate's
        // seek/open, so a disconnected point read does no further per-SSTable work.
        scan_cancel.check()?;

        let outcome = probe_single_partition(path, partition_key, schema, scan_cancel.clone())?;
        match outcome {
            SinglePartitionCompaction::DefinitelyAbsent => {
                // Pruned by the presence oracle (already counted). No run.
            }
            SinglePartitionCompaction::Rows(rows) => {
                if rows.is_empty() {
                    // Authoritative empty (prefix-collision for an absent key): the
                    // partition is not in this SSTable. No run.
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
            SinglePartitionCompaction::IndexUnavailable => {
                // Fail-safe: scan this ONE SSTable, forwarding only the target
                // partition. The missing index costs speed, never correctness.
                let adapter =
                    SSTableRowIteratorAdapter::open(path, run_index, schema, None, scan_cancel.clone())?;
                runs.push(Box::new(SinglePartitionFilterRun {
                    inner: adapter,
                    key: partition_key.to_vec(),
                }));
            }
        }
    }

    if runs.is_empty() {
        return Ok(None);
    }
    Ok(Some(KWayMerger::from_row_iterators(runs, schema)?))
}

/// Open `path` and probe it for `partition_key` via the core seek primitive.
///
/// Runs the async open + seek on the shared bridge runtime. The reader is opened
/// with buffered I/O (never mmap/direct — the file may be deleted after the read)
/// and the cooperative cancel token wired in, matching the full-scan producer.
fn probe_single_partition(
    path: &Path,
    partition_key: &[u8],
    schema: &TableSchema,
    scan_cancel: ScanCancel,
) -> Result<SinglePartitionCompaction> {
    let path = path.to_path_buf();
    let schema = schema.clone();
    let key = partition_key.to_vec();
    block_on_async(async move {
        let mut config = Config::default();
        config.storage.use_mmap = false;
        config.storage.disk_access_mode = DiskAccessMode::Buffered;
        let platform = Arc::new(Platform::new(&config).await?);
        let mut reader = SSTableReader::open(&path, &config, platform).await?;
        reader.set_scan_cancel(scan_cancel);
        reader
            .read_single_partition_for_compaction(&key, Some(&schema))
            .await
    })
}
