//! Crash-durability barrier for the flush -> WAL-truncate handoff (issue #1392).
//!
//! When a memtable flush finishes, the same data lives in two places: the newly
//! written SSTable components on disk and the still-intact WAL. Truncating the
//! WAL discards the WAL copy, so it is only safe *after* the SSTable is truly
//! durable.
//!
//! Fsyncing each component file (Data.db, Filter.db, ...) persists the file
//! *contents*, but on POSIX filesystems it does **not** persist the parent
//! directory's entries. After a power loss the freshly created dirents can be
//! missing even though the file contents were flushed — leaving the data in
//! neither a reachable SSTable nor the (already truncated) WAL.
//!
//! This module closes that gap: on every WAL-truncating flush it fsyncs the
//! **full ancestor chain** from the SSTable's leaf directory up to (and
//! including) the configured data root, deepest-first, *after* the component
//! files are synced and *before* the WAL is truncated. It also makes the
//! truncate-failure path explicit (leave the WAL intact as a durable replay
//! marker) rather than silently swallowing the error.
//!
//! Syncing the whole leaf→data-root chain unconditionally — rather than only
//! the directories a *given* flush attempt happened to create — closes a
//! retry/partial-failure hazard (issue #1392): if an earlier attempt created
//! `data_dir/ks/table` but crashed before its full directory-sync chain
//! completed, the retry sees those dirs already present, so a
//! "sync-only-what-I-created" barrier would sync just the leaf and truncate the
//! WAL while the keyspace/table dirents were still not durable. Directory fsync
//! is cheap and idempotent, so re-syncing an already-durable dir is harmless
//! and removes the hazard entirely.
//!
//! The [`DurabilityBarrier`] trait is a test seam: production uses
//! [`RealDurabilityBarrier`], while tests inject fakes that record the ordering
//! of filesystem operations or fault a specific step.

use crate::error::{Error, Result};
use crate::storage::write_engine::wal::{TruncateError, WriteAheadLog};
use std::path::{Path, PathBuf};

/// Outcome of a successful [`finalize_flush_durability`] call.
///
/// In every variant the SSTable has already been fully written and published
/// (its components' contents are fsynced and its directory entries persisted).
/// The variant tells the caller how to reconcile flush state (issue #1392):
///
/// * [`FlushDurabilityOutcome::Durable`] — the WAL was truncated, or was left
///   intact but is still a valid replay marker. The flush is clean; the caller
///   commits state (clears the memtable, advances the generation) and returns
///   success.
/// * [`FlushDurabilityOutcome::WalTruncateFailedAfterCommit`] — the WAL truncate
///   failed *after* `set_len(0)` had already zeroed the WAL. The WAL is no
///   longer replayable, so the published SSTable is the **only** durable copy
///   of the flushed mutations. The caller MUST still commit flush state (clear
///   the memtable and advance the generation) so a retry writes a NEW
///   generation instead of overwriting the published SSTable, and then surface
///   the carried error. Returning the error *without* committing state would
///   let a retry reuse this generation and `File::create` would overwrite the
///   sole durable copy — a crash mid-retry would then lose the data entirely.
#[derive(Debug)]
pub(crate) enum FlushDurabilityOutcome {
    /// The handoff completed cleanly; the WAL is truncated or still replayable.
    Durable,
    /// The SSTable is durably published but the WAL truncate failed after
    /// zeroing the WAL. State MUST be committed before the error is surfaced.
    WalTruncateFailedAfterCommit(Error),
}

/// Seam over the two durability-critical operations performed at the end of a
/// flush. Splitting them behind a trait lets tests assert their ordering and
/// inject faults without touching the real filesystem.
pub(crate) trait DurabilityBarrier {
    /// Fsync `dir` so that directory entries created during the flush (the new
    /// SSTable's component files) are persisted.
    fn sync_dir(&self, dir: &Path) -> Result<()>;

    /// Truncate the WAL now that the SSTable is durable.
    ///
    /// Returns [`TruncateError`] on failure so the caller can distinguish a
    /// failure that leaves the WAL intact ([`TruncateError::BeforeMutation`])
    /// from one that has already zeroed it
    /// ([`TruncateError::AfterMutation`], issue #1392).
    fn truncate_wal(&self, wal: &mut WriteAheadLog) -> std::result::Result<(), TruncateError>;
}

/// Production barrier: performs real directory fsyncs and WAL truncation.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct RealDurabilityBarrier;

impl DurabilityBarrier for RealDurabilityBarrier {
    fn sync_dir(&self, dir: &Path) -> Result<()> {
        sync_directory(dir)
    }

    fn truncate_wal(&self, wal: &mut WriteAheadLog) -> std::result::Result<(), TruncateError> {
        wal.truncate_checked()
    }
}

/// Make a directory's entries durable.
///
/// On Unix this opens the directory and calls `fsync` on the resulting
/// descriptor, which is the portable POSIX way to persist newly created
/// dirents. On non-Unix targets directory fsync is not available the same way;
/// the per-file fsyncs performed by the component writers remain the durability
/// guarantee there, so this is a documented no-op.
#[cfg(unix)]
fn sync_directory(dir: &Path) -> Result<()> {
    let handle = std::fs::File::open(dir).map_err(|e| {
        Error::Storage(format!(
            "Failed to open data directory {} for fsync: {e}",
            dir.display()
        ))
    })?;
    handle.sync_all().map_err(|e| {
        Error::Storage(format!(
            "Failed to fsync data directory {}: {e}",
            dir.display()
        ))
    })
}

#[cfg(not(unix))]
fn sync_directory(_dir: &Path) -> Result<()> {
    Ok(())
}

/// Like [`std::fs::create_dir_all`], but maps failures to [`Error::Storage`]
/// and treats a raced/pre-existing directory as success (issue #1392).
///
/// Directory *creation* no longer needs to record which ancestors it
/// materialized: the flush durability barrier fsyncs the entire leaf→data-root
/// chain unconditionally (see [`dirs_to_sync`]), so there is nothing to track.
pub(crate) fn create_dir_all(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    match std::fs::create_dir(path) {
        Ok(()) => Ok(()),
        // Raced/pre-existing: another actor already made it, or it existed.
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => Ok(()),
        // Parent missing: create ancestors first, then retry.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            match path.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => {
                    create_dir_all(parent)?;
                }
                _ => {
                    return Err(Error::Storage(format!(
                        "Failed to create directory {}: {e}",
                        path.display()
                    )));
                }
            }
            match std::fs::create_dir(path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => Ok(()),
                Err(e) => Err(Error::Storage(format!(
                    "Failed to create directory {}: {e}",
                    path.display()
                ))),
            }
        }
        Err(e) => Err(Error::Storage(format!(
            "Failed to create directory {}: {e}",
            path.display()
        ))),
    }
}

/// Compute the ordered list of directories to fsync so that every dirent along
/// the newly written SSTable's path is persisted before the WAL is truncated
/// (issue #1392).
///
/// Walks from the leaf SSTable directory up to (and including) `data_root`,
/// deepest-first. This is the whole ancestor chain that could hold a
/// not-yet-durable dirent for the SSTable — the leaf (its component dirents),
/// the table directory (the leaf's own entry), the keyspace directory (the
/// table's entry), and the data root (the keyspace's entry).
///
/// Syncing the FULL chain on every flush — regardless of which dirs this
/// particular attempt created — is deliberate (issue #1392): a retry after a
/// partially completed earlier attempt sees the ancestors already present, so a
/// "sync-only-what-I-created" barrier would skip them and truncate the WAL
/// while they were still not durable. Directory fsync is idempotent, so
/// re-syncing an already-durable dir is harmless.
///
/// The result is ordered deepest-first so each child directory is synced before
/// its parent — matching the POSIX convention that a parent's fsync only needs
/// to follow the creation (and here the sync) of the child it references. The
/// walk never ascends above `data_root`: it stops as soon as it reaches it, and
/// defensively stops if `leaf` is not under `data_root` at all.
fn dirs_to_sync(leaf: &Path, data_root: &Path) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut cur = leaf;
    loop {
        dirs.push(cur.to_path_buf());
        if cur == data_root {
            break;
        }
        match cur.parent() {
            // Only ascend while we stay within the configured data root; never
            // fsync directories above it.
            Some(parent) if !parent.as_os_str().is_empty() && parent.starts_with(data_root) => {
                cur = parent;
            }
            _ => break,
        }
    }
    dirs
}

/// Complete the durability handoff at the end of a flush.
///
/// `data_path` is the SSTable's Data.db path; its parent directory is the leaf
/// SSTable directory (falling back to `data_dir` if `data_path` has no parent).
/// `data_dir` is the write engine's configured data root. Ordering is
/// load-bearing (issue #1392):
///
/// 1. fsync the **full ancestor chain** from the leaf SSTable directory up to
///    (and including) `data_dir`, deepest-first, so every directory entry along
///    the new SSTable's path is durable.
/// 2. Only then truncate the WAL — the WAL copy of the data is discarded here.
///
/// The full chain is synced on EVERY flush, not just the directories this
/// attempt created (see [`dirs_to_sync`]). Fsyncing only the leaf — or only the
/// dirs a given attempt created — would persist the component dirents but could
/// leave the leaf's (or keyspace's) own entry in its parent unsynced, so a
/// crash after the WAL truncate could lose the entire new SSTable directory.
/// This is especially dangerous on a RETRY after a partially completed earlier
/// attempt: the ancestors already exist, so a "created this attempt" list would
/// be empty and only the leaf would be synced. Directory fsync is cheap and
/// idempotent, so re-syncing already-durable ancestors is harmless.
///
/// If any directory fsync fails, the error is propagated and the WAL is
/// **not** truncated, so recovery can still replay from the WAL.
///
/// The WAL truncate is phase-aware (issue #1392):
///
/// * [`TruncateError::BeforeMutation`] — the truncate failed without touching
///   the WAL contents, so the WAL is still intact. The error is surfaced as a
///   warning and the WAL is deliberately left in place: it remains a durable
///   replay marker, and replay is idempotent (identical mutations reconcile by
///   last-write-wins timestamp), so this degrades to a redundant re-flush
///   rather than data loss.
/// * [`TruncateError::AfterMutation`] — the truncate failed *after* `set_len(0)`
///   already zeroed the WAL. The WAL is **no longer** a replayable marker, so
///   this returns [`FlushDurabilityOutcome::WalTruncateFailedAfterCommit`]
///   carrying the error. The published SSTable is now the ONLY durable copy of
///   the flushed data, so the caller must treat the flush as COMMITTED for
///   state purposes (clear the memtable and advance the generation) BEFORE
///   surfacing the error — otherwise a retry would reuse this generation and
///   `File::create` would overwrite the sole durable copy, losing data on a
///   crash mid-retry (issue #1392).
///
/// Every emitted SSTable component's *contents* must already be fsynced by the
/// component writers (see [`crate::storage::sstable::writer::SSTableWriter::finish`])
/// before this function runs. Ordering is load-bearing: component-content
/// fsyncs (inside `writer.finish()`) happen first, then this function fsyncs
/// the directory, then it truncates the WAL.
pub(crate) fn finalize_flush_durability(
    barrier: &dyn DurabilityBarrier,
    data_path: &Path,
    data_dir: &Path,
    wal: &mut WriteAheadLog,
) -> Result<FlushDurabilityOutcome> {
    // Step 1: persist the new SSTable's dirents BEFORE dropping the WAL copy.
    // (The component *contents* were already fsynced inside `writer.finish()`.)
    // This fsyncs the FULL ancestor chain from the leaf SSTable directory up to
    // (and including) the data root, deepest-first, so every directory entry
    // along the SSTable's path is durable — not just the leaf, and regardless
    // of whether an earlier flush attempt already created some ancestors
    // (issue #1392).
    let sstable_dir = data_path.parent().unwrap_or(data_dir);
    for dir in dirs_to_sync(sstable_dir, data_dir) {
        barrier.sync_dir(&dir)?;
    }

    // Step 2: it is now durable-safe to truncate the WAL.
    match barrier.truncate_wal(wal) {
        Ok(()) => Ok(FlushDurabilityOutcome::Durable),
        Err(TruncateError::BeforeMutation(e)) => {
            // The WAL was never mutated; it is still a valid replay marker.
            log::warn!(
                "WAL truncate failed before mutating the WAL ({e}). Leaving the \
                 WAL intact as a durable replay marker; the next startup will \
                 replay it idempotently (last-write-wins), so no data is lost."
            );
            Ok(FlushDurabilityOutcome::Durable)
        }
        Err(TruncateError::AfterMutation(e)) => {
            // The WAL was already zeroed by set_len(0) before the failure, so it
            // is NO LONGER a replayable marker. The published SSTable is now the
            // ONLY durable copy of these mutations. Report the flush as COMMITTED
            // (carrying the error) so the caller advances the generation and
            // clears the memtable BEFORE surfacing the failure — a retry must
            // write a NEW generation rather than overwrite the published SSTable
            // (which `File::create` would do at the reused generation), which
            // would lose the data on a crash mid-retry (issue #1392).
            Ok(FlushDurabilityOutcome::WalTruncateFailedAfterCommit(
                Error::Storage(format!(
                    "WAL truncate failed AFTER zeroing the WAL ({e}); the WAL is \
                     no longer a replayable recovery marker. The flushed SSTable \
                     is durable and the generation has been advanced so a retry \
                     will not overwrite it."
                )),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
    use crate::types::Value;
    use std::cell::RefCell;
    use tempfile::TempDir;

    /// A single durability-relevant filesystem operation, recorded by the test
    /// seam to assert ordering (the directory fsync MUST precede the WAL
    /// truncate).
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum FsOp {
        /// fsync of a directory whose dirents changed during the flush (the leaf
        /// SSTable directory and/or a newly created ancestor). Carries the path
        /// so tests can assert *which* directories were synced and in what order.
        SyncDir(PathBuf),
        /// Truncation of the WAL to zero length.
        WalTruncate,
    }

    fn test_mutation(id: i32, name: &str) -> Mutation {
        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(id));
        let ops = vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }];
        Mutation::new(table_id, pk, None, ops, 1234567890, None)
    }

    /// Records the order of operations; `sync_dir` optionally faults.
    struct RecordingBarrier {
        ops: RefCell<Vec<FsOp>>,
        fail_dir_sync: bool,
        /// Fault the truncate BEFORE mutating the WAL (WAL stays intact).
        fail_truncate_before_mutation: bool,
        /// Fault the truncate AFTER `set_len(0)` has already zeroed the WAL.
        fail_truncate_after_mutation: bool,
    }

    impl RecordingBarrier {
        fn new() -> Self {
            Self {
                ops: RefCell::new(Vec::new()),
                fail_dir_sync: false,
                fail_truncate_before_mutation: false,
                fail_truncate_after_mutation: false,
            }
        }
    }

    impl DurabilityBarrier for RecordingBarrier {
        fn sync_dir(&self, dir: &Path) -> Result<()> {
            if self.fail_dir_sync {
                return Err(Error::Storage("injected dir fsync fault".to_string()));
            }
            self.ops.borrow_mut().push(FsOp::SyncDir(dir.to_path_buf()));
            Ok(())
        }

        fn truncate_wal(&self, wal: &mut WriteAheadLog) -> std::result::Result<(), TruncateError> {
            if self.fail_truncate_before_mutation {
                // WAL contents untouched.
                return Err(TruncateError::BeforeMutation(Error::Storage(
                    "injected pre-mutation truncate fault".to_string(),
                )));
            }
            if self.fail_truncate_after_mutation {
                // Model a failure that occurs AFTER set_len(0): actually zero the
                // WAL (mutating it), then report an after-mutation fault. The WAL
                // is now empty and is no longer a replayable marker.
                wal.truncate().map_err(TruncateError::AfterMutation)?;
                return Err(TruncateError::AfterMutation(Error::Storage(
                    "injected post-set_len truncate fault".to_string(),
                )));
            }
            self.ops.borrow_mut().push(FsOp::WalTruncate);
            wal.truncate().map_err(TruncateError::BeforeMutation)
        }
    }

    // Criterion 1: directory fsync happens BEFORE the WAL truncate.
    #[test]
    fn dir_fsync_precedes_wal_truncate() {
        let dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(dir.path()).unwrap();
        wal.append(&test_mutation(1, "Alice")).unwrap();
        wal.sync().unwrap();

        let barrier = RecordingBarrier::new();
        let outcome = finalize_flush_durability(
            &barrier,
            &dir.path().join("nb-1-big-Data.db"),
            dir.path(),
            &mut wal,
        )
        .unwrap();
        assert!(matches!(outcome, FlushDurabilityOutcome::Durable));

        assert_eq!(
            barrier.ops.into_inner(),
            vec![FsOp::SyncDir(dir.path().to_path_buf()), FsOp::WalTruncate],
            "directory fsync must be recorded before the WAL truncate"
        );
        // Truncate succeeded: WAL is now empty.
        assert_eq!(wal.replay().unwrap().mutations.len(), 0);
    }

    // Criterion 3: a faulted directory fsync leaves the WAL untruncated.
    #[test]
    fn faulted_dir_fsync_leaves_wal_intact() {
        let dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(dir.path()).unwrap();
        wal.append(&test_mutation(7, "Bob")).unwrap();
        wal.sync().unwrap();

        let mut barrier = RecordingBarrier::new();
        barrier.fail_dir_sync = true;

        let err = finalize_flush_durability(
            &barrier,
            &dir.path().join("nb-1-big-Data.db"),
            dir.path(),
            &mut wal,
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Storage(_)),
            "faulted dir fsync must surface as an error"
        );
        // The WAL was never touched: the truncate op was not recorded and the
        // entry is still replayable.
        assert!(barrier.ops.into_inner().is_empty());
        let replayed = wal.replay().unwrap().mutations;
        assert_eq!(
            replayed.len(),
            1,
            "WAL must remain untruncated on dir-fsync fault"
        );
    }

    // Criterion 2: a faulted WAL truncate does not fail the flush, leaves the
    // WAL intact as a durable replay marker, and the entry is still replayable
    // (idempotent recovery, not loss).
    #[test]
    fn faulted_truncate_leaves_replayable_wal() {
        let dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(dir.path()).unwrap();
        wal.append(&test_mutation(9, "Carol")).unwrap();
        wal.sync().unwrap();

        let mut barrier = RecordingBarrier::new();
        barrier.fail_truncate_before_mutation = true;

        // The flush-finalize does NOT fail even though truncate faulted.
        let outcome = finalize_flush_durability(
            &barrier,
            &dir.path().join("nb-1-big-Data.db"),
            dir.path(),
            &mut wal,
        )
        .expect("truncate fault must not fail the flush");
        assert!(
            matches!(outcome, FlushDurabilityOutcome::Durable),
            "a before-mutation truncate fault leaves a replayable WAL: still Durable"
        );

        // The dir was synced but the truncate never zeroed the WAL.
        assert_eq!(
            barrier.ops.into_inner(),
            vec![FsOp::SyncDir(dir.path().to_path_buf())]
        );

        // Recovery: the mutation is still in the WAL and replays idempotently.
        let replayed = wal.replay().unwrap().mutations;
        assert_eq!(
            replayed.len(),
            1,
            "failed truncate must leave the WAL intact for replay, not lose data"
        );
        assert_eq!(replayed[0].table.keyspace, "test_ks");
    }

    // Issue #1392: a WAL truncate that fails AFTER `set_len(0)` has already
    // zeroed the WAL must NOT be swallowed as "WAL intact". The WAL is no longer
    // a replayable marker, so finalize returns
    // `WalTruncateFailedAfterCommit(err)` — the SSTable is committed (so the
    // caller advances the generation and clears the memtable) but the error is
    // carried so it is still surfaced to the caller.
    #[test]
    fn faulted_truncate_after_mutation_reports_committed_with_error() {
        let dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(dir.path()).unwrap();
        wal.append(&test_mutation(11, "Dave")).unwrap();
        wal.sync().unwrap();

        let mut barrier = RecordingBarrier::new();
        barrier.fail_truncate_after_mutation = true;

        let outcome = finalize_flush_durability(
            &barrier,
            &dir.path().join("nb-1-big-Data.db"),
            dir.path(),
            &mut wal,
        )
        .expect("a post-mutation truncate failure is COMMITTED, not a hard error");
        let err = match outcome {
            FlushDurabilityOutcome::WalTruncateFailedAfterCommit(e) => e,
            FlushDurabilityOutcome::Durable => {
                panic!("post-mutation truncate failure must not report Durable")
            }
        };
        assert!(
            matches!(err, Error::Storage(msg) if msg.contains("no longer a replayable recovery marker")),
            "post-mutation truncate failure must carry a storage error"
        );

        // The dir was synced (recorded); the WAL was mutated (zeroed) so it is
        // NOT a replay marker anymore — this is precisely why the error must be
        // propagated rather than swallowed.
        assert_eq!(
            barrier.ops.into_inner(),
            vec![FsOp::SyncDir(dir.path().to_path_buf())]
        );
        let replayed = wal.replay().unwrap().mutations;
        assert_eq!(
            replayed.len(),
            0,
            "the WAL was zeroed by set_len(0); it is no longer replayable, which \
             is why finalize must NOT report success"
        );
    }

    // The production barrier really fsyncs an existing directory without error.
    #[test]
    fn real_barrier_syncs_existing_directory() {
        let dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(dir.path()).unwrap();
        wal.append(&test_mutation(1, "Alice")).unwrap();
        wal.sync().unwrap();

        finalize_flush_durability(
            &RealDurabilityBarrier,
            &dir.path().join("nb-1-big-Data.db"),
            dir.path(),
            &mut wal,
        )
        .unwrap();
        assert_eq!(wal.replay().unwrap().mutations.len(), 0);
    }

    // Issue #1392 (FINDING 1): on the FIRST flush into a brand-new
    // keyspace/table path, the barrier must fsync the leaf AND every ancestor up
    // to the data root — deepest first — BEFORE the WAL truncate, so a crash
    // cannot lose the freshly created directory tree.
    #[test]
    fn first_flush_fsyncs_new_ancestor_dirs_before_truncate() {
        let root = TempDir::new().unwrap();
        let data_dir = root.path();
        // WAL lives at the data_dir root (as in a real engine layout).
        let mut wal = WriteAheadLog::create(data_dir).unwrap();
        wal.append(&test_mutation(1, "Alice")).unwrap();
        wal.sync().unwrap();

        // Leaf SSTable dir: data_dir/test_ks/test_table (both ancestors new).
        let ks_dir = data_dir.join("test_ks");
        let leaf_dir = ks_dir.join("test_table");
        let data_path = leaf_dir.join("nb-1-big-Data.db");

        let barrier = RecordingBarrier::new();
        finalize_flush_durability(&barrier, &data_path, data_dir, &mut wal).unwrap();

        // Expected: fsync the leaf (its component dirents), then the keyspace dir
        // (persists the leaf's own entry), then the data_dir (persists the
        // keyspace's own entry) — deepest-first — and only THEN truncate the WAL.
        assert_eq!(
            barrier.ops.into_inner(),
            vec![
                FsOp::SyncDir(leaf_dir),
                FsOp::SyncDir(ks_dir),
                FsOp::SyncDir(data_dir.to_path_buf()),
                FsOp::WalTruncate,
            ],
            "every ancestor dir up to the data root must be fsynced (deepest \
             first) before the WAL truncate"
        );
        assert_eq!(wal.replay().unwrap().mutations.len(), 0);
    }

    // Issue #1392 (roborev r6): the whole class of retry/partial-failure loss.
    // An earlier flush attempt already created data_dir/ks/table (so a
    // "sync-only-what-I-created" barrier would have an EMPTY created set), then
    // crashed before completing its directory-sync chain. On the RETRY the
    // barrier must STILL fsync the full leaf→data-root chain (leaf, table dir,
    // keyspace dir, data root) before truncating the WAL — proving the fix syncs
    // the whole chain unconditionally rather than only newly created dirs.
    #[test]
    fn retry_with_preexisting_dirs_still_fsyncs_full_chain_before_truncate() {
        let root = TempDir::new().unwrap();
        let data_dir = root.path();
        let mut wal = WriteAheadLog::create(data_dir).unwrap();
        wal.append(&test_mutation(1, "Alice")).unwrap();
        wal.sync().unwrap();

        // Pre-create the whole tree, exactly as a failed earlier attempt would
        // leave it. `create_dir_all` no longer tracks anything, so there is no
        // "created this attempt" set to fall back on.
        let ks_dir = data_dir.join("test_ks");
        let leaf_dir = ks_dir.join("test_table");
        create_dir_all(&leaf_dir).unwrap();
        assert!(leaf_dir.exists());
        let data_path = leaf_dir.join("nb-1-big-Data.db");

        let barrier = RecordingBarrier::new();
        finalize_flush_durability(&barrier, &data_path, data_dir, &mut wal).unwrap();

        // The full chain is synced deepest-first even though nothing was created
        // this attempt — and only THEN is the WAL truncated.
        assert_eq!(
            barrier.ops.into_inner(),
            vec![
                FsOp::SyncDir(leaf_dir),
                FsOp::SyncDir(ks_dir),
                FsOp::SyncDir(data_dir.to_path_buf()),
                FsOp::WalTruncate,
            ],
            "a retry over pre-existing dirs must still fsync the full leaf→\
             data-root chain (not just the leaf) before the WAL truncate"
        );
        assert_eq!(wal.replay().unwrap().mutations.len(), 0);
    }

    // Issue #1392: `create_dir_all` materializes the full tree and is idempotent
    // (re-creating an existing tree succeeds).
    #[test]
    fn create_dir_all_is_idempotent() {
        let root = TempDir::new().unwrap();
        let ks = root.path().join("ks");
        let tbl = ks.join("tbl");

        create_dir_all(&tbl).unwrap();
        assert!(tbl.is_dir());
        // Re-creating an existing tree is a no-op success.
        create_dir_all(&tbl).unwrap();
        // A new leaf under an existing parent also succeeds.
        let tbl2 = ks.join("tbl2");
        create_dir_all(&tbl2).unwrap();
        assert!(tbl2.is_dir());
    }

    // Issue #1392: `dirs_to_sync` walks the full leaf→data-root chain, ordered
    // deepest-first, and never ascends above the data root.
    #[test]
    fn dirs_to_sync_is_full_chain_deepest_first() {
        let root = TempDir::new().unwrap();
        let data_dir = root.path().to_path_buf();
        let ks = data_dir.join("ks");
        let leaf = ks.join("tbl");

        let dirs = dirs_to_sync(&leaf, &data_dir);
        assert_eq!(dirs, vec![leaf, ks, data_dir.clone()]);

        // The leaf being the data root itself yields just the root.
        assert_eq!(dirs_to_sync(&data_dir, &data_dir), vec![data_dir.clone()]);

        // Defensive: a leaf not under the data root does not ascend above it.
        let stray = TempDir::new().unwrap();
        let stray_leaf = stray.path().join("elsewhere");
        assert_eq!(
            dirs_to_sync(&stray_leaf, &data_dir),
            vec![stray_leaf],
            "must not fsync directories outside the configured data root"
        );
    }
}
