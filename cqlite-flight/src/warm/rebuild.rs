//! The warm rebuild's expensive half (issue #2383, campsite split of
//! `registry.rs`): single-flight per-generation reader opens (fix A),
//! cancel-aware opens (fix C), and the authoritative rebind-by-inode match
//! (fix B). Split out so `registry.rs` stays within the campsite threshold.
//!
//! ## Fix A — single-flight opens
//!
//! The round-8 field spin: M concurrent `do_get`s that all MISS for one cold
//! table each open (and thus full-parse the `Index.db` of) EVERY generation —
//! the log showed 8× "Parsed 1586932 partition entries" for one logical query,
//! pinning tokio workers. The registry's epoch guard deduped the CACHE but only
//! AFTER every racer had already paid the parse. [`OpenCoalescer`] moves the
//! coalescing point EARLIER: concurrent opens of the SAME generation identity
//! share ONE real open+parse; the leader opens, the followers block on a condvar
//! and clone the resulting `Arc<SSTableReader>` with zero re-parse. It does NOT
//! serialise whole rebuilds (each rebuild still runs its own probe/swap), so the
//! existing swap-time race tests (`concurrent_same_key_rebuild_dedups...`,
//! `slow_rebuild_does_not_overwrite_a_faster_newer_swap`) keep exercising their
//! forced concurrency without deadlocking.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex, PoisonError, Weak};

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::{Config, Error, Platform};

use crate::cancel::CancelFlag;

use super::budget::account_footprint;
use super::identity::GenerationId;
use super::probe::GenerationEntry;
use super::registry::{WarmReader, WarmTableRegistry};
use super::WarmError;

/// The map size past which a fresh leader election opportunistically prunes slots
/// whose reader has since been evicted/dropped (a dead `Weak`). Keeps the
/// coalescer's memory bounded on a long-running server without a background task.
const COALESCER_PRUNE_THRESHOLD: usize = 1024;

/// Per-generation single-flight for reader opens (issue #2383 fix A).
///
/// Keyed on the inode-stable [`GenerationId`], so two per-query snapshot hardlink
/// dirs (same inodes, new paths) coalesce. Each [`OpenSlot`] caches a `Weak` to
/// the opened reader: while that reader is still alive ANYWHERE (in the warm
/// cache or held by an in-flight request), a concurrent miss for the same
/// generation clones it instead of re-parsing — so coalescing survives even the
/// FAST in-process opens a test drives, not just a real 1.58M-entry parse. The
/// `Weak` keeps NO reader alive, so the warm budget's LRU eviction is unaffected;
/// once a generation's reader is truly gone the slot re-opens on the next miss.
/// Rebind (fix B) mutates the shared reader in place, so a coalesced reader always
/// reflects the current live path (no stale-path serve).
#[derive(Default)]
pub(super) struct OpenCoalescer {
    inflight: Mutex<HashMap<GenerationId, Arc<OpenSlot>>>,
}

/// The shared rendezvous for one generation's open.
struct OpenSlot {
    state: Mutex<SlotState>,
    ready: Condvar,
}

#[derive(Default)]
enum SlotState {
    /// The leader is opening; followers wait.
    #[default]
    Pending,
    /// The reader was opened; followers clone it while the `Weak` is live.
    Done(Weak<SSTableReader>),
    /// The last open attempt failed; the next caller re-leads.
    Failed,
}

impl OpenCoalescer {
    /// Coalesce the open of generation `id`: the first caller (leader) runs
    /// `do_open`; concurrent callers whose reader is still alive clone it. Returns
    /// `(reader, real_open)` where `real_open` is `true` iff THIS call performed
    /// the actual open (drives the reader-opens work-done metric — a coalesced
    /// caller reports `false`). On leader FAILURE (or a since-evicted reader) the
    /// caller falls through to its own `do_open` (fail-closed).
    fn open<F>(&self, id: GenerationId, do_open: F) -> Result<(Arc<SSTableReader>, bool), WarmError>
    where
        F: FnOnce() -> Result<Arc<SSTableReader>, WarmError>,
    {
        // Elect a role under the map lock. A slot in `Done` with a LIVE reader is a
        // fast coalesced hit (no wait); a dead/failed/absent slot makes us the
        // leader (we reset it to `Pending` so concurrent callers wait on us).
        let (slot, is_leader) = {
            let mut map = self.inflight.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(slot) = map.get(&id) {
                let mut st = slot.state.lock().unwrap_or_else(PoisonError::into_inner);
                match &*st {
                    SlotState::Done(weak) => {
                        if let Some(reader) = weak.upgrade() {
                            return Ok((reader, false));
                        }
                        *st = SlotState::Pending; // reader evicted → re-lead
                        drop(st);
                        (Arc::clone(slot), true)
                    }
                    SlotState::Failed => {
                        *st = SlotState::Pending;
                        drop(st);
                        (Arc::clone(slot), true)
                    }
                    SlotState::Pending => {
                        drop(st);
                        (Arc::clone(slot), false)
                    }
                }
            } else {
                if map.len() >= COALESCER_PRUNE_THRESHOLD {
                    prune_dead(&mut map);
                }
                let slot = Arc::new(OpenSlot {
                    state: Mutex::new(SlotState::Pending),
                    ready: Condvar::new(),
                });
                map.insert(id, Arc::clone(&slot));
                (slot, true)
            }
        };

        if is_leader {
            let result = do_open();
            {
                let mut st = slot.state.lock().unwrap_or_else(PoisonError::into_inner);
                *st = match &result {
                    Ok(reader) => SlotState::Done(Arc::downgrade(reader)),
                    Err(_) => SlotState::Failed,
                };
            }
            slot.ready.notify_all();
            return result.map(|r| (r, true));
        }

        // Follower: wait for the leader's outcome.
        let mut st = slot.state.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            match &*st {
                SlotState::Pending => {
                    st = slot.ready.wait(st).unwrap_or_else(PoisonError::into_inner);
                }
                SlotState::Done(weak) => {
                    if let Some(reader) = weak.upgrade() {
                        return Ok((reader, false));
                    }
                    // The reader was evicted between notify and our wake: open it
                    // ourselves (fail-closed correctness over dedup).
                    drop(st);
                    return do_open().map(|r| (r, true));
                }
                SlotState::Failed => {
                    drop(st);
                    return do_open().map(|r| (r, true));
                }
            }
        }
    }
}

/// Drop slots whose reader has been evicted/dropped (a dead `Weak`) and are not
/// mid-open. Runs under the map lock; slot locks are always taken map-first, so
/// this is deadlock-free with the open path.
fn prune_dead(map: &mut HashMap<GenerationId, Arc<OpenSlot>>) {
    map.retain(|_, slot| {
        let st = slot.state.lock().unwrap_or_else(PoisonError::into_inner);
        match &*st {
            SlotState::Done(weak) => weak.strong_count() > 0,
            // Keep Pending (in-flight) and Failed (a concurrent caller may re-lead).
            SlotState::Pending | SlotState::Failed => true,
        }
    });
}

impl WarmTableRegistry {
    /// Open the ADDED generations, single-flight-coalesced (issue #2383 fix A) and
    /// cancel-aware (fix C). Fail-closed: any open error (or a cancellation)
    /// returns immediately WITHOUT partial state. Readers are opened with the SAME
    /// UDT-registry posture as the cold path (none — see the `registry` module
    /// doc; #2349 wires a real registry into both paths together).
    ///
    /// Returns `(opened, real_opens)`: `real_opens` counts only the opens THIS
    /// call actually performed (coalesced follower opens do not count), so the
    /// reader-opens work-done metric equals #distinct generations parsed, not
    /// #callers × #generations.
    pub(super) fn open_added(
        &self,
        added: &[&GenerationEntry],
        _schema: &TableSchema,
        cancel: &CancelFlag,
    ) -> Result<(Vec<WarmReader>, u64), WarmError> {
        if added.is_empty() {
            return Ok((Vec::new(), 0));
        }
        // One current-thread runtime per rebuild (reader open is async); reused
        // across every added generation in this rebuild, like `PruneRuntime`.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| WarmError::Runtime(format!("build runtime: {e}")))?;
        let platform = self.platform(&runtime)?;
        let config = Config::default();
        // Bridge the request's async-token `CancelFlag` onto the synchronous
        // `ScanCancel` the Index.db parse loop polls (issue #2383 fix C / #2264).
        let scan_cancel = cancel.scan_cancel();

        // Open in a DETERMINISTIC (generation-id sorted) order across ALL callers
        // (issue #2383 fix A): each caller probes the dir independently, so their
        // `read_dir`-ordered `added` slices can differ. Without a common order,
        // concurrent misses would race on DIFFERENT generations first and split
        // the coalescing per phase (one real open per generation PER order),
        // defeating the single-flight. A shared order makes all racers converge on
        // the same generation at the same step → exactly one real open per
        // generation. Identity is inode-stable, so the order is stable.
        let mut added: Vec<&GenerationEntry> = added.to_vec();
        added.sort_by_key(|e| e.id);

        let mut opened = Vec::with_capacity(added.len());
        let mut real_opens = 0u64;
        for entry in &added {
            #[cfg(test)]
            self.run_open_barrier();
            if cancel.is_cancelled() {
                return Err(WarmError::Cancelled);
            }
            let (reader, real) = self.coalescer.open(entry.id, || {
                open_one_reader(
                    &runtime,
                    &entry.path,
                    &config,
                    Arc::clone(&platform),
                    scan_cancel.clone(),
                )
                .map(Arc::new)
            })?;
            if real {
                real_opens += 1;
            }
            let footprint = account_footprint(&entry.path);
            opened.push(WarmReader {
                id: entry.id,
                reader,
                footprint,
                last_access: 0,
            });
        }
        Ok((opened, real_opens))
    }

    /// Lazily build + cache the shared [`Platform`] used to open readers.
    pub(super) fn platform(
        &self,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<Arc<Platform>, WarmError> {
        let mut guard = self.platform.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(p) = guard.as_ref() {
            return Ok(Arc::clone(p));
        }
        let platform = runtime
            .block_on(Platform::new(&Config::default()))
            .map_err(|e| WarmError::Runtime(format!("build platform: {e}")))?;
        let platform = Arc::new(platform);
        *guard = Some(Arc::clone(&platform));
        Ok(platform)
    }
}

/// Whether a cached reader's backing `Data.db` still resolves on disk (issue
/// #2352). A cheap `metadata` stat on the reader's `file_path` — the path is
/// almost always dentry-cached, and this runs only on the per-request warm probe
/// path, never a hot inner loop. Returning `false` (an ENOENT/`stat` failure)
/// means the reader was opened from a path that has since been cleared (a
/// per-query snapshot dir), so it must be re-opened or REBOUND from the current
/// live dir rather than served (which would ENOENT on its next lazy scan re-open).
pub(super) fn reader_backing_present(reader: &SSTableReader) -> bool {
    std::fs::metadata(reader.file_path()).is_ok()
}

/// Whether `live_path` is AUTHORITATIVELY the same on-disk generation as a cached
/// reader of identity `id` and size `expected_size` — the rebind gate (issue
/// #2383 fix B, the #2356 "rebind-by-inode" direction).
///
/// Re-resolves the live candidate's identity and requires an EXACT
/// `(device, inode, generation)` + size match. Cassandra snapshot files are
/// hardlinks to the immutable SSTable, so a same-inode candidate is byte-identical
/// — matching is proof the cached parsed state is still valid for the live path.
/// Anything short of a full match (a `stat` failure, a recycled inode, a size
/// mismatch) fails CLOSED: the caller falls back to a full re-open + re-parse
/// rather than guessing (issue #28 no-heuristics).
pub(super) fn rebind_matches(id: GenerationId, expected_size: u64, live_path: &Path) -> bool {
    match (
        GenerationId::resolve(live_path),
        std::fs::metadata(live_path),
    ) {
        (Some(live_id), Ok(md)) => live_id == id && md.len() == expected_size,
        _ => false,
    }
}

/// Open ONE reader, cancel-aware (issue #2383 fix C), with the SAME UDT-registry
/// posture as the cold path.
///
/// The cold Flight path (`KWayMerger::new_cancellable`) opens readers with
/// `udt_registry = None`, so — to keep warm a parse-cost-only change with no
/// read-semantics divergence — this does NOT set a registry either. Wiring a real
/// registry into BOTH paths together is issue #2349; see the `registry` module
/// doc. The `cancel` flag is polled inside the O(entries) `Index.db` parse
/// (`open_cancellable`), so a mid-parse client disconnect surfaces as
/// [`Error::Cancelled`] and is mapped to [`WarmError::Cancelled`] — never masked
/// as an `Open` failure, never run to completion.
fn open_one_reader(
    runtime: &tokio::runtime::Runtime,
    path: &Path,
    config: &Config,
    platform: Arc<Platform>,
    cancel: ScanCancel,
) -> Result<SSTableReader, WarmError> {
    let reader = runtime
        .block_on(SSTableReader::open_cancellable(
            path, config, platform, cancel,
        ))
        .map_err(|source| match source {
            Error::Cancelled => WarmError::Cancelled,
            source => WarmError::Open {
                path: path.to_path_buf(),
                source,
            },
        })?;
    debug_assert!(
        !reader.has_udt_registry(),
        "warm reader must match the cold path's no-UDT-registry posture (#2349)"
    );
    Ok(reader)
}
