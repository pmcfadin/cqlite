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
//! (cancel-aware — see [`OpenCoalescer::open`]) and clone the resulting
//! `Arc<SSTableReader>` with zero re-parse. It does NOT serialise whole rebuilds
//! (each rebuild still runs its own probe/swap), so the existing swap-time race
//! tests (`concurrent_same_key_rebuild_dedups...`,
//! `slow_rebuild_does_not_overwrite_a_faster_newer_swap`) keep exercising their
//! forced concurrency without deadlocking.
//!
//! ## Fix A × Fix B interaction — the Weak-hit path-liveness gate (issue #2383
//! ## blocker 1, post-review)
//!
//! `registry::rebuild`'s rebind pass only walks the CURRENTLY CACHED reader set
//! for a table (`inner.tables.get(key)`) — an LRU-EVICTED-but-still-alive reader
//! (kept alive only by an in-flight `WarmSet` `Arc` a prior request is still
//! streaming) is invisible to it, so rebind can never repoint its path. A
//! coalesced [`SlotState::Done`] `Weak` hit here therefore CANNOT assume its
//! reader's path is live just because it upgraded: every `Done` serve is ALSO
//! gated on [`reader_backing_present`] before being handed out. A dead path
//! (its per-query snapshot dir was cleared while the reader outlived the
//! registry's own cache entry) is treated as a miss and falls through to
//! `do_open` from the live `entry.path` — never re-serving a stale, ENOENT-prone
//! reader (the #2352 class). See `spin_tests_2383::evicted_but_inflight_reader_is_not_served_with_dead_path`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex, PoisonError, Weak};
use std::time::Duration;

use cqlite_core::schema::{TableSchema, UdtRegistry};
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

/// How often a waiting FOLLOWER re-checks its request's [`CancelFlag`] (issue
/// #2383 roborev-1653 Medium): a plain `Condvar::wait` never re-observes
/// cancellation, so a cancelled follower would otherwise sit behind the leader's
/// FULL Index.db open and only notice afterwards — exactly the field's
/// "cancellation doesn't land" symptom. A bounded `wait_timeout` loop keeps this
/// cheap (one wake per interval, not per entry) while making a follower's cancel
/// latency roughly this interval, never "whatever the leader's whole parse costs".
const FOLLOWER_CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(75);

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
/// A live `Weak` upgrade is NOT sufficient to serve, though — see the module doc
/// ("Fix A × Fix B interaction") for why every `Done` hit is ALSO gated on
/// [`reader_backing_present`].
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
    /// The reader was opened; followers clone it while the `Weak` is live AND
    /// its backing path resolves (issue #2383 blocker 1).
    Done(Weak<SSTableReader>),
    /// The last open attempt failed; the next caller re-leads.
    Failed,
}

/// RAII guard: if the leader's `do_open` PANICS (unwinds) before the guard is
/// disarmed, transitions the slot to [`SlotState::Failed`] and wakes every
/// follower on drop — without this, a panicking leader would leave the slot
/// `Pending` forever, hanging every follower on the condvar (issue #2383,
/// post-review hardening).
struct FailSlotOnUnwind<'a> {
    slot: &'a OpenSlot,
    armed: bool,
}

impl Drop for FailSlotOnUnwind<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        {
            let mut st = self
                .slot
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if matches!(*st, SlotState::Pending) {
                *st = SlotState::Failed;
            }
        }
        self.slot.ready.notify_all();
    }
}

/// The caller's elected role for one generation-id open (see
/// [`OpenCoalescer::open`]).
enum Role {
    /// A coalesced hit: the reader is alive AND its backing path resolves.
    Hit(Arc<SSTableReader>),
    /// Elected leader: run `do_open` and publish the outcome.
    Lead(Arc<OpenSlot>),
    /// Elected follower: wait for the leader, cancel-aware.
    Follow(Arc<OpenSlot>),
    /// The cached reader is alive but its backing path is DEAD (issue #2383
    /// blocker 1 — an LRU-evicted-but-in-flight generation whose snapshot dir
    /// was cleared). Never served; falls through to our own `do_open` exactly
    /// like a `Failed` slot — NOT a re-election through the slot (NIT 4: this
    /// intentionally lets M concurrent callers each re-open here rather than
    /// coalescing; fail-closed correctness beats dedup on this rare path).
    DeadPathFallThrough,
}

impl OpenCoalescer {
    /// Coalesce the open of generation `id`: the first caller (leader) runs
    /// `do_open`; concurrent callers whose reader is alive AND whose backing
    /// path resolves (issue #2383 blocker 1) clone it. Returns
    /// `(reader, real_open)` where `real_open` is `true` iff THIS call performed
    /// the actual open (drives the reader-opens work-done metric — a coalesced
    /// caller reports `false`).
    ///
    /// A FOLLOWER's wait is cancel-aware (issue #2383 roborev-1653 Medium): it
    /// re-checks `cancel` on a bounded [`FOLLOWER_CANCEL_POLL_INTERVAL`] wake and
    /// returns [`WarmError::Cancelled`] promptly instead of sitting behind the
    /// leader's full open — the leader itself is unaffected and still publishes
    /// its outcome for any other waiter.
    ///
    /// On leader FAILURE (including a leader PANIC, via [`FailSlotOnUnwind`]), a
    /// dead-path `Done` reader, or a since-evicted reader, the caller falls
    /// through to its own `do_open` (fail-closed: correctness/liveness over
    /// dedup on these rare paths).
    fn open<F>(
        &self,
        id: GenerationId,
        cancel: &CancelFlag,
        do_open: F,
    ) -> Result<(Arc<SSTableReader>, bool), WarmError>
    where
        F: FnOnce() -> Result<Arc<SSTableReader>, WarmError>,
    {
        // Elect a role under the map lock. A slot in `Done` with a LIVE reader
        // AND a live backing path is a fast coalesced hit (no wait); a
        // dead-path/failed/absent slot makes us the leader (we reset it to
        // `Pending` so concurrent callers wait on us) or falls straight through.
        let role = {
            let mut map = self.inflight.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(slot) = map.get(&id) {
                let mut st = slot.state.lock().unwrap_or_else(PoisonError::into_inner);
                match &*st {
                    SlotState::Done(weak) => match weak.upgrade() {
                        Some(reader) if reader_backing_present(&reader) => Role::Hit(reader),
                        Some(_dead_path_reader) => {
                            drop(st);
                            Role::DeadPathFallThrough
                        }
                        None => {
                            *st = SlotState::Pending; // reader dropped → re-lead
                            drop(st);
                            Role::Lead(Arc::clone(slot))
                        }
                    },
                    SlotState::Failed => {
                        *st = SlotState::Pending;
                        drop(st);
                        Role::Lead(Arc::clone(slot))
                    }
                    SlotState::Pending => {
                        drop(st);
                        Role::Follow(Arc::clone(slot))
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
                Role::Lead(slot)
            }
        };

        match role {
            Role::Hit(reader) => Ok((reader, false)),
            Role::DeadPathFallThrough => do_open().map(|r| (r, true)),
            Role::Lead(slot) => {
                let mut guard = FailSlotOnUnwind {
                    slot: &slot,
                    armed: true,
                };
                let result = do_open();
                guard.armed = false;
                {
                    let mut st = slot.state.lock().unwrap_or_else(PoisonError::into_inner);
                    *st = match &result {
                        Ok(reader) => SlotState::Done(Arc::downgrade(reader)),
                        Err(_) => SlotState::Failed,
                    };
                }
                slot.ready.notify_all();
                result.map(|r| (r, true))
            }
            Role::Follow(slot) => {
                let mut st = slot.state.lock().unwrap_or_else(PoisonError::into_inner);
                loop {
                    match &*st {
                        SlotState::Pending => {
                            if cancel.is_cancelled() {
                                return Err(WarmError::Cancelled);
                            }
                            let (guard, _timed_out) = slot
                                .ready
                                .wait_timeout(st, FOLLOWER_CANCEL_POLL_INTERVAL)
                                .unwrap_or_else(PoisonError::into_inner);
                            st = guard;
                        }
                        SlotState::Done(weak) => match weak.upgrade() {
                            Some(reader) if reader_backing_present(&reader) => {
                                return Ok((reader, false));
                            }
                            _ => {
                                // Evicted, or its backing path is dead (issue
                                // #2383 blocker 1 / the #2352 ENOENT class):
                                // never serve a stale/dead reader. Fall through
                                // to our own `do_open` from the live
                                // `entry.path` (fail-closed over dedup — NIT 4:
                                // this intentionally lets M followers re-open
                                // here, never re-electing a leader through the
                                // slot).
                                drop(st);
                                return do_open().map(|r| (r, true));
                            }
                        },
                        SlotState::Failed => {
                            drop(st);
                            return do_open().map(|r| (r, true));
                        }
                    }
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
    /// returns immediately WITHOUT partial state. Each reader is opened WITH the
    /// resolved `udt_registry` (issue #2349), matching the cold path exactly so a
    /// `frozen<UDT>`-in-collection cell decodes structurally on both paths.
    ///
    /// Returns `(opened, real_opens)`: `real_opens` counts only the opens THIS
    /// call actually performed (coalesced follower opens do not count), so the
    /// reader-opens work-done metric equals #distinct generations parsed, not
    /// #callers × #generations.
    pub(super) fn open_added(
        &self,
        added: &[&GenerationEntry],
        _schema: &TableSchema,
        udt_registry: Option<&UdtRegistry>,
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
            let (reader, real) = self.coalescer.open(entry.id, cancel, || {
                // Test-only rendezvous (issue #3940): fires on the OPENING
                // thread (whichever role reaches a real open), downstream of
                // every flight-side cancel gate above and of the coalescer's own
                // follower poll, immediately before the Index.db open+parse. A cancel tripped here is therefore
                // observable ONLY by the parse's `ScanCancel` polling (fix C),
                // which is what makes the mid-parse-cancel repro deterministic
                // instead of sleep-calibrated. No-op in production.
                #[cfg(test)]
                self.run_open_parse_barrier();
                open_one_reader(
                    &runtime,
                    &entry.path,
                    &config,
                    Arc::clone(&platform),
                    udt_registry,
                    scan_cancel.clone(),
                )
                .map(Arc::new)
            })?;
            if real {
                real_opens += 1;
            }
            // Issue #2412 §D: account the footprint from the JUST-OPENED
            // reader's ACTUAL Index.db residency (lazy-open leaves it `false`
            // for the common Summary-usable BIG shape), not a blanket
            // "always resident" assumption — the summary-only accounting spec
            // Requirement 4 requires.
            let footprint = account_footprint(&entry.path, reader.index_is_materialized());
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

/// Open ONE reader, cancel-aware (issue #2383 fix C), threading the SAME resolved
/// UDT registry as the cold path (issue #2349).
///
/// The cold Flight path (`KWayMerger::new_with_gc_and_registry_cancellable`) opens
/// each reader WITH its resolved registry via `set_udt_registry` so a `frozen<UDT>`
/// cell inside a collection decodes structurally (the #1234 data-loss class). The
/// warm path must match EXACTLY (spec non-goal: warm is a parse-cost change, never
/// a read-semantics change), so this sets the same registry on the freshly-opened
/// reader BEFORE it is shared as an `Arc` (the only point a `&mut self` exists —
/// see `from_readers.rs`'s shared-reader UDT contract). `udt_registry = None` (a
/// DDL with no `CREATE TYPE`) leaves the reader registry-free, identical to the
/// cold path's `None`. The `cancel` flag is polled inside the O(entries) `Index.db`
/// parse (`open_cancellable`), so a mid-parse client disconnect surfaces as
/// [`Error::Cancelled`] and is mapped to [`WarmError::Cancelled`] — never masked
/// as an `Open` failure, never run to completion.
fn open_one_reader(
    runtime: &tokio::runtime::Runtime,
    path: &Path,
    config: &Config,
    platform: Arc<Platform>,
    udt_registry: Option<&UdtRegistry>,
    cancel: ScanCancel,
) -> Result<SSTableReader, WarmError> {
    let mut reader = runtime
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
    if let Some(registry) = udt_registry {
        reader.set_udt_registry(registry.clone());
    }
    debug_assert_eq!(
        reader.has_udt_registry(),
        udt_registry.is_some(),
        "warm reader UDT-registry posture must match the cold path (#2349)"
    );
    Ok(reader)
}
