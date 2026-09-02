//! The warm-handle registry (issue #2310, WS1 #2345 / WS3 #2342).
//!
//! [`WarmTableRegistry`] holds parsed, open `Arc<SSTableReader>`s keyed on
//! inode-stable generation identity (Decision 1) and adopts `Database::refresh()`'s
//! fail-closed diff/swap contract (#1749, Decision 3): a rebuild opens ADDED
//! generations BEFORE swapping, keeps UNCHANGED generations' parsed state, drops
//! REMOVED ones, and swaps atomically under a write guard — so any open failure
//! mutates nothing and in-flight requests holding `Arc` clones complete against
//! the pre-swap set.
//!
//! ## UDT-registry posture — identical to the cold path (WS1 #2345 → wired #2349)
//!
//! The reader-based merge seam `KWayMerger::new_from_readers` takes NO
//! `udt_registry` parameter (a shared `Arc` reader has no `&mut self` for
//! `set_udt_registry`), so a reader must be opened WITH its registry already
//! resolved BEFORE it is wrapped in `Arc`. Issue #2349 threads a resolved
//! `Option<&UdtRegistry>` (from the ticket DDL's `CREATE TYPE` statements, via
//! `udt_registry_from_cql`) through `warm_readers` → `rebuild` → `open_added` →
//! `open_one_reader`, which calls `set_udt_registry` on each freshly-opened
//! reader BEFORE sharing it — the exact same registry the cold path sets via
//! `KWayMerger::new_with_gc_and_registry_cancellable`. Both paths therefore flip
//! TOGETHER: `has_udt_registry()` is `true` iff the DDL declares UDTs, so a
//! `frozen<UDT>` cell inside a collection decodes structurally on both (the #1234
//! data-loss class), and the warm-vs-cold read semantics never diverge. A DDL with
//! no `CREATE TYPE` resolves to `None` and both paths stay registry-free.
//!
//! ## File-lifetime contract (from `from_readers.rs`)
//!
//! `new_from_readers` requires the backing `Data.db` not be deleted while any
//! `Arc<SSTableReader>` clone is alive. This registry only ever evicts its OWN
//! reference (per #1749's fail-closed model) and never deletes/replaces a
//! generation's file out from under a live `Arc`; a snapshot's hardlinked inode
//! outlives the per-query dir (the link keeps the inode alive), so THIS
//! registry's own actions satisfy the contract trivially.
//!
//! That is not the whole story (issue #2352): an EXTERNAL actor — the Trino
//! connector's per-query `SnapshotManager.clearSnapshot` — deletes the snapshot
//! dir after its query, outside this registry's control. Inode identity keeps
//! a cached reader's `Arc` alive, but its full-scan path re-opens `Data.db`
//! LAZILY by its stored path (unlike the point-read path's dedicated fd), so a
//! later warm hit could serve a dead path → ENOENT mid-merge. The
//! path-liveness gate (`cached_paths_all_present`, at each warm-hit site) covers
//! this via rebuild — see its adjudication comment for the residual left open.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use cqlite_core::observability::{self as obs, catalog};
use cqlite_core::schema::{TableSchema, UdtRegistry};
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::Platform;

use crate::cancel::CancelFlag;

use super::budget::DEFAULT_WARM_BUDGET_BYTES;
use super::identity::{GenerationId, GenerationSet};
use super::metrics::{RefreshOutcome, WarmMetrics};
use super::probe::{self, GenerationEntry, ProbeOutcome};
use super::{WarmError, WarmSet};

/// Process-wide level backing `cqlite.flight.warm_tables` (issue #2684) — the
/// current number of tables with a live warm reader set. Set to the
/// post-mutation `Inner.tables.len()` at each registry mutation site (rebuild
/// insert / `evict_to_budget` removal) while the `Inner` lock is held, so the
/// remove-then-reinsert transient a rebuild performs never dips the reading.
///
/// A process-wide `static` (not per-registry state) mirrors
/// [`crate::saturation::blocking_tasks_in_use_level`]: production runs exactly
/// one [`WarmTableRegistry`], and a level reader consumable without an OTel stack
/// lets up/down tests assert the gauge moves. Tests that read it exactly use one
/// `#[test]` per binary (one process), as `issue_2370_gauge_readback_test.rs`
/// documents for the other process-global gauges.
static WARM_TABLES: AtomicI64 = AtomicI64::new(0);

/// Read the current process-wide warm-table level (issue #2684) — the same value
/// that drives `cqlite.flight.warm_tables`. Feature-independent (maintained
/// regardless of the `observability` OTel feature; only the emission is gated),
/// mirroring [`crate::saturation::blocking_tasks_in_use_level`].
pub fn warm_table_count() -> i64 {
    WARM_TABLES.load(Ordering::SeqCst)
}

/// Record the post-mutation warm-table count: store the process-wide level AND
/// emit the gauge (total-only, no attributes — matching the saturation gauges).
/// Called under the `Inner` lock so the emitted value is the exact current size.
///
/// Counts tables with a LIVE (non-empty) warm reader set — the spec's definition
/// ("tables with a live warm reader set"). A rebuild against an empty
/// on-disk generation set can leave a table entry with zero readers (the
/// generation was retired from disk); such an entry is not a live warm table, so
/// it is excluded — which makes retirement observably DECREMENT the gauge.
fn record_warm_tables(inner: &Inner) {
    let live = inner
        .tables
        .values()
        .filter(|t| !t.readers.is_empty())
        .count();
    let level = i64::try_from(live).unwrap_or(i64::MAX);
    WARM_TABLES.store(level, Ordering::SeqCst);
    obs::record_gauge(catalog::FLIGHT_WARM_TABLES, level, &[]);
}

/// The logical-table half of the warm cache key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableKey {
    /// Keyspace name.
    pub keyspace: String,
    /// Table name.
    pub table: String,
}

impl TableKey {
    /// Build a key from keyspace + table names.
    pub fn new(keyspace: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            keyspace: keyspace.into(),
            table: table.into(),
        }
    }
}

/// One warm generation: its identity, the open reader, and byte accounting.
///
/// `pub(super)` so the rebuild's expensive half (single-flight opens + rebind,
/// [`super::rebuild`]) can construct these (issue #2383, campsite split).
pub(super) struct WarmReader {
    pub(super) id: GenerationId,
    pub(super) reader: Arc<SSTableReader>,
    pub(super) footprint: u64,
    pub(super) last_access: u64,
}

/// Warm state for one logical table.
struct TableWarm {
    ddl_hash: u64,
    schema: Arc<TableSchema>,
    /// Newest-generation-first (LWW tie-break rank).
    readers: Vec<WarmReader>,
    /// Cached snapshot `manifest.json` for the fast-path probe (Decision 2b).
    manifest: Option<Vec<u8>>,
}

impl TableWarm {
    fn generation_set(&self) -> GenerationSet {
        GenerationSet::from_ids(self.readers.iter().map(|r| r.id).collect())
    }
}

#[derive(Default)]
struct Inner {
    tables: HashMap<TableKey, TableWarm>,
    used_bytes: u64,
    tick: u64,
}

impl Inner {
    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }
}

/// A flight-owned warm cache of open SSTable readers keyed on generation
/// identity.
pub struct WarmTableRegistry {
    inner: Mutex<Inner>,
    /// Lazily-built shared platform for reader opens. `pub(super)` for
    /// [`super::rebuild`] (campsite split, issue #2383).
    pub(super) platform: Mutex<Option<Arc<Platform>>>,
    budget_bytes: u64,
    metrics: Arc<WarmMetrics>,
    /// Per-generation single-flight for reader opens (issue #2383 fix A): M
    /// concurrent misses for one table coalesce onto ONE real open+parse per
    /// generation instead of opening ×M. See [`super::rebuild::OpenCoalescer`].
    pub(super) coalescer: super::rebuild::OpenCoalescer,
    /// Test-only rendezvous invoked inside [`Self::rebuild`] AFTER the added
    /// generations are opened but BEFORE the swap lock is taken — lets the
    /// concurrent-same-key-rebuild race test hold two threads "past the probe,
    /// before either swaps" deterministically. `None` (a no-op) in production.
    #[cfg(test)]
    swap_barrier: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// Test-only hook invoked at the TOP of each `open_added` loop
    /// iteration (before its cancel check) — lets the mid-rebuild cancellation
    /// test trip the cancel flag BETWEEN two added-generation opens. `None` in
    /// production. `pub(super)` for [`super::rebuild`] (campsite split).
    #[cfg(test)]
    pub(super) open_barrier: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// Test-only hook invoked INSIDE the coalesced real-open closure, on the
    /// leader's own thread, immediately BEFORE `open_one_reader` runs the
    /// Index.db open+parse — i.e. downstream of EVERY flight-side cancel gate
    /// (`rebuild`'s pre-rebuild + pre-open checks, `open_added`'s per-iteration
    /// check, and the coalescer's follower-wait poll), so a cancellation tripped
    /// from here can only be observed by the parse's own `ScanCancel` polling
    /// (issue #2383 fix C). That is what lets the mid-parse-cancel test position
    /// its cancel STRUCTURALLY instead of with a calibrated sleep (issue #3940).
    /// `None` (a no-op) in production. `pub(super)` for [`super::rebuild`].
    #[cfg(test)]
    pub(super) open_parse_barrier: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl Default for WarmTableRegistry {
    fn default() -> Self {
        Self::with_budget(DEFAULT_WARM_BUDGET_BYTES)
    }
}

impl WarmTableRegistry {
    /// A registry with the fixed named default byte budget (Decision 4).
    pub fn new() -> Self {
        Self::default()
    }

    /// A registry with an explicit byte budget (test/bench hook; production uses
    /// the fixed [`DEFAULT_WARM_BUDGET_BYTES`] via [`Self::new`]).
    pub fn with_budget(budget_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            platform: Mutex::new(None),
            budget_bytes: budget_bytes.max(1),
            metrics: Arc::new(WarmMetrics::default()),
            coalescer: super::rebuild::OpenCoalescer::default(),
            #[cfg(test)]
            swap_barrier: Mutex::new(None),
            #[cfg(test)]
            open_barrier: Mutex::new(None),
            #[cfg(test)]
            open_parse_barrier: Mutex::new(None),
        }
    }

    /// The shared metrics handle (hit/miss/evict/refresh-outcome + reader-opens
    /// work probe) for the #2289/#1494 bench harness and tests.
    pub fn metrics(&self) -> Arc<WarmMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Obtain the warm reader set for one request, probing staleness and
    /// rebuilding only the delta on a change (fail-closed).
    ///
    /// * `dir` — the resolved SSTable directory (from `DirSource::resolve`).
    /// * `snapshot` — the ticket's snapshot name (enables the manifest fast path).
    /// * `cancel` — the request cancel flag; a pre-cancelled request does ZERO
    ///   probe/rebuild work and returns [`WarmError::Cancelled`] by variant.
    #[allow(clippy::too_many_arguments)]
    pub fn warm_readers(
        &self,
        key: &TableKey,
        ddl_hash: u64,
        schema: &TableSchema,
        udt_registry: Option<&UdtRegistry>,
        dir: &Path,
        snapshot: Option<&str>,
        cancel: &CancelFlag,
    ) -> Result<WarmSet, WarmError> {
        // Cancellation (issue #2264/#1473): zero work when pre-cancelled.
        if cancel.is_cancelled() {
            return Err(WarmError::Cancelled);
        }
        let snapshot_mode = snapshot.is_some_and(|s| !s.is_empty());

        // Snapshot the cached manifest for the fast-path probe (only when the
        // cached entry's DDL still matches — a DDL change invalidates the cache).
        let cached_manifest = {
            let inner = self.lock_inner();
            inner
                .tables
                .get(key)
                .filter(|t| t.ddl_hash == ddl_hash)
                .and_then(|t| t.manifest.clone())
        };

        match probe::probe_generation_set(dir, snapshot_mode, cached_manifest.as_deref(), cancel)? {
            ProbeOutcome::UnchangedByManifest => {
                // Manifest fast path matched: serve the cached set with no
                // reader-open/parse — but ONLY when every cached reader's backing
                // path still resolves (issue #2352, cached_paths_all_present); a
                // dead path (e.g. a cleared per-query snapshot dir) falls through
                // to a re-enumeration + rebuild instead of ENOENTing mid-scan. A
                // rare race (a concurrent rebuild replaced the entry) also falls
                // back here. Accepted TOCTOU residual — see the adjudication
                // comment on `cached_paths_all_present` (roborev 1644).
                if self.cached_paths_all_present(key, ddl_hash) {
                    if let Some(hit) = self.try_hit(key, ddl_hash, None) {
                        return Ok(hit);
                    }
                }
                let entries = probe::enumerate_generations(dir)?;
                let manifest = if snapshot_mode {
                    probe::read_manifest_checked(dir)?
                } else {
                    None
                };
                self.finish_enumerated(
                    key,
                    ddl_hash,
                    schema,
                    udt_registry,
                    entries,
                    manifest,
                    cancel,
                )
            }
            ProbeOutcome::Enumerated {
                entries, manifest, ..
            } => self.finish_enumerated(
                key,
                ddl_hash,
                schema,
                udt_registry,
                entries,
                manifest,
                cancel,
            ),
        }
    }

    /// Resolve an authoritatively-enumerated generation set into a warm hit (set
    /// unchanged) or a fail-closed delta rebuild.
    #[allow(clippy::too_many_arguments)]
    fn finish_enumerated(
        &self,
        key: &TableKey,
        ddl_hash: u64,
        schema: &TableSchema,
        udt_registry: Option<&UdtRegistry>,
        entries: Vec<GenerationEntry>,
        manifest: Option<Vec<u8>>,
        cancel: &CancelFlag,
    ) -> Result<WarmSet, WarmError> {
        let current_set = ProbeOutcome::set(&entries);
        // Serve a warm hit only when the generation SET matches AND every cached
        // reader's backing path still resolves (issue #2352, cached_paths_all_present):
        // a dead cached path (e.g. its snapshot dir was cleared) forces the
        // fail-closed rebuild below instead of ENOENTing on a later re-open.
        // Accepted check-then-hit TOCTOU residual — see the adjudication comment
        // on `cached_paths_all_present` (roborev 1644).
        if self.cached_paths_all_present(key, ddl_hash) {
            if let Some(hit) = self.try_hit(key, ddl_hash, Some((&current_set, manifest.clone()))) {
                return Ok(hit);
            }
        }
        self.rebuild(
            key,
            ddl_hash,
            schema,
            udt_registry,
            entries,
            current_set,
            manifest,
            cancel,
        )
    }

    /// Try to serve a warm hit. `expected` is `None` for the manifest fast path
    /// (which already proved the set is unchanged) or `Some((set, manifest))` for
    /// the authoritative path (the cached set must equal `set`). Returns `None`
    /// when there is no usable cached entry (→ the caller rebuilds).
    fn try_hit(
        &self,
        key: &TableKey,
        ddl_hash: u64,
        expected: Option<(&GenerationSet, Option<Vec<u8>>)>,
    ) -> Option<WarmSet> {
        let mut inner = self.lock_inner();
        let tick = inner.next_tick();
        let entry = inner.tables.get_mut(key)?;
        if entry.ddl_hash != ddl_hash {
            return None;
        }
        if let Some((expected_set, new_manifest)) = expected {
            if entry.generation_set() != *expected_set {
                return None;
            }
            // Refresh the cached manifest so the next request can take the fast
            // path (the authoritative probe just proved this manifest current).
            if new_manifest.is_some() {
                entry.manifest = new_manifest;
            }
        }
        for r in &mut entry.readers {
            r.last_access = tick;
        }
        let set = WarmSet {
            readers: entry
                .readers
                .iter()
                .map(|r| Arc::clone(&r.reader))
                .collect(),
            schema: Arc::clone(&entry.schema),
            outcome: RefreshOutcome::Unchanged,
            reader_opens: 0,
        };
        self.metrics.record_hit();
        self.metrics.record_refresh(RefreshOutcome::Unchanged);
        Some(set)
    }

    /// Fail-closed delta rebuild (Decision 3, mirrors `refresh_tables`): open only
    /// ADDED generations (BEFORE any swap), keep UNCHANGED parsed state, drop
    /// REMOVED, swap atomically. Any open failure returns the typed error with the
    /// previously warm set fully intact.
    ///
    /// Epoch guard (issue #2310, roborev 1639): right before the swap, re-checks
    /// that the live cache entry's generation set still matches
    /// `probe_start_set` (the state THIS delta was computed against). If a
    /// concurrent rebuild already installed a fresher result for the same key in
    /// the meantime, this rebuild discards its own (now-stale-probe) opened
    /// readers and hands back the current, fresher set instead of overwriting
    /// newer state with an older probe result.
    #[allow(clippy::too_many_arguments)]
    fn rebuild(
        &self,
        key: &TableKey,
        ddl_hash: u64,
        schema: &TableSchema,
        udt_registry: Option<&UdtRegistry>,
        entries: Vec<GenerationEntry>,
        current_set: GenerationSet,
        manifest: Option<Vec<u8>>,
        cancel: &CancelFlag,
    ) -> Result<WarmSet, WarmError> {
        if cancel.is_cancelled() {
            return Err(WarmError::Cancelled);
        }

        // Which generations are already warm (and DDL still matches)? Snapshot
        // their ids so we open only the delta. Held briefly; opening happens
        // OUTSIDE the lock (slow I/O). `probe_start_set` is what THIS rebuild's
        // delta was computed against — the epoch guard below re-checks it hasn't
        // moved by the time we reach the swap (roborev 1639).
        // Snapshot each cached generation's identity AND a CLONE of its reader
        // `Arc` under the brief lock; the path-liveness `stat`s and any rebind run
        // OUTSIDE the lock (no filesystem I/O under the registry mutex). Cloning
        // the `Arc` lets us rebind the cached reader in place (#2383) — the same
        // reader is shared with the live cache entry, so a rebind is observed at
        // the swap without a re-open.
        let cached: Vec<(GenerationId, Arc<SSTableReader>)> = {
            let inner = self.lock_inner();
            inner
                .tables
                .get(key)
                .filter(|t| t.ddl_hash == ddl_hash)
                .map(|t| {
                    t.readers
                        .iter()
                        .map(|r| (r.id, Arc::clone(&r.reader)))
                        .collect()
                })
                .unwrap_or_default()
        };
        // `probe_start_set` is the FULL cached generation SET (by identity) this
        // delta was computed against — the epoch guard below compares the LIVE
        // entry's set to it to detect a concurrent fresher install. Path-liveness
        // / rebind never change the identity set, so the guard uses ALL cached ids.
        let probe_start_set = GenerationSet::from_ids(cached.iter().map(|(id, _)| *id).collect());

        // Live entries by identity, for the rebind match (issue #2383 fix B).
        let live_by_id: HashMap<GenerationId, &GenerationEntry> =
            entries.iter().map(|e| (e.id, e)).collect();

        // Path-liveness (#2352) + rebind-by-inode (#2383/#2356). A cached
        // generation counts as "already warm" (kept, ZERO re-parse) when EITHER:
        //  (a) its current backing `Data.db` path still resolves, OR
        //  (b) its path is dead (its per-query snapshot dir was cleared) but the
        //      SAME generation is present in the current live dir — an
        //      AUTHORITATIVE `(device, inode, generation)` + size match (Cassandra
        //      snapshot files are hardlinks to the immutable SSTable; #28
        //      no-heuristics). We REBIND the reader's lazy-scan path to that live
        //      hardlink instead of re-opening + re-parsing the whole Index.db.
        // A dead-path generation with NO identity-matching live entry fails CLOSED:
        // it lands in `added` and is fully re-opened from the live dir.
        let mut alive_ids: HashSet<GenerationId> = HashSet::new();
        let mut rebind_hits: u64 = 0;
        for (id, reader) in &cached {
            let current_path = reader.file_path();
            if std::fs::metadata(&current_path).is_ok() {
                alive_ids.insert(*id);
            } else if let Some(live) = live_by_id.get(id) {
                if super::rebuild::rebind_matches(*id, reader.file_size(), &live.path) {
                    reader.rebind_path(&live.path);
                    alive_ids.insert(*id);
                    rebind_hits += 1;
                }
            }
        }
        // Record the rebinds NOW (not gated on the swap outcome): a rebind
        // mutates the shared `Arc<SSTableReader>` in place (ArcSwap `file_path`),
        // so the repoint is already observed by the live cache entry — the
        // snapshot-lifecycle-closure work probe (flight-warm-snapshot-closure §D:
        // distinguishes a warm-hit-with-rebind from a full rebuild).
        self.metrics.record_rebind_hits(rebind_hits);

        // ADDED = present now, not already warm/rebound on a LIVE path. Open them
        // (fail-closed) — genuinely-new generations and dead-path ones with no
        // identity-matching live hardlink to rebind onto.
        let added: Vec<&GenerationEntry> = entries
            .iter()
            .filter(|e| !alive_ids.contains(&e.id))
            .collect();
        let (opened, reader_opens) = match self.open_added(&added, schema, udt_registry, cancel) {
            Ok(opened) => opened,
            Err(e) => {
                // The previously warm set is untouched. Record the fail-closed
                // retention outcome (unless it was a plain cancellation).
                if !matches!(e, WarmError::Cancelled) {
                    self.metrics
                        .record_refresh(RefreshOutcome::FailClosedRetained);
                }
                return Err(e);
            }
        };

        // Test-only rendezvous: hold here (past the probe + open, before the swap)
        // so a concurrent same-key rebuild can be driven deterministically.
        #[cfg(test)]
        self.run_swap_barrier();

        // Swap atomically under the write guard.
        let mut inner = self.lock_inner();

        // Epoch guard (issue #2310, roborev 1639): if the LIVE entry's DDL
        // matches ours but its generation set has already moved past
        // `probe_start_set` — the state THIS rebuild's delta was computed
        // against — a concurrent rebuild already installed a fresher result
        // under this same key while we were opening our (now stale-probe)
        // delta. Overwriting it would regress the cache to an older view, so we
        // DISCARD our opened readers (dropped below, never swapped in) and hand
        // back the CURRENT, already-fresher set instead: it satisfies this
        // request (it is at least as fresh as what we probed). A live entry
        // with a DIFFERENT ddl_hash, or no live entry at all, is not a valid
        // fresher substitute — those fall through to the normal rebuild below
        // exactly as before. Folded into the `FailClosedRetained` metric
        // (adjudicated: "the previously-installed — here, concurrently
        // NEWER — set was retained instead of being overwritten by an older
        // probe result") rather than adding a new bounded label.
        let fresher_installed = inner.tables.get(key).is_some_and(|live| {
            live.ddl_hash == ddl_hash && live.generation_set() != probe_start_set
        });
        if fresher_installed {
            let tick = inner.next_tick();
            if let Some(entry) = inner.tables.get_mut(key) {
                for r in &mut entry.readers {
                    r.last_access = tick;
                }
                let current = WarmSet {
                    readers: entry
                        .readers
                        .iter()
                        .map(|r| Arc::clone(&r.reader))
                        .collect(),
                    schema: Arc::clone(&entry.schema),
                    outcome: RefreshOutcome::FailClosedRetained,
                    reader_opens: 0,
                };
                drop(inner);
                // `opened` (our now-discarded readers) drops here, releasing
                // their `Arc<SSTableReader>`s without ever touching `used_bytes`
                // — the work of opening them was still real I/O, so it still
                // counts toward the reader-opens work-done probe.
                drop(opened);
                self.metrics.record_reader_opens(reader_opens);
                self.metrics
                    .record_refresh(RefreshOutcome::FailClosedRetained);
                return Ok(current);
            }
        }

        let tick = inner.next_tick();
        let mut freed: u64 = 0;
        let mut removed_count: u64 = 0;

        // KEEP unchanged generations' parsed state; DROP removed (evict now).
        let mut kept: Vec<WarmReader> = Vec::new();
        if let Some(prev) = inner.tables.remove(key) {
            if prev.ddl_hash == ddl_hash {
                for r in prev.readers {
                    // Keep a prior reader's parsed state ONLY when its generation
                    // is still present AND its backing path is still live (issue
                    // #2352). A present-but-dead-path generation is dropped here
                    // and RE-OPENED from the live dir via `added`, so it is a
                    // refresh (not an eviction) and is not counted as removed.
                    if current_set.contains(&r.id) && alive_ids.contains(&r.id) {
                        kept.push(r);
                    } else {
                        freed = freed.saturating_add(r.footprint);
                        if !current_set.contains(&r.id) {
                            removed_count += 1;
                            // Issue #2059 §C: a generation truly gone from disk —
                            // drop its process-global key-cache entries. A
                            // present-but-dead-path generation (#2352, re-opened
                            // from the live dir with the SAME inode identity) is a
                            // refresh, NOT a removal, so it is deliberately NOT
                            // invalidated — its entries stay valid across the rebind.
                            r.reader.invalidate_key_cache_entries();
                        }
                    }
                }
            } else {
                // DDL changed: the whole prior entry is invalid.
                for r in prev.readers {
                    freed = freed.saturating_add(r.footprint);
                    removed_count += 1;
                    // Issue #2059 §C: drop each dropped generation's global
                    // key-cache entries.
                    r.reader.invalidate_key_cache_entries();
                }
            }
        }

        // NEW reader set = kept + newly-opened, newest-generation-first.
        //
        // DEDUP by generation identity under this swap lock (fail-closed against a
        // concurrent same-key rebuild race): two requests can each MISS for the
        // same table, both compute the SAME added delta from a pre-swap snapshot,
        // and both open the same added generation OUTSIDE the lock. The first
        // swap installs it; when THIS (second) swap re-reads `prev`, `kept` now
        // already carries that generation (it is in `current_set`), and our own
        // `opened` carries a SECOND copy. Keeping both would double-count the
        // footprint and hand out two `WarmReader`s per inode → permanent
        // `used_bytes` drift → spurious LRU evictions. So we count/keep a newly
        // opened reader ONLY when its generation is not already present.
        let mut readers = kept;
        let mut present: HashSet<GenerationId> = readers.iter().map(|r| r.id).collect();
        let mut added_bytes: u64 = 0;
        for mut r in opened {
            if !present.insert(r.id) {
                // A concurrent rebuild already installed this generation; its
                // footprint is already accounted. Drop our duplicate copy (the
                // `Arc<SSTableReader>` is released here).
                continue;
            }
            r.last_access = tick;
            added_bytes = added_bytes.saturating_add(r.footprint);
            readers.push(r);
        }
        for r in &mut readers {
            r.last_access = tick;
        }
        readers.sort_by(|a, b| b.id.generation.cmp(&a.id.generation).then(b.id.cmp(&a.id)));

        inner.used_bytes = inner
            .used_bytes
            .saturating_sub(freed)
            .saturating_add(added_bytes);
        let out = WarmSet {
            readers: readers.iter().map(|r| Arc::clone(&r.reader)).collect(),
            schema: Arc::new(schema.clone()),
            outcome: RefreshOutcome::RebuiltDelta,
            reader_opens,
        };
        let protected: Vec<GenerationId> = readers.iter().map(|r| r.id).collect();
        inner.tables.insert(
            key.clone(),
            TableWarm {
                ddl_hash,
                schema: Arc::clone(&out.schema),
                readers,
                manifest,
            },
        );
        // Emit `cqlite.flight.warm_tables` (issue #2684) post-insert while the
        // `Inner` lock is held: the count is exact, and emitting AFTER the swap
        // avoids the remove-then-reinsert transient dip (this rebuild removed the
        // same key earlier). `evict_to_budget` below emits again if it removes.
        record_warm_tables(&inner);

        // Enforce the byte budget (LRU eviction), never evicting a generation in
        // THIS request's returned set.
        let lru_evicted = self.evict_to_budget(&mut inner, &protected);

        self.metrics.record_evicts(removed_count + lru_evicted);
        self.metrics.record_reader_opens(reader_opens);
        self.metrics.record_miss();
        self.metrics.record_refresh(RefreshOutcome::RebuiltDelta);
        Ok(out)
    }

    /// Evict least-recently-used (table, generation) entries until the accounted
    /// footprint is within budget, never evicting a `protected` generation (the
    /// current request's set). Returns the number evicted.
    fn evict_to_budget(&self, inner: &mut Inner, protected: &[GenerationId]) -> u64 {
        let mut evicted = 0u64;
        while inner.used_bytes > self.budget_bytes {
            // Find the global LRU victim not in `protected`.
            let victim = inner
                .tables
                .iter()
                .flat_map(|(k, t)| {
                    t.readers
                        .iter()
                        .map(move |r| (k.clone(), r.id, r.last_access, r.footprint))
                })
                .filter(|(_, id, _, _)| !protected.contains(id))
                .min_by_key(|(_, _, last_access, _)| *last_access);
            let Some((vkey, vid, _, vbytes)) = victim else {
                // Nothing evictable without dropping the current request's set.
                break;
            };
            let mut table_removed = false;
            if let Some(t) = inner.tables.get_mut(&vkey) {
                // Issue #2059 fix round (review Medium): a CAPACITY eviction here is
                // NOT a generation removal — the victim generation is still fully
                // present on disk and will likely be re-read soon (memory pressure,
                // not deletion, is precisely why it was evicted from the warm set).
                // Deliberately do NOT invalidate the process-global key cache: doing
                // so would drop shared cache entries exactly under the memory pressure
                // where the cache is most valuable, and would pull cache state out
                // from under any OTHER live reader of the same physical file (a
                // co-resident `SSTableManager`, another warm entry, or this same
                // generation re-opened on the next `do_get` miss). Correctness is
                // preserved by the cache's fail-closed identity match + repopulation;
                // we only release this warm entry's reader pin/retention and let the
                // key cache's own byte-budget LRU reclaim entries on its own schedule
                // if/when memory pressure requires it. GENUINE generation removal
                // (compaction/drop, DDL change) still invalidates — see `refresh.rs`
                // and the delta-rebuild removal branches above, which invalidate only
                // disk-absent generations.
                t.readers.retain(|r| r.id != vid);
                if t.readers.is_empty() {
                    inner.tables.remove(&vkey);
                    table_removed = true;
                }
            }
            if table_removed {
                // A table left the warm set: emit the post-removal count (issue
                // #2684). Only a WHOLE-table removal changes the live-table
                // count; evicting one generation of a still-warm
                // multi-generation table does not, so the gauge is emitted
                // exactly when the level moves.
                record_warm_tables(inner);
            }
            inner.used_bytes = inner.used_bytes.saturating_sub(vbytes);
            evicted += 1;
        }
        evicted
    }

    // `open_added` (single-flight coalesced, cancel-aware) + `platform` live in
    // [`super::rebuild`] (issue #2383, campsite split).

    fn lock_inner(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether every cached reader for `key` (when its DDL still matches) has a
    /// backing `Data.db` that still resolves on disk (issue #2352). The warm
    /// cache keys parsed state on inode-stable generation identity, so a
    /// fresh per-query snapshot hardlink dir (same inodes, new path) is a set
    /// match — but a full-scan re-opens `Data.db` LAZILY by the PATH the reader
    /// was opened from (`SSTableReader::new_scan_cursor` → `File::open`), unlike
    /// the point-read path's dedicated fd. When that path was an ephemeral
    /// snapshot dir the connector has since cleared, serving the cached reader
    /// would ENOENT mid-merge; this gate forces a dead-path set to a rebuild
    /// instead. Reader `Arc`s are cloned under a brief lock; the `stat`s run
    /// outside it. No cached entry returns `true` vacuously.
    ///
    /// Adjudicated residual — check-then-hit TOCTOU (#2352 adjudication, roborev
    /// job 1644, rust-reviewer 2026-07-12): this gate `stat`s at T; the lazy scan
    /// re-open happens LATER at T+δ, POST-LOCK — a path can die in [T, T+δ).
    /// Accepted: the lock can't close the window (the open is inside the
    /// streaming producer); a death there is a transient I/O error on one
    /// request, never stale data; the NEXT request's gate self-heals via
    /// rebuild; true closure (durable fd/mmap, or rebind-by-inode) is #2356.
    fn cached_paths_all_present(&self, key: &TableKey, ddl_hash: u64) -> bool {
        let readers: Vec<Arc<SSTableReader>> = {
            let inner = self.lock_inner();
            match inner.tables.get(key).filter(|t| t.ddl_hash == ddl_hash) {
                Some(t) => t.readers.iter().map(|r| Arc::clone(&r.reader)).collect(),
                None => return true,
            }
        };
        readers
            .iter()
            .all(|r| super::rebuild::reader_backing_present(r))
    }

    /// Install the test-only swap rendezvous (see the field doc).
    #[cfg(test)]
    pub(crate) fn set_swap_barrier(&self, f: Arc<dyn Fn() + Send + Sync>) {
        *self
            .swap_barrier
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(f);
    }

    /// Invoke the swap rendezvous if one is installed (clone the `Arc` out first
    /// so it is called WITHOUT holding the `swap_barrier` lock).
    #[cfg(test)]
    fn run_swap_barrier(&self) {
        let hook = self
            .swap_barrier
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(f) = hook {
            f();
        }
    }

    /// Install the test-only per-open rendezvous (see the field doc).
    #[cfg(test)]
    pub(crate) fn set_open_barrier(&self, f: Arc<dyn Fn() + Send + Sync>) {
        *self
            .open_barrier
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(f);
    }

    /// Invoke the per-open rendezvous if one is installed. `pub(super)` for
    /// [`super::rebuild::WarmTableRegistry::open_added`] (campsite split).
    #[cfg(test)]
    pub(super) fn run_open_barrier(&self) {
        let hook = self
            .open_barrier
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(f) = hook {
            f();
        }
    }

    /// Install the test-only pre-parse rendezvous (see the field doc).
    #[cfg(test)]
    pub(crate) fn set_open_parse_barrier(&self, f: Arc<dyn Fn() + Send + Sync>) {
        *self
            .open_parse_barrier
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(f);
    }

    /// Invoke the pre-parse rendezvous if one is installed. The `Arc` is cloned
    /// out first so the hook runs WITHOUT holding the `open_parse_barrier` lock
    /// (same discipline as [`Self::run_swap_barrier`]). `pub(super)` for
    /// [`super::rebuild::WarmTableRegistry::open_added`] (campsite split).
    #[cfg(test)]
    pub(super) fn run_open_parse_barrier(&self) {
        let hook = self
            .open_parse_barrier
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(f) = hook {
            f();
        }
    }

    /// Test-only: the total accounted footprint (`used_bytes`).
    #[cfg(test)]
    pub(crate) fn debug_used_bytes(&self) -> u64 {
        self.lock_inner().used_bytes
    }

    /// Test-only: the number of cached `WarmReader`s for `key` (counts DUPLICATES,
    /// so the race test can catch a double-installed generation).
    #[cfg(test)]
    pub(crate) fn debug_reader_count(&self, key: &TableKey) -> usize {
        self.lock_inner()
            .tables
            .get(key)
            .map(|t| t.readers.len())
            .unwrap_or(0)
    }

    /// Test-only: the number of DISTINCT generation ids cached for `key`.
    #[cfg(test)]
    pub(crate) fn debug_distinct_gen_count(&self, key: &TableKey) -> usize {
        self.lock_inner()
            .tables
            .get(key)
            .map(|t| t.readers.iter().map(|r| r.id).collect::<HashSet<_>>().len())
            .unwrap_or(0)
    }
}

// `reader_backing_present` + `open_one_reader` moved to [`super::rebuild`]
// (issue #2383, campsite split).

// Registry integration tests over real in-process SSTables (issue #2310), in a
// separate file (campsite rule) loaded here.
#[cfg(test)]
#[path = "registry_tests.rs"]
mod registry_tests;

// Issue #2383 resolve-phase CPU-spin RED repros (rebind / single-flight / cancel
// granularity), in a separate file (campsite rule) loaded here.
#[cfg(test)]
#[path = "spin_tests_2383.rs"]
mod spin_tests_2383;

// Issue #2412 §D (Stage 5) — the warm registry pins summary-only index memory,
// in a separate file (campsite rule) loaded here.
#[cfg(test)]
#[path = "summary_only_memory_tests.rs"]
mod summary_only_memory_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_key_equality_and_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TableKey::new("ks", "t"));
        assert!(set.contains(&TableKey::new("ks", "t")));
        assert!(!set.contains(&TableKey::new("ks", "other")));
    }
}
