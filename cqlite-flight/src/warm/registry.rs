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
//! ## UDT-registry posture — identical to the cold path (WS1 #2345, follow-up #2349)
//!
//! The reader-based merge seam `KWayMerger::new_from_readers` takes NO
//! `udt_registry` parameter (a shared `Arc` reader has no `&mut self` for
//! `set_udt_registry`), so a reader must be opened WITH its registry already
//! resolved BEFORE it is wrapped in `Arc`. The cold Flight path, however, opens
//! its readers via `KWayMerger::new_cancellable`, which passes `udt_registry =
//! None` — so the cold path currently decodes WITHOUT a UDT registry. To keep the
//! spec non-goal (warm is a PARSE-COST change only, never a read-semantics
//! change), the warm path opens readers with the EXACTLY SAME posture: no UDT
//! registry. [`open_one_reader`] therefore does NOT set a registry, and both
//! paths hand the merge readers with `has_udt_registry() == false`. Parity is
//! guaranteed by identical posture, not by matching a non-null authority.
//!
//! Wiring a real UDT registry into BOTH paths (so a frozen/nested UDT cell in a
//! collection decodes structurally instead of as `Blob`, the #1234 data-loss
//! class) is tracked as a single follow-up, issue #2349 — it must land for the
//! cold path and the warm path together so they never diverge.
//!
//! ## File-lifetime contract (from `from_readers.rs`)
//!
//! `new_from_readers` requires the backing `Data.db` not be deleted while any
//! `Arc<SSTableReader>` clone is alive. This registry only ever evicts its OWN
//! reference (per #1749's fail-closed model) and never deletes/replaces a
//! generation's file out from under a live `Arc`; a snapshot's hardlinked inode
//! outlives the per-query dir (the link keeps the inode alive), so the contract
//! is satisfied trivially.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::{Config, Platform};

use crate::cancel::CancelFlag;

use super::budget::{account_footprint, DEFAULT_WARM_BUDGET_BYTES};
use super::identity::{GenerationId, GenerationSet};
use super::metrics::{RefreshOutcome, WarmMetrics};
use super::probe::{self, GenerationEntry, ProbeOutcome};
use super::{WarmError, WarmSet};

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
struct WarmReader {
    id: GenerationId,
    reader: Arc<SSTableReader>,
    footprint: u64,
    last_access: u64,
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
    /// Lazily-built shared platform for reader opens.
    platform: Mutex<Option<Arc<Platform>>>,
    budget_bytes: u64,
    metrics: Arc<WarmMetrics>,
    /// Test-only rendezvous invoked inside [`Self::rebuild`] AFTER the added
    /// generations are opened but BEFORE the swap lock is taken — lets the
    /// concurrent-same-key-rebuild race test hold two threads "past the probe,
    /// before either swaps" deterministically. `None` (a no-op) in production.
    #[cfg(test)]
    swap_barrier: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// Test-only hook invoked at the TOP of each [`Self::open_added`] loop
    /// iteration (before its cancel check) — lets the mid-rebuild cancellation
    /// test trip the cancel flag BETWEEN two added-generation opens. `None` in
    /// production.
    #[cfg(test)]
    open_barrier: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
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
            #[cfg(test)]
            swap_barrier: Mutex::new(None),
            #[cfg(test)]
            open_barrier: Mutex::new(None),
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
    pub fn warm_readers(
        &self,
        key: &TableKey,
        ddl_hash: u64,
        schema: &TableSchema,
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
                // reader-open/parse. A rare race (a concurrent rebuild replaced
                // the entry) falls back to an authoritative re-enumeration.
                if let Some(hit) = self.try_hit(key, ddl_hash, None) {
                    return Ok(hit);
                }
                let entries = probe::enumerate_generations(dir)?;
                let manifest = snapshot_mode.then(|| probe::read_manifest(dir)).flatten();
                self.finish_enumerated(key, ddl_hash, schema, entries, manifest, cancel)
            }
            ProbeOutcome::Enumerated {
                entries, manifest, ..
            } => self.finish_enumerated(key, ddl_hash, schema, entries, manifest, cancel),
        }
    }

    /// Resolve an authoritatively-enumerated generation set into a warm hit (set
    /// unchanged) or a fail-closed delta rebuild.
    fn finish_enumerated(
        &self,
        key: &TableKey,
        ddl_hash: u64,
        schema: &TableSchema,
        entries: Vec<GenerationEntry>,
        manifest: Option<Vec<u8>>,
        cancel: &CancelFlag,
    ) -> Result<WarmSet, WarmError> {
        let current_set = ProbeOutcome::set(&entries);
        if let Some(hit) = self.try_hit(key, ddl_hash, Some((&current_set, manifest.clone()))) {
            return Ok(hit);
        }
        self.rebuild(
            key,
            ddl_hash,
            schema,
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
        let cached_ids: Vec<GenerationId> = {
            let inner = self.lock_inner();
            inner
                .tables
                .get(key)
                .filter(|t| t.ddl_hash == ddl_hash)
                .map(|t| t.readers.iter().map(|r| r.id).collect())
                .unwrap_or_default()
        };
        let probe_start_set = GenerationSet::from_ids(cached_ids.clone());

        // ADDED = present now, not already warm. Open them (fail-closed).
        let added: Vec<&GenerationEntry> = entries
            .iter()
            .filter(|e| !cached_ids.contains(&e.id))
            .collect();
        let opened = match self.open_added(&added, schema, cancel) {
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
        let reader_opens = opened.len() as u64;

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
        if let Some(live) = inner.tables.get(key) {
            if live.ddl_hash == ddl_hash && live.generation_set() != probe_start_set {
                let tick = inner.next_tick();
                let entry = inner.tables.get_mut(key).expect("checked Some above");
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
                    if current_set.contains(&r.id) {
                        kept.push(r);
                    } else {
                        freed = freed.saturating_add(r.footprint);
                        removed_count += 1;
                    }
                }
            } else {
                // DDL changed: the whole prior entry is invalid.
                for r in prev.readers {
                    freed = freed.saturating_add(r.footprint);
                    removed_count += 1;
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
            if let Some(t) = inner.tables.get_mut(&vkey) {
                t.readers.retain(|r| r.id != vid);
                if t.readers.is_empty() {
                    inner.tables.remove(&vkey);
                }
            }
            inner.used_bytes = inner.used_bytes.saturating_sub(vbytes);
            evicted += 1;
        }
        evicted
    }

    /// Open the ADDED generations. Fail-closed: any open error (or a
    /// cancellation) returns immediately WITHOUT partial state. Readers are opened
    /// with the SAME UDT-registry posture as the cold path (none — see the module
    /// doc; #2349 wires a real registry into both paths together).
    fn open_added(
        &self,
        added: &[&GenerationEntry],
        _schema: &TableSchema,
        cancel: &CancelFlag,
    ) -> Result<Vec<WarmReader>, WarmError> {
        if added.is_empty() {
            return Ok(Vec::new());
        }
        // One current-thread runtime per rebuild (reader open is async); reused
        // across every added generation in this rebuild, like `PruneRuntime`.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| WarmError::Runtime(format!("build runtime: {e}")))?;
        let platform = self.platform(&runtime)?;
        let config = Config::default();

        let mut opened = Vec::with_capacity(added.len());
        for entry in added {
            #[cfg(test)]
            self.run_open_barrier();
            if cancel.is_cancelled() {
                return Err(WarmError::Cancelled);
            }
            let reader = open_one_reader(&runtime, &entry.path, &config, Arc::clone(&platform))?;
            let footprint = account_footprint(&entry.path);
            opened.push(WarmReader {
                id: entry.id,
                reader: Arc::new(reader),
                footprint,
                last_access: 0,
            });
        }
        Ok(opened)
    }

    /// Lazily build + cache the shared [`Platform`] used to open readers.
    fn platform(&self, runtime: &tokio::runtime::Runtime) -> Result<Arc<Platform>, WarmError> {
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

    fn lock_inner(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
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

    /// Invoke the per-open rendezvous if one is installed.
    #[cfg(test)]
    fn run_open_barrier(&self) {
        let hook = self
            .open_barrier
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

/// Open ONE reader with the SAME UDT-registry posture as the cold path.
///
/// The cold Flight path (`KWayMerger::new_cancellable`) opens readers with
/// `udt_registry = None`, so — to keep warm a parse-cost-only change with no
/// read-semantics divergence — this does NOT set a registry either. Wiring a real
/// registry into BOTH paths together is issue #2349; see the module doc.
fn open_one_reader(
    runtime: &tokio::runtime::Runtime,
    path: &Path,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<SSTableReader, WarmError> {
    let reader = runtime
        .block_on(SSTableReader::open(path, config, platform))
        .map_err(|source| WarmError::Open {
            path: path.to_path_buf(),
            source,
        })?;
    debug_assert!(
        !reader.has_udt_registry(),
        "warm reader must match the cold path's no-UDT-registry posture (#2349)"
    );
    Ok(reader)
}

// Registry integration tests over real in-process SSTables (issue #2310), in a
// separate file (campsite rule) loaded here.
#[cfg(test)]
#[path = "registry_tests.rs"]
mod registry_tests;

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
