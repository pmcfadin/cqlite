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
//! ## UDT-registry contract (WS1 #2345, from the #2346 review)
//!
//! The reader-based merge seam `KWayMerger::new_from_readers` takes NO
//! `udt_registry` parameter (a shared `Arc` reader has no `&mut self` for
//! `set_udt_registry`). So the registry MUST open each reader WITH its UDT
//! registry already resolved BEFORE wrapping it in `Arc` — otherwise a
//! frozen/nested UDT cell silently decodes as `Blob` (the #1234 data-loss class),
//! NOT an error. [`open_one_reader`] resolves + sets the registry and asserts it
//! is present via `SSTableReader::has_udt_registry`. (A Flight ticket carries a
//! single-table DDL with no `CREATE TYPE` bodies, so the resolved registry is the
//! Cassandra-5 default set — the same authority the cold path had; the plumbing
//! is present and provable so a future ticket that DOES carry UDT bodies is
//! decoded structurally, never dropped.)
//!
//! ## File-lifetime contract (from `from_readers.rs`)
//!
//! `new_from_readers` requires the backing `Data.db` not be deleted while any
//! `Arc<SSTableReader>` clone is alive. This registry only ever evicts its OWN
//! reference (per #1749's fail-closed model) and never deletes/replaces a
//! generation's file out from under a live `Arc`; a snapshot's hardlinked inode
//! outlives the per-query dir (the link keeps the inode alive), so the contract
//! is satisfied trivially.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use cqlite_core::schema::{TableSchema, UdtRegistry};
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
        // OUTSIDE the lock (slow I/O).
        let cached_ids: Vec<GenerationId> = {
            let inner = self.lock_inner();
            inner
                .tables
                .get(key)
                .filter(|t| t.ddl_hash == ddl_hash)
                .map(|t| t.readers.iter().map(|r| r.id).collect())
                .unwrap_or_default()
        };

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

        // Swap atomically under the write guard.
        let mut inner = self.lock_inner();
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
        let mut readers = kept;
        let mut added_bytes: u64 = 0;
        for mut r in opened {
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

    /// Open the ADDED generations, resolving each reader's UDT registry BEFORE
    /// wrapping in `Arc` (the #2345 contract). Fail-closed: any open error (or a
    /// cancellation) returns immediately WITHOUT partial state.
    fn open_added(
        &self,
        added: &[&GenerationEntry],
        schema: &TableSchema,
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
        let udt_registry = resolve_udt_registry(schema);

        let mut opened = Vec::with_capacity(added.len());
        for entry in added {
            if cancel.is_cancelled() {
                return Err(WarmError::Cancelled);
            }
            let reader = open_one_reader(
                &runtime,
                &entry.path,
                &config,
                Arc::clone(&platform),
                udt_registry.clone(),
            )?;
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
}

/// Open ONE reader and set its UDT registry BEFORE it is shared (the #2345
/// contract). Proves the registry is present via `has_udt_registry`; a reader
/// that somehow lost it is a hard error rather than a silent `Blob`-decoding trap.
fn open_one_reader(
    runtime: &tokio::runtime::Runtime,
    path: &Path,
    config: &Config,
    platform: Arc<Platform>,
    udt_registry: UdtRegistry,
) -> Result<SSTableReader, WarmError> {
    let mut reader = runtime
        .block_on(SSTableReader::open(path, config, platform))
        .map_err(|source| WarmError::Open {
            path: path.to_path_buf(),
            source,
        })?;
    reader.set_udt_registry(udt_registry);
    debug_assert!(
        reader.has_udt_registry(),
        "warm reader must be UDT-registry-aware before sharing (#2345/#1234)"
    );
    Ok(reader)
}

/// Resolve the UDT registry to wire onto a warm reader (the #2345 contract).
///
/// A Flight ticket carries a single-table DDL with no `CREATE TYPE` bodies, so
/// the authoritative registry is the Cassandra-5 default set — the same authority
/// the cold path had. The plumbing is present and provable so a reader whose
/// SSTable holds frozen/nested UDT cells is decoded structurally (never dropped
/// as `Blob`, the #1234 class) once a registry with those bodies is available.
fn resolve_udt_registry(_schema: &TableSchema) -> UdtRegistry {
    UdtRegistry::with_cassandra5_defaults()
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

    #[test]
    fn resolve_udt_registry_is_non_null() {
        // The warm open must always have a registry to set (never `None` → the
        // #1234 silent-Blob trap). A defaults registry is a valid, non-panicking
        // authority.
        let schema = crate::testutil::simple_schema();
        let _reg = resolve_udt_registry(&schema);
    }
}
