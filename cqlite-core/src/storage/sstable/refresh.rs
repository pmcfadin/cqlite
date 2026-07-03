//! Explicit, concurrency-safe SSTable directory refresh (issue #1749).
//!
//! CQLite opens a data directory once and snapshots the discovered SSTable
//! generations. A Cassandra flush/compaction (or a CQLite `--flush`) may add or
//! remove generations underneath a long-lived handle at any time, but a warm
//! session never re-scans on its own — that is the documented per-surface
//! *freshness contract*: the library handle is a **snapshot at open**, and
//! [`SSTableManager::refresh_tables`] is the ONLY way its reader set changes.
//!
//! `refresh_tables` re-runs the *same* directory discovery that
//! [`StorageEngine::open`](crate::storage::StorageEngine::open) used (TOC /
//! filename-component based — no content sniffing, no heuristics), diffs the
//! discovered canonical `Data.db` paths against the held reader set, and applies
//! the difference:
//!
//! - **added** generations are opened (standard [`SSTableReader::open`]:
//!   bloom/Index/Statistics parsed, the issue #1626 corrupt-`Statistics.db`
//!   hard-fail inherited) and become queryable;
//! - **removed** generations are dropped (the underlying reader closes once the
//!   last in-flight scan's `Arc` clone is dropped);
//! - **unchanged** generations keep their existing `Arc<SSTableReader>` — their
//!   parsed Index/Statistics/bloom state is *not* rebuilt (refresh of an
//!   unchanged directory is ~free).
//!
//! Application is **atomic and fail-closed**: every added generation is opened
//! *before* the write guard is taken, so if any open fails the whole refresh
//! returns that typed error and mutates nothing — no partial view (Decision 3).
//!
//! Concurrency reuses the existing lock discipline (Decision 4): queries resolve
//! their reader list once under the `RwLock` read guard and hold `Arc` clones, so
//! a scan already in flight completes against the pre-refresh set and is never
//! affected by a concurrent refresh. Two concurrent `refresh_tables` calls fully
//! serialize on a dedicated refresh mutex held end-to-end (discovery through
//! swap): the second cannot start until the first has applied its diff, so its
//! discovery set already reflects the first's changes and it becomes a zero-delta
//! no-op — a stale discovery set can never remove a generation a concurrent
//! refresh just added. The refresh mutex is NEVER taken on a query path.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{
    extract_keyspace_and_table_name, extract_table_name, is_apple_double_sidecar, reader,
    SSTableId, SSTableManager, MAX_SSTABLE_SCAN_DEPTH,
};
use crate::Result;

/// Outcome of a single [`SSTableManager::refresh_tables`] call.
///
/// Counts reflect what *this* refresh actually applied under the write guard:
/// two concurrent refreshes serialize, and the second observes the first's
/// changes already applied (so it reports a zero-delta no-op).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshReport {
    /// Number of distinct logical tables present after the refresh.
    pub tables_scanned: usize,
    /// SSTable generations newly opened and added by this refresh.
    pub readers_added: usize,
    /// SSTable generations dropped by this refresh (no longer on disk).
    pub readers_removed: usize,
}

/// How a manager discovers its SSTable generations — recorded at construction so
/// [`SSTableManager::refresh_tables`] re-runs *exactly* the same discovery.
#[derive(Debug, Clone)]
pub(crate) enum DiscoverySource {
    /// Recursively scan `base_path` (mirrors `SSTableManager::new` /
    /// `StorageEngine::open` — the library-handle path).
    BasePath,
    /// Scan a fixed set of pre-discovered table directories (mirrors
    /// `SSTableManager::new_from_discovered_paths`).
    TableDirs(Vec<PathBuf>),
}

/// Compute the `table_readers` key for a Data.db `path` using the **base-path**
/// discovery keying — the single source of truth shared by `load_existing_sstables`
/// and [`SSTableManager::refresh_tables`] so the two can never drift.
///
/// Priority (identical to the original inline logic, issue #680):
///   1. `keyspace.table` from the filesystem path (when the table name is
///      meaningful, i.e. not merely the scan base directory name);
///   2. the unqualified table name from the path (same base-dir exclusion);
///   3. the table name embedded in the SSTable header (last resort).
pub(crate) fn base_path_table_key(
    path: &Path,
    base_dir_name: &str,
    header_table_name: &str,
) -> Option<String> {
    if let Some((keyspace, table_name)) = extract_keyspace_and_table_name(path) {
        if table_name.as_str() != base_dir_name {
            return Some(format!("{}.{}", keyspace, table_name));
        }
    }
    if let Some(name) = extract_table_name(path) {
        if name.as_str() != base_dir_name {
            return Some(name);
        }
    }
    if header_table_name != "test_table" && !header_table_name.is_empty() {
        return Some(header_table_name.to_string());
    }
    None
}

/// Compute the `table_readers` key for a Data.db `path` using the
/// **table-directories** discovery keying — the single source of truth shared by
/// `load_from_table_directories` and [`SSTableManager::refresh_tables`].
pub(crate) fn table_dir_table_key(path: &Path) -> Option<String> {
    if let Some((keyspace, table_name)) = extract_keyspace_and_table_name(path) {
        Some(format!("{}.{}", keyspace, table_name))
    } else {
        extract_table_name(path)
    }
}

/// Canonicalize `p` for stable diffing, falling back to the path as-given when
/// the file no longer exists (a removed generation) or cannot be canonicalized.
/// Never panics — uses `unwrap_or_else`, not `unwrap`.
fn canon(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

impl SSTableManager {
    /// Open an `SSTableReader` at `path` and wire the schema/UDT registries onto
    /// it (when the `state_machine` feature is enabled), returning a shared
    /// handle. Extracted so the initial load paths and refresh open readers
    /// identically. The `SSTableReader::open` error — including the issue #1626
    /// corrupt-`Statistics.db` hard-fail — is propagated to the caller, which
    /// decides whether to skip (initial best-effort load) or abort (fail-closed
    /// refresh).
    pub(crate) async fn open_reader_with_schema(
        &self,
        path: &Path,
    ) -> Result<Arc<reader::SSTableReader>> {
        #[cfg_attr(not(feature = "state_machine"), allow(unused_mut))]
        let mut reader =
            reader::SSTableReader::open(path, &self.config, self.platform.clone()).await?;

        #[cfg(feature = "state_machine")]
        {
            let schema_reg_guard = self.schema_registry.read().await;
            if let Some(ref registry_rwlock) = *schema_reg_guard {
                reader.set_schema_registry(Arc::clone(registry_rwlock));

                // Pre-resolve the registry schema into the reader's sync cache here,
                // in this async context (issue #1692, AG3). The sync schema-fallback
                // tier of `get_table_schema` then reads a plain field instead of
                // `block_on`-ing the async registry on a tokio worker thread.
                reader.resolve_registry_schema().await;

                // Also set the UDT registry for UDT-aware collection parsing (Issue #238).
                let schema_registry = registry_rwlock.read().await;
                let udt_registry_lock = schema_registry.get_udt_registry();
                let udt_registry = udt_registry_lock.read().await.clone();
                reader.set_udt_registry(udt_registry);
            }
        }

        Ok(Arc::new(reader))
    }

    /// List the current on-disk `Data.db` paths using the manager's recorded
    /// [`DiscoverySource`] — the same discovery the manager was built with.
    async fn discover_data_file_paths(&self) -> Result<Vec<PathBuf>> {
        match &self.discovery_source {
            DiscoverySource::BasePath => {
                if !self.platform.fs().exists(&self.base_path).await? {
                    return Ok(Vec::new());
                }
                SSTableManager::find_data_files(
                    &self.platform,
                    &self.base_path,
                    MAX_SSTABLE_SCAN_DEPTH,
                )
                .await
            }
            DiscoverySource::TableDirs(dirs) => {
                let mut out = Vec::new();
                for dir in dirs {
                    if !self.platform.fs().exists(dir).await? {
                        continue;
                    }
                    let mut entries = match self.platform.fs().read_dir(dir).await {
                        Ok(entries) => entries,
                        Err(_) => continue,
                    };
                    while let Some(entry) = entries.next_entry().await? {
                        let path = entry.path();
                        if let Some(fname) = path.file_name().and_then(|n| n.to_str()) {
                            if fname.ends_with("-Data.db") && !is_apple_double_sidecar(fname) {
                                out.push(path);
                            }
                        }
                    }
                }
                Ok(out)
            }
        }
    }

    /// Compute the `table_readers` key for a discovered `path`/`reader` under the
    /// manager's discovery keying.
    fn table_key_for(&self, path: &Path, reader: &reader::SSTableReader) -> Option<String> {
        match &self.discovery_source {
            DiscoverySource::BasePath => {
                let base_dir_name = self
                    .base_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("");
                base_path_table_key(path, base_dir_name, &reader.header().table_name)
            }
            DiscoverySource::TableDirs(_) => table_dir_table_key(path),
        }
    }

    /// Re-scan the data directory and atomically apply added/removed SSTable
    /// generations to the held reader set. See the [module docs](self) for the
    /// full contract (snapshot-at-open, explicit refresh, in-flight isolation,
    /// fail-closed atomicity).
    pub async fn refresh_tables(&self) -> Result<RefreshReport> {
        // 0. Serialize the WHOLE refresh (discovery through swap) so two
        //    concurrent refreshes cannot interleave: without this, a refresh with
        //    a stale discovery set (step 1) could, at swap time (step 5), remove a
        //    generation a concurrent refresh already added (TOCTOU, Decision 4).
        //    Held only across the refresh; queries never take this lock.
        let _refresh_guard = self.refresh_lock.lock().await;

        // 1. Rediscover the current on-disk Data.db set (no readers opened yet).
        let discovered_paths = self.discover_data_file_paths().await?;

        // Precompute a raw-path -> canonical-path cache for EVERY path this
        // refresh will diff. Filesystem canonicalization happens ONLY here (and
        // in step 4 for freshly opened readers), never inside the write-guarded
        // critical section (step 5), which must perform zero syscalls. A
        // reader's `file_path` is immutable and `canon` is a pure function of
        // path + filesystem, so a cache built now stays valid for the whole
        // call.
        let mut canon_cache: HashMap<PathBuf, PathBuf> =
            HashMap::with_capacity(discovered_paths.len());
        for p in &discovered_paths {
            canon_cache.entry(p.clone()).or_insert_with(|| canon(p));
        }

        // 2. Snapshot the canonical paths currently held (short read guards),
        //    extending the cache with every held reader's file_path so the
        //    guarded section can resolve them without syscalls. Union both maps
        //    so no held generation is missed even under a pre-existing
        //    SSTableId (filename) collision across keyspaces.
        let mut held_canon: HashSet<PathBuf> = HashSet::new();
        {
            let readers = self.readers.read().await;
            let table_readers = self.table_readers.read().await;
            for r in readers.values().chain(table_readers.values().flatten()) {
                let c = canon_cache
                    .entry(r.file_path.clone())
                    .or_insert_with(|| canon(&r.file_path))
                    .clone();
                held_canon.insert(c);
            }
        }

        // Cache-only canonicalization: reads the precomputed map, falling back
        // to the raw path (NO syscall) for any path not seen during precompute
        // — matching `canon`'s own fallback for an un-canonicalizable path.
        let canon_of = |p: &Path| -> PathBuf {
            canon_cache
                .get(p)
                .cloned()
                .unwrap_or_else(|| p.to_path_buf())
        };

        let discovered_canon: HashSet<PathBuf> =
            discovered_paths.iter().map(|p| canon_of(p)).collect();

        // 3. Candidate additions = discovered paths not already held.
        let added_paths: Vec<PathBuf> = discovered_paths
            .iter()
            .filter(|p| !held_canon.contains(&canon_of(p)))
            .cloned()
            .collect();

        // 4. Open every added generation OUTSIDE the write guard. Fail-closed:
        //    the first open error aborts the whole refresh, mutating nothing.
        let mut opened: Vec<(
            PathBuf,
            SSTableId,
            Option<String>,
            Arc<reader::SSTableReader>,
        )> = Vec::with_capacity(added_paths.len());
        for path in &added_paths {
            let Some(filename) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let sstable_id = SSTableId::from_filename(filename);
            let reader_arc = self.open_reader_with_schema(path).await?;
            let key = self.table_key_for(path, &reader_arc);
            opened.push((canon_of(path), sstable_id, key, reader_arc));
        }

        // 5. Apply the diff under the write guards (short critical section).
        let mut readers = self.readers.write().await;
        let mut table_readers = self.table_readers.write().await;

        // 5a. Removal: retain only readers still present on disk. Every
        //     canonical path below comes from the precomputed cache — the
        //     guarded section performs zero filesystem syscalls.
        let before_canon: HashSet<PathBuf> = readers
            .values()
            .map(|r| canon_of(&r.file_path))
            .chain(
                table_readers
                    .values()
                    .flatten()
                    .map(|r| canon_of(&r.file_path)),
            )
            .collect();

        readers.retain(|_id, r| discovered_canon.contains(&canon_of(&r.file_path)));
        for list in table_readers.values_mut() {
            list.retain(|r| discovered_canon.contains(&canon_of(&r.file_path)));
        }
        table_readers.retain(|_key, list| !list.is_empty());

        let after_removal_canon: HashSet<PathBuf> = readers
            .values()
            .map(|r| canon_of(&r.file_path))
            .chain(
                table_readers
                    .values()
                    .flatten()
                    .map(|r| canon_of(&r.file_path)),
            )
            .collect();
        let readers_removed = before_canon.difference(&after_removal_canon).count();

        // 5b. Addition: insert the pre-opened readers, skipping any a concurrent
        //     refresh already added (preserving that refresh's warm Arc).
        let mut readers_added = 0usize;
        for (cpath, sstable_id, key, reader_arc) in opened {
            if after_removal_canon.contains(&cpath) {
                continue;
            }
            // Guard against inserting the same freshly-opened path twice if the
            // discovery listed a duplicate (defensive; discovery does not).
            if readers.values().any(|r| canon_of(&r.file_path) == cpath) {
                continue;
            }
            readers.insert(sstable_id, Arc::clone(&reader_arc));
            if let Some(key) = key {
                table_readers.entry(key).or_default().push(reader_arc);
            }
            readers_added += 1;
        }

        let tables_scanned = table_readers.len();

        Ok(RefreshReport {
            tables_scanned,
            readers_added,
            readers_removed,
        })
    }
}

#[cfg(all(test, feature = "write-support", feature = "state_machine"))]
mod tests {
    use super::*;
    use crate::schema::{Column, KeyColumn, TableSchema};
    use crate::storage::write_engine::{
        CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
    };
    use crate::types::Value;
    use crate::{Config, Platform};
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn users_schema() -> TableSchema {
        TableSchema {
            keyspace: "ks_ptr".to_string(),
            table: "users".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "value".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    async fn flush_one_partition(data_dir: &Path, wal_dir: &Path, id: i32) {
        let config = WriteEngineConfig::new(
            data_dir.to_path_buf(),
            wal_dir.to_path_buf(),
            users_schema(),
        );
        let mut engine = WriteEngine::new(config).expect("write engine");
        let table_id = TableId::new("ks_ptr", "users");
        let pk = PartitionKey::single("id", Value::Integer(id));
        let ops = vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text(format!("v{}", id)),
        }];
        engine
            .write_async(Mutation::new(
                table_id,
                pk,
                None,
                ops,
                1_000 + id as i64,
                None,
            ))
            .await
            .expect("write");
        engine.flush().await.expect("flush");
    }

    /// Build a source dir holding two generations (`nb-1` = partition 1, `nb-2` =
    /// partition 2), returning the `.../ks_ptr/users` table directory.
    async fn build_two_generations(root: &Path) -> PathBuf {
        let data_dir = root.join("data");
        let wal_dir = root.join("wal");
        let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, users_schema());
        let mut engine = WriteEngine::new(config).expect("write engine");
        for id in [1_i32, 2_i32] {
            let table_id = TableId::new("ks_ptr", "users");
            let pk = PartitionKey::single("id", Value::Integer(id));
            let ops = vec![CellOperation::Write {
                column: "value".to_string(),
                value: Value::Text(format!("v{}", id)),
            }];
            engine
                .write_async(Mutation::new(
                    table_id,
                    pk,
                    None,
                    ops,
                    1_000 + id as i64,
                    None,
                ))
                .await
                .expect("write");
            engine.flush().await.expect("flush");
        }
        data_dir.join("ks_ptr").join("users")
    }

    fn copy_generation(src_table_dir: &Path, dst_table_dir: &Path, gen: u32) {
        std::fs::create_dir_all(dst_table_dir).expect("mkdir dst");
        let prefix = format!("nb-{}-big-", gen);
        let mut copied = 0;
        for entry in std::fs::read_dir(src_table_dir).expect("read src") {
            let entry = entry.expect("entry");
            let name = entry.file_name();
            let name = name.to_str().expect("utf8");
            if name.starts_with(&prefix) {
                std::fs::copy(entry.path(), dst_table_dir.join(name)).expect("copy");
                copied += 1;
            }
        }
        assert!(copied > 0, "copied generation {} components", gen);
    }

    fn scan_partition_ids(
        rows: &[(crate::RowKey, crate::ScanRow)],
    ) -> std::collections::BTreeSet<i32> {
        rows.iter()
            .map(|(key, _)| {
                let b = key.as_bytes();
                assert_eq!(b.len(), 4, "int pk is 4 bytes");
                i32::from_be_bytes([b[0], b[1], b[2], b[3]])
            })
            .collect()
    }

    /// Spec scenario "In-flight scan unaffected by concurrent refresh".
    ///
    /// A scan resolves its reader list once (under the read guard) and holds
    /// `Arc` clones; a scan's output is therefore a pure function of the reader
    /// set it captured. This test pins the isolation invariant deterministically
    /// (no timing): it captures the exact `Arc` reader set an in-flight scan
    /// would hold on the pre-refresh directory, adds a generation and refreshes,
    /// and asserts (a) the captured set is byte-for-byte unchanged by the refresh
    /// — so a scan bound to it still yields the pre-refresh content {1} — and
    /// (b) a scan started AFTER the refresh sees the post-refresh content {1, 2}.
    #[tokio::test]
    async fn in_flight_scan_unaffected_by_concurrent_refresh() {
        let src = TempDir::new().expect("tmp src");
        let src_table_dir = build_two_generations(src.path()).await;

        let live = TempDir::new().expect("tmp live");
        let live_table_dir = live.path().join("ks_ptr").join("users");
        copy_generation(&src_table_dir, &live_table_dir, 1);

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let manager = SSTableManager::new(
            live.path(),
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("manager");

        let schema = users_schema();
        let table_id = crate::types::TableId::new("ks_ptr.users");

        // Pre-refresh scan content is {1}; capture the exact Arc reader set an
        // in-flight scan would hold (the read-guard snapshot the scan clones).
        let pre_rows = manager
            .scan(&table_id, None, None, None, Some(&schema))
            .await
            .expect("pre scan");
        assert_eq!(
            scan_partition_ids(&pre_rows),
            std::collections::BTreeSet::from([1]),
            "pre-refresh scan content is gen-1 only"
        );
        let held_snapshot: Vec<Arc<reader::SSTableReader>> = {
            let table_readers = manager.table_readers.read().await;
            table_readers.values().flatten().map(Arc::clone).collect()
        };
        assert_eq!(
            held_snapshot.len(),
            1,
            "in-flight scan holds one gen-1 reader"
        );

        // Add gen-2 and refresh while that snapshot is "in flight".
        copy_generation(&src_table_dir, &live_table_dir, 2);
        let report = manager.refresh_tables().await.expect("refresh");
        assert_eq!(report.readers_added, 1, "gen-2 added");

        // (a) The captured in-flight reader set is unchanged by the refresh: the
        //     same single gen-1 Arc (pointer identity), and NOT the gen-2 reader.
        //     A scan bound to this unchanged set is therefore still exactly {1}.
        let post_snapshot: Vec<Arc<reader::SSTableReader>> = {
            let table_readers = manager.table_readers.read().await;
            table_readers.values().flatten().map(Arc::clone).collect()
        };
        assert_eq!(post_snapshot.len(), 2, "manager now holds both generations");
        assert_eq!(
            held_snapshot.len(),
            1,
            "the in-flight snapshot is untouched by refresh"
        );
        assert!(
            post_snapshot
                .iter()
                .any(|r| Arc::ptr_eq(r, &held_snapshot[0])),
            "the pre-refresh gen-1 reader Arc survives the refresh unchanged"
        );
        let gen2_reader = post_snapshot
            .iter()
            .find(|r| !Arc::ptr_eq(r, &held_snapshot[0]))
            .expect("gen-2 reader present post-refresh");
        assert!(
            !Arc::ptr_eq(gen2_reader, &held_snapshot[0]),
            "the added gen-2 reader is NOT part of the in-flight snapshot (isolation)"
        );

        // (b) A scan started AFTER the refresh sees the post-refresh set {1, 2}.
        let post_rows = manager
            .scan(&table_id, None, None, None, Some(&schema))
            .await
            .expect("post scan");
        assert_eq!(
            scan_partition_ids(&post_rows),
            std::collections::BTreeSet::from([1, 2]),
            "post-refresh scan sees the new generation"
        );
    }

    /// Spec scenario "Unchanged directory is a cheap no-op": a refresh over an
    /// unchanged directory reports a zero delta AND keeps the exact same reader
    /// `Arc` objects (pointer identity — warm state preserved).
    #[tokio::test]
    async fn refresh_noop_preserves_reader_arc_identity() {
        let tmp = TempDir::new().expect("tmp");
        let data_dir = tmp.path().join("data");
        let wal_dir = tmp.path().join("wal");
        flush_one_partition(&data_dir, &wal_dir, 1).await;

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let manager = SSTableManager::new(
            &data_dir,
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("manager");

        // Snapshot the reader Arc(s) for the table before refresh.
        let before: Vec<Arc<reader::SSTableReader>> = {
            let table_readers = manager.table_readers.read().await;
            table_readers.values().flatten().map(Arc::clone).collect()
        };
        assert_eq!(before.len(), 1, "one generation expected pre-refresh");

        let report = manager.refresh_tables().await.expect("refresh");
        assert_eq!(report.readers_added, 0, "no-op: nothing added");
        assert_eq!(report.readers_removed, 0, "no-op: nothing removed");

        let after: Vec<Arc<reader::SSTableReader>> = {
            let table_readers = manager.table_readers.read().await;
            table_readers.values().flatten().map(Arc::clone).collect()
        };
        assert_eq!(after.len(), 1, "still one generation after no-op refresh");
        assert!(
            Arc::ptr_eq(&before[0], &after[0]),
            "no-op refresh must keep the SAME reader Arc (warm state preserved)"
        );
    }

    /// Two SEQUENTIAL refreshes with a generation added between them must both
    /// apply correctly: the first is a no-op over the unchanged directory, the
    /// second picks up the newly-added generation. Deterministic (no timing).
    #[tokio::test]
    async fn sequential_refreshes_each_apply_correctly() {
        let src = TempDir::new().expect("tmp src");
        let src_table_dir = build_two_generations(src.path()).await;

        let live = TempDir::new().expect("tmp live");
        let live_table_dir = live.path().join("ks_ptr").join("users");
        copy_generation(&src_table_dir, &live_table_dir, 1);

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let manager = SSTableManager::new(
            live.path(),
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("manager");

        // First refresh over the unchanged directory: zero delta.
        let first = manager.refresh_tables().await.expect("first refresh");
        assert_eq!(first.readers_added, 0, "first refresh adds nothing");
        assert_eq!(first.readers_removed, 0, "first refresh removes nothing");

        // Add gen-2, then refresh again: the second refresh picks it up.
        copy_generation(&src_table_dir, &live_table_dir, 2);
        let second = manager.refresh_tables().await.expect("second refresh");
        assert_eq!(second.readers_added, 1, "second refresh adds gen-2");
        assert_eq!(second.readers_removed, 0, "second refresh removes nothing");

        let held = {
            let table_readers = manager.table_readers.read().await;
            table_readers
                .values()
                .flatten()
                .map(Arc::clone)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            held.len(),
            2,
            "both generations held after the two refreshes"
        );
    }

    /// Regression for the TOCTOU race (issue #1749, Decision 4): N `refresh()`
    /// calls launched concurrently after a generation is added must serialize on
    /// the refresh mutex. The final reader set must equal the on-disk truth
    /// ({gen-1, gen-2}), EXACTLY ONE refresh may report the addition (the rest are
    /// zero-delta no-ops), and NO reader may vanish (a stale-discovery refresh must
    /// not remove what a concurrent refresh just added). Deterministic: correctness
    /// is asserted on the final state and the aggregate deltas, with no timing.
    #[tokio::test]
    async fn concurrent_refreshes_serialize_no_reader_vanishes() {
        let src = TempDir::new().expect("tmp src");
        let src_table_dir = build_two_generations(src.path()).await;

        let live = TempDir::new().expect("tmp live");
        let live_table_dir = live.path().join("ks_ptr").join("users");
        copy_generation(&src_table_dir, &live_table_dir, 1);

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let manager = SSTableManager::new(
            live.path(),
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("manager");

        // Add gen-2 on disk, then fire several refreshes concurrently.
        copy_generation(&src_table_dir, &live_table_dir, 2);
        let (r0, r1, r2, r3) = tokio::join!(
            manager.refresh_tables(),
            manager.refresh_tables(),
            manager.refresh_tables(),
            manager.refresh_tables(),
        );
        let reports = [
            r0.expect("refresh 0"),
            r1.expect("refresh 1"),
            r2.expect("refresh 2"),
            r3.expect("refresh 3"),
        ];

        // Exactly one refresh applied the +1 addition; the rest are no-ops. No
        // refresh may report a removal (nothing left the directory).
        let total_added: usize = reports.iter().map(|r| r.readers_added).sum();
        let total_removed: usize = reports.iter().map(|r| r.readers_removed).sum();
        assert_eq!(
            total_added, 1,
            "exactly one serialized refresh adds gen-2 (others are no-ops)"
        );
        assert_eq!(
            total_removed, 0,
            "no refresh may remove a reader a concurrent refresh just added"
        );

        // Final held set equals the on-disk truth: both generations, none vanished.
        let held = {
            let table_readers = manager.table_readers.read().await;
            table_readers
                .values()
                .flatten()
                .map(Arc::clone)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            held.len(),
            2,
            "final reader set equals on-disk truth ({{gen-1, gen-2}})"
        );
        let scanned = manager
            .scan(
                &crate::types::TableId::new("ks_ptr.users"),
                None,
                None,
                None,
                Some(&users_schema()),
            )
            .await
            .expect("post scan");
        assert_eq!(
            scan_partition_ids(&scanned),
            std::collections::BTreeSet::from([1, 2]),
            "both partitions queryable after concurrent refreshes"
        );
    }
}
