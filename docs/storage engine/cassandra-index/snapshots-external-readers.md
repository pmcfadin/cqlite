# Snapshots & External Readers: Cassandra SSTable Protection Patterns

## Summary

Cassandra snapshots provide external processes (like CQLite's Arrow Flight reader) a stable, hardlink-based view of SSTables without reading live data. The mechanism: `TakeSnapshotTask` creates hardlinks to active SSTables into a timestamped `snapshots/<tag>/` directory tree, with lifecycle guarantees through reference counting (SSTableReader's GlobalTidy/obsoletion model). External readers holding snapshots are immune to concurrent compaction/deletion as long as they remain within the snapshot directory. However, **no seam currently exists** to externally query live memtable data + all flushed SSTables in a single read — answering Q1 requires new Cassandra instrumentation (CEP-11 Memtable API on trunk opens a path).

---

## Key Classes & Interfaces

| Class | File | Responsibility |
|-------|------|-----------------|
| **SnapshotManager** | `service/snapshot/SnapshotManager.java:61` | Singleton managing snapshot lifecycle; schedules periodic cleanup; registers/unregisters MBean; maintains COW snapshot list for low-latency reads |
| **TableSnapshot** | `service/snapshot/TableSnapshot.java:65` | Immutable metadata (keyspace, table, tag, createdAt, expiresAt, snapshotDirs Set<File>, ephemeral flag); tracks completion state (incomplete → complete lifecycle); `getId()` → dedup key |
| **TakeSnapshotTask** | `service/snapshot/TakeSnapshotTask.java:56` | Executes snapshot creation; flushes memtable if needed; calls `SSTableReader.createLinks()` for each SSTable; writes manifest + schema to snapshot dirs |
| **ClearSnapshotTask** | `service/snapshot/ClearSnapshotTask.java:37` | Removes snapshot metadata from manager; optionally deletes snapshot directory tree (hardlinks can be reclaimed once ref count hits 0) |
| **SSTableReader.GlobalTidy** | `io/sstable/format/SSTableReader.java:1695` | Static ConcurrentMap<Descriptor, Ref<GlobalTidy>>; holds obsoletion Runnable; runs cleanup when all InstanceTidier refs released; **never deletes hardlinked files** |
| **SSTableReader.InstanceTidier** | `io/sstable/format/SSTableReader.java:1571` | Per-instance tidier; holds ref to GlobalTidy; released when SSTableReader closes; runs async cleanup barrier + closeables |
| **Directories** | `db/Directories.java:*` | Static methods: `getSnapshotDirectory(Descriptor, String)` → create if missing; `getSnapshotDirectoryWithoutCreation()` → no-create variant; paths: `<cf dir>/snapshots/<snapshot name>/` or `<cf dir>/snapshots/<snapshot name>/.<index name>` for indexes |
| **FileUtils** | `io/util/FileUtils.java:127` | `createHardLink(File from, File to)` → wraps `Files.createLink(to.toPath(), from.toPath())`; throws if target exists; rate-limitable via RateLimiter |
| **SnapshotManifest** | `service/snapshot/SnapshotManifest.java:*` | JSON manifest written to `snapshots/<tag>/manifest.json`; lists DATA components + TTL + createdAt + ephemeral flag |

---

## Extension Points & Pluggability

1. **Snapshot Type Filter** (SSTableReader predicate)
   - `SnapshotOptions.sstableFilter`: `Predicate<SSTableReader>` filter at `TakeSnapshotTask.createSnapshot():161`
   - Allows selective snapshot of SSTables (e.g., repaired-only)
   - Entry point: `systemSnapshot(tag, type, sstableFilter, entities)` → `SnapshotOptions.java:84`

2. **Rate Limiter for Hardlink Creation**
   - `SnapshotOptions.rateLimiter`: `RateLimiter` passed to `SSTableReader.createLinks(snapshotPath, rateLimiter, ephemeral)` at `TakeSnapshotTask:166`
   - Throttles hardlink ops per sec (configurable via MBean `SnapshotManagerMBean.setHardlinkThrottlePerSecond()` → `SnapshotManagerMBean.java:113`)
   - No JMX seam to alter rate mid-snapshot; set before task execution

3. **Snapshot Manager Lifecycle**
   - `SnapshotManagerMBean` (JMX): list, take, clear snapshots; set hardlink throttle
   - `INotificationConsumer` (CassandraNotificationHub): reacts to `TableDroppedNotification`, `TablePreScrubNotification`, `TruncationNotification` → auto-clear relevant snapshots
   - Cleanup executor: `ScheduledExecutorPlus snapshotCleanupExecutor` → configurable cleanup interval via `SNAPSHOT_CLEANUP_PERIOD_SECONDS` property

4. **Ephemeral Snapshot Mode**
   - `SnapshotOptions.ephemeral`: boolean flag; passed to `createLinks(…, ephemeralSnapshot)` → `SSTableReader.java:1164`
   - Affects macOS hardlink case-sensitivity workaround (`TableSnapshot.java:429`, platform-aware via `PlatformEnum.MACOS`)
   - Marker file: `snapshots/<tag>/ephemeral.snapshot` → loaded via manifest

5. **SSTable Format Hooks (Trunk)**
   - `SSTableFormat` interface (pluggable format API on trunk): no direct snapshot coupling, but new format implementations inherit `createLinks()` from SSTableReader
   - Snapshot creation is format-agnostic: hardlinks are component files (Data.db, Index.db, etc.), not format-specific

---

## Hard Couplings (Breakage Points for Alternative Engines)

1. **Hardlink Dependency**
   - Snapshots **require filesystem hardlink support** (via `Files.createLink()`)
   - External readers must be on same filesystem as source SSTables
   - **Failure mode**: snapshot creation throws `FSWriteError` on filesystems without hardlink support (e.g., S3, some network mounts)
   - **Workaround path**: `createHardLinkWithoutConfirm()` skips errors; used rarely in codebase

2. **Reference Counting Tight Coupling (SSTableReader ↔ GlobalTidy)**
   - External readers MUST hold `Ref<SSTableReader>` while reading snapshots
   - Once all Refs released → GlobalTidy.tidy() → obsoletion Runnable executes → **file deletion**
   - **Risk for external readers**: if reader closes refs before scan finishes, underlying snapshot hardlinks become eligible for deletion by next maintenance task
   - No API to "freeze" an SSTable indefinitely without holding a Ref

3. **Snapshot Directory Layout is Hardcoded**
   - Path structure: `<data_dir>/tables/<keyspace>/<table_id>/snapshots/<tag>/`
   - No hook to customize snapshot directory hierarchy
   - Indexes hardcoded to: `<data_dir>/tables/<keyspace>/<table_id>/snapshots/<tag>/.<index_name>/`
   - External discovery must parse this structure or use SnapshotManager JMX

4. **Flush Required Before Snapshot (by Default)**
   - `TakeSnapshotTask.call():128` → flushes memtable unless `skipFlush=true`
   - Memtable write-path snapshot (`performSnapshot()`) is separate from SSTable snapshot
   - **Caveat**: skipFlush=true creates incomplete snapshot (memtable data not in SSTable yet)
   - External reader on live data cannot see unflushed writes via snapshot

5. **No Cross-Node Coordination in Snapshot API**
   - `SnapshotManager` is per-node; no built-in cluster-wide snapshot semantics
   - Distributed snapshot requires external orchestration (e.g., Cassandra backup tools, third-party)
   - Snapshot TTL + expiration cleanup is local only

6. **SSTable Descriptor Coupling**
   - Snapshot hardlinks indexed by `Descriptor` (keyspace, table, sstable_id, version, component)
   - Changing Descriptor format (e.g., new SSTable version) could break snapshot dir traversal
   - `SnapshotLoader.loadSnapshots()` parses descriptor from filesystem paths → tight coupling to naming scheme

7. **Memtable Flush Ordering Implicit**
   - No guarantee that snapshot captures consistent view of multiple tables
   - Each table's memtable flush → SSTable creation → snapshot is independent
   - External reader expecting cross-table consistency must implement own barriers

---

## Q1 Relevance: Analytical Read Sees All Node-Local State

**Problem**: DataFusion/Trino reading via CQLite Arrow Flight sees only flushed SSTables, not current memtable contents.

**Current Gap**: 
- Snapshots freeze SSTable set, not memtable state
- `TakeSnapshotTask.skipFlush=true` can defer flush but doesn't expose memtable data in snapshot
- No API to atomically snapshot(SSTable set + memtable iter)

**Trunk Path (CEP-11)**:
- `src/java/org/apache/cassandra/db/memtable/Memtable_API.md` documents pluggable Memtable interface
- Experimental hook: `Memtable.performSnapshot(snapshotName)` → custom impl could write memtable contents to snapshot dir
- **Still requires CQLite to**: parse memtable dump format + merge with SSTable reads in Flight connector

**Missing Seam**: No unified "read all live data from this node at timestamp T" API. Would require:
1. Freezing memtable (or draining to SSTable)
2. Snapshot SSTable set
3. OR: Custom Memtable impl exporting snapshot-compatible format
4. OR: Extend Flight connector to query Cassandra's query engine directly (bypasses snapshot limitation, hits write-path lock contention)

---

## Q2 Relevance: Alternative/Adjacent Storage Engine Feasibility

**Alternative Engine In-JVM**:
- Snapshot mechanism is **format-agnostic** (just hardlinks components)
- Pluggability: `SSTableFormat` interface on trunk (pluggable format SPI)
- **Blocker**: must implement `SSTableReader` contract (reference counting, GlobalTidy coupling)
- External deletion cleanup via obsoletion model is **tightly coupled to reference counting**
- **Verdict**: feasible but requires implementing SSTableReader's tidy model + hardlink support

**Adjacent OLAP Engine (Sidecar)**:
- Snapshots are **perfect for sidecar access** (stable, no locking)
- Take snapshot → external process reads from `snapshots/<tag>/` → zero interference with Cassandra writes
- **Gotcha**: memtable data not included (see Q1)
- **No seam**: Cassandra doesn't expose snapshot locations via structured API; sidecar must parse filesystem or use JMX

**Write Support in CQLite**:
- Snapshots **protect against SSTable deletion**, not memtable writes
- CQLite writes create own SSTable files in write-dir (out of Cassandra's scope)
- Compaction on CQLite-written files isn't integrated with Cassandra snapshot lifecycle
- **Verdict**: CQLite can read snapshots safely; write integration requires out-of-tree orchestration

---

## Trunk-vs-5.0 Deltas

| Aspect | Trunk | Cassandra 5.0 |
|--------|-------|---------------|
| **Memtable API** | CEP-11 pluggable Memtable (new; see `db/memtable/Memtable_API.md`) | Hardcoded SkipListMemtable + optional alternative (no stable SPI) |
| **SSTableFormat API** | Pluggable format SPI (new; versioning hooks) | BIG (na/nb) + BTI (da) as hardcoded implementations |
| **Snapshot Type Enum** | `SnapshotType` (USER, MANUAL, PERIODIC, AUTO) | Same; no trunk changes evident |
| **Hardlink Rate Limiting** | MBean throttle (`setHardlinkThrottlePerSecond`) | Same |
| **Snapshot Manifest Format** | JSON + ephemeral flag in manifest | Likely same; no code drift in `SnapshotManifest` |
| **Cleanup Executor** | ScheduledExecutorPlus; configurable cleanup period | Same; cleanup disabled/enabled via property |
| **Notification-Driven Cleanup** | INotificationConsumer on TableDropped, etc. | Same mechanism |
| **File Layout** | No changes to `snapshots/<tag>/` structure | Same |

**No breaking changes** in snapshot infrastructure between 5.0 and trunk; CEP-11/pluggable format are *additive*.

---

## Safe Snapshot-Based External Read Pattern

1. **Initiate snapshot** (optional TTL, no skip-flush):
   ```
   SnapshotManager.instance.executeTask(
     new TakeSnapshotTask(manager, SnapshotOptions.userSnapshot(tag, ks.table)))
   ```

2. **Discover snapshot dirs** (via JMX or filesystem scan):
   - List: `SnapshotManager.getSnapshots()` 
   - Or parse: `<data_dir>/tables/<keyspace>/<table_id>/snapshots/<tag>/`

3. **Open hardlinks** (external reader):
   - Read Data.db, Index.db, Summary.db from snapshot dir
   - Do NOT hold references to source SSTableReader; read files directly via OS filesystem
   - No risk from Cassandra compaction/deletion (hardlinks unaffected)

4. **Release snapshot**:
   - `SnapshotManager.clearSnapshot()` (explicit)
   - Or wait for TTL expiration + automatic cleanup
   - Cleanup task deletes hardlinks when all internal Refs released

---

## Known Limitations (Snapshot Perspective)

- **No distributed snapshot** across cluster (snapshot is per-node)
- **No memtable inclusion** (snapshot freezes SSTables, not write buffer)
- **Hardlink-only** (no copy fallback for unsupported filesystems)
- **Case-sensitivity quirk on macOS** (hardlink dedup uses case-insensitive tag comparison)
- **Implicit flush** (snapshot triggers memtable flush unless skipFlush=true)
- **No snapshot versioning** (manifests are static JSON, no update API)

---

## File Anchors

- SnapshotManager lifecycle: `src/java/org/apache/cassandra/service/snapshot/SnapshotManager.java:61–227`
- Snapshot creation + hardlink trigger: `src/java/org/apache/cassandra/service/snapshot/TakeSnapshotTask.java:155–193`
- SSTable hardlink impl: `src/java/org/apache/cassandra/io/sstable/format/SSTableReader.java:1183–1203`
- GlobalTidy/obsoletion: `src/java/org/apache/cassandra/io/sstable/format/SSTableReader.java:1695–1798`
- Snapshot dir layout: `src/java/org/apache/cassandra/db/Directories.java:624–675`
- Hardlink creation: `src/java/org/apache/cassandra/io/util/FileUtils.java:127–172`
- CEP-11 Memtable API (trunk): `src/java/org/apache/cassandra/db/memtable/Memtable_API.md`
