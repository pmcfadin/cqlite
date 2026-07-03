# Cassandra Lifecycle Notifications Subsystem

**Summary**: Cassandra's notification system provides an event-driven feed of memtable and SSTable lifecycle changes at the per-table level via `Tracker`, plus a global SSTable version change aggregator via `SSTablesGlobalTracker`. Both expose public `subscribe()` APIs for third-party code to register consumers, enabling adjacent engines to track state changes. However, notifications fire only on committed state (post-flush, post-compaction), not on uncommitted memtable writes—limiting real-time visibility into in-memory data for Q1 (freshness).

## Key Classes & Interfaces

| Class | File | Responsibility |
|-------|------|-----------------|
| `INotification` | `src/java/org/apache/cassandra/notifications/INotification.java:27` | Marker interface for all notification types |
| `INotificationConsumer` | `src/java/org/apache/cassandra/notifications/INotificationConsumer.java:20` | `handleNotification(INotification, Object)` callback interface |
| `Tracker` | `src/java/org/apache/cassandra/db/lifecycle/Tracker.java:91` | Per-table lifecycle tracker; maintains `CopyOnWriteArrayList<INotificationConsumer>` subscribers (line 95); dispatches 10+ notification types |
| `SSTablesGlobalTracker` | `src/java/org/apache/cassandra/service/SSTablesGlobalTracker.java:55` | Global SSTable version tracker; maintains its own subscriber set (line 88); registers via `register()/unregister()` (lines 114–128) |

## Notification Types & Coverage

All inherit `INotification`; all fired from `Tracker.notify()` dispatch loop (line 590–593):

| Type | File | Fired When | Data |
|------|------|-----------|------|
| `SSTableAddedNotification` | line 30 | Flush complete or loading at startup | `Iterable<SSTableReader> added`, optional `Memtable memtable` source (line 46–50) |
| `InitialSSTableAddedNotification` | line 26 | ColumnFamilyStore initialization (line 24 javadoc) | `Iterable<SSTableReader> added` |
| `SSTableListChangedNotification` | line 25 | Compaction completes (line 29: `OperationType compactionType`) | `added`, `removed`, compaction type (for ops like COMPACTION, CLEANUP, etc.) |
| `SSTableDeletingNotification` | line 25 | Right before SSTable removal (line 23 javadoc) | `SSTableReader deleting` |
| `SSTableRepairStatusChanged` | line 25 | Repair status changes | `Collection<SSTableReader> sstables` |
| `SSTableMetadataChanged` | line 23 | SSTable level/metadata updated | `SSTableReader sstable`, `StatsMetadata oldMetadata` |
| `MemtableSwitchedNotification` | line 22 | Memtable rotated (new actively-written instance) | `Memtable previous`, `Memtable next` |
| `MemtableRenewedNotification` | line 22 | Memtable reused after flush | `Memtable renewed` |
| `MemtableDiscardedNotification` | line 22 | Memtable dropped (e.g., failed flush) | `Memtable memtable` |
| `TruncationNotification` | line 27 | During truncate, after flush, before discard (line 24 javadoc) | `ColumnFamilyStore cfs`, `boolean disableSnapshot`, `long truncatedAt`, `DurationSpec.IntSecondsBound ttl` |
| `TableDroppedNotification` | line 24 | Table dropped | `ColumnFamilyStore cfs`, ttl |
| `TablePreScrubNotification` | line 23 | Before scrub begins | `ColumnFamilyStore cfs` |

## Extension Points & Pluggability

### Subscription Access (Q2 Partial Answer)
1. **Per-table (Tracker)**: Third-party code calls `ColumnFamilyStore.getTracker().subscribe(INotificationConsumer)` (public method at `src/java/org/apache/cassandra/db/ColumnFamilyStore.java:1903`). Tracker holds subscriptions in thread-safe `CopyOnWriteArrayList` (line 95 Tracker.java); `unsubscribe()` also public (line 612 Tracker.java).

2. **Global (SSTablesGlobalTracker)**: Register via `StorageService.instance.sstablesTracker.register(INotificationConsumer)` (public method at `src/java/org/apache/cassandra/service/SSTablesGlobalTracker.java:114`). Only fires `SSTablesVersionsInUseChangeNotification`, not per-SSTable changes. Used by StorageService itself for Gossip updates (line 784 StorageService.java: lambda registers to update sstable versions in Gossip state).

3. **No built-in registry**: No global listener registry; subscribing requires direct access to Tracker or sstablesTracker instance. Third-party JARs can only subscribe if they run inside Cassandra JVM (not external readers).

### Built-in Subscribers
- **CompactionStrategyManager** (`src/java/org/apache/cassandra/db/compaction/CompactionStrategyManager.java:1052`, line 1061–1087): Listens to SSTableAdded, SSTableListChanged, repair/metadata/deleting notifications; coordinates compaction scheduling.
- **SecondaryIndexManager** (`src/java/org/apache/cassandra/index/SecondaryIndexManager.java:1887`): Listens to SSTableAddedNotification (non-memtable-sourced only); triggers blocking index builds (line 1894–1901).
- **StorageAttachedIndexGroup** (`src/java/org/apache/cassandra/index/sai/StorageAttachedIndexGroup.java`): Listens to SSTableAdded, SSTableListChanged, MemtableRenewed/DiscardedNotification (line 277–287); coordinates SAI memtable indexing lifecycle.
- **SASIIndex** (`src/java/org/apache/cassandra/index/sasi/SASIIndex.java:87`): Implements INotificationConsumer; subscribes via SecondaryIndexManager.
- **RouteJournalIndex** (Accord) (`src/java/org/apache/cassandra/index/accord/RouteJournalIndex.java`): Listens to MemtableRenewed/Discarded; maintains accord routing journal state.
- **SnapshotManager** (`src/java/org/apache/cassandra/service/snapshot/SnapshotManager.java:501`): Listens to Truncation, TableDropped, TablePreScrub; auto-takes snapshots before destructive ops (line 503–530).

## Hard Couplings (Q2: Alternative Engine Challenges)

1. **Tracker anchored to ColumnFamilyStore**: Tracker is instantiated per CFS (constructor line 109 Tracker.java), not pluggable. Alternative storage engine would need to provide equivalent `getTracker()` interface or re-implement subscription layer.

2. **Notification types immutable, no versioning**: Adding a new notification type requires code change; no schema evolution. External readers cannot extend notification set.

3. **Memtable as opaque ref**: Notifications carry `Memtable` objects without serialization/description. External readers cannot inspect memtable contents; only track lifecycle events (switch/renew/discard).

4. **SSTableReader as primary handle**: All SSTable changes reference `SSTableReader` (Cassandra's Java class), not path/descriptor only. External reader must implement/map Cassandra's SSTableReader interface to consume notifications.

5. **Synchronous dispatch, no ordering guarantee**: `Tracker.notify()` calls subscribers synchronously in `CopyOnWriteArrayList` order (line 590–593). No ordering guarantee across tables; concurrent flushes on different tables fire independently. No replay mechanism for missed notifications.

6. **No durable log or position**: Notifications are ephemeral in-process events. An adjacent engine that crashes loses notification history; no recovery mechanism (e.g., no LSN/offset to re-sync from).

7. **ColumnFamilyStore.getTracker() is public but CFS requires full in-JVM access**: Cannot remotely subscribe; third-party must be a Cassandra plugin or in-process client.

## Q1 & Q2 Relevance

**Q1 (Freshness via Arrow Flight)**: Notification system is **insufficient alone**. 
- ✓ Subscribing to SSTableAddedNotification + MemtableSwitchedNotification lets adjacent engine know when state changes occur.
- ✗ Notifications fire *after* flush (SSTableAddedNotification) or *during* rotation (MemtableSwitchedNotification), not real-time on memtable writes.
- ✗ No way to query current memtable contents via notification; only lifecycle events.
- ✗ Arrow Flight reader sees only flushed SSTables unless it independently polls/reads the active memtable (requires either: CEP-11 Memtable API for structured access, or direct mmap of WAL/commitlog files).

**Q2 (CQLite as Alternative Engine)**:
- ✓ **Pluggable at subscription layer**: CQLite's Rust binding could register an INotificationConsumer in-JVM (via JNI) to receive SSTable/memtable events, enabling real-time sync of local state.
- ✓ **Built-in extensibility**: Index implementations (SASI, SAI, Accord RouteJournal) already register as consumers; CQLite consumer would slot into same pattern.
- ✗ **No seam for engine replacement**: Tracker is hardcoded per CFS, not injected. Alternative storage engine cannot override Tracker class; must work alongside it.
- ✗ **No durable bootstrap**: On engine restart, no way to query "what SSTables exist?" other than filesystem scan. Notifications don't provide catch-up replay.
- ⚠ **Memtable access limited**: CEP-11 Memtable API exists (trunk), but notifications carry opaque Memtable refs; engine cannot deserialize/iterate memtable rows via notification alone.

## Trunk vs. 5.0 Notes

- **MemtableRenewed/MemtableDiscardedNotification**: Likely new in trunk (CEP-11 Memtable refactor). Cassandra 5.0 may not have renew lifecycle; check CASSANDRA-XX.
- **Memtable type**: Trunk has pluggable Memtable interface (src/java/org/apache/cassandra/db/memtable/Memtable_API.md); 5.0 has concrete SkipListMemtable. Notifications always carry Memtable type; implementations differ by version.
- **SSTablesGlobalTracker**: Introduced CASSANDRA-15897 (appears via NoSpamLogger, line 58); unknown if in 5.0. Check if SSTablesVersionsInUseChangeNotification exists in 5.0 release branch.
- **INotificationConsumer API**: Marker interface + single method; stable API, likely present in 5.0.

## Proposed Answers for Q1 & Q2

**For Q1 (freshness on analytical read)**:  
The notification system alone cannot solve freshness. Needed:
1. CEP-11 Memtable API to export active memtable rows (requires Cassandra code change; no Cassandra 5.0 support).
2. OR: dedicated "memtable snapshot" endpoint that CQLite Arrow Flight server can poll on each read request (not live, but fresher than last-flush).
3. OR: CQLite subscribes to MemtableSwitchedNotification, preemptively reads WAL/memtable before next rotation, caches in parallel store.

**For Q2 (CQLite as adjacent engine)**:  
✓ **Feasible for read-only sync**: CQLite in-process consumer can track SSTable set; Rust side calls back to CQLite core for data load. Requires: JNI binding + Rust callback handler + persistent state on CQLite side mapping Descriptor → CQLite reader handle.  
✗ **Not feasible as storage-engine replacement**: Tracker, ColumnFamilyStore, and compaction strategy are core architectural; no seam to inject alternative implementation in 5.0/trunk.  
⚠ **Feasible as separate OLAP store**: CQLite could run as external service, subscribe in-JVM via plugin/agent, and sync materialized views. Would need: custom agent that instantiates subscriber on every CFS, durable state recovery, and WAL reader to backfill memtables on startup.
