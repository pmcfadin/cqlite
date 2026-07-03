# Apache Cassandra Write Path: Mutation Apply → Memtable → SSTable

## Summary

The write path coordinates mutation durability (commitlog), memtable insertion, view updates, and index maintenance across a global write order. Entry points are `Mutation.apply()` and `Keyspace.apply()`, which delegate to `KeyspaceWriteHandler` for durability coordination and `TableWriteHandler` for per-table writes. Mutations flow: **Mutation → Keyspace.applyInternal() → CassandraKeyspaceWriteHandler.beginWrite() (commitlog) → ColumnFamilyStore per-table handler → Memtable.put()** with materialized-view and index updates interleaved. The architecture is primarily **hardcoded** (write handlers instantiated directly in constructors), though **Memtable implementations are pluggable via CEP-11 configuration**.

## Key Classes & Interfaces

| Class/Interface | File:Line | Responsibility |
|-----------------|-----------|-----------------|
| `Mutation` | `src/java/org/apache/cassandra/db/Mutation.java:68` | Immutable representation of a multi-table write; entry points are `apply()`, `applyFuture()` |
| `Keyspace` | `src/java/org/apache/cassandra/db/Keyspace.java:81` | Manages keyspace and its tables; orchestrates multi-table mutations via write handler |
| `KeyspaceWriteHandler` (interface) | `src/java/org/apache/cassandra/db/KeyspaceWriteHandler.java:23` | Pluggable durability & ordering; defines `beginWrite()`, `createContextForIndexing()`, `createContextForRead()` |
| `CassandraKeyspaceWriteHandler` | `src/java/org/apache/cassandra/db/CassandraKeyspaceWriteHandler.java:32` | Default implementation: commits to commitlog, acquires global write order (`Keyspace.writeOrder`) |
| `TableWriteHandler` (interface) | `src/java/org/apache/cassandra/db/TableWriteHandler.java:23` | Per-table write contract; single method `write(PartitionUpdate, WriteContext, boolean)` |
| `CassandraTableWriteHandler` | `src/java/org/apache/cassandra/db/CassandraTableWriteHandler.java:24` | Default implementation: delegates to `ColumnFamilyStore.apply()` → memtable |
| `WriteContext` (interface) | `src/java/org/apache/cassandra/db/WriteContext.java:27` | Marker for write ordering; used across read/write/index paths |
| `CassandraWriteContext` | `src/java/org/apache/cassandra/db/CassandraWriteContext.java:26` | Holds `OpOrder.Group` (for ordering) and `CommitLogPosition` (durability); implements `AutoCloseable` |
| `ColumnFamilyStore` | `src/java/org/apache/cassandra/db/ColumnFamilyStore.java:1515` | Table-level write entry (`apply()` method); fetches current memtable via `data.getMemtableFor()`, invokes `memtable.put()` |
| `Memtable` (interface) | `src/java/org/apache/cassandra/db/memtable/Memtable.java:60` | Pluggable write store; key method is `put(PartitionUpdate, UpdateTransaction, OpOrder.Group, [boolean])` returning time delta |
| `CommitLog` | (implied) | Singleton that durably appends mutations; called via `CommitLog.instance.add(mutation)` |

## Extension Points / Pluggability Seams

### 1. **Memtable Implementations (CEP-11, Cassandra 4.1+)**
   - **Seam**: `Memtable.Factory` interface (methods `create()`, `writesShouldSkipCommitLog()`, `writesAreDurable()`, `streamToMemtable()`, `streamFromMemtable()`)
   - **How**: Per-table `cassandra.yaml` configuration under `memtable.configurations.<name>` specifies `class_name` and parameters
   - **Lookup**: Parsed and instantiated via `MemtableParams` during table metadata creation; factory selected per table
   - **Q1/Q2 relevance**: Alternative memtable implementations can intercept writes (e.g., to persist to external store or expose via OLAP interface) but are downstream of commitlog
   - **Trunk vs 5.0**: CEP-11 is **in Trunk (7.0)**, not in Cassandra 5.0; 5.0 only has built-in SkipListMemtable

### 2. **SSTableFormat (Pluggable Storage Format)**
   - **Seam**: `SSTableFormat<R, W>` interface (methods `getWriterFactory()`, `getReaderFactory()`, allComponents, etc.)
   - **How**: Registered at class load time; selection is per-sstable descriptor (format version baked at write time)
   - **Scope**: Controls SSTable component layout, reading, and compaction logic, **not write-path coordination**
   - **Q1/Q2 relevance**: Orthogonal to mutation-apply path; defines what gets flushed, not how mutations reach memtable

### 3. **ViewManager (Materialized View Coordination)**
   - **Seam**: `ViewManager.updatesAffectView(mutation, ...)`, `ViewManager.forTable(metadata).pushViewReplicaUpdates()`
   - **How**: Per-keyspace instance; integrated into `Keyspace.applyInternal()` at lines 459–577
   - **Scope**: Acquires per-partition locks to ensure view updates don't race writes; pushes view mutations through same apply path
   - **Q1/Q2 relevance**: MV updates use same write-path machinery, so an alternative write handler must replicate MV locking semantics

### 4. **Index Updates (Secondary Indexes)**
   - **Seam**: `ColumnFamilyStore.indexManager.newUpdateTransaction()`, invoked in `apply()` at line 1558
   - **How**: Integrated into per-table write; optional via `updateIndexes` boolean
   - **Scope**: Generates index mutations that go through the same write-path
   - **Q1/Q2 relevance**: Index updates are writes; must be coordinated with base-table writes

### 5. **OpOrder (Global Write Ordering)**
   - **Seam**: `Keyspace.writeOrder` (static, class-level `OpOrder` singleton)
   - **How**: All mutations acquire a group via `writeOrder.start()` in `CassandraKeyspaceWriteHandler.beginWrite()` (line 47)
   - **Scope**: Enforces cross-table and cross-keyspace write order; ensures flushed SSTables reflect causal order
   - **Q1/Q2 relevance**: **Critical for Q1**: any alternative write handler must maintain the same ordering guarantees so reads (memtable + SSTable) see consistent snapshots

## Hard Couplings

### 1. **Write Handlers Are Hardcoded (Not Pluggable)**
   - `CassandraKeyspaceWriteHandler` is instantiated directly in `Keyspace` constructors (lines 287, 298)
   - `CassandraTableWriteHandler` is instantiated directly in `ColumnFamilyStore` (line 590)
   - **Q2 impact**: To replace write handlers (e.g., for OLAP sidecar or alternative engine), requires:
     - Modifying Keyspace/ColumnFamilyStore constructors to accept handler suppliers, OR
     - Subclassing both classes

### 2. **CommitLog Is a Singleton**
   - `CommitLog.instance.add(mutation)` in `CassandraKeyspaceWriteHandler.addToCommitLog()` (line 99)
   - **Q1 impact**: Commitlog writes are tied to the primary mutation apply path; memtable flushes only create SSTables (no reverse sync)
   - **Q1 solution**: To include memtable in reads, must replay commitlog or poll memtable directly; no built-in "read-through-memtable" mode

### 3. **Memtable Lifecycle Is Bound to ColumnFamilyStore**
   - Memtable is fetched via `data.getMemtableFor()` inside `ColumnFamilyStore.apply()` (line 1523)
   - Memtable switching and flush decisions are internal to `ColumnFamilyStore.data` (likely `MemtablePool`)
   - **Q1/Q2 impact**: Memtable flushes are asynchronous and not exposed to external writes; alternative writers must replicate this lifecycle

### 4. **View Locking Is Per-Partition, Heavyweight**
   - `ViewManager.acquireLockFor(lockKey)` where `lockKey = Objects.hash(mutation.key(), tableId)` (line 473)
   - Lock acquisition has timeout and retry logic (lines 486–530)
   - **Q2 impact**: An alternative in-JVM write handler must implement the same lock protocol to avoid deadlock with view updates

### 5. **Mutation Serialization Version Tied to MessagingService**
   - `Mutation.SERIALIZATION_VERSION_COUNT` and `MessagingService.Version` enum (lines 92–95, 63–65)
   - Used for inter-node replication; **not directly relevant to local write path** but affects schema assumptions on remote replicas
   - **Q2 impact**: If CQLite writes were to be replicated to Cassandra, serialization compatibility is required

## Q1 & Q2 Relevance

### Q1: Analytical Reads Reflecting All Node-Local State

**Finding**: The write path **does not expose a read-through-memtable mode**. SSTables and memtables are physically separate; reads hit SSTables (via `SSTableReader`), memtables are only accessed during:
- Write-path conflicts (read-during-write in `Memtable.put()`)
- Compaction (memtable flush → SSTable)
- Explicit `getMemtable()` calls in test/admin code

**What's needed for Q1**:
1. **Memtable polling seam**: Add a post-apply hook that notifies external systems (e.g., Arrow Flight server) of memtable state changes
   - Current gap: `Keyspace.applyInternal()` is synchronous; no callback for "data flushed" or "memtable epoch changed"
2. **Memtable visibility**: Make current memtables queryable by a background process
   - Current seam: `ColumnFamilyStore.getMemtables()` is internal; not exposed as a public read interface
3. **Order guarantee**: Reads must see all writes up to a given timestamp or OpOrder epoch
   - Current seam: `OpOrder.Group` is used for durability ordering but not exposed for read coordination

**Cassandra 5.0 vs Trunk**: No difference; CEP-11 memtable pluggability does **not** add a "read-through-memtable" hook in Trunk.

### Q2: CQLite as Alternative/Adjacent Storage Engine

**Finding**: Cassandra's write path is **tightly coupled to commitlog and memtable lifecycle**. CQLite could fit as:

#### (a) **Adjacent OLAP Engine (Sidecar)**
- Read SSTables directly (already done by CQLite)
- **New**: Tap into memtable state via:
  - Commitlog tailing (read `CommitLog.currentSegment()` and replay)
  - Memtable snapshot exports (requires adding a public API; currently flush-only)
  - View update interception (would require hooking `ViewManager`)
- **No code changes needed to write path** if reads are post-hoc
- **Gap**: No built-in mechanism to know when memtable is dirty or has been flushed; must poll or rely on JMX/metrics

#### (b) **In-JVM Alternative Write Handler**
- Replace `CassandraTableWriteHandler` to write to CQLite instead of (or in addition to) Cassandra memtable
- **Requires**:
  - Modify `ColumnFamilyStore` constructor to accept a configurable handler factory
  - Implement `TableWriteHandler` interface (1 method: `write(PartitionUpdate, WriteContext, boolean)`)
  - Coordinate with `OpOrder` for ordering (pass `context.getGroup()` or request a new group)
  - Respect `updateIndexes` flag to generate index mutations
  - Coordinate with `ViewManager` for MV updates (or reimplement MV locking)
- **Hardcoded gaps**: KeyspaceWriteHandler (commitlog) is not replaceable without forking; would need to decide: commitlog-first (durability first) or skip commitlog (unsafe without persistent memtable)

#### (c) **Full Replacement Storage Engine** (Most Ambitious)
- Replace both commitlog and memtable with CQLite
- **Requires**:
  - Make `KeyspaceWriteHandler` pluggable (currently hardcoded to `CassandraKeyspaceWriteHandler`)
  - Implement `KeyspaceWriteHandler` to store mutations in CQLite
  - Implement `Memtable` interface for CQLite (or fake a thin proxy)
  - Ensure `OpOrder.Group` coordination and view locking still work
  - Replicate compaction logic (STCS, LCS, etc.) or reuse Cassandra's compaction against CQLite SSTables
- **Blockers**:
  - Paxos/Accord transaction system expects certain mutation routing; CQLite must support same transaction conflict detection (lines 100–104 in Mutation.java)
  - CDC (Change Data Capture) taps commitlog; would need CQLite to expose a CDC-compatible log

## Trunk vs Cassandra 5.0 Deltas

| Feature | 5.0 | Trunk (7.0) | Impact on Q1/Q2 |
|---------|-----|-------------|-----------------|
| CEP-11 Pluggable Memtable API | ❌ No | ✅ Yes; `Memtable.Factory` + per-table config | Q2: allows custom memtable (but no read-through-memtable hook) |
| `KeyspaceWriteHandler` interface | ❌ No; hardcoded `Keyspace.apply()` | ✅ Yes; interface + impl | Q2: still hardcoded to `CassandraKeyspaceWriteHandler` in constructors (not swappable) |
| `TableWriteHandler` interface | ❌ No | ✅ Yes; interface + impl | Q2: ditto, hardcoded in `ColumnFamilyStore` |
| Accord Transaction Support | ❌ No | ✅ Yes; `PotentialTxnConflicts` + routing | Q2: alternative handler must support Accord mutations |
| Memtable Pluggability Config | ❌ Built-in only (SkipListMemtable) | ✅ `cassandra.yaml` memtable.configurations | Q2: easier to plug in custom memtable (but still not a read interface) |

**Key Insight**: Trunk added *interfaces* (`KeyspaceWriteHandler`, `TableWriteHandler`, pluggable `Memtable`) but kept constructors hardcoded, so swapping is still blocked without forking. The interfaces are for future extensibility, not current pluggability.

## Hypotheses (Out of Scope but Noted)

1. **Q1 blocker**: No "read_through_memtable" CQL mode or background polling hook. Analytical reads must choose between stale SSTables or custom tailing/polling logic.
2. **Q2 opportunity**: A thin wrapper `Memtable` implementation could redirect `put()` calls to CQLite, bypassing Cassandra memtable entirely, if `OpOrder` and view locking are preserved.
3. **CEP-11 incomplete**: Despite pluggable memtables, the write path remains brittle to other changes (Accord, CDC, index logic); CEP-11 is a foundation, not a full storage-engine abstraction.
