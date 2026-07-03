# CommitLog-CDC Subsystem Index

**Target**: `src/java/org/apache/cassandra/db/commitlog/`  
**Repo**: Apache Cassandra trunk (base.version 7.0)  
**Focus**: CDC mechanics, visibility, durability, external tailer API

## Summary

CommitLog is a durable write-ahead log that records every mutation before it is applied to memtables. On CDC-enabled clusters, the CommitLogSegmentManagerCDC subclass hard-links completed segments to `cdc_raw` directory and maintains per-segment CDC index files tracking the synced byte offset. Visibility to CDC consumers is via file-system polling of cdc_raw (no explicit durability fence or stream API); segments blocked by `cdc_block_writes` transition to CDCState.FORBIDDEN. The system is tightly coupled to global CommitLog.instance singleton and segment IDs derived from wall-clock time. Trunk (v7.0) introduces CEP-11 pluggable memtables (via Memtable.Factory.writesAreDurable()) enabling alternative engines to bypass commitlog for durability while retaining it for CDC.

## Key Classes & Responsibilities

| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| **CommitLog** | CommitLog.java:76 | Singleton coordinator; manages segments, archival, metrics; decides CDC vs Standard manager via DatabaseDescriptor.getCommitLogSegmentMgrProvider() |
| **AbstractCommitLogSegmentManager** | AbstractCommitLogSegmentManager.java:71 | Base class for segment lifecycle; manages activeSegments queue, availableSegment staging, allocatingFrom pointer |
| **CommitLogSegmentManagerCDC** | CommitLogSegmentManagerCDC.java:51 | CDC variant; tracks CDC disk usage via CDCSizeTracker; hard-links segments; enforces cdc_block_writes threshold |
| **CommitLogSegmentManagerStandard** | CommitLogSegmentManagerStandard.java | Non-CDC variant; simple discard-on-flush |
| **CommitLogSegment** | CommitLogSegment.java:68 | Individual segment file (32MB default); tracks CDCState (PERMITTED/FORBIDDEN/CONTAINS); writes sync markers & CDC index files |
| **CommitLogReader** | CommitLogReader.java:57 | Deserializes mutations from segments; handles CRC validation, minPosition seek, tolerateTruncation |
| **CommitLogReplayer** | CommitLogReplayer.java:71 | Implements CommitLogReadHandler; replays mutations on recovery; tracks per-table persisted intervals; calls handleCDCReplayCompletion to hard-link replayed segments |
| **CommitLogDescriptor** | CommitLogDescriptor.java | Segment metadata: ID (from wall-clock), version, messaging version, compression, encryption; infers `.cdc` and `.cdc-index` filenames |
| **CommitLogReadHandler** | CommitLogReadHandler.java:25 | Interface for mutation processing; methods: shouldSkipSegmentOnError, handleUnrecoverableError, handleMutation |

## Extension Points & Pluggability Seams

### CommitLogSegmentManager Selection
**Location**: `src/java/org/apache/cassandra/config/DatabaseDescriptor.java:295-297`
```java
private static Function<CommitLog, AbstractCommitLogSegmentManager> commitLogSegmentMgrProvider = c ->
    DatabaseDescriptor.isCDCEnabled()
        ? new CommitLogSegmentManagerCDC(c, DatabaseDescriptor.getCommitLogLocation())
        : new CommitLogSegmentManagerStandard(c, DatabaseDescriptor.getCommitLogLocation());
```
- **Seam**: `DatabaseDescriptor.getCommitLogSegmentMgrProvider()` returns a function; could be overridden at startup
- **Usage**: `CommitLog.construct()` calls provider

### CommitLogReadHandler Interface
**Location**: `src/java/org/apache/cassandra/db/commitlog/CommitLogReadHandler.java:25`
- **Methods**: 
  - `void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)` — process a deserialized mutation
  - `boolean shouldSkipSegmentOnError(CommitLogReadException)` — error handling callback
  - `void handleUnrecoverableError(CommitLogReadException)` — abort callback
- **Implementations**: CommitLogReplayer (recovery), external tailer could implement this
- **Limitation**: No streaming API; mutations must be deserialized fully (no row-by-row lazy iteration)

### CEP-11 Pluggable Memtable API (Trunk Only)
**Location**: `src/java/org/apache/cassandra/db/memtable/Memtable_API.md`
- **Control interface** (Memtable.Factory):
  - `boolean writesAreDurable()` — if true, CommitLog skips durability writes (keeps CDC writes)
  - `boolean writesShouldSkipCommitLog()` — if true, no CommitLog writes at all (incompatible with CDC/PITR)
  - `boolean streamToMemtable()` / `boolean streamFromMemtable()` — control streaming behavior
- **Seam**: Allows alternative storage engines to declare their own durability, bypassing CommitLog.add() durability requirements while retaining CDC tracking
- **Not in 5.0**: This API does not exist in Cassandra 5.0.x

### CommitLogArchiver Hook
**Location**: `src/java/org/apache/cassandra/db/commitlog/CommitLogArchiver.java`
- **Purpose**: Archive/delete segments after flush; point-in-time restore
- **Extensibility**: Could be replaced for custom retention

## Hard Couplings & Assumptions

### Global Singleton
- **CommitLog.instance** (CommitLog.java:80) is static singleton; code path assumes single instance per JVM
- **Impact**: Alternative storage engine must replace CommitLog.construct() or hook into CommitLog.add()

### Segment ID Derivation
- **CommitLogSegment.java:90-91**: `replayLimitId = idBase = Math.max(currentTimeMillis(), maxId + 1)`
- **Coupling**: IDs are derived from wall-clock time at startup; no stable hash/UUID
- **Impact**: Segment IDs are not deterministic across restarts; sidecar reading must handle ID collisions

### CDC State Machine Locking
- **CommitLogSegment.java:79**: `final Object cdcStateLock = new Object()`
- **Coupling**: State transitions (PERMITTED → FORBIDDEN → PERMITTED, PERMITTED → CONTAINS) are synchronized on segment's cdcStateLock
- **Impact**: External reader polling cdc_raw must account for in-flight state changes; race between state change and hard-link creation is possible (mitigated by idempotent link creation)

### Hard-Link Dependency
- **CommitLogSegmentManagerCDC.java:209, 249**: `FileUtils.createHardLink(segment.logFile, segment.getCDCFile())`
- **Coupling**: CDC visibility depends on filesystem hard-link semantics (POSIX behavior assumed)
- **Impact**: Cannot be implemented on filesystems without hard-link support (e.g., some cloud object stores); hard-links tie cdc_raw to commitlog disk

### CDC Index File Synchronization
- **CommitLogSegment.java:381-395**: `writeCDCIndexFile(desc, offset, complete)` writes synced offset + optional "COMPLETED" marker
- **Coupling**: Index files written synchronously after segment.sync(flush=true); no batch/async option
- **Impact**: Sidecar must poll index files for offset updates; atomicity guaranteed only within a single segment's sync cycle

### Synchronous CDC Size Tracking
- **CommitLogSegmentManagerCDC.java:336-345**: `CDCSizeTracker.processNewSegment()` synchronizes on segment.cdcStateLock; state set before link creation
- **Coupling**: State and link are not atomic; brief race window exists
- **Impact**: Sidecar starting mid-race may see hard-link without state, or state without link

### Per-Table CDC Flag
- **Mutation.java:113**: `update.metadata().params.cdc` — CDC enabled/disabled per-table
- **Coupling**: Mutations know CDC status at write time; CommitLogSegmentManagerCDC.allocate() checks `mutation.trackedByCDC()`
- **Impact**: Segment may contain CDC and non-CDC data mixed; external reader must respect per-table CDC flag

### No Explicit Durability Fence for CDC
- **CommitLogSegment.java:365-366**: CDC index file written after flush; no fsync guarantee documented
- **Coupling**: Index file updates rely on filesystem sync semantics; no explicit `FileChannel.force()`
- **Impact**: CDC offset visibility depends on underlying filesystem's sync interval (typically <1 second on ext4, potentially longer on network storage)

## Q1 Relevance: Freshness for Analytical Reads

**Question**: When DataFusion or Trino reads a node via CQLite's Arrow Flight, how to see ALL node-local state (memtable + flushed SSTables)?

### Current State
- **CDC provides partial solution**: Hard-linked segments in cdc_raw are visible once `CDCState.CONTAINS` or sync completes
- **Latency floor for unflushed tail**:
  1. Mutation enters memtable (fast, <1µs)
  2. CommitLogSegmentManagerCDC.allocate() called (sets CDCState.CONTAINS if `trackedByCDC()` true)
  3. Hard-link created on segment creation (if PERMITTED) or at first CDC write (if transitioning FORBIDDEN→PERMITTED)
  4. CommitLogSegment.sync() called (~10-100ms batching interval, configurable)
  5. writeCDCIndexFile() writes offset (~5-50ms depending on storage)
  6. External reader polls cdc_raw (polling interval, typically >100ms)
  - **Total: ~110ms–1s latency** (batching + polling + storage sync)

### Missing Pieces
- **Memtable is not CDC-tracked**: Uncommitted memtable data is invisible to CDC consumers
  - Cassandra tracks memtable contents via Memtable.getFlushSet() but CDC does not expose this API
  - For freshness, sidecar must either (a) poll memtable directly (requires JVM access), or (b) accept stale view until flush
- **CDC index file is source-of-truth for offset**: External reader must parse `.cdc-index` files to determine readable range
  - No explicit durability marker; "COMPLETED" flag only written on segment close
  - Partial reads mid-segment are possible if reader doesn't respect offset

### Enabling Full Freshness (not yet implemented)
- **Required**: Expose memtable contents to CDC consumer via a public API (e.g., CommitLogReadHandler-like interface for memtable rows)
- **Alternative**: CommitLog.getCurrentPosition() is already exposed; sidecar could use it + CDC segments to bound reads, but would need memtable API
- **CEP-11 hook**: Pluggable memtable with `streamFromMemtable()=true` could expose unflushed data on demand (see Memtable_API.md)

## Q2 Relevance: Feasibility as Alternative/OLAP Engine

**Question**: How feasible is CQLite as (a) replacement storage engine inside Cassandra, (b) adjacent OLAP engine?

### Seams Available (Trunk v7.0)

#### (a) As Replacement Engine
- **CEP-11 Memtable API** (Memtable_API.md) allows:
  - Custom Memtable.Factory implementation
  - `writesAreDurable()=true` to skip CommitLog durability (keep CDC)
  - Per-table memtable selection via `WITH memtable='custom'`
  - **Limitation**: Still uses CommitLog.instance for CDC/PITR; must participate in recovery

- **SSTableFormat API** (src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java):
  - Pluggable SSTable reader/writer via reflection
  - Declared in schema metadata
  - **Limitation**: Not deeply investigated here; CommitLog still owned by storage layer

- **CommitLog is mandatory**:
  - CommitLog.add() called before memtable.put() (Mutation.java:218-237, CassandraKeyspaceWriteHandler.java:99)
  - No configuration to skip CommitLog entirely (CEP-11 writesShouldSkipCommitLog() requires custom memtable)
  - **Hard requirement**: Alternative engine must hook into or replace CommitLog.add()

#### (b) As Adjacent OLAP Engine (Sidecar)
- **CommitLogReadHandler interface** allows external reader to replay mutations
- **CommitLogReader API** is public; supports minPosition for resuming from checkpoint
- **CDC segment hard-links** enable low-latency tailing via cdc_raw polling
- **Limitations**:
  - No streaming/lazy API; must deserialize all mutations (memory/CPU cost)
  - No memtable visibility; must wait for flush
  - Segment ID collisions on restart (not deterministic)
  - CDC index files must be polled externally; no callback/event API

### Hard Coupling Blockers

| Blocker | Location | Impact on CQLite |
|---------|----------|-----------------|
| CommitLog.instance singleton | CommitLog.java:80 | Replacement engine needs dedicated CommitLog or shim |
| Mutation.trackedByCDC() check | CommitLogSegmentManagerCDC.java:189 | CDC tracking is per-mutation; sidecar cannot infer from segment alone |
| No explicit durability API | CommitLog.java (no interface) | Adding custom durability requires reflection/patching |
| Segment ID = currentTimeMillis() | CommitLogSegment.java:91 | Sidecar replay order depends on ID uniqueness; races on restart |
| FileUtils.createHardLink | CommitLogSegmentManagerCDC.java:209 | CDC visibility tied to filesystem hard-link support |

### Verdict
- **Replacement engine**: Feasible with CEP-11 memtable hook + custom CommitLog integration (medium effort)
- **Adjacent OLAP sidecar**: Feasible for low-latency CDC tailing (low effort), but full freshness requires memtable API (not exposed)

## Trunk vs. 5.0 Deltas

### New in Trunk (v7.0)
- **CEP-11 Memtable API** (Memtable_API.md): Pluggable memtable implementations with writesAreDurable()/writesShouldSkipCommitLog()
  - Affects Q2: Enables alternative engines to declare own durability
  - **Not in 5.0**: 5.0 has hardcoded memtable, no pluggable interface

- **Memtable.Factory interface**: Factory pattern for memtable instantiation with configuration parameters
  - **Not in 5.0**: 5.0 lacks factory pattern

### Unchanged (Exist in Both 5.0 and Trunk)
- **CommitLogSegmentManagerCDC** hard-linking strategy
- **CDCState machine** (PERMITTED/FORBIDDEN/CONTAINS)
- **CommitLogReader/CommitLogReplayer** APIs
- **CDC index file** format (.cdc-index with offset + optional COMPLETED marker)

### Notes
- Build.xml lists `<property name="base.version" value="7.0"/>`, confirming trunk version
- No version guards found in commitlog source; CEP-11 is seamlessly integrated in v7.0

## What Is NOT in the CommitLog

- **Memtable contents**: CommitLog stores mutations but not memtable state (schema, timestamps, row order)
- **Compaction decisions**: CommitLog has no record of which SSTables exist or their generation
- **Flush boundaries**: No explicit marker for "flush started here"; inferred from CommitLog.discardCompletedSegments()
- **Read-time metadata**: Timestamps, TTLs, deletions are in mutations but not indexed; CommitLog reader must deserialize all mutations to access them
- **Cluster-level state**: CommitLog is per-node; no replication factor, quorum, or consistency level information

## Hypotheses (Beyond Scope)

1. **CDCSizeTracker background recalculation race**: submitOverflowSizeRecalculation() submits async directory walk; size-in-progress may be inconsistent with actual disk state during polling. Affects sidecar consistency if sizes cached.

2. **Hard-link namespace collision**: If cdc_raw is on same filesystem as commitlog and a malicious consumer creates segments directly in cdc_raw, sidecar may confuse replayed segments with active ones. (Assumes no authentication on cdc_raw directory.)

3. **Memtable flushing blocking CommitLog**: If memtable flush stalls, CommitLog segments cannot be discarded; sidecar sees stale segments. Affects freshness SLA.
