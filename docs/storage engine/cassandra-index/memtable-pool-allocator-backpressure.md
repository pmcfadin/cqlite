# Memtable Pool & Allocator Backpressure Subsystem

## Summary

The memtable memory-accounting and flush-backpressure mechanism governs when memtables are flushed under memory pressure and who decides the WHEN. A global `MemtablePool` (one instance per JVM) tracks on-heap and off-heap allocations across all live memtables. Each memtable holds a `MemtableAllocator` with two `SubAllocator` children that report ownership to the pool's `SubPool` instances. When usage exceeds a configurable `cleanThreshold` ratio, the pool's `MemtableCleanerThread` is awakened to invoke `flushLargestMemtable()`, which iterates all active memtables, reads their memory via `addMemoryUsageTo()`, and forces a flush of the largest consumer. Any custom or persistent memtable that does not integrate with this pool (via allocator adoption, `addMemoryUsageTo()` reporting, and `setDiscarding()/setDiscarded()` state transitions) will either never flush or silently OOM the JVM—this is the hard Q2 coupling for alternative engines.

---

## Key Classes & Interfaces

| Class | File | Responsibility |
|-------|------|-----------------|
| `MemtablePool` | `src/java/org/apache/cassandra/utils/memory/MemtablePool.java:43` | Abstract base; holds two `SubPool` instances (onHeap, offHeap) and delegates to `MemtableCleanerThread`. Checks `needsCleaning()` by polling `SubPool.used() > nextClean`. |
| `MemtablePool.SubPool` | `MemtablePool.java:103` | Tracks allocated bytes, reclaiming bytes, and computes the next clean threshold as `reclaiming + limit * cleanThreshold`. Provides `needsCleaning()`, `allocated()`, `acquired()`, `released()`, `reclaiming()`, `reclaimed()`. |
| `MemtableCleanerThread` | `src/java/org/apache/cassandra/utils/memory/MemtableCleanerThread.java:40` | Wraps `Clean` task (inner class, line 44) in an infinite-loop executor. Waits on `WaitQueue` until pool signals via `trigger()`. Calls `cleaner.clean()` (MemtableCleaner) and decrements `numPendingTasks` after completion. |
| `MemtableCleaner` | `src/java/org/apache/cassandra/utils/memory/MemtableCleaner.java:27` | Functional interface: `Future<Boolean> clean()`. Typically bound to `AbstractAllocatorMemtable::flushLargestMemtable`. Passed to pool constructor. |
| `MemtableAllocator` | `src/java/org/apache/cassandra/utils/memory/MemtableAllocator.java:32` | Abstract; owns two `SubAllocator` children (onHeap, offHeap). Transitions through lifecycle states: LIVE → DISCARDING (via `setDiscarding()`) → DISCARDED (via `setDiscarded()`). Allows temporary overshoot when DISCARDING to permit in-flight writes to complete before flush begins. |
| `MemtableAllocator.SubAllocator` | `MemtableAllocator.java:106` | Tracks one shard's ownership (`owns: LongAdder`) and reclaiming amount (`reclaiming` volatile long). Calls parent SubPool methods: `allocated()`, `acquired()`, `released()`, `reclaiming()`, `reclaimed()`. Computes `ownershipRatio() = owns.sum() / parent.limit`. |
| `AbstractAllocatorMemtable` | `src/java/org/apache/cassandra/db/memtable/AbstractAllocatorMemtable.java:57` | Static `MEMORY_POOL` (global singleton MemtablePool). Implements `addMemoryUsageTo(MemoryUsage)` by reading `allocator.onHeap().ownershipRatio()` and `.owns()`. Provides `flushLargestMemtable()` (static, lines 257–326): iterates `ColumnFamilyStore.activeMemtables()`, calls `addMemoryUsageTo()` on each, selects largest by ratio, signals owner to flush. |
| `Memtable.MemoryUsage` | `src/java/org/apache/cassandra/db/memtable/Memtable.java:263` | Data holder: `ownsOnHeap`, `ownsOffHeap` (bytes), `ownershipRatioOnHeap`, `ownershipRatioOffHeap` (float, 0–1). Accumulated across memtable + all secondary indexes. |
| `Memtable` (interface) | `src/java/org/apache/cassandra/db/memtable/Memtable.java:60` | Defines `addMemoryUsageTo(MemoryUsage)`, `markExtraOnHeapUsed()`, `markExtraOffHeapUsed()`. Factory can return `writesAreDurable()` (skip flush), `writesShouldSkipCommitLog()` (opt out of commitlog entirely). |
| `HeapPool` | `src/java/org/apache/cassandra/utils/memory/HeapPool.java:31` | MemtableAllocationType.heap_buffers: on-heap only, offHeap = 0. |
| `SlabPool` | `src/java/org/apache/cassandra/utils/memory/SlabPool.java:21` | MemtableAllocationType.heap_buffers (slabbed) or offheap_buffers (both on-heap and off-heap). |
| `NativePool` | `src/java/org/apache/cassandra/utils/memory/NativePool.java:21` | MemtableAllocationType.offheap_objects: delegates to `NativeAllocator`. |

---

## Extension Points & Pluggability Seams

### 1. **Memtable.Factory** (CEP-11 Pluggable Memtables)
**Location**: `src/java/org/apache/cassandra/db/memtable/Memtable.java:77`

- Classes must provide static `FACTORY` field OR `factory(Map<String, String>)` method
- Loaded via reflection by `MemtableParams` (no hardcoded class path)
- Controls memory durability: `writesAreDurable()`, `writesShouldSkipCommitLog()`, `streamToMemtable()`, `streamFromMemtable()`
- **Q1/Q2 implication**: Custom memtables can declare write durability to skip commitlog replay, but MUST NOT skip the pool integration if they want flushing coordination

### 2. **MemtableCleaner Interface**
**Location**: `src/java/org/apache/cassandra/utils/memory/MemtableCleaner.java:27`

- Single method: `Future<Boolean> clean()`
- Passed to `MemtablePool` constructor
- Default binding: `AbstractAllocatorMemtable::flushLargestMemtable`
- **Q1/Q2 implication**: Alternative engines can provide their own cleaner to control flush policy (e.g., time-based, RocksDB LSM-level sweep)

### 3. **MemtableAllocationType Enum + Pool Selection**
**Location**: `src/java/org/apache/cassandra/config/Config.java:1369`, used in `AbstractAllocatorMemtable.createMemtableAllocatorPool()` (line 82)

- Config key: `memtable_allocation_type` (default: `heap_buffers`)
- Values: `unslabbed_heap_buffers`, `unslabbed_heap_buffers_logged`, `heap_buffers`, `offheap_buffers`, `offheap_objects`
- Selects pool class at startup (one global MEMORY_POOL per JVM)
- **Q1/Q2 implication**: Alternative engines can provide new MemtableAllocationType values and pool classes, BUT must integrate the cleaner callback

### 4. **addMemoryUsageTo() Implementation Hook**
**Location**: `src/java/org/apache/cassandra/db/memtable/Memtable.java:238`

- Memtable interface method; each implementation reports its own memory consumption
- `AbstractAllocatorMemtable.addMemoryUsageTo()` reads from `allocator.onHeap().owns()` and `.offHeap().owns()`
- Called by `flushLargestMemtable()` (line 275 in AbstractAllocatorMemtable) to compute ownership ratios
- **Q1/Q2 implication**: If a custom memtable does not implement this, `flushLargestMemtable()` sees zero usage and will never select it for flush

### 5. **Allocator State Machine (LIVE → DISCARDING → DISCARDED)**
**Location**: `src/java/org/apache/cassandra/utils/memory/MemtableAllocator.java:39` (LifeCycle enum)

- `setDiscarding()`: marks allocator reclaiming; allows in-flight writes to exceed limit (line 84–88)
- `setDiscarded()`: releases all memory owned (line 94–98)
- Enables graceful flush under memory pressure: memory is marked reclaiming BEFORE flush actually writes data
- **Q1/Q2 implication**: Persistent memtables that do not support setDiscarding() cannot participate in the backpressure protocol

---

## Hard Couplings & Assumptions

### 1. **Global Pool Singleton & Per-Memtable Allocator Adoption**
```java
// src/java/org/apache/cassandra/db/memtable/AbstractAllocatorMemtable.java:120
this.allocator = MEMORY_POOL.newAllocator(metadataRef.toString());
```
- Each memtable MUST call `pool.newAllocator()` to get its allocator
- Every `allocate(size, opGroup)` call MUST report to the pool or memory goes untracked
- **Risk**: A custom memtable that allocates memory outside this allocator silently OOMs the JVM; the pool thinks memory is free when it is not

### 2. **needsCleaning() Polling on Every Allocation**
```java
// src/java/org/apache/cassandra/utils/memory/MemtablePool.java:88-91 & MemtableAllocator.SubAllocator:allocated() line 207
parent.allocated(size);       // increments parent.allocated (SubPool)
maybeClean();                 // calls pool.needsCleaning(); if true, cleaner.trigger()
```
- After every allocation, the pool recomputes `nextClean` and checks `needsCleaning()`
- Trigger rate depends on `cleanThreshold` (config: `memtable_cleanup_threshold`, default ~0.3–0.4)
- **Risk**: A memtable that stops calling `allocate()` (e.g., pre-allocates a large buffer once) will never trigger cleanup again

### 3. **Flush Selection via Highest Ownership Ratio**
```java
// src/java/org/apache/cassandra/db/memtable/AbstractAllocatorMemtable.java:281
float ratio = Math.max(usage.ownershipRatioOnHeap, usage.ownershipRatioOffHeap);
if (ratio > largestRatio) {
    largestMemtable = current;
    largestRatio = ratio;
}
```
- Selects memtable with largest ownership ratio, NOT absolute size
- Ratio is `owns() / pool.limit`
- **Risk**: A small memtable on a small limit pool may flush before a large memtable on a large limit, depending on configuration isolation

### 4. **Active Memtables List Enumeration**
```java
// src/java/org/apache/cassandra/db/memtable/AbstractAllocatorMemtable.java:266
for (Memtable currentMemtable : ColumnFamilyStore.activeMemtables())
```
- `flushLargestMemtable()` is static; iterates ALL ColumnFamilyStore instances and their active memtables
- No per-table or per-pool filtering; global decision
- **Risk**: If a table is invisible to `activeMemtables()`, its memory is not accounted for in flush selection (e.g., system tables, dropped tables still held in memory)

### 5. **Reclaiming Counter for In-Flight Writes During Flush**
```java
// src/java/org/apache/cassandra/utils/memory/MemtableAllocator.java:205-215, updateReclaiming() line 264-276
parent.reclaiming(cur - prev);  // marks "being flushed" memory separately
```
- When a memtable is set to DISCARDING state, its `owns()` is moved to `reclaiming()`
- The pool tracks both `allocated` and `reclaiming` separately; `nextClean` includes reclaiming in its calculation
- **Risk**: A hung flush (e.g., I/O stall) keeps memory marked reclaiming indefinitely, preventing other flushes from being triggered

### 6. **Config Bind at Startup: Single MemtableAllocationType for All Tables**
```java
// src/java/org/apache/cassandra/config/Config.java:624
public MemtableAllocationType memtable_allocation_type = MemtableAllocationType.heap_buffers;
```
- `memtable_allocation_type` is process-wide; selects the ONE pool class that all tables share
- Per-table memtable implementation (via `CREATE TABLE ... WITH memtable = '...'`) is pluggable, but the allocator pool is global
- **Risk**: A custom memtable that requires a different pool class (e.g., persistent memory) cannot coexist with standard tables in the same process

### 7. **Synchronous Memory Reporting**
```java
// src/java/org/apache/cassandra/db/memtable/AbstractAllocatorMemtable.java:188-194
public void addMemoryUsageTo(MemoryUsage stats) {
    stats.ownershipRatioOnHeap += getAllocator().onHeap().ownershipRatio();
    stats.ownershipRatioOffHeap += getAllocator().offHeap().ownershipRatio();
    stats.ownsOnHeap += getAllocator().onHeap().owns();
    stats.ownsOffHeap += getAllocator().offHeap().owns();
}
```
- Called synchronously from `flushLargestMemtable()` without locking
- Reads from a `LongAdder` (eventually consistent, not atomic snapshot)
- **Risk**: Under extreme concurrency, the snapshot is stale; a memtable's ownership ratio may change between reading and selection

---

## Q1 Relevance: Memtable Freshness & Analytical Read Visibility

**Q1: When an analytical query (e.g., via Arrow Flight / Trino) reads a node through CQLite's connector, the read sees only flushed SSTables. What must change in Cassandra so an analytical read on one node reflects ALL node-local state — memtable contents PLUS every SSTable — not just the latest flush?**

### Current Constraint
- The pool's `needsCleaning()` check and flush trigger run synchronously on write paths (in `allocate()`, lines 207–209 of SubAllocator)
- Flush is reactive: triggered only when `used() > nextClean`
- Between flushes, data resides in the memtable but is **invisible to external readers** (Arrow Flight, CQLite, snapshot-based exports)

### Required Changes
1. **Async flush decoupling**: Move the flush trigger off the write path. Currently, the memtable allocator calls `parent.allocated()` → `maybeClean()` → `cleaner.trigger()` synchronously; this blocks the write. For Q1 freshness, this must be async-capable (currently it is async—`MemtableCleanerThread` is a background thread—but the trigger signal is synchronous).

2. **Explicit reader-requested flush API**: Add a method for external readers (e.g., Arrow Flight handler) to force-flush the current memtable before reading. This is NOT in the current code; it would require new seams:
   - `ColumnFamilyStore.forceFlushForRead()` or similar
   - Reader integration: Arrow Flight server calls this before snapshot

3. **Memtable inclusion in Arrow Flight / external read snapshots**: The current Cassandra read path includes only flushed SSTables. The Arrow Flight connector or external reader must:
   - Call `ColumnFamilyStore.getAllMemtables()` (or similar) to enumerate live AND flushing memtables
   - Snapshot their state independently (not via SSTable) and include in the read result

4. **CEP-11 persistent memtable opportunity**: A memtable that declares `writesAreDurable() = true` and `writesShouldSkipCommitLog() = true` can be read directly without flushing. But it still must integrate the pool's `addMemoryUsageTo()` and `setDiscarding()` protocol to avoid OOM and to be coordinated with other memtable flushes.

### Current Code Blocking Q1
- `AbstractAllocatorMemtable.flushLargestMemtable()` (line 257) has no external entry point for reader-driven flush
- Arrow Flight integration in Cassandra does not call flush before snapshot; it reads current SSTables only
- Memtable content is not exposed to the Arrow Flight protocol layer (would require `Memtable.rowIterator()` to be part of the connector's scan)

---

## Q2 Relevance: CQLite as Alternative or Adjacent Engine

**Q2: How feasible is CQLite as (a) an alternative/replacement storage engine inside Cassandra, and (b) an adjacent OLAP storage engine running alongside the normal engine? Where does "the storage engine" actually live in Cassandra's code, what seams exist, what has no seam?**

### Feasibility Posture

#### (a) CQLite as Alternative Engine (In-Process Replacement)
**Risk Level**: HIGH. Requires wholesale integration with the pool + allocator subsystem.

**Hard Coupling Points**:
1. **MemtablePool adoption (MANDATORY)**: CQLite-as-memtable MUST call `pool.newAllocator()` in constructor. If it bypasses this, `flushLargestMemtable()` will not see its memory and OOM will occur silently.
2. **addMemoryUsageTo() implementation (MANDATORY)**: Must implement `Memtable.addMemoryUsageTo()` accurately. If it reports zero, it will never be selected for flush.
3. **setDiscarding()/setDiscarded() state transitions (MANDATORY)**: Must support these lifecycle methods. A persistent CQLite memtable can implement `writesAreDurable() = true`, but still must respond to `setDiscarding()` (mark its data as reclaiming) so other tables' flushes are coordinated.
4. **Flush trigger policy decision (OPTIONAL but risky)**: Can provide a custom `MemtableCleaner`, but only one cleaner per JVM. If two alternative engines coexist, they compete for the same `MemtableCleanerThread`.
5. **Global pool singleton (HARD LIMIT)**: The pool is process-wide; there is ONE pool per JVM. CQLite cannot have its own pool unless it forks Cassandra or runs in a sidecar.

**Workaround for In-Process Coexistence**: CQLite could implement a "pass-through" memtable that allocates from the standard pool but delegates writes/reads to an embedded CQLite instance. This is a wrapper, not a true replacement, and still incurs the pool's memory accounting overhead.

#### (b) CQLite as Adjacent Engine (Sidecar / Out-of-Process)
**Risk Level**: LOW. No coupling required.

**Feasibility**: Trivial. A sidecar Arrow Flight server or external reader can pull data from Cassandra via existing streaming APIs (BulkLoader, snapshot export, CDC) and load into CQLite. No integration with the pool is needed because CQLite runs in its own JVM. The only constraint is **Q1 freshness**: the sidecar sees only flushed SSTables and committed commitlog entries unless Cassandra provides an explicit reader-flush API.

### Missing Seams in Current Cassandra

1. **Per-Table Pool Isolation**: There is no seam to assign different tables to different pools (e.g., analytical vs. transactional tables). All tables share the global `MEMORY_POOL`.
   - **Fix Required for Q2(a)**: Add `MemtablePool pool` field to `ColumnFamilyStore` and factory method to select pool per-table or per-keyspace.

2. **Reader-Driven Flush Hook**: No seam for external readers to force-flush memtables before read.
   - **Fix Required for Q1**: Add `ColumnFamilyStore.forceFlushForRead()` and call it from Arrow Flight handler.

3. **Pluggable Allocator Class**: `MemtableAllocationType` is an enum; adding a new type requires modifying `Config.java`. Not extensible without recompile.
   - **Fix Required for Q2(a)**: Allow custom allocator classes via reflection (like Memtable.Factory).

4. **Passive Memory Accounting**: A memtable that allocates outside the pool (e.g., via a custom allocator or persistent memory) has no way to report its usage retroactively. The pool only sees allocations made via `SubAllocator.allocate()`.
   - **Fix Required for Q2(a)**: Add a `MemtablePool.reportExternalAllocation()` method to allow alternative allocators to register memory.

5. **Flush Policy Customization**: The policy is hardcoded to `flushLargestMemtable()`. No seam to select a different policy per-pool or per-table.
   - **Fix Required for Q2(a)**: Make `MemtableCleaner` a per-pool, per-table option (not just startup config).

---

## Trunk (7.0) vs. Cassandra 5.0 Differences

| Aspect | Cassandra 5.0 | Trunk (7.0) |
|--------|---------------|------------|
| **CEP-11 Pluggable Memtables** | Implemented (4.1+) | Fully integrated; TrieMemtable is default |
| **Memtable.Factory Interface** | Present; basic factory pattern | Same; no changes observed |
| **MemtableAllocationType** | Has 5 variants (heap, slab, native) | Same (lines 1371–1375) |
| **MemtablePool Location** | `AbstractAllocatorMemtable.MEMORY_POOL` | Same (line 61) |
| **addMemoryUsageTo() Method** | Present in AbstractAllocatorMemtable | Same (lines 188–194) |
| **MemtableCleanerThread** | Present; infinite-loop executor | Same (line 40) |
| **writesAreDurable() / writesShouldSkipCommitLog()** | CEP-11 introduced; present | Same (lines 107–123 in Memtable.java) |
| **Persistent Memtable Seam** | Limited; factory flags only | Same |
| **Q1 Support (Reader Flush Hook)** | No explicit seam observed | No explicit seam; same as 5.0 |
| **Q2 Support (Per-Table Pool)** | Global pool only | Global pool only; no per-table option |

**Conclusion**: The core pool + allocator + backpressure subsystem is **stable** from 5.0 → trunk. No breaking changes observed. The CEP-11 pluggable memtable seam is mature. Gaps for Q1 and Q2 are NOT trunk-vs-5.0 regressions; they are design gaps that exist in both versions.

---

## Hypothesis: Why This Subsystem Blocks Q1 & Q2

1. **For Q1 (Freshness)**: The pool + allocator design ensures durability (data doesn't get lost under memory pressure) but not *freshness* for analytical readers. A memtable that has been live for seconds is invisible to external readers until it is flushed. Closing this gap requires (a) reader-driven flush APIs and (b) memtable inclusion in the external read snapshot—neither exists today.

2. **For Q2 (Alternative Engine Feasibility)**: CQLite can be a sidecar (b) easily, but as an in-process replacement (a), it MUST adopt the pool's accounting protocol to avoid silent OOM. The pool subsystem is not designed to be replaced; it can only be extended via custom MemtableCleaner and Memtable.Factory. A true CQLite-as-engine would require forking or wrapping.

---

## References

- `src/java/org/apache/cassandra/utils/memory/MemtablePool.java:88` – `needsCleaning()` check
- `src/java/org/apache/cassandra/utils/memory/MemtableCleanerThread.java:73` – `Clean.run()` loop logic
- `src/java/org/apache/cassandra/db/memtable/AbstractAllocatorMemtable.java:257` – `flushLargestMemtable()` entry point
- `src/java/org/apache/cassandra/db/memtable/Memtable.java:238` – `addMemoryUsageTo()` contract
- `src/java/org/apache/cassandra/db/memtable/Memtable_API.md` – CEP-11 design doc
- `src/java/org/apache/cassandra/config/Config.java:1369` – `MemtableAllocationType` enum
