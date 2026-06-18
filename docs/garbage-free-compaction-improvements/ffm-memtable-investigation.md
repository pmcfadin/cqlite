# FFM (Foreign Function & Memory) Memtable — Feasibility Investigation & Design Sketch

> Status: investigation only — no code changes. All file:line references are against trunk
> (HEAD `3831d8265d` at time of writing).
>
> Goal of the eventual implementation: a memtable whose **data lives fully off-heap** in
> `java.lang.foreign` `MemorySegment`s owned by an `Arena` whose lifetime equals the
> memtable's; **minimal copying** on both write and read paths; **pluggable** through the
> CEP-11 memtable API; **opt-in** the way `heap_buffers` / `offheap_buffers` /
> `offheap_objects` are today.

---

## Executive summary

- The pluggable memtable API (CEP-11) is fully sufficient to ship an FFM memtable as an
  opt-in implementation: factories are loaded reflectively by class name from
  `cassandra.yaml` `memtable.configurations` and selected per table with
  `WITH memtable = '<name>'` (`MemtableParams.java:217-256`, `Memtable_API.md:6-91`).
  No core change is strictly required to plug one in.
- Memory accounting (limits, cleanup thresholds, flush triggering) is cleanly separated
  from actual allocation: `MemtablePool.SubPool` / `MemtableAllocator.SubAllocator` only
  track numbers (`MemtablePool.java:103-262`, `MemtableAllocator.java:106-297`). An FFM
  allocator can reuse this machinery unchanged while sourcing bytes from Arena-owned
  segments.
- Even in the most aggressive mode today (`offheap_objects`), only **leaf data** is
  off-heap (key bytes, clustering bytes, cell headers+values). The partition index
  (skip-list nodes), per-row objects (`BTreeRow`, BTree node `Object[]`s), per-cell and
  per-clustering Java shells, `DeletionInfo` (always heap — `BTreePartitionUpdater.java:152-155`),
  and all stats stay on heap. TrieMemtable additionally moves the partition-index *trie
  nodes* off-heap (`InMemoryTrie.java:151`, `TrieMemtable.java:96`), but its per-partition
  content values (`BTreePartitionData`) remain heap objects.
- The copying story: writes always copy mutation buffers into memtable memory via
  `Cloner` (unavoidable — one copy); reads in both off-heap modes copy **everything back
  on-heap** through `EnsureOnHeap.CloneToHeap` (`EnsureOnHeap.java:51-123`); flush in the
  `offheap_objects` mode is already nearly zero-copy thanks to the new
  `NativeAccessor`/`writeMemory` path (`NativeAccessor.java:68-71`,
  `DataOutputPlus.java:48`).
- Memtable memory reclamation is gated by **two OpOrder barriers**: a write barrier at
  switch-out and a read barrier before `discard()`
  (`ColumnFamilyStore.java:1247-1286`, `1439-1452`). This maps *exactly* onto
  `Arena.ofShared().close()`: by the time today's code calls `free()`, no reader or
  writer can touch the memory, so an Arena close at the same point is safe by
  construction — and unlike `Unsafe`, a straggling access throws
  `IllegalStateException` instead of reading freed memory.
- **JDK availability is the gating constraint.** Trunk builds for JDK 11/17/21
  (`build.xml:47-48`); FFM is final only in JDK 22+ (JEP 454; preview in 19–21,
  incubator before that). An in-tree FFM memtable therefore cannot ship until the
  baseline includes ≥22 — but an **out-of-tree plugin jar** compiled for JDK 22+ and
  loaded by class name works today on a JDK-22+ runtime, which is the recommended
  prototyping vehicle.
- Recommended first slice: an `FFMAllocator` (Arena + segment slabs) plus
  segment-backed `DecoratedKey`/`Clustering`/`Cell` mirroring the existing `Native*`
  classes, reusing `SkipListMemtable`'s structure, with reads initially still going
  through `CloneToHeap`. Then add a `ValueAccessor<MemorySegment>` (the freshly-landed
  `NativeAccessor`/`NativeData` abstraction is a near-exact template) to make flush and
  reads zero-copy.

---

# Phase 1 — Current architecture

## 1.1 The pluggable memtable API (CEP-11)

**Interface.** `Memtable` (`src/java/org/apache/cassandra/db/memtable/Memtable.java:60`)
extends `UnfilteredSource`; the contract groups:

- *Construction*: nested `Memtable.Factory` (`Memtable.java:77-153`) with
  `create(commitLogLowerBound, metadataRef, owner)` (`Memtable.java:88`) plus optional
  durability/streaming hooks (`writesShouldSkipCommitLog()` `:107`,
  `writesAreDurable()` `:120`, `streamToMemtable()` `:133`, `streamFromMemtable()` `:149`).
- *Writes*: `put(PartitionUpdate, UpdateTransaction, OpOrder.Group, boolean)`
  (`Memtable.java:205`).
- *Reads*: `rowIterator`/`partitionIterator` via `UnfilteredSource` (`Memtable.java:207`).
- *Memory tracking*: `addMemoryUsageTo(MemoryUsage)` (`Memtable.java:238`),
  `markExtraOnHeapUsed`/`markExtraOffHeapUsed` (`Memtable.java:293,303`).
- *Flush*: `getFlushSet(from, to)` returning `FlushablePartitionSet`
  (`Memtable.java:311-346`).
- *Lifecycle*: `switchOut(writeBarrier, commitLogUpperBound)` (`Memtable.java:362`),
  `discard()` (`Memtable.java:368`), `accepts(opGroup, commitLogPosition)`
  (`Memtable.java:375`), `shouldSwitch(reason, metadata)` (`Memtable.java:420`).
- `Memtable.Owner` (`Memtable.java:160-182`) is the CFS-facing callback surface:
  `signalFlushRequired`, `getCurrentMemtable`, `localRangeSplits(shardCount)` — the
  latter is how sharded memtables get token-space splits.

**Selection.** `MemtableParams` (`src/java/org/apache/cassandra/schema/MemtableParams.java`)
resolves a configuration name to a factory:

- `cassandra.yaml` defines named configurations under `memtable: configurations:`
  (`conf/cassandra.yaml:799-806`; format documented in `Memtable_API.md:11-52`).
- Per-table opt-in: `CREATE/ALTER TABLE ... WITH memtable = '<configuration>'`
  (`Memtable_API.md:74-91`). Unset tables use the `default` configuration
  (`MemtableParams.java:98-104`).
- Factory instantiation is reflective: class name resolved (default package
  `org.apache.cassandra.db.memtable.` prepended if unqualified,
  `MemtableParams.java:227`), then either a static `factory(Map<String,String>)` method
  or a static `FACTORY` field (`MemtableParams.java:237-244`). **This is the natural JDK
  gate and the out-of-tree plugin hook**: a fully-qualified class in an external jar
  works (the API doc explicitly supports out-of-tree implementations), and class loading
  simply fails with a `ConfigurationException` on an unsupported runtime
  (`MemtableParams.java:251-256`).
- Examples of factory shape: `TrieMemtable.factory(Map)` consuming a `shards` option
  (`TrieMemtable.java:756-800`); `ShardedSkipListMemtable.factory(Map)`
  (`ShardedSkipListMemtable.java:529`); `SkipListMemtableFactory.INSTANCE` as default
  (`MemtableParams.java:99`).

**Class hierarchy.**

```
Memtable (interface)                                  Memtable.java:60
 └─ AbstractMemtable                                  AbstractMemtable.java:46
     │   (stats: ColumnsCollector, StatsCollector, minTimestamp — all on heap,
     │    AbstractMemtable.java:52-77)
     └─ AbstractMemtableWithCommitlog                 AbstractMemtableWithCommitlog.java
         │   (write barrier + commit-log bound acceptance, :40-74)
         └─ AbstractAllocatorMemtable                 AbstractAllocatorMemtable.java:57
             │   (owns a MemtableAllocator from the global static MEMORY_POOL :61,120;
             │    switchOut→allocator.setDiscarding :164-168;
             │    discard→allocator.setDiscarded :170-174; scheduled flush :206-251;
             │    flushLargestMemtable cleaner callback :257-326)
             ├─ SkipListMemtable                      SkipListMemtable.java:68
             ├─ AbstractShardedMemtable               AbstractShardedMemtable.java
             │    │  (default shard count = cores, MEMTABLE_SHARD_COUNT, :46)
             │    ├─ ShardedSkipListMemtable          ShardedSkipListMemtable.java
             │    └─ TrieMemtable                     TrieMemtable.java:91
```

**Lifecycle walk-through** (driven by `ColumnFamilyStore`):

1. *Construction*: `cfs.createMemtable()` → `memtableFactory.create(...)`
   (`ColumnFamilyStore.java:1461-1464`). `AbstractAllocatorMemtable` constructor grabs a
   fresh `MemtableAllocator` from the global pool (`AbstractAllocatorMemtable.java:120`).
2. *Writes*: `Memtable.put` runs inside an `OpOrder.Group` from `Keyspace.writeOrder`;
   `accepts()` routes writes that started before the switch barrier to the old memtable
   (`AbstractMemtableWithCommitlog.java:71-94`, `Memtable.java:375`).
3. *Switch*: `ColumnFamilyStore.Flush` constructor (under the Tracker monitor) creates
   the new memtable, swaps it into the live view, calls
   `oldMemtable.switchOut(writeBarrier, commitLogUpperBound)` and **then** issues the
   write barrier (`ColumnFamilyStore.java:1247-1271`). `switchOut` also flips the
   allocator to `DISCARDING` (`AbstractAllocatorMemtable.java:164-168`) which marks its
   owned memory "reclaiming" in the pool (`MemtableAllocator.java:130-135, 264-276`).
4. *Flush*: `Flush.run()` awaits the write barrier (`ColumnFamilyStore.java:1283-1286`),
   marks memtables flushing, and runs `Flushing.flushRunnables` →
   `memtable.getFlushSet(...)` → `FlushRunnable.writeSortedContents()` appends each
   partition's `unfilteredIterator()` to the sstable writer
   (`Flushing.java:61-124, 155-181`).
5. *Discard*: `Flush.reclaim()` issues a **read** barrier on `cfs.readOrdering` and only
   after `readBarrier.await()` (chained after the post-flush task) calls
   `memtable.discard()` (`ColumnFamilyStore.java:1439-1452`). `discard()` →
   `allocator.setDiscarded()` → all owned memory released back to pool accounting
   (`AbstractAllocatorMemtable.java:170-174`, `MemtableAllocator.java:141-156`), plus
   implementation-specific freeing (below).

## 1.2 Memory allocation architecture

All in `src/java/org/apache/cassandra/utils/memory/`.

**Pool/allocator split.** `MemtablePool` holds two `SubPool`s (onHeap/offHeap) which are
*pure accounting*: `limit`, `allocated`, `reclaiming`, `nextClean`
(`MemtablePool.java:103-262`). `tryAllocate` is an `addAndGet` against the limit
(`MemtablePool.java:153-172`); when `used() > nextClean` the `MemtableCleanerThread`
fires the cleaner (`MemtablePool.java:127-149`), which is
`AbstractAllocatorMemtable::flushLargestMemtable` (`AbstractAllocatorMemtable.java:86`)
— it picks the memtable with the largest on-or-off-heap ownership ratio and flushes it
(`AbstractAllocatorMemtable.java:257-326`).

`MemtableAllocator` (per memtable) holds two `SubAllocator`s that own a slice of the
corresponding `SubPool` (`MemtableAllocator.java:32-78`). `SubAllocator.allocate(size,
opGroup)` blocks on the pool's `hasRoom` wait-queue unless the op-group is blocking a
flush, in which case it may overshoot the limit (`MemtableAllocator.java:170-198`).
Lifecycle is `LIVE → DISCARDING → DISCARDED` (`MemtableAllocator.java:39-58`):
`setDiscarding` marks owned bytes as reclaiming; `setDiscarded` calls `releaseAll()`
(`MemtableAllocator.java:141-156`).

**The global pool is static and type-fixed at startup**:
`AbstractAllocatorMemtable.MEMORY_POOL` (`AbstractAllocatorMemtable.java:61`) is built
from `memtable_allocation_type` + `memtable_heap_space` / `memtable_offheap_space` +
`memtable_cleanup_threshold` (`AbstractAllocatorMemtable.java:80-114`;
config enum `Config.java:1286-1307`, default `heap_buffers` `Config.java:565`;
yaml docs `conf/cassandra.yaml:808-841`):

| `memtable_allocation_type` | Pool | Allocator | Data bytes live in |
|---|---|---|---|
| `unslabbed_heap_buffers` | `HeapPool` (`HeapPool.java:31`) | `HeapPool.Allocator` | individual heap `ByteBuffer.allocate` per value (`HeapPool.java:70-74`) |
| `unslabbed_heap_buffers_logged` | `HeapPool.Logged` (`HeapPool.java:87`) | same + allocation listener (simulation) | heap buffers |
| `heap_buffers` (default) | `SlabPool` heap variant (`SlabPool.java:25-34`) | `SlabAllocator(allocateOnHeapOnly=true)` | 1 MiB on-heap slab regions, bump-pointer sliced (`SlabAllocator.java:50, 77-113, 197-206`) |
| `offheap_buffers` | `SlabPool` (off-heap limit > 0) | `SlabAllocator(allocateOnHeapOnly=false)` | 1 MiB `ByteBuffer.allocateDirect` regions; freed by `MemoryUtil.clean` at discard (`SlabAllocator.java:115-120`) |
| `offheap_objects` | `NativePool` (`NativePool.java:21-33`) | `NativeAllocator` | raw `malloc` regions (8 KiB→1 MiB doubling) via JNA `Native.malloc` (`NativeAllocator.java:212-272`, `MemoryUtil.java:85-93`); freed by `Native.free` at discard (`NativeAllocator.java:274-280`) |

Notes relevant to an FFM redesign:

- Both slab-style allocators bump-pointer-carve regions with a CAS race on region
  swap and a **global static stash of race-allocated regions reused across memtables**
  (`SlabAllocator.java:54, 136-152`; `NativeAllocator.java:56-63, 244-258`). A
  cross-memtable region stash is incompatible with arena-per-memtable ownership and
  would have to be dropped (it exists only to avoid wasting a lost-CAS allocation).
- Allocations > 128 KiB bypass regions (`SlabAllocator.java:51, 91-99`;
  `NativeAllocator.java:53, 216-219, 260-272`).
- `NativeAllocator` implements `AddressBasedAllocator { long allocate(int, OpOrder.Group) }`
  (`AddressBasedAllocator.java`), i.e. the entire native side trades in raw `long`
  addresses.
- The per-write `Cloner` supports *context-aware cloning*: the updater pre-estimates the
  bytes a merge will need and carves one contiguous chunk, sub-allocating from it
  without further synchronization (`NativeAllocator.NativeCloner`,
  `NativeAllocator.java:116-205`; buffer flavor `MemtableBufferAllocator.java:39-108`;
  driven from `BTreePartitionUpdater.makeMergedPartition`,
  `BTreePartitionUpdater.java:87-115`, unused remainder returned via `adjustUnused()`
  `BTreePartitionUpdater.java:215-220`).

## 1.3 What is actually off-heap today — inventory

Memtable content for a partition is: `DecoratedKey` → (`AtomicBTreePartition` |
trie value `BTreePartitionData`) → BTree of `Row`s → `Cell`s, plus `DeletionInfo`,
static row, `EncodingStats`, `RegularAndStaticColumns`.

| Component | `heap_buffers` | `offheap_buffers` | `offheap_objects` | `offheap_objects` + TrieMemtable |
|---|---|---|---|---|
| Partition index structure (skip-list nodes / trie nodes) | heap | heap (skip-list) | heap (skip-list) | **off-heap** trie node buffers (`InMemoryTrie.java:151`, `TrieMemtable.java:96`); content refs on heap (`InMemoryTrie.java:162-176`) |
| Partition key bytes | heap slab | **off-heap** slab (`BufferDecoratedKey` over direct slice) | **off-heap** (`NativeDecoratedKey` 4-byte len + bytes, `NativeDecoratedKey.java:36-57`) | off-heap twice: byte-comparable form in trie + cloned key (clone via `SkipListMemtable.put`-equivalent; trie iteration rebuilds heap keys, `TrieMemtable.java:350-356`) |
| Partition key object shell + `Token` | heap | heap | heap (`DecoratedKey` subclass + Token, counted via `ROW_OVERHEAD_HEAP_SIZE`, `SkipListMemtable.java:74-82, 226-253`) | heap on read (rebuilt `BufferDecoratedKey`) |
| Partition object (`AtomicBTreePartition` + `BTreePartitionData`) | heap | heap | heap (`AtomicBTreePartition.EMPTY_SIZE`, `BTreePartitionData.UNSHARED_HEAP_SIZE`, `SkipListMemtable.java:242-243`) | heap (`BTreePartitionData` is the trie content value, `TrieMemtable.java:120,528`) |
| BTree node arrays (`Object[]`) per partition | heap | heap | heap (`BTreePartitionUpdater.java:124`) | heap |
| Row object (`BTreeRow` header: clustering ref, `LivenessInfo`, `Deletion`, `Object[] btree`) | heap | heap | heap (only the *clustering* it points to is native) | heap |
| Clustering bytes | heap slab | **off-heap** slab (`Clustering.clone(ByteBufferCloner)` → heap `ByteBuffer` shells over direct memory) | **off-heap** packed (`NativeClustering` offsets+bitmap+data, `NativeClustering.java:59-97`) | off-heap |
| Clustering object shell | heap (`BufferClustering` + `ByteBuffer[]`) | heap (`BufferClustering` + one heap `ByteBuffer` object per component) | heap, fixed ~`EMPTY_SIZE` (single `peer`, `NativeClustering.java:43-47`) | same |
| Cell header (timestamp/ttl/ldt/length) | heap (`BufferCell` fields) | heap (`BufferCell` fields) | **off-heap** packed record (`NativeCell` layout `cellpath?:ts:ttl:ldt:len:data`, `NativeCell.java:41-46, 141-163`) | same |
| Cell value bytes | heap slab | **off-heap** slab | **off-heap** (same record) | same |
| Cell object shell | heap `BufferCell` + heap `ByteBuffer` object | heap `BufferCell` + heap `ByteBuffer` object (direct-buffer *view* object is on heap) | heap `NativeCell` shell: object header + column ref + 8-byte peer (`NativeCell.java:39, 255-264`) | same |
| Complex cell path | heap | off-heap value, heap `CellPath` object on read (`NativeCell.java:212-220`) | same | same |
| `DeletionInfo` / range tombstones | **heap always** — explicitly cloned with `HeapCloner` (`BTreePartitionUpdater.java:152-156`) | heap | heap | heap |
| Static row | as regular rows | as regular rows | as regular rows | as regular rows |
| `EncodingStats`, `ColumnsCollector`, min timestamps | heap (`AbstractMemtable.java:52-77`) | heap | heap | heap (per shard, `TrieMemtable.java:530-548`) |
| Skip-list per-partition overhead (node + index) | heap, ~`ROW_OVERHEAD_HEAP_SIZE` measured at startup (`SkipListMemtable.java:74-82,226-253`) | heap | heap | n/a (replaced by off-heap trie) |

**What dominates heap in `offheap_objects` mode** (SkipListMemtable): per *partition* —
skip-list node + index entries + `AtomicBTreePartition` + `BTreePartitionData` +
`DecoratedKey`/Token shells (this is exactly what `ROW_OVERHEAD_HEAP_SIZE` estimates,
`SkipListMemtable.java:226-253`); per *row* — `BTreeRow` + BTree leaf array slot +
`LivenessInfo`/`Deletion` objects; per *cell* — the `NativeCell` shell (~32 B) and, on
*every read*, fresh heap "hollow" `ByteBuffer` views (`NativeCell.java:186-190`,
`MemoryUtil.java:122-140`). For small cells the heap shells can rival or exceed the
off-heap payload. TrieMemtable removes the per-partition skip-list overhead (its trie
nodes are off-heap) but keeps everything from `BTreePartitionData` down on heap.
`DeletionInfo` is a structural heap-only gap on all paths.

## 1.4 The copying story today

### Write path (one mandatory copy, sometimes two)

1. A `PartitionUpdate` arrives holding heap (or commit-log direct) buffers.
2. `Memtable.put` obtains a per-write `Cloner`: `allocator.cloner(opGroup)`
   (`SkipListMemtable.java:121`, `TrieMemtable.java:552`).
3. Partition key: cloned on first insertion — `cloner.clone(update.partitionKey())`
   (`SkipListMemtable.java:125`); native flavor allocates and `memcpy`s
   (`NativeAllocator.java:105-108`, `NativeDecoratedKey.java:36-57`); buffer flavor
   copies into a slab slice (`ByteBufferCloner.java:44-48, 94-106`). TrieMemtable
   additionally writes the key's byte-comparable form into the trie
   (`TrieMemtable.java:572-575`, `InMemoryTrie` chain blocks).
4. Rows/cells: `BTreePartitionUpdater.insert/merge` clones every surviving `Row`,
   `Clustering`, `Cell` through the cloner (`BTreePartitionUpdater.java:160-201`);
   `NativeCell` constructor performs the `memcpy` into the region
   (`NativeCell.java:141-163`); `ByteBufferCloner.clone` does the slab copy
   (`ByteBufferCloner.java:94-106`). Context-aware cloning batches these into one
   region allocation per update (`BTreePartitionUpdater.java:87-115`).
5. Range tombstones get a *second* copy semantics-wise: cloned to **heap**
   (`BTreePartitionUpdater.java:152-156`).

This single mutation-buffer→memtable copy is inherent (the mutation buffer is transient,
pooled, or commit-log owned) and would remain in an FFM design.

### Read path (the big one): `EnsureOnHeap`

Reads run inside `cfs.readOrdering` op-groups
(`ColumnFamilyStore.java:299`, `ReadExecutionController.java:137-146`), which protect
memtable memory until the group closes (see §1.6). But returned objects can escape that
scope (row cache, paging, index maintenance, anything that retains a `Row`), so both
off-heap allocators force a **full defensive copy of every result back on-heap**:

- `MemtableAllocator.ensureOnHeap()` returns `EnsureOnHeap.CloneToHeap` for
  `NativeAllocator` (`NativeAllocator.java:67, 207-210`) and for off-heap
  `SlabAllocator` (`SlabAllocator.java:65-75`); heap allocators return NoOp
  (`HeapPool.java:76-79`).
- `CloneToHeap` re-clones partition keys, rows, markers and deletion info with
  `HeapCloner` (`EnsureOnHeap.java:51-123`).
- Hooked into every memtable read surface: `AtomicBTreePartition` wraps *every*
  accessor — `deletionInfo()`, `staticRow()`, `partitionKey()`, `getRow()`,
  `lastRow()`, `unfilteredIterator(...)`, `iterator()`
  (`AtomicBTreePartition.java:197-233`); TrieMemtable wraps reads identically via
  `MemtablePartition` (`TrieMemtable.java:675-754`) and passes
  `allocator.ensureOnHeap()` into partition/row iterators (`TrieMemtable.java:311, 323`).
- Flush explicitly opts out: the flush set iterates with `EnsureOnHeap.NOOP` since the
  memtable is guaranteed alive until flush completes (`TrieMemtable.java:474-480`).

Consequence: in `offheap_buffers`/`offheap_objects`, **every memtable read allocates a
heap copy of every key, clustering, cell value and deletion it touches.** This is the
single largest copying cost an FFM design should attack — but note it is a *policy*
forced by unsafe lifetimes, not by the storage format.

### Flush path

- Flush iterates live partitions and feeds `partition.unfilteredIterator()` to
  `writer.append(iter)` (`Flushing.java:164-181`). No bulk materialization, but each
  row/cell is serialized field-by-field.
- `offheap_objects` is already copy-minimal here: `NativeAccessor.write(NativeData,
  DataOutputPlus)` streams cell bytes straight from the native address into the output
  buffer via `out.writeMemory(address, size)` (`NativeAccessor.java:68-71`,
  `DataOutputPlus.java:48`, `BufferedDataOutputStreamPlus.java:144`);
  `NativeClustering.writeValueSkippingNullAndEmpty` does the same for clustering
  components (`NativeClustering.java:125-129, 157-176`). This `NativeData`/
  `NativeAccessor` machinery landed recently (commit `063e1fe3d2`, "Introduce
  NativeAccessor to avoid new ByteBuffer allocation on flush for each NativeCell") and
  is the in-tree proof that a non-ByteBuffer `ValueAccessor` works end-to-end.
- `offheap_buffers`/heap modes write through `ByteBuffer` accessors (no extra copy
  beyond the output buffer, but heap view objects per cell).

## 1.5 Unsafe / legacy native memory mechanisms FFM would replace

- `MemoryUtil` (`utils/memory/MemoryUtil.java`) is the core: reflective capture of
  `sun.misc.Unsafe.theUnsafe` plus direct-`ByteBuffer` internals (`address`,
  `capacity`, `limit`, `position`, `att` field offsets) (`MemoryUtil.java:47-72`);
  `malloc`/`free` via **JNA** (`MemoryUtil.java:85-93`); raw get/set/copy
  (`MemoryUtil.java:95-261`); "hollow" direct buffers fabricated with
  `unsafe.allocateInstance` and field-poked addresses
  (`MemoryUtil.java:122-189`) — this is how `NativeCell.byteBufferValue()` and
  `NativeDecoratedKey.getKey()` materialize views; explicit `Cleaner` invocation using
  `jdk.internal.ref.Cleaner` + `sun.nio.ch.DirectBuffer` (`MemoryUtil.java:330-359`).
- Endian-typed wrappers: `NativeEndianMemoryUtil`, `BigEndianMemoryUtil`,
  `LittleEndianMemoryUtil` (used by `NativeCell`/`NativeClustering`/
  `NativeDecoratedKey` record layouts).
- `io.util.Memory` (`io/util/Memory.java`) — Unsafe-backed off-heap arrays used by
  index summaries, compression metadata etc. (not memtable-specific but same
  dependency).
- `InMemoryTrie` reads/writes its off-heap node buffers through **agrona
  `UnsafeBuffer`** over `ByteBuffer.allocateDirect` chunks (`InMemoryTrie.java:88-94,
  140-160`; `InMemoryReadTrie.java:242-275`), freed by `MemoryUtil.clean`
  (`InMemoryTrie.java:186-196`).
- Runtime needs `--add-exports jdk.unsupported/sun.misc=ALL-UNNAMED`, `--add-opens
  java.base/jdk.internal.ref=ALL-UNNAMED`, `java.base/sun.nio.ch` etc.
  (`conf/jvm17-server.options:69-76`, `conf/jvm21-server.options:90-109`).

**JDK pressure**: `sun.misc.Unsafe` memory-access methods were deprecated for removal in
JDK 23 (JEP 471) and emit runtime warnings since JDK 24 (JEP 498), with
disable-by-default and removal staged in subsequent releases; `jdk.internal.ref.Cleaner`
and direct-buffer internals are similarly hostile territory. FFM (plus
`VarHandle`/`MethodHandles` for the on-heap cases) is the sanctioned replacement. This
investigation's memtable is the natural first beachhead because the lifetime model
(§1.6) matches arenas perfectly.

## 1.6 Lifecycle and ownership: OpOrder is the safety contract

`OpOrder` (`utils/concurrent/OpOrder.java:27-113`) provides epoch-style grouping:
producers wrap operations in `start()/close()` groups; a consumer creates a `Barrier`,
`issue()`s it, and `await()`s completion of all groups started before issue.

Two independent orders protect memtable memory:

1. **Write order** (`Keyspace.writeOrder`): the Flush constructor swaps the memtable and
   issues the write barrier (`ColumnFamilyStore.java:1247-1271`); `accepts()` directs
   pre-barrier writes to the old memtable
   (`AbstractMemtableWithCommitlog.java:71-94`). `Flush.run()` calls
   `writeBarrier.markBlocking(); writeBarrier.await()`
   (`ColumnFamilyStore.java:1283-1286`) so no in-flight write can touch the memtable
   after flush begins. `markBlocking` also lets blocked allocations overshoot the pool
   limit so flushes can't deadlock on memory
   (`MemtableAllocator.java:181-185`).
2. **Read order** (`cfs.readOrdering`, `ColumnFamilyStore.java:299`): every local read
   holds a group for the duration of the query
   (`ReadExecutionController.java:137-146`). `Flush.reclaim()` issues a read barrier
   and defers `memtable.discard()` until `readBarrier.await()` (plus post-flush
   completion) on the `reclaimExecutor` (`ColumnFamilyStore.java:1439-1452`). Since the
   memtable was already removed from the live view (`markFlushing`,
   `ColumnFamilyStore.java:1291-1293`), no *new* read can discover it; the barrier
   drains the old ones.

Only then does `discard()` actually free memory:
`NativeAllocator.setDiscarded()` → `Native.free` per region
(`NativeAllocator.java:274-280`); `SlabAllocator.setDiscarded()` →
`MemoryUtil.clean` per direct region (`SlabAllocator.java:115-120`);
`TrieMemtable.discard()` → `shard.data.discardBuffers()` (`TrieMemtable.java:163-177`,
`InMemoryTrie.java:186-196`).

Residual hazard today: this is *convention*, unverifiable by the runtime. Any object
that escapes its read group (which is exactly why `EnsureOnHeap` exists) and is touched
after discard reads freed native memory — silent corruption or SIGSEGV. **This is the
precise hole FFM arenas close.**

---

# Phase 2 — FFM design sketch

## 2.1 Arena lifecycle mapping

**Proposal**: one `Arena.ofShared()` per memtable (or per shard for a TrieMemtable-style
implementation — see 2.4), created in the factory's `create()`, closed in `discard()`
exactly where `setDiscarded()`/`discardBuffers()` free memory today.

Why shared, not confined: writes arrive on many mutation threads, reads on others, flush
on flush executors — multi-thread access is mandatory. (`Arena.ofConfined()` restricts
access *and* close to the owning thread; any other thread gets `WrongThreadException`.)

**JDK semantics of shared-arena close** (final API, JDK 22+, JEP 454):

- `close()` may be called from any thread. It performs a global thread-local handshake:
  it synchronizes with every thread that might be mid-access into the arena's segments.
  Accesses racing with close either complete before the close or fail with
  `IllegalStateException` ("already closed"); memory is unmapped/freed only when no
  access can still be in flight. There is **no torn read of freed memory and no
  use-after-free** — the failure mode is an exception, never corruption.
- The handshake makes shared close *expensive* (it briefly involves all threads), so it
  must be infrequent. Once per memtable flush (seconds-to-minutes cadence) is ideal;
  per-row or per-read arenas would be a misuse.
- `MemorySegment.asByteBuffer()` views inherit the arena lifetime: buffer access after
  close also throws `IllegalStateException`, which extends the safety net to ByteBuffer
  interop.
- If close is called *while another thread holds a pending access*, close itself may
  fail with `IllegalStateException` in narrow windows; retry is the documented pattern.
  In our design the OpOrder read barrier makes that window empty in the non-buggy case.

**OpOrder × Arena.close interaction**: by construction, `discard()` runs strictly after
(a) the write barrier await — no writers — and (b) the read barrier await — no readers
started before the memtable left the live view (`ColumnFamilyStore.java:1283-1286,
1439-1452`). So `arena.close()` at the `discard()` point is exactly as safe as today's
`free()`, with one categorical improvement: **a lifetime bug (escaped row read after
discard) becomes a thrown `ISE` instead of memory corruption.** The OpOrder machinery is
kept as the *liveness/ordering* mechanism (it also drives accounting and commit-log
bounds); the Arena becomes the *enforcement* mechanism. They are complementary, not
redundant.

Failure handling: if `arena.close()` throws (straggler access), log + retry on the
reclaim executor; the memtable is already out of the view so the straggler will finish.
Never leave the arena unclosed silently — that's the native-memory-leak case; consider a
`Cleaner`-registered fallback (FFM arenas are not auto-collected for `ofShared()`).

## 2.2 Data layout: structured records vs blob-append

Two layers, mirroring what works today:

1. **Slab layer (blob append)**: allocate large segments from the arena
   (`arena.allocate(1 MiB)`), bump-pointer carve as `SlabAllocator`/`NativeAllocator.Region`
   do (`NativeAllocator.java:311-360`). FFM gives two carving options:
   - keep `long offset` arithmetic within a `MemorySegment region` and represent
     allocations as `(region, offset, length)`; or
   - `region.asSlice(offset, length)` producing a zero-length-checked child segment
     (allocation-free? **no** — a slice is a new heap object, ~similar weight to today's
     hollow ByteBuffers). For the stored records prefer *(segment, offset)* flyweights
     to avoid per-cell slice objects; create slices only at API boundaries.
   Per-update context cloning carries over directly: `estimateCloneSize` → one slab
   carve → sub-allocate (`BTreePartitionUpdater.java:87-115` needs no change).
2. **Record layer (structured)**: re-encode today's hand-rolled native records as
   `MemoryLayout`s with derived `VarHandle`s:

   ```java
   // NativeCell layout, today hand-offset (NativeCell.java:41-46)
   static final MemoryLayout CELL = MemoryLayout.structLayout(
       ValueLayout.JAVA_BYTE.withName("hasPath"),
       ValueLayout.JAVA_LONG_UNALIGNED.withName("timestamp"),
       ValueLayout.JAVA_INT_UNALIGNED.withName("ttl"),
       ValueLayout.JAVA_INT_UNALIGNED.withName("ldt"),
       ValueLayout.JAVA_INT_UNALIGNED.withName("length"));
       // followed by length bytes of value, then optional path
   static final VarHandle TIMESTAMP = CELL.varHandle(groupElement("timestamp"));
   ```

   Unaligned layouts are required because records are byte-packed at arbitrary slab
   offsets (same as today). VarHandles buy: typed/endian-explicit access (replacing the
   three `*EndianMemoryUtil` classes), JIT-friendly constant offsets, and optional
   volatile/acquire-release modes — the latter matters for the trie, whose correctness
   depends on `putIntVolatile` publication (`InMemoryTrie.java:200-237`,
   `attachChildToSparse` ordering comments `InMemoryTrie.java:317-331`); agrona
   `UnsafeBuffer.putIntVolatile` maps to
   `segment.set(JAVA_INT_UNALIGNED, off, v)` with a `VarHandle` release/volatile mode.
   Variable-size payloads (values, clusterings, keys) stay raw:
   `MemorySegment.copy(...)` in, `segment.asSlice`/`mismatch`/`copy` out.

Recommendation: structured layouts for the fixed headers (cell header, clustering
offset table, key length prefix — i.e. byte-for-byte the formats in
`NativeCell.java:141-163`, `NativeClustering.java:59-97`,
`NativeDecoratedKey.java:36-57`), blob-append for everything else. Do **not** invent a
new row-level format in v1; keep per-cell/per-clustering records so the existing BTree
merge machinery works unchanged. A consolidated off-heap *row* record (header + cells
contiguous) is the v2 opportunity, and is what a cursor-style read path (cf. the
garbage-free cursor-compaction work in this directory) would want.

## 2.3 Zero/minimal-copy read path

Three escalation levels:

1. **Parity (v1)**: keep `EnsureOnHeap.CloneToHeap` for reads. Safe, simple, identical
   behavior to `offheap_objects`. The FFM win at this level is lifetime safety +
   Unsafe removal only.
2. **SegmentValueAccessor (v2)**: add `ValueAccessor<MemorySegment>` (or a
   `SegmentData` wrapper mirroring `NativeData`). The brand-new
   `NativeAccessor`/`NativeData` abstraction (`NativeAccessor.java:41-56`,
   `NativeData.java`, `AddressBasedNativeData.java:26-81`) is a 1:1 template:
   `getAddress()→segment+offset`, `nativeDataSize()→length`,
   `asByteBuffer()→segment.asByteBuffer()`, `slice()→asSlice()`. Everything
   `NativeAccessor` does with `MemoryUtil` becomes `MemorySegment.copy`/`mismatch`;
   `write(value, DataOutputPlus)` keeps using `out.writeMemory(...)`
   (`DataOutputPlus.java:48`) via `segment.address()+offset` — or grows a
   `writeMemory(MemorySegment, long, int)` overload to stay in safe API. Flush then
   reads cells **in place** with zero materialization, as `offheap_objects` already
   does; comparisons (`compare`, `ClusteringComparator`) run off-heap via
   `MemorySegment.mismatch` without buffer views.
3. **Escape-analysis-honest reads (v3)**: `EnsureOnHeap` becomes a *boundary* transform
   rather than a blanket one: within the read op-group, rows are served as
   segment-backed flyweights (safe: arena open while group held, §2.1); only sinks that
   retain data past the group (row cache population, paging state, index builds,
   anything calling `Clustering.retainable()` — cf. `NativeClustering.java:280-295`)
   clone to heap. With FFM this hardens from "convention" to "runtime-checked": a missed
   clone throws ISE on later access instead of corrupting. This dovetails with the
   cursor/garbage-free direction of this branch (cursor-compaction-plan.md's prime
   constraint): a memtable read *cursor* over consolidated off-heap rows would make
   memtable→sstable flush and memtable reads allocation-free in steady state, the same
   property `CursorCompactor` enforces for sstable→sstable.

ByteBuffer interop note: `segment.asByteBuffer()` allocates a (heap) view object per
call, just like today's `MemoryUtil.getByteBuffer` hollow buffers
(`NativeCell.java:186-190`) — so level 2/3 should prefer accessor-generic code paths
(`ValueAccessor`-parameterized, already pervasive) over `toBuffer` conversions.

## 2.4 Write path & concurrency

- **Cloning**: unchanged shape. `FFMCloner implements Cloner` performing
  `MemorySegment.copy(srcHeapArrayOrBuffer, ..., slab, offset, len)`; the one
  mutation→memtable copy remains. Context-aware estimation carries over
  (`BTreePartitionUpdater.java:87-115`). Source access: heap `byte[]` and `ByteBuffer`
  copy into segments via `MemorySegment.ofArray`/`MemorySegment.ofBuffer` + `copy` —
  no Unsafe.
- **Allocation strategy**: per-memtable shared arena; region carving with the same
  CAS-bump scheme (`NativeAllocator.java:221-258`) — drop the cross-memtable
  `RACE_ALLOCATED` stash (incompatible with arena ownership; the waste it prevents is
  one lost region per CAS race, bounded and rare). Oversize (>128 KiB) allocations go
  straight to `arena.allocate(size)` like today's oversize regions
  (`NativeAllocator.java:260-272`).
- **Sharding fit**: TrieMemtable's model — N shards, single-writer-per-shard via
  `ReentrantLock`, concurrent readers (`TrieMemtable.java:109-114, 550-596`) — fits
  arenas well. Options: (a) one shared arena per memtable, all shards carve from it
  (simplest, one close at discard); (b) arena per shard (close in the existing per-shard
  `discardBuffers()` loop, `TrieMemtable.java:172-176`; N smaller handshakes, better
  NUMA/locality story, slightly more bookkeeping). Even with single-writer shards,
  *confined* arenas remain impossible (readers + flush threads), so per-shard arenas are
  still `ofShared()`. Recommendation: per-memtable in v1; revisit per-shard if close
  handshake cost shows up.
- **The trie itself**: `InMemoryTrie`'s chunked buffers
  (`InMemoryTrie.java:88-94, 140-160`) port mechanically: `UnsafeBuffer` chunk →
  `MemorySegment` chunk from the arena; volatile node-pointer publication via VarHandle
  volatile modes (§2.2). This also retires agrona-on-direct-ByteBuffer and
  `MemoryUtil.clean` from the trie path.

## 2.5 Pluggability / opt-in shape

Two viable shapes:

1. **New memtable class (recommended)** — `class_name: FFMMemtable` (or
   `org.example.FFMMemtable` from a plugin jar) under `memtable.configurations`, chosen
   per-table with `WITH memtable = 'ffm'`. The factory creates the arena-backed
   allocator itself instead of `MEMORY_POOL.newAllocator()`. Accounting integration
   *without* a new pool type: `MemtablePool.SubPool.newAllocator()` is public
   (`MemtablePool.java:248-251`), so the FFM allocator can be a `MemtableAllocator`
   subclass wired to the existing global pool's sub-pools — limits, cleanup
   thresholds, `flushLargestMemtable`, metrics and `markBlocking` overshoot all work
   unchanged, regardless of which `memtable_allocation_type` built the global pool.
   The only soft wrinkle: with the default `heap_buffers` pool, `memtable_offheap_space`
   defaults still apply to the off-heap sub-pool limit, which is exactly what we want
   FFM allocations charged against. Per-table opt-in, no global switch, coexists with
   other memtables on other tables.
2. **New `memtable_allocation_type` (e.g. `offheap_segments`)** — a sixth enum value
   (`Config.java:1286-1307`) building an `FFMPool` in
   `createMemtableAllocatorPoolInternal` (`AbstractAllocatorMemtable.java:91-114`), so
   *existing* memtable classes (SkipList/Trie) transparently get FFM-backed cloners the
   way they get `NativeAllocator` today. Bigger blast radius (global, affects every
   table, requires in-tree JDK gating in config validation), but it is the true
   analogue of the current opt-in style and reuses `NativeCell`-equivalents across all
   memtable classes.

Path: prototype as (1) out-of-tree; productionize as (1) in-tree once the JDK baseline
allows; consider (2) only after the allocator is proven, since (2) is pure wiring at
that point.

## 2.6 JDK constraints (precise)

- **Trunk today**: `build.xml:47-48` — `java.default=11`,
  `java.supported="11,17,21"`; per-version blocks at `build.xml:290, 334, 399`. Runtime
  scripts ship `jvm11/17/21-server.options`.
- **FFM availability**: incubator (`jdk.incubator.foreign`) JDK 14–18; **preview**
  `java.lang.foreign` JDK 19 (JEP 424), 20 (JEP 434), 21 (JEP 442); **final** JDK 22
  (JEP 454), stable through current releases (JDK 25 LTS). On JDK 21 the classes exist
  only as preview API: both `javac --enable-preview --release 21` *and* `java
  --enable-preview` are required, and preview class files are release-locked —
  unsuitable for shipping.
- Therefore: **no trunk-supported JDK provides final FFM.** Options, most to least
  attractive:
  1. *Out-of-tree plugin jar* compiled `--release 22` (or 25), loaded reflectively via
     `memtable.configurations` (`MemtableParams.java:227-244`) on nodes running JDK 22+.
     Zero in-tree changes; the right prototype vehicle now.
  2. *In-tree, JDK-gated*: wait for the trunk baseline to add a 22+ JDK (the 6.0-era
     JDK lineup is still in motion; FFM-final is in every JDK ≥22, so any future LTS
     bump — e.g. 25 — unlocks it). Then the factory class is compiled normally and
     config validation rejects it on older runtimes with a clean
     `ConfigurationException`.
  3. *Multi-release jar / separate source set with reflective dispatch*: possible
     (compile FFM sources with a newer javac into `META-INF/versions/22`), but the ant
     build compiles a single source/target (`build.xml:675-678`) — this adds real build
     complexity and is only worth it if in-tree delivery must precede the baseline bump.
  4. `MethodHandle`-based reflection against `java.lang.foreign` at runtime: works but
     forfeits the API's type-safety and most of its JIT friendliness on the hot path;
     not recommended for a memory allocator hot path.
- Side benefit once on 22+: `--enable-native-access=ALL-UNNAMED` replaces several
  `--add-opens` lines; the JNA `malloc` dependency for memtables disappears
  (`MemoryUtil.java:85-93`).

## 2.7 Risks and open questions (register)

| # | Risk / question | Notes & mitigation |
|---|---|---|
| R1 | Bounds+liveness check overhead vs Unsafe on per-cell hot paths | FFM accesses carry bounds and session-liveness checks; C2 hoists them in loops, and JDK 21+ brought shared-session access to near-Unsafe parity in published benchmarks, but Cassandra's access pattern is pointer-chasing (one 8-byte read per VarHandle hit), the worst case for check elision. Must be measured (microbench: NativeCell vs SegmentCell timestamp/value access; macro: write/read/flush throughput). |
| R2 | Shared-arena close handshake cost | One global handshake per memtable (or per shard) per flush. At normal flush cadence this is noise; verify on many-table clusters where flushes are frequent (hundreds of tables → hundreds of closes). Mitigation: per-memtable (not per-shard) arenas; batch closes on the reclaim executor. |
| R3 | Heap-object shells still dominate small-cell workloads | FFM alone doesn't remove `BTreeRow`/cell shells (§1.3). Honest framing: v1 ≈ `offheap_objects` heap profile with safer lifetimes. The full win needs the v2/v3 record+cursor work. |
| R4 | `asByteBuffer()` / `asSlice()` view churn at interop boundaries | Same class of cost as today's hollow buffers; keep `ValueAccessor`-generic paths, create views only at escape boundaries. |
| R5 | Native memory accounting drift | Arena allocates region-granular; pool accounting is byte-granular via SubAllocators (unchanged). Keep them decoupled as today (regions vs owns) — but `Arena` also has its own `MemorySegment` bookkeeping; do **not** double-report to JMX. NMT visibility: FFM memory shows under a distinct NMT category, which is operationally *better* than JNA malloc (invisible to NMT). |
| R6 | Flush interop: `DataOutputPlus.writeMemory(long address, int length)` takes a raw address | `segment.address()` works but bypasses liveness checks on the write; safe because flush holds the memtable; cleaner long-term: `writeMemory(MemorySegment, offset, length)` overload. |
| R7 | OpOrder discipline violations surfacing as ISE | A behavior *change*: bugs that silently corrupted now throw. Good for safety, but means latent escapes (if any exist) become user-visible exceptions on upgrade to the FFM memtable. Treat every ISE in testing as a real pre-existing lifetime bug. |
| R8 | Trie volatile-publication semantics on MemorySegment | Must replicate `putIntVolatile` happens-before edges (`InMemoryTrie.java:200-237`); FFM VarHandles support volatile access modes **only for aligned access** — the trie's 4-byte-aligned node pointers are fine (block layout is 32-byte aligned), but this must be asserted, not assumed. |
| R9 | jamm/ObjectSizes metering | `ROW_OVERHEAD_HEAP_SIZE`-style estimation (`SkipListMemtable.java:226-253`) and `@Unmetered` conventions need FFM-aware equivalents (segments must not be deep-measured). |
| R10 | Upgrade/rollback | Memtables are volatile — no on-disk format involvement; rollback = `ALTER TABLE ... memtable = 'default'` + flush. Lowest-risk category of storage change. Commit-log replay works unchanged (replay is just `put`s). |
| R11 | GC interactions | Off-heap data invisible to GC (as today); risk is *over*-allocating arenas relative to `memtable_offheap_space` if accounting bugs creep in — pool limits remain the enforcement point. |
| Q1 | Per-memtable vs per-shard arenas (close cost vs locality)? | Measure under R2. |
| Q2 | Should `DeletionInfo`/range tombstones finally move off-heap in the new design (today always heap, `BTreePartitionUpdater.java:152-156`)? | Yes in v2 record design; keep heap in v1 for parity. |
| Q3 | Does `Token` (heap, hash-cached) need an off-heap representation for full "all data off-heap"? | Probably not worth it; tokens are tiny and hot. |
| Q4 | Reuse `NativeData` interface with a segment-backed impl vs new `SegmentData`? | `NativeData.getAddress()` returning `long` invites unchecked access; prefer a parallel interface, share the accessor patterns. |
| Q5 | When does trunk's JDK baseline include ≥22? | External dependency; determines in-tree timing (§2.6). |

## 2.8 Suggested prototype plan (smallest end-to-end slice first)

1. **Slice 0 — out-of-tree skeleton** (JDK 22+/25 jar): `FFMMemtable extends
   SkipListMemtable`-shape class + `factory(Map)`; config it via
   `memtable.configurations`; verify create/put/read/flush/discard with the *existing*
   pool allocator first (pure plumbing, proves the plugin path).
2. **Slice 1 — FFMAllocator**: `Arena.ofShared()` per memtable; region carving;
   `Cloner` producing `SegmentDecoratedKey`/`SegmentClustering`/`SegmentCell`
   (byte-identical layouts to `Native*` so flush/test oracles can compare); reads via
   `CloneToHeap`; accounting through `MemtablePool.SubPool.newAllocator()`. Close arena
   in `discard()`. This is the end-to-end proof: lifetime, accounting, flush, OpOrder.
3. **Slice 2 — differential + lifetime harness**: same-workload write to
   `SkipListMemtable(offheap_objects)` and `FFMMemtable`, compare full unfiltered walks
   byte-for-byte (the cursor-compaction harness pattern from this branch,
   `garbage-free-compaction-improvements/cursor-compaction-plan.md`); stress
   flush-vs-read races to show ISE-not-corruption on injected escapes; JFR
   allocation-gate the read/flush paths (jfr-reports techniques from this branch).
4. **Slice 3 — SegmentValueAccessor**: `ValueAccessor<MemorySegment>` modeled on
   `NativeAccessor` (`NativeAccessor.java:54+`); zero-copy flush via `writeMemory`;
   off-heap comparisons via `mismatch`. Benchmark vs `offheap_objects` (R1).
5. **Slice 4 — read-path escape narrowing**: flyweight reads inside the op-group,
   clone only at retention boundaries; quantify read allocation reduction.
6. **Slice 5 — trie integration**: `InMemoryTrie` chunks on arena segments (per-shard
   or per-memtable); volatile VarHandle audit (R8).
7. **Slice 6 — in-tree proposal**: once the JDK baseline allows — choose shape §2.5(1)
   vs (2), write the CEP/JIRA with benchmark evidence.

## 2.9 Open questions (consolidated)

- Q1–Q5 above, plus:
- Should the FFM memtable own a *new* `memtable_allocation_type` from day one for
  operator familiarity, or is per-table `memtable = 'ffm'` the better opt-in UX? (§2.5)
- Is a consolidated off-heap **row record** (enabling a memtable read cursor) in scope
  for the first in-tree version, or strictly v2? It is the only way to shed the
  per-row/per-cell heap shells that dominate `offheap_objects` heap usage (§1.3), and
  it aligns with this branch's garbage-free cursor direction.
- Can `EnsureOnHeap` be reframed API-wise as "ensure-retainable" (clone only what
  escapes) for *all* off-heap modes, independent of FFM? That would benefit
  `offheap_objects` today and shrink the FFM delta.
