> SCOPE NOTE (2026-06-11): separate future project — NOT part of the cursor-compaction branch work. Compaction work targets JDK 21+ and must not depend on this.

# Plan: Fully off-heap memtable on the FFM API (Arena / MemorySegment)

> Companion to `ffm-memtable-investigation.md` (architecture facts, file:line anchors).
> Working notes, never committed.
>
> **MOONSHOT (decided 2026-06-10):** 100% of memtable data off-heap in Arenas, whatever it
> takes — including core read-path interface changes if the end state requires them. Long
> horizon. **JDK minimum: 24** (decided): FFM final and mature, sun.misc.Unsafe
> memory-access methods formally deprecated-for-removal (JEP 498) — the legacy mechanism
> this replaces is officially dying. No preview flags, no multi-release contortions, no
> out-of-tree gating: in-tree development from the start (out-of-tree jar remains
> available as an iteration-speed convenience only). Pluggable via CEP-11; opt-in.

## End state (design north star, before the phases)

A memtable is: one or more shared Arenas holding (a) a segment-backed trie index whose
content references are RECORD OFFSETS, not object pointers; (b) consolidated partition
records (partition deletion, range tombstones, static row ref, stats deltas); (c)
consolidated row records (clustering | liveness | deletion | cell directory | cells) —
every byte of user data and every index node off-heap. The native read surface is a
CURSOR over records: reusable flyweights, zero per-row/cell allocation. The existing
UnfilteredRowIterator surface survives as a compatibility shim that materializes only at
that boundary — and shrinks in importance if/when the read path itself goes cursor-based
(deliberate convergence: this memtable's records and the cursor-compaction/read-path
cursor direction are the same architecture meeting from two ends).
Token/key shells, hollow ByteBuffers, BTreeRow/Cell objects: none exist on this path.

## Definition of "100% off-heap" (precise claim)

Off-heap: partition index nodes, partition records (incl. DeletionInfo/range tombstones —
heap-only on every path today), row records (liveness/deletion/clustering/cells
consolidated), all key/clustering/cell bytes, cell paths, static rows, and token material
where storable (byte-comparable key form in the trie already encodes ordering).
Irreducibly on-heap and explicitly OUTSIDE the claim: the Memtable root object, arena and
segment bookkeeping (O(#segments), not O(data)), config/aggregate-stats shells, and O(1)
reusable flyweights per active operation. The measurable definition the gates enforce:
heap residency attributable to memtable CONTENT is zero (heap-dump assertion), and
per-operation heap allocation is O(1) regardless of data touched.

## Verification spine (every phase gates on all four)

1. **Differential oracle**: identical mutation streams into `offheap_objects` (SkipList and
   Trie) vs the FFM memtable → identical CQL read results AND byte-identical flushed
   sstables (we already own sstable byte-comparison tooling from the compaction harness).
2. **Allocation-scaling gates** (ThreadMXBean technique from this branch): write path,
   read path, and flush path measured at N vs 10N rows; per-element heap allocation must
   not scale (phase-dependent ceilings, ratcheted down each phase).
3. **Lifecycle safety tests**: Arena.ofShared().close() under concurrent readers —
   OpOrder read/write barriers precede discard (ColumnFamilyStore.java:1247-1286,
   1439-1452), so close-during-read must be *unreachable*; tests prove the barrier
   ordering and that a violated barrier surfaces as IllegalStateException, never
   corruption. Plus leak tests (arena closed exactly once, native bytes return to 0).
4. **JMH**: segment read/write vs Unsafe baseline (the known FFM risk: bounds/liveness
   checks on pointer-chasing access); read/write/flush throughput vs `offheap_objects`.

## Phase 0 — Foundations (small)

- In-tree module skeleton compiled `--release 24`, selected via the
  `memtable.configurations` class-name factory (MemtableParams.java:227-244).
- Port the verification spine: differential memtable harness, allocation gates, JMH
  shells. Baseline all four against `offheap_objects` BEFORE writing any FFM code.
- Decision recorded: build on the TrieMemtable lineage (its InMemoryTrie is already
  buffer-backed — InMemoryTrie.java:151 — the shortest path to a segment-backed index)
  while keeping the SkipList shape as the differential reference.

## Phase 1 — Parity slice: arenas + segment leaf records (the Native* mirror)

- One shared Arena per memtable shard; SubPool accounting integration
  (SubPool.newAllocator() is public — limits/cleanup machinery inherited unchanged).
- `Segment*` records mirroring the Native* layouts byte-for-byte
  (NativeDecoratedKey.java:36-57, NativeClustering.java:59-97, NativeCell.java:41-46):
  same packed `cellpath?:ts:ttl:ldt:len:data` cell record, allocated by slab-carving
  within large segments (one mandatory write-path copy, as today).
- Reads still materialize via CloneToHeap (unchanged semantics); index structures still
  heap (skip-list or heap-trie content refs). Heap shells remain — THIS PHASE PROVES
  LIFECYCLE AND ACCOUNTING, not memory wins.
- Exit: differential oracle green incl. byte-identical flushes; lifecycle tests green;
  JMH within agreed envelope of offheap_objects (target: ±10%); allocation gates
  baseline recorded.

## Phase 2 — Zero-copy reads

- `SegmentValueAccessor implements ValueAccessor<MemorySegment>` (template: the
  NativeAccessor/writeMemory flush work, commit 063e1fe3d2) so comparators, serializers
  and the flush path consume segment slices directly.
- EnsureOnHeap becomes a no-op for FFM memtables (safety argument: OpOrder read barrier
  spans the entire read — investigation §1.6 — so segments outlive every reader that can
  see them; this is the same contract Native* relies on, now enforced by the arena).
- Hollow-ByteBuffer-per-read churn (NativeCell.java:186-190, MemoryUtil.java:122-140)
  eliminated for FFM paths.
- Exit: read-path allocation gate ratchets to O(1) per query for leaf access; read JMH
  must beat offheap_objects (this is the phase that pays rent); differential green.

## Phase 3 — Off-heap row & partition records (kill the shells)

- Consolidated row record: [flags | clustering ref/inline | liveness | deletion |
  cell-count | cell directory | cells] in one segment region; VarHandle-struct header,
  blob tails. Replaces BTreeRow + LivenessInfo/Deletion objects + per-cell shells.
- Partition record: partition deletion + RANGE TOMBSTONES off-heap (DeletionInfo is
  heap-only everywhere today — BTreePartitionUpdater.java:152-156 — this closes a
  structural gap, not just an FFM win) + static row ref + stats deltas.
- Read surface: flyweight cursors over records (one reusable cursor per read; aligns
  with the cursor-compaction direction — same flyweight discipline, and the future
  read-path cursor could consume these records natively).
- Mutation path: copy-on-write record rebuild within the arena (memtable updates are
  merge-and-replace at row granularity already — BTreePartitionUpdater semantics —
  superseded record space reclaimed only at flush, same as slabs today; measure waste).
- Exit: per-row/cell heap residency == 0 (verified by heap-dump assertion test +
  allocation gates); differential + Harry histories green; JMH write path within
  envelope, read path improved or neutral vs phase 2.

## Phase 4 — Off-heap index

- Segment-backed InMemoryTrie buffers (the trie already addresses growable buffers by
  int offsets — swap backing to arena segments; content "refs" become record offsets
  instead of heap object pointers, removing the on-heap content array,
  InMemoryTrie.java:162-176).
- Partition lookup → record offset → flyweight partition/row cursors. No
  AtomicBTreePartition / BTreePartitionData on the FFM path (concurrency: per-shard
  single-writer as TrieMemtable today, TrieMemtable.java:96 sharding).
- Exit: per-PARTITION heap residency == 0; full verification spine green; memory
  accounting accuracy validated (reported native bytes vs RSS delta in a soak).

## Phase 5 — Productization

- Opt-in config story (named memtable configuration; per-table selection; docs), metrics
  (arena bytes, segment utilization, waste ratio), guardrails (refuse on JDK < 22 with
  actionable message).
- Soak: Harry history workloads + the randomized differential generator; flush/compact
  interaction at scale; rollback story (switch config back; next memtable instance is
  plain — no persistent format involvement at all).
- Upstream path: CEP from the start (this is a CEP-scale feature: new storage
  representation + JDK-24 floor); the phased evidence (differential oracles, allocation
  gates, JMH) is the CEP's supporting data.

## Decision points & risks

- **JDK 24 floor** is an assumption of this plan, not yet a project-wide fact — the CEP
  must carry it (motivation: JEP 498 Unsafe deprecation makes the status quo a dead end
  regardless).
- **Bounds-check tax** (phase 1 JMH): if segment access loses badly to Unsafe on
  pointer-chasy reads, mitigations: fewer/larger segments, struct VarHandles over slices,
  read batching via cursors (phase 3 largely removes pointer-chasing anyway). Abort
  criterion: if phase 2 cannot beat offheap_objects reads, stop and publish findings.
- **Arena close handshake** at scale (many tables/shards): measure close latency in soak;
  fallback: confined-arena-per-writer + shared only for the read-visible region.
- **Record waste** under heavy overwrite (phase 3 copy-on-write): measure; mitigation is
  flush-pressure accounting of dead bytes (count superseded record sizes against the
  cleanup threshold).
- **DeletionInfo/RT semantics** (phase 3): the heap-only cloning today hints at subtle
  mutability assumptions — investigate before designing the record (open question #1).
