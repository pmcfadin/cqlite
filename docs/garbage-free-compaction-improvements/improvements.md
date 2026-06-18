# Storage-engine improvements: an aggressive backlog

> Drafted 2026-06-12 on branch `cursor-compaction-completion`. This is a *brainstorm to be
> aggressive*, not a committed plan — ideas range from "land it next week" to "research
> moonshot". Each entry states the idea, why it pays, how it could be built, the expected
> payoff, and the honest risk. Ordered roughly by **payoff ÷ effort**, biggest-bang-for-buck
> first, with the moonshots (GPU, columnar) at the end.
>
> Grounding: the cursor-compaction work proved that the engine's dominant cost at scale is
> **per-object churn** — `new byte[]` per value, per-row/cell/clustering Java shells, iterator
> allocation — and that a **byte-level cursor over reusable flyweights** can erase an entire
> allocation class (2–5× faster compaction, ~100× less garbage). Most ideas below generalize
> that lever to the rest of the engine: read path, memtable, and the CPU/GPU substrate.
>
> Companion docs: `quick-perf-wins.md` (10 ranked low-risk wins, several already on this
> branch), `ffm-memtable-investigation.md` + `ffm-memtable-offheap-plan.md` (off-heap memtable
> design), `bti-sstable-specification.md` (the format these touch),
> `tombstone-histogram-spool-proposal.md` (the 3 MiB spool).

---

## Tier 0 — already scoped, just do them

These are written up elsewhere; listed here so the backlog is complete. All low-risk,
value-identical, verified by the differential suites / allocation gates.

- **Memoize `Columns.deserializeSubset`** for repeated sparse-row shapes (read path); the
  write-side twin `encodeBitmap(Row, superset)`. ~100–150 B/row erased on the everyday
  partial-update shape. (`quick-perf-wins.md` #1, #3.)
- **Lazily allocate the 3 MiB tombstone-histogram spool** in `MetadataCollector` — 62–69% of
  within-compaction allocation on this branch's profiles, gone for tombstone-free tables.
  (`tombstone-histogram-spool-proposal.md`.)
- **Kill `Enum.values()` clones** on the read/message hot paths (`Kind.ALL_KINDS` already
  exists). (`quick-perf-wins.md` #2, #7.)
- **`apply`/`accumulate` instead of BTree iterators** in `writeComplexColumn` /
  `mergeStaticRows` / `guardCollectionSize`. (`quick-perf-wins.md` #5, #6, #9.)
- The micro-TODO sweep in `SSTableCursorReader` / `CursorCompactor` (cell-flags fast path,
  precomputed next-state, cell header class). (`cursor-compaction-plan.md` §"Micro-TODO".)

---

## Tier 1 — the cursor lever, generalized

### 1. Cursor-based **read path** (the big one)

**Idea.** Today only *compaction* reads sstables through the garbage-free cursor
(`SSTableCursorReader`). Every other read — point queries, range scans, repair, streaming —
goes through `SSTableIdentityIterator` → `UnfilteredDeserializer`, materializing `Row` /
`Cell` / `ClusteringPrefix` / `Columns` objects per element. Extend the cursor to the read
path: a `UnfilteredRowIterator` that is a thin compatibility shim over a cursor, materializing
objects **only at the coordinator/CQL boundary**, and not at all for merge/filter/skip.

**Why it pays.** Reads are the larger fraction of most clusters' work, and they pay the *same*
`ByteArrayAccessor.read` per-value allocation (CASSANDRA-20428) that motivated the cursor in
the first place. The byte-comparable BTI row index already lets a cursor *seek* within a
partition without deserializing; partial-range scanners (increment 4) were built deliberately
as "the read-path dividend". The merge logic is already written and differentially verified.

**How.** Reuse `SSTableCursorReader` + the increment-4 seek API. Build `mergeIterator`
equivalents that compare raw byte windows (the cursor merge already does this). Materialize at
`ColumnFilter` application / serialization-to-client only. Gate behind a flag exactly like
cursor compaction; verify with a *differential read* harness (same pattern: read the same data
both ways, assert identical results + allocation gate). This is a large, multi-increment
effort but de-risked by the compaction work.

**Payoff.** Potentially the single largest GC reduction available — reads are everywhere.
**Risk.** High surface area (every read code path eventually). Mitigated by opt-in + the
differential pattern that already works.

### 2. Off-heap **arena memtable** on FFM (`MemorySegment` / `Arena`)

**Idea.** A memtable whose data — keys, clusterings, cells, *and* index nodes, per-row/per-
partition records — lives 100% off-heap in `Arena`-owned `MemorySegment`s, exposed through a
**cursor over records** (reusable flyweights, zero per-row/cell allocation), with the
`UnfilteredRowIterator` surface kept only as a boundary shim. Pluggable via CEP-11, opt-in.

**Why it pays.** Even `offheap_objects` today keeps the partition index (skip-list nodes),
`BTreeRow`/`Cell`/`Clustering` shells, `DeletionInfo`, and all stats on heap, and **copies
everything back on-heap on read** (`EnsureOnHeap.CloneToHeap`). A real arena memtable removes
that GC pressure entirely and — critically — its record layout is the *same architecture* as
the cursor read/compaction path, meeting from the other end (write into arena records →
flush/compact straight from them, never materializing).

**How.** `Arena.ofShared().close()` maps exactly onto Cassandra's existing two-OpOrder-barrier
reclamation (write barrier at switch-out, read barrier before discard) — by the time `free()`
is called today, no reader/writer can touch the memory, so an arena close at the same point is
safe by construction, and a straggler throws `IllegalStateException` instead of reading freed
memory (unlike `Unsafe`). First slice: `FFMAllocator` (arena + segment slabs) + segment-backed
`DecoratedKey`/`Clustering`/`Cell` mirroring the `Native*` classes, reusing `SkipListMemtable`
structure, reads initially via `CloneToHeap`; then a `ValueAccessor<MemorySegment>` (the new
`NativeAccessor`/`NativeData` is a near-exact template) to make flush + reads zero-copy.

**Payoff.** Removes the largest remaining on-heap allocator in the engine; flush becomes
near-memcpy. **Risk.** JDK baseline — FFM is final in 22+ (the offheap plan picks **JDK 24**:
FFM mature, `Unsafe` memory access deprecated-for-removal per JEP 498). Out-of-tree plugin jar
works *today* on a 22+ runtime as the prototyping vehicle. See `ffm-memtable-offheap-plan.md`.

> **Scope guard:** the FFM memtable is a **separate future project**, JDK 24+. The cursor
> compaction branch targets JDK 21+ and must not depend on it. Listed here because it is the
> write-side half of the "records, not objects" architecture.

### 3. **Vectorized (SIMD) merge and compare** via the Vector API

**Idea.** The cursor merge's inner loop is `Arrays.compareUnsigned` over byte windows and vint
decode over byte buffers. Use the JDK Vector API (`jdk.incubator.vector`) for the
length-bounded unsigned compares and for bulk vint scanning, and SIMD-accelerate the
byte-comparable key compares in trie walks.

**Why it pays.** Merge is the hottest loop in compaction; key compare is the hottest in reads.
`compareUnsigned` already auto-vectorizes for long runs, but vint decode, flag scanning, and
short-window compares do not. The cursor's flat byte layout is *exactly* what SIMD wants.

**How.** Replace scalar compare/scan in `CursorCompactor`/`SSTableCursorReader`/`Walker` hot
spots with `ByteVector` ops behind a fallback. Measure with the existing JMH benches
(`CompactionBench`) and the allocation/throughput gates. Keep scalar paths for correctness
parity (differential suites already pin output bytes).

**Payoff.** Single-digit-to-low-double-digit % on compaction/scan CPU, no format change.
**Risk.** Vector API is still incubating (warmup/inlining cliffs); gains are data-shape
dependent. Pure CPU win, fully covered by existing byte-identity verification.

### 4. **Prefetch + branch-reduction** in the cursor inner loops

**Idea.** The cursor cell-flags decode is flagged `// HOTSPOT: surprisingly expensive`. Add
the specialized fast path for the dominant `USE_ROW_TIMESTAMP` live cell (collapses 5 mask
tests + 3 conditionals + a call to constants), precompute next-state when flags are read, and
software-prefetch the next partition's index page during the current partition's merge.

**Why it pays.** Per-cell work runs millions of times/second; the author already profiled
these sites. Branch mispredicts and data-dependent loads dominate once allocation is gone.

**How.** `quick-perf-wins.md` #8 sketch for the fast path; `Reference.reachabilityFence`-style
prefetch hints aren't available, but reordering reads to issue the next page fetch early
(direct-I/O / mmap touch) is. Verify with JFR + differential byte identity.

**Payoff.** Low-double-digit % on the cursor cell loop realistic. **Risk.** Low; tiny diffs,
byte-identity-verified.

---

## Tier 2 — format and encoding

### 5. **Dictionary / front-coding for clustering keys** in Data.db

**Idea.** Wide partitions repeat clustering prefixes heavily (time-series: same device id,
varying timestamp). The BTI *index* already front-codes (separators store only the
distinguishing suffix), but Data.db stores each clustering in full. Add optional per-block
prefix-compression of clusterings (store the shared-prefix length + suffix), or a per-sstable
clustering-component dictionary.

**Why it pays.** Smaller Data.db → less I/O, less page-cache pressure, faster scans. Time-
series is the canonical Cassandra workload and the worst case for clustering redundancy.

**How.** New format version (so it's gated and back-compatible — old readers keep working,
compaction rewrites forward). The cursor writer is the natural place to add it since it already
holds raw clustering bytes and computes block boundaries. Differential harness extends to the
new version.

**Payoff.** 10–40% Data.db shrink on clustering-heavy schemas (workload dependent). **Risk.**
Format change = careful versioning + broad testing; read path must decode it. Medium-high
effort, real payoff.

### 6. **Columnar / PAX layout for analytical sstables**

**Idea.** A row-grouped columnar layout (à la Parquet/PAX) as an alternate format for tables
flagged analytical: values of one column stored contiguously within a row group, enabling
column-pruning scans, run-length/dictionary/delta encoding per column, and late
materialization.

**Why it pays.** Big-scan / aggregation workloads read far less data (only referenced columns)
and compress far better (homogeneous column runs). Pairs naturally with the cursor read path
(scan a column without touching others) and with GPU/SIMD (homogeneous arrays).

**How.** A genuinely new `SSTableFormat` (not a tweak) — the most invasive idea here. Likely
scoped to a specific opt-in table property. Reuse byte-comparable keys + BTI partition index
for the row-group index.

**Payoff.** Order-of-magnitude on analytical scans; substantial compression. **Risk.** Largest
effort on this list; changes the read engine's mental model. Research-grade.

### 7. **Adaptive index granularity** per partition

**Idea.** `column_index_size` is one global default (16 KiB). Pick granularity per partition
from its size: index every row for tiny partitions (the format already supports 0 KiB → "in-
cache trie-indexed sstables outperform `ConcurrentSkipListMap` for reads"), coarser for huge
ones, balancing index size against seek cost.

**Why it pays.** Fixed granularity is wrong at both ends — wasteful index for small partitions,
poor seek resolution... actually too-fine for huge ones inflates `Rows.db`. Adaptive hits the
size/lookup tradeoff the format doc explicitly calls out.

**How.** The cursor BTI writer already cuts blocks by byte threshold; make the threshold a
function of (running) partition size. Reader is agnostic (trie is self-describing). Differential
suites pin output, so add scenarios at each regime.

**Payoff.** Faster reads on small partitions, smaller indexes on huge ones. **Risk.** Low-
medium; heuristic tuning + test coverage.

---

## Tier 3 — I/O substrate

### 8. **`io_uring` / async batched I/O** for compaction and flush

**Idea.** Replace the synchronous read/write of compaction inputs/outputs with batched async
submission (`io_uring` on Linux via a JNI/FFM shim, or `AsynchronousFileChannel` as a
portable fallback). Keep many reads in flight across the N input sstables; overlap output
writes with input reads.

**Why it pays.** Compaction is throughput-bound on I/O once CPU/GC is reduced (which the cursor
work did). N-way merge naturally has N independent read streams — ideal for deep queues. NVMe
wants high queue depth to saturate.

**How.** An I/O-layer abstraction under `io.util` (FileHandle/Rebufferer) with an `io_uring`
backend behind FFM (JDK 22+) or JNI. Start with compaction (bounded, server-side, no client
latency coupling). Measure MB/s and CPU-per-byte.

**Payoff.** Higher compaction throughput on fast storage; lower syscall overhead. **Risk.**
Platform-specific; FFM/JNI complexity. Compaction-only scope limits blast radius.

### 9. **GPU-accelerated compression / checksums** (offload the embarrassingly-parallel bytes)

**Idea.** Offload the byte-parallel, per-chunk operations of the write path to the GPU:
LZ4/Zstd block compression, CRC32/xxHash chunk checksums, and bloom-filter hashing. These run
over independent fixed-size chunks — the canonical GPU workload.

**Why it pays.** Compression + checksumming are a real slice of flush/compaction/streaming CPU.
They are *perfectly* parallel (chunk i is independent of chunk j), high arithmetic intensity,
and operate on contiguous byte buffers the cursor already produces. A single GPU can compress
many GB/s.

**How.** Batch K output chunks, ship to GPU (CUDA/ROCm via FFM downcalls, or a vendor lib like
nvCOMP for compression and a custom kernel for CRC), pull back compressed+checksummed chunks.
Pipeline so transfer overlaps compute. Gate behind a node capability flag; CPU path stays the
default and the verification fallback. Only worth it above a batch-size threshold (PCIe
transfer has fixed cost) — measure the crossover.

**Payoff.** Frees CPU cores for the merge itself; higher write throughput on GPU-equipped
nodes. **Risk.** PCIe transfer can eat the win for small batches; deployment heterogeneity
(most C* nodes have no GPU); output must be bit-identical to the CPU codec (nvCOMP LZ4 ≠ Java
LZ4 byte stream — likely need GPU kernels matching the exact codec, or accept format-version
differentiation). **This is opportunistic: only nodes that have a GPU benefit, and only at
scale.** Highest-novelty, real-but-narrow payoff.

### 10. **GPU-accelerated bulk merge / sort for major compactions**

**Idea.** For large major compactions, offload the *merge* of sorted byte-comparable key
streams (and the value reconciliation) to the GPU: a massively-parallel multiway merge over
the flat byte records the cursor already produces.

**Why it pays.** Major compactions move terabytes; merge is the core loop. GPUs do parallel
merge/sort extremely well, and byte-comparable keys mean the GPU compares **raw bytes** with no
type logic — exactly the cursor's model. The arena-memtable / cursor record layout is already
"flat bytes", which is what you must DMA to the device.

**How.** Far more speculative than #9 — value reconciliation (newest-wins, TTL/tombstone
rules, counter contexts) is branchy and not naturally SIMT. A realistic first cut: GPU does the
**merge/sort of (key, source, offset) tuples**, CPU does reconciliation on the ordered stream.
Even that requires the records to be GPU-resident (pairs well with an arena/`MemorySegment`
layout that can be mapped to device memory). Strictly a research spike; measure whether
transfer + kernel beats the now-very-fast CPU cursor merge.

**Payoff.** Potentially large on major compactions on GPU nodes. **Risk.** Very high — likely
transfer-bound, reconciliation doesn't map to SIMT, and the CPU cursor path is already fast.
Honest assessment: **#9 (compression/checksum offload) is the realistic GPU win; #10 is a
moonshot.** Spike it small before believing it.

---

## Tier 4 — allocator and runtime

### 11. **Thread-local arena scratch for the iterator path** (bridge until cursor-everywhere)

**Idea.** Until the cursor read path lands, give the iterator read/merge path a thread-local
ring of reusable scratch buffers for value deserialization (the `new byte[length]` per value),
recycled per request. A narrower, lower-risk version of the cursor lever for the existing path.

**Why it pays.** Captures a chunk of the CASSANDRA-20428 allocation without rewriting the read
path. **How.** Pool in `ByteArrayAccessor.read` callers / `SerializationHelper`. **Risk.**
Lifetime correctness (a recycled buffer must not outlive the request) — the cursor approach is
cleaner, this is a stopgap. Medium.

### 12. **Pluggable, generational-aware GC tuning + region pinning**

**Idea.** Now that compaction is garbage-free, the dominant remaining churn is read-path and
memtable. Pair the arena memtable (#2) with ZGC/Generational-ZGC defaults and explicitly keep
the large, long-lived structures (BTI non-leaf trie pages, bloom filters) in page cache / off-
heap so GC never scans them. Document a "low-pause storage profile".

**Why it pays.** The engine fights the GC less the more data is off-heap; tuning + off-heap
placement compound. **How.** Mostly configuration + the off-heap work above. **Risk.** Low;
config and docs, gated by measurement.

### 13. **Compressed/short pointers and smaller flyweights** in the cursor structures

**Idea.** Audit the cursor's reusable structures for footprint — `long[]` bloom scratch, the
descriptor buffers, the marker arrays — and shrink/share them. The format already uses
variable-length distance pointers in tries; apply the same parsimony to in-memory scratch so
the working set fits in L2.

**Why it pays.** Smaller working set = more of the merge stays in cache = higher IPC. **How.**
Profile cache misses (perf/JFR), size reusables to the 99th-percentile case, grow-on-demand.
**Risk.** Low; bounded by the differential gates.

---

## How to choose

If I had to sequence for **impact per unit risk**:

1. Tier 0 (this week — already written up).
2. #4 prefetch/branch-reduction and #3 SIMD merge (pure CPU, byte-identity-verified, no format
   or JDK change).
3. #1 cursor read path (largest GC win; long but de-risked).
4. #2 arena memtable (parallel track, JDK 24, separate project).
5. #8 io_uring for compaction (throughput on fast storage).
6. #5 clustering compression (real Data.db shrink; format-version work).
7. #9 GPU compression/checksum offload (narrow but real on GPU nodes).
8. #6 columnar and #10 GPU merge (research moonshots — spike before committing).

The throughline: **the cursor proved that flat bytes + reusable flyweights beat objects.**
Every tier above is that same bet applied somewhere new — the read path, the memtable, the
CPU's vector units, the GPU's lanes, and the on-disk encoding.
