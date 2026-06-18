# BTI SSTables and Compaction Behavior — Specification

> Written 2026-06-12 on branch `cursor-compaction-completion`. Companion to the upstream
> format doc `src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md` (the
> authoritative reference for the on-disk *index* trie encodings) and to
> `cursor-compaction-plan.md` (the journal of the cursor compaction work).
>
> Purpose: a single document that (a) describes the BTI sstable as a complete on-disk
> object — data component *and* index components — and (b) specifies how **both**
> compaction paths (the iterator path and the garbage-free cursor path) produce a BTI
> sstable, what they must agree on byte-for-byte, and where the format's invariants live.
> This is the reference the differential harness encodes as executable assertions.

---

## 1. What BTI is

BTI = "Big Trie-Indexed". It is one of two on-disk sstable formats
(`SSTableFormat`), the other being BIG. Introduced by
[CEP-25](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-25%3A+Trie-indexed+SSTable+format).

The defining property:

- **The data component (`Data.db`) is byte-for-byte the same format as BIG.** Partitions,
  rows, range-tombstone markers, and cells are serialized identically by the shared
  `UnfilteredSerializer` / `Cell.Serializer` / `SerializationHeader` machinery. A BTI and a
  BIG sstable compacted from the same inputs have *identical Data.db bytes*.
- **Only the primary index differs.** BIG uses a sampled, binary-searched partition index
  (`Index.db` of `(key, position)` entries + an in-memory `IndexSummary`, with promoted
  per-partition row index blocks embedded in `Index.db`). BTI replaces both indexes with
  **on-disk byte-comparable tries**:
  - `Partitions.db` — a trie mapping the shortest distinguishing prefix of each
    byte-ordered partition key to either a data-file position (small partition) or a
    row-index position (wide partition).
  - `Rows.db` — one trie per wide partition, mapping clustering-key separators to
    index-block positions within the partition.

BTI is the trunk default direction. Before this branch, cursor compaction only wrote BIG;
increment 3 added BTI output, so the garbage-free path now runs on the default format
(journal: "without BTI output, the whole cursor path silently stops running the day a
cluster switches its default format").

### 1.1 Components on disk

| Component | BIG | BTI | Contents |
|---|---|---|---|
| `Data.db` | ✓ | ✓ | Partitions → rows/markers → cells. **Identical format in both.** |
| `Statistics.db` | ✓ | ✓ | `SerializationHeader` (encoding bases + schema snapshot) + `StatsMetadata` (min/max ts, histograms, etc.) |
| `CompressionInfo.db` | ✓ | ✓ | Per-chunk offsets/CRC when compression is enabled |
| `Filter.db` | ✓ | ✓ | Bloom filter over partition keys (may be `AlwaysPresentFilter` if `bloom_filter_fp_chance = 1.0` — see finding #23) |
| `TOC.txt` / `Digest.crc32` | ✓ | ✓ | Component manifest / whole-file digest |
| `Index.db` | ✓ | — | BIG: `(key, position, [promoted row index])` entries |
| `Summary.db` | ✓ | — | BIG: in-memory-loaded sample of `Index.db` |
| `Partitions.db` | — | ✓ | BTI: partition-key trie |
| `Rows.db` | — | ✓ | BTI: per-partition row-index tries |

> Note: BTI **does not use the key cache** and its index entries are never persisted in an
> in-memory structure — `TrieIndexEntry.unsharedHeapSize()` / `serializeForCache()` throw
> deliberately (`TrieIndexEntry.java:73,87`). Non-leaf trie pages are expected to stay hot
> in the page cache instead.

---

## 2. The Data.db format (shared by BIG and BTI)

This is the part both compaction paths must produce identically, and the part the
differential harness pins byte-for-byte. Everything here is format-version dependent; the
description is the current (5.0 `oa`/`da`-family) layout.

### 2.1 Encoding bases — the `SerializationHeader`

Timestamps, TTLs, and local deletion times in Data.db are **delta-encoded** against
per-sstable minimums carried in the `EncodingStats` of the `SerializationHeader`
(`SerializationHeader.java`, `EncodingStats.java`). This is what makes small values encode
as one-byte vints and it is why the header is required to read a single cell:

- `minTimestamp` — base for every timestamp delta (epoch `TIMESTAMP_EPOCH`).
- `minLocalDeletionTime` — base for every local-deletion-time delta (`DELETION_TIME_EPOCH`).
- `minTTL` — base for every TTL delta (`TTL_EPOCH`).

The header also carries the schema snapshot needed to interpret the bytes: partition key
type, clustering types, and the **ordered static and regular column sets**. The column sets
matter because rows that omit columns encode a *subset* against this superset (§2.5).

> Compaction implication: the output header is the **union** of all input headers
> (`SerializationHeader.make`). Both paths must derive `hasStatic`, the column superset, and
> the encoding bases from the merged inputs — the cursor path reads `hasStatic` from the
> sstable headers, not from current schema metadata (finding #22: a table whose last static
> column was dropped still has static rows on disk; the *header* decides).

### 2.2 Partition layout

A partition in Data.db is:

```
[partition key: 2-byte short length + key bytes]
[partition-level DeletionTime: markedForDeleteAt vint (Δ) + localDeletionTime vint32 (Δ)]
[static row]                       ← present iff header.hasStatic()
[unfiltered]*                      ← rows and range-tombstone markers, in clustering order
[end-of-partition marker: single 0x01 byte]
```

The "partition header length" is the byte distance from the partition start to the end of
the static row (or to the end of the partition-level deletion time if there is no static
row). Both index implementations need it. The cursor seam passes it explicitly
(`CursorIndexWriter.endPartition(... int headerLength ...)`); note it must be tracked as a
`long` for the giant-partition case (the seam's `indexBlockStartOffset` is a `long` for the
same reason — an `int` wraps negative past 2 GiB and corrupts every block offset).

### 2.3 Unfiltered = row or marker

Every unfiltered begins with a **flags byte**:

| Bit | Mask | Name | Meaning |
|---|---|---|---|
| 0 | 0x01 | `END_OF_PARTITION` | No more unfiltereds in this partition |
| 1 | 0x02 | `IS_MARKER` | This unfiltered is a range-tombstone marker, not a row |
| 2 | 0x04 | `HAS_TIMESTAMP` | Row liveness carries a (primary-key) timestamp |
| 3 | 0x08 | `HAS_TTL` | Row liveness is expiring (TTL + local expiration time follow) |
| 4 | 0x10 | `HAS_DELETION` | Row has a row-level deletion |
| 5 | 0x20 | `HAS_ALL_COLUMNS` | Row contains every column in the header superset (no subset encoded) |
| 6 | 0x40 | `HAS_COMPLEX_DELETION` | Row carries at least one complex-column deletion |
| 7 | 0x80 | `EXTENSION_FLAG` | A second (extended) flags byte follows |

Extended flags byte (only if `EXTENSION_FLAG`):

| Bit | Mask | Name |
|---|---|---|
| 0 | 0x01 | `IS_STATIC` (the static row) |
| 1 | 0x02 | `HAS_SHADOWABLE_DELETION` (deprecated since 4.0) |

### 2.4 Row body

For a non-static row:

```
[flags] [ext flags?]
[clustering]                       ← Clustering.serializer over clustering types; absent for static
[row size: unsigned vint]          ← body size + size of the prev-size vint that follows
[previous unfiltered size: vint]   ← distance from previous unfiltered start (reverse-iteration aid)
--- row body (counted by row size) ---
[timestamp delta]                  ← iff HAS_TIMESTAMP
[ttl delta] [local-expiration delta] ← iff HAS_TTL
[row DeletionTime]                 ← iff HAS_DELETION
[column subset]                    ← iff NOT HAS_ALL_COLUMNS (§2.5)
[cell / complex-column data]*      ← one group per present column, in header column order
```

Static rows are identical from the row body onward but carry no clustering and hard-code
`previousUnfilteredSize = 0` (a static row does not advance the prev-size chain).

> The `previousUnfilteredSize` field tripped the cursor path early: it is written to disk
> but skipped by every current reader, so the original cursor writer wrote a literal `0` for
> it. The differential harness caught it as 2601/5100 divergent bytes (finding #2). It is now
> tracked and written exactly as the iterator does, including its own length feeding back
> into the row-size vint.

### 2.5 Column subset (sparse rows)

When a row omits header columns, `HAS_ALL_COLUMNS` is clear and a subset is encoded
(`Columns.serializeSubset` / `deserializeSubset`):

- **Superset < 64 columns:** a single unsigned-vint **bitmap**; a set bit marks a *missing*
  column. (Encoded value 0 is reserved/avoided — that case sets `HAS_ALL_COLUMNS` instead.)
- **Superset ≥ 64 columns ("large subset"):** a leading `(supersetCount − presentCount)`
  vint, then either the present-column indices or the missing-column index deltas, whichever
  set is smaller. The mode boundary is exactly 64.

> The ≥64 large-subset encoding and its **exact mode-selection boundary** are part of the
> cursor's supported surface (wide schemas, >64 columns, ~2,000-column tables in the scale
> suite). The cursor writer mirrors `Columns.Serializer` in both modes and at the boundary.

### 2.6 Cells

Each cell:

```
[cell flags: 1 byte]
[timestamp delta]                  ← iff NOT USE_ROW_TIMESTAMP
[local deletion time delta]        ← iff (IS_DELETED or IS_EXPIRING) and NOT USE_ROW_TTL
[ttl delta]                        ← iff IS_EXPIRING and NOT USE_ROW_TTL
[path]                             ← iff column is complex (collection element / UDT field)
[value]                            ← iff NOT HAS_EMPTY_VALUE
```

Cell flags byte:

| Bit | Mask | Name |
|---|---|---|
| 0 | 0x01 | `IS_DELETED` (tombstone cell, no value) |
| 1 | 0x02 | `IS_EXPIRING` (has TTL — mutually exclusive with `IS_DELETED`) |
| 2 | 0x04 | `HAS_EMPTY_VALUE` |
| 3 | 0x08 | `USE_ROW_TIMESTAMP` (cell timestamp == row liveness timestamp) |
| 4 | 0x10 | `USE_ROW_TTL` (cell TTL + local deletion == row's) |

> `IS_DELETED` and `IS_EXPIRING` are **mutually exclusive** (`if / else if` in
> `Cell.Serializer`); `IS_EXPIRING` strictly means `ttl != NO_TTL`, not "has an expiration
> time". The original cursor code violated both, emitting `IS_DELETED|IS_EXPIRING` plus a
> wasted `00` TTL byte on every tombstone cell (finding #3). The flag rebuild now mirrors
> `Cell.Serializer` exactly.

### 2.7 Complex columns (multi-cell collections / non-frozen UDTs)

A complex column's cell group is:

```
[complex DeletionTime]             ← iff the row's HAS_COMPLEX_DELETION is set AND this column has one
[cell count: unsigned vint32]
[cell]*                            ← path-ordered (map key / set element / list TimeUUID / UDT field index)
```

Frozen collections are a **single** cell (`column.isComplex()` is false) and were always
supported; the complex-column work is multi-cell columns only.

> Writing complex columns is the subtle part of the cursor path: a column's cell count is
> only known after its merge, and the row-level `HAS_COMPLEX_DELETION` flag is only known
> after *every* column merges. The cursor streams cells into a reusable row buffer as they
> win, records each complex column's start offset + count + deletion in a small reusable
> marker array, and `writeRowEnd` splices `[deletion][count]` in at each marker and decides
> the row flag before any byte reaches the data file. Rows with no complex columns keep the
> original direct path (enforced by byte identity).

### 2.8 Range-tombstone markers

When `IS_MARKER` is set:

```
[flags = 0x02]
[clustering bound or boundary]     ← ClusteringBoundOrBoundary.serializer
[marker size: unsigned vint]       ← SSTable only
[previous unfiltered size: vint]   ← SSTable only
[DeletionTime]  or  [end DeletionTime][start DeletionTime]   ← bound vs boundary
```

A plain bound (open or close) carries one `DeletionTime`; a **boundary** (close-then-open at
one clustering) carries two, written end-deletion then start-deletion.

> The marker body size is predicted-then-written in two places; a long-domain vs `(int)`-cast
> mismatch on the local-deletion-time delta corrupts the size vint for far-future deletions.
> This bit the cursor's complex-deletion marker sizing (finding #25, fixed) and the upstream
> iterator's RT-marker `serializedMarkerBodySize` has the *same* latent bug (upstream JIRA #2).

---

## 3. The BTI index components

Detailed trie-node encodings (node types `PAYLOAD_ONLY` / `SINGLE` / `SPARSE` / `DENSE`,
pointer-size specializations, page packing) are in the upstream `BtiFormat.md` and not
repeated here. This section covers what compaction must produce and the payloads it writes.

### 3.1 Byte-comparable keys

Both tries are keyed by the **byte-comparable** representation of keys
(`ByteComparable` / `ByteSource`, CASSANDRA-6936): a serialization whose unsigned
lexicographic byte order equals the typed order. Partition keys use the decorated
(token-prefixed) form; clustering keys use the clustering-comparator form. Tries store only
the **shortest distinguishing prefix**, so the structure is ~2n nodes and prefixes are
shared.

> Compaction implication: the cursor holds **raw serialized clustering bytes**, but the trie
> builders want `ClusteringPrefix` / `ByteComparable`. `BtiCursorIndexWriter` bridges this
> with a reusable lazy view (`ClusteringDescriptorPrefixView`) that parses component
> boundaries from the descriptor's buffer on demand. Because the trie APIs **retain
> references across calls** (`RowIndexWriter` keeps `prevMax`/`prevSep`;
> `PartitionIndexBuilder` keeps `lastKey`/`lastPayload`), block-boundary clusterings and
> partition keys must be **snapshotted** (`snapshotOf` / `ByteBufferUtil.clone`) before they
> escape into the builder — the reusable view must never leak. This is the one accepted
> bounded allocation of the cursor BTI path: a per-~16KB-block snapshot, plus a partition-key
> clone per partition.

### 3.2 Partition index — `Partitions.db`

Built bottom-up by `PartitionIndexBuilder` (iterator path) or fed via
`BtiTableWriter.IndexWriter.append` (cursor path). Written from the bottom up; the "header"
is the last three longs in the file:

```
[trie node pages...]
[smallest key, short length][largest key, short length]
[smallest-key file position: long]
[key count: long]
[root node position: long]
```

Each leaf payload is a `TrieIndexEntry`:

- **Direct-to-data** (small partition, no row index): the payload encodes `~position` (bit
  complement so 0-with-index and 0-without-index differ). The trie node's payload bits carry
  the pointer length; ≥8 means a key-hash byte precedes it (low bits of the key hash, an
  extra mismatch filter beyond the bloom filter).
- **Indexed** (wide partition): the payload points into `Rows.db` at the partition's row
  index. Serialized form (`TrieIndexEntry.serialize`):
  ```
  [data file position: unsigned vint]
  [row-index trie root − basePosition: vint]
  [row index block count: unsigned vint32]
  [partition DeletionTime]
  ```

`PartitionIndexBuilder` computes the shortest unique prefix lazily by delaying each key
until it has seen the next (`diffPoint` between consecutive keys), and supports **early open**
by snapshotting a partial trie tail once data/row-index/partition-index files have all synced
to the needed positions (`buildPartial` / `markXxxSynced` / `PartitionIndexEarly`).

### 3.3 Row index — `Rows.db`

One trie per wide partition, built by `RowIndexWriter`. Layout per partition:

```
[trie node pages...]
[partition key, short length]
[partition data-file position: unsigned vint]
[root node position: vint Δ from data position]
[number of rows in partition: unsigned vint]
[partition DeletionTime: 12 bytes]
```

Each trie leaf payload (`RowIndexReader.IndexInfo`):

- offset within the partition where the index block starts, and
- (if `pb ≥ 8`) the `DeletionTime` **active at the start of that block** — required so a
  merge entering mid-partition knows the open range deletion.

The trie keys are **separators**: for consecutive blocks, `RowIndexWriter.add` computes the
shortest `ByteComparable` such that `prevMax < separator ≤ nextMin`
(`ByteComparable.separatorGt`). `complete(endPos)` adds a trailing nudged separator after the
last block so greater lookups reject quickly.

> Row index is only built when a partition spans **more than one** index block. Single-block
> partitions store `trieRoot = -1` and the partition index entry points direct-to-data — "an
> index of one block adds no information". Both paths must agree on this exactly (findings
> #5, #6 were boundary disagreements: the tail block's inclusion of the end-of-partition
> marker byte, and the promote-at->1-blocks-including-tail decision).

---

## 4. Index granularity (`column_index_size`)

The row index indexes **blocks** of rows, not every row. A new block boundary is taken when
the bytes written since the last boundary reach `column_index_size` (default
`DEFAULT_GRANULARITY = 16 KiB` for BTI; note the BIG default historically was 64 KiB).

```java
if (currentOffsetInPartition() - indexBlockStartOffset >= rowIndexBlockSize)
    addIndexBlock();
```

This exact predicate is mirrored in both the iterator partition writer
(`BtiFormatPartitionWriter.addUnfiltered`) and the cursor seam
(`BtiCursorIndexWriter.rowWritten`). Static rows reset the block clock
(`notePosition`/`staticRowWritten`) without participating in a block.

---

## 5. How compaction produces a BTI sstable

Compaction merges N input sstables (and the live overlap set, for purge decisions) into one
or more outputs of the current format. Two pipelines exist, selected at
`AbstractCompactionPipeline.create` by `DatabaseDescriptor.isCursorCompactionEnabled()`:

1. **Iterator path** — `CompactionIterator` → `UnfilteredRowIterator`s merged through
   `UnfilteredRowIterators.merge` → `BtiTableWriter` → `BtiFormatPartitionWriter`
   (extends `SortedTablePartitionWriter`) → `RowIndexWriter` + `PartitionIndexBuilder`.
   Materializes `Row` / `Cell` / `ClusteringPrefix` objects at every step.

2. **Cursor path** — `CursorCompactor` → `SSTableCursorReader`s merged at the byte level →
   `SSTableCursorWriter` → `CursorIndexWriter` seam (`BigCursorIndexWriter` or
   `BtiCursorIndexWriter`). **Garbage-free**: reusable flyweights, retained-and-cleared
   `DataOutputBuffer`s, raw `byte[]` windows compared with `Arrays.compareUnsigned`, no
   per-row/cell/partition allocation in steady state.

Both run inside the **same production `CompactionTask.execute()`** — only the pipeline
selection flips. Their Data.db, Partitions.db, and Rows.db outputs must be **byte-identical**.

### 5.1 The cursor write seam

The cursor writer owns all Data.db serialization and the open range-tombstone marker; the
format-specific index production is abstracted behind `CursorIndexWriter`, an **event-shaped**
seam (because `UnfilteredDescriptor`s are transient/reused):

```
startPartition(partitionStart, positionAfterHeader)
  reset(); notePosition(positionAfterHeader)
staticRowWritten(position)            // resets the block clock, no block participation
rowWritten(descriptor, rowStart, rowEnd, openMarker)   // per row OR marker
  capture block-boundary clustering at row time (NOT at block boundary)
  if offset-in-partition − indexBlockStartOffset ≥ granularity → cut a block
endPartition(key, keyBytes, keyLength, headerLength, partitionDeletion, partitionEnd)
  index the final partial block; complete the row trie iff >1 block; append the partition entry
close()                               // release per-instance trie state
```

- **`BigCursorIndexWriter`** — the original BIG index logic, moved verbatim behind the seam
  (a pure refactor, gated by a byte-identity run): promoted index blocks, `Index.db` entries,
  bloom filter, summary. (Note the `AlwaysPresentFilter` cast guard, finding #23.)
- **`BtiCursorIndexWriter`** — feeds `RowIndexWriter` (separators between block-boundary
  clusterings) and the partition index (`TrieIndexEntry` via `BtiTableWriter.IndexWriter`),
  matching `BtiFormatPartitionWriter` block-for-block, including the single-block-partition
  skip (entry position −1) and the partition-length-before-end-marker payload (the `−1` in
  `complete(partitionEnd − 1 − partitionStart)`).

### 5.2 The merge semantics both paths must reproduce

The output bytes are determined by the merge, which both paths implement identically:

- **Cell reconciliation** — newest timestamp wins; on a timestamp tie the iterator rule is
  *left/current wins unless the challenger's raw value bytes are strictly greater*
  (`Cells.resolveRegular` → unsigned lexicographic compare on **raw** value bytes, not the
  vint-prefixed wire bytes). TTL tie-break: both expiring, same ts/expiration → lower TTL
  wins. (Cursor findings #4, #21 were inverted polarity and wire-byte-vs-raw-byte bugs here.)
- **Complex columns** — a path-ordered N-way merge nested in the column merge; equal paths
  reconcile by the same cell rules; the complex deletion is newest-wins, shadowed by any
  active range/partition deletion, and shadows cells at or below its timestamp.
- **Counters** — `CounterContext` shard merge (`CursorCounterContexts` mirrors
  `CounterContext`), CASSANDRA-7346 tombstone supremacy, the `Flag.LOCAL` marked-shard clear
  on every value, and the tombstone value tie-break (finding #26).
- **Purge / GC** — a cell/tombstone is purged when older than `gcBefore` *and* not shadowed
  by overlapping non-participating sstables. TTL-expiry → tombstone conversion uses `nowInSec`,
  **overridden to `gcBefore` for Accord-enabled tables** (finding #24). Dropped columns are
  filtered per-source pre-merge against per-column drop horizons (finding #22).
- **Strict liveness** (materialized views) — `enforceStrictLiveness` is honored identically.

### 5.3 What must match, and how it is verified

`DifferentialCompactionTester` compacts identical inputs through both production pipelines
and asserts **byte-for-byte identical** output of every component — Data.db, Partitions.db,
Rows.db, Statistics.db, Filter.db, CompressionInfo.db — with **no allowlist** (any divergence
in any component fails). It then runs a second generation (re-compacts the cursor's own
output through both paths) to catch write-side corruption only the next merge can observe,
runs extended `IVerifier` on every output, and runs allocation gates that fail if cursor
allocation scales with rows/cells/markers/bytes. Both the BIG and BTI suites run in the
ladder; the randomized soak generates both formats.

This is why the format invariants above are stated as equalities: each is, somewhere, a
failing differential assertion if violated.

---

## 6. Reading a BTI sstable (for completeness)

- **Point lookup**: bloom filter → walk `Partitions.db` trie by the byte-comparable
  decorated key → leaf payload gives data position (compare the full key stored there, since
  only a prefix is indexed) or a `Rows.db` position → walk the row trie to the candidate
  block → linear scan the block in `Data.db`. On a cache hit: a few `DENSE` transitions, one
  or two `SPARSE`/`SINGLE` binary searches, one buffer compare — no object allocation.
- **Range / slice**: trie floor/ceiling + `ValueIterator` / `ReverseValueIterator`. Reverse
  reads stack a block's row positions and pop them, then request the previous block.
- **Code**: `o.a.c.io.tries` (generic trie read/write — `Walker`, `ValueIterator`,
  `IncrementalDeepTrieWriterPageAware`) and `o.a.c.io.sstable.format.bti`
  (`PartitionIndex`, `RowIndexReader`, `BtiTableReader`).

---

## 7. Pointers

- Upstream format reference (trie node encodings, page packing):
  `src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md`
- Byte-comparable types: `src/java/org/apache/cassandra/utils/bytecomparable/ByteComparable.md`
- Data.db serialization: `db/rows/UnfilteredSerializer.java`, `db/rows/Cell.java`,
  `db/SerializationHeader.java`, `db/Columns.java`,
  `io/sstable/format/SortedTablePartitionWriter.java`
- Iterator BTI write: `format/bti/BtiTableWriter.java`, `BtiFormatPartitionWriter.java`,
  `RowIndexWriter.java`, `PartitionIndexBuilder.java`, `TrieIndexEntry.java`
- Cursor write seam: `io/sstable/CursorIndexWriter.java`, `BigCursorIndexWriter.java`,
  `format/bti/BtiCursorIndexWriter.java`, `db/compaction/SSTableCursorWriter.java`,
  `CursorCompactor.java`
- Differential verification: `test/unit/.../db/compaction/differential/`
