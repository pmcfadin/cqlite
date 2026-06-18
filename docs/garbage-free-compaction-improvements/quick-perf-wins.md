# Quick performance wins: ranked top 10

> Code-reading-driven survey of trunk (branch `cursor-compaction-completion`), 2026-06-10.
> Scope: high-impact (per-row / per-cell / per-partition / per-request hot paths), low-effort
> (<= ~100 changed lines, no format/protocol/config changes, value-identical refactors preferred).
>
> Excluded (already fixed on this branch): cursor-path `ClusteringPrefix.Kind.ALL_KINDS` sites,
> cursor `prevUnfilteredSize` / cell-flags / tie-break / stats fixes, the cursor sparse-row
> subset-mask fix (`f92a993e34`). Two known-but-unfixed items documented elsewhere on this branch
> are ranked below and marked **[already documented on this branch]**.
>
> Verification tooling referenced throughout:
> - **Allocation gate**: `test/unit/org/apache/cassandra/db/compaction/differential/CursorCompactionAllocationGateTest.java`
>   (compare allocation growth between an N-row and 10N-row run; assert sub-linear growth). The same
>   technique works for iterator-path read/write loops.
> - **Differential suites**: `test/unit/org/apache/cassandra/db/compaction/differential/*` assert
>   byte-identical sstable output, which proves value-identity for anything on the write path.
> - **JMH**: `test/microbench/org/apache/cassandra/test/microbench/` (e.g. `CompactionBench`,
>   `DeletionTimeDeSerBench` show the house style for serializer benches).

---

## #1 — Memoize `Columns.Serializer.deserializeSubset` for repeated sparse-row shapes (read path)

**What it does today** — `src/java/org/apache/cassandra/db/Columns.java:574-605`:

```java
public Columns deserializeSubset(Columns superset, DataInputPlus in) throws IOException
{
    long encoded = in.readUnsignedVInt();
    if (encoded == 0L)
        return superset;
    else if (superset.size() >= 64)
        return deserializeLargeSubset(in, superset, (int) encoded);
    else
    {
        try (BTree.FastBuilder<ColumnMetadata> builder = BTree.fastBuilder())
        {
            ...
            return new Columns(builder.build(), firstComplexIdx);   // fresh Columns + BTree per row
        }
    }
}
```

Every row that does not contain every header column ("sparse row") materializes a brand-new
`Columns` (BTree leaf array + `Columns` object, ~100-150 B) even though real workloads have a tiny
number of distinct sparse shapes (usually one: "the column the update didn't set").

**Why it's hot** — per sparse row during deserialization on the iterator read path:
`SSTableIdentityIterator` → `UnfilteredDeserializer` → `UnfilteredSerializer.deserialize` →
`deserializeRowBody` (`src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java:622`:
`Columns columns = hasAllColumns ? headerColumns : Columns.serializer.deserializeSubset(headerColumns, in)`).
Also hit per sparse row when deserializing read responses and mutations
(`UnfilteredRowIteratorSerializer.deserialize` → same row body path). Sparse rows are the everyday
shape for partial updates, cell deletes, and null columns — this branch measured ~150 B/row of
garbage for the identical pattern on the cursor side (commit `f92a993e34`) and fixed only the
cursor reader; the iterator read path (i.e., **all normal reads**) still pays it.

**The fix** — mirror the cursor fix's economics with a one-slot memo on the superset `Columns`
(supersets are long-lived: one per sstable `SerializationHeader`). For the `< 64` branch the wire
encoding is a single `long` bitmap, so the cache key is trivially the decoded long:

```java
// in Columns (immutable): single-slot, racy-but-safe memo (String.hashCode idiom)
private transient long cachedSubsetEncoded = -1;   // -1 == invalid (encoded 0 never reaches here)
private transient Columns cachedSubset;

// in deserializeSubset's small branch:
Columns cached = superset.lookupSubset(encoded);
if (cached != null) return cached;
... build as today ...
superset.memoizeSubset(encoded, result);
return result;
```

Worst case (two interleaved shapes ping-ponging) degrades to today's behavior; results are
value-identical because `Columns` is immutable and the bitmap fully determines the subset. If
single-slot feels fragile, a 4-entry direct-mapped array (`encoded & 3`) is still ~30 lines.

**Expected impact** — eliminates ~100-150 B/row allocation for every sparse row read from disk or
received over messaging; read-heavy workloads with partial updates allocate this millions of times
per second per node (it is the read-side twin of the cursor fix that removed ~830 KB per 12k-row
compaction). Order of magnitude: tens of MB/s less garbage on busy read paths.

**Effort** — ~30-40 lines, 1 file (`Columns.java`). Low risk: pure memoization of an immutable
value. **Verification** — JMH bench deserializing a partition of sparse rows (pattern of
`DeletionTimeDeSerBench`); allocation-gate-style test comparing N vs 10N sparse-row reads;
existing unit tests for `Columns.Serializer` cover correctness.

---

## #2 — Stop cloning `Kind.values()` in iterator-path clustering deserializers **[already documented on this branch]**

**What it does today** — `Kind.values()` clones a 15-element enum array (~80 B + header) on every
call:

- `src/java/org/apache/cassandra/db/ClusteringPrefix.java:483` (`skip`), `:494` (`deserialize`),
  `:664` (`Deserializer.prepare`, marker branch)
- `src/java/org/apache/cassandra/db/ClusteringBoundOrBoundary.java:119` (`deserialize`)

```java
Kind kind = Kind.values()[in.readByte()];     // fresh array clone per call
```

The shared copy already exists — `ClusteringPrefix.Kind.ALL_KINDS` (`ClusteringPrefix.java:89`),
added by this branch for the cursor sites — these four iterator-path sites just don't use it yet.

**Why it's hot** —
- per range-tombstone marker read: `UnfilteredSerializer.deserializeOne`/`deserializeTombstonesOnly`
  (`UnfilteredSerializer.java:515`) → `ClusteringBoundOrBoundary.serializer.deserialize` → `:119`;
- per skipped/compared unfiltered in sstable index-block scans: `ClusteringPrefix.Deserializer.prepare`
  (`:664`) fires for every non-row unfiltered;
- per index entry in big-format wide-partition reads: `IndexInfo.Serializer.skip/deserialize`
  (`src/java/org/apache/cassandra/io/sstable/IndexInfo.java:133-134,143-144`) calls
  `ClusteringPrefix.serializer.skip` **twice** and `deserialize` twice → `:483/:494`. Index blocks
  are binary-searched on every wide-partition slice read.

**The fix** — replace the four `Kind.values()[...]` with `Kind.ALL_KINDS[...]` (and keep the
existing bounds behavior — `ArrayIndexOutOfBoundsException` on corrupt input is unchanged).

**Expected impact** — removes an 80-100 B allocation per marker / per index-info entry / per
skipped unfiltered; tombstone-heavy reads and wide-partition (big format) reads notice most.

**Effort** — 4 lines, 2 files. Zero semantic risk. **Verification** — existing serializer round-trip
unit tests; JMH on `IndexInfo` deserialization or a tombstone-heavy read bench.

---

## #3 — Garbage-free column-subset *encoding* for sparse rows (write/messaging path)

**What it does today** — writing a sparse row runs
`Columns.serializer.serializeSubset(row.columns(), headerColumns, out)`
(`src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java:241-242`), where:

1. `row.columns()` allocates a Guava view per row — `src/java/org/apache/cassandra/db/rows/BTreeRow.java:257-260`:
   ```java
   public Collection<ColumnMetadata> columns()
   { return Collections2.transform(columnData(), ColumnData::column); }
   ```
   (`columnData()` itself allocates an `AbstractCollection` wrapper, `BTreeRow.java:313`, and
   iterating allocates the transform iterator plus the underlying `BTreeSearchIterator`).
2. `encodeBitmap` (`src/java/org/apache/cassandra/db/Columns.java:609-631`) allocates another
   `BTreeSearchIterator` via `superset.iterator()` (`Columns.java:612`, backed by
   `BTree.slice`, `Columns.java:366-369`).
3. On the messaging path the row body is sized *and* serialized
   (`UnfilteredSerializer.java:367-368` `serializedSubsetSize` then `:241-242` `serializeSubset`),
   so the whole bundle — wrappers, search iterator, bitmap scan — runs **twice per sparse row**.

**Why it's hot** — per sparse row on: memtable flush and iterator compaction
(`SortedTableWriter.addRow` → `SortedTablePartitionWriter.addUnfiltered` →
`UnfilteredSerializer.serialize` → `serializeRowBody`), read-response serialization
(`UnfilteredRowIteratorSerializer.serializeWithoutKey:168`), and mutation serialization. Same
"sparse rows are the everyday shape" argument as #1.

**The fix** — add a Row-aware overload `Columns.Serializer.encodeBitmap(Row, Columns superset)`
computing the missing-columns bitmap with zero wrappers, using the same pattern the row body
already uses (`row.accumulate` + the `SerializationHelper`-cached superset search iterator,
`src/java/org/apache/cassandra/db/rows/SerializationHelper.java:55-74`):

```java
// sketch: bitmap of missing superset columns, walking row's ColumnData directly
long bitmap = ...accumulate over row.columnData(), advancing helper.iterator(isStatic),
              setting bits for skipped superset positions...;   // mirrors encodeBitmap's logic
out.writeUnsignedVInt(bitmap);
```

`serializedSubsetSize` for the `< 64` case becomes `sizeofUnsignedVInt(encodeBitmap(row, ...))` with
the same overload. The `>= 64` structural branch keeps the current code (rare shape — same
restriction the cursor fix accepted).

**Expected impact** — removes 3-5 small allocations (plus a redundant bitmap recomputation on the
messaging path) per sparse row written; flush/compaction/read-response serialization of
partial-update workloads notices. Same order as #1 (write-side twin).

**Effort** — ~50-70 lines across `Columns.java` + `UnfilteredSerializer.java` (the call sites pass
`row` instead of `row.columns()`). Risk: low — output bytes must be identical, which the
differential compaction suites verify directly. **Verification** — differential suites
(byte-identical sstables), allocation gate on iterator-path flush, JMH on `UnfilteredSerializer`.

---

## #4 — Lazily allocate the 3 MiB tombstone-histogram spool in `MetadataCollector` **[already documented on this branch]**

**What it does today** — `src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java:122`:

```java
protected StreamingTombstoneHistogramBuilder estimatedTombstoneDropTime =
    new StreamingTombstoneHistogramBuilder(TOMBSTONE_HISTOGRAM_BIN_SIZE,
                                           TOMBSTONE_HISTOGRAM_SPOOL_SIZE,   // 2 MiB long[] + 1 MiB int[]
                                           TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS);
```

3 MiB allocated per sstable writer (every flush, every compaction output, streaming, scrub) before
a single row is written; tombstone/TTL-free tables never touch it.

**Why it's hot** — one per `MetadataCollector`; JFR on this branch showed it as 62-69% of
within-compaction sampled allocation (~90 MiB over 30 small compactions). Full analysis and fix
plan: `garbage-free-compaction-improvements/tombstone-histogram-spool-proposal.md`.

**The fix** — allocate the spool lazily on first `update()` (first tombstone/TTL cell); ~20 lines in
`StreamingTombstoneHistogramBuilder`. Statistics.db output is byte-identical for any input.

**Expected impact** — eliminates 3 MiB/writer for tombstone-free tables; biggest single allocation
source inside `CompactionTask.execute` on this branch's profiles.

**Effort** — ~20-30 lines, 1 file. **Verification** — proposal doc includes the plan; differential
suites + allocation gate already exercise it.

---

## #5 — Replace `stream().allMatch` with a loop in `UnfilteredRowIterators.mergeStaticRows`

**What it does today** — `src/java/org/apache/cassandra/db/rows/UnfilteredRowIterators.java:492`:

```java
if (iterators.stream().allMatch(iter -> iter.staticRow().isEmpty()))
    return Rows.EMPTY_STATIC_ROW;
```

Allocates a stream pipeline (+ spliterator + predicate evaluation machinery) per merged partition.

**Why it's hot** — once per partition merge for any table **with static columns** (the
`columns.isEmpty()` short-circuit above it handles static-less tables): every multi-source read
(`SinglePartitionReadCommand.queryMemtableAndDiskInternal` → `UnfilteredRowIterators.merge` →
`UnfilteredRowMergeIterator.create` → `mergeStaticRows`), every coordinator data resolution, repair,
and iterator-path compaction. Static columns are common in time-series/entity models.

**The fix** — value-identical loop:

```java
boolean allEmpty = true;
for (int i = 0; i < iterators.size(); i++)
    if (!iterators.get(i).staticRow().isEmpty()) { allEmpty = false; break; }
if (allEmpty)
    return Rows.EMPTY_STATIC_ROW;
```

(Same shape as trunk's recent CASSANDRA-21199 `StorageProxy` fix, `9017e18fa1`.)

**Expected impact** — ~100-200 B per partition merge on static-column tables; point-read-heavy
workloads on such tables see it per read.

**Effort** — 5 lines, 1 file, zero risk. **Verification** — existing merge unit tests; trivially
reviewable.

---

## #6 — Use `apply`/`accumulate` instead of iterators in `writeComplexColumn` / `sizeOfComplexColumn`

**What it does today** — `src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java:281-290`
and `:388-399`:

```java
for (Cell<?> cell : data)                                  // ComplexColumnData.iterator() per column
    Cell.serializer.serialize(cell, column, out, rowLiveness, header);
...
for (Cell<?> cell : data)                                  // again on the size pass
    size += Cell.serializer.serializedSize(cell, column, rowLiveness, header);
```

Each `for (Cell<?> cell : data)` allocates a BTree iterator per complex column per row. The simple-
cell path in the same file was already converted to the allocation-free
`row.apply(...)`/`row.accumulate(...)` pattern (`UnfilteredSerializer.java:252, :374`); the complex
branch was left behind. `ComplexColumnData` already exposes the needed primitives
(`src/java/org/apache/cassandra/db/rows/ComplexColumnData.java:127-134`:
`accumulate(LongAccumulator)`, `accumulate(BiLongAccumulator, arg)` and `apply`).

**Why it's hot** — per complex (collection/UDT, multi-cell) column per row on: flush, iterator
compaction, mutation serialization, and read-response serialization — and twice per row on the
messaging path (size + serialize). Collection-heavy schemas pay it on every row.

**The fix** — `data.accumulate((cell, v) -> v + Cell.serializer.serializedSize(...), size)` for the
size pass, and an `apply`-based equivalent (reusing the `SerializationHelper` carrier fields, same
trick as `serializeColumnData`) for the write pass.

**Expected impact** — one iterator allocation (~48 B) per complex column per row removed (×2 on
messaging); collection-heavy write/flush/compaction workloads notice.

**Effort** — ~25-35 lines, 1 file. Risk low; byte output unchanged. **Verification** — differential
suites with collection columns (`PartialSetDifferentialCompactionTest` already covers complex
columns); allocation gate.

---

## #7 — Sweep: cached `VALUES` arrays for per-message deserializer `Enum.values()[...]` sites

**What it does today** — each of these clones the enum array on every inbound message / filter
expression (verified by reading callers; cold sites excluded to the appendix):

| Site | Frequency |
|---|---|
| `db/ReadCommand.java:1453`, `:1491` | once per inbound read message (replica side), per `Kind.values()[in.readByte()]` |
| `db/filter/RowFilter.java:688` | per filter **expression** per read command |
| `db/filter/DataLimits.java:1194` | per read command |
| `db/filter/AbstractClusteringIndexFilter.java:83` | per read command |
| `db/filter/ColumnSubselection.java:232` | per column subselection per command |
| `cql3/selection/Selector.java:263` | per selector in aggregation paging states |
| `service/accord/txn/TxnCondition.java:712`, `TxnDataValue.java:90`, `TxnReferenceValue.java:225`, `service/accord/journal/CommandChanges.java:266` | per Accord txn message component |

**Why it's hot** — read commands are deserialized on every replica for every read; a SELECT with a
3-expression `RowFilter` allocates 4+ enum-array clones before any data is touched. Accord txn
deserialization multiplies this per transaction.

**The fix** — the standard idiom, per enum:

```java
private static final Kind[] ALL = Kind.values();   // adjacent to the enum
...
Kind kind = ALL[in.readByte()];
```

**Expected impact** — removes ~80-150 B × (sites hit) per request message; small per item but the
sweep covers every read/write/txn message on every node. Best impact-to-risk ratio after #2.

**Effort** — ~2 lines per site, ~25 lines total across 8-10 files; zero semantic risk (bounds
behavior unchanged). **Verification** — existing message round-trip tests
(`ReadCommand`/`RowFilter` serializer tests) cover all sites.

---

## #8 — Specialize the common cell-flag pattern in `SSTableCursorReader.readCellHeader` (author-flagged TODO)

**What it does today** — `src/java/org/apache/cassandra/io/sstable/SSTableCursorReader.java:156-185`,
under the existing `// HOTSPOT: suprisingly expensive` (`:156`) and
`// TODO: specialize common case where flags == HAS_VALUE | USE_ROW_TS?` (`:171`) comments:

```java
cellFlags = dataReader.readUnsignedByte();
boolean hasValue        = Cell.Serializer.hasValue(cellFlags);
boolean isDeleted       = Cell.Serializer.isDeleted(cellFlags);
boolean isExpiring      = Cell.Serializer.isExpiring(cellFlags);
boolean useRowTimestamp = Cell.Serializer.useRowTimestamp(cellFlags);
boolean useRowTTL       = Cell.Serializer.useRowTTL(cellFlags);
long timestamp = useRowTimestamp ? rowLiveness.timestamp() : serializationHeader.readTimestamp(dataReader);
long localDeletionTime = useRowTTL ? ... : (isDeleted || isExpiring ? ... : Cell.NO_DELETION_TIME);
int ttl = useRowTTL ? ... : (isExpiring ? ... : Cell.NO_TTL);
localDeletionTime = Cell.decodeLocalDeletionTime(localDeletionTime, ttl, deserializationHelper);
```

For the overwhelmingly common live cell written with the row's timestamp and no TTL,
`cellFlags == USE_ROW_TIMESTAMP_MASK (0x08)`, all five mask tests, three conditionals, and
`decodeLocalDeletionTime` resolve to constants.

**Why it's hot** — per cell during cursor compaction:
`compactPartition` → `advance` → `CellCursor.readCellHeader` — the innermost loop of the
garbage-free compaction path; the cell-flag decode runs millions of times per second (the adjacent
HOTSPOT comment was written from profiling on this very branch).

**The fix** — a guarded fast path:

```java
cellFlags = dataReader.readUnsignedByte();
if (cellFlags == Cell.Serializer.USE_ROW_TIMESTAMP_MASK)   // live cell, row ts, no ttl, has value
{
    cellLiveness.reset(rowLiveness.timestamp(), Cell.NO_TTL, Cell.NO_DELETION_TIME);
    cellPath = cellColumn.isComplex() ? cellColumn.cellPathSerializer().deserialize(dataReader) : null;
    return true;
}
... existing general path ...
```

(CPU win, not allocation; it is the one item here whose payoff is branch/inlining rather than GC.)

**Expected impact** — shaves a handful of branches + a call per cell on the cursor compaction inner
loop; single-digit % on cursor-compaction cell decode is realistic given the author already flagged
the site from profiles.

**Effort** — ~12-15 lines, 1 file. Risk: low — fast path must be provably equivalent for that flag
byte (it is: each general-path expression collapses by substitution). **Verification** — the
differential suites assert byte-identical output vs the iterator path for all scenarios; JFR/JMH
before/after on `CompactionBench`-style cursor runs.

---

## #9 — `guardCollectionSize`: skip the purge when the raw size can't trigger, hoist `nowInSeconds`

**What it does today** — `src/java/org/apache/cassandra/io/sstable/format/SortedTableWriter.java:428-467`,
called per row written (`addRow:232`, `addStaticRow:213`). When collection guardrails are enabled:

```java
for (ColumnMetadata column : row.columns())                       // Guava wrapper per row (see #3)
{
    ...
    ComplexColumnData liveCells = cells.purge(DeletionPurger.PURGE_ALL, FBUtilities.nowInSeconds());
    ...                                                           // purge allocates a new
    int cellsSize = liveCells.dataSize();                         // ComplexColumnData per collection
    int cellsCount = liveCells.cellsCount();                      // column per row, every row
    if (!Guardrails.collectionSize.triggersOn(cellsSize, null) && ...)
        continue;
```

Every collection column of every row written gets a full purge + dataSize walk even when the
collection is nowhere near the threshold, plus a `FBUtilities.nowInSeconds()` call per column.

**Why it's hot** — per collection column per row on flush and compaction whenever
`collection_size_*_threshold` / `items_per_collection_*_threshold` guardrails are enabled (common in
managed/multi-tenant deployments). The purge allocation is the expensive part.

**The fix** — purged size/count are bounded above by raw size/count (`purge` only removes data), and
`triggersOn` is monotone in its argument, so:

```java
long now = FBUtilities.nowInSeconds();                       // hoisted out of the column loop
...
if (!Guardrails.collectionSize.triggersOn(cells.dataSize(), null) &&
    !Guardrails.itemsPerCollection.triggersOn(cells.cellsCount(), null))
    continue;                                                // raw can't trigger => purged can't
ComplexColumnData liveCells = cells.purge(DeletionPurger.PURGE_ALL, now);
... existing logic on liveCells ...
```

Decision outcomes are identical: a guard fires iff the purged value triggers, and we only compute
the purged value when the (>=) raw value triggers.

**Expected impact** — removes one `ComplexColumnData` allocation + cell walk per collection column
per row for all non-violating rows (i.e., ~all rows); collection-heavy tables with guardrails
enabled notice on every flush/compaction.

**Effort** — ~15 lines, 1 file. Risk: low (monotonicity argument is local and reviewable).
**Verification** — guardrail unit tests (`GuardrailCollectionSizeTest` etc.) already assert
warn/fail behavior at thresholds; allocation gate or JFR on a collection-heavy flush.

---

## #10 — Fast path in `Row.Merger.merge` for the no-listener two/one-source cases

**What it does today** — `src/java/org/apache/cassandra/db/rows/Row.java:752-825`. For every merged
row (other than the already-fast-pathed "1 version, live deletion" case) it builds, **per row**:
a `BTreeSearchIterator` per input (`columnDataIterators.add(row.iterator())`, `:792`), a fresh
`MergeIterator` (`MergeIterator.get(...)`, `:805` — candidate array, heap, wrapper), then drains it
into `dataBuffer`.

**Why it's hot** — per row whose clustering exists in more than one source: every read that touches
a memtable plus an sstable (the default for recently-written data), data resolution, and
iterator-path compaction. Call chain: `UnfilteredRowIterators.merge` →
`UnfilteredRowMergeIterator.MergeReducer.getReduced` → `Row.Merger.merge`.

**The fix (bounded version)** — when `listener == null` (no per-version bookkeeping needed) and
exactly two versions are present with `activeDeletion.isLive()`, delegate to the existing BTree
merge used by the memtable write path:

```java
if (rowsToMerge == 2 && listenerAbsent && activeDeletion.isLive())
{
    Row a = firstNonNull(rows); Row b = secondNonNull(rows);
    return Rows.merge(a, b);          // Row.java / Rows.java:239 → BTreeRow.merge, no MergeIterator
}
```

`Rows.merge` (`src/java/org/apache/cassandra/db/rows/Rows.java:239-262`) reconciles liveness,
deletion, complex deletions, and cells with the same semantics (it is the canonical row-merge used
by `AtomicBTreePartition`); equivalence still needs to be argued carefully for shadowing corner
cases, which is why this ranks last despite the largest steady-state payoff.

**Expected impact** — removes 3-4 allocations + merge-heap setup per merged row on the memtable+
sstable read path; the most frequently executed multi-source row operation in the system.

**Effort** — ~25-40 lines, 1-2 files, **moderate semantic risk** (highest of the ten; requires the
equivalence argument plus randomized differential coverage). **Verification** — the branch's
`RandomDifferentialCompactionTest`/Harry suites are exactly the right harness: route the fast path
into compaction merges and assert byte-identical output; add a unit test diffing
`Rows.merge(a,b)` vs `Merger` output over randomized rows (the `Rows.diff` machinery at
`Rows.java:133` can express the assertion).

---

# Appendix — longlist (didn't make the cut)

Candidates examined and consciously excluded, with one-line reasons.

**Real but more effort / higher risk (good follow-up tickets):**
- `UnfilteredSerializer.serialize(Row...)` sstable branch double-writes every row body through
  `DataOutputBuffer.scratchBuffer` (`UnfilteredSerializer.java:203-212`) — full memcpy per row;
  fixing means trusting `serializedRowBodySize` for the size prefix (corruption risk if they ever
  diverge) — > 100 lines of careful work.
- `Row.Merger` per-source `row.iterator()` + non-reusable `MergeIterator` for the general N-source
  case — subsumed by #10's bounded version; full reusable merge machinery is a bigger project.
- BTI/byte-comparable: `AbstractTimeUUIDType.asComparableBytes` (`AbstractTimeUUIDType.java:97`) and
  `UUIDType.java:129` allocate a 16-byte buffer + `ByteSource` per clustering component — per-row in
  trie index writes/seeks, but the `ByteSource` abstraction makes a zero-alloc fix non-local.
- `Digest.updateWithLong/updateWithInt` (`db/Digest.java:172-194`) hash byte-at-a-time per cell on
  digest reads — switching to `Hasher.putLong` changes byte order (Guava is LE) and therefore digest
  values → cross-version digest mismatches; only safe with a digest-version bump. Not low-risk.
- `CompactionController.getPurgeEvaluator` (`CompactionController.java:254-293`) allocates
  `memtable.rowIterator(key)` per compacted partition just to test presence, plus a capturing lambda
  — real but needs a presence-check API on `Memtable` (cross-impl change).
- CQL result path materializes `List<List<byte[]>>` (`cql3/ResultSet.java`) — protocol-shaped;
  not a <= 100-line fix.

**Per-partition / per-response, small constant (low payoff):**
- `UnfilteredRowIteratorSerializer.serialize/serializedSize` allocate a fresh `SerializationHeader`
  + `SerializationHelper` per partition per response (`UnfilteredRowIteratorSerializer.java:101,156,181,200`) — two small objects per response partition.
- `DuplicateRowChecker.duringCompaction` (`db/transform/DuplicateRowChecker.java:121-129`) allocates
  two `Transformation`s per partition — small, and the checker exists for safety.
- `SortedTableWriter.onRow/onStaticRow` capture-lambda per row (`SortedTableWriter.java:295-310`) —
  only when flush observers (SAI) exist; `onStaticRow` is once per partition and likely
  escape-analyzed.
- `CompactionIterator` constructor stream (`CompactionIterator.java:246`) — once per compaction.
- `SSTableSimpleScanner` stream (`SSTableSimpleScanner.java:83`) — once per scanner.

**Checked and already optimized (no action; kept here so the next sweep skips them):**
- `MetadataCollector.update(Cell)` call-site splitting (`MetadataCollector.java:250-287`) and
  `Cell.Serializer.serialize` monomorphization (`Cell.java:297-338`) — the in-codebase templates.
- `BloomFilter.indexes` reusable thread-local (`utils/BloomFilter.java:35-102`).
- `SerializationHelper`/`DeserializationHelper` reusable carrier fields + cached search iterators.
- `Mutation` serialization caching (`Mutation.java:520`), `ReadResponse` buffer sizing via moving
  average (`ReadResponse.java:230-233`), `Message.deserializeParams` `NO_PARAMS` fast path.
- `Tracing.trace` non-varargs overloads; `Sampler.addSample` isEnabled guards;
  `ColumnFamilyStore.apply` guards `update.dataSize()` and row-cache invalidation.
- `QueryCancellationChecker.maybeCancel` rate-limits via `approxTime` (`ReadCommand.java:849-887`).
- `BTreeRow.purge` fast-path via `minLocalDeletionTime` and identity-preserving
  `transformAndFilter` (`BTreeRow.java:490-533`).
- `Verb.fromId` array lookup; `VIntCoding` thread-local only on the non-Plus fallback;
  `NativeCell.value()` returns `NativeData` (no buffer wrapper); `Rows.collectStats` accumulator
  style; hints (`HintsBuffer/HintsCatalog`) explicitly de-lambda'd.

**Cold-path `values()`/stream sites (not worth touching):**
- `tcm/sequences/*.java`, `tcm/Discovery.java:252`, `tcm/membership/Directory.java:605` — cluster
  metadata ops, rare.
- `streaming/StreamingState.java:177`, `JMXNotificationProgressListener.java:74` — streaming/JMX.
- `cql3/constraints/*.java:72,78` — DDL-time.
- `net/MessagingService.java:291`, `ThreadLocalByteBufferHolder.java:41`,
  `ConsistencyLevel.java:62-65`, `OperationType.java:81` — static init or rare.
- `db/aggregation/AggregationSpecification.java:256` — per paging state with GROUP BY only.
- `db/compression/CompressionDictionary*.java` — dictionary lifecycle events.
- `ArtificialLatency` streams (`net/ArtificialLatency.java:341,395-400`) — JMX accessors.
- `service/reads/repair/ReadRepairEvent` streams — diagnostics events, off by default.
- `StartupClusterConnectivityChecker`, `Rebuild`, `UncommittedDataFile` `Pattern.compile` — startup.
- `ColumnFamilyStore.java:2728,3315` streams — exceptional/administrative paths.
- `UnfilteredRowIterators.mergeStaticRows` was the only stream found in `db/rows`/`db/partitions`
  hot code (see #5); `db/transform` is stream-free on the per-row path.
