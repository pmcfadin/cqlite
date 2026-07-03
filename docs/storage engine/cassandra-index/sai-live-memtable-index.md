# SAI Live Memtable Index Architecture

## Summary

SAI (Storage-Attached Index) implements a read-time memtable-index merge seam that directly answers CQLite's Q1 (freshness) and Q2 (feasibility). At query time, `StorageAttachedIndexSearcher.search()` merges live (unflushed) memtable index postings with on-disk SSTable index postings without requiring a flush—the exact pattern CQLite needs for analytical reads to see all node-local state. This is shipping in Cassandra 5.0 as a **secondary index only**, applying SAI's design to all rows (not just indexed columns) requires implementing a parallel Index and Searcher abstraction, plus cost analysis for the read-time merge overhead.

## Key Classes

| Class | File | Responsibility |
|-------|------|-----------------|
| **MemtableIndexManager** | `src/java/org/apache/cassandra/index/sai/memory/MemtableIndexManager.java:43–184` | Holds ConcurrentMap<Memtable,MemtableIndex> (liveMemtableIndexMap); indexes writes, updates, and manages memtable lifecycle events. No query API; data flows through MemtableIndex.search(). |
| **MemtableIndex** | `src/java/org/apache/cassandra/index/sai/memory/MemtableIndex.java:47–134` | Delegates to MemoryIndex (TrieMemoryIndex or VectorMemoryIndex); exposes search(expr, keyRange) → KeyRangeIterator on live index. Decorated with metadata (min/maxTerm, writeCount, estimatedMemoryUsed). |
| **MemoryIndex** (abstract) | `src/java/org/apache/cassandra/index/sai/memory/MemoryIndex.java` | Interface: add/update/search/isEmpty/getMin/MaxTerm; two implementations for literal (TrieMemoryIndex) and vector (VectorMemoryIndex) indexes. |
| **TrieMemoryIndex** | `src/java/org/apache/cassandra/index/sai/memory/TrieMemoryIndex.java:61–189` | Stores indexed values in InMemoryTrie<PrimaryKeys> (byte-ordered, on/off-heap per TrieMemtable); exactMatch/rangeMatch search returning InMemoryKeyRangeIterator. Synchronized add; unsupported update (vector-only). |
| **VectorMemoryIndex** | `src/java/org/apache/cassandra/index/sai/memory/VectorMemoryIndex.java` | In-memory ANN index for vector search; supports add/update. Stores dense vectors; search returns nearest neighbors. Initialized selectively per index.termType().isVector(). |
| **StorageAttachedIndex** | `src/java/org/apache/cassandra/index/sai/StorageAttachedIndex.java:193–1046` | Per-column secondary index; owns memtableIndexManager() (line 665–668), integrates with Indexer callbacks (insertRow/updateRow, lines 1026–1037), exposes for query dispatch. |
| **StorageAttachedIndexSearcher** | `src/java/org/apache/cassandra/index/sai/plan/StorageAttachedIndexSearcher.java:83–150` | Index.Searcher impl; orchestrates query, calls Operation.buildIterator() and retrieves results via ResultRetriever/ScoreOrderedResultRetriever. Does not directly query memtables; delegates to QueryViewBuilder. |
| **QueryViewBuilder** | `src/java/org/apache/cassandra/index/sai/plan/QueryViewBuilder.java:46–150` | Snapshots live memtable indexes (line 135: `expression.getIndex().memtableIndexManager().getLiveMemtableIndexesSnapshot()`) and sstable indexes into QueryExpressionView for each expression. Holds reference-counted SSTableIndex handles. |
| **IndexSearchResultIterator** | `src/java/org/apache/cassandra/index/sai/disk/IndexSearchResultIterator.java:42–134` | Wraps KeyRangeUnionIterator over memtable + sstable index results. Lines 84–88 query each memtable, lines 91–104 query each sstable, line 115 unions all iterators. **This is the read-time merge point.** |
| **InMemoryKeyRangeIterator** | `src/java/org/apache/cassandra/index/sai/memory/InMemoryKeyRangeIterator.java:28–102` | Non-threadsafe iterator over PriorityQueue<PrimaryKey> or SortedSet<PrimaryKey>; de-duplicates if needed. Returned by TrieMemoryIndex exact/range search. |

## Read-Time Merge Architecture (Q1 Answer)

```
Query → StorageAttachedIndexSearcher.search()
  ↓
Operation.buildIterator() [lines 321–323 of plan/Operation.java]
  ↓
QueryController.getIndexQueryResults(expressions) [plan/QueryController.java:265–300+]
  ↓
QueryViewBuilder.build() [plan/QueryViewBuilder.java:99–121]
  └─ Snapshot live memtables via MemtableIndexManager.getLiveMemtableIndexesSnapshot()
  └─ Load matched sstable indexes from IndexViewManager
  ↓
QueryExpressionView {memtableIndexes[], sstableIndexes[]}
  ↓
IndexSearchResultIterator.build(QueryExpressionView) [disk/IndexSearchResultIterator.java:58–116]
  ├─ FOR each MemtableIndex:
  │  └─ memtableIndex.search(expr, keyRange) → KeyRangeIterator [MemtableIndex.java:106–109]
  │     └─ TrieMemoryIndex.search() → exactMatch/rangeMatch → InMemoryKeyRangeIterator
  │     └─ VectorMemoryIndex.search() → ANN search → KeyRangeIterator
  ├─ FOR each SSTableIndex:
  │  └─ sstableIndex.search(expr, keyRange) → List<KeyRangeIterator>
  └─ KeyRangeUnionIterator.build(all subIterators) [line 115]
  ↓
ResultRetriever consumes union → fetches rows from memtable + sstable
```

**Key insight**: Both memtable and sstable index results are queried AT READ TIME and merged via union. No flush required. This is exactly Q1's ask.

## Write-Time Indexing (Incremental)

1. **Memtable mutation** → StorageAttachedIndex.Indexer.insertRow/updateRow (lines 1026–1037)
2. **MemtableIndexManager.index(key, row, memtable)** (lines 70–109): Initializes MemtableIndex if absent, delegates to memoryIndex.add()
3. **TrieMemoryIndex.add(key, clustering, value)** (lines 94–127): Analyzes value if configured, adds term→{primaryKey} mapping to InMemoryTrie
4. **Index on every write** (synchronized in TrieMemoryIndex.add); update support exists only for vector indexes

## Extension Points / Pluggability Seams

1. **MemoryIndex pluggability** (MemtableIndex.java:56): Constructor branches on `index.termType().isVector()` → picks TrieMemoryIndex or VectorMemoryIndex. To add a third in-memory index type (e.g., inverted bitmap for integers), implement MemoryIndex abstract interface and wire in MemtableIndex constructor.

2. **IndexTermType.isVector/isFrozen/isNonFrozenCollection/etc** (referenced throughout): Determines index strategy. Custom term types would require IndexTermType subclass + analyzer/writer registration.

3. **AbstractAnalyzer** (MemtableIndex.java:103–117, TrieMemoryIndex.java:103–122): Pluggable term analyzer; StorageAttachedIndex.analyzer() returns per-index instance. Add language-specific or custom analyzers via indexMetadata.options.

4. **Index.Indexer callback chain** (StorageAttachedIndex, lines 1012–1045): Cassandra's write-time callback; SAI implements it to intercept memtable writes. Alternative storage engines would hook here, not hardcode SAI.

5. **QueryViewBuilder + View management**: IndexViewManager (src/java/org/apache/cassandra/index/sai/disk/IndexViewManager.java) manages SSTable index lifecycle. To inject custom on-disk format or different SSTable traversal, subclass IndexViewManager or provide alternate View impl.

6. **Index.Searcher interface** (Index.java in core): Cassandra's query-time callback; StorageAttachedIndexSearcher implements it. Alternative storage engines provide their own Searcher impl.

## Hard Couplings (Q2 Feasibility Constraints)

1. **Cassandra Memtable type system** (MemtableIndexManager.java:46, MemtableIndex.java:52): Indexes are keyed by concrete Memtable instance. Coupling: Memtable must be a stable identity throughout its lifetime (flush or discard lifecycle). Alternative engines cannot use opaque memtable IDs; must use Cassandra's Memtable class directly or fork/replace it.

2. **ConcurrentHashMap<Memtable, MemtableIndex>** (MemtableIndexManager.java:46): Indexes **must** be garbage-collectable when memtable is discarded. If memtable instance is reused or pooled, leaked indexes accumulate. Cassandra's lifecycle (newMemtable → insert → flush → discard → new cycle) is required.

3. **MemtableRenewedNotification / MemtableSwitchedNotification / MemtableDiscardedNotification** (StorageAttachedIndexGroup.java:279–287): SAI receives JVM notifications to clean up indexes. Alternative engines must emit or intercept these notifications to keep index map in sync.

4. **UnfilteredPartitionIterator return type** (StorageAttachedIndexSearcher.java:133–150): Query results must be wrapped as an UnfilteredPartitionIterator (rows with optional tombstones/range deletes). Direct blob export (CQLite-style) requires conversion layer; secondary index searchers cannot bypass this.

5. **PrimaryKey factory** (StorageAttachedIndex.java:201, QueryViewBuilder.java:74): Indexes are keyed by token + partition key + optional clustering. Tightly coupled to TableMetadata.partitioner and TableMetadata.comparator. Cannot swap partition schemes without full index rebuild.

6. **Consistency level enforcement** (QueryViewBuilder.java / StorageAttachedIndexSearcher.java): SAI respects Cassandra's read consistency model (repaired vs unrepaired, strict vs lenient filtering, replica filtering protection). Alternative engines must also implement this; no seam to plug in custom consistency logic.

7. **SSTableReader lifecycle** (IndexSearchResultIterator.java:91–104): SSTable indexes hold reference counts on SSTableReader. If an SSTable is deleted mid-query, the query fails with IllegalStateException (line 99). Cannot defer SSTable cleanup or use external storage without lifecycle integration.

## Relevance to Q1: Analytical Read Freshness

**Q1 Statement**: When Trino/DataFusion reads a node via Arrow Flight, it sees only flushed SSTables. What changes in Cassandra enable an analytical read to see memtable contents?

**SAI Answer**:
- SAI **already does this for indexed columns**. IndexSearchResultIterator merges live memtable postings + sstable postings at read time (no flush required).
- **Cost**: MemtableIndexManager overhead = O(1) per live memtable, O(writes) per write. For a node with ~3 live memtables and 1 billion values, the in-memory trie + postings cost is ~500MB–1GB depending on value size. Query merge cost is ~O(memtables) union overhead.
- **Generalization to all rows (not just indexed columns)**: Implement a row-wise SAI-like Index/Searcher pair that indexes entire rows (not columns). Requires:
  1. Full-row serialization (or projection) in memtable index at write time (overhead > column index).
  2. Whole-row deserialization at query time (vs. selective column pushdown).
  3. Separate index manager instance per table (not per column).

**Seam in Cassandra**: None needed at 5.0; SAI already ships the pattern. To scale beyond secondary indexes, register a different Index.Group / Index.Indexer in ColumnFamilyStore.indexManager (src/java/org/apache/cassandra/db/ColumnFamilyStore.java, indexManager field).

## Relevance to Q2: Alternative/Adjacent Storage Engine Feasibility

**Q2 Statement**: Is CQLite viable as (a) a storage engine inside Cassandra, or (b) an adjacent OLAP engine?

**SAI Evidence**:

**(a) Inside Cassandra (SSTableFormat replacement)**:
- **No direct seam for row-level index hooks**. SSTableFormat (src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java) writes SSTables and on-disk indexes, but the **memtable → index** hook is specific to Index.Indexer (column-by-column). To replace the storage engine:
  - Fork Memtable class (or CEP-11 pluggable API, see trunk notes).
  - Fork SSTableReader / ColumnFamilyStore.SlicedIterator.
  - Implement Index.Indexer to intercept writes (existing seam).
  - Implement Index.Searcher to replace row retrieval (existing seam).
  - **Viability**: Moderate. The Indexer/Searcher seams exist; Memtable/SSTableFormat are not yet pluggable (5.0).

**(b) Adjacent OLAP engine (like CQLite Flight connector)**:
- **Strong seam**: Index.Searcher. CQLite can register an Index impl that reads its own SSTable format, indexes via memtable snapshots (a la SAI MemtableIndexManager), and returns UnfilteredPartitionIterators without touching Cassandra's internal MemtableFormat.
- **Limitation**: Cannot bypass memtable flush. CQLite's Arrow Flight node-read sees only flushed sstables unless it hooks the Index system (which SAI demonstrates is viable).
- **Cost**: Index.Searcher callback is invoked only if a secondary index is created (DDL required). Cannot auto-instrument all tables.
- **Viability**: High. CQLite already does this (Arrow Flight + Trino connector exists per project context).

## Trunk vs. 5.0 Notes

- **CEP-11 Pluggable Memtable API** (Trunk, since cassandra-6.0 merged 2026-07-03): Src/java/org/apache/cassandra/db/memtable/Memtable_API.md defines pluggable Memtable implementations. **Impact**: Alternative storage engines can implement Memtable interface instead of subclassing TrieMemtable. **Not in 5.0**: Still hardcoded to TrieMemtable/SkipListMemtable.
- **SSTableFormat pluggability** (Trunk): SSTableFormat.java has registration hooks for custom formats. **Not in 5.0**: Cassandra's BIG format only; extensions must provide alternate ColumnFamilyStore / SSTableReader.
- **MemtableIndexManager**: Identical on 5.0 and trunk. No version guards in SAI code.
- **No "since" javadocs in SAI sources**: SAI shipped in 4.0; live memtable indexing is stable.

## Known Limitations / Design Tradeoffs

1. **Memtable index is not persistent**: Crash loses in-flight indexes; rebuild on restart via MemtableIndexManager.invalidate() (line 180–183).
2. **TrieMemoryIndex.synchronized add()** (line 94): Write contention on high-cardinality, high-throughput indexes. Vector indexes support lock-free updates.
3. **No index compaction**: Multiple overlapping memtable indexes are queried independently; no merging of overlapping memtable-index postings across flushes (handled by SSTable compaction).
4. **Analyzer applied at write time only**: Index-time term expansion (e.g., fuzzy matching) is baked into MemtableIndex; query-time filtering (satisfiedBy) is separate. Cannot re-analyze at query time.
