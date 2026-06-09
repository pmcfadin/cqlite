## Storage-Attached Index (SAI)

One-paragraph summary: Cover SAI data structures and files, lifecycle with SSTables, and query flow for numeric/text and vector queries.

### In this chapter you will learn
- SAI on-disk structures and segments
- How SAI integrates with SSTable lifecycle
- Vector index component files, HNSW graph layout, and ANN query path
- Query paths for range/LIKE and vector similarity
- Practical examples and limitations

## File Layout and Segments

SAI builds per-column indexes that are persisted alongside the base table's SSTables. Each indexed column produces one or more immutable on-disk segments. During flush and compaction, new SAI segments are created and old ones are merged according to index policy, independently of the base table's SSTables.

- Per-column indexing: each indexed column (numeric, text, vector) has its own writer/reader and set of segment files.
- Segments are immutable: new segments are added on flush; compactions merge segments and drop obsolete postings.
- Base-table authority: SAI returns candidate primary keys; rows are materialized via the normal read path against `Data.db`.

SAI 5.0 uses a single on-disk version identifier `aa` (lowercase two characters). All component filenames embed this version, following the pattern:

```
<sstable_descriptor>-SAI+aa+[<index_name>]+<ComponentDisplayName>.db
```

The display name in the filename is the string value passed to the `IndexComponent` enum constructor -- not the Java enum name. For example, enum `TERMS_DATA` maps to display name `TermsData`, `POSTING_LISTS` to `PostingLists`, `COLUMN_COMPLETION_MARKER` to `ColumnComplete`, and `GROUP_COMPLETION_MARKER` to `GroupComplete`. Assembly logic is in
[`Version.java:142-155`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/format/Version.java).

![SAI file layout](diagrams/sai-file-layout)
  - Alt-text: SAI per-column segments (numeric/text/vector) and their on-disk components.
  - Caption: SAI segments are written per indexed column; vector segments sit alongside numeric/text segments.

## Vector indexes on disk

When a column is indexed with `CREATE CUSTOM INDEX ... USING 'StorageAttachedIndex'` on a
`vector<float, N>` column, SAI writes five component files per SSTable segment:

```
<descriptor>-SAI+aa+[<index_name>]+TermsData.db
<descriptor>-SAI+aa+[<index_name>]+CompressedVectors.db
<descriptor>-SAI+aa+[<index_name>]+PostingLists.db
<descriptor>-SAI+aa+[<index_name>]+Meta.db
<descriptor>-SAI+aa+[<index_name>]+ColumnComplete.db
```

The `VECTOR_COMPONENTS` set is defined in
[`V1OnDiskFormat.java:92-96`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/V1OnDiskFormat.java):

```java
VECTOR_COMPONENTS = EnumSet.of(
    COLUMN_COMPLETION_MARKER, META, COMPRESSED_VECTORS, TERMS_DATA, POSTING_LISTS)
```

### TermsData.db -- jvector HNSW graph

`TermsData.db` stores a jvector `OnDiskGraphIndex` -- an HNSW (Hierarchical Navigable Small
World) graph. On open, `DiskAnn` reads the graph at the byte offset stored in
`ComponentMetadata.offset` for the `TERMS_DATA` component
([`DiskAnn.java:58-60`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/vector/DiskAnn.java)):

```java
SegmentMetadata.ComponentMetadata termsMetadata = componentMetadatas.get(IndexComponent.TERMS_DATA);
graphHandle = indexFiles.termsData();
graph = new CachingGraphIndex(
    new OnDiskGraphIndex<>(RandomAccessReaderAdapter.createSupplier(graphHandle),
                           termsMetadata.offset));
```

`CachingGraphIndex` wraps `OnDiskGraphIndex` and keeps recently traversed nodes in heap to reduce disk
seeks on repeated graph queries.

### CompressedVectors.db -- Product Quantization store

`CompressedVectors.db` holds optional PQ-compressed vectors produced by the jvector library. At the
offset for `COMPRESSED_VECTORS` in `ComponentMetadataMap` a single boolean byte signals whether PQ
data follows
([`DiskAnn.java:62-70`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/vector/DiskAnn.java)):

```java
long pqSegmentOffset = componentMetadatas.get(IndexComponent.COMPRESSED_VECTORS).offset;
reader.seek(pqSegmentOffset);
boolean containsCompressedVectors = reader.readBoolean();
if (containsCompressedVectors)
    compressedVectors = CompressedVectors.load(reader, reader.getFilePointer());
```

When present, PQ vectors enable two-pass scoring: a fast approximate pass via
`CompressedVectors.approximateScoreFunctionFor()`, followed by full-precision exact scoring of the
top-K candidates. See
[`VectorIndexSegmentSearcher.java:267-281`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/segment/VectorIndexSegmentSearcher.java).

### PostingLists.db -- ordinals map

For vector indexes, `PostingLists.db` is an `OnDiskOrdinalsMap` -- a bidirectional mapping between
HNSW graph node ordinals and SSTable segment row IDs
([`DiskAnn.java:73-74`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/vector/DiskAnn.java)):

```java
SegmentMetadata.ComponentMetadata postingListsMetadata =
    componentMetadatas.get(IndexComponent.POSTING_LISTS);
ordinalsMap = new OnDiskOrdinalsMap(
    indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);
```

The on-disk layout written by
[`VectorPostingsWriter.java:34-111`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/vector/VectorPostingsWriter.java)
is (within the segment's byte range):

1. `int` -- count of deleted ordinals
2. N x `int` -- deleted ordinal values
3. Ordinal to row ID section:
   - `int` -- total vector count
   - count x `long` -- per-ordinal offsets (8 bytes each) pointing to postings lists
   - count postings lists, each: `int` (size) + size x `int` (row IDs)
4. Row ID to ordinal reverse section:
   - K x (`int` rowId, `int` ordinal) pairs sorted by rowId
   - `long` at the very end: file offset of the start of the reverse-lookup section

Source: [`OnDiskOrdinalsMap.java:41-66`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/vector/OnDiskOrdinalsMap.java).

### SegmentMetadata fields

All segment types (numeric, literal, vector) share the same `META` file structure. The file contains a
VInt segment count followed by N records. Each record is serialized in this order
([`SegmentMetadata.java:118-176`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/segment/SegmentMetadata.java)):

| Field | Type | Description |
|-------|------|-------------|
| `rowIdOffset` | `long` | Add to segment row ID to get SSTable row ID |
| `numRows` | `long` | Number of indexed rows in this segment |
| `minSSTableRowId` | `long` | Lowest SSTable row ID in segment |
| `maxSSTableRowId` | `long` | Highest SSTable row ID in segment |
| `minKey` | `int`-length-prefixed bytes | Min primary key as OSS50 comparable bytes |
| `maxKey` | `int`-length-prefixed bytes | Max primary key as OSS50 comparable bytes |
| `minTerm` | `int`-length-prefixed bytes | Min indexed column value |
| `maxTerm` | `int`-length-prefixed bytes | Max indexed column value |
| `ComponentMetadataMap` | nested | Per-component `{root, offset, length, attributes}` map |

`ComponentMetadata` stores three `long` values (`root`, `offset`, `length`) plus a `Map<String,String>`
for additional attributes, serialized as `int` count followed by UTF-8 key-value pairs.

### PrimaryKeyMap -- row ID to primary key resolution

After graph search returns `(rowId, score)` pairs, SAI resolves them to full primary keys via
`PrimaryKeyMap`
([`PrimaryKeyMap.java:71-97`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/PrimaryKeyMap.java)):

- `primaryKeyFromRowId(long sstableRowId)` returns `PrimaryKey`
- `rowIdFromPrimaryKey(PrimaryKey key)` returns `long`
- `ceiling(Token)` / `floor(Token)` return token boundary row IDs

Two concrete implementations exist
([`V1OnDiskFormat.java:133-137`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/V1OnDiskFormat.java)):

- `SkinnyPrimaryKeyMap.Factory` -- tables without clustering: reads `PARTITION_KEY_BLOCKS*` only
- `WidePrimaryKeyMap.Factory` -- tables with clustering: adds `CLUSTERING_KEY_BLOCKS*`

### ANN query path

[`VectorIndexSegmentSearcher.orderBy()`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/segment/VectorIndexSegmentSearcher.java)
orchestrates the full ANN path:

1. Decompose the query vector from the CQL `Expression` lower bound.
2. Compute `topK = optimizeFor.topKFor(limit)` (oversampling for recall).
3. **Key-range restriction** (when a WHERE clause limits partitions):
   - Resolve `minSSTableRowId` / `maxSSTableRowId` via `PrimaryKeyMap.ceiling()` / `floor()`.
   - If the candidate row count is at most `maxBruteForceRows`, skip graph traversal and run
     exact brute-force scoring over `OnDiskOrdinalsMap.OrdinalsView` directly.
   - Otherwise, build a `SparseFixedBitSet` of allowed ordinals and pass it to the graph
     searcher as an acceptance filter.
4. **Graph search** -- `DiskAnn.search()` calls jvector `GraphSearcher` on the cached HNSW
   graph. With PQ present, a fast approximate pass precedes full-precision reranking.
5. **Ordinal to row ID** -- `OnDiskOrdinalsMap.RowIdsView.getSegmentRowIdsMatching()`.
6. **Row ID to PrimaryKey** -- `RowIdToPrimaryKeyWithScoreIterator` calls
   `PrimaryKeyMap.primaryKeyFromRowId()`.
7. Scored primary keys from all segments are merged by the caller, then rows are fetched from
   `Data.db` via the standard read path.

## Query Flow

At query time, SAI chooses a path based on predicate type:

- Numeric: range predicates probe numeric trees/structures to produce candidate primary keys.
- Text: equality/prefix-like predicates probe term dictionaries and posting lists.
- Vector: a vector similarity search over vector segments returns approximate nearest-neighbor
  candidates via the HNSW graph search described above.

Candidates are then deduplicated/merged and fetched from the base table using the standard read path
(Bloom -> Index -> Summary -> Data). Non-indexed predicates are applied as filters on the fetched rows.

![SAI query flow](diagrams/sai-query-flow)
  - Alt-text: Dispatcher routes numeric/text/vector queries; candidates merged and passed to base read path.
  - Caption: SAI generates candidate primary keys which are validated via the SSTable read path.

Illustrative examples (trimmed):

- Numeric range: "Find orders with `amount` in [100, 200)." The `amount` SAI probes its numeric
  segment(s) to produce candidate row keys, then the base table fetch validates and returns rows
  in-range.
- Text prefix: "Find pages with `title` starting with `foo`." The `title` SAI looks up the
  term/prefix to retrieve posting lists, yielding candidates that are fetched and post-filtered.
- Vector similarity: "Return top-10 nearest neighbors to query vector q in `embedding`." The
  `embedding` SAI performs HNSW graph search over vector segments to return candidates ranked by
  distance, then base rows are fetched and returned (respecting LIMIT).

### Caching and Memory Behavior

- Segment-level structures (term dictionaries, numeric trees, vector graph nodes) are memory-mapped or
  buffered; hot paths benefit from the OS page cache. Implementations keep small per-segment metadata
  and open file handles.
- For vectors, `CachingGraphIndex` keeps recently traversed HNSW nodes in heap to reduce disk seeks
  on repeated queries. PQ metadata is loaded once per segment open.

### Compaction of Index Segments

- SAI compaction merges per-column segments, drops obsolete postings/entries, and rewrites compacted
  segments. Compaction policy is independent of base-table compaction but aligned at lifecycle
  boundaries (flush/cleanup).
- Merging reduces candidate duplication and improves locality; during compaction, readers see a stable
  view via segment switching.

### Corruption and Error Handling

- Segment checksum and metadata validation occur on open and sometimes at read boundaries; failures
  typically isolate to specific segments. The index can be rebuilt for affected columns while the base
  table remains intact.
- On validation failure, queries may skip a bad segment (degraded results) or surface errors based on
  configuration; operators should run validation tools and rebuild.

### Complexity Notes

- Numeric range: O(log S + R) per segment, where S is segment size and R is number of results in-range.
- Text term/prefix: O(log S + P) for term lookup plus posting traversal P.
- Vector ANN (HNSW): O(log N * ef) per segment, where N is graph node count and ef is the exploration
  beam width. When PQ compression is present, a two-pass strategy applies: fast approximate scoring via
  `CompressedVectors`, then exact rerank of top-ef candidates. Brute-force fallback is O(N) and triggers
  when the key-restricted candidate count falls below `maxBruteForceRows` (computed as
  `maximumNodeConnections * expectedNodesVisited`).

### Key Takeaways
- SAI is per-column and segment-based; segments are immutable and merged by compaction.
- All SAI 5.0 files use the single on-disk version `aa`; filenames follow the pattern
  `<descriptor>-SAI+aa+[<index_name>]+<ComponentDisplayName>.db`.
- Vector indexes write five component files per segment: `TermsData.db` (HNSW graph),
  `CompressedVectors.db` (optional PQ), `PostingLists.db` (ordinals map), `Meta.db`,
  and `ColumnComplete.db`.
- Numeric/text/vector predicates are routed to specialized segment readers to produce candidates.
- Base-table SSTables remain authoritative; SAI candidates are validated via the normal read path.
- Prefer SAI over 2i for range, LIKE, and vector searches in Cassandra 5.0.

### References
- Cassandra 5.0.8
  - SAI root: `https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai`
  - SAI on-disk formats: `https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk`
  - SAI query path: `https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/query`
  - SAI v1 disk (includes vector indexing classes): `https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1`
  - Vector CQL type (`VectorType`): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/marshal/VectorType.java`
  - SAI version identifier: [`Version.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/format/Version.java)
  - Index component enum: [`IndexComponent.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/format/IndexComponent.java)
  - On-disk format registry: [`V1OnDiskFormat.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/V1OnDiskFormat.java)
  - Segment metadata: [`SegmentMetadata.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/segment/SegmentMetadata.java)
  - Vector disk index: [`DiskAnn.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/vector/DiskAnn.java)
  - Vector postings writer: [`VectorPostingsWriter.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/vector/VectorPostingsWriter.java)
  - Ordinals map: [`OnDiskOrdinalsMap.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/vector/OnDiskOrdinalsMap.java)
  - Vector segment searcher: [`VectorIndexSegmentSearcher.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/v1/segment/VectorIndexSegmentSearcher.java)
  - Primary key map: [`PrimaryKeyMap.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/index/sai/disk/PrimaryKeyMap.java)

For implementation details, see Appendix C.
