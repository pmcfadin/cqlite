## Storage-Attached Index (SAI)

One-paragraph summary: Cover SAI data structures and files, lifecycle with SSTables, and query flow for numeric/text and vector queries.

### In this chapter you will learn
- SAI on-disk structures and segments
- How SAI integrates with SSTable lifecycle
- Query paths for range/LIKE and vector similarity
- Practical examples and limitations

## File Layout and Segments

SAI builds per-column indexes that are persisted alongside the base table’s SSTables. Each indexed column produces one or more immutable on-disk segments. During flush and compaction, new SAI segments are created and old ones are merged according to index policy, independently of the base table’s SSTables.

- Per-column indexing: each indexed column (numeric, text, vector) has its own writer/reader and set of segment files.
- Segments are immutable: new segments are added on flush; compactions merge segments and drop obsolete postings.
- Base-table authority: SAI returns candidate primary keys; rows are materialized via the normal read path against `Data.db`.

![SAI file layout](diagrams/sai-file-layout)
  - Alt-text: SAI per-column segments (numeric/text/vector) and their on-disk components.
  - Caption: SAI segments are written per indexed column; vector segments sit alongside numeric/text segments.

## Query Flow

At query time, SAI chooses a path based on predicate type:

- Numeric: range predicates probe numeric trees/structures to produce candidate primary keys.
- Text: equality/prefix-like predicates probe term dictionaries and posting lists.
- Vector: a vector similarity search over vector segments returns approximate nearest-neighbor candidates.

Candidates are then deduplicated/merged, and fetched from the base table using the standard read path (Bloom → Index → Summary → Data). Non-indexed predicates are applied as filters on the fetched rows.

![SAI query flow](diagrams/sai-query-flow)
  - Alt-text: Dispatcher routes numeric/text/vector queries; candidates merged and passed to base read path.
  - Caption: SAI generates candidate primary keys which are validated via the SSTable read path.

Illustrative examples (trimmed):

- Numeric range: “Find orders with `amount` in [100, 200).” The `amount` SAI probes its numeric segment(s) to produce candidate row keys, then the base table fetch validates and returns rows in-range.
- Text prefix: “Find pages with `title` starting with `foo`.” The `title` SAI looks up the term/prefix to retrieve posting lists, yielding candidates that are fetched and post-filtered.
- Vector similarity: “Return top-10 nearest neighbors to query vector q in `embedding`.” The `embedding` SAI performs a vector similarity search over vector segments to return candidates ranked by distance, then base rows are fetched and returned (respecting LIMIT).

### Caching and Memory Behavior

- Segment-level structures (term dictionaries, numeric trees, vector blocks) are memory-mapped or buffered; hot paths benefit from the OS page cache. Implementations may keep small per-segment metadata and open-file handles.
- For vectors, blocks and auxiliary structures (e.g., centroids/quantization metadata when present) are accessed on demand; memory usage scales with number of active segments and query concurrency.

### Compaction of Index Segments

- SAI compaction merges per-column segments, drops obsolete postings/entries, and rewrites compacted segments. Compaction policy is independent of base-table compaction but aligned at lifecycle boundaries (flush/cleanup).
- Merging reduces candidate duplication and improves locality; during compaction, readers see a stable view via segment switching.

### Corruption and Error Handling

- Segment checksum and metadata validation occur on open and sometimes at read boundaries; failures typically isolate to specific segments. The index can be rebuilt for affected columns while the base table remains intact.
- On validation failure, queries may skip a bad segment (degraded results) or surface errors based on configuration; operators should run validation tools and rebuild.

### Complexity Notes

- Numeric range: O(log S + R) per segment, where S is segment size and R is number of results in-range.
- Text term/prefix: O(log S + P) for term lookup plus posting traversal P.
- Vector KNN: roughly O(C · B · log S) depending on the segment algorithm, where C is candidate beams/centroids, B blocks probed; exact complexity varies by implementation.

### Key Takeaways
- SAI is per-column and segment-based; segments are immutable and merged by compaction.
- Numeric/text/vector predicates are routed to specialized segment readers to produce candidates.
- Base-table SSTables remain authoritative; SAI candidates are validated via the normal read path.
- Prefer SAI over 2i for range, LIKE, and vector searches in Cassandra 5.0.
- Vector indexing integrates with SAI; queries return approximate nearest neighbors efficiently.

### References
- Cassandra 5.0.0 (pinned)
  - SAI root: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai`
  - SAI on-disk formats: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai/disk`
  - SAI query path: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai/query`
  - SAI v1 disk (includes vector indexing classes): `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai/disk/v1`
  - Vector CQL type (`VectorType`): `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/marshal/VectorType.java`
  
For implementation details, see Appendix C.


