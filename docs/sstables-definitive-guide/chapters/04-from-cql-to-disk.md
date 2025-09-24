## From CQL to Disk

This chapter traces a CQL mutation from client to durable storage: how it becomes a partition with clustering rows and cells, how memtables and the commit log (WAL) capture it, and how a flush turns memory state into SSTable components: `Data.db`, `Index.db`, `Summary.db`, `Filter.db`, `Statistics.db`, and `CompressionInfo.db`.

### In this chapter you will learn
- How mutations map to partitions, clustering keys, and cells
- How memtables and the WAL capture writes before flush
- How flush builds Data/Index/Summary/Stats/Filter/CompressionInfo
- The core steps of the flush pipeline via short pseudocode

## Mutation to Partition Structures

A single CQL mutation is normalized into a partition key, zero or more clustering keys, and one or more cells. Deletes are represented as tombstones (partition-, row-, cell-level) and range tombstones. The on-disk `Data.db` encodes this with a serialization header derived from schema and a sequence of unfiltered rows and markers.

## Memtables and WAL

On write, Cassandra appends to a commit log segment and updates an in-memory memtable for the affected table. The memtable is an ordered map keyed by partition and clustering. When the memtable is full or a trigger fires, a flush converts the in-memory view into immutable SSTable components and discards the memtable.

## Flush Pipeline

Diagram: the flush pipeline from memtable to per-component outputs.

- Diagram (Mermaid source): `../diagrams/flush-pipeline.mmd`
- Alt text: Flush pipeline from Memtable/WAL to per-component files with TOC.
- Caption: Flush steps create `Data.db` then derive `Index/Summary/Filter/Statistics/CompressionInfo` and `TOC.txt`.

Minimal pseudocode for the pipeline:

```text
// Memtable → SSTable (simplified)
for each partition in memtable in key order:
  write_partition_to(Data.db)
  record_index_entry(Index.db)
sample_index_for_summary(Summary.db)
build_bloom_over_partition_keys(Filter.db)
collect_stats_and_write(Statistics.db)
write_compression_metadata(CompressionInfo.db)
emit_component_listing(TOC.txt)
```

### Cassandra 5.0 writer touchpoints

- Writer orchestrator builds `Data/Index/Summary/Statistics` and emits `TOC.txt`: see `SSTableWriter` and format-specific `BigTableWriter` (pinned below).

For a concrete implementation walkthrough of a writer, see Appendix C.

### Tiny flush artifact (trimmed, from `test_basic`)

`TOC.txt` shows the emitted components for one table in `test_basic/simple_table`:

```text
Data.db
Statistics.db
Digest.crc32
TOC.txt
CompressionInfo.db
Filter.db
Index.db
Summary.db
```

### Sidebar: Version Differences (3.x/4.x)

- 3.x/4.x embedded more assumptions in component headers and older compression metadata; 5.0’s BTI family refines component boundaries. Side effects for this chapter are minor; see detailed differences in Chapters 5, 6, and 9 where formats matter most.

### Key Takeaways
- Flush writes immutable components alongside a `TOC.txt` enumerating what was produced.
- `Data.db` is primary; `Index/Summary/Filter` accelerate lookups; `Statistics` and `CompressionInfo` guide reads and maintenance.
- Writers operate in a stable order: write data, derive indexes/summaries, then metadata.

### References
- Cassandra 5.0.0:
  - `SSTableWriter`: [org.apache.cassandra.io.sstable.SSTableWriter](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableWriter.java)
  - `BigTableWriter`: [org.apache.cassandra.io.sstable.format.big.BigTableWriter](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableWriter.java)
  - Memtables: [org.apache.cassandra.db.memtable](https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/db/memtable)
  
For implementation details, see Appendix C.
