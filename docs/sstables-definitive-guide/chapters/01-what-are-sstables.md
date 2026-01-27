## What Are SSTables?

SSTables are immutable, sorted-string table files that persist Cassandra’s in-memory data structures to disk. They pair with memtables and the write-ahead log (WAL) in an LSM-tree design: writes land in memory and log, then flush to disk as SSTables. Immutability enables concurrent readers, simple compaction, and predictable IO.

### In this chapter you will learn
- The relationship between LSM-trees, memtables, WAL, and SSTables
- How SSTables participate in Cassandra’s read and write paths
- How formats evolved from big → mc/mm → BTI, and why
- Directory layout and naming conventions, including `TOC.txt`

## Overview

At a high level, Cassandra batches updates in a memtable and appends them to a WAL for durability. When a memtable fills or a flush is triggered, data is written out as an immutable SSTable on disk. Because SSTables are append-only artifacts with sorted partitions, reads can navigate quickly using auxiliary components (Bloom filter, index, summary) without rewriting data in place.

## Role in Cassandra Read/Write Path

- Write path (implementation view):
  - Client mutation → append to WAL → update memtable (TrieMemtable: byte-ordered prefix trie with shared prefixes)
  - Flush triggers `SSTableWriter` to build components:
    - Serialize rows into `Data.db` (optionally compressed in fixed-size chunks)
    - Emit partition digests and offsets to `Index.db`; build `Summary.db` samples
    - Construct `Filter.db` (Bloom) and accumulate `Statistics.db`
    - Write `CompressionInfo.db`, `Digest.crc32`, and `TOC.txt`
- Read path (point read, implementation view):
  - Compute partition key digest and check Bloom (`Filter.db`); negative → stop
  - Use `Summary.db` to narrow a region of `Index.db`; seek to exact index entry
  - Translate to `Data.db` position; read aligned to compression chunk boundaries and decompress just the needed bytes

See cross-links: Chapter 4 (flush pipeline) and Chapter 10 (read path decision tree). For a quick visual of component relationships, see the diagram referenced in Chapter 2 (`../diagrams/sstable-components.svg`, Mermaid source committed alongside).

## Evolution of Formats

- big (3.x/4.x): classic multi-file layout; partition index stores digests → data offsets; promoted index used for wide partitions
- mc/mm (4.x): iterative improvements on big; header/version flags and metadata evolve; tooling and defaults shift
- BTI (5.0): B-Tree/Trie Indexed family; improves lookup characteristics and index layout, reducing amplification for certain patterns while preserving the multi-component model

The on-disk component set remains recognizable across versions, but metadata and index structures evolve. Chapter 17 covers BTI in detail.

## Directory Layout and Naming

SSTable file names follow `{prefix}-{generation}-{format}-{Component}.db` with components enumerated in `TOC.txt`. Below is a tiny, real `TOC.txt` from `test_basic/simple_table`:

```1:9:/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6de93b70934a11f08d448925b7a9e804/nb-1-big-TOC.txt
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

- Component set is stable (Data/Index/Summary/Filter/Stats/CompressionInfo/TOC/Digest)
- Naming differs by format tag (`big`, `mc/mm`, `bti`); reader/writer internals improved in 5.0
- Some 3.x/4.x tools and flags changed defaults; this guide assumes 5.0 behavior unless noted

### Key Takeaways
- SSTables are immutable, sorted disk artifacts produced by memtable flushes
- Reads follow Bloom → Index → Summary → Data; writes never mutate existing SSTables
- The component set is consistent across versions; 5.0 advances internal layout with BTI
- `TOC.txt` is the single source of truth for which component files exist

### References

- Cassandra 5.0.0 (pinned):
  - `SSTableReader` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableReader.java`
  - `SSTableWriter` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableWriter.java`
  - `Descriptor` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/Descriptor.java`
- See also: Chapter 2 (components), Chapter 10 (read flow). For an implementation walkthrough, see Appendix C.


