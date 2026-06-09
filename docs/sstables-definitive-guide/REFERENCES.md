# References and Source Map

## Version Baseline and Pinning Policy
- Baseline: Apache Cassandra 5.0 (not 5.1). Older versions (3.x/4.x) are cited only for historical deltas and in sidebars.
- Pinning: Prefer permalinks to the `cassandra-5.0.0` tag. When stability matters beyond tags, use commit-SHA permalinks.
- Callouts: When behavior differs from 3.x/4.x, add an O’Reilly-style sidebar in the chapter and link the older code/doc.

## Apache Cassandra (Core) — 5.0 Namespace Map
- Format and components: `org.apache.cassandra.io.sstable.format.*`, `...format.big.*`, `...format.bti.*`
- Index summary: `org.apache.cassandra.io.sstable.IndexSummary`
- Read path: `org.apache.cassandra.db.SinglePartitionReadCommand`, `org.apache.cassandra.db.partitions.UnfilteredPartitionIterator`
- Write/flush path: `org.apache.cassandra.db.memtable.*`, `org.apache.cassandra.io.sstable.SSTableWriter`
- Bloom filter: `org.apache.cassandra.utils.bloom.*`
- Compression: `org.apache.cassandra.io.compress.*`, `org.apache.cassandra.io.sstable.metadata.CompressionMetadata`
- Tombstones and rows: `org.apache.cassandra.db.rows.*`, `org.apache.cassandra.db.filter.*`
- Compaction: `org.apache.cassandra.db.compaction.*` (STCS/LCS/TWCS; UCS sidebar)
- Statistics: `org.apache.cassandra.io.sstable.metadata.*`
- BTI: `org.apache.cassandra.io.sstable.format.bti.*`

## Storage-Attached Indexes (SAI)
- Package root: `org.apache.cassandra.index.sai.*` (segment builders, on-disk formats, query path)
- Include vector index components where relevant (segments, readers/writers, query ops)

## Tools and Utilities
- `org.apache.cassandra.tools.*` — `sstabledump`, `sstablemetadata`, `sstablescrub`

## CQLite Code Map (for examples in this guide)
- SSTable components: `cqlite-core/src/storage/sstable/`
  - Reader: `reader.rs` (`SSTableReader`, the single read path; opt-in `BlockSource::Mapped` mmap), `schema_aware_reader.rs`
  - Index: `index.rs`, `index_reader.rs`, `summary_reader.rs`
  - Filter: `bloom.rs`
  - Compression: `compression.rs`, `compression_info.rs`, `chunk_decompressor.rs`
  - Validation: `validation.rs`
  - BTI: `bti/`
- Parser encodings: `cqlite-core/src/parser/vint.rs`, `.../factory.rs`, `.../visitor.rs`
- Schema mapping: `cqlite-core/src/schema/`
- CLI usages: `cqlite-cli/src/` and tests under `cqlite-cli/tests/`

## Selected External Reading
- Cassandra 5.0 documentation (SSTables, compaction, SAI)
- Datastax engineering blogs on LSM, compaction, tombstones
- ScyllaDB engineering posts for comparative context

## Referencing Guidance
- Prefer linking to `cassandra-5.0.0` tag; include class and package names in the text.
- Use short code excerpts only when necessary; otherwise point to permalinks.
- Keep a per-chapter mini bibliography at the end of each file.

## Pinned Upstream Links (Cassandra 5.0.0)

- SSTable core:
  - `SSTableReader` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableReader.java`
  - `SSTableWriter` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableWriter.java`
  - `BigTableReader` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java`
  - `BigTableWriter` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableWriter.java`
- Components:
  - `IndexSummary` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/IndexSummary.java`
  - `StatsMetadata` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java`
  - `CompressionMetadata` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java`
  - `BloomFilter` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/utils/bloom/BloomFilter.java`
  - `Descriptor` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/Descriptor.java`
- SAI (Storage-Attached Indexes):
  - Root: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai`
  - Disk formats: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai/disk`
  - Query: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai/query`

## Source Map

For a component-by-component mapping to classes and permalinks, see `docs/sstables-definitive-guide/references/source-map.md`.
