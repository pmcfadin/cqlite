# Source Map — Cassandra 5.0.0 (Pinned)

This source map pins key classes and packages in Apache Cassandra 5.0.0 to the SSTable components and related topics covered in this guide. Links are permalinks to the `cassandra-5.0.0` tag. Prefer these when citing upstream code. When multiple files participate, we link the package directory and call out representative classes.

Notes:
- Component names follow Cassandra’s multi-file layout: `Data.db`, `Index.db`, `Summary.db`, `Filter.db`, `Statistics.db`, `CompressionInfo.db`, `TOC.txt`, `Digest.crc32`.
- Format families include `big` and BTI.
- Storage-Attached Indexes (SAI) includes vector indexing in 5.0.

---

## Core SSTable Components

### Data.db (read/write path)

| Component/Topic | Cassandra class/package | Permalink (5.0.0) | Notes |
|---|---|---|---|
| Reader (big format) | `org.apache.cassandra.io.sstable.format.big.BigTableReader` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java` | Reads Data.db and related components |
| Writer (big format) | `org.apache.cassandra.io.sstable.format.big.BigTableWriter` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableWriter.java` | Builds Data/Index/Summary/Stats/Filter/CompressionInfo |
| Generic reader base | `org.apache.cassandra.io.sstable.SSTableReader` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableReader.java` | Format-agnostic reader orchestration |
| Generic writer base | `org.apache.cassandra.io.sstable.SSTableWriter` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableWriter.java` | Format-agnostic writer orchestration |
| Serialization header | `org.apache.cassandra.db.SerializationHeader` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/SerializationHeader.java` | Schema-driven on-disk encodings |
| Row/partition structures | `org.apache.cassandra.db.rows.*` | `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/db/rows` | Unfiltered rows, deletions, tombstones |

### Index.db (partition index)

| Component/Topic | Cassandra class/package | Permalink (5.0.0) | Notes |
|---|---|---|---|
| Index entry (big) | `org.apache.cassandra.io.sstable.format.big.RowIndexEntry` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/RowIndexEntry.java` | Per-partition index metadata/positions |
| Index file writer (big) | `org.apache.cassandra.io.sstable.format.big.BigTableWriter` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableWriter.java` | Creates `Index.db` during flush |
| Reader integration | `org.apache.cassandra.io.sstable.SSTableReader` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableReader.java` | Seeks via index to Data.db |

### Summary.db (promoted index / sampling)

| Component/Topic | Cassandra class/package | Permalink (5.0.0) | Notes |
|---|---|---|---|
| Summary | `org.apache.cassandra.io.sstable.IndexSummary` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/IndexSummary.java` | Sampled entries enabling faster navigation |
| Summary builder | `org.apache.cassandra.io.sstable.IndexSummaryBuilder` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/IndexSummaryBuilder.java` | Builds `Summary.db` during flush |

### Filter.db (Bloom filter)

| Component/Topic | Cassandra class/package | Permalink (5.0.0) | Notes |
|---|---|---|---|
| Bloom filter (API) | `org.apache.cassandra.utils.bloom.BloomFilter` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/utils/bloom/BloomFilter.java` | In-memory Bloom filter structure |
| Bloom calculations | `org.apache.cassandra.utils.bloom.BloomCalculations` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/utils/bloom/BloomCalculations.java` | FPR/bitset sizing |
| Bloom factory | `org.apache.cassandra.utils.bloom.FilterFactory` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/utils/bloom/FilterFactory.java` | Creates Bloom instances |
| Bitset impl | `org.apache.cassandra.utils.obs.OffHeapBitSet` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/utils/obs/OffHeapBitSet.java` | Off-heap bit set used by filters |

### Statistics.db (table-level statistics)

| Component/Topic | Cassandra class/package | Permalink (5.0.0) | Notes |
|---|---|---|---|
| Stats metadata | `org.apache.cassandra.io.sstable.metadata.StatsMetadata` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java` | Histograms, timestamps, repair/level info |
| Collector | `org.apache.cassandra.io.sstable.metadata.MetadataCollector` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java` | Gathers stats during flush |
| Serializer | `org.apache.cassandra.io.sstable.metadata.MetadataSerializer` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/metadata/MetadataSerializer.java` | Writes/reads `Statistics.db` |

### CompressionInfo.db (chunk metadata)

| Component/Topic | Cassandra class/package | Permalink (5.0.0) | Notes |
|---|---|---|---|
| Compression metadata | `org.apache.cassandra.io.compress.CompressionMetadata` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java` | Offsets map, chunk sizes, checksums |
| Chunk reader | `org.apache.cassandra.io.compress.CompressedChunkReader` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/compress/CompressedChunkReader.java` | Reads compressed chunks |
| Parameters | `org.apache.cassandra.io.compress.CompressionParameters` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/compress/CompressionParameters.java` | Algorithm/option configuration |

### Digest.crc32 and Data Integrity

| Component/Topic | Cassandra class/package | Permalink (5.0.0) | Notes |
|---|---|---|---|
| Data integrity (per-chunk) | `org.apache.cassandra.io.util.DataIntegrityMetadata` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/util/DataIntegrityMetadata.java` | CRC handling for chunks |
| CRC implementation (pure Java) | `org.apache.cassandra.utils.PureJavaCrc32` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/utils/PureJavaCrc32.java` | CRC32 implementation used in IO |

### TOC.txt and Component Enumeration

| Component/Topic | Cassandra class/package | Permalink (5.0.0) | Notes |
|---|---|---|---|
| Descriptor and components | `org.apache.cassandra.io.sstable.Descriptor` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/Descriptor.java` | Naming, component paths |
| Writer (TOC emission) | `org.apache.cassandra.io.sstable.SSTableWriter` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableWriter.java` | Writes `TOC.txt` listing components |

---

## SSTable Format Families

### `big` format

- Package: `org.apache.cassandra.io.sstable.format.big`
  - Directory permalink: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big`
  - Representative classes: `BigTableReader`, `BigTableWriter`, `RowIndexEntry`

### BTI format (B-Tree/Trie Indexed)

- Package: `org.apache.cassandra.io.sstable.format.bti`
  - Directory permalink: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/bti`
  - Notes: BTI format family classes and readers/writers live here (Cassandra 5.0)

---

## Storage-Attached Indexes (SAI)

- Root package: `org.apache.cassandra.index.sai`
  - Directory permalink: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai`
- Disk formats and segments: `org.apache.cassandra.index.sai.disk`
  - Directory permalink: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai/disk`
- Query path: `org.apache.cassandra.index.sai.query`
  - Directory permalink: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai/query`
- Vector indexing (5.0): `org.apache.cassandra.index.sai.disk.v1` (vector classes live under this hierarchy)
  - Directory permalink: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai/disk/v1`

### SAI Vector — Key Classes

| Topic | Cassandra class | Permalink (5.0.0) | Notes |
|---|---|---|---|
| Vector CQL type | `org.apache.cassandra.db.marshal.VectorType` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/marshal/VectorType.java` | Type definition for `vector<elem, n>` |
| SAI index (entry point) | `org.apache.cassandra.index.sai.StorageAttachedIndex` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai/StorageAttachedIndex.java` | Index implementation used for SAI (incl. vector) |
| SAI searcher | `org.apache.cassandra.index.sai.StorageAttachedIndexSearcher` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/index/sai/StorageAttachedIndexSearcher.java` | Query/search integration |

---

## Tools (for examples and verification)

| Tool | Cassandra class | Permalink (5.0.0) | Notes |
|---|---|---|---|
| sstabledump | `org.apache.cassandra.tools.SSTableDump` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/tools/SSTableDump.java` | Dump rows and metadata |
| sstablemetadata | `org.apache.cassandra.tools.SSTableMetadata` | `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/tools/SSTableMetadata.java` | Show `Statistics.db` contents |

---

## CQLite Code Touchpoints (for cross-references)

These modules are used in chapter code references to illustrate how CQLite consumes the above components.

- `cqlite-core/src/storage/sstable/reader.rs`
- `cqlite-core/src/storage/sstable/index_reader.rs`
- `cqlite-core/src/storage/sstable/summary_reader.rs`
- `cqlite-core/src/storage/sstable/bloom.rs`
- `cqlite-core/src/storage/sstable/compression_info.rs`
- `cqlite-core/src/storage/sstable/chunk_decompressor.rs`
- `cqlite-core/src/storage/sstable/metadata` (if present) and `.../statistics`

---

Key policy: All upstream citations should target the permalinks above (5.0.0 tag) unless a deeper file/class link is explicitly needed.


