# Appendix E — Glossary

Concise definitions; cross-link to first use in the chapters.

- SSTable: Immutable on-disk table used by Cassandra to store partitions and rows.
- TOC (`TOC.txt`): Text file listing the set of components (Data/Index/Summary/etc.).
- `Data.db`: Primary data file containing serialized partitions, rows, and cells.
- `Index.db`: Partition index linking keys to `Data.db` offsets.
- `Summary.db`: Sampled promoted index for faster index navigation.
- `Filter.db`: Bloom filter for negative lookups.
- `Statistics.db`: Table-level metadata including histograms and timestamps.
- `CompressionInfo.db`: Chunk sizes, offsets, and checksums for compressed `Data.db`.
- Digest (`Digest.crc32`): File CRC for integrity verification.
- VInt: Variable-length integer encoding used for lengths and counters.
- ZigZag: Encoding mapping signed integers to small unsigned values.
- UDT: User-defined type in CQL.
- SAI: Storage-Attached Index; secondary indexing subsystem (incl. vector).
- BTI: B-Tree/Trie indexed SSTable format family in Cassandra 5.x.
  See `17-bti-formats.md`.

- TrieMemtable: An **opt-in** memtable in Cassandra 5.0, using a byte-ordered prefix trie
  (`InMemoryTrie`) for efficient memory usage and reduced GC pressure. Stores partitions using
  `ByteComparable` keys with prefix sharing; sharded across CPU cores for write concurrency.
  The **default** memtable in 5.0 is `SkipListMemtable` (`MemtableParams.java:99`); opt in via
  `memtable: { class: TrieMemtable }` in the table schema or `cassandra.yaml`.
  See `04-from-cql-to-disk.md` and
  [TrieMemtable.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/memtable/TrieMemtable.java).

- big format: Legacy SSTable format family (pre-BTI) with classic `BigTableReader`/`Writer`.
  See `02-anatomy-of-an-sstable.md` and `17-bti-formats.md` for contrasts.

- LCS/STCS/TWCS: Compaction strategies. See `15-compaction-strategies.md`.

References:
- Cassandra 5.0 source packages: see `references/source-map.md`.

