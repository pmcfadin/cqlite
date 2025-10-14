## Versioning and Format Matrix (quick reference)

A one-page map of Cassandra releases to SSTable version tags and on-disk deltas. Use this to gate parsing logic by `Descriptor` and to find upstream anchors.

| Release | SSTable tag(s) | Notable on-disk deltas | Gates/notes | Anchors |
|---|---|---|---|---|
| 3.x | `mc` (and earlier series) | Legacy BIG layouts; size-prefixed Snappy in places; no NB | BIG family only | `Descriptor`, `BigTableReader` |
| 4.x | `md`/`me` (varies by minor) | BIG refinements; summary/token changes; no NB | Gate by tag; consult per-release notes | `Descriptor`, `IndexSummary` |
| 5.0 | `V5_0NewBig` (NB coexists with BIG) | NB: header-less `Data.db`, trailing per-chunk CRCs, CompressionInfo-driven chunk map; Index entry variant with optional 2-byte length prefix in BIG | Gate by `Descriptor` format (`nb` vs `big`); detect BIG index entry variant by prefix | `SSTableReader`, `RowIndexEntry`, `CompressionMetadata` |

### How to use this matrix
- Read `Descriptor` to determine format/tag; gate parsing paths accordingly.
- For BIG Index entries, handle both variants (non-prefixed and length-prefixed); see Chapter 06.
- For NB `Data.db`, ignore header/magic and validate trailing per-chunk CRCs using `CompressionInfo.db`.

References (pinned to 5.0.0 where relevant):
- `Descriptor`: https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/Descriptor.java
- `SSTableReader`: https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableReader.java
- `RowIndexEntry` (BIG): https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/RowIndexEntry.java
- `CompressionMetadata`: https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/compress/CompressionMetadata.java
