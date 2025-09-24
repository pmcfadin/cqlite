# Appendix C — Reference Walkthroughs with Code

In this appendix you will learn:
- An end-to-end `Data.db` read path using Cassandra concepts and components
- Where Index and Summary readers participate in the read path
- How to correlate types to parsing behavior

## Walkthrough: Data.db point read (Cassandra semantics)

Conceptually, a point read follows: Bloom → Index → Summary → Data. Cassandra defines serialization via `SerializationHeader` and marshaller types, while `IndexSummary` and `RowIndexEntry` guide seeks.

Pinned upstream anchors (5.0.0):
- `SSTableReader` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableReader.java`
- `IndexSummary` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/IndexSummary.java`
- `RowIndexEntry` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/RowIndexEntry.java`
- `SerializationHeader` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/SerializationHeader.java`

Step-by-step (using `test-data/datasets/test_basic` as mental model):
1) Bloom filter: check partition key digest in `Filter.db` (negative → stop).
2) Summary: binary search in `Summary.db` over tokens to find nearest `index_offset`.
3) Index: scan `Index.db` from `index_offset` to find matching partition digest; read `RowIndexEntry`.
4) Data: seek to `Data.db` position from `RowIndexEntry`; read partition header, then row/cell payloads using `SerializationHeader`.

Tiny trimmed example (conceptual):
```
Summary entry: token=12345 index_offset=0x0000_2A10
Index entry: key_digest=ab..cd data_offset=0x0001_0030
Data read @0x0001_0030: partition header + row [len=0x12] ...
```

Row and cell serialization are defined by `rows.*` and the marshaller types (`db.marshal.*`).

Index and Summary provide navigation primitives: `IndexSummary` samples `RowIndexEntry` positions for efficient seeks into `Index.db` and then `Data.db`.

## Key Takeaways
- Schema-aware decoding eliminates guesswork; comparators come from the schema.
- Index and Summary readers narrow reads before hitting `Data.db` bytes.
- Validate with small, trimmed output from real SSTables (e.g., `sstabledump | head -n 10`).

## References
- Cassandra 5.0: `SSTableReader`, `IndexSummary`, `RowIndexEntry`, `SerializationHeader` (see Source Map)

