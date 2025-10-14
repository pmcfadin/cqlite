## How to find a row by key (flow card)

This one-pager stitches Bloom → (Summary → Index) → Data for BIG vs BTI, with byte-level seeks and failure paths.

### Overview
- Bloom check (Filter.db) → early negative exit when absent
- Summary jump (Summary.db) → locate `index_offset`
- Index scan (Index.db or BTI index) → locate partition digest, get `data_offset`
- Data read (Data.db) → parse partition + rows via `SerializationHeader`

### BIG (legacy/newbig) flow
1) Bloom
- Load `Filter.db` if present; `might_contain(partition_digest)`.
- Negative → stop. Positive → continue.

2) Summary
- Binary search tokens to find nearest `index_offset`.

3) Index
- From `index_offset`, parse entries.
- Variant gate:
  - If first u16 == `0x0010`, non-length-prefixed: read marker → digest → varint `data_offset`.
  - Else treat first u16 as `entry_length`, then assert next u16 == `0x0010`.
- Promoted index payload may be present for wide partitions; fall back to scan when absent.

4) Data
- Seek to `data_offset` in `Data.db`.
- NB format note: `Data.db` has no header; chunk boundaries come from `CompressionInfo.db`.
- Parse partition header and rows using `SerializationHeader`.

Failure/negative path
- If Index digest != target → advance to next entry in Index page/region.
- If `size == 0` in C5.0 → sequential scan from `data_offset` until next partition.

### BTI (5.0) flow (high level)
- Bloom: same semantics.
- Summary: token-sorted summary guides to BTI index structures.
- Index: BTI-specific reader locates partition; payload layout differs from BIG.
- Data: seek to `data_offset`; parse via `SerializationHeader`.

### Byte-level seek checklist
- Summary → `index_offset`: verify within file bounds and monotonically increasing tokens.
- Index → `data_offset`: validate marker `0x0010` (BIG) and digest length = 16 bytes.
- Data seek: ensure `data_offset` points within a valid chunk boundary (NB: use `CompressionInfo.db`).

### Tiny hex anchors
```text
// BIG, non-length-prefixed (index excerpt)
00000000: 0010 6b88 bf20 a251 11f0 a3fe f1a5 5138 |..k.. .Q......Q8|

// BIG, length-prefixed (index excerpt)
00000000: 001a 0010 37ac 9f53 bd8e 4da5 a41a 240f |....7..S..M...$.
```

### References
- `IndexSummary`: https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/IndexSummary.java
- BIG reader: https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java
- `SerializationHeader`: https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/SerializationHeader.java
- BTI format: `org.apache.cassandra.io.sstable.format.bti`
