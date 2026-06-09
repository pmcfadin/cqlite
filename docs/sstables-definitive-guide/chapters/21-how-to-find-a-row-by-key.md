## How to find a row by key (flow card)

This one-pager stitches Bloom → (Summary → Index) → Data for BIG vs BTI, with byte-level
seeks and failure paths.

### Overview
- Bloom check (`Filter.db`) → early negative exit when absent
- BIG: Summary jump (`Summary.db`) → locate `index_offset`; **BTI has no `Summary.db`**
- Index scan (`Index.db` for BIG, `Partitions.db` trie for BTI) → get `data_offset`
- Data read (`Data.db`) → parse partition + rows via `SerializationHeader`

### BIG (legacy/newbig) flow

**1) Bloom**
- Load `Filter.db` if present; `might_contain(decorated_key)`.
- Negative → stop. Positive → continue.

**2) Summary**
- Binary search **partition keys** (decorated-key order, token-first byte comparison) to find
  the nearest `index_offset` into `Index.db`.
- Source: `IndexSummary.binarySearch(PartitionPosition key)` —
  [`IndexSummary.java#L127-L152`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/indexsummary/IndexSummary.java#L127)
- Note: the binary search operates on **decorated keys**, not raw token values.

**3) Index**
- From `index_offset`, parse entries sequentially until the target is found or passed.
- **Entry format** (always length-prefixed — there is no marker byte, no MD5 digest):
  ```
  u16     key_length        <- raw partition-key byte count (e.g. 0x0010 for a 16-byte UUID)
  bytes   raw_key_bytes     <- partition key, key_length bytes
  vint    data_offset       <- byte offset into Data.db
  vint    promoted_index_len<- length of promoted index block (0 = none)
  bytes   promoted_index    <- present only when promoted_index_len > 0
  ```
- Source: `BigTableReader.java:298` uses `ByteBufferUtil.readWithShortLength(in)` — the `u16`
  is **always** a key length.
  [`BigTableReader.java#L298-L325`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java#L298)
- **Compare** raw key bytes to target. On match → deserialize `RowIndexEntry`.
- On mismatch → call `RowIndexEntry.Serializer.skip(in, descriptor.version)` and advance to
  next entry.
  Source: `BigTableReader.java:349`

> **Note on `0x0010`:** The value `0x0010` is the key length for a 16-byte UUID partition key.
> It is **not** a fixed marker. An earlier version of this guide misidentified it as a format
> marker followed by an MD5 digest — that was incorrect (Issue #552). Every entry uses a plain
> `u16` key length prefix regardless of key type.

**4) Data**
- Seek to `data_offset` in `Data.db`.
- NB format note: `Data.db` has no standalone header; chunk boundaries come from
  `CompressionInfo.db`.
- Parse partition header and rows using `SerializationHeader`.

**Failure/negative path**
- If raw key != target → call `Serializer.skip()` and advance to the next Index.db entry.
- If no entry matches within the scan window → partition absent (false-positive bloom hit).

---

### BTI (5.0) exact-key lookup — full detail

BTI uses a trie partition index (`Partitions.db`) instead of the Summary → Index scan chain.
**There is no `Summary.db` and no `Index.db` in a BTI SSTable.**
Source: [`BtiFormat.java#L83-L102`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.java#L83)

**1) Bloom** — `BtiTableReader.isPresentInFilter(dk)`. Negative → stop.

**2) PartitionIndex trie (`Partitions.db`)** —
`partitionIndex.openReader().exactCandidate(dk)`. Returns `NOT_FOUND` → stop.
Otherwise returns `indexPos`:
- `indexPos >= 0` → position in `Rows.db`. Proceed to step 3a.
- `indexPos < 0` → `~indexPos` is a direct position in `Data.db` (small partition, no row
  index). Proceed to step 3b.

Source: `BtiTableReader.java:223-276`
[`BtiTableReader.java#L223`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiTableReader.java#L223)

**3a) `Rows.db` key verify + TrieIndexEntry** — Read `u16`-prefixed partition key at
`indexPos`; compare to target. Mismatch → stop. Deserialize `TrieIndexEntry`:
```
vint    dataFilePosition   <- offset of partition start in Data.db
vint    indexTrieRoot      <- delta-encoded relative to current file position
u32     rowIndexBlockCount
bytes   DeletionTime       <- 12 bytes (version "oa": mfda 8 B + ldt 4 B)
```
Source: [`TrieIndexEntry.java#L90-L118`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/TrieIndexEntry.java#L90)

**3b) `Data.db` direct** — Read `u16`-prefixed key from `~indexPos`; compare to target.
Mismatch → stop. Use `~indexPos` as `dataFilePosition`.

**4) Data seek** — Seek `dataFilePosition` in `Data.db`; parse with `SerializationHeader`.

> **Key cache absent in BTI:** `TrieIndexEntry.serializeForCache()` throws
> `AssertionError("BTI SSTables should not use key cache")`. All BTI lookups traverse the
> trie.
> Source: [`TrieIndexEntry.java#L68-L76`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/TrieIndexEntry.java#L68)

---

### Byte-level seek checklist

- **BIG — Summary → Index:** verify `index_offset` is within file bounds; summary entries are
  in decorated-key (token-first) order.
- **BIG — Index → Data:** the leading `u16` is the key byte length; compare raw key bytes to
  target. No marker, no digest. Call `Serializer.skip()` on mismatch.
- **BTI — Partitions.db → Data:** sign-bit of `indexPos` routes to `Rows.db` (>= 0) or
  directly to `Data.db` (< 0, decode as `~indexPos`).
- **Data seek (both formats):** ensure `data_offset` points within a valid chunk boundary
  (NB compressed: use `CompressionInfo.db`).

---

### Annotated hex — BIG Index.db entries

```text
// BIG Index.db -- 16-byte UUID partition key (key_length = 0x0010 = 16, NOT a marker)
00000000: 0010 1529 1a77 d739 4e73 8397 b787 442f  |...).w.9Ns....D/|
00000010: 3a1f 00 00 ...
//         ^^^^ ^^   ^^
//         uuid  |    promoted_index_len = 0
//               data_offset vint

// BIG Index.db -- 26-byte composite key (key_length = 0x001a = 26)
00000000: 001a 0010 37ac 9f53 bd8e 4da5 a41a 240f  |....7..S..M...$.|
//         ^^^^
//         key_length = 26  (not a marker!)
```

---

### References
- `IndexSummary` (5.0.8): [`org.apache.cassandra.io.sstable.indexsummary.IndexSummary`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/indexsummary/IndexSummary.java)
- BIG reader: [`BigTableReader.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java)
- BIG raw-key comparison (no digest): [`BigTableReader.java#L298-L325`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java#L298)
- BTI has no Summary.db: [`BtiFormat.java#L100-L108`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.java#L83)
- TrieIndexEntry on-disk format: [`TrieIndexEntry.java#L90-L118`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/TrieIndexEntry.java#L90)
- Key cache absent in BTI: [`TrieIndexEntry.java#L68-L76`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/TrieIndexEntry.java#L68)
- `SerializationHeader`: [`SerializationHeader.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java)
- BTI format spec: `org.apache.cassandra.io.sstable.format.bti` (see `BtiFormat.md` in-tree)
