---
title: "Appendix C — Reference Walkthroughs with Code"
description: "In this appendix you will..."
sidebar:
  label: "Appendix C — Reference Walkthroughs with Code"
  order: 103
---

In this appendix you will learn:
- An end-to-end `Data.db` read path using Cassandra concepts and components
- Where Index and Summary readers participate in the read path
- How to correlate types to parsing behavior

## Walkthrough: Data.db point read (Cassandra semantics)

Conceptually, a point read follows: Bloom → Index → Summary → Data. Cassandra defines serialization via `SerializationHeader` and marshaller types, while `IndexSummary` and `RowIndexEntry` guide seeks.

Pinned upstream anchors (cassandra-5.0.8):
- `SSTableReader` — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/SSTableReader.java`
- `IndexSummary` — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/indexsummary/IndexSummary.java`
- `RowIndexEntry` — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/RowIndexEntry.java`
- `SerializationHeader` — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java`

Step-by-step (using `test-data/datasets/test_basic` as mental model):
1) Bloom filter: check partition key in `Filter.db` (negative → stop).
2) Summary: binary search in `Summary.db` over tokens to find nearest `index_offset`.
3) Index: scan `Index.db` from `index_offset` to find matching partition key (u16-prefixed raw bytes); read `RowIndexEntry`.
4) Data: seek to `Data.db` position from `RowIndexEntry`; read partition header, then row/cell payloads using `SerializationHeader`.

Tiny trimmed example (conceptual):
```
Summary entry: token=12345 index_offset=0x0000_2A10
Index entry: key=[u16 len][raw key bytes] data_position=vint
Data read @data_position: partition header + row [len=0x12] ...
```

Row and cell serialization are defined by `rows.*` and the marshaller types (`db.marshal.*`).

Index and Summary provide navigation primitives: `IndexSummary` samples `RowIndexEntry` positions for efficient seeks into `Index.db` and then `Data.db`.

## Walkthrough: NB chunk CRC verification (unit-testable)

NB `Data.db` has no global header. CRC32 is appended inline to each compressed chunk in `Data.db`. `CompressionInfo.db` provides chunk start offsets only — no CRCs.

Example (real test file `event_store`):
```
chunk 0: start=0x0000 comp_len=7729 expected=0x001daf10 computed=0x001daf10 match=true
chunk 1: start=0x1e35 comp_len=2666 expected=0x657f7155 computed=0x657f7155 match=true
```
Compute CRC32 over compressed bytes only; compare to trailing 4-byte big-endian u32 appended inline after each chunk in `Data.db`.

## Walkthrough: BIG Index.db entry parse

`BigTableWriter` writes each BIG `Index.db` entry as: a u16 big-endian key length, the raw key bytes, then a vint data position (followed by optional promoted index). There is no marker byte and no MD5 digest — the 2-byte field is the key length.

Tiny hex → parsed struct → assertion.

Input (non-promoted):
```
0010 6b88 bf20 a251 11f0 a3fe f1a5 5138 3fb9 00
```
Parse:
- key_length = 0x0010 (= 16 bytes)
- key_bytes = `6b88 bf20 a251 11f0 a3fe f1a5 5138 3fb9` (16 raw bytes)
- data_position = vint (byte `00`, = position 0)

Assertion: `key_length == key_bytes.len; data_position >= 0`.

Source: `BigTableWriter.java:277` — `ByteBufferUtil.writeWithShortLength(key.getKey(), writer)` writes u16 length then raw bytes; `RowIndexEntry.serialize` writes the vint position.

## Walkthrough: Bloom on-disk decode (Filter.db)

Given bytes:
```
0000 0005 0000 0002 a4c0 e2a8 02a2 a1b3 77
```
Decode:
- `hashCount = 5` (4 bytes: `00 00 00 05`) — number of hash functions (k)
- `wordCount = 2` (4 bytes: `00 00 00 02`) — 64-bit word count of bitset (= 16 raw bytes follow)
- bitset payload = next `wordCount × 8` bytes

Bit packing: new format (NB) copies raw memory bytes in native byte order. Old format writes each 64-bit word little-endian (LSB first). Neither format uses big-endian words. Consult `BloomFilterSerializer` and `OffHeapBitSet` for serialization details.

Source: `BloomFilterSerializer.java:53-54` (hashCount then bitset.serialize); `OffHeapBitSet.java:117` (wordCount = bytes/8, then raw copy).

## Key Takeaways
- Schema-aware decoding eliminates guesswork; comparators come from the schema.
- Index and Summary readers narrow reads before hitting `Data.db` bytes.
- BIG `Index.db` entries use u16 BE key length + raw key bytes + vint position; no digest field exists.
- Per-chunk CRC32 (NB `Data.db`) validates each compressed chunk via 4-byte big-endian u32 appended inline after the compressed bytes.
- Use `crc32fast::hash()` for efficient checksum computation.
- Validate with small, trimmed output from real SSTables (e.g., `sstabledump | head -n 10`).

## References
- Cassandra 5.0.8: `SSTableReader`, `IndexSummary`, `RowIndexEntry`, `SerializationHeader` (see Source Map)
- CRC32 library: `crc32fast` crate (https://docs.rs/crc32fast/)

## CLI examples

Minimal commands against a sample generation:

```bash
# Inspect statistics and Bloom FPR target
sstablemetadata nb-1-big-Statistics.db

# Dump index/summary entries (trimmed)
sstabledump nb-1-big-Index.db | head -n 50

# Verify digest over components (use Cassandra's verifier or equivalent)
sstableverify /var/lib/cassandra/data/ks/table-uuid/
```

Align outputs with the toy `test_basic/simple_table` used in examples elsewhere in this guide.
