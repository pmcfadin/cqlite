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

## Walkthrough: NB chunk CRC verification (unit-testable)

NB `Data.db` has no global header. Validation is per compressed chunk using `CompressionInfo.db`:

Example (real test file `event_store`):
```
chunk 0: start=0x0000 comp_len=7729 expected=0x001daf10 computed=0x001daf10 match=true
chunk 1: start=0x1e35 comp_len=2666 expected=0x657f7155 computed=0x657f7155 match=true
```
Compute CRC32 over compressed bytes only; compare to trailing 4-byte big-endian u32 after each chunk.

## Walkthrough: Header CRC32 Validation (Legacy/BIG only)

Some legacy/BIG family components may prepend a 4-byte CRC32 checksum to protect header integrity. NB format does not use header CRCs. This walkthrough demonstrates detection and validation for BIG artifacts only. Do not apply this logic to NB `Data.db`.

### Schematic Example (legacy/BIG component with header CRC)

```
[4 bytes: CRC32 prefix] [header bytes starting with magic number]
```

High-level validation sketch:

```rust
let first4 = read_be_u32()?; // potential magic or CRC
if CassandraVersion::from_magic_number(first4).is_none() {
    let expected_crc = first4;
    let header = read_header_bytes()?; // from offset 4
    let computed = crc32fast::hash(&header);
    if computed != expected_crc { return Err(Error::HeaderCorruption); }
    parse_header(&header)?;
} else {
    // No prefix; parse header from offset 0
    parse_header_from_offset_0()?;
}
```

Scope note: NB `Data.db` begins with compressed chunk bytes and uses trailing per‑chunk CRCs; see the NB chunk CRC walkthrough above for the canonical NB integrity path.

## Walkthrough: BIG Index.db entry parse (both variants)

Tiny hex → parsed struct → assertion.

Input A (non-length-prefixed):
```
0010 6b88 bf20 a251 11f0 a3fe f1a5 5138 3fb9 00
```
Parse:
- marker = 0x0010
- digest = 16 bytes (0x6b88…0x3fb9)
- data_offset = vint (next bytes)

Assertion: marker == 0x0010; digest.len == 16; offset >= 0.

Input B (length-prefixed):
```
001a 0010 37ac 9f53 bd8e 4da5 a41a 240f 8f5a 6cfd 00 00 04 80 00 4f 88
```
Parse:
- entry_len = 0x001a
- marker = 0x0010
- digest = 16 bytes (0x37ac…0x6cfd)
- data_offset = vint (subsequent bytes)

Assertion: entry_len == bytes_consumed; marker == 0x0010; digest.len == 16.

## Walkthrough: Bloom on-disk decode (Filter.db)

Given bytes:
```
0000 0005 0000 0002 a4c0 e2a8 02a2 a1b3 77
```
Decode:
- bitset_length_bytes = 5
- k = 2
- payload = 5 bytes

Bit packing: bits are LSB-first within each byte; consult `BloomFilter` writer/reader for word ordering (big-endian u64 words when serialized as words).

## Key Takeaways
- Schema-aware decoding eliminates guesswork; comparators come from the schema.
- Index and Summary readers narrow reads before hitting `Data.db` bytes.
- **Header CRC32 validation** (Legacy/BIG only) detects corruption before parsing where present.
- Use `crc32fast::hash()` for efficient checksum computation.
- Validate with small, trimmed output from real SSTables (e.g., `sstabledump | head -n 10`).

## References
- Cassandra 5.0: `SSTableReader`, `IndexSummary`, `RowIndexEntry`, `SerializationHeader` (see Source Map)
- CQLite: Header CRC32 validation in `cqlite-core/src/storage/sstable/reader/header.rs`
- CRC32 library: `crc32fast` crate (https://docs.rs/crc32fast/)

## CLI examples

Minimal commands against a sample generation:

```bash
# Inspect statistics and Bloom FPR target
sstablemetadata nb-1-big-Statistics.db

# Dump index/summary entries (trimmed)
sstabledump nb-1-big-Index.db | head -n 50

# Verify digest over components (use Cassandra’s verifier or equivalent)
sstableverify /var/lib/cassandra/data/ks/table-uuid/
```

Align outputs with the toy `test_basic/simple_table` used in examples elsewhere in this guide.

