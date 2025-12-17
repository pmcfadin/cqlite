## Index.db and Summary.db

This chapter explains the partition index (`Index.db`) and the sampled summary (`Summary.db`), and how they guide binary search and seeks into `Data.db`. It also outlines token-range iteration behavior.

### In this chapter you will learn
- The structure of index entries and promoted index behavior
- How summary sampling accelerates lookups
- How binary search is guided from summary to index to data
- How token range iteration interacts with the index

## Partition Index Structure

`Index.db` primarily stores partition key digests and, depending on format, may include offsets and sizes.

Annotated example (BIG, one entry):
```
00000000: 0010 6b88 bf20 a251 11f0 a3fe f1a5 5138  |..k.. .Q......Q8|
00000010: 3fb9 00                                   |?. .             |
```
- `0010` → marker (partition key digest follows)
- `6b88…3fb9` → 16-byte digest
- `00` → start of length/offset field (variable-length; see reader)

Annotated example (length-prefixed variant — BIG, one entry):
```
00000000: 001a 0010 37ac 9f53 bd8e 4da5 a41a 240f  |....7..S..M...$.
00000010: 8f5a 6cfd 0000 0480 004f 88               |.Zl......O.     |
```
- `001a` → entry length (26 bytes)
- `0010` → marker (partition key digest follows)
- `37ac…6cfd` → 16-byte digest
- `0000 0480 004f 88` → variable-length fields: data offset (and optional size/payload per format)

Tiny side-by-side comparison (first 12–16 bytes):
```
// No length prefix (legacy/BIG):
0010 6b88 bf20 a251 11f0 a3fe f1a5 | 0010 + 16B digest ...

// With 2-byte length prefix (some 5.0 BIG tables):
001a 0010 37ac 9f53 bd8e 4da5 a41a | 001a + 0010 + 16B digest ...
```

Variant gating (BIG):

Pseudo-structs per variant (field order, big-endian for fixed-width):

```
// No length prefix (legacy/BIG variant)
u16 marker = 0x0010
u128 partition_key_digest
varint data_offset
[optional promoted-index payload]

// With 2-byte length prefix (some 5.0 BIG tables)
u16 entry_length
u16 marker = 0x0010
u128 partition_key_digest
varint data_offset
[optional promoted-index payload]
```

Gate detection is handled by the BIG reader; consult `org.apache.cassandra.io.sstable.format.big.BigTableReader` and `RowIndexEntry` for exact parsing. Implementations must handle both variants by detecting an initial length field that precedes the `0x0010` marker.

Digest and collisions:
- Partition key digest is 16 bytes; derived from the partition key via Cassandra’s partitioner (e.g., Murmur3Partitioner). Treat digest as an index key; on match, validate by reading the `Data.db` key to guard against extremely rare collisions.

Promoted index payload (BIG):
- Emitted for wide partitions. The payload follows the offset field when present; readers identify it by entry payload length (length-prefixed variant) or by probing entry structure (non-prefixed). See `RowIndexEntry` for exact fields.

Mini-parser (variant-tolerant) — conceptual:
```text
pos = 0
prefix = read_u16_be()
if prefix == 0x0010:
  // non-length-prefixed variant
  marker = prefix
else:
  entry_len = prefix
  marker = read_u16_be()
  assert(marker == 0x0010)

digest = read_16_bytes()
data_offset = read_vint_u64()
payload_len = (entry_len - bytes_consumed_so_far) if entry_len else 0
promoted_index = read_bytes(payload_len) if payload_len > 0
```

Promoted index (BIG): emitted for wide partitions to accelerate within-partition seeks. Readers detect presence via entry payload structure and fall back to scan when absent. See `org.apache.cassandra.io.sstable.format.big` reader/writer for details.

## Summary.db Format

`Summary.db` samples index entries for faster navigation. It contains a subset of partition keys at configurable intervals (default: every 128 partitions).

### File Structure

```
+------------------------+
| Header (24 bytes)      |
+------------------------+
| Offset Table (LE u32[])| <- Little-endian!
+------------------------+
| Entry Data             |
|   key + position (BE)  |
+------------------------+
| First Key (serialized) |
+------------------------+
| Last Key (serialized)  |
+------------------------+
```

### Header Format (24 bytes, big-endian)

```c
struct summary_header {
    be32 min_index_interval;      // Minimum partitions between entries (usually 128)
    be32 entries_count;           // Number of sampled entries
    be64 summary_entries_size;    // Size of offset table + entry data
    be32 sampling_level;          // Sampling level (1-128)
    be32 size_at_full_sampling;   // Entries at full sampling
};
```

Annotated example:
```
00000000: 00 00 00 80  // min_index_interval = 128
00000004: 00 00 00 08  // entries_count = 8
00000008: 00 00 00 00 00 00 00 e0  // summary_entries_size = 224
00000010: 00 00 00 80  // sampling_level = 128
00000014: 00 00 00 08  // size_at_full_sampling = 8
```

### Offset Table (Little-Endian!)

**Critical gotcha**: Unlike all other Cassandra formats, the offset table uses **little-endian** encoding.

```c
le32 offsets[entries_count];  // Offset to each entry within entry data section
```

Example for 3 entries:
```
00000018: 00 00 00 00  // Entry 0 at offset 0
0000001c: 18 00 00 00  // Entry 1 at offset 24 (LE!)
00000020: 30 00 00 00  // Entry 2 at offset 48 (LE!)
```

### Entry Format

Entries have **no length prefix**. Key boundaries are determined by offset differences.

```c
struct summary_entry {
    byte key[];        // Variable length - no prefix!
    be64 position;     // Position in Index.db file
};
```

Key length calculation:
```
key_length = next_offset - current_offset - 8  // Subtract 8 for position field
```

**Important**: Tokens are NOT stored in Summary.db entries. The `position` field points to a byte offset in Index.db, not a token.

### Serialized Keys (File End)

```c
struct serialized_key {
    be32 size;
    byte key[size];
};
```

First and last keys are serialized at the end of the file for quick boundary lookups.

## Partition Lookup Flow

1. **Summary.db lookup**: Binary search by partition key to find nearest sampled entry
2. **Index.db scan**: Read from `position` offset, scan forward to find exact partition
3. **Data.db seek**: Use offset from Index.db entry to read partition data

Note: Token-based iteration is not directly supported by Summary.db since tokens are not stored. Token iteration must compute tokens from partition keys.

### BTI Notes
- BTI’s indexing can alter how promoted index information is structured; the high-level flow (Summary → Index → Data) remains intact, but entry payloads differ. Ensure readers gate parsing on `Descriptor` format.

### Key Takeaways
- `Index.db` maps partition keys to positions; `Summary.db` accelerates binary search.
- Sampling reduces memory while preserving fast seeks.
- Token-range iteration combines summary jumps with index scans.

### References
- Cassandra 5.0.0:
  - `IndexSummary`: [org.apache.cassandra.io.sstable.IndexSummary](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/IndexSummary.java)
  - `SSTableReader`: [org.apache.cassandra.io.sstable.SSTableReader](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableReader.java)
  - BIG reader: [org/apache/cassandra/io/sstable/format/big/BigTableReader.java](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java)
  
For implementation details, see Appendix C.


