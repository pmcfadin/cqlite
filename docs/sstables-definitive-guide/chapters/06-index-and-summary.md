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
- `00` → unsigned VInt offset (value 0, single byte for values 0-127)

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

**Critical: VInt Offset Encoding (NB Format)**

For NB format SSTables (Cassandra 5.0+), the `data_offset` uses **Cassandra VInt encoding**, NOT a length-prefixed byte array:

```
// NB format (Cassandra 5.0+) - DigestFormat
u16 marker = 0x0010
u128 partition_key_digest
vint data_offset          // VInt encoded, 1-9 bytes based on value magnitude
vint promoted_size        // VInt encoded size of following promoted index data
byte promoted_data[promoted_size]  // Promoted index (only if size > 0)
```

VInt encoding (from `DataInputPlus.java`):
- First byte's leading 1-bits indicate total byte count
- `0x00-0x7F`: 1 byte, value = byte itself
- `0x80-0xBF`: 2 bytes
- `0xC0-0xDF`: 3 bytes
- etc.

Example from `sensor_data` Index.db:
```
0x00       -> 1 byte  -> value = 0
0xb0 0x5d  -> 2 bytes -> value = 12381
0xc0 0x5f 0x11 -> 3 bytes -> value = 24337
```

**Important**: NB format offsets are **relative to the Data.db data section** (excluding the compression header, typically 30 bytes). Add the header size when seeking:
```
file_offset = index_offset + header_size
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

## Writing Index.db

This section documents the SSTable write workflow for generating Index.db and Summary.db components.

### Index.db Entry Format (Write)

When writing Index.db entries in BIG format (NB variant), each entry follows this structure:

```c
struct index_entry {
    be16 marker = 0x0010;          // Partition key digest marker
    byte digest[16];               // MD5 hash of partition key bytes
    vint data_offset;              // Byte offset in Data.db (VInt encoded)
    vint promoted_index_length;    // Length of promoted index data
    byte promoted_index_data[promoted_index_length];  // Only if length > 0
};
```

**Key Requirements:**

1. **Marker**: Always `0x0010` (big-endian), indicating partition key digest follows
2. **Digest**: MD5 hash of raw partition key bytes (16 bytes)
3. **Data Offset**: VInt-encoded byte offset in Data.db where partition starts
4. **Promoted Index**: Length of 0 for simple partitions (M5 Stage 0 implementation)

### Index.db Offset Tracking

**Critical: Capture offset BEFORE writing entry** (Issue #407)

When adding entries to Index.db, the file offset where each entry starts must be captured BEFORE writing the entry bytes. This is essential for accurate Summary.db sampling:

```rust
// Capture the offset BEFORE writing
let index_offset = buffer.len() as u64;

// Write entry (marker + digest + position + promoted_index_length)
write_entry(&mut buffer, key, data_offset)?;

// Return IndexEntryInfo for Summary.db sampling
IndexEntryInfo {
    index_offset,      // Where this entry starts in Index.db
    entry_size,        // How many bytes were written
}
```

**IndexEntryInfo Structure:**
- `index_offset`: Byte offset in Index.db where this entry starts
- `entry_size`: Size of this entry in bytes (varies due to VInt encoding)

This information is used by Summary.db sampling to record accurate Index.db positions.

### MD5 Digest Calculation

The partition key digest is computed as:

```rust
let digest = md5::compute(&partition_key_bytes);
```

The digest is the MD5 hash of the raw partition key bytes (not the token). This allows readers to:
1. Binary search Index.db by digest
2. Validate matches against the actual partition key in Data.db
3. Guard against rare MD5 collisions

### VInt Encoding for Offsets

Data offsets use Cassandra's unsigned VInt encoding:

- **1 byte** for values 0-127 (0x00-0x7F)
- **2 bytes** for values 128-16383 (0x80-0xBFFF)
- **3 bytes** for values 16384-2097151 (0xC0-0xDFFFFF)
- And so on...

Example offset encodings:
```
0       → 0x00           (1 byte)
127     → 0x7F           (1 byte)
128     → 0x80 0x80      (2 bytes)
12381   → 0xB0 0x5D      (2 bytes)
16384   → 0xC0 0x40 0x00 (3 bytes)
```

Variable VInt sizes affect entry sizes and must be accounted for when computing Summary.db offsets.

### Promoted Index (M5 Stage 0: Skipped)

For M5 Stage 0 (simple partitions), promoted index is not written:

```rust
// Write promoted index length (0 = no promoted index)
encode_unsigned(0, &mut buffer);
```

Promoted index is used for wide partitions (many clustering keys) to enable fast within-partition seeks. This can be added in future stages for wide partition support.

### Token Ordering Requirement

Index.db entries MUST be written in token order, matching Data.db partition ordering. This is enforced by the writer:

```rust
if key.token <= last_token {
    return Err("Partitions must be written in token order");
}
```

## Writing Summary.db

Summary.db samples Index.db entries for efficient partition lookup without reading the full index.

### Sampling Strategy

**Default Sampling Interval**: 128 entries

Summary.db samples every Nth entry from Index.db where N = `min_index_interval`. The first entry is always sampled (entry 0), then entries at intervals of 128 (entry 128, 256, 384, etc.).

**Sampling Logic:**
```rust
// Sample first entry and every 128th entry
if entry_count % 128 == 0 {
    summary_writer.add_entry(&key, index_offset)?;
}
```

**Trade-offs:**
- Smaller interval (e.g., 64) = more memory, faster lookups
- Larger interval (e.g., 256) = less memory, more I/O during lookups

Cassandra default of 128 provides a good balance for most workloads.

### When to Sample

Sampling decision is made during partition writes, using the `IndexEntryInfo` returned by `IndexWriter::add_partition()`:

```rust
// Write partition to Data.db
let data_offset = data_writer.write_partition(&key, &mutations, &schema)?;

// Add entry to Index.db and get offset info
let entry_info = index_writer.add_partition(&key, data_offset)?;

// Sample for Summary.db if at interval boundary
if sample_counter % 128 == 0 {
    summary_writer.add_entry(&key, entry_info.index_offset)?;
}
sample_counter += 1;
```

**Critical**: Use the actual `index_offset` from `entry_info`, not an estimated value. VInt encoding causes variable entry sizes, making offset estimation unreliable.

### Summary.db Entry Format (Write)

Summary entries have **no length prefix**. Key boundaries are determined by offset table:

```c
struct summary_entry {
    byte key[];        // Variable length partition key bytes (no prefix!)
    be64 position;     // Position in Index.db file (big-endian)
};
```

Entry serialization:
```rust
// Write key bytes (no length prefix!)
buffer.extend_from_slice(&key_bytes);

// Write position (big-endian u64)
buffer.extend_from_slice(&index_position.to_be_bytes());
```

### Summary.db Offset Table

The offset table records the starting position of each entry within the entry data section.

**Critical Gotcha**: The offset table uses **little-endian** encoding (unlike all other Cassandra components):

```rust
// Write offset table (LITTLE-ENDIAN!)
for offset in entry_offsets {
    buffer.extend_from_slice(&offset.to_le_bytes());
}
```

**Offset Calculation:**
```rust
let mut entry_offsets = Vec::new();
let mut entry_data = Vec::new();

for entry in entries {
    // Record offset BEFORE writing entry data
    entry_offsets.push(entry_data.len() as u32);

    // Write key and position
    entry_data.extend_from_slice(&entry.key);
    entry_data.extend_from_slice(&entry.position.to_be_bytes());
}
```

### Summary.db Header Format (Write)

The header is 24 bytes (big-endian):

```rust
fn write_header(&self, buffer: &mut Vec<u8>, entries_count: u32, summary_entries_size: u64) {
    // min_index_interval (u32, BE)
    buffer.extend_from_slice(&self.min_index_interval.to_be_bytes());

    // entries_count (u32, BE)
    buffer.extend_from_slice(&entries_count.to_be_bytes());

    // summary_entries_size (u64, BE) = offset_table_size + entry_data_size
    buffer.extend_from_slice(&summary_entries_size.to_be_bytes());

    // sampling_level (u32, BE) - typically same as min_index_interval
    buffer.extend_from_slice(&self.min_index_interval.to_be_bytes());

    // size_at_full_sampling (u32, BE) - entries count at full sampling
    buffer.extend_from_slice(&entries_count.to_be_bytes());
}
```

**summary_entries_size Calculation:**
```rust
let offset_table_size = entry_count * 4;  // u32 per entry
let entry_data_size = total_key_bytes + (entry_count * 8);  // keys + positions
let summary_entries_size = offset_table_size + entry_data_size;
```

### First and Last Keys

Summary.db stores serialized first and last keys at the end of the file for quick boundary lookups:

```rust
// Write first key (length-prefixed, big-endian)
buffer.extend_from_slice(&(first_key.len() as u32).to_be_bytes());
buffer.extend_from_slice(&first_key);

// Write last key (length-prefixed, big-endian)
buffer.extend_from_slice(&(last_key.len() as u32).to_be_bytes());
buffer.extend_from_slice(&last_key);
```

These are tracked automatically during writes:
- First key: Captured on first `add_entry()` call
- Last key: Updated on every `add_entry()` call

## Component Integration Workflow

The complete SSTable write workflow coordinates all components:

### Write Order (Critical)

Components MUST be written in this order:

1. **Statistics.db** - Provides delta encoding baseline (FIRST)
2. **Data.db** - Main partition/row data
3. **Index.db** - Partition index (uses Data.db offsets)
4. **Summary.db** - Sampled index entries (uses Index.db offsets)
5. **Filter.db** - Bloom filter
6. **Digest.crc32** - Data.db checksum
7. **TOC.txt** - Table of contents (LAST, publication barrier)

### Data.db → Index.db → Summary.db Flow

**Phase 1: Write Partition to Data.db**
```rust
// Write partition and get Data.db offset
let data_offset = data_writer.write_partition(&key, &mutations, &schema)?;
// data_offset = byte position where partition starts in Data.db
```

**Phase 2: Write Index.db Entry**
```rust
// Add Index.db entry and get offset info
let entry_info = index_writer.add_partition(&key, data_offset)?;
// entry_info.index_offset = byte position where entry starts in Index.db
// entry_info.entry_size = size of entry in bytes (varies due to VInt)
```

**Phase 3: Sample for Summary.db**
```rust
// Sample every 128th entry
if sample_counter % 128 == 0 {
    summary_writer.add_entry(&key, entry_info.index_offset)?;
}
```

**Phase 4: Add to Bloom Filter**
```rust
// Add partition key to Filter.db
filter_writer.add_key(&key);
```

### Complete Example

```rust
// Initialize writers
let mut data_writer = DataWriter::new(stats);
let mut index_writer = IndexWriter::new();
let mut summary_writer = SummaryWriter::new(128);
let mut filter_writer = FilterWriter::new(filter_path, capacity, 0.01)?;

let mut sample_counter = 0;

// For each partition (in token order)
for (key, mutations) in partitions {
    // 1. Write to Data.db
    let data_offset = data_writer.write_partition(&key, &mutations, &schema)?;

    // 2. Write to Index.db
    let entry_info = index_writer.add_partition(&key, data_offset)?;

    // 3. Sample for Summary.db
    if sample_counter % 128 == 0 {
        summary_writer.add_entry(&key, entry_info.index_offset)?;
    }
    sample_counter += 1;

    // 4. Add to Bloom filter
    filter_writer.add_key(&key);
}

// Finalize components
let data_bytes = data_writer.finish()?;
let index_bytes = index_writer.finish()?;
let summary_bytes = summary_writer.finish()?;
filter_writer.finish().await?;
```

### Offset Relationships

The offset relationships between components:

```
Data.db:
  [Partition 1 at offset 0]
  [Partition 2 at offset 250]
  [Partition 3 at offset 500]

Index.db:
  [Entry 1 at offset 0: digest + data_offset=0]
  [Entry 2 at offset 20: digest + data_offset=250]
  [Entry 3 at offset 40: digest + data_offset=500]

Summary.db:
  [Entry 0 sampled: key + index_offset=0]    ← Sample every 128th
  [Entry 128 sampled: key + index_offset=X]  ← (if 128+ partitions)
```

**Lookup Flow (Read):**
1. Summary.db: Binary search by key → find nearest entry → get Index.db offset
2. Index.db: Scan from offset → find exact partition → get Data.db offset
3. Data.db: Seek to offset → read partition data

### Memory Efficiency

**Streaming Writes**: All writers use streaming serialization to avoid unbounded memory growth:

- **IndexWriter**: Serializes entries immediately to buffer
- **SummaryWriter**: Stores entries in-memory (small, sampled subset)
- **DataWriter**: Serializes rows immediately to buffer
- **FilterWriter**: Uses disk-based Bloom filter construction

Memory usage is bounded by:
- Number of sampled entries (not total entries)
- Bloom filter size (configurable)
- Statistics metadata (fixed size)

### References
- Cassandra 5.0.0:
  - `IndexSummary`: [org.apache.cassandra.io.sstable.IndexSummary](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/IndexSummary.java)
  - `SSTableReader`: [org.apache.cassandra.io.sstable.SSTableReader](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableReader.java)
  - BIG reader: [org/apache/cassandra/io/sstable/format/big/BigTableReader.java](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java)
  - BIG writer: [org/apache/cassandra/io/sstable/format/big/BigTableWriter.java](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableWriter.java)

- CQLite Implementation:
  - `cqlite-core/src/storage/sstable/writer/index_writer.rs` - Index.db writer
  - `cqlite-core/src/storage/sstable/writer/summary_writer.rs` - Summary.db writer
  - `cqlite-core/src/storage/sstable/writer/data_writer.rs` - Data.db writer
  - `cqlite-core/src/storage/sstable/writer/mod.rs` - SSTableWriter coordinator

For implementation details, see Appendix C.


