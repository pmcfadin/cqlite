## Index.db and Summary.db

This chapter explains the partition index (`Index.db`) and the sampled summary (`Summary.db`), and how they guide binary search and seeks into `Data.db`. It also outlines token-range iteration behavior.

### In this chapter you will learn
- The structure of index entries and promoted index behavior
- How summary sampling accelerates lookups
- How binary search is guided from summary to index to data
- How token range iteration interacts with the index

## Partition Index Structure

`Index.db` (the BIG / NB partition index) stores, for every partition, the **raw partition
key** (length-prefixed) together with its byte offset into `Data.db`. There is **no `0x0010`
marker and no MD5 digest** — that was a long-standing documentation error (Issue #552). Each
entry is:

```
[key_len: u16 BE]                    ← length of the raw partition key
[raw partition key bytes: key_len]   ← the partition key exactly as serialized in Data.db
[data_offset: unsigned vint]         ← byte offset into the Data.db data section
[promoted_index_len: unsigned vint]  ← byte length of the promoted index (0 = none)
[promoted_index_data: promoted_index_len bytes]
```

> **The leading `u16` is the partition key LENGTH, not a marker.** It varies with the key:
> `0x0010` for a 16-byte UUID key, `0x0026` (= 38) for a 38-byte composite partition key,
> `0x0004` for a 4-byte `int` key, and so on. Earlier revisions of this guide misread the
> `0x0010` produced by single-UUID tables as a fixed marker; it is simply the length of a
> 16-byte key.

Annotated example — single UUID key (`simple_table`, key length 16):
```
00000000: 0010 1529 1a77 d739 4e73 8397 b787 442f  |...).w.9Ns....D/|
00000010: 3a1f 0000 ...                            |:.. .          |
```
- `0010` → key length = 16
- `1529…3a1f` → 16-byte raw partition key (a UUID)
- `00` → unsigned VInt data offset (value 0)
- `00` → unsigned VInt promoted-index length (0, no promoted index)

Annotated example — composite key (`multi_partition_table`, `PRIMARY KEY ((tenant_id, user_id), …)`, key length 38):
```
00000000: 0026 0010 98e0 5820 982d 411c 961f 26d1  |.&....X .-A...&.|
00000010: 0574 74e4 0000 109d 159a 2b08 da4a d1be  |.tt.......+..J..|
00000020: 78c9 0f87 83e5 c100 ...                  |x..... .       |
```
- `0026` → key length = 38
- `0010 98e0…74e4 00 0010 9d15…c1 00` → 38-byte composite key. Cassandra frames each
  component as `[len: u16 BE][value][0x00 end-of-component]`, so two 16-byte UUIDs become
  `0010<uuid1>00 0010<uuid2>00` = 19 + 19 = 38 bytes.
- the data-offset and promoted-index-length vints follow the key.

Pseudo-struct (field order; big-endian for fixed-width):
```
u16   key_len
bytes raw_partition_key[key_len]
vint  data_offset
vint  promoted_index_len
bytes promoted_index[promoted_index_len]   // only when promoted_index_len > 0
```

**Critical: VInt offset encoding (NB format)**

The `data_offset` and `promoted_index_len` fields use **Cassandra unsigned VInt encoding**,
not a fixed-width or length-prefixed byte array:

VInt encoding (from `DataInputPlus.java`):
- First byte's leading 1-bits indicate total byte count
- `0x00-0x7F`: 1 byte, value = byte itself
- `0x80-0xBF`: 2 bytes
- `0xC0-0xDF`: 3 bytes
- etc.

Example offsets:
```
0x00       -> 1 byte  -> value = 0
0xb0 0x5d  -> 2 bytes -> value = 12381
0xc0 0x5f 0x11 -> 3 bytes -> value = 24337
```

**Important**: NB format offsets are **relative to the Data.db data section** (excluding the
compression header, typically 30 bytes). Add the header size when seeking:
```
file_offset = index_offset + header_size
```

Keys and collisions:
- The on-disk key is the raw partition key, so lookups can compare keys directly (no digest /
  collision handling is required at the index layer). On a hash-based point lookup that misses,
  readers fall back to a full scan that compares raw keys (see Issue #517).

Promoted index payload (BIG):
- Emitted for wide partitions to accelerate within-partition seeks. Its length is given
  explicitly by `promoted_index_len`; when it is 0 the entry has no promoted index. See
  `org.apache.cassandra.io.sstable.format.big` reader/writer for the payload fields.

> **Promoted index trigger (data-size based, not key-count based):** A new
> `IndexInfo` block is emitted whenever the uncompressed data written for the current
> partition since the last block boundary reaches `column_index_size` (default 64 KiB).
> Source: [`BigFormatPartitionWriter.java:49`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigFormatPartitionWriter.java#L49)
> (`DEFAULT_GRANULARITY = 64 * 1024`) and line 213 (`if (currentPosition() - startPosition >= indexSize)`).
> A promoted index is only included when **two or more** `IndexInfo` blocks result
> (`RowIndexEntry.create()` lines 227-239). Partitions smaller than 64 KiB produce a plain
> `RowIndexEntry` with serialized size = 0 and no promoted index data.

> **DeletionTime field order ("oa" vs legacy, MC-4):** For SSTable version "oa" (5.0+),
> `DeletionTime` inside a promoted index is serialized as:
> - LIVE: 1 byte `0x80` (high bit set)
> - Non-LIVE: 8 bytes `markedForDeleteAt` (sign bit must be 0, high bit used as flags), then
>   4 bytes `localDeletionTime` as unsigned int
>
> For legacy versions (< "oa") the `LegacySerializer` writes `localDeletionTime` (4 bytes,
> signed int) **first**, then `markedForDeleteAt` (8 bytes). Gate on
> `version.hasUintDeletionTime` (`BigFormat.java:409`).
> Source: [`DeletionTime.java#L210-L219`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/DeletionTime.java#L210)

Mini-parser — conceptual:
```text
pos = 0
key_len            = read_u16_be()
raw_key            = read_bytes(key_len)
data_offset        = read_vint_u64()
promoted_index_len = read_vint_u64()
promoted_index     = read_bytes(promoted_index_len)   // empty when length == 0
```

> Note: a BTI-indexed SSTable does **not** use `Index.db` at all — it uses the
> `Partitions.db` / `Rows.db` trie structures (see Ch.17). So `Index.db`, when present, is
> always this BIG-format partition index; there is no separate "BTI Index.db" variant to detect.

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
- BTI SSTables do **not** use `Index.db` or `Summary.db` at all. The BIG-format
  `Summary → Index → Data` flow does not apply to BTI. BTI uses `Partitions.db` (a
  page-aware trie, `PartitionIndex`) for O(log n) key lookup and `Rows.db` for the
  row index. See Ch.17 for the complete BTI format.
  Source: [`BtiFormat.java#L83-L102`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.java#L83)

### Key Takeaways
- `Index.db` maps partition keys to positions; `Summary.db` accelerates binary search.
- Sampling reduces memory while preserving fast seeks.
- Token-range iteration combines summary jumps with index scans.

## Writing Index.db

This section documents the SSTable write workflow for generating Index.db and Summary.db components.

### Index.db Entry Format (Write)

When writing Index.db entries in BIG format (NB variant), each entry follows this structure.
The entry begins with the partition key **length** and the **raw partition key bytes** — there
is no `0x0010` marker and no MD5 digest (Issue #552):

```c
struct index_entry {
    be16 key_len;                  // Length of the raw partition key
    byte raw_key[key_len];         // Raw partition key bytes (same as in Data.db)
    vint data_offset;              // Byte offset in Data.db (VInt encoded)
    vint promoted_index_length;    // Length of promoted index data (0 = none)
    byte promoted_index_data[promoted_index_length];  // Only if length > 0
};
```

**Key Requirements:**

1. **Key length**: `be16` length of the raw partition key (e.g. `0x0010` for a 16-byte UUID,
   `0x0026` for a 38-byte composite key, `0x0004` for a 4-byte `int`). This is a LENGTH, not a
   marker.
2. **Raw key**: The raw serialized partition key bytes, byte-for-byte identical to the key in
   Data.db (no hashing, no digest).
3. **Data Offset**: VInt-encoded byte offset in Data.db where the partition starts.
4. **Promoted Index**: Length of 0 for simple partitions (M5 Stage 0 implementation).

### Index.db Offset Tracking

**Critical: Capture offset BEFORE writing entry** (Issue #407)

When adding entries to Index.db, the file offset where each entry starts must be captured BEFORE writing the entry bytes. This is essential for accurate Summary.db sampling:

```rust
// Capture the offset BEFORE writing
let index_offset = buffer.len() as u64;

// Write entry (key_len + raw key + position + promoted_index_length)
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

### Raw Key Storage (no digest)

Index.db stores the **raw partition key bytes** length-prefixed; it does NOT store an MD5
digest. (A prior version of this guide incorrectly described an MD5 digest — see Issue #552.)
Because the raw key is on disk, readers can:
1. Binary search / scan Index.db comparing the raw partition key directly
2. Use the inline `data_offset` to seek straight into Data.db
3. Avoid any hash-collision handling at the index layer

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

Promoted index is used for wide partitions — those where uncompressed data written for
the current partition since the last `IndexInfo` block boundary reaches `column_index_size`
(default 64 KiB, `BigFormatPartitionWriter.DEFAULT_GRANULARITY`). A partition with few but
large rows can trigger this; one with many tiny rows below 64 KiB will not. A promoted index
is only included when **two or more** `IndexInfo` blocks result (`RowIndexEntry.create()`
lines 227-239). This can be added in future stages for wide partition support.
Source: [`BigFormatPartitionWriter.java#L49,L213`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigFormatPartitionWriter.java#L49)

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

    // sampling_level (u32, BE) - downsampling level: 1..BASE_SAMPLING_LEVEL (128).
    // For a freshly written SSTable use BASE_SAMPLING_LEVEL (128), NOT min_index_interval.
    // They share the same default value (128) by coincidence but diverge after downsampling.
    // Source: Downsampling.java:34 (BASE_SAMPLING_LEVEL=128); IndexSummary.java:88-94,226-229
    buffer.extend_from_slice(&self.sampling_level.to_be_bytes()); // 128 for new SSTables

    // size_at_full_sampling (u32, BE) - ESTIMATED entry count at full sampling.
    // Formula: total_partition_count / min_index_interval  (NOT entries_count).
    // After downsampling, entries_count < size_at_full_sampling; they are only equal for a
    // freshly written SSTable when sampling_level == BASE_SAMPLING_LEVEL == 128.
    // Source: IndexSummary.java:235-237 (getMaxNumberOfEntries() returns sizeAtFullSampling)
    buffer.extend_from_slice(&self.size_at_full_sampling.to_be_bytes());
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

1. **Statistics.db** - Written first in the CQLite implementation to provide a delta
   encoding baseline. Note: this ordering is a CQLite implementation decision; Cassandra's
   `BigTableWriter` streams `Data.db` as the primary output and computes statistics
   alongside—there is no format constraint requiring Statistics.db before Data.db.
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

Index.db (16-byte UUID keys → each entry is 2 + 16 + vint + vint bytes):
  [Entry 1 at offset 0:  key_len + raw_key + data_offset=0]
  [Entry 2 at offset 20: key_len + raw_key + data_offset=250]
  [Entry 3 at offset 40: key_len + raw_key + data_offset=500]

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
  - `IndexSummary`: [org.apache.cassandra.io.sstable.indexsummary.IndexSummary](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/indexsummary/IndexSummary.java) (package path changed in 5.0.x)
  - BIG reader (was SSTableReader): [org.apache.cassandra.io.sstable.format.big.BigTableReader](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java) (`SSTableReader` is now abstract; BIG reading is in `BigTableReader`)
  - BIG reader: [org/apache/cassandra/io/sstable/format/big/BigTableReader.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java)
  - BIG writer: [org/apache/cassandra/io/sstable/format/big/BigTableWriter.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigTableWriter.java)

- Cassandra 5.0.8 — additional citations:
  - IndexSummary offset LE encoding: [`IndexSummary.java#L411-L418`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/indexsummary/IndexSummary.java#L411)
  - Promoted index 64 KiB trigger: [`BigFormatPartitionWriter.java#L49,L213`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigFormatPartitionWriter.java#L49)
  - RowIndexEntry binary format: [`RowIndexEntry.java#L57-L68`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/RowIndexEntry.java#L57)
  - DeletionTime "oa" serialization (mfda first): [`DeletionTime.java#L210-L219`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/DeletionTime.java#L210)
  - BASE_SAMPLING_LEVEL=128: [`Downsampling.java#L34`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/Downsampling.java#L34)

- CQLite Implementation:
  - `cqlite-core/src/storage/sstable/writer/index_writer.rs` - Index.db writer
  - `cqlite-core/src/storage/sstable/writer/summary_writer.rs` - Summary.db writer
  - `cqlite-core/src/storage/sstable/writer/data_writer.rs` - Data.db writer
  - `cqlite-core/src/storage/sstable/writer/mod.rs` - SSTableWriter coordinator

For implementation details, see Appendix C.


