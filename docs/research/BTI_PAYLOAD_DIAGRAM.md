# BTI Payload Format - Visual Diagram

## Complete BTI Partition Index Entry Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                     BTI Trie Node                                │
├─────────────────────────────────────────────────────────────────┤
│  [Header: 1 byte]                                               │
│   ┌─────────────┬─────────────┐                                 │
│   │ Node Type   │ Payload Bits│                                 │
│   │  (4 bits)   │  (4 bits)   │                                 │
│   └─────────────┴─────────────┘                                 │
│                                                                  │
│  [Node Data: variable]                                          │
│   - Single: 1 byte + 1 pointer                                  │
│   - Sparse: N bytes + N pointers                                │
│   - Dense: range + pointers                                     │
│                                                                  │
│  [Payload: variable] ← WE ARE HERE                              │
│   ┌──────────────┬────────────────────────┐                     │
│   │  Hash Byte   │     Position (SizedInts)│                    │
│   │   (1 byte)   │     (size bytes)        │                    │
│   └──────────────┴────────────────────────┘                     │
│        ↓                    ↓                                    │
│   Filter hash         Data.db offset                            │
│   lower 8 bits        or Row index offset                       │
└─────────────────────────────────────────────────────────────────┘
```

## Payload Size Decoding

```
Step 1: Read Node Header
┌─────────────────────────┐
│ Header Byte = 0xB7      │  Example: PayloadOnly node with payload
├─────────────────────────┤
│ Upper nibble = 0xB = 11 │  → Node type (not important for payload)
│ Lower nibble = 0x7 = 11 │  → payloadBits
└─────────────────────────┘
         ↓
Step 2: Calculate Size
┌─────────────────────────┐
│ size = payloadBits - 7  │
│      = 11 - 7           │
│      = 4 bytes          │
└─────────────────────────┘
         ↓
Step 3: Read Payload
┌─────────────────────────────────────┐
│ Hash:     [0x00]                    │  1 byte
│ Position: [0x00][0x04][0x80][0x00] │  4 bytes (big-endian)
└─────────────────────────────────────┘
         ↓
Step 4: Decode Position
┌─────────────────────────┐
│ 0x00048000             │  Big-endian i32
│   = 294,912 bytes      │  ~295 KB
│   = Data.db offset     │  (positive = direct-to-data)
└─────────────────────────┘
```

## SizedInts Encoding Examples

```
┌──────┬────────────────────────────┬─────────────────┬──────────────┐
│ Size │  Bytes (Big-Endian)        │  Value          │  Max Value   │
├──────┼────────────────────────────┼─────────────────┼──────────────┤
│  1   │  [0x7F]                    │  127            │  127         │
│  2   │  [0x01][0x00]              │  256            │  32,767      │
│  3   │  [0x00][0x04][0x80]        │  1,152          │  8,388,607   │
│  4   │  [0x00][0x04][0x80][0x00]  │  294,912        │  ~2 GB       │
│  5   │  [0x01][0x00][...]         │  4,294,967,296  │  ~512 GB     │
│  6   │  [0x00][0x01][0x00][...]   │  1,099,511...   │  ~128 TB     │
│  7   │  [0x00][0x00][0x01][...]   │  281,474...     │  ~32 PB      │
│  8   │  [0x7F][0xFF][...]         │  i64::MAX       │  Full i64    │
└──────┴────────────────────────────┴─────────────────┴──────────────┘
```

## Position Sign Encoding

```
Positive Position (Row Indexed Partition)
┌────────────────────────────────────────┐
│ position = 0x00012345 (positive)       │
│           = 74,565 bytes               │
│           → Offset in Rows.db          │
│                                        │
│ Read Rows.db at offset 74,565          │
│   → Get row index trie                 │
│   → Navigate for clustering key        │
│   → Get final Data.db offset           │
└────────────────────────────────────────┘

Negative Position (Direct to Data)
┌────────────────────────────────────────┐
│ position = 0xFFFFFFFF... (negative)    │
│           = ~pos bitwise NOT           │
│           = ~294,912                   │
│           = -294,913                   │
│           → Direct Data.db offset      │
│                                        │
│ Decode: offset = ~position             │
│       = ~(-294,913)                    │
│       = 294,912                        │
│                                        │
│ Seek Data.db to offset 294,912         │
│   → Read partition data directly       │
└────────────────────────────────────────┘
```

## Complete Example: Lookup "AMZN" Partition

```
Step 1: Encode partition key
┌─────────────────────────────────────┐
│ Partition key: "AMZN"               │
│ Byte-comparable: [0x41][0x4D]...   │
└─────────────────────────────────────┘
         ↓
Step 2: Navigate BTI trie
┌─────────────────────────────────────┐
│ Start at root node                  │
│ Follow transitions for each byte    │
│   0x41 → child node                 │
│   0x4D → child node                 │
│   0x5A → child node                 │
│   0x4E → leaf node with payload     │
└─────────────────────────────────────┘
         ↓
Step 3: Read leaf node
┌─────────────────────────────────────┐
│ Header: 0x0B                        │
│   Node type: 0x0 (PayloadOnly)      │
│   Payload bits: 0xB (11)            │
│                                     │
│ Payload size: 11 - 7 = 4 bytes      │
└─────────────────────────────────────┘
         ↓
Step 4: Read payload
┌─────────────────────────────────────┐
│ Hash byte: 0x00                     │
│ Position: [0x00][0x04][0x80][0x00] │
│         = 294,912                   │
│         → Positive = direct-to-data │
└─────────────────────────────────────┘
         ↓
Step 5: Seek to Data.db
┌─────────────────────────────────────┐
│ Open Data.db                        │
│ Seek to offset 294,912              │
│ Read partition data for "AMZN"      │
│   - Partition header                │
│   - Static columns                  │
│   - Clustering rows                 │
└─────────────────────────────────────┘
```

## Memory Layout in Index File

```
Partitions.db file structure:

Offset    Content
────────────────────────────────────────────────────────────
0x0000    [BTI Header: magic, version, root offset, etc.]
...
0x0100    [Trie Node 1]
           ├─ Header: 0x2B (Sparse node, payloadBits=11)
           ├─ Transition count: 3
           ├─ Bytes: [0x41][0x47][0x4D]  (A, G, M)
           ├─ Pointers: [...]
           └─ Payload: [0x00][0x00][0x04][0x80][0x00]
0x0150    [Trie Node 2]
           ...
0x0200    [Trie Node 3 - "AMZN" leaf]
           ├─ Header: 0x0B (PayloadOnly, payloadBits=11)
           └─ Payload:
               ├─ Hash: 0x00
               └─ Position: 0x00048000 (294,912)
                           ^^^^^^^^^
                           This is the Data.db offset!
...
```

## Key Insights

1. **Variable Length**: Payload size depends on file offset magnitude
   - Small files (< 2GB) → 4 bytes
   - Medium files (< 512GB) → 5 bytes
   - Large files → 6-8 bytes

2. **Space Efficient**: Only stores bytes needed
   - 295 KB offset → 4 bytes
   - 5 MB offset → 4 bytes
   - 2 GB offset → 4 bytes
   - 3 GB offset → 5 bytes

3. **Direct Seekable**: No parsing needed
   - Extract offset from payload
   - Seek directly to Data.db position
   - No sequential scanning required

4. **Hash Filter**: First byte prevents false positives
   - Quick rejection of non-matching keys
   - Avoids expensive Data.db reads

## Comparison: Legacy vs BTI

```
Legacy Index.db Format:
┌─────────────────────────────────────┐
│ [Key length: 2 bytes]               │
│ [Key bytes: variable]               │
│ [Position: 8 bytes fixed]           │  Always 8 bytes
└─────────────────────────────────────┘
  ↑
  Wastes space for small offsets

BTI Partitions.db Format:
┌─────────────────────────────────────┐
│ [Trie navigation: O(key length)]    │
│ [Payload:                           │
│   - Hash: 1 byte                    │
│   - Position: 1-8 bytes]            │  Variable size
└─────────────────────────────────────┘
  ↑
  Space efficient + fast navigation
```

## Usage in CQLite

```rust
// Pseudo-code for partition lookup using BTI

pub fn lookup_partition(key: &PartitionKey) -> Result<PartitionData> {
    // 1. Encode key for trie navigation
    let encoded_key = encode_byte_comparable(key);

    // 2. Navigate BTI trie
    let mut navigator = TrieNavigator::new(root_offset);
    let node = navigator.find(encoded_key)?;

    // 3. Read node header
    let header = read_u8()?;
    let payload_bits = header & 0x0F;

    // 4. Read payload
    let hash_byte = read_u8()?;
    let size = payload_bits - 7;
    let position = sized_ints::read(reader, size)?;

    // 5. Decode position
    let data_offset = if position < 0 {
        !position as u64  // Direct-to-data
    } else {
        position as u64   // Row index (need second lookup)
    };

    // 6. Seek to Data.db and read
    data_file.seek(data_offset)?;
    read_partition_data(data_file)
}
```

This diagram shows the complete flow from BTI navigation to Data.db offset extraction!
