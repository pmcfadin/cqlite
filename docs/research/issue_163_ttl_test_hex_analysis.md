# Issue #163: ttl_test_table Statistics.db Hex Analysis

## File Analysis

File: `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/ttl_test_table-6af66a30a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db`

Total size: 5235 bytes (0x1473)

## Section Breakdown

### Header (bytes 0x00-0x1F, 32 bytes)

```
Offset    Hex                                              ASCII
00000000  00 00 00 04 26 29 1b 05  00 00 00 00 00 00 00 2c  |....&).........,|
00000010  00 00 00 01 00 00 00 65  00 00 00 02 00 00 01 a5  |.......e........|
```

**Parsed fields:**
- `0x00-0x03`: version = 0x00000004 (4) - nb-format identifier
- `0x04-0x07`: statistics_kind = 0x26291b05
- `0x08-0x0B`: reserved = 0x00000000
- `0x0C-0x0F`: data_length = 0x0000002c (44 bytes)
- `0x10-0x13`: metadata1 = 0x00000001
- `0x14-0x17`: metadata2 = 0x00000065 (101)
- `0x18-0x1B`: metadata3 = 0x00000002
- `0x1C-0x1F`: checksum = 0x000001a5 (421)

### EncodingStats Section (bytes 0x20+)

**What parser expects to parse:**
1. metadata_type (u32 BE)
2. data_length (VInt)
3. partitioner_len (VInt)
4. partitioner string (UTF-8)
5. _metadata1 (VInt) - skip
6. _metadata2 (VInt) - skip
7. minTimestamp (VInt)
8. minLocalDeletionTime (VInt)
9. minTTL (VInt)
10. **SerializationHeader columns start here** ← This is where we need to identify the exact offset

**Actual bytes at 0x20:**
```
00000020  00 00 00 03 00 00 13 8d  5a 9e 83 9c 00 2b 6f 72  |........Z....+or|
          [metadata_t] [????????]  [????????] [VInt?] [data starts]
```

Let me manually decode each field:

1. **metadata_type (u32 BE)**: `00 00 00 03` = 3
2. Next needs to be a **VInt for data_length**

### VInt Decoding Reference

VInt format (Cassandra unsigned VInt):
- If byte < 0x80: single byte value
- If byte >= 0x80: multi-byte encoding (first byte encodes length in high bits)

Let's decode byte by byte from 0x24:

```
Offset 0x24: 00 00 13 8d 5a 9e 83 9c
```

**Byte 0x24-0x25**: `00 00` - This could be:
- Two separate VInts (both value 0)?
- Part of a multi-byte value?

**Hypothesis 1**: Maybe data_length is NOT a VInt but a u32?

Let me check if bytes 0x24-0x27 are a u32:
`00 00 13 8d` = 5005 decimal

**Hypothesis 2**: The structure might be:
- 0x20-0x23: metadata_type = 0x00000003 (u32)
- 0x24-0x27: **section_length (u32)** = 0x0000138d (5005 bytes)
- 0x28+: VInt-encoded data starts

Let me verify this by looking at offset 0x28:
```
Offset 0x28: 5a 9e 83 9c 00 2b 6f 72  67
```

`5a` = 90 decimal (< 0x80), so this is a single-byte VInt = 90

But that doesn't make sense for partitioner length. Let me look at 0x29:
```
0x29: 9e
```

`9e` = 158 = 0x9e. This is >= 0x80, so it's a multi-byte VInt.

Actually, let me reconsider. Looking at offset 0x2B:
```
00 2b 6f 72 67 2e 61 70 ...
```

`2b` = 43 decimal. And if we look at the ASCII:
```
6f 72 67 2e 61 70 61 63 68 65 2e 63 61 73 73 61 6e 64 72 61 ...
= "org.apache.cassandra.dht.Murmur3Partitioner"
```

So `0x2b` (43) is the partitioner string length! This means the partitioner string is at offset 0x2C.

### Verified EncodingStats Structure

```
0x20-0x23: metadata_type = 0x00000003 (u32 BE)
0x24-0x2A: ??? (7 bytes of unknown structure)
0x2B:      partitioner_len = 0x2b (43) - single-byte VInt
0x2C-0x56: partitioner string = "org.apache.cassandra.dht.Murmur3Partitioner"
0x57+:     Remaining EncodingStats data
```

The **7 unknown bytes** at 0x24-0x2A are:
```
00 00 13 8d 5a 9e 83
```

Let me check if this is TWO VInts:
- First VInt: `00` = 0
- Second VInt: `00` = 0
- Third VInt: `13` = 19
- Fourth VInt: `8d` = 141

Hmm, that's 4 separate single-byte VInts. But the parser only skips 2 metadata VInts before timestamps.

### Alternative Theory: Bloom Filter Data

Looking at the hex after partitioner (offset 0x57):
```
00000050  72 3f 84 7a e1 47 ae 14  7b cf dd b8 49 00 00 01  |r?.z.G..{...I...|
00000060  38 ff ff ff fe 0d 19 01  64 bc df 50 d8 9f 55 b6  |8.......d..P..U.|
...
```

This looks like bloom filter data (lots of seemingly random bytes).

After bloom filter, we expect to find the SerializationHeader.

### Finding SerializationHeader Start

From the research doc, SerializationHeader should start with:
- VInt length (for partition key type)
- `(org.apache.cassandra.db.marshal.UUIDType` string
- 0x00 0x00 section marker

Let me search for the partition key type string in hex. Looking at offset 0x1390:
```
00001390  75 ed 65 c2 f0 12 e3 ce  e5 c1 51 80 28 6f 72 67  |u.e.......Q.(org|
000013a0  2e 61 70 61 63 68 65 2e  63 61 73 73 61 6e 64 72  |.apache.cassandr|
000013b0  61 2e 64 62 2e 6d 61 72  73 68 61 6c 2e 55 55 49  |a.db.marshal.UUI|
000013c0  44 54 79 70 65 00 00 03  0e 65 78 70 69 72 69 6e  |DType....expirin|
```

**FOUND IT!** At offset **0x139A**: `(org.apache.cassandra.db.marshal.UUIDType`

The byte before it at 0x1399 is `0x28` = 40 decimal. But that's the '(' character, not the length!

Let me look at 0x1398:
```
0x1398: 51 80
```

`0x51` = 81. But let me check if this is a multi-byte VInt:
- `0x51` = 01010001 binary
- High bit NOT set, so single byte = 81 decimal

Hmm, but the string "(org.apache.cassandra.db.marshal.UUIDType" is longer than 81 bytes.

Wait, let me count: `(org.apache.cassandra.db.marshal.UUIDType` = 40 characters.

So the VInt is `0x51 0x80`? Let me decode this as a multi-byte VInt:
- First byte: `0x51` = 01010001
- If multi-byte, we need to check continuation...

Actually, let me look more carefully at 0x1399:
```
0x1399: 80 28
```

`0x80` = 10000000 binary. This is a special VInt encoding!

In Cassandra VInt format:
- `0x80` followed by one byte means value is in next byte
- So `0x80 0x28` = 40 decimal (0x28)

**So the partition key type length is 40 bytes!**

Therefore:
- **Offset 0x1399**: VInt length = 0x80 0x28 = 40
- **Offset 0x139B-0x13C2**: Partition key type string (40 bytes) = `(org.apache.cassandra.db.marshal.UUIDType`
- **Offset 0x13C3-0x13C4**: Section marker = 0x00 0x00
- **Offset 0x13C5**: Should be VInt for regular column count = 0x03 (3 columns)

Let me verify at 0x13C3:
```
000013c0  44 54 79 70 65 00 00 03  0e 65 78 70 69 72 69 6e  |DType....expirin|
```

Yes! `0x00 0x00 0x03` - section marker followed by column count 3!

## Key Finding

**SerializationHeader starts at offset 0x1399** (5017 decimal from file start, or 4985 bytes after the 32-byte header).

This means the parser needs to:
1. Parse header (32 bytes)
2. Skip 4985 bytes of EncodingStats/bloom filter/other metadata
3. Start parsing SerializationHeader at exactly offset 0x1399

## Problem Identified

The current parser in `parse_minimal_encoding_stats()` does:
1. Skip metadata_type (4 bytes) → offset 0x24
2. Parse data_length VInt
3. Parse partitioner_len VInt
4. Skip partitioner string
5. Skip 2 metadata VInts
6. Parse timestamps (3 VInts)
7. **Immediately try to parse SerializationHeader**

But the SerializationHeader is NOT immediately after the timestamps! There's a huge bloom filter section in between.

### Next Steps

We need to:
1. Add debug logging to see EXACTLY where the parser is after parsing minTTL
2. Identify what's between minTTL and SerializationHeader offset 0x1399
3. Add logic to skip or parse that intermediate section

