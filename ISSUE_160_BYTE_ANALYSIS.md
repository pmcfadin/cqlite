# Issue #160: Byte-Level Analysis

## First Row - Detailed Byte Breakdown

### Block First 64 Bytes
```
[00, 10, 15, 29, 1a, 77, d7, 39, 4e, 73, 83, 97, b7, 87, 44, 2f,
 3a, 1f, 7f, ff, ff, ff, 80, 00, 00, 00, 00, 00, 00, 00, 24, 82,
 5b, 1e, c8, 21, af, 08, 07, 00, 00, 00, 02, 30, 36, 0f, 08, 01,
 08, 00, 00, 00, 28, 08, 05, 61, 73, 63, 69, 69, 08, 04, 80, 00]
```

### Parsing Trace

**Partition Header (offset 0-13, 14 bytes consumed):**
- 0: 0x00 (flags)
- 1: 0x10 (key length VInt = 8)
- 2-9: key bytes (8 bytes)
- 10-11: timestamp VInt = -460 (2 bytes: `0x15, 0x29`)
- 12-13: localDeletionTime VInt = -7108 (2 bytes: `0x1a, 0x77`)
- **End offset: 14**

**Row Header (offset 14-18):**
- 14: 0xd7 is actually the FIRST BYTE OF ROW FLAGS! But debug says offset 15?
- Debug says: "🟢 ROW START at offset 15"
- Debug says: "🚩 Row flags: 0b01000100 (0x44)"

**CRITICAL ERROR FOUND**: The partition header parsing is WRONG!

Looking at the debug output:
```
🔵 PARTITION HEADER START at offset 0
  📌 Flags byte: 0x00 at offset 0
  📐 Reading partition key length at offset 1, bytes: [10, 15, 29, 1a, 77]
  📐 Key length: 8 (consumed 1 bytes)
  🔑 Partition key: 8 bytes at offset 2
  🗑️  Reading partition deletion timestamp at offset 10
  🗑️  Timestamp: -460 (consumed 2 bytes)
  🗑️  Reading partition localDeletionTime at offset 12
  🗑️  LocalDeletionTime: -7108 (consumed 2 bytes)
🔵 PARTITION HEADER END: consumed 14 bytes, new offset 14
```

But then:
```
🟢 ROW START at offset 15
```

**The partition header end is 14, but row start is 15!** There's a FLAGS BYTE at offset 14 that's being read BEFORE parse_row is called.

Looking at parse_partition code:
```rust
// Read flags byte
let flags = data[offset];  // This reads at offset 14
offset += 1;                // Now offset is 15
```

So the actual row starts at offset 14, not 15. The debug is correct, but the FLAGS byte (0xd7) is being lost!

## Analysis of Partition Key

Looking at offsets 1-9:
- Offset 1: 0x10 = VInt 8 (key length)
- Offset 2-9: `[15, 29, 1a, 77, d7, 39, 4e, 73]` = 8 byte key

But wait - the debug shows key bytes as offsets 2-9, which matches. So partition header consumed bytes 0-13 correctly.

## The Real Issue: Row Flags Reading

After partition header (offset 14), the code does:
```rust
// Read flags byte
let flags = data[offset];  // offset = 14, reads 0xd7
offset += 1;                // offset = 15

// ... but debug shows flags as 0x44, not 0xd7!
```

Looking at offset 14 in the hex dump: **It's 0x44, not 0xd7!**

Let me re-index the hex:
```
Offset: 00  01  02  03  04  05  06  07  08  09  10  11  12  13  14  15
Byte:   00  10  15  29  1a  77  d7  39  4e  73  83  97  b7  87  44  2f
```

So:
- Offset 2-9: `[15, 29, 1a, 77, d7, 39, 4e, 73]` = partition key (8 bytes)
- Offset 10: 0x83 (should be partition deletion timestamp VInt start)
- Offset 11: 0x97
- Offset 12: 0xb7
- Offset 13: 0x87
- Offset 14: 0x44 = row flags

But the debug shows:
- timestamp at offset 10: reads as -460 (2 bytes)
- localDeletionTime at offset 12: reads as -7108 (2 bytes)

**Let me decode the VInts manually:**
- VInt at offset 10 (`0x83, 0x97`): This is a 2-byte VInt encoding -460
- VInt at offset 12 (`0xb7, 0x87`): This is a 2-byte VInt encoding -7108

So partition header ends at offset 14 correctly!

## Row Structure Analysis

Starting at offset 14:
```
14: 0x44 (flags) - HAS_TIMESTAMP (0x04) + HAS_ALL_COLUMNS not set (0x00) + something else
15: 0x2f (row size VInt start)
```

Debug shows:
```
  📏 Reading row size at offset 15, bytes: [2f, 3a, 1f, 7f, ff]
  📏 Row size: 47 (consumed 1 bytes)
```

So:
- Offset 14: flags = 0x44
- Offset 15: row size VInt = 47 (1 byte: 0x2f)
- Offset 16: prev size VInt start

But wait - parse_row is called with offset=15 because the flags were already read in parse_partition!

**AH! The issue is in parse_partition loop:**

```rust
// Read flags byte
let flags = data[offset];  // offset = 14
offset += 1;                // offset = 15

// Parse row
match self.parse_row(data, offset, flags, schema, &partition_key)
```

So parse_row receives offset=15, which is CORRECT. The row body starts at 15, flags are passed separately.

## Actual Cell Data Location

From debug:
```
🟣 CELLS START at offset 19
  🔹 Cell #3 (column index 3)
  🔹 Column name: ascii_field, type: ascii
🔍 parse_cell: column=ascii_field, start_offset=29, flags_byte=0b00000000 (0x00)
  ⏰ Parsing cell timestamp at offset 30, bytes: [24, 82, 5b, 1e, c8]
  ⏰ After timestamp, offset=31
📝 Parsing text at offset 31, first 10 bytes: [82, 5b, 1e, c8, 21, af, 08, 07, 00, 00]
```

Offset 31 bytes: `[82, 5b, 1e, c8, 21, af, 08, 07, 00, 00]`

The parser reads 0x82 as a VInt length = 603, which is WRONG.

**The real problem**: The timestamp parsing consumed only 1 byte (VInt value 36 from 0x24), leaving offset at 31. But then it tries to parse text length and reads 0x82, which is part of UNRELATED DATA.

## Row Size Validation

Debug shows:
```
⚠️  WARNING: Row size mismatch! Expected 47 bytes, actually consumed 620
```

Expected row size is 47 bytes (from offset 16 to end of row body).
- Row size field says: 47 bytes from row_start_offset
- Actual consumption: 620 bytes

The row_start_offset is set AFTER reading row_size:
```rust
offset = data.len() - remaining.len();
let row_start_offset = offset; // Mark for validation
```

So row_start_offset should be the offset AFTER row_size and prev_size are read.

## Hypothesis

The issue is that **clustering columns are NOT being parsed but should be**!

Looking at the Java source, BEFORE row size, there should be clustering key bytes for tables with clustering columns.

Let me check if the schema has clustering columns...
