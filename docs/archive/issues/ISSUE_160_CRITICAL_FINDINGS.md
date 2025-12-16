# Issue #160: Critical Byte-Level Findings

## Problem Summary

The V5CompressedLegacy parser has a 374-byte offset gap caused by:
1. **Incorrect row boundary calculation**
2. **Parsing continuing beyond actual row end**
3. **Subsequent rows being misaligned**

## First Row Analysis (CORRECT)

### Partition Header (offsets 0-13):
- Offset 0: flags = 0x00
- Offset 1: key_len VInt = 8 (1 byte)
- Offsets 2-9: key bytes (8 bytes)
- Offsets 10-11: deletion timestamp VInt (2 bytes)
- Offsets 12-13: local deletion time VInt (2 bytes)
- **Total: 14 bytes consumed**

### Row Header (offset 14):
- Offset 14: **ROW FLAGS = 0x44** (read in parse_partition loop, NOT in parse_row)
- parse_row called with offset=15

### Row Body (offsets 15-63):
- Offset 15: row_size VInt = 47 (1 byte)
- Offset 16: prev_size VInt = 58 (1 byte)
- **row_start_offset = 16** (after row_size)
- **row_end_offset = 16 + 47 = 63**
- Offset 17: row timestamp VInt = -16 (1 byte)
- Offset 18: column bitmap VInt (1 byte)
- Offsets 19-62: cell data

**Total row consumption**: offsets 14-63 = 49 bytes (flags byte + 48 bytes body)

## Second Row Analysis (MISALIGNED!)

### Expected Structure:
After first row ends at offset 63, next row should start at offset 64 with:
- Offset 64: **ROW FLAGS** or **END_OF_PARTITION marker**

### Actual Parse:
```
🟢 ROW START at offset 64
  🚩 Row flags: 0b00000000 (0x00)
  📏 Row size: 79 (consumed 1 bytes)
```

**FLAGS = 0x00** means:
- No END_OF_PARTITION (bit 0x01 not set)
- No HAS_TIMESTAMP (bit 0x04 not set)
- No HAS_TTL (bit 0x08 not set)
- No HAS_DELETION (bit 0x10 not set)
- No HAS_ALL_COLUMNS (bit 0x20 not set)

So this looks like a VALID row flags byte. But then:
- Row size = 79 bytes
- Prev size = 33 bytes
- Column bitmap = 0x04 (only columns 0,1,3 present)

But the parsed cells seem reasonable, yet the row size is wrong (expected 79, consumed 11).

## Root Cause Hypothesis

**The issue is NOT with first row parsing - it's with the ROW FLAGS LOOP in parse_partition!**

Looking at the code structure:
```rust
// Parse unfiltered items (rows) until END_OF_PARTITION
loop {
    if offset >= data.len() {
        break;
    }

    // Read flags byte
    let flags = data[offset];
    offset += 1;

    // Check for END_OF_PARTITION
    if flags & END_OF_PARTITION != 0 {
        break;
    }

    // Parse row
    match self.parse_row(data, offset, flags, schema, &partition_key) {
        Ok((row_value, new_offset)) => {
            partition_entries.push(...);
            offset = new_offset;
        }
        Err(e) => {
            warn!(...);
            break;
        }
    }
}
```

**The problem**: After parsing the first row, `offset = 63`. Then:
1. Loop continues
2. Reads flags at offset 63: `data[63]` = ??
3. offset += 1; // offset = 64
4. Calls parse_row(data, 64, flags, ...)

**BUT**: We don't know what byte is at offset 63 in the block! Let me look at the first 64 bytes again:

```
[00, 10, 15, 29, 1a, 77, d7, 39, 4e, 73, 83, 97, b7, 87, 44, 2f,
 3a, 1f, 7f, ff, ff, ff, 80, 00, 00, 00, 00, 00, 00, 00, 24, 82,
 5b, 1e, c8, 21, af, 08, 07, 00, 00, 00, 02, 30, 36, 0f, 08, 01,
 08, 00, 00, 00, 28, 08, 05, 61, 73, 63, 69, 69, 08, 04, 80, 00]
```

This is only the first 64 bytes, so offsets 0-63. Offset 63 (index 63) = 0x00.

**So the byte at offset 63 is 0x00**, which is being read as the NEXT row's flags!

**WAIT!** The issue is that the first row ends at offset 63, meaning the LAST byte of the row is at offset 62. So offset 63 is the FIRST byte of the NEXT ROW (or END_OF_PARTITION marker).

But our debug says:
```
✅ Row size validation passed: 47 bytes
🟢 ROW END: consumed 48 bytes, new offset 63
```

The row consumed 48 bytes (including the flags byte read outside parse_row), ending at offset 63. But the row body itself is 47 bytes starting from offset 16, so it spans offsets 16-62. Offset 63 is the NEXT STRUCTURE.

**Ah! I see the issue now!** The row_end_offset = 16 + 47 = 63, which is CORRECT. But after parse_row returns with offset=63, that's the END of the row body. The NEXT thing at offset 63 should be:
- Either END_OF_PARTITION flag
- Or NEXT ROW's flags

But in the parse_partition loop, it reads flags at offset 63, which is 0x00. This could be:
1. END_OF_PARTITION not set, so continue parsing
2. Next row's flags

Since 0x00 doesn't have END_OF_PARTITION bit set, it continues to parse_row(64, ...).

**THE CRITICAL INSIGHT**: We're reading ONE BYTE TOO EARLY! The row ended at offset 63, but we should be reading the flags at offset 63, NOT incrementing to 64 first!

Wait, let me re-trace:
- First row ends with parse_row returning offset=63
- Loop sets offset = new_offset = 63
- Loop reads flags = data[63] = 0x00
- Loop increments offset += 1; // offset = 64
- Loop calls parse_row(data, 64, flags=0x00, ...)

This seems correct! We read flags at 63, then parse row starting at 64.

So the issue must be that the ROW ITSELF is consuming the wrong number of bytes. Let me check if there's an off-by-one error in the row body calculation.

**NEW HYPOTHESIS**: The `row_size` field represents the size from AFTER THE ROW_SIZE FIELD to END OF ROW, but it does NOT include the `prev_size` field!

Let me check the Cassandra source again. According to UnfilteredSerializer.java, the serialization order is:
1. Row flags (outside)
2. Row size (UNSIGNED VInt)
3. Previous unfiltered size (UNSIGNED VInt)
4. Row timestamp (conditional)
5. ...cells...

And `row_size` represents the size from the CURRENT POSITION (after writing row_size) to the end of the row. So it INCLUDES prev_size.

But what if `row_size` is calculated BEFORE writing prev_size? Let me check if row_size includes or excludes prev_size.

Looking at the Java source:
```java
int rowBodySize = serializedRowBodySize(...);
out.writeUnsignedVInt(rowBodySize); // Write row size
out.writeUnsignedVInt(previousUnfilteredSize); // Write prev size
// ... write row body ...
```

So `row_size` is written BEFORE `prev_size`, and it represents the size of everything AFTER row_size. So it INCLUDES prev_size!

**CONCLUSION**: The row_size field of 47 bytes should cover:
- prev_size field (1 byte)
- row timestamp (1 byte)
- column bitmap (1 byte)
- cells (44 bytes)
- Total: 47 bytes ✓

So row spans offsets 16-62 (47 bytes), and offset 63 is the next structure. This is CORRECT!

But why is the second row parsing garbage?

**FINAL HYPOTHESIS**: The first row's cells are NOT parsing correctly! We're hitting the boundary check and truncating cell #3 (ascii_field), but the ACTUAL cell data might extend beyond what we think.

Let me look at the cell #3 parsing:
```
🔹 Cell #3 (column index 3) at offset 29
  🔹 Column name: ascii_field, type: ascii
🔍 parse_cell: column=ascii_field, start_offset=29, flags_byte=0b00000000 (0x00)
  ⏰ Parsing cell timestamp at offset 30, bytes: [24, 82, 5b, 1e, c8]
  ⏰ After timestamp, offset=31
📝 Parsing text at offset 31, first 10 bytes: [82, 5b, 1e, c8, 21, af, 08, 07, 00, 00]
📝 Parsed length=603, new offset=33
  ⚠️  Cell parsing would exceed row boundary (636 > 63), truncating
```

The cell tries to read 603 bytes, which would end at offset 636, WAY beyond the row boundary of 63.

**THE REAL ISSUE**: The cell is parsing TIMESTAMP bytes as TEXT LENGTH! The timestamp parsing is WRONG!

Looking at offset 29-31:
- Offset 29: cell flags = 0x00
- Flags & 0x08 (CELL_USE_ROW_TIMESTAMP) == 0, so parse individual timestamp
- Offset 30: timestamp bytes [24, 82, 5b, 1e, c8]
- VInt parse reads 0x24 as value 36 (1 byte consumed)
- Offset 31: should be TEXT LENGTH, but reads [82, ...] as 603

**BREAKTHROUGH**: The timestamp value 36 (0x24) at offset 30 seems suspicious. Let me check what the ACTUAL bytes should be at offsets 29-31 in the context of the row.

Actually, I think the issue is that **we're NOT supposed to parse individual timestamps for these cells**! The row has a row-level timestamp at offset 17 (-16), and cells should USE that timestamp, not parse their own!

Let me check the cell flags again:
- Cell #0 (account_balance): flags = 0xff (all bits set!)
- Cell #1 (active): flags = 0xff
- Cell #2 (age): flags = 0x00
- Cell #3 (ascii_field): flags = 0x00

**FLAGS = 0xFF is SUSPICIOUS!** Let me check what 0xff means:
- Bit 0x01: CELL_IS_DELETED = 1
- Bit 0x02: CELL_IS_EXPIRING = 1
- Bit 0x04: CELL_HAS_EMPTY_VALUE = 1
- Bit 0x08: CELL_USE_ROW_TIMESTAMP = 1
- Bit 0x10: CELL_USE_ROW_TTL = 1
- Bit 0x20: CELL_HAS_NULL_VALUE = 1
- Bit 0x40: CELL_EXTENDED_FLAGS = 1
- Bit 0x80: (not defined)

So 0xff means the cell is deleted, expiring, empty, uses row timestamp AND TTL, is null, has extended flags, etc. This is contradictory!

**I think the cell data structure is COMPLETELY WRONG!** The bytes at offset 19-21 (0xff, 0xff) are NOT cell flags - they're something else entirely!

Let me re-think the structure. Maybe we're missing a field BEFORE the cells?

Actually, looking at the Java source again, I notice that for tables WITH clustering keys, there's a "clustering prefix" that gets serialized BEFORE the row size. But for tables WITHOUT clustering (like this one), there's still a size indicator for the clustering prefix (which would be 0).

**CRITICAL REALIZATION**: We're skipping the clustering prefix entirely! Even for tables with NO clustering keys, Cassandra might serialize a LENGTH field (0 bytes).

Let me check if there's a clustering size field that we're missing...
