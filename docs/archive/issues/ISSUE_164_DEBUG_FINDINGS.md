# Issue #164 Debug Findings

## Problem Summary
The V5CompressedLegacy parser is failing to extract cell values because it's attempting to parse **partition key data** as if it were a regular cell column.

## Key Evidence

### 1. Debug Output Shows Partition Key Parsing Attempt
```
=== PARSING COLUMN: id (uuid) ===
Current offset: 37
Remaining data length: 16347
First 32 bytes at current offset: 08070000000230360f0801080000002808056173636969080480004f21080000
Marker byte at offset 37: 0x08 (expected 0x08)

!!! PARSE ERROR !!!
Failed to parse column 'id' at column index 0 (offset 37)
Error: Corruption("Cell 'id': expected UUID length 16, got 7")
```

### 2. Schema Analysis
From test definition (`test_v5_compressed_legacy_extracts_cells`):

**Partition Keys:**
- `id` (uuid) - position 0

**Regular Columns (schema.columns):**
- `account_balance` (decimal)
- `active` (boolean)
- `age` (int)
- `ascii_field` (ascii)
- ... (15 more columns)

### 3. Reference Data Validation
From JSONL output for first partition:
```json
{"partition":{"key":["15291a77-d739-4e73-8397-b787442f3a1f"],"position":30},"rows":[...]}
```

The partition key UUID is `15291a77-d739-4e73-8397-b787442f3a1f`.

### 4. Binary Format Analysis

Decompressed block structure at offset 37 (where parser starts trying to parse cells):
```
Offset 37: 08 07 00 00 00 02 30 36 0f 08 01 08 00 00 00 28 08 05 61 73 63 69 69 ...
           |  |  |____________|       |  |  |____________|  |  |  |
           |  |       |               |  |       |         |  |  +-- "ascii" (5 bytes)
           |  |       |               |  |       |         |  +-- Length: 5
           |  |       |               |  |       |         +-- Marker: 0x08
           |  |       |               |  |       +-- 4-byte value: 0x00000028 = 40
           |  |       |               |  +-- Length: 1 (bool)
           |  |       |               +-- Marker: 0x08
           |  |       +-- VInt/bytes (complex field)
           |  +-- Length: 7 bytes
           +-- Marker: 0x08
```

### 5. Pattern Recognition

The hex `08 05 61 73 63 69 69` decodes to:
- `0x08` = marker
- `0x05` = length 5
- `0x6173636969` = "ascii" (UTF-8)

This matches the **4th column** `ascii_field` from schema, NOT the 1st!

### 6. Offset Calculation Issue

The parser is at offset 37 trying to parse what it thinks is the first cell (`id` uuid from partition keys), but it should be parsing:
- Offset 37 appears to be the first **REGULAR CELL** (after row header)
- The first cell should be `account_balance` (decimal), which is schema.columns[0]

**CRITICAL ERROR**: The parser is iterating over `schema.columns` correctly, but the error message shows `"id (uuid)"` which is from `schema.partition_keys`, NOT `schema.columns`!

## Root Cause Analysis

Looking at the parser code in `parse_row_data_with_offset`:

```rust
let columns_in_order = &schema.columns;  // ← Correct: using regular columns only

for (col_idx, column) in columns_in_order.iter().enumerate() {
    // ...
    match self.parse_cell_value_schema_order(data, offset, column, reader) {
        // ...
    }
}
```

This is CORRECT - it's using `schema.columns` which does NOT include partition keys.

**BUT** - somewhere the column being passed to `parse_cell_value_schema_order` is actually the partition key `id` instead of the first regular column `account_balance`!

## Hypothesis

The issue is likely in:
1. **Schema construction** - partition keys might be incorrectly included in `schema.columns`
2. **Wrong column list** - parser might be using partition_keys instead of columns
3. **Test setup** - schema passed to parser might have wrong column list

## Next Steps

1. Add debug instrumentation to print the ACTUAL column list being used in parse_row_data_with_offset
2. Verify `schema.columns` does NOT contain partition keys when passed to parser
3. Check if schema is being constructed correctly in the test
4. Verify offset 37 is indeed the correct start of first cell data

## Expected Fix

The parser should be trying to parse `account_balance` (decimal) at offset 37, NOT `id` (uuid).

If offset 37 contains the bytes `08 07 00 00 00 02 30 36 0f`, this is:
- Marker: 0x08
- Length: 7
- Data: 00 00 00 02 30 36 0f (7 bytes for decimal)

This suggests decimal format is:
- [u8 marker: 0x08]
- [u8 length: 7]
- [7 bytes: scale + unscaled value]

Which differs from current parser expectation that expects:
- [u8 marker: 0x08]
- [u8 total_len]
- [i32 scale: 4 bytes]
- [remaining bytes: unscaled]

If total_len=7, then scale (4 bytes) + unscaled (3 bytes) = 7 bytes total. This matches!

**Remaining Question**: Why is the error message showing "id (uuid)" when it should show "account_balance (decimal)"?
