# Issue #160 - V5CompressedLegacy Parser Offset Fix Summary

## Root Causes Identified

### 1. Partition Key Length Encoding (8-byte offset error)
**Problem**: Partition key length was parsed as **SIGNED VInt with ZigZag encoding** instead of **UNSIGNED VInt**.

**Evidence**:
- Byte `0x10` was decoded as signed VInt → ZigZag(16) = 8
- This caused parser to read only 8 bytes of a 16-byte UUID partition key
- **8 bytes of partition key data were skipped**

**Fix**: Changed `parse_vint()` to `parse_unsigned_vint32()` in partition header parsing.

```rust
// BEFORE (WRONG):
let (remaining, key_len_signed) = crate::parser::vint::parse_vint(&data[offset..])
let key_len = key_len_signed as usize;  // ZigZag decoding: 16 → 8

// AFTER (CORRECT):
let (remaining, key_len_u32) = crate::parser::vint::parse_unsigned_vint32(&data[offset..])
let key_len = key_len_u32 as usize;  // Direct value: 16
```

### 2. Partition Deletion Time Encoding (12-byte offset error)  
**Problem**: Partition deletion times were parsed as **VInts** but are actually **FIXED-WIDTH** fields.

**Evidence**:
- After fixing partition key length, parser still failed
- Byte sequence `[7f, ff, ff, ff, 80, 00, 00, 00, 00, 00, 00, 00]` couldn't parse as VInts
- `0xFF` byte requires 9 bytes for VInt, which exceeded VInt32 limits
- Pattern matches **8-byte timestamp + 4-byte localDeletionTime** (fixed big-endian)

**Fix**: Changed VInt parsing to fixed-width 8+4 byte reads.

```rust
// BEFORE (WRONG):
let (remaining, timestamp) = parse_vuint(&data[offset..])?;  // VInt
let (remaining, local_del) = parse_unsigned_vint32(&data[offset..])?;  // VInt32

// AFTER (CORRECT):
let mut ts_bytes = [0u8; 8];
ts_bytes.copy_from_slice(&data[offset..offset + 8]);
let timestamp = i64::from_be_bytes(ts_bytes);  // Fixed 8 bytes

let mut ldt_bytes = [0u8; 4];
ldt_bytes.copy_from_slice(&data[offset..offset + 4]);
let local_del = i32::from_be_bytes(ldt_bytes);  // Fixed 4 bytes
```

### 3. Cell Flag Validation (Masking Bug)
**Problem**: Invalid cell flags were **masked** instead of **rejected**, hiding offset corruption.

**Evidence**:
- Cell flag `0x24` has high bit 0x20 set (HAS_ALL_COLUMNS - a ROW flag, not cell flag)
- `CELL_FLAGS_MASK = 0x1F` converted `0x24 → 0x04`, hiding the error
- Valid cell flags are **ONLY** 0x00-0x1F (bits 0x20/0x40/0x80 indicate offset corruption)

**Fix**: Removed mask, added strict validation rejecting high bits.

```rust
// BEFORE (WRONG):
const CELL_FLAGS_MASK: u8 = 0x1F;
let flags = data[offset] & CELL_FLAGS_MASK;  // Masked 0x24 → 0x04

// AFTER (CORRECT):
let flags = data[offset];
if flags & 0xE0 != 0 {
    return Err(Error::corruption(format!(
        "Invalid cell flags {:#04x} at offset {}: high bits set indicate offset misalignment",
        flags, offset
    )));
}
```

## Total Offset Error

**Combined error: 8 + 12 = 20 bytes**
- 8 bytes from partition key length (signed vs unsigned)
- 12 bytes from partition deletion times (fixed vs VInt)
- This closely matches the reported "19 bytes behind" observation

## Validation Results

### Before Fix:
- Cell #0 flag: `0x24` (invalid - has row flag bit)
- Test: **FAILED** - "Invalid cell flags 0x24 at offset X"

### After Fix:
- Partition header: **30 bytes** (1 flag + 1 vint + 16 key + 8 ts + 4 ldt)
- Row starts at offset 30 with valid flag `0x24` (HAS_TIMESTAMP | HAS_ALL_COLUMNS)
- Cells parse correctly starting at local_offset 3
- Test: **PASSED** ✅

## Files Modified

1. `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`
   - Lines 214-222: Partition key length → unsigned VInt
   - Lines 232-252: Partition deletion → fixed 8+4 bytes
   - Lines 675-683: Cell flag validation (removed mask)
   - Lines 10-13: Updated format documentation

## Key Learnings

1. **Cassandra V5CompressedLegacy format mixing**:
   - Partition-level: Fixed-width deletion times
   - Row-level: VInt-encoded deletion times
   - Partition key: Unsigned VInt length
   - Row sizes: Unsigned VInt

2. **Flag validation is critical**:
   - Cell flags MUST be 0x00-0x1F
   - Any high bits (0x20/0x40/0x80) indicate offset corruption
   - Never mask flags - validate and reject invalid values

3. **Signed vs Unsigned VInt**:
   - ZigZag encoding significantly changes decoded values
   - Always verify encoding type from Cassandra source
   - `0x10` unsigned = 16, signed (zigzag) = 8

## Testing

```bash
# Run test
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
    cargo test --package cqlite-core storage::sstable::reader::tests::tests::test_v5_compressed_legacy_extracts_cells

# Result: PASSED ✅
```

## Next Steps

1. Remove debug `eprintln!` statements from v5_compressed_legacy.rs
2. Run full test suite to ensure no regressions
3. Validate against additional test datasets
4. Update any other parsers that might have similar issues
