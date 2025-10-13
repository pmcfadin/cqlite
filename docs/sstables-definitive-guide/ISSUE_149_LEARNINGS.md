# Issue #149 Learnings - Index.db and Compression Implementation Details

This document captures critical implementation details discovered while fixing Issue #149 (Queries fail when Index.db reports size=0). These should be integrated into the main guide.

## Suggested Additions to Chapter 06: Index.db and Summary.db

### Index.db Entry Format Variations (Cassandra 5.0)

**Discovery:** Index.db entries in Cassandra 5.0 have format variations that must be handled:

#### Format 1: Standard Entry (no length prefix)
```
0x0010                    // 2-byte marker
<16-byte key digest>      // Partition key digest (MD5/Murmur3)
<1-byte length>           // Variable-length offset field length (1-9 bytes)
<N-byte offset>           // Big-endian offset value
```

#### Format 2: Entry with Length Prefix (some C5.0 tables)
```
0x001a                    // 2-byte entry length (26 bytes = 0x1a)
0x0010                    // 2-byte marker
<16-byte key digest>      // Partition key digest
<1-byte length>           // Offset field length
<N-byte offset>           // Big-endian offset value
```

**Implementation Note:** Parsers must detect the length prefix (0x001a or similar) and skip it before reading the 0x0010 marker.

**Example from `user_activity` table:**
```
00 1a 00 10 37 ac 9f 53  bd 8e 4d a5 a4 1a 24 0f
8f 5a 6c fd 00 00 04 80  00 4f 88 ...
```
- `00 1a` = 26-byte entry length
- `00 10` = partition key marker
- `37 ac ... 6c fd` = 16-byte digest
- `00 00 04 80 00 4f 88` = variable-length offset (0x00048000 = offset, 0x4f88 = size)

### Size Field Semantics in Cassandra 5.0

**Critical Discovery:** In Cassandra 5.0, Index.db **does NOT reliably store partition sizes**.

- **C5.0 Behavior:** Size field often reports `0x0000` (size=0)
- **C4.x Behavior:** Size field contained actual partition size

**Why This Matters:**
- Readers CANNOT rely on Index.db size field for offset-based reads
- When size=0, must fall back to **sequential scan** from the offset
- This is NOT an error condition—it's expected C5.0 behavior

**Implementation Pattern:**
```rust
if entry.size == 0 {
    // C5.0: Use sequential scan fallback
    return self.scan_for_key(table_id, key).await;
} else {
    // C4.x or valid size: Use offset-based read
    let file_offset = entry.offset + header_size;
    return self.read_value_at_offset(file_offset, entry.size).await;
}
```

### Offset Semantics

**Critical Detail:** Index.db offsets are **relative to data section start**, NOT absolute file positions.

**Correct Calculation:**
```
file_offset = index_entry.offset + actual_header_size
```

**Common Error:** Adding hardcoded values like +30 bytes
- Header sizes vary by format: 30 bytes (legacy), 4096 bytes (BIG), etc.
- NEVER use hardcoded adjustments
- ALWAYS calculate actual header size from file format

## Suggested Additions to Chapter 09: CompressionInfo.db and Chunking

### Snappy Compression Format Variations

**Discovery:** Snappy compression has TWO distinct formats in Cassandra:

#### Format 1: Legacy (C4.x and earlier)
```
<4-byte big-endian size>    // Uncompressed size
<compressed data>            // Raw Snappy compressed bytes
```

**Characteristics:**
- Size prefix allows validation BEFORE decompression
- Enables early detection of decompression bombs
- Used in legacy Cassandra formats

#### Format 2: Raw Snappy (Cassandra 5.0 NB format)
```
<compressed data>            // Raw Snappy frame, NO size prefix
```

**Characteristics:**
- No size prefix—data starts with Snappy frame header
- Cannot detect decompression bombs until AFTER decompression
- Used in C5.0 "nb" (new big) format
- More space-efficient (saves 4 bytes per block)

### Decompression Bomb Protection Strategy

**Challenge:** Must handle both formats while protecting against malicious data.

**Implementation Approach:**

1. **Try prefixed format first (if data length >= 4):**
   ```rust
   if data.len() >= 4 {
       let claimed_size = u32::from_be_bytes([data[0..4]]);

       // Only try prefixed format if size is reasonable
       if claimed_size > 0 && claimed_size <= MAX_SIZE {
           // Attempt decompression of data[4..]
       }
       // If size is unreasonable (>128MB), it's likely raw format
   }
   ```

2. **Fall back to raw Snappy:**
   ```rust
   // C5.0 NB format—no prefix
   let decompressed = decompress_raw(data)?;

   // Validate AFTER decompression
   if decompressed.len() > MAX_SIZE {
       return Err("Decompression bomb detected");
   }
   ```

**Key Insight:** A "size" of 3.8GB in the first 4 bytes is likely compressed data being misinterpreted as a size prefix, not a real attack. Skip prefixed format and try raw.

### NB Format Block Reading

**Discovery:** Cassandra 5.0 NB format treats the entire data section as a single compressed unit.

**Reading Strategy:**
```
1. Seek to data section start (after header)
2. Calculate remaining bytes: file_size - current_position
3. Read entire remaining section as ONE block
4. Decompress using raw Snappy (no size prefix)
5. Parse decompressed data as partition entries
```

**Anti-Pattern:** Don't try to read fixed-size "blocks"—there's only one compressed blob.

## Suggested Additions to Appendix C: Walkthroughs

### Walkthrough: Index.db Size=0 Fallback

**Scenario:** Reading a partition when Index.db reports size=0

**Step-by-step:**

1. **Bloom filter check** (unchanged)
   ```
   if !bloom_filter.might_contain(key) { return None; }
   ```

2. **Index lookup:**
   ```
   let entry = index.find_entry(table_id, key)?;
   // entry.offset = 0x4000, entry.size = 0
   ```

3. **Detect size=0 condition:**
   ```rust
   if entry.size == 0 {
       log::debug!("Index reports size=0, using sequential scan");
       return self.scan_for_key(table_id, key).await;
   }
   ```

4. **Sequential scan from offset:**
   ```rust
   // Seek to data section start + index offset
   file.seek(header_size + entry.offset)?;

   // Read and decompress blocks sequentially
   while let Some(block) = read_next_block()? {
       let entries = parse_block_entries(&block)?;

       for (entry_table_id, entry_key, entry_value) in entries {
           if entry_table_id == *table_id && entry_key == *key {
               return Ok(Some(entry_value));
           }
       }
   }
   ```

5. **Performance Note:** Sequential scan is slower but necessary for C5.0 compatibility

### Walkthrough: Multi-Format Snappy Decompression

**Scenario:** Decompressing a block that might be prefixed or raw Snappy

**Algorithm:**
```rust
fn decompress_snappy(data: &[u8]) -> Result<Vec<u8>> {
    const MAX_SIZE: usize = 128 * 1024 * 1024; // 128MB

    // Try prefixed format (C4.x legacy)
    if data.len() >= 4 {
        let claimed_size = u32::from_be_bytes([data[0..4]]) as usize;

        // Only valid if reasonable
        if claimed_size > 0 && claimed_size <= MAX_SIZE {
            if let Ok(decompressed) = decompress(&data[4..]) {
                if decompressed.len() == claimed_size {
                    return Ok(decompressed); // Success!
                }
            }
            // Size was reasonable but decompression failed—try raw
        }
        // Size > MAX_SIZE means likely raw format, not a prefix
    }

    // Try raw Snappy (C5.0 NB format)
    let decompressed = decompress(data)?;

    // Validate AFTER decompression
    if decompressed.len() > MAX_SIZE {
        return Err("Decompression bomb protection: size exceeds 128MB");
    }

    Ok(decompressed)
}
```

**Key Points:**
- Try both formats automatically
- Don't reject early based on "suspicious" prefix values
- Post-decompression validation is the final safety net

## Testing Recommendations

### Test Cases for Index.db Readers

1. **Legacy format (no length prefix)**
   - Standard C4.x Index.db files
   - Entries start with 0x0010 marker

2. **C5.0 format with length prefix**
   - Tables like `user_activity` with 0x001a prefix
   - Must skip prefix to find 0x0010 marker

3. **Size=0 entries**
   - C5.0 tables where Index.db reports size=0
   - Must use sequential scan fallback

4. **Mixed format resilience**
   - Reader should handle both formats in same codebase
   - Detect format automatically

### Test Cases for Compression

1. **Prefixed Snappy (legacy)**
   - 4-byte size prefix followed by compressed data
   - Validate size matches decompressed result

2. **Raw Snappy (C5.0 NB)**
   - No prefix, just Snappy frame
   - Validate via post-decompression check

3. **Malformed data**
   - Bogus size claims (e.g., 3.8GB) should fall through to raw format
   - Actual decompression bombs (valid compression expanding >128MB) should be rejected

4. **Empty/small files**
   - Handle edge cases gracefully

## Common Pitfalls

### ❌ Pitfall 1: Hardcoded Offset Adjustments
```rust
// WRONG - hardcoded +30 assumes fixed header size
let file_offset = index_entry.offset + 30;
```

**Correct:**
```rust
// Calculate actual header size from format
let file_offset = index_entry.offset + self.actual_header_size;
```

### ❌ Pitfall 2: Rejecting Size=0 as Error
```rust
// WRONG - size=0 is valid in C5.0
if entry.size == 0 {
    return Err("Invalid size");
}
```

**Correct:**
```rust
// C5.0 expected behavior
if entry.size == 0 {
    return self.sequential_scan_fallback(entry.offset);
}
```

### ❌ Pitfall 3: Early Rejection of "Large" Sizes
```rust
// WRONG - might be raw Snappy data, not a real size claim
if claimed_size > MAX_SIZE {
    return Err("Decompression bomb");
}
```

**Correct:**
```rust
// Skip prefixed format, try raw instead
if claimed_size > MAX_SIZE {
    // Likely raw format, not an actual size prefix
    return decompress_raw(data);
}
```

### ❌ Pitfall 4: Assuming Fixed Block Sizes
```rust
// WRONG - C5.0 NB uses entire data section as one block
while let Some(block) = read_fixed_size_block(4096)? { ... }
```

**Correct:**
```rust
// Read entire remaining data section
let remaining = file_size - current_position;
let block = read_bytes(remaining)?;
```

## References

- **Issue #149:** Queries fail when Index.db reports size=0
- **Commits:** c239745, 9dda181, d5efa3b
- **Test Files:**
  - `test-data/datasets/sstables/test_timeseries/user_activity-*.../nb-1-big-Index.db`
  - `test-data/datasets/sstables/test_collections/collection_table-*.../nb-1-big-Data.db`

## Integration Checklist

- [ ] Add Index.db format variations to Chapter 06
- [ ] Document size=0 semantics in Chapter 06
- [ ] Add Snappy format variations to Chapter 09
- [ ] Add decompression bomb strategy to Chapter 09
- [ ] Add size=0 fallback walkthrough to Appendix C
- [ ] Add multi-format Snappy walkthrough to Appendix C
- [ ] Update common pitfalls section
- [ ] Add test case recommendations

---

*Document created: 2025-10-10*
*Based on implementation work for Issue #149*

## Issue #153 Learnings - NB Format Structure (Authoritative)

### Discovery

Investigation of "unknown magic numbers" (`0x71160000`, `0xf1185c00`) revealed these were **not** magic numbers or CRC checksums, but the first 4 bytes of compressed chunk data in NB format files.

**Initial Misunderstanding:** These bytes were incorrectly documented as "Header CRC32 prefixes" that protected SSTable metadata.

**Authoritative Clarification (from senior devs):** NB format Data.db files have no magic number or global header whatsoever. The file starts directly with compressed chunk data.

### NB Format Key Facts (Authoritative)

1. **No magic number in Data.db** - format is identified by filename only (`nb-1-big-Data.db`)
2. **No global header** - file starts directly with chunk data
3. **CRCs are trailing** - come after each chunk, not before
4. **Requires CompressionInfo.db** - chunk boundaries not self-describing
5. **CRC algorithm** - Java `java.util.zip.CRC32`, big-endian

### Data.db Structure

```
Offset 0: [chunk_0_compressed_bytes: variable length]
          [crc32_chunk_0: 4 bytes, big-endian]
          [chunk_1_compressed_bytes: variable length]
          [crc32_chunk_1: 4 bytes, big-endian]
          ...
```

**Key Points:**
- First byte of file is first byte of compressed data (NOT a magic number)
- Chunk boundaries determined by CompressionInfo.db (offset/length pairs)
- CRC32 validation happens AFTER reading each chunk
- No way to parse Data.db standalone - must read CompressionInfo.db first

### CompressionInfo.db Format

```
[chunk_count: varint]
[chunk_0_offset: varint]
[chunk_0_length: varint]
[chunk_1_offset: varint]
[chunk_1_length: varint]
...
```

### Incorrect Assumptions (Now Corrected)

- First 4 bytes are CRC32 of remaining header → **Wrong** (no header exists)
- First 4 bytes are magic number → **Wrong** (it's chunk data)
- First 4 bytes are leading CRC prefix → **Wrong** (CRCs are trailing)
- Can parse Data.db standalone → **Wrong** (need CompressionInfo.db first)
- NB format has any file header → **Wrong** (completely header-less)

### Why This Matters

**Reading NB Format Files:**
1. Parse CompressionInfo.db to get chunk map
2. For chunk N:
   - Seek to Data.db offset from CompressionInfo.db
   - Read `length` bytes (compressed data)
   - Read next 4 bytes as CRC32 (big-endian)
   - Validate CRC32 over compressed bytes
   - Decompress chunk
   - Parse row data

**Anti-Patterns:**
- Don't try to read magic number from Data.db
- Don't expect any header bytes
- Don't treat first 4 bytes as special
- Don't assume CRCs come before chunks

### Implementation Impact

- Must parse CompressionInfo.db before Data.db
- Chunk reading requires offset/length from compression metadata
- CRC validation happens after reading each chunk (trailing 4 bytes)
- No header parsing needed for NB format Data.db
- Format detection relies on filename pattern, not file content

### Documentation Updates

The following sections were updated based on authoritative NB format specification:

1. **Chapter 09: CompressionInfo and Chunking**
   - Added "NB Format: Chunking Without Headers" section
   - Documented Data.db structure (header-less)
   - Documented CompressionInfo.db format (varint offset/length pairs)
   - Added CRC32 algorithm details (trailing, big-endian)
   - Added common pitfalls section

2. **Chapter 20: Checksums and Integrity**
   - Updated "Header CRC32 Prefixes" section with note: **legacy formats only, NOT NB**
   - Added "NB Format: Trailing Chunk CRCs" section
   - Documented CRC placement (after chunks, not before)
   - Documented validation process (read chunk, read CRC, validate)
   - Updated Key Takeaways to clarify NB vs legacy differences

3. **ISSUE_149_LEARNINGS.md** (this file)
   - Added Issue #153 learnings section
   - Documented authoritative NB format structure
   - Clarified incorrect assumptions
   - Provided implementation guidance

### References

- **Issue #153:** Investigation and root cause analysis
- **Issue #155:** Proper NB format implementation plan (if exists)
- **Senior dev explanation:** Authoritative format specification
- **Cassandra source:** `org.apache.cassandra.io.compress.CompressionMetadata`

### Key Learnings

1. **Trust authoritative sources** - When senior devs provide format specs, update documentation immediately
2. **Format detection matters** - NB format identified by filename, not file content
3. **No assumptions without validation** - Don't assume header structures without confirming
4. **Trailing vs leading** - CRC placement varies by format (NB: trailing, legacy: varies)
5. **CompressionInfo.db is critical** - NB format Data.db is unreadable without it

---

*Issue #153 section added: 2025-10-10*
*Based on authoritative NB format clarification from senior developers*
