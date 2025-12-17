# SerializationHeader Format Analysis - Collection Type Parsing Issue

**Date**: 2025-12-17
**Issue**: #215 - Collection type parsing failures
**Research Goal**: Identify byte patterns that differ in collection type descriptors
**Status**: Partially resolved - VInt fix applied

## Executive Summary

The failing tables have **collection types** (lists, sets, maps) in their SerializationHeader. The VInt length parsing fix has been applied and successfully fixed `static_columns_table`.

**Current pass rate**: 28/33 tables (85%)

### VInt Fix Applied (2025-12-17)

Changed type length parsing from `parse_u8()` to `parse_vuint()` in:
- `enhanced_statistics_parser.rs` lines 467, 541, 836
- Increased max type_len from 200 to 1000 bytes

This fixed `static_columns_table`. The remaining 5 tables (frozen_collections_table, typed_collections_table, nested_collections_table, collections_with_udts, chat_messages) have a deeper issue with the marker-based header search algorithm.

### Root Cause (Resolved)

Collection type descriptors are **longer** than simple types (80-200+ bytes vs 28-45 bytes) and require **multi-byte VInt encoding** for their length field. The parser was using single-byte parsing which misinterpreted VInt continuation bytes.

### Remaining Issue

The `0x00 0x00` marker-based search for SerializationHeader start doesn't correctly locate the header for collection-heavy tables. A refactor to sequential parsing from known offsets is needed.

## Failing Tables Analysis

### 1. static_columns_table (test_basic) - ✅ FIXED

**Schema**: Has static column `static_data` (text)
**Status**: Now passes after VInt fix
**Type String Length**: Single-byte encoding (< 128 bytes)

**Pattern**: Single-byte length `0x28` (40 bytes) for simple UTF8Type - **FIXED WITH VINT PARSING**

---

### 2. typed_collections_table (test_collections)

**Type String at offset** `0x01340`:

```
0x01340:  61 00 00 00 01 00 08 00  00 01 99 b7 13 68 69 00  |a............hi.|
...
0x01360:  68 61 6c 2e 4c 69 73 74  54 79 70 65 28 6f 72 67  |hal.ListType(org|
0x01370:  2e 61 70 61 63 68 65 2e  63 61 73 73 61 6e 64 72  |.apache.cassandr|
0x01380:  61 2e 64 62 2e 6d 61 72  73 68 61 6c 2e 42 79 74  |a.db.marshal.Byt|
0x01390:  65 73 54 79 70 65 29                              |esType)|
```

**Column**: `blob_list` → `ListType(BytesType)` = **83 bytes** (0x53)

**Pattern**:
- Byte before column name: `0x53` (expected single-byte length)
- But actual type string: `org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.BytesType)` = 83 bytes

**Problem**: Length byte `0x53` is interpreted as length, not as part of multi-byte VInt\!

---

### 3. frozen_collections_table (test_collections)

**Type String at offset** `0x01340`:

```
0x01340:  6e 5f 70 72 6f 70 65 72  74 69 65 73 80 a6 6f 72  |n_properties..or|
0x01350:  67 2e 61 70 61 63 68 65  2e 63 61 73 73 61 6e 64  |g.apache.cassand|
0x01360:  72 61 2e 64 62 2e 6d 61  72 73 68 61 6c 2e 46 72  |ra.db.marshal.Fr|
0x01370:  6f 7a 65 6e 54 79 70 65  28 6f 72 67 2e 61 70 61  |ozenType(org.apa|
```

**Column**: `frozen_properties` → `FrozenType(MapType(...))` = **166 bytes** (0xa6)

**Pattern**:
- Byte sequence before column name: `0x80 0xa6`
- This is a **2-byte VInt** encoding length 166:
  - High bit set in `0x80` signals continuation
  - Actual length: `(0x80 & 0x7F) << 8 | 0xa6` = `0x00a6` = 166

**KEY FINDING**: Parser expects single byte, doesn't decode 2-byte VInt\!

---

### 4. nested_collections_table (test_collections)

**Type String at offset** `0x01340`:

```
0x01340:  73 5f 62 79 5f 67 61 6d  65 80 d1 6f 72 67 2e 61  |s_by_game..org.a|
0x01350:  70 61 63 68 65 2e 63 61  73 73 61 6e 64 72 61 2e  |pache.cassandra.|
0x01360:  64 62 2e 6d 61 72 73 68  61 6c 2e 4d 61 70 54 79  |db.marshal.MapTy|
0x01370:  70 65 28 6f 72 67 2e 61  70 61 63 68 65 2e 63 61  |pe(org.apache.ca|
```

**Column**: `scores_by_game` → `MapType(UTF8Type, FrozenType(ListType(Int32Type)))` = **209 bytes** (0xd1)

**Pattern**:
- Byte sequence: `0x80 0xd1`
- 2-byte VInt: length = 209
- Nested collection: Map → Frozen List → Int32

---

### 5. collections_with_udts (test_collections)

**Type String at offset** `0x01790`:

```
0x01790:  65 6e 63 79 5f 63 6f 6e  74 61 63 74 73 82 aa 6f  |ency_contacts..o|
0x017a0:  72 67 2e 61 70 61 63 68  65 2e 63 61 73 73 61 6e  |rg.apache.cassan|
0x017b0:  64 72 61 2e 64 62 2e 6d  61 72 73 68 61 6c 2e 4d  |dra.db.marshal.M|
0x017c0:  61 70 54 79 70 65 28                              |apType(|
```

**Column**: `emergency_contacts` → `MapType(UTF8Type, FrozenType(UserType(...)))` = **682 bytes** (0x02aa)

**Pattern**:
- Byte sequence: `0x82 0xaa`
- This is actually **0x02aa** = 682 bytes
- Multi-byte VInt: `(0x82 & 0x7F) << 8 | 0xaa` = `0x02aa`
- Contains UDT (User Defined Type) with nested address UDT

---

### 6. chat_messages (test_wide_rows)

**Type String at offset** `0x01650`:

```
0x01650:  80 cf 6f 72 67 2e 61 70  61 63 68 65 2e 63 61 73  |..org.apache.cas|
0x01660:  73 61 6e 64 72 61 2e 64  62 2e 6d 61 72 73 68 61  |sandra.db.marsha|
0x01670:  6c 2e 4d 61 70 54 79 70  65 28 6f 72 67 2e 61 70  |l.MapType(org.ap|
```

**Column**: `reactions` → `MapType(UTF8Type, FrozenType(SetType(UUIDType)))` = **207 bytes** (0xcf)

**Pattern**:
- Byte sequence: `0x80 0xcf`
- 2-byte VInt: length = 207
- Nested frozen collection

---

## VInt Encoding Format (Cassandra Spec)

### Single-Byte Encoding (0-127)
```
0x00 - 0x7F  →  value = byte
```

### Two-Byte Encoding (128-16383)
```
0x80-0xFF [byte2]  →  value = ((byte1 & 0x7F) << 8) | byte2
Example: 0x80 0xa6  →  (0x00 << 8) | 0xa6 = 166
```

### Multi-Byte Encoding (larger values)
- Continuation bit (0x80) signals more bytes follow
- See: `cqlite-core/src/parser/vint.rs` for full implementation

---

## Current Parser Limitation

**File**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/parser/enhanced_statistics_parser.rs`

**Function**: `parse_serialization_header_at_offset()` (line 378)

```rust
// Step 7: Parse each regular column
for col_idx in 0..column_count {
    // Column name length (single byte)  ← BUG: Assumes single byte\!
    let (remaining, name_len) = parse_u8(input)?;

    // ... parse column name ...

    // Column type length (single byte)  ← BUG: Assumes single byte\!
    let (remaining, type_len) = parse_u8(remaining)?;

    // Column type (UTF-8 string)
    let (remaining, type_bytes) = take(type_len as usize)(remaining)?;
    // ...
}
```

**Problem**:
- Uses `parse_u8()` for type length
- Cannot handle 2-byte VInt encoding (`0x80 0xa6` → 166)
- Treats `0x80` as length 128, reads wrong data
- Parsing fails with UTF-8 decode error

---

## Solution: Use VInt Parser

**Change Required**:

```rust
// Replace parse_u8() with parse_vuint() for type length
let (remaining, type_len_u64) = parse_vuint(remaining)?;
let type_len = type_len_u64 as usize;

// Validate type length
if type_len == 0 || type_len > 1000 {  // Allow up to 1000 bytes for nested collections
    return Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Verify,
    )));
}
```

**Key Changes**:
1. Replace `parse_u8()` with `parse_vuint()` for column type length
2. Increase max type length from 200 to 1000 bytes (nested collections are large)
3. Keep single-byte for column **name** length (names are always short)

---

## Test Case Examples

### Simple Type (WORKS)
```
Column: age
Type: org.apache.cassandra.db.marshal.Int32Type (41 bytes)
Encoding: 0x29 [41 bytes of type string]
Parser: ✅ parse_u8() reads 0x29 = 41
```

### Collection Type (FAILS)
```
Column: tags
Type: org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type) (77 bytes)
Encoding: 0x80 0x4d [77 bytes of type string]
Parser: ❌ parse_u8() reads 0x80 = 128, tries to read 128 bytes, hits wrong data
```

### Nested Collection (FAILS)
```
Column: scores_by_game
Type: MapType(UTF8Type,FrozenType(ListType(Int32Type))) (209 bytes)
Encoding: 0x80 0xd1 [209 bytes of type string]
Parser: ❌ parse_u8() reads 0x80 = 128, parsing fails
```

---

## Recommended Fix Locations

1. **`parse_serialization_header_at_offset()`** (line 378-579)
   - Line 542: Change column type length from `parse_u8()` to `parse_vuint()`
   - Line 549: Increase max type_len validation from 200 to 1000

2. **`parse_regular_columns()`** (line 697-924)
   - Line 836: Change type length from single byte to VInt
   - Line 839: Increase max type_len from 200 to 1000

3. **Static columns parsing** (line 436-517)
   - Line 467: Change static column type length to VInt
   - Line 476: Increase max type_len from 200 to 1000

---

## Validation Plan

After fix, test with:
```bash
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test --package cqlite-core \
  statistics_db_real_file_test::test_parse_static_columns_table

env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test --package cqlite-core \
  statistics_db_real_file_test::test_parse_typed_collections_table

# ... repeat for all 6 failing tables
```

Expected result: All 6 tables should now parse successfully with correct column types.

---

## Additional Observations

### Byte Pattern Summary

| Table | Column Type | Type String Length | VInt Encoding | Hex Pattern |
|-------|-------------|-------------------|---------------|-------------|
| static_columns_table | UTF8Type | 40 | Single | `0x28` |
| typed_collections_table | ListType(BytesType) | 83 | Two-byte | `0x53` or `0x80 0x53`? |
| frozen_collections_table | FrozenType(MapType(...)) | 166 | Two-byte | `0x80 0xa6` |
| nested_collections_table | MapType(UTF8,Frozen(List(...))) | 209 | Two-byte | `0x80 0xd1` |
| collections_with_udts | MapType(UTF8,Frozen(UserType)) | 682 | Two-byte | `0x82 0xaa` |
| chat_messages | MapType(UTF8,Frozen(SetType)) | 207 | Two-byte | `0x80 0xcf` |

**Note**: Need to verify exact VInt encoding - Cassandra VInt format may differ from standard variable-length integer encoding.

---

## References

- Issue #215: 6 failing tables with collections
- Issue #163: SerializationHeader parsing implementation
- `cqlite-core/src/parser/vint.rs` - VInt parser implementation
- `docs/sstables-definitive-guide/chapters/appendix-b-encodings-cheat-sheet.md` - VInt format
- Cassandra 5.0 source: `org.apache.cassandra.utils.vint.VIntCoding`
EOFMARKER < /dev/null