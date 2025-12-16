# Issue #160: Row Format Research Summary

## Research Objective
Identify the exact byte-level format causing 374-byte offset discrepancy between expected cell location (offset 31) and actual cell location (offset 405) in V5CompressedLegacy parser.

---

## Key Findings

### 1. Clustering Prefix Handling ✅

**Question**: Is clustering prefix always present, even for tables with zero clustering columns?

**Answer**: NO - For tables with `ClusteringTypes: []`:
- Java code: `if (types.isEmpty()) return ByteArrayAccessor.factory.clustering();`
- **Zero bytes** are read from input stream
- Early return before calling `deserializeValuesWithoutSize()`

**Impact on Issue #160**: Clustering prefix is **NOT** the source of the 374-byte gap.

---

### 2. Complete Row Deserialization Order ✅

From `UnfilteredSerializer.java` (Cassandra 5.0 trunk):

```
deserializeOne():
  1. Read flags (1 byte)
  2. Read extended flags (0-1 bytes, if 0x80 set)
  3. IF IS_MARKER: deserialize ClusteringBoundOrBoundary
     ELSE: deserialize regular row:
       a. Clustering.serializer.deserialize()  // 0 bytes for empty clustering
       b. deserializeRowBody()
```

```
deserializeRowBody():
  1. rowSize (unsigned VInt) - SSTable format only
  2. prevUnfilteredSize (unsigned VInt) - SSTable format only
  3. IF HAS_TIMESTAMP: timestamp (delta VInt)
  4. IF HAS_TTL: ttl (delta VInt) + localDeletionTime (delta VInt)
  5. IF HAS_DELETION: deletionTime (delta VInt) + localDeletionTime (delta VInt)
  6. IF NOT HAS_ALL_COLUMNS: column bitmap (varies)
  7. For each present column: deserialize cell
```

**Impact**: Rust parser order matches Java - no missing fields identified.

---

### 3. Delta Encoding in Timestamps/TTL ⚠️

**Critical Discovery**: Cassandra uses **delta encoding** for temporal fields:

```java
// SerializationHeader.java
public long readTimestamp(DataInputPlus in) throws IOException {
    long delta = VIntCoding.readVInt(in);
    return baseTimestamp + delta;
}
```

**Rust Parser Issue**: Lines 238-244 in v5_compressed_legacy.rs:
```rust
let (remaining, ts) = crate::parser::vint::parse_vint(&data[offset..])?;
offset = data.len() - remaining.len();
// BUG: 'ts' is the delta, not the absolute timestamp!
// Should be: timestamp = header.min_timestamp + ts;
```

**Same issue** for TTL (line 250) and localDeletionTime (line 255).

**Impact**: While this affects timestamp values, it **doesn't affect offset calculation** (still reads correct number of bytes).

---

### 4. Cell Flag Semantics ✅

**Confirmed from Cell.java**:
- `HAS_EMPTY_VALUE (0x04)`: **INVERTED LOGIC**
  - Flag **NOT set** (0) = cell has value bytes
  - Flag **SET** (1) = cell has no value bytes (empty)

Rust parser (line 432) correctly implements this:
```rust
if flags & CELL_HAS_EMPTY_VALUE != 0 {
    // Flag IS set = empty value (no bytes to read)
    return Ok((self.empty_value_for_type(&column.data_type), offset));
}
```

**Impact**: Cell parsing logic is correct.

---

## The 374-Byte Mystery: Analysis

### What the Gap is NOT:
1. ❌ Missing clustering prefix (reads 0 bytes for this table)
2. ❌ Missing row body fields (all fields accounted for)
3. ❌ Incorrect cell parsing (logic matches Java)
4. ❌ Wrong flag interpretation (matches spec)

### Hypothesis: Block-Level Structure

The 374-byte gap likely originates from:

#### Theory A: Multiple Partitions
- Decompressed block may contain **multiple partitions**
- Each partition has: flags (1) + key_len (VInt) + key (N bytes) + deletion_time (2 VInts)
- Parser may be reading **first partition header** but expecting cells from **later partition**

#### Theory B: Compression Block Header
- Decompression may leave metadata at block start
- Parser assumes cell data immediately after partition header
- Actual layout: `[block_metadata: 374 bytes] [partition_1] [partition_2] ...`

#### Theory C: Partition Header Miscalculation
- Partition header parsing (lines 169-203) may be reading wrong number of bytes
- Specifically: deletion time VInts may be signed vs unsigned mismatch

#### Theory D: Row Size Validation Missing
- Parser reads `row_size` (line 228) but **doesn't use it**
- `row_size` should indicate total bytes until next row
- Gap may indicate we're at wrong partition or wrong row

---

## Test Case: simple_table

### Schema Analysis:
```
Table: test_basic.simple_table
KeyType: UUIDType (partition key only, 16 bytes)
ClusteringTypes: [] (no clustering columns)
RegularColumns: 18 columns
  - name (text)
  - age (int)
  - salary (bigint)
  - height (float)
  - weight (double)
  - active (boolean)
  - created (timestamp)
  - birth_date (date)
  - work_time (time)
  - description (blob)
  - account_balance (decimal)
  - session_id (timeuuid)
  - ip_address (inet)
  - small_number (tinyint)
  - medium_number (smallint)
  - duration_val (duration)
  - varchar_field (varchar)
  - ascii_field (ascii)

Statistics:
  - totalRows: 1000
  - totalColumnsSet: 18000
  - Compression: SnappyCompressor
  - EncodingStats minTimestamp: 1759713124861209
  - EncodingStats minLocalDeletionTime: 1442880000
  - EncodingStats minTTL: 0
```

### Expected Partition Format:
```
[partition_flags: 1 byte]
[key_length: VInt = 16]
[key_bytes: 16 bytes UUID]
[deletion_timestamp: VInt]
[deletion_localDeletionTime: VInt]
[row_1_flags: 1 byte]
[row_1_data...]
[row_2_flags: 1 byte]
[row_2_data...]
...
[row_1000_data...]
[END_OF_PARTITION: flags=0x01]
```

### Debug Output from Issue:
```
Expected cells at offset 31
Actual cells at offset 405
Gap: 374 bytes
```

**Analysis**:
- Offset 31 would be: 1 + VInt + 16 + 2*VInt ≈ 20-25 bytes for partition header
- Plus row flags + row metadata ≈ 5-10 bytes
- **Total expected: ~30 bytes** ✓ matches offset 31

**But cells are at offset 405!**
- This suggests **374 bytes of data** between partition header and first cell
- Could be: partition header actually larger, or multiple rows parsed, or wrong partition

---

## Recommended Actions

### Immediate: Add Debug Instrumentation

Modify `parse_partition()` to log every byte read:

```rust
fn parse_partition(&self, data: &[u8], mut offset: usize, schema: Option<&TableSchema>)
    -> Result<(Vec<(TableId, RowKey, Value)>, usize)>
{
    let start_offset = offset;
    eprintln!("🔍 parse_partition START at offset {}", offset);

    // Parse partition header
    eprintln!("  Reading partition flags at offset {}", offset);
    let partition_flags = data[offset];
    offset += 1;
    eprintln!("    partition_flags = {:#04x}", partition_flags);

    eprintln!("  Reading key_length at offset {}", offset);
    let (remaining, key_len_signed) = parse_vint(&data[offset..])?;
    let bytes_read = data[offset..].len() - remaining.len();
    eprintln!("    key_length = {} ({} bytes VInt)", key_len_signed, bytes_read);
    offset += bytes_read;

    // ... (continue for ALL fields)

    eprintln!("🔍 parse_partition HEADER COMPLETE at offset {} (consumed {} bytes)",
              offset, offset - start_offset);
}
```

### Secondary: Validate Row Size

Use `row_size` field to validate:

```rust
// After reading row_size (line 228)
let row_start_offset = offset;
let (remaining, row_size) = parse_unsigned_vint32(&data[offset..])?;
offset = data.len() - remaining.len();

// ... parse row body ...

// VALIDATE
let actual_row_bytes = offset - row_start_offset;
if actual_row_bytes != row_size as usize {
    warn!("Row size mismatch: expected {} bytes, read {} bytes",
          row_size, actual_row_bytes);
}
```

### Tertiary: Hex Dump Analysis

Extract raw bytes from Data.db at offsets 0-500:

```bash
# Decompress first block
$ sstabledump test_basic/simple_table-*/nb-1-big-Data.db --raw-output /tmp/block0.bin

# Hex dump
$ xxd -l 500 /tmp/block0.bin
```

Compare with parser's offset tracking to identify where 374 bytes are consumed.

---

## Java Code References

### Complete Row Parsing (UnfilteredSerializer.java)

**deserializeOne()** - Lines ~380-420:
```java
int flags = in.readUnsignedByte();
if (isEndOfPartition(flags)) return null;
int extendedFlags = readExtendedFlags(in, flags);
if (kind(flags) == RANGE_TOMBSTONE_MARKER) {
    ClusteringBoundOrBoundary bound = ClusteringBoundOrBoundary.serializer.deserialize(...);
    return deserializeMarkerBody(in, header, bound);
} else {
    builder.newRow(Clustering.serializer.deserialize(in, version, header.clusteringTypes()));
    return deserializeRowBody(in, header, helper, flags, extendedFlags, builder);
}
```

**deserializeRowBody()** - Lines ~437-520:
```java
long rowSize = in.readUnsignedVInt();
long prevUnfilteredSize = in.readUnsignedVInt();
if (hasTimestamp(flags)) {
    long timestamp = header.readTimestamp(in);
    if (hasTTL(flags)) {
        int ttl = header.readTTL(in);
        int localDeletionTime = header.readLocalDeletionTime(in);
    }
}
if (hasDeletion(flags)) {
    long timestamp = header.readDeletionTime(in);
    int localDeletionTime = header.readLocalDeletionTime(in);
}
Columns columns = hasAllColumns(flags) ? header.columns()
    : Columns.serializer.deserializeSubset(header.columns(), in);
// ... parse cells
```

### Clustering Deserialization (Clustering.java)

**deserialize()** - Lines ~150-160:
```java
public Clustering<byte[]> deserialize(DataInputPlus in, int version, List<AbstractType<?>> types) {
    if (types.isEmpty())
        return ByteArrayAccessor.factory.clustering();  // EARLY RETURN - reads nothing
    byte[][] values = ClusteringPrefix.serializer.deserializeValuesWithoutSize(in, types.size(), version, types);
    return ByteArrayAccessor.factory.clustering(values);
}
```

### ClusteringPrefix Format (ClusteringPrefix.java)

**deserializeValuesWithoutSize()** - Lines ~595-615:
```java
byte[][] deserializeValuesWithoutSize(DataInputPlus in, int size, int version, List<AbstractType<?>> types) {
    assert size > 0;  // Only called when types.size() > 0
    byte[][] values = new byte[size][];
    int offset = 0;
    while (offset < size) {
        long header = in.readUnsignedVInt();  // 2 bits per element
        int limit = Math.min(size, offset + 32);
        while (offset < limit) {
            values[offset] = isNull(header, offset) ? null
                : (isEmpty(header, offset) ? EMPTY_BYTE_ARRAY
                : types.get(offset).readArray(in, maxValueSize));
            offset++;
        }
    }
    return values;
}
```

### Cell Format (Cell.java)

**deserialize()** - Lines ~400-450:
```java
int flags = in.readUnsignedByte();
int extendedFlags = (flags & EXTENDED_FLAG) != 0 ? in.readUnsignedByte() : 0;
long timestamp = (flags & USE_ROW_TIMESTAMP) == 0 ? header.readTimestamp(in) : rowLiveness.timestamp();
if ((flags & (IS_DELETED | IS_EXPIRING)) != 0) {
    if ((flags & USE_ROW_TTL) == 0)
        localDeletionTime = header.readLocalDeletionTime(in);
    if ((flags & IS_EXPIRING) != 0 && (flags & USE_ROW_TTL) == 0)
        ttl = header.readTTL(in);
}
boolean hasValue = (flags & HAS_EMPTY_VALUE) == 0;  // INVERTED!
if (!hasValue) return new BufferCell(..., EMPTY_BYTE_BUFFER, null);
V value = column.type.readValue(in, maxValueSize);
```

---

## Conclusion

### What We Know:
1. ✅ Row format specification matches Rust parser structure
2. ✅ Clustering prefix reads 0 bytes for zero-column tables
3. ✅ Cell parsing logic is correct
4. ⚠️ Delta encoding not implemented (doesn't affect offsets)
5. ❓ **374-byte gap origin unknown** - likely block or partition structure issue

### What We Need:
1. **Debug instrumentation** to track exact byte consumption
2. **Hex dump** of raw Data.db at problem offsets
3. **Block structure analysis** to verify partition boundaries
4. **Comparison** with sstabledump debug output

### Most Likely Root Cause:
The parser is correctly implementing row format, but **starting at wrong offset** due to:
- Misunderstanding of decompressed block structure
- Missing block-level metadata
- Incorrect partition header size calculation
- Wrong interpretation of compression format

**Next Step**: Add byte-level debug logging to identify where 374 bytes are consumed.

---

## Deliverables

1. ✅ **CASSANDRA_50_ROW_FORMAT_RESEARCH.md**: Complete format analysis with Java code analysis
2. ✅ **CASSANDRA_50_FORMAT_SPECIFICATION.md**: Field-by-field specification with pseudocode
3. ✅ **ISSUE_160_ROW_FORMAT_RESEARCH_SUMMARY.md**: This summary document with actionable findings

All three documents provide evidence that the **row format is correctly implemented**, but the **374-byte gap likely originates from block or partition structure** rather than row-level parsing.
