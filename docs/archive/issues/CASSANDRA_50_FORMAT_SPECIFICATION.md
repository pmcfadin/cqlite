# Cassandra 5.0 Row Format Specification (Complete)

## Source: Apache Cassandra 5.0 trunk (UnfilteredSerializer.java)

---

## 1. Complete Deserialization Sequence

### High-Level Flow (deserializeOne method)

```java
// Source: UnfilteredSerializer.java, lines ~380-420
private Unfiltered deserializeOne(DataInputPlus in, SerializationHeader header,
                                  DeserializationHelper helper, Row.Builder builder)
throws IOException
{
    assert builder.isSorted();

    // STEP 1: Read flags byte
    int flags = in.readUnsignedByte();
    if (isEndOfPartition(flags))
        return null;

    // STEP 2: Read extended flags (if EXTENSION_FLAG 0x80 set)
    int extendedFlags = readExtendedFlags(in, flags);

    // STEP 3: Branch based on unfiltered kind
    if (kind(flags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
    {
        // Range tombstone marker path
        ClusteringBoundOrBoundary<byte[]> bound =
            ClusteringBoundOrBoundary.serializer.deserialize(in, helper.version,
                                                             header.clusteringTypes());
        return deserializeMarkerBody(in, header, bound);
    }
    else
    {
        // Regular row path
        if (isStatic(extendedFlags))
            throw new IOException("Corrupt flags value for unfiltered partition...");

        // STEP 4: Deserialize clustering prefix
        builder.newRow(Clustering.serializer.deserialize(in, helper.version,
                                                         header.clusteringTypes()));

        // STEP 5: Deserialize row body
        return deserializeRowBody(in, header, helper, flags, extendedFlags, builder);
    }
}
```

**Key Observation**: Steps are **strictly ordered** and **mandatory** (except clustering may read 0 bytes).

---

## 2. Clustering Prefix Format

### Clustering.serializer.deserialize()

```java
// Source: Clustering.java, lines ~150-160
public Clustering<byte[]> deserialize(DataInputPlus in, int version,
    List<AbstractType<?>> types) throws IOException
{
    if (types.isEmpty())
        return ByteArrayAccessor.factory.clustering();

    byte[][] values = ClusteringPrefix.serializer
        .deserializeValuesWithoutSize(in, types.size(), version, types);
    return ByteArrayAccessor.factory.clustering(values);
}
```

**Critical**: For `types.isEmpty()` (zero clustering columns):
- Returns **immediately** without reading any bytes
- Creates empty clustering object

### For Non-Empty Clustering: deserializeValuesWithoutSize()

```java
// Source: ClusteringPrefix.java, lines ~595-615
byte[][] deserializeValuesWithoutSize(DataInputPlus in, int size,
    int version, List<AbstractType<?>> types) throws IOException
{
    assert size > 0;  // Only called when size > 0
    byte[][] values = new byte[size][];
    int offset = 0;

    // Process in batches of 32
    while (offset < size)
    {
        // Read header VInt (2 bits per element)
        long header = in.readUnsignedVInt();
        int limit = Math.min(size, offset + 32);

        while (offset < limit)
        {
            // Decode 2-bit status for each element
            values[offset] = isNull(header, offset)
                ? null
                : (isEmpty(header, offset) ? ByteArrayUtil.EMPTY_BYTE_ARRAY
                : types.get(offset).readArray(in,
                    DatabaseDescriptor.getMaxValueSize()));
            offset++;
        }
    }
    return values;
}
```

**Format Breakdown**:
- **Batch size**: 32 elements per batch
- **Header**: Unsigned VInt with 2 bits per element
  - `00`: Present (value bytes follow)
  - `01`: Empty (no bytes, return empty byte array)
  - `11`: Null (no bytes, return null)
- **Values**: Only written for "present" (00) elements, using type-specific serialization

---

## 3. Row Body Format (deserializeRowBody)

```java
// Source: UnfilteredSerializer.java, lines ~437-520
private Row deserializeRowBody(DataInputPlus in,
                               SerializationHeader header,
                               DeserializationHelper helper,
                               int flags,
                               int extendedFlags,
                               Row.Builder builder) throws IOException
{
    // FIELD 1 & 2: Row size tracking (SSTable format only)
    if (helper.includes(DeserializationHelper.Flag.DATA_SIZE))
    {
        long rowSize = in.readUnsignedVInt();
        long prevUnfilteredSize = in.readUnsignedVInt();
        in = new TrackedDataInputPlus(in);
    }

    // FIELD 3-5: Primary key liveness info (if HAS_TIMESTAMP 0x04)
    if (hasTimestamp(flags))
    {
        long timestamp = header.readTimestamp(in);  // Delta-encoded VInt
        if (hasTTL(flags))  // if HAS_TTL 0x08
        {
            int ttl = header.readTTL(in);  // Delta-encoded VInt
            int localDeletionTime = header.readLocalDeletionTime(in);  // Delta-encoded VInt
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.expiring(timestamp, ttl, localDeletionTime));
        }
        else
        {
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp));
        }
    }

    // FIELD 6-7: Row deletion (if HAS_DELETION 0x10)
    if (hasDeletion(flags))
    {
        long timestamp = header.readDeletionTime(in);  // Delta-encoded VInt
        int localDeletionTime = header.readLocalDeletionTime(in);  // Delta-encoded VInt
        builder.addRowDeletion(Row.Deletion.regular(new DeletionTime(timestamp, localDeletionTime)));
    }

    // FIELD 8: Column subset bitmap (if NOT HAS_ALL_COLUMNS 0x20)
    Columns columns = hasAllColumns(flags)
        ? header.columns()
        : Columns.serializer.deserializeSubset(header.columns(), in);

    // FIELD 9: Column data (cells)
    final LivenessInfo rowLiveness = builder.partitionKeyLivenessInfo();
    for (int i = 0; i < columns.columnCount(); i++)
    {
        ColumnMetadata column = columns.getColumn(i);
        if (column.isSimple())
            readSimpleColumn(column, in, header, helper, builder, rowLiveness);
        else
            readComplexColumn(column, in, header, helper, builder, rowLiveness);
    }

    return builder.build();
}
```

### Field Summary (Row Body):

| Field | Condition | Type | Purpose |
|-------|-----------|------|---------|
| rowSize | SSTable format | Unsigned VInt | Total bytes in row body |
| prevUnfilteredSize | SSTable format | Unsigned VInt | Size of previous unfiltered |
| timestamp | HAS_TIMESTAMP (0x04) | Delta VInt | Primary key liveness timestamp |
| ttl | HAS_TTL (0x08) | Delta VInt | Time-to-live |
| localDeletionTime | HAS_TTL (0x08) | Delta VInt | When TTL expires locally |
| deletionTimestamp | HAS_DELETION (0x10) | Delta VInt | Row deletion timestamp |
| deletionLocalTime | HAS_DELETION (0x10) | Delta VInt | Row deletion local time |
| columnBitmap | NOT HAS_ALL_COLUMNS (0x20) | Varies | Which columns present |
| cells | Always (for present columns) | Varies | Cell data |

---

## 4. Delta Encoding (SerializationHeader)

### header.readTimestamp()

```java
// Source: SerializationHeader.java
public long readTimestamp(DataInputPlus in) throws IOException
{
    return timestampSerializer.deserialize(in);
}

// TimestampSerializer (inner class)
public long deserialize(DataInputPlus in) throws IOException
{
    long delta = VIntCoding.readVInt(in);
    return baseTimestamp + delta;
}
```

**Format**: VInt delta + base value from encoding stats

**Similar for**:
- `readTTL()`: Delta from `minTTL`
- `readLocalDeletionTime()`: Delta from `minLocalDeletionTime`
- `readDeletionTime()`: Delta from `minTimestamp`

---

## 5. Cell Format

### Simple Cell (readSimpleColumn)

```java
// Source: UnfilteredSerializer.java, lines ~650-670
private void readSimpleColumn(ColumnMetadata column, DataInputPlus in,
                              SerializationHeader header, DeserializationHelper helper,
                              Row.Builder builder, LivenessInfo rowLiveness) throws IOException
{
    if (helper.includes(DeserializationHelper.Flag.HAS_COMPLEX_DELETION))
    {
        if (in.readBoolean())  // Complex deletion present?
        {
            long timestamp = header.readDeletionTime(in);
            int localDeletionTime = header.readLocalDeletionTime(in);
            // ... apply deletion
        }
    }

    Cell<?> cell = Cell.serializer.deserialize(in, rowLiveness, column, header, helper);
    if (cell != null)
        builder.addCell(cell);
}
```

### Cell.serializer.deserialize()

```java
// Source: Cell.java, lines ~400-450
public Cell<V> deserialize(DataInputPlus in, LivenessInfo rowLiveness,
                           ColumnMetadata column, SerializationHeader header,
                           DeserializationHelper helper) throws IOException
{
    int flags = in.readUnsignedByte();

    // Extended flags (if 0x40 set)
    int extendedFlags = (flags & EXTENDED_FLAG) != 0 ? in.readUnsignedByte() : 0;

    // Timestamp (if NOT USE_ROW_TIMESTAMP 0x08)
    long timestamp = (flags & USE_ROW_TIMESTAMP) == 0
        ? header.readTimestamp(in)
        : rowLiveness.timestamp();

    // TTL/Deletion (complex conditions based on flags)
    int ttl = LivenessInfo.NO_TTL;
    int localDeletionTime = LivenessInfo.NO_EXPIRATION_TIME;

    if ((flags & (IS_DELETED | IS_EXPIRING)) != 0)
    {
        if ((flags & USE_ROW_TTL) == 0)
            localDeletionTime = header.readLocalDeletionTime(in);

        if ((flags & IS_EXPIRING) != 0 && (flags & USE_ROW_TTL) == 0)
            ttl = header.readTTL(in);
    }

    // Value bytes (if NOT IS_DELETED 0x01)
    boolean isDeleted = (flags & IS_DELETED) != 0;
    if (isDeleted)
        return new BufferCell(column, timestamp, ttl, localDeletionTime, null, null);

    // Check for empty/null value
    boolean hasValue = (flags & HAS_EMPTY_VALUE) == 0;  // INVERTED LOGIC!
    if (!hasValue)
        return new BufferCell(column, timestamp, ttl, localDeletionTime,
                             ByteBufferUtil.EMPTY_BYTE_BUFFER, null);

    // Read value bytes
    V value = column.type.readValue(in, helper.getMaxValueSize());
    return new BufferCell(column, timestamp, ttl, localDeletionTime, value, null);
}
```

### Cell Flags:

| Flag | Bit | Meaning |
|------|-----|---------|
| IS_DELETED | 0x01 | Cell is tombstone |
| IS_EXPIRING | 0x02 | Cell has TTL |
| HAS_EMPTY_VALUE | 0x04 | **INVERTED**: Flag=0 means has value, Flag=1 means empty |
| USE_ROW_TIMESTAMP | 0x08 | Use row's timestamp (don't read separate) |
| USE_ROW_TTL | 0x10 | Use row's TTL (don't read separate) |
| HAS_NULL_VALUE | 0x20 | Value is null |
| EXTENDED_FLAG | 0x40 | Extended flags byte follows |

---

## 6. Complete Row Format Pseudocode

```rust
fn parse_row(data: &[u8], mut offset: usize, flags: u8, ext_flags: Option<u8>,
             schema: &TableSchema, header: &SerializationHeader) -> Result<(Value, usize)>
{
    // 1. Clustering prefix
    if schema.clustering_keys.is_empty() {
        // No bytes read
    } else {
        offset = parse_clustering_prefix(data, offset, schema)?;
    }

    // 2. Row sizes (SSTable format)
    let (remaining, row_size) = parse_unsigned_vint(&data[offset..])?;
    offset = data.len() - remaining.len();

    let (remaining, prev_size) = parse_unsigned_vint(&data[offset..])?;
    offset = data.len() - remaining.len();

    // 3. Liveness info (if HAS_TIMESTAMP)
    if flags & HAS_TIMESTAMP != 0 {
        let (remaining, ts_delta) = parse_vint(&data[offset..])?;
        offset = data.len() - remaining.len();
        let timestamp = header.min_timestamp + ts_delta;

        if flags & HAS_TTL != 0 {
            let (remaining, ttl_delta) = parse_vint(&data[offset..])?;
            offset = data.len() - remaining.len();
            let ttl = header.min_ttl + ttl_delta;

            let (remaining, ldt_delta) = parse_vint(&data[offset..])?;
            offset = data.len() - remaining.len();
            let local_deletion_time = header.min_local_deletion_time + ldt_delta;
        }
    }

    // 4. Row deletion (if HAS_DELETION)
    if flags & HAS_DELETION != 0 {
        let (remaining, del_ts_delta) = parse_vint(&data[offset..])?;
        offset = data.len() - remaining.len();

        let (remaining, del_ldt_delta) = parse_vint(&data[offset..])?;
        offset = data.len() - remaining.len();
    }

    // 5. Column bitmap (if NOT HAS_ALL_COLUMNS)
    let column_bitmap = if flags & HAS_ALL_COLUMNS == 0 {
        let (remaining, bitmap) = parse_vint(&data[offset..])?;
        offset = data.len() - remaining.len();
        Some(bitmap)
    } else {
        None
    };

    // 6. Parse cells
    let cells = parse_cells(data, offset, column_bitmap, schema, header)?;

    Ok((Value::Map(cells), offset))
}
```

---

## 7. Test Case Analysis: simple_table

### Schema (from Statistics.db):
- **KeyType**: UUIDType (16 bytes)
- **ClusteringTypes**: `[]` (empty - no clustering columns)
- **RegularColumns**: 18 columns (account_balance, created, small_number, ...)
- **StaticColumns**: (none)

### Encoding Stats:
- **minTTL**: 0
- **minLocalDeletionTime**: 1442880000
- **minTimestamp**: 1759713124861209

### Expected Row Format:
```
[flags: 1 byte]
[ext_flags: 0-1 bytes if 0x80 set]
[clustering: 0 bytes for empty clustering]
[row_size: VInt]
[prev_size: VInt]
[timestamp_delta: VInt if HAS_TIMESTAMP]
[ttl_delta: VInt if HAS_TTL]
[local_deletion_time_delta: VInt if HAS_TTL]
[deletion_ts_delta: VInt if HAS_DELETION]
[deletion_ldt_delta: VInt if HAS_DELETION]
[column_bitmap: VInt if NOT HAS_ALL_COLUMNS]
[cell_1] [cell_2] ... [cell_N]
```

### For Row with HAS_ALL_COLUMNS | HAS_TIMESTAMP (flags = 0x24):
```
flags: 0x24 (0010 0100)
  - HAS_TIMESTAMP (0x04) ✓
  - HAS_ALL_COLUMNS (0x20) ✓

[1 byte: flags = 0x24]
[VInt: row_size]
[VInt: prev_size]
[VInt: timestamp_delta]  (since HAS_TIMESTAMP set)
[VInt: column_bitmap]     // Wait - should NOT be present if HAS_ALL_COLUMNS!
[cells...]
```

**CRITICAL ERROR IN RUST PARSER**: Line 274-282 reads column bitmap even when `HAS_ALL_COLUMNS` is set!

```rust
// BUG: This reads bitmap when flags & HAS_ALL_COLUMNS == 0
// But HAS_ALL_COLUMNS=0 means "not all columns", so we SHOULD read
// If HAS_ALL_COLUMNS=0x20 is SET, we should NOT read bitmap
let column_bitmap = if flags & HAS_ALL_COLUMNS == 0 {
    // This condition is CORRECT: flag NOT set = read bitmap
    // ...
}
```

Actually, the logic looks correct. Let me re-read...

`flags & HAS_ALL_COLUMNS == 0` means:
- Flag 0x20 is NOT set
- Therefore NOT all columns present
- Therefore we SHOULD read bitmap

`flags & HAS_ALL_COLUMNS != 0` means:
- Flag 0x20 IS set
- Therefore all columns present
- Therefore NO bitmap needed

**The Rust logic is correct!**

---

## 8. The Missing 374 Bytes: Remaining Questions

After complete format analysis:

### What We've Confirmed:
1. ✅ Clustering prefix for zero columns reads **0 bytes**
2. ✅ Row body format matches Rust parser
3. ✅ Cell format matches Rust parser
4. ✅ Flag logic is correct

### What Remains Unknown:
1. ❓ Why 374-byte offset gap?
2. ❓ Is decompression adding metadata?
3. ❓ Are there multiple partitions in block?
4. ❓ Is block header format different?

### Next Investigation Steps:
1. **Hex dump**: Extract raw bytes at offsets 31 and 405
2. **Block structure**: Verify partition boundaries
3. **Compression**: Check if decompression leaves headers
4. **Row size validation**: Use `row_size` field to validate parsing

### Hypothesis:
The 374 bytes may be in the **partition header** or **block header**, not in the row format itself. The parser may be correctly parsing the row, but starting at the wrong offset due to:
- Misunderstanding partition header size
- Missing block-level metadata
- Incorrect decompression offset calculation

**Recommendation**: Instrument the parser to log cumulative byte counts and compare against actual Data.db offsets.
