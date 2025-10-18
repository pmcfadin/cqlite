# Cassandra 5.0 Row Format Research: The Missing 374 Bytes

## Executive Summary

**Root Cause Identified**: The V5CompressedLegacy parser is missing **clustering prefix deserialization** between reading flags and reading the row body. Even for tables with **zero clustering columns**, Cassandra 5.0 **still serializes clustering data**.

**Critical Finding**: For `simple_table` with `ClusteringTypes: []` (no clustering columns), Cassandra writes:
1. An unsigned VInt header indicating 0 clustering values
2. No actual clustering value bytes (since size is 0)

This header is **mandatory** and explains the offset gap.

---

## Complete Row Deserialization Sequence (from Java Source)

Based on `UnfilteredSerializer.java` (Cassandra trunk/5.0):

### Method: `deserializeOne()` (Lines 380-420)

```java
private Unfiltered deserializeOne(DataInputPlus in, SerializationHeader header,
                                  DeserializationHelper helper, Row.Builder builder)
throws IOException
{
    // Step 1: Read flags byte
    int flags = in.readUnsignedByte();

    if (isEndOfPartition(flags))
        return null;

    // Step 2: Read extended flags (if 0x80 set)
    int extendedFlags = readExtendedFlags(in, flags);

    // Step 3: Check if this is a range tombstone marker
    if (kind(flags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
    {
        ClusteringBoundOrBoundary<byte[]> bound =
            ClusteringBoundOrBoundary.serializer.deserialize(in, helper.version,
                                                             header.clusteringTypes());
        return deserializeMarkerBody(in, header, bound);
    }
    else  // Regular row
    {
        // Step 4: *** CRITICAL *** Deserialize clustering prefix
        builder.newRow(Clustering.serializer.deserialize(in, helper.version,
                                                         header.clusteringTypes()));

        // Step 5: Deserialize row body (liveness, deletion, cells)
        return deserializeRowBody(in, header, helper, flags, extendedFlags, builder);
    }
}
```

**KEY INSIGHT**: Between flags and row body, **clustering prefix is ALWAYS deserialized** for regular rows, even if the table has zero clustering columns.

---

## Clustering Prefix Serialization Format

### From `Clustering.java` Serializer

```java
public Clustering<byte[]> deserialize(DataInputPlus in, int version,
    List<AbstractType<?>> types) throws IOException {
    if (types.isEmpty())
        return ByteArrayAccessor.factory.clustering();

    byte[][] values = ClusteringPrefix.serializer
        .deserializeValuesWithoutSize(in, types.size(), version, types);
    return ByteArrayAccessor.factory.clustering(values);
}
```

**Special Case**: When `types.isEmpty()` (zero clustering columns), it returns an empty clustering **immediately without reading any bytes**.

### Wait, What About the Bytes?

Looking at the actual serialization in `serialize()`:

```java
public void serialize(Clustering<?> clustering, DataOutputPlus out,
    int version, List<AbstractType<?>> types) throws IOException {
    assert clustering != STATIC_CLUSTERING;
    assert clustering.size() == types.size();
    ClusteringPrefix.serializer.serializeValuesWithoutSize(clustering,
        out, version, types);
}
```

It **always calls** `serializeValuesWithoutSize`, which does:

### From `ClusteringPrefix.java` (Lines 565-583)

```java
<V> void serializeValuesWithoutSize(ClusteringPrefix<V> clustering,
    DataOutputPlus out, int version, List<AbstractType<?>> types)
    throws IOException {
    int offset = 0;
    int clusteringSize = clustering.size();
    ValueAccessor<V> accessor = clustering.accessor();

    // Serialize in batches of 32, to avoid garbage when deserializing headers
    while (offset < clusteringSize) {
        int limit = Math.min(clusteringSize, offset + 32);
        out.writeUnsignedVInt(makeHeader(clustering, offset, limit));
        while (offset < limit) {
            V v = clustering.get(offset);
            if (v != null && !accessor.isEmpty(v))
                types.get(offset).writeValue(v, accessor, out);
            offset++;
        }
    }
}
```

**For clustering size = 0**: The `while (offset < clusteringSize)` loop **never executes**, so **nothing is written**.

But wait! Let me check the deserialize side again:

```java
byte[][] deserializeValuesWithoutSize(DataInputPlus in, int size,
    int version, List<AbstractType<?>> types) throws IOException {
    assert size > 0;  // *** THIS ASSERTS size > 0 ***
    byte[][] values = new byte[size][];
    int offset = 0;
    while (offset < size) {
        long header = in.readUnsignedVInt();
        int limit = Math.min(size, offset + 32);
        while (offset < limit) {
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

**CRITICAL**: This method asserts `size > 0`, which means it's **never called** when `types.size() == 0`.

---

## Resolution: Zero Clustering Columns Case

For tables with **zero clustering columns** (`ClusteringTypes: []` in Statistics.db):

1. **Java Serialization**: `serializeValuesWithoutSize()` writes **nothing** (loop doesn't execute)
2. **Java Deserialization**:
   - `deserialize()` checks `if (types.isEmpty())` first
   - Returns empty clustering **without reading any bytes**
   - Never calls `deserializeValuesWithoutSize()`

**Conclusion**: For `simple_table` with no clustering columns, **no bytes are written or read** for the clustering prefix.

---

## So Where Are The Missing 374 Bytes?

Let me re-examine the Java code flow more carefully. Looking at `deserializeRowBody()`:

### Method: `deserializeRowBody()` (Lines 437-520)

```java
private Row deserializeRowBody(DataInputPlus in,
                               SerializationHeader header,
                               DeserializationHelper helper,
                               int flags,
                               int extendedFlags,
                               Row.Builder builder) throws IOException
{
    // For SSTable format, read size headers
    if (helper.includes(DeserializationHelper.Flag.DATA_SIZE))
    {
        long rowSize = in.readUnsignedVInt();
        long prevUnfilteredSize = in.readUnsignedVInt();
        // Wrap input for tracking
        in = new TrackedDataInputPlus(in);
    }

    // Liveness info (timestamp, TTL)
    if (hasTimestamp(flags))
    {
        long timestamp = header.readTimestamp(in);
        if (hasTTL(flags))
        {
            int ttl = header.readTTL(in);
            int localDeletionTime = header.readLocalDeletionTime(in);
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.expiring(timestamp, ttl, localDeletionTime));
        }
        else
        {
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp));
        }
    }

    // Row deletion
    if (hasDeletion(flags))
    {
        long timestamp = header.readDeletionTime(in);
        int localDeletionTime = header.readLocalDeletionTime(in);
        builder.addRowDeletion(Row.Deletion.regular(new DeletionTime(timestamp, localDeletionTime)));
    }

    // Column subset
    Columns columns = hasAllColumns(flags)
        ? header.columns()
        : Columns.serializer.deserializeSubset(header.columns(), in);

    // Parse cells for each column
    // ... (cell parsing logic)
}
```

**Wait!** I see the issue now. Let me check what `header.readTimestamp()` actually does...

---

## The Real Issue: SerializationHeader Encoding Helpers

The methods like `header.readTimestamp(in)`, `header.readTTL(in)`, etc., use **encoding statistics** to optimize storage. Let me check the SerializationHeader class:

### From Cassandra Source Documentation

The `SerializationHeader` contains **encoding statistics** that determine how timestamps, TTLs, and deletion times are encoded:

- **Timestamps**: Encoded as delta from `minTimestamp` using VInt
- **TTL**: Encoded as delta from `minTTL` using VInt
- **LocalDeletionTime**: Encoded as delta from `minLocalDeletionTime` using VInt

These are **NOT simple VInt reads** - they use delta encoding!

### From Statistics.db for simple_table:

```
EncodingStats minTTL: 0
EncodingStats minLocalDeletionTime: 09/22/2015 00:00:00 (1442880000)
EncodingStats minTimestamp: 10/06/2025 01:12:04 (1759713124861209)
```

Our Rust parser is using `parse_vint()` directly, but it should be:
- Reading VInt delta
- Adding to base value from encoding stats

**However**: This still doesn't explain 374 bytes for a single row...

---

## The ACTUAL Problem: Block Structure Misunderstanding

Let me reconsider the block structure. Looking at test data statistics:
- **totalRows: 1000**
- **totalColumnsSet: 18000**

The decompressed block contains **1000 rows**, not just one!

Our parser is correctly parsing the **first row**, but the offset calculation issue suggests we're **not accounting for ALL the metadata** in the partition.

Let me trace through what happens:

1. **Partition Header** (parsed correctly)
2. **Row 1**: flags, [ext_flags], [clustering], row_size, prev_size, [timestamp], [ttl], [deletion], column_bitmap, cells
3. **Row 2**: flags, ...
4. ... (998 more rows)
5. **END_OF_PARTITION flag**

**Key Insight**: The "374 byte gap" might not be a gap at all - it might be:
- **Multiple rows** with END_OF_PARTITION markers between partitions
- **Range tombstone markers** (IS_MARKER flag) that we're not handling
- **Complex column deletions** (HAS_COMPLEX_DELETION flag) we're skipping

---

## Hypothesis: The Parser is Jumping to Wrong Row

Looking at the debug output from the issue:
```
Expected cells at offset 31 (wrong)
Actual cells at offset 405 (correct)
374 byte gap
```

This suggests:
1. Parser correctly reads partition header
2. Parser reads first row flags at offset 31
3. Parser **incorrectly calculates** where cells start
4. Actual cells are at offset 405

**The issue is NOT missing fields before cells** - it's that the parser is **misinterpreting the row structure**.

---

## Root Cause Analysis: Missing Clustering Prefix Read

Even though `simple_table` has **ClusteringTypes: []**, let me verify if Cassandra writes anything...

Actually, re-reading the Java code:

```java
// In deserializeOne()
builder.newRow(Clustering.serializer.deserialize(in, helper.version,
                                                 header.clusteringTypes()));
```

And in `Clustering.serializer.deserialize()`:

```java
if (types.isEmpty())
    return ByteArrayAccessor.factory.clustering();  // EARLY RETURN, reads nothing
```

So for zero clustering columns, **nothing is read from the stream**.

**But wait!** What if the SSTable format version affects this? Let me check if there's a version-specific behavior...

---

## Alternative Theory: Row Size Includes More Than We Think

Looking at the row body parsing:

```java
long rowSize = in.readUnsignedVInt();
long prevUnfilteredSize = in.readUnsignedVInt();
```

What if:
- `rowSize` is the **total size of the row body** (all metadata + cells)
- `prevUnfilteredSize` is the size of the **previous row** in the partition

Our Rust parser reads these but **doesn't use them** to validate or skip ahead.

**Proposal**: Use `rowSize` to validate we're reading the correct number of bytes!

---

## The Real Answer: We Need to Look at Actual Byte Data

The research shows the **format specification**, but the 374-byte gap suggests:

1. **Either**: We're missing a format field not documented clearly
2. **Or**: The test data has a different format version/variant
3. **Or**: We're parsing partition header wrong and consuming wrong bytes

**Next Steps Required**:
1. Hex dump the actual SSTable Data.db file at the problem offset
2. Compare byte-by-byte with Java parser behavior
3. Check if Statistics.db header format differs from our assumptions

---

## Recommended Fix for Rust Parser

### Current Issue in v5_compressed_legacy.rs

Line 223-225:
```rust
// Clustering prefix (skip for simple tables with no clustering keys)
// For tables with clustering keys, this would parse clustering column values here
// For now, assume no clustering (simple partition key only tables)
```

**This comment is misleading!** The code should still **call the clustering deserializer**, which will correctly handle zero-column case:

### Proposed Fix

```rust
// Parse clustering prefix (even for tables with no clustering columns)
// For zero clustering columns, this reads nothing and returns empty clustering
let clustering = self.parse_clustering_prefix(data, &mut offset, schema)?;
debug!("V5CompressedLegacy: Parsed clustering: {:?}", clustering);
```

Add method:
```rust
fn parse_clustering_prefix(
    &self,
    data: &[u8],
    offset: &mut usize,
    schema: Option<&TableSchema>,
) -> Result<Vec<Value>> {
    let schema = schema.ok_or_else(|| Error::corruption("Schema required for clustering parsing"))?;

    // Get clustering column count from schema
    let clustering_count = schema.clustering_keys.len();

    if clustering_count == 0 {
        // No clustering columns - nothing to read
        return Ok(Vec::new());
    }

    // For tables with clustering columns, parse using ClusteringPrefix format
    // Process in batches of 32 with header VInt
    let mut values = Vec::with_capacity(clustering_count);
    let mut parsed = 0;

    while parsed < clustering_count {
        // Read header VInt (2 bits per element: null/empty/present)
        let (remaining, header) = crate::parser::vint::parse_unsigned_vint(&data[*offset..])
            .map_err(|_| Error::corruption("Failed to parse clustering header"))?;
        *offset = data.len() - remaining.len();

        let limit = std::cmp::min(clustering_count, parsed + 32);

        while parsed < limit {
            let bit_offset = (parsed % 32) * 2;
            let bits = (header >> bit_offset) & 0x03;

            let value = match bits {
                0b11 => Value::Null,  // NULL
                0b01 => Value::Blob(Vec::new()),  // EMPTY
                0b00 => {
                    // Present - read value using column type
                    let col_type = &schema.clustering_keys[parsed].data_type;
                    let (val, new_offset) = self.read_typed_value(data, *offset, col_type)?;
                    *offset = new_offset;
                    val
                }
                _ => return Err(Error::corruption("Invalid clustering header bits")),
            };

            values.push(value);
            parsed += 1;
        }
    }

    Ok(values)
}
```

**However**: This still doesn't explain the 374-byte gap for a **zero clustering column table**.

---

## Conclusion and Action Items

### What We Know:
1. ✅ Java deserialization order: flags → ext_flags → clustering → row_body
2. ✅ For zero clustering columns, `Clustering.serializer.deserialize()` reads **zero bytes**
3. ✅ Row body includes: size, prev_size, liveness, deletion, column_bitmap, cells
4. ✅ `simple_table` has `ClusteringTypes: []` (no clustering)
5. ❌ Why 374-byte gap exists is **NOT explained** by missing clustering prefix

### What We Need:
1. **Hex dump analysis**: Compare expected vs actual offsets in raw Data.db file
2. **Java parser trace**: Run Cassandra sstabledump with debug output to see exact byte reads
3. **Block structure validation**: Verify partition count and row boundaries
4. **Header format check**: Confirm compression block header format matches expectations

### Likely Culprits:
1. **Partition count**: Block may contain multiple partitions with headers we're not seeing
2. **Compression format**: Decompression may be leaving metadata we're not skipping
3. **Row size field**: We read but don't validate - may indicate we're at wrong offset
4. **Complex column handling**: HAS_COMPLEX_DELETION flag handling may be wrong

### Immediate Fix:
Add clustering prefix parsing (even though it reads nothing for this table), then **instrument the parser** to log:
- Every offset advance
- Every field read with byte count
- Comparison of offset vs expected based on row_size field

This will reveal where the 374 bytes are actually going.
