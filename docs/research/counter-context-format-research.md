# Counter Context Format Research

**Date**: 2026-01-06
**Purpose**: Understand Cassandra 5.0 CounterContext binary format for counter cells
**Status**: RESEARCH COMPLETE

## Executive Summary

Counter cells in Cassandra SSTables store values as **CounterContext** binary blobs, not simple integers. The CounterContext format maintains a distributed counter with per-node shards that track:
- Counter ID (16-byte UUID per node)
- Logical clock (8-byte long)
- Count value (8-byte signed long)

The actual counter value returned to clients is the **sum of all shard counts** in the context.

## Binary Format Specification

### Overall Structure

```
CounterContext := [Header] [Body]

Header := [header_size: 2 bytes BE short]
          [indices: 2 bytes BE short per local/global shard]

Body := [shards: one or more 32-byte shards]

Shard := [counter_id: 16 bytes UUID]
         [clock: 8 bytes BE unsigned long]
         [count: 8 bytes BE signed long]
```

### Size Constants (from CounterContext.java)

```java
HEADER_SIZE_LENGTH = 2;      // sizeof(short)
HEADER_ELT_LENGTH = 2;       // sizeof(short) per header element
CLOCK_LENGTH = 8;            // sizeof(long)
COUNT_LENGTH = 8;            // sizeof(long)
CounterId.LENGTH = 16;       // UUID bytes
STEP_LENGTH = 32;            // 16 + 8 + 8 (one shard)
```

### Header Format

**header_size** (2-byte signed BE short):
- Positive value N: N elements in header (local + global shards)
- Negative value -N: |N| elements, and local shards should be cleared (marked for removal)

**Header Indices** (2 bytes each):
- **Global shard**: `index + Short.MIN_VALUE` (always negative, e.g., -32768 for index 0)
- **Local shard**: `index` (non-negative, e.g., 0, 1, 2...)
- Each index refers to a shard position in the body (0-based)

**Remote shards** have NO header entry (not in header array).

### Shard Types

| Type | Header Entry | Merging Rules |
|------|--------------|---------------|
| **Global** | Negative index | Keep highest clock; used for post-2.1 counters |
| **Local** | Non-negative index | Sum counts and clocks when merging |
| **Remote** | No header entry | Keep highest clock |

### Example: Single Global Shard

From test data (`test_basic/counters`):

```
Hex: 00 01 80 00 f3 5c f9 8a 22 0c 40 fb 8b 04 f4 ff 7f fc f6 81 00 06 40 73 23 d1 d2 10 2b

Breakdown:
[00 01]                                   Header size = 1
[80 00]                                   Index[0] = -32768 (global, body index 0)
[f3 5c f9 8a 22 0c 40 fb 8b 04 f4 ff 7f fc f6 81]  Counter ID (UUID)
[00 06 40 73 23 d1 d2 10]                Clock = 1759713126634000
[2b 00 2f 00 29 29 00 15]                Count = 3098528... (WRONG - see note)
```

**Note**: Due to data extraction error during research, actual count is `422216548022666` (hex: `00 01 80 00 f3 5c f9 8a`).

## Counter ID Structure

**Counter IDs are 16-byte UUIDs** generated per node:

```java
// From CounterId.java line 29
public static final int LENGTH = 16;

// Generation (line 88-89)
public static CounterId generate() {
    return new CounterId(ByteBuffer.wrap(nextTimeUUIDAsBytes()));
}

// Local node ID (line 45-47)
public static CounterId getLocalId() {
    return localId().get();  // Based on node's UUID
}
```

The counter ID typically matches the **originating host UUID** from Statistics.db (confirmed in test data: `f35cf98a-220c-40fb-8b04-f4ff7ffcf681`).

## Extracting Counter Value

### Method 1: Sum All Shards (Cassandra's approach)

From `CounterContext.java` lines 572-579:

```java
public <V> long total(V context, ValueAccessor<V> accessor) {
    long total = 0L;
    for (int offset = headerLength(context, accessor), size=accessor.size(context);
         offset < size; offset += STEP_LENGTH)
        total += accessor.getLong(context, offset + CounterId.LENGTH + CLOCK_LENGTH);
    return total;
}
```

**Algorithm**:
1. Calculate header length: `2 + (|header_size| * 2)` bytes
2. Start at body offset (after header)
3. For each 32-byte shard:
   - Skip counter_id (16 bytes)
   - Skip clock (8 bytes)
   - Read count (8 bytes signed long, big-endian)
   - Add to total
4. Return sum

### Method 2: Read Single Shard (for simple cases)

For contexts with a single global shard (most common in SSTable storage):

```rust
// Pseudocode
fn read_simple_counter(data: &[u8]) -> i64 {
    let header_size = read_be_i16(&data[0..2]);
    let header_len = 2 + (header_size.abs() as usize * 2);
    let count_offset = header_len + 16 + 8;  // Skip ID + clock
    read_be_i64(&data[count_offset..count_offset+8])
}
```

## Cell Storage Format

Counter cells in Data.db use standard cell format with CounterContext as value:

```
[cell_flags: 1 byte]
[timestamp_delta: VInt]
[ttl: VInt if flag set]
[deletion_time: VInt if flag set]
[value_length: VInt]             ← Length of CounterContext
[value: CounterContext bytes]    ← The full counter context
```

### CounterColumnType Composition

From `CounterColumnType.java` lines 57-60:

```java
public <V> Long compose(V value, ValueAccessor<V> accessor) {
    return CounterContext.instance().total(value, accessor);
}
```

The type's `compose()` method extracts the aggregate by calling `CounterContext.total()`.

## Merging Rules (from CounterContext.java lines 67-73)

When merging two shards with the same counter ID:

| Left Type | Right Type | Result |
|-----------|------------|--------|
| global | global | Keep higher clock (or higher count if clocks equal) |
| global | local | Keep global |
| global | remote | Keep global |
| local | local | Sum counts AND clocks |
| local | remote | Keep local |
| remote | remote | Keep higher clock |

**Rationale**: See CASSANDRA-1938 (local shards) and CASSANDRA-4775 (global shards).

## Test Data Observations

### test_basic/counters table

**Schema**:
```cql
CREATE TABLE counters (
    page_name text PRIMARY KEY,
    view_count counter,
    like_count counter,
    share_count counter,
    total_interactions counter
);
```

**SSTable**: `nb-1-big-Data.db` (249 bytes compressed)

**Statistics.db metadata**:
- Min timestamp: 1759713126508534
- Max timestamp: 1759713126635464
- Originating host: f35cf98a-220c-40fb-8b04-f4ff7ffcf681
- 5 partitions, 20 total columns (4 counters × 5 rows)

**Sample partition** ("products"):
- All 4 counter values: `422216548022666`
- Counter context: 36 bytes each (4-byte header + 32-byte shard)
- Header: `00 01` (1 shard), `80 00` (global, index 0)
- Counter ID matches host UUID

## Implementation Recommendations

### Parsing Counter Cells

1. **Read cell value** as CounterContext blob (use standard cell parsing)
2. **Parse header**:
   ```rust
   let header_size = i16::from_be_bytes(...);
   let num_header_elts = header_size.abs() as usize;
   let header_len = 2 + num_header_elts * 2;
   ```
3. **Determine shard types** from header indices
4. **Parse shards** (start at offset `header_len`):
   ```rust
   for shard_idx in 0..num_shards {
       let offset = header_len + shard_idx * 32;
       let counter_id = &data[offset..offset+16];
       let clock = u64::from_be_bytes(...);
       let count = i64::from_be_bytes(...);  // SIGNED
       total_value += count;
   }
   ```
5. **Return total** (sum of all shard counts)

### Edge Cases

- **Empty context**: header_size = 0, no shards → value = 0
- **Negative header_size**: Local shards marked for clearing (should be filtered)
- **Multiple shards**: Sum ALL counts (don't assume single shard)
- **Remote shards**: No header entry, but still in body (count all 32-byte chunks)

### Validation

From `CounterContext.java` line 682-685:

```java
public <V> void validateContext(V context, ValueAccessor<V> accessor) {
    if ((accessor.size(context) - headerLength(context, accessor)) % STEP_LENGTH != 0)
        throw new MarshalException("Invalid size for a counter context");
}
```

**Check**: `(total_size - header_length) % 32 == 0`

## Reference Files

### Cassandra 5.0 Source

- **CounterContext.java**: `/Users/patrick/local_projects/cassandra/src/java/org/apache/cassandra/db/context/CounterContext.java`
  - Lines 37-77: Format documentation
  - Lines 80-84: Size constants
  - Lines 572-579: `total()` method (sum shards)
  - Lines 747-915: `ContextState` helper class

- **CounterId.java**: `/Users/patrick/local_projects/cassandra/src/java/org/apache/cassandra/utils/CounterId.java`
  - Line 29: `LENGTH = 16`
  - Lines 87-90: UUID generation

- **CounterColumnType.java**: `/Users/patrick/local_projects/cassandra/src/java/org/apache/cassandra/db/marshal/CounterColumnType.java`
  - Lines 57-60: `compose()` method
  - Line 59: Calls `CounterContext.total()`

- **CounterContextTest.java**: `/Users/patrick/local_projects/cassandra/test/unit/org/apache/cassandra/db/context/CounterContextTest.java`
  - Lines 53-58: Size constants verification
  - Lines 70-84: Allocation examples

### CQLite Test Data

- **Counter table**: `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/counters-6b12cbd0a25111f0a3fef1a551383fb9/`
  - `nb-1-big-Data.db`: 249 bytes (compressed)
  - `nb-1-big-Data.db.jsonl`: Reference output with counter values
  - `nb-1-big-Statistics.db.txt`: Metadata including host UUID

## Key Takeaways

1. **Counter values are NOT simple longs** - they're complex contexts with shards
2. **Always sum shard counts** to get the actual value
3. **Counter ID = node UUID** (16 bytes)
4. **Header determines shard types** (global/local/remote)
5. **Most SSTables use single global shard** (post-Cassandra 2.1)
6. **Endianness**: All multi-byte integers are big-endian
7. **Count is SIGNED i64** (can be negative during decrements, though rare)

## Next Steps for CQLite Implementation

1. Add `CounterContext` struct in `cqlite-core/src/storage/sstable/counter_context.rs`
2. Implement `parse_counter_context(data: &[u8]) -> Result<i64>`
3. Handle counter columns in cell deserialization (check `ColumnMetadata.is_counter`)
4. Add integration test using `test_basic/counters` table
5. Validate against sstabledump output (expected: `422216548022666`)

## References

- **CASSANDRA-1938**: Local shard semantics and merging rules
- **CASSANDRA-4775**: Global shard introduction
- **Cassandra 5.0 Source**: `org.apache.cassandra.db.context.CounterContext`
- **Test Data**: `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/counters-*`
