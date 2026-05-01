# Issue #438 Analysis: Composite Partition Key Format Bug

## Problem Summary

All 4 time-series tables with composite partition keys fail Cassandra 5 import with errors like:
- `EOF after 0 bytes out of 180177`
- `Invalid Columns subset bytes; too many bits set:1110`
- `SSTable first key > last key`

**Root cause**: CQLite omits the end-of-component (EOC) byte after the last component in composite partition keys.

## The Bug

### Cassandra's Actual Format (CompositeType)

According to `cassandra/src/java/org/apache/cassandra/db/marshal/CompositeType.java` (lines 47-70):

```
<component><component><component> ...
where <component> is:
  <length of value><value><'end-of-component' byte>
```

The end-of-component byte is:
- `0x00` for partition keys
- `0x01` for inclusive query bounds
- `0xFF` (-1) for exclusive bounds

**Every component** includes the EOC byte, including the last one.

### CQLite's Current Implementation

From `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/write_engine/mutation.rs` (lines 234-254):

```rust
// Multi-component key: [len1][val1][0x00][len2][val2][0x00]...[lenN][valN]
// 0x00 separator after each component EXCEPT the last (Issue #380, #422)
let num_components = self.columns.len();
for (i, (_, value)) in self.columns.iter().enumerate() {
    let value_bytes = self.serialize_value(value, &schema.partition_keys[i])?;
    let len = value_bytes.len();
    // 2-byte big-endian length prefix
    result.extend_from_slice(&(len as u16).to_be_bytes());
    result.extend_from_slice(&value_bytes);

    // Add 0x00 separator after each component EXCEPT the last
    if i < num_components - 1 {  // ← BUG: should be unconditional
        result.push(0x00);
    }
}
```

### Evidence from Real SSTable

From `test-data/datasets/sstables/test_timeseries/app_metrics-*/nb-1-big-Index.db`:

```
Partition key: ("goal", "interest")
Bytes: 00 04 67 6f 61 6c 00 00 08 69 6e 74 65 72 65 73 74 00
       ^^^^^ ^^^^^^^^^^^ ^^ ^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^ ^^
       len1  "goal"     EOC len2  "interest"                EOC
```

Both components have EOC bytes (0x00), including the last one.

## Format Comparison

### Two-component key: ("app1", "cpu_usage")

**CQLite current** (18 bytes):
```
00 04 61 70 70 31 00 00 09 63 70 75 5f 75 73 61 67 65
^^    ^^^^^^^^^^    ^^    ^^^^^^^^^^^^^^^^^^^^^^^^
len1  "app1"       sep    len2  "cpu_usage"
```

**Cassandra correct** (19 bytes):
```
00 04 61 70 70 31 00 00 09 63 70 75 5f 75 73 61 67 65 00
^^    ^^^^^^^^^^    ^^    ^^^^^^^^^^^^^^^^^^^^^^^^    ^^
len1  "app1"       EOC   len2  "cpu_usage"            EOC
```

**Difference**: Missing trailing 0x00 byte.

### Three-component key: ("api", "ERROR", timestamp)

**CQLite current** (24 bytes):
```
00 03 61 70 69 00 00 05 45 52 52 4f 52 00 00 08 00 00 01 8c c2 51 f4 00
                                                                        ^^
                                                                      missing!
```

**Cassandra correct** (25 bytes):
```
00 03 61 70 69 00 00 05 45 52 52 4f 52 00 00 08 00 00 01 8c c2 51 f4 00 00
                                                                        ^^ ^^
                                                                     value EOC
```

## Impact

### Affected Tables (5 composite-key tables)

From `test-data/schemas/time-series.cql`:

1. **app_metrics**: `(application_id TEXT, metric_name TEXT)` - line 40
2. **user_activity**: `(user_id UUID, activity_date DATE)` - line 60
3. **stock_prices**: `(symbol TEXT, trading_day DATE)` - line 80
4. **log_entries**: `(service_name TEXT, log_level TEXT, hour_bucket TIMESTAMP)` - line 101
5. **tick_data**: `(symbol TEXT, exchange TEXT, minute_bucket TIMESTAMP)` - line 165

### Error Manifestations

1. **Index.db parsing**: Cassandra expects 19 bytes but CQLite writes 18, causing offset misalignment
2. **Data.db partition headers**: Wrong key length causes all subsequent partition reads to fail
3. **Token ordering**: Murmur3 hash computed on wrong bytes, breaking BST ordering assumptions
4. **Bloom filter**: Keys hashed incorrectly, causing false negatives

## Cassandra Source Validation

### AbstractCompositeType.split() (lines 138-149)

```java
while (!ByteBufferAccessor.instance.isEmptyFromOffset(bb, offset)) {
    offset += getComparatorSize(i++, bb, ByteBufferAccessor.instance, offset);
    ByteBuffer value = ByteBufferAccessor.instance.sliceWithShortLength(bb, offset);
    offset += ByteBufferAccessor.instance.sizeWithShortLength(value);
    l.add(value);
    offset++; // skip end-of-component  ← ALWAYS executes
}
```

The `offset++` happens after **every component**, proving all components need EOC bytes.

### ValueAccessor.sliceWithShortLength()

```java
default V sliceWithShortLength(V input, int offset) {
    int size = getUnsignedShort(input, offset);  // Read 2-byte length
    return slice(input, offset + 2, size);       // Skip length, return data
}

default int sizeWithShortLength(V value) {
    return 2 + size(value);  // 2-byte length prefix + data
}
```

This confirms the format: `[u16 BE length][data]`, then caller skips EOC byte separately.

## Documentation Error

The SSTable Definitive Guide at `/Users/patrick/local_projects/cqlite/docs/sstables-definitive-guide/chapters/appendix-b-encodings-cheat-sheet.md` (lines 252-273) **incorrectly states**:

```
[u16 BE: lenN][componentN_bytes]  ← NO trailing 0x00

**CRITICAL**: The 0x00 separator appears after each component EXCEPT the last.
```

This is **WRONG**. The correct format is:
```
[u16 BE: lenN][componentN_bytes][0x00]  ← EOC byte ALWAYS present
```

The documentation was written based on Issues #380 and #422, which were likely misinterpreted.

## Fix Required

### Code Changes

**File**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/write_engine/mutation.rs`

**Line 250-253**: Remove conditional, always add EOC byte:

```rust
// OLD (incorrect):
if i < num_components - 1 {
    result.push(0x00);
}

// NEW (correct):
result.push(0x00);  // EOC byte after every component
```

**Line 234**: Update comment:

```rust
// OLD:
// Multi-component key: [len1][val1][0x00][len2][val2][0x00]...[lenN][valN]
// 0x00 separator after each component EXCEPT the last (Issue #380, #422)

// NEW:
// Multi-component key: [len1][val1][0x00][len2][val2][0x00]...[lenN][valN][0x00]
// Each component has an end-of-component (EOC) byte, always 0x00 for partition keys
// See: cassandra/db/marshal/CompositeType.java lines 47-70
```

**Line 620**: Update test expectation (mutation.rs test):

```rust
let expected = vec![
    0x00, 0x04, // len1 = 4
    0x00, 0x00, 0x00, 0x2A, // int = 42
    0x00, // EOC byte after component 1
    0x00, 0x05, // len2 = 5
    b'h', b'e', b'l', b'l', b'o', // text = "hello"
    0x00, // EOC byte after component 2 (was missing!)
];
```

### Documentation Fix

**File**: `/Users/patrick/local_projects/cqlite/docs/sstables-definitive-guide/chapters/appendix-b-encodings-cheat-sheet.md`

**Lines 257-263**: Correct the format:

```markdown
[u16 BE: len1][component1_bytes][0x00]
[u16 BE: len2][component2_bytes][0x00]
...
[u16 BE: lenN][componentN_bytes][0x00]  ← EOC byte ALWAYS present
```

**Line 263**: Update critical note:

```markdown
**CRITICAL**: Every component has an end-of-component (EOC) byte (0x00 for partition keys).
```

**Lines 267-273**: Update Example 1:

```
0x00 0x04                 ← length of int component (4 bytes)
0x00 0x00 0x00 0x2A       ← int value 42
0x00                      ← EOC byte after first component
0x00 0x05                 ← length of text component (5 bytes)
0x68 0x65 0x6C 0x6C 0x6F  ← text value "hello"
0x00                      ← EOC byte after last component
```

Total: 14 bytes (was 13)

**Line 327**: Update key takeaway:

```markdown
- **Partition keys**: Single-component keys have no length prefix; multi-component keys use 2-byte BE lengths with 0x00 EOC byte after EVERY component (including the last).
```

## Testing

### Unit Test Addition

Add test to verify EOC byte after last component:

```rust
#[test]
fn test_partition_key_eoc_on_last_component() {
    // Test that EOC byte appears after the last component (Issue #438)
    let schema = create_test_schema(vec![("app", "text"), ("metric", "text")], vec![]);
    let pk = PartitionKey::new(vec![
        ("app".to_string(), Value::Text("goal".to_string())),
        ("metric".to_string(), Value::Text("interest".to_string())),
    ]);

    let bytes = pk.to_bytes(&schema).unwrap();

    // Expected: [len1][val1][0x00][len2][val2][0x00]
    let expected = vec![
        0x00, 0x04, // len1 = 4
        b'g', b'o', b'a', b'l', // "goal"
        0x00, // EOC byte
        0x00, 0x08, // len2 = 8
        b'i', b'n', b't', b'e', b'r', b'e', b's', b't', // "interest"
        0x00, // EOC byte (critical!)
    ];
    assert_eq!(bytes, expected);
    assert_eq!(bytes.len(), 18); // 2+4+1 + 2+8+1 = 18
}
```

### Integration Test

Run export → import cycle with time-series tables:

```bash
# Generate mutations for app_metrics
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/cqlite-issue438 \
  --schema test-data/schemas/time-series.cql \
  --mutation '{"table":{"keyspace":"test_timeseries","table":"app_metrics"},"partition_key":[{"Text":"goal"},{"Text":"interest"}],"clustering_key":[{"Timestamp":1704067200000}],"operations":[{"Write":{"column":"value","value":{"Double":42.5}}}],"timestamp_micros":1704067200000000}'

# Flush to SSTable
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/cqlite-issue438 \
  --schema test-data/schemas/time-series.cql \
  --flush

# Export for Cassandra
cargo run --package cqlite-cli --features write-support -- \
  export-sstable /tmp/export438 --keyspace test_timeseries --table app_metrics \
  --writable --write-dir /tmp/cqlite-issue438 \
  --schema test-data/schemas/time-series.cql

# Import to Cassandra
docker exec -it cassandra-test /opt/cassandra/tools/bin/nodetool import -t test_timeseries app_metrics /tmp/export438

# Verify
docker exec -it cassandra-test /opt/cassandra/bin/cqlsh -e "SELECT * FROM test_timeseries.app_metrics"
```

Should succeed without errors.

## Related Issues

- **#380**: Original composite key implementation (may have introduced bug)
- **#422**: Follow-up composite key fix (preserved the bug)
- **#430-#437**: Previous E2E fixes for simple_table (single partition key)

## References

- Cassandra source: `~/local_projects/cassandra/src/java/org/apache/cassandra/db/marshal/CompositeType.java`
- Cassandra source: `~/local_projects/cassandra/src/java/org/apache/cassandra/db/marshal/AbstractCompositeType.java`
- CQLite implementation: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/write_engine/mutation.rs`
- Documentation: `/Users/patrick/local_projects/cqlite/docs/sstables-definitive-guide/chapters/appendix-b-encodings-cheat-sheet.md`
