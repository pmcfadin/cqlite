# Issue #164: Current Status & Blocker

## Summary

**Status**: Multi-chunk stitching implemented but only parsing 1 entry instead of 1000 rows.

**Root Cause**: The V5CompressedLegacy parser assumes **1 partition = 1 row**, but Cassandra's data model allows **1 partition with multiple rows**.

## What's Working ✅

1. **Multi-chunk stitching**: Successfully decompresses and concatenates all 41 chunks (663,863 bytes)
2. **Schema-aware cell parsing**: All 18 column types parse correctly with proper types
3. **Single row parsing**: First row parses perfectly with all 18 cells
4. **Test infrastructure**: Tests pass (but with lowered expectations)

## What's NOT Working ❌

**Only 1 entry parsed instead of 1000 rows**

```
Read 1 entries from simple_table  ← Should be 1000!
Entry 0: [18 columns with correct types] ✅
```

## Root Cause Analysis

### Cassandra Data Model

```
Partition (identified by partition key)
├─ Row 1 (clustering key or static columns)
├─ Row 2
├─ Row 3
└─ ... Row 1000
```

**The simple_table likely has**:
- **1 partition** with UUID partition key
- **1000 rows** inside that partition (with different clustering keys or static columns)

### Current Parser Assumption (WRONG)

```rust
// parse_block() expects:
while offset < data.len() {
    let partition_key = parse_partition_header();  // ← Assumes each row has own partition!
    let row_data = parse_row_data();
    results.push((table_id, partition_key, row_value));
}
```

This finds 1 partition header → parses 1 row → calculates next offset → **finds invalid data** → stops.

### Correct Parser Logic (NEEDED)

```rust
while offset < data.len() {
    let partition_key = parse_partition_header();

    // Parse ALL rows within this partition!
    while row_exists_in_partition() {
        let row_data = parse_row_data();
        results.push((table_id, partition_key.clone(), row_value));
    }
}
```

## Evidence

### 1. Test Expectation
From `v5_compressed_legacy_integration_test.rs:207`:
```rust
"simple_table", 1000  // expected row count per JSONL (actual: 999)
```

### 2. JSONL File Structure
```bash
$ wc -l test-data/.../nb-1-big-Data.db.jsonl
999
```

Each JSONL line represents a **row**, not a partition. If there were 1000 partitions, there would be 1000 separate partition keys.

### 3. Parser Behavior
- Parses partition header at offset 0 successfully
- Parses row header + 18 cells successfully
- Calculates `next_offset = 633` using row_size
- Offset 633 has **no valid partition header** (flags=0x92, key_len=234 is invalid)
- **Why?** Because offset 633 is the start of ROW 2, not PARTITION 2!

## What Needs to Change

### File: `v5_compressed_legacy.rs`

**Current `parse_block()` structure**:
```rust
fn parse_block(...) -> Vec<(TableId, RowKey, Value)> {
    while offset < data.len() {
        // Parse partition header
        let (partition_key, offset) = parse_partition_header(data, offset)?;

        // Parse ONE row
        let (cells, row_header, offset) = parse_row_data_with_offset(...)?;

        results.push((table_id, partition_key, Value::Map(cells)));
        // ❌ WRONG: Assumes we're done with this partition!
    }
}
```

**Needed structure**:
```rust
fn parse_block(...) -> Vec<(TableId, RowKey, Value)> {
    while offset < data.len() {
        // Parse partition header
        let (partition_key, offset) = parse_partition_header(data, offset)?;

        // Parse ALL rows in this partition!
        loop {
            match parse_row_data_with_offset(...) {
                Ok((cells, row_header, new_offset)) => {
                    results.push((table_id, partition_key.clone(), Value::Map(cells)));
                    offset = new_offset;

                    // Check if next bytes are a new partition header or another row
                    if is_partition_header(data, offset) {
                        break; // Start new partition
                    }
                    // Otherwise continue parsing rows in same partition
                }
                Err(_) => break, // End of partition
            }
        }
    }
}
```

## Key Questions

### Q1: How to detect partition boundaries vs row boundaries?

**Options**:
1. **Row count in partition header**: Does partition header encode how many rows follow?
2. **End marker**: Is there a special byte sequence marking end of partition?
3. **Heuristic detection**: Try to parse partition header; if it fails, assume it's another row

**Need to check**: Cassandra 5.0 V5CompressedLegacy partition header format documentation

### Q2: What distinguishes partition header from row header?

Current partition header structure (lines 325-382):
```rust
[flags: u8]
[key_len: u8]
[key_bytes: key_len bytes]
[partition_deletion_time: i32 BE]
[unknown_8_bytes: 8 bytes]
```

Row header structure (lines 269-324):
```rust
[row_flags: u8]
[extended_flags: u8 if 0x80 set]
[row_size: VInt]
[prev_size: VInt]
[timestamp: VInt if 0x04 set]
[ttl: VInt if 0x08 set]
[deletion: 2 VInts if 0x10 set]
[column_bitmap: VInt + bytes if NOT 0x20]
```

**Distinguishing feature**: Partition flags are simple (≤0x20), row flags can be complex (0x04, 0x08, 0x10, 0x20, 0x80).

### Q3: Simple table schema

Does `simple_table` have:
- **No clustering keys**: All 1000 rows in single partition (unusual but possible)
- **Clustering keys**: 1000 rows with different clustering key values

**Action**: Check schema extraction from Statistics.db to see clustering key definition.

## Immediate Next Steps

1. **Investigate partition header format**:
   - Check if partition header includes row count
   - Look for end-of-partition marker in format docs

2. **Modify parse_block() to handle multiple rows per partition**:
   - Add inner loop to parse all rows in partition
   - Detect partition boundary (new partition header vs another row)

3. **Test with single partition**:
   - Verify we can parse all 1000 rows from one partition
   - Validate against JSONL ground truth

## Estimated Time

- **Research partition/row boundaries**: 1-2 hours
- **Implement multi-row partition parsing**: 2-3 hours
- **Test and validate**: 1-2 hours
- **Total**: 4-7 hours

## Risk Assessment

**High confidence this is the issue** because:
1. Test expects 1000 rows but we parse 1 entry
2. Offset after first row (633) has invalid partition header data
3. Cassandra data model supports multiple rows per partition
4. Current parser assumes 1 partition = 1 row

**Low risk of breaking existing functionality** because:
1. Change is isolated to V5CompressedLegacy parser
2. Other formats use different parsers
3. Extensive test coverage will catch regressions

## References

- Cassandra partition/row model: https://cassandra.apache.org/doc/latest/cassandra/data-modeling/data-modeling-conceptual.html
- V5CompressedLegacy format: `docs/sstables-definitive-guide/chapters/05-data-db-format.md`
- Current implementation: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs` lines 154-244
