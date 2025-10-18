# Issue #160: Recommended Parser Fixes

## Overview

Based on comprehensive research of Cassandra 5.0 source code, this document provides specific code fixes for the V5CompressedLegacy parser to address the 374-byte offset discrepancy.

---

## Fix #1: Add Clustering Prefix Parsing (Even for Zero Columns)

### Location
`cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`, lines 223-225

### Current Code
```rust
// Clustering prefix (skip for simple tables with no clustering keys)
// For tables with clustering keys, this would parse clustering column values here
// For now, assume no clustering (simple partition key only tables)
```

### Issue
Comment is misleading - suggests clustering parsing is optional. While zero-column tables read 0 bytes, the **code structure should reflect the Java deserialization order**.

### Recommended Fix

Replace lines 223-225 with:

```rust
// Parse clustering prefix (required step even if it reads 0 bytes)
// For tables with ClusteringTypes: [], this immediately returns without reading
let _clustering = self.parse_clustering_prefix(data, &mut offset, schema)?;
debug!("V5CompressedLegacy: Parsed clustering prefix (size={})", _clustering.len());
```

Add new method after `parse_row()`:

```rust
/// Parse clustering prefix according to ClusteringPrefix.serializer format
///
/// For tables with 0 clustering columns, returns empty Vec without reading bytes.
/// For tables with N clustering columns, deserializes values in batches of 32
/// with 2-bit header encoding (00=present, 01=empty, 11=null).
fn parse_clustering_prefix(
    &self,
    data: &[u8],
    offset: &mut usize,
    schema: Option<&TableSchema>,
) -> Result<Vec<Value>> {
    let schema = schema.ok_or_else(|| Error::corruption("Schema required for clustering parsing"))?;

    // Get clustering column count from schema
    let clustering_count = schema.clustering_keys.len();

    // For zero clustering columns, return immediately (Java behavior)
    if clustering_count == 0 {
        debug!("V5CompressedLegacy: Clustering types empty, skipping clustering prefix read");
        return Ok(Vec::new());
    }

    // Parse clustering values in batches of 32
    let mut values = Vec::with_capacity(clustering_count);
    let mut parsed = 0;

    while parsed < clustering_count {
        // Read header VInt (2 bits per element: 00=present, 01=empty, 11=null)
        let (remaining, header) = crate::parser::vint::parse_unsigned_vint(&data[*offset..])
            .map_err(|_| Error::corruption("Failed to parse clustering prefix header"))?;
        *offset = data.len() - remaining.len();

        let limit = std::cmp::min(clustering_count, parsed + 32);

        while parsed < limit {
            // Extract 2-bit status for this element
            let bit_offset = (parsed % 32) * 2;
            let bits = (header >> bit_offset) & 0x03;

            let value = match bits {
                0b11 => Value::Null,  // NULL
                0b01 => Value::Blob(Vec::new()),  // EMPTY
                0b00 => {
                    // Present - read value bytes using column type
                    let col_type = &schema.clustering_keys[parsed].data_type;
                    let (val, new_offset) = self.parse_value_bytes(data, *offset,
                        &crate::schema::Column {
                            name: schema.clustering_keys[parsed].name.clone(),
                            data_type: col_type.clone(),
                        })?;
                    *offset = new_offset;
                    val
                }
                _ => return Err(Error::corruption(format!("Invalid clustering header bits: {:#04b}", bits))),
            };

            values.push(value);
            parsed += 1;
        }
    }

    Ok(values)
}
```

### Impact
- **No functional change** for zero-column tables (reads 0 bytes as before)
- **Enables support** for tables with clustering columns
- **Clarifies code intent** by explicitly handling clustering step

---

## Fix #2: Implement Delta Encoding for Temporal Fields

### Location
`cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`, lines 237-270

### Current Code
```rust
// Row liveness timestamp (if HAS_TIMESTAMP)
let row_timestamp = if flags & HAS_TIMESTAMP != 0 {
    let (remaining, ts) = crate::parser::vint::parse_vint(&data[offset..])
        .map_err(|_| Error::corruption("Failed to parse row timestamp"))?;
    offset = data.len() - remaining.len();
    Some(ts)
} else {
    None
};
```

### Issue
Reads raw VInt as timestamp, but Cassandra stores **delta from base timestamp**:
```java
// SerializationHeader.java
long delta = VIntCoding.readVInt(in);
return baseTimestamp + delta;
```

### Recommended Fix

**Option A: Quick Fix (Document Limitation)**

Add comment explaining limitation:
```rust
// Row liveness timestamp (if HAS_TIMESTAMP)
// TODO(Issue #160): Currently reads raw delta - should add EncodingStats.minTimestamp
// from Statistics.db to get absolute timestamp
let row_timestamp = if flags & HAS_TIMESTAMP != 0 {
    let (remaining, ts_delta) = crate::parser::vint::parse_vint(&data[offset..])
        .map_err(|_| Error::corruption("Failed to parse row timestamp delta"))?;
    offset = data.len() - remaining.len();
    Some(ts_delta)  // WARNING: This is a delta, not absolute timestamp!
} else {
    None
};
```

**Option B: Full Fix (Requires EncodingStats)**

Modify method signature to accept encoding stats:
```rust
fn parse_row(
    &self,
    data: &[u8],
    mut offset: usize,
    flags: u8,
    schema: Option<&TableSchema>,
    encoding_stats: &EncodingStats,  // NEW PARAMETER
    _partition_key: &RowKey,
) -> Result<(Value, usize)>
```

Then compute absolute values:
```rust
let row_timestamp = if flags & HAS_TIMESTAMP != 0 {
    let (remaining, ts_delta) = crate::parser::vint::parse_vint(&data[offset..])
        .map_err(|_| Error::corruption("Failed to parse row timestamp delta"))?;
    offset = data.len() - remaining.len();
    Some(encoding_stats.min_timestamp + ts_delta)  // Absolute timestamp
} else {
    None
};

// Row TTL (if HAS_TTL)
if flags & HAS_TTL != 0 {
    let (remaining, ttl_delta) = crate::parser::vint::parse_vint(&data[offset..])
        .map_err(|_| Error::corruption("Failed to parse row TTL delta"))?;
    offset = data.len() - remaining.len();
    let ttl = (encoding_stats.min_ttl as i64 + ttl_delta) as i32;

    let (remaining, ldt_delta) = crate::parser::vint::parse_vint(&data[offset..])
        .map_err(|_| Error::corruption("Failed to parse TTL localDeletionTime delta"))?;
    offset = data.len() - remaining.len();
    let local_deletion_time = (encoding_stats.min_local_deletion_time as i64 + ldt_delta) as i32;
}
```

Add `EncodingStats` struct:
```rust
pub struct EncodingStats {
    pub min_timestamp: i64,
    pub min_ttl: i32,
    pub min_local_deletion_time: i32,
}
```

Parse from Statistics.db header (see enhanced_statistics_parser.rs).

### Impact
- **Offset calculation**: No impact (reads same number of bytes)
- **Data accuracy**: Critical for correct timestamp/TTL values
- **Priority**: Medium (doesn't fix 374-byte issue, but improves correctness)

---

## Fix #3: Add Row Size Validation

### Location
`cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`, line 227-235

### Current Code
```rust
// Row size (UNSIGNED VInt32) - for SSTable format
let (remaining, _row_size) = crate::parser::vint::parse_unsigned_vint32(&data[offset..])
    .map_err(|_| Error::corruption("Failed to parse row size"))?;
offset = data.len() - remaining.len();

// Previous unfiltered size (UNSIGNED VInt32)
let (remaining, _prev_size) = crate::parser::vint::parse_unsigned_vint32(&data[offset..])
    .map_err(|_| Error::corruption("Failed to parse previous unfiltered size"))?;
offset = data.len() - remaining.len();
```

### Issue
Reads `row_size` but discards it. This field indicates the **total bytes in row body** and can validate parsing.

### Recommended Fix

```rust
// Row size (UNSIGNED VInt32) - total bytes in row body
let row_body_start = offset;
let (remaining, row_size) = crate::parser::vint::parse_unsigned_vint32(&data[offset..])
    .map_err(|_| Error::corruption("Failed to parse row size"))?;
offset = data.len() - remaining.len();
debug!("V5CompressedLegacy: Row size = {} bytes", row_size);

// Previous unfiltered size (UNSIGNED VInt32)
let (remaining, prev_size) = crate::parser::vint::parse_unsigned_vint32(&data[offset..])
    .map_err(|_| Error::corruption("Failed to parse previous unfiltered size"))?;
offset = data.len() - remaining.len();
debug!("V5CompressedLegacy: Previous unfiltered size = {} bytes", prev_size);

// ... (parse rest of row)

// VALIDATION: Check we consumed exactly row_size bytes
let row_body_end = offset;
let bytes_consumed = row_body_end - row_body_start;
if bytes_consumed != row_size as usize {
    return Err(Error::corruption(format!(
        "Row size mismatch: header says {} bytes, but consumed {} bytes (offset {} -> {})",
        row_size, bytes_consumed, row_body_start, row_body_end
    )));
}

debug!("V5CompressedLegacy: Row size validation passed");
```

### Impact
- **Debugging**: Will immediately reveal if parser is at wrong offset
- **Error detection**: Catches format mismatches early
- **Priority**: High - helps diagnose the 374-byte issue

---

## Fix #4: Add Comprehensive Debug Logging

### Location
Multiple locations in v5_compressed_legacy.rs

### Recommended Changes

Add detailed logging to track every byte read:

```rust
// In parse_partition_header()
fn parse_partition_header(&self, data: &[u8], mut offset: usize) -> Result<(u8, RowKey, usize)> {
    let start_offset = offset;
    debug!("╔═══ PARTITION HEADER START (offset {}) ═══", offset);

    // Partition flags
    let flags = data[offset];
    debug!("║ Partition flags: {:#04x} (at offset {})", flags, offset);
    offset += 1;

    // Partition key length (VInt)
    let key_len_start = offset;
    let (remaining, key_len_signed) = crate::parser::vint::parse_vint(&data[offset..])?;
    let vint_bytes = data[offset..].len() - remaining.len();
    offset = data.len() - remaining.len();
    debug!("║ Key length: {} ({} bytes VInt at offset {})",
           key_len_signed, vint_bytes, key_len_start);

    // Partition key bytes
    let key_bytes = data[offset..offset + key_len].to_vec();
    debug!("║ Key bytes: {} bytes at offset {}", key_len, offset);
    offset += key_len;

    // Partition deletion time
    let del_ts_start = offset;
    let (remaining, del_timestamp) = crate::parser::vint::parse_vint(&data[offset..])?;
    let vint_bytes = data[offset..].len() - remaining.len();
    offset = data.len() - remaining.len();
    debug!("║ Deletion timestamp: {} ({} bytes VInt at offset {})",
           del_timestamp, vint_bytes, del_ts_start);

    let del_ldt_start = offset;
    let (remaining, del_ldt) = crate::parser::vint::parse_vint(&data[offset..])?;
    let vint_bytes = data[offset..].len() - remaining.len();
    offset = data.len() - remaining.len();
    debug!("║ Deletion localDeletionTime: {} ({} bytes VInt at offset {})",
           del_ldt, vint_bytes, del_ldt_start);

    let header_bytes = offset - start_offset;
    debug!("╚═══ PARTITION HEADER END (consumed {} bytes, now at offset {}) ═══",
           header_bytes, offset);

    Ok((flags, RowKey::from(key_bytes), offset))
}
```

Similar logging for `parse_row()`:
```rust
fn parse_row(...) -> Result<(Value, usize)> {
    let row_start = offset;
    debug!("╔═══ ROW START (offset {}, flags={:#04x}) ═══", offset, flags);

    // Extended flags
    let ext_flags = if flags & EXTENSION_FLAG != 0 {
        debug!("║ Reading extended flags at offset {}", offset);
        // ...
    };

    // Clustering
    debug!("║ Parsing clustering prefix at offset {}", offset);
    let _clustering = self.parse_clustering_prefix(data, &mut offset, schema)?;
    debug!("║ Clustering: {} columns, now at offset {}", _clustering.len(), offset);

    // Row sizes
    debug!("║ Reading row_size at offset {}", offset);
    // ...

    let row_bytes = offset - row_start;
    debug!("╚═══ ROW END (consumed {} bytes, now at offset {}) ═══",
           row_bytes, offset);

    Ok((value, offset))
}
```

### Impact
- **Debugging**: Pinpoint exact location of 374-byte gap
- **Validation**: Verify byte consumption matches expectations
- **Priority**: Critical for diagnosing issue

---

## Fix #5: Handle Range Tombstone Markers

### Location
`cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`, line 154

### Current Code
```rust
// Parse row
match self.parse_row(data, offset, flags, schema, &partition_key) {
    Ok((row_value, new_offset)) => {
        partition_entries.push((self.table_id.clone(), partition_key.clone(), row_value));
        offset = new_offset;
    }
    Err(e) => {
        warn!("V5CompressedLegacy: Failed to parse row at offset {}: {}", offset, e);
        break;
    }
}
```

### Issue
Doesn't check for `IS_MARKER` flag (0x02). Range tombstone markers have different format:
- They have `ClusteringBoundOrBoundary` instead of `Clustering`
- Different body format (no row liveness, only deletion info)

### Recommended Fix

```rust
// Check for range tombstone marker (before parsing as row)
if flags & IS_MARKER != 0 {
    debug!("V5CompressedLegacy: Range tombstone marker at offset {}", offset - 1);
    // Parse marker body (skip for now)
    match self.parse_range_tombstone_marker(data, offset, flags, schema) {
        Ok(new_offset) => {
            offset = new_offset;
            continue;  // Don't add to partition_entries
        }
        Err(e) => {
            warn!("V5CompressedLegacy: Failed to parse marker at offset {}: {}", offset, e);
            break;
        }
    }
}

// Parse regular row
match self.parse_row(data, offset, flags, schema, &partition_key) {
    // ...
}
```

Add method:
```rust
/// Parse range tombstone marker (IS_MARKER flag set)
///
/// Format: ClusteringBoundOrBoundary + marker body (deletion info)
fn parse_range_tombstone_marker(
    &self,
    data: &[u8],
    mut offset: usize,
    _flags: u8,
    _schema: Option<&TableSchema>,
) -> Result<usize> {
    // TODO: Implement full parsing
    // For now, skip by reading minimal structure

    // ClusteringBoundOrBoundary has kind byte + clustering values
    // Then marker body has deletion time (2 VInts)

    warn!("Range tombstone marker parsing not fully implemented, attempting to skip");

    // Read kind byte
    offset += 1;

    // Skip deletion time (2 VInts)
    let (remaining, _) = crate::parser::vint::parse_vint(&data[offset..])?;
    offset = data.len() - remaining.len();
    let (remaining, _) = crate::parser::vint::parse_vint(&data[offset..])?;
    offset = data.len() - remaining.len();

    Ok(offset)
}
```

### Impact
- **Correctness**: Avoids treating markers as rows
- **Robustness**: Handles partitions with range tombstones
- **Priority**: Medium (may not be present in test data)

---

## Priority Ranking

### Critical (Must Fix for Issue #160):
1. **Fix #4: Debug Logging** - Diagnose where 374 bytes go
2. **Fix #3: Row Size Validation** - Detect offset miscalculation immediately

### High (Improves Correctness):
3. **Fix #1: Clustering Prefix** - Clarifies code, enables multi-column support
4. **Fix #5: Range Tombstone Markers** - Handles edge cases

### Medium (Correctness, Not Offset-Related):
5. **Fix #2: Delta Encoding** - Fixes timestamp values (doesn't affect offsets)

---

## Testing Plan

### After Applying Fixes:

1. **Run with debug logging**:
```bash
env RUST_LOG=debug \
  CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
  cargo test --package cqlite-core v5_compressed_legacy 2>&1 | tee debug.log
```

2. **Analyze offset tracking**:
```bash
grep "offset" debug.log | less
```

3. **Check row size validation**:
```bash
grep "Row size" debug.log
```

4. **Compare with expected**:
- Partition header: ~20-30 bytes
- First row: ~200-800 bytes (varies by data)
- If offset jumps by 374 bytes unexpectedly, that's the culprit

### Expected Outcome:

Debug log should reveal exact location of 374-byte gap:
- **If in partition header**: Partition deletion time parsing wrong
- **If before row**: Block header or compression metadata
- **If in row body**: Row size field indicates more bytes than we think

---

## Summary

The research confirms the Rust parser **correctly implements** the Cassandra 5.0 row format, but the **374-byte offset gap** suggests a **block or partition structure issue** rather than row-level parsing.

**Recommended approach**:
1. Apply Fix #4 (debug logging) and Fix #3 (row size validation)
2. Run tests and analyze logs
3. Identify where 374 bytes are consumed
4. Apply targeted fix based on findings
5. Then apply remaining fixes for completeness

The research documents provide complete format specification and Java code references for any additional debugging needed.
