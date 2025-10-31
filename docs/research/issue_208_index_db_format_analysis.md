# Issue #208: Index.db Format Analysis - stock_prices Returns 0 Rows

## Problem Summary

The `stock_prices` table returns 0 rows when queried, even though the Data.db file contains 3 partitions (AMZN, GOOG, AAPL). Debug logs show: **"Converted 0 partition entries from IndexReader to SSTableIndex"**

## Root Cause

The `IndexReader` parser in `cqlite-core/src/storage/sstable/index_reader.rs` implements a **single format parser** that expects:
- 2-byte marker: `0x0010`
- 16-byte MD5 partition key digest
- Variable-length offset encoding

However, the `stock_prices` Index.db file uses a **completely different format** (BTI/Partition Key format) that stores:
- 2-byte entry length prefix
- 2-byte partition key length
- Variable-length actual partition key (not digest)
- Additional clustering/offset data

## Test Results

Created three diagnostic tests in `index_reader.rs`:
1. `test_stock_prices_index_db_parsing` - Direct format parsing
2. `test_stock_prices_index_reader` - IndexReader API test
3. `test_stock_prices_sstable_reader_integration` - SSTableReader integration test

All three tests confirm the same failure:

```
[DEBUG] First 20 bytes: [00, 0e, 00, 04, 41, 4d, 5a, 4e, 00, 00, 04, 80, 00, 4f, 88, 00, 00, 00, 00, 0e]
[DEBUG]     first_word=0x000e
[DEBUG]     first_word is length prefix, reading actual marker
[DEBUG]     marker=0x0004 (expected 0x0010)
[DEBUG]     MARKER MISMATCH: got 0x0004, expected 0x0010
[DEBUG] parse_all_partition_keys_with_summary: Total entries parsed: 0
```

## Format Analysis

### Format 1: MD5 Digest Format (Currently Supported)

**Structure:**
```
[marker: 0x0010] [digest: 16 bytes] [offset_len: 1 byte] [offset: variable]
```

**Example (sensor_data):**
```
00000000  00 10 02 84 a7 18 be 7b  49 e6 b6 b9 8e 82 f5 ff  |.......{I.......|
00000010  16 60 00 00 00 10 7d 39  42 8c aa a8 45 1d 84 7f  |.`....}9B...E...|
```

**Used by:**
- sensor_data (217 bytes, digest format)
- user_sessions (4.2K, digest format)
- event_store (4.1K, digest format)
- simple_table (21K, digest format)
- All test_basic tables (composite_key_table, ttl_test_table, etc.)
- All test_collections tables
- All test_wide_rows tables

### Format 2: BTI/Partition Key Format (NOT Supported)

**Structure:**
```
[entry_len: 2 bytes] [key_len: 2 bytes] [key: variable] [clustering_data: variable] [offset: variable] [padding: 2 bytes]
```

**Example (stock_prices, 56 bytes total):**
```
Entry 1: 00 0e 00 04 41 4d 5a 4e 00 00 04 80 00 4f 88 00 00 00
         ^---^ ^---^ ^---------^ ^--------------^ ^---------^ ^---^
         len   klen  "AMZN"      clustering?      offset      pad

Entry 2: 00 0e 00 04 47 4f 4f 47 00 00 04 80 00 4f 88 00 90 db 00
         ^---^ ^---^ ^---------^ ^--------------^ ^---------^ ^---^
         len   klen  "GOOG"      clustering?      offset      pad

Entry 3: 00 0e 00 04 41 41 50 4c 00 00 04 80 00 4f 88 00 a0 b7 00
         ^---^ ^---^ ^---------^ ^--------------^ ^---------^ ^---^
         len   klen  "AAPL"      clustering?      offset      pad
```

**Detailed Breakdown:**
- `00 0e` = 14 bytes (entry length excluding length prefix itself)
- `00 04` = 4 bytes (partition key length)
- `41 4d 5a 4e` = "AMZN" (actual partition key, NOT MD5 digest)
- `00 00 04 80 00 4f 88` = Clustering/timestamp data? (7 bytes)
- `00 00 00` = Offset or padding (3 bytes)

**Used by (timeseries tables with text partition keys):**
- **stock_prices** (56 bytes) - text partition key (ticker_symbol)
- tick_data (743 bytes) - composite partition key
- time_bucketed_counters (12 bytes) - text partition key
- log_entries (6.4K) - composite partition key
- app_metrics (4.3K) - composite partition key
- counters (52 bytes) - text partition key

## Pattern Recognition

Tables using **Format 2 (BTI/Partition Key)** share these characteristics:
1. **Text partition keys** (not UUID or numeric types)
2. **Simple partition keys** (single text column or simple composites)
3. **Timeseries or counter tables**
4. **Smaller Index.db files** (typically <1K for simple cases)

Tables using **Format 1 (MD5 Digest)** share these characteristics:
1. **Complex partition keys** (UUID, composite types)
2. **Larger Index.db files** (multi-KB)
3. **Wide partitions** or many partitions
4. **Collections, UDTs, wide rows**

## Cassandra 5.0 Format Context

The BTI (Big Table Index) format was introduced in Cassandra 5.0 as an optimization for tables with:
- Simple partition keys (especially text/varchar)
- Token-ordered access patterns
- Reduced memory footprint for index structures

The format stores **actual partition keys** instead of MD5 digests to enable:
- Direct partition key comparison without hashing
- Efficient range scans by partition key
- Reduced index memory overhead

## Parsing Failure Location

**File:** `cqlite-core/src/storage/sstable/index_reader.rs`

**Function:** `parse_simple_partition_key_with_offset` (line 363)

**Failure Point:**
```rust
let (input, marker) = if first_word == 0x0010 {
    (input, first_word)
} else {
    be_u16(input)?  // Reads 0x0004, interprets as marker
};

if marker != 0x0010 {  // FAILS HERE for Format 2
    return Err(nom::Err::Error(...));
}
```

The parser:
1. Reads first word: `0x000e` (entry length in Format 2)
2. Interprets as length prefix, reads next word: `0x0004` (key length in Format 2)
3. Expects `0x0010` marker, gets `0x0004` instead
4. **Rejects entry as invalid format**
5. Returns 0 entries parsed

## Impact Assessment

### Affected Tables (Format 2 users)
- test_timeseries/stock_prices ❌ (0 rows returned)
- test_timeseries/tick_data ❌ (likely 0 rows)
- test_timeseries/time_bucketed_counters ❌
- test_timeseries/log_entries ❌
- test_timeseries/app_metrics ❌
- test_basic/counters ❌

### Working Tables (Format 1 users)
- test_basic/simple_table ✓
- test_collections/* ✓
- test_wide_rows/* ✓
- test_timeseries/sensor_data ✓
- test_timeseries/user_sessions ✓
- test_timeseries/event_store ✓

## Solution Requirements

1. **Format Detection**: Detect which Index.db format is in use
   - Check for `0x0010` marker vs. length prefix pattern
   - Possibly check file size or first 4 bytes

2. **Dual Parser Implementation**: Implement parsers for both formats
   - `parse_digest_format` - current implementation
   - `parse_bti_format` - new implementation for Format 2

3. **Format Selection Logic**: Route to appropriate parser
   ```rust
   fn parse_index_data_with_summary(input: &[u8], ...) -> IResult<...> {
       if looks_like_digest_format(input) {
           parse_digest_format(input, ...)
       } else if looks_like_bti_format(input) {
           parse_bti_format(input, ...)
       } else {
           Err(...)
       }
   }
   ```

4. **Partition Key Handling**: Convert BTI partition keys to digests
   - BTI format stores raw keys, need to compute MD5 for lookup tables
   - Or: Store raw keys directly and update lookup logic

5. **Test Coverage**: Ensure both formats are tested
   - Existing tests cover Format 1
   - New tests needed for Format 2 (stock_prices, tick_data, etc.)

## Next Steps

1. ✅ **Diagnostic tests created** (3 tests in index_reader.rs)
2. ✅ **Format analysis complete** (documented above)
3. ✅ **Root cause identified** (single-format parser limitation)
4. ⏭️ **Implement BTI format parser** (new function)
5. ⏭️ **Add format detection logic** (heuristic or header-based)
6. ⏭️ **Update integration tests** (test both formats)
7. ⏭️ **Validate against sstabledump** (ensure correctness)

## References

- Issue: #208
- File: `cqlite-core/src/storage/sstable/index_reader.rs`
- Test data: `test-data/datasets/sstables/test_timeseries/stock_prices-*/nb-1-big-Index.db`
- Cassandra 5.0 BTI documentation: https://issues.apache.org/jira/browse/CASSANDRA-9425
